#!/usr/bin/env python3
"""
Option Chain poller (instrument-master → option-contracts → market-quote → Telegram)
- Uses instruments JSON from assets.upstox.com (cached)
- Resolves option contract instrument_keys for given underlying+expiry
- Fetches market quotes in batches and builds CE/PE summary
"""
import os, time, logging, requests, json, gzip, io, math, html
from urllib.parse import quote_plus
import datetime

# ---------- CONFIG / ENV ----------
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN','').strip()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN','').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID','').strip()

OPTION_SYMBOL_NIFTY = os.getenv('OPTION_SYMBOL_NIFTY') or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv('OPTION_EXPIRY_NIFTY') or "2025-10-07"

OPTION_SYMBOL_TCS = os.getenv('OPTION_SYMBOL_TCS') or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv('OPTION_EXPIRY_TCS') or "2025-10-28"

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 5)
INSTRUMENTS_TTL_SECONDS = int(os.getenv('INSTRUMENTS_TTL_SECONDS') or 24*3600)

UPSTOX_INSTRUMENTS_JSON_URL = os.getenv('UPSTOX_INSTRUMENTS_JSON_URL') or "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
UPSTOX_MARKET_QUOTE_URL = os.getenv('UPSTOX_MARKET_QUOTE_URL') or "https://api.upstox.com/v2/market-quote/quotes"
# Option greeks endpoint (optional, docs mention v3)
UPSTOX_OPTION_GREEK_URL = os.getenv('UPSTOX_OPTION_GREEK_URL') or "https://api.upstox.com/v3/market-quote/option-greeks"

# -------- logging & headers ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)
HEADERS = {"Accept":"application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

# ---------- cache paths ----------
CACHE_DIR = "./cache"
os.makedirs(CACHE_DIR, exist_ok=True)
INSTR_CACHE_PATH = os.path.join(CACHE_DIR, "instruments_complete.json")

# ---------- helpers ----------
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=12)
        r.raise_for_status()
        return True
    except Exception as e:
        logging.warning("Telegram send failed: %s", e)
        return False

def _is_cache_fresh(path):
    return os.path.exists(path) and (time.time() - os.path.getmtime(path) < INSTRUMENTS_TTL_SECONDS)

def _write_cache(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)

def _read_cache(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _download_instruments(url):
    logging.info("Trying instruments URL: %s", url)
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.content
        # try gz
        try:
            dec = gzip.decompress(data).decode('utf-8')
            js = json.loads(dec)
            return js
        except Exception:
            # try direct JSON
            try:
                js = json.loads(data.decode('utf-8'))
                return js
            except Exception:
                # try CSV fallback - not implemented; return None
                return None
    except Exception as e:
        logging.warning("Download failed: %s", e)
        return None

def fetch_and_cache_instruments(force_refresh=False):
    if not force_refresh and _is_cache_fresh(INSTR_CACHE_PATH):
        try:
            cached = _read_cache(INSTR_CACHE_PATH)
            logging.info("Using cached instruments (%d items)", len(cached) if isinstance(cached, list) else 0)
            return cached
        except Exception:
            logging.warning("Failed reading cache, will redownload.")
    js = _download_instruments(UPSTOX_INSTRUMENTS_JSON_URL)
    if js:
        # normalize if dict containing lists
        if isinstance(js, dict):
            # try to find the big list
            for v in js.values():
                if isinstance(v, list):
                    js = v
                    break
        try:
            _write_cache(INSTR_CACHE_PATH, js)
            logging.info("Cached instruments to %s", INSTR_CACHE_PATH)
        except Exception as e:
            logging.warning("Failed writing cache: %s", e)
        return js
    # fallback: stale cache if present
    if os.path.exists(INSTR_CACHE_PATH):
        try:
            cached = _read_cache(INSTR_CACHE_PATH)
            logging.warning("Using stale cached instruments (%d items)", len(cached) if isinstance(cached, list) else 0)
            return cached
        except Exception as e:
            logging.error("No usable instruments cache: %s", e)
            return []
    logging.error("Unable to download instruments and no cache found.")
    return []

def ms_to_ymd(ms):
    try:
        return datetime.datetime.utcfromtimestamp(ms/1000.0).strftime("%Y-%m-%d")
    except Exception:
        return None

def resolve_option_instruments_for(instruments, underlying_term, expiry_yyyy_mm_dd):
    """
    Return list of instrument dicts (option contracts) for underlying + expiry.
    We look for segment 'NSE_FO' rows whose expiry matches and name/tradingsymbol contains underlying_term.
    """
    term = underlying_term.strip().lower()
    out = []
    for row in instruments:
        if not isinstance(row, dict):
            continue
        seg = row.get("segment") or row.get("exchangeSegment") or ""
        if seg != "NSE_FO":
            continue
        exp = row.get("expiry") or row.get("expiry_date") or row.get("expiryTimestamp")
        # expiry in ms sometimes
        if exp:
            exp_ymd = None
            if isinstance(exp, (int,float)):
                exp_ymd = ms_to_ymd(exp)
            else:
                # if string like '2025-10-07'
                try:
                    exp_ymd = str(exp).split("T")[0]
                except:
                    exp_ymd = None
            if exp_ymd != expiry_yyyy_mm_dd:
                continue
        else:
            continue
        name = str(row.get("name","")).lower()
        ts = str(row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol") or "").lower()
        # match underlying term in name or trading symbol
        if term in name or term in ts:
            # ensure it's an option (CE or PE)
            if "ce" in ts.split() or "pe" in ts.split() or ts.strip().endswith("ce") or ts.strip().endswith("pe"):
                out.append(row)
    # sort by strike price if present
    def _strike_key(r):
        try:
            return float(r.get("strike_price") or r.get("strike") or 0)
        except:
            return 0
    return sorted(out, key=_strike_key)

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------- Market-quote fetch ----------
def fetch_quotes_for_instrument_keys(instrument_keys):
    """
    Calls UPSTOX_MARKET_QUOTE_URL with instrument_key param.
    Supports batching (Upstox docs mention limits; use batch_size=200 as safe).
    Returns dict: instrument_key -> quote_json
    """
    out = {}
    if not instrument_keys:
        return out
    batch_size = 200
    for batch in chunk_list(instrument_keys, batch_size):
        params = "&".join(f"instrument_key={quote_plus(str(k))}" for k in batch)
        url = UPSTOX_MARKET_QUOTE_URL + "?" + params
        logging.info("Market-quote fetch URL: %s", url[:400])
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            j = r.json()
            # response shape: likely { "data": [ ... ] } or list; handle both
            data = j.get("data") if isinstance(j, dict) and "data" in j else j
            if isinstance(data, list):
                for item in data:
                    key = item.get("instrument_key") or item.get("instrumentKey") or item.get("instrument_token") or None
                    if key:
                        out[key] = item
            elif isinstance(data, dict):
                # single item
                key = data.get("instrument_key") or data.get("instrumentKey")
                if key:
                    out[key] = data
        except Exception as e:
            logging.warning("Market-quote batch fetch failed: %s", e)
    return out

# ---------- Build option chain summary using instrument rows + quotes ----------
def build_option_chain_from_instruments(instrument_rows, quotes_map, window=5):
    """
    instrument_rows: list of option contract rows for an underlying+expiry (sorted by strike)
    quotes_map: instrument_key -> quote json
    Returns strikes list of dicts: { strike, ce: {ltp, oi, iv, key}, pe: {...} }
    """
    strikes = {}
    for row in instrument_rows:
        ik = row.get("instrument_key") or row.get("instrumentKey") or row.get("instrumentToken") or row.get("instrument_token")
        ts = row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol") or ""
        strike = row.get("strike_price") or row.get("strike") or row.get("strikePrice")
        if strike is None:
            # try parsing from trading symbol (last token)
            try:
                parts = str(ts).split()
                for p in parts:
                    if p.isdigit():
                        strike = float(p)
                        break
            except:
                strike = None
        if strike is None:
            continue
        strike = float(strike)
        q = quotes_map.get(ik) or {}
        # extract LTP, OI, IV robustly
        ltp = q.get("last_traded_price") or q.get("ltp") or q.get("lastPrice") or None
        oi = q.get("open_interest") or q.get("oi") or q.get("openInterest") or None
        iv = q.get("iv") or q.get("implied_volatility") or q.get("IV") or None
        # decide CE vs PE from trading symbol
        tsl = str(ts).lower()
        side = "ce" if " ce" in tsl or tsl.endswith("ce") or "call" in tsl else ("pe" if " pe" in tsl or tsl.endswith("pe") or "put" in tsl else None)
        rec = strikes.setdefault(strike, {"strike": strike, "ce": None, "pe": None})
        info = {"instrument_key": ik, "trading_symbol": ts, "ltp": ltp, "oi": oi, "iv": iv}
        if side == "ce":
            rec["ce"] = info
        elif side == "pe":
            rec["pe"] = info
    # convert to sorted list
    strikes_list = [strikes[k] for k in sorted(strikes.keys())]
    return strikes_list

def short_text_for_side(side):
    if not side:
        return "NA"
    ltp = side.get("ltp")
    oi = side.get("oi")
    iv = side.get("iv")
    try:
        l = f"{float(ltp):,.2f}" if ltp is not None else "NA"
    except:
        l = str(ltp) if ltp is not None else "NA"
    try:
        o = f"{int(float(oi)):,}" if oi not in (None,"") else "NA"
    except:
        o = str(oi) if oi not in (None,"") else "NA"
    try:
        v = f"{float(iv):.2f}" if iv not in (None,"") else "NA"
    except:
        v = str(iv) if iv not in (None,"") else "NA"
    return f"{l} / {o} / {v}"

def build_summary_text_from_strikes(label, strikes_list, atm_strike=None, window=5):
    lines = []
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    lines.append(f"📊 <b>Option Chain — {html.escape(label)}</b> — {ts}")
    if not strikes_list:
        lines.append("No option chain data available.")
        return "\n".join(lines)
    try:
        atm = float(atm_strike) if atm_strike is not None else None
    except:
        atm = None
    # find index nearest atm
    idx = 0
    if atm is not None:
        idx = min(range(len(strikes_list)), key=lambda i: abs(float(strikes_list[i]["strike"]) - atm))
    else:
        idx = len(strikes_list)//2
    start = max(0, idx - window)
    end = min(len(strikes_list)-1, idx + window)
    lines.append("<code>Strike    CE(LTP / OI / IV)       |      PE(LTP / OI / IV)</code>")
    for i in range(start, end+1):
        s = strikes_list[i]
        strike = s.get("strike")
        ce = short_text_for_side(s.get("ce"))
        pe = short_text_for_side(s.get("pe"))
        atm_mark = " ⭑" if (atm is not None and float(strike) == atm) else ""
        lines.append(f"<code>{str(int(float(strike))).rjust(6)}{atm_mark}   {ce.ljust(20)} | {pe}</code>")
    return "\n".join(lines)

# ---------- Orchestration ----------
def poll_once_and_send():
    instruments = fetch_and_cache_instruments(force_refresh=False)
    if not instruments:
        logging.warning("No instruments available — skipping this cycle.")
        return
    # resolve option contract rows for each underlying+expiry
    nifty_option_rows = resolve_option_instruments_for(instruments, "nifty", OPTION_EXPIRY_NIFTY)
    tcs_option_rows   = resolve_option_instruments_for(instruments, "tcs", OPTION_EXPIRY_TCS)
    logging.info("Found option row counts: Nifty=%d, TCS=%d", len(nifty_option_rows), len(tcs_option_rows))

    # collect instrument_keys to fetch quotes
    nifty_keys = [r.get("instrument_key") or r.get("instrumentToken") or r.get("instrument_token") for r in nifty_option_rows]
    tcs_keys   = [r.get("instrument_key") or r.get("instrumentToken") or r.get("instrument_token") for r in tcs_option_rows]
    all_keys = [k for k in (nifty_keys + tcs_keys) if k]

    quotes_map = fetch_quotes_for_instrument_keys(all_keys)

    # build strikes lists
    nifty_strikes = build_option_chain_from_instruments(nifty_option_rows, quotes_map, window=STRIKE_WINDOW)
    tcs_strikes   = build_option_chain_from_instruments(tcs_option_rows, quotes_map, window=STRIKE_WINDOW)

    # Try to guess ATM: use underlying's nearest strike from underlying LTP if available
    # attempt to find underlying instrument_key from instruments (NSE_INDEX|Nifty 50 or NSE_EQ|INE... )
    def find_underlying_ltp(instruments, underlying_search):
        for r in instruments:
            ks = (r.get("instrument_key") or r.get("instrumentKey") or "")
            ts = str(r.get("tradingsymbol") or r.get("trading_symbol") or r.get("symbol") or "").lower()
            name = str(r.get("name") or "").lower()
            if underlying_search in ks.lower() or underlying_search in ts or underlying_search in name:
                q = fetch_quotes_for_instrument_keys([ks])
                qq = q.get(ks) if isinstance(q, dict) else None
                if qq:
                    return qq.get("last_traded_price") or qq.get("ltp")
        return None

    nifty_under_ltp = find_underlying_ltp(instruments, "nifty")
    tcs_under_ltp   = find_underlying_ltp(instruments, "tcs")

    nifty_atm = None
    tcs_atm = None
    if nifty_under_ltp:
        try: nifty_atm = float(nifty_under_ltp)
        except: pass
    if tcs_under_ltp:
        try: tcs_atm = float(tcs_under_ltp)
        except: pass

    # for summary we pass atm as numeric nearest strike (optional)
    if nifty_strikes:
        # find nearest strike present to nifty_atm
        atm_strike_n = None
        if nifty_atm:
            atm_strike_n = min([s["strike"] for s in nifty_strikes], key=lambda x: abs(x - nifty_atm))
        summary = build_summary_text_from_strikes("Nifty 50", nifty_strikes, atm_strike_n, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent Nifty summary (ATM approx %s)", atm_strike_n)
    else:
        logging.info("No Nifty strikes to send.")

    if tcs_strikes:
        atm_strike_t = None
        if tcs_atm:
            atm_strike_t = min([s["strike"] for s in tcs_strikes], key=lambda x: abs(x - tcs_atm))
        summary = build_summary_text_from_strikes("TCS", tcs_strikes, atm_strike_t, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent TCS summary (ATM approx %s)", atm_strike_t)
    else:
        logging.info("No TCS strikes to send.")

def main():
    logging.info("Starting Option Chain poller. Interval: %ss. Nifty expiry=%s | TCS expiry=%s", POLL_INTERVAL, OPTION_EXPIRY_NIFTY, OPTION_EXPIRY_TCS)
    # initial load
    instruments = fetch_and_cache_instruments(force_refresh=False)
    logging.info("Initial instruments loaded: %d", len(instruments) if instruments else 0)
    while True:
        try:
            poll_once_and_send()
        except Exception as e:
            logging.exception("Unhandled error during poll: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
