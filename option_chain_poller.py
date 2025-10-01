#!/usr/bin/env python3
"""
Final Option Chain poller with:
- instruments master download & cache
- resolve option contracts for underlying + expiry
- select ATM +/- STRIKE_WINDOW strikes (default 10)
- batch market-quote requests with retry/backoff/throttle
- build compact CE/PE summary and send to Telegram
"""
import os, time, logging, requests, json, gzip, io, html, datetime
from urllib.parse import quote_plus

# ------------- CONFIG -------------
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN','').strip()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN','').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID','').strip()

OPTION_SYMBOL_NIFTY = os.getenv('OPTION_SYMBOL_NIFTY') or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv('OPTION_EXPIRY_NIFTY') or "2025-10-07"

OPTION_SYMBOL_TCS = os.getenv('OPTION_SYMBOL_TCS') or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv('OPTION_EXPIRY_TCS') or "2025-10-28"

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 10)  # default: 10 strikes each side
INSTRUMENTS_TTL_SECONDS = int(os.getenv('INSTRUMENTS_TTL_SECONDS') or 24*3600)

UPSTOX_INSTRUMENTS_JSON_URL = os.getenv('UPSTOX_INSTRUMENTS_JSON_URL') or "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
UPSTOX_MARKET_QUOTE_URL = os.getenv('UPSTOX_MARKET_QUOTE_URL') or "https://api.upstox.com/v2/market-quote/quotes"

# ------------- Logging & headers -------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)
HEADERS = {"Accept":"application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

# ------------- Cache paths -------------
CACHE_DIR = "./cache"
os.makedirs(CACHE_DIR, exist_ok=True)
INSTR_CACHE_PATH = os.path.join(CACHE_DIR, "instruments_complete.json")

# ------------- Helpers -------------
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
        # try gzipped JSON
        try:
            dec = gzip.decompress(data).decode('utf-8')
            js = json.loads(dec)
            return js
        except Exception:
            pass
        # try plain JSON
        try:
            js = json.loads(data.decode('utf-8'))
            return js
        except Exception:
            logging.warning("Downloaded instruments but couldn't parse as JSON.")
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
        # normalize if dict
        if isinstance(js, dict):
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
    term = underlying_term.strip().lower()
    out = []
    for row in instruments:
        if not isinstance(row, dict):
            continue
        seg = row.get("segment") or row.get("exchangeSegment") or ""
        if seg != "NSE_FO":
            continue
        exp = row.get("expiry") or row.get("expiry_date") or row.get("expiryTimestamp") or row.get("expiry_ts")
        if not exp:
            continue
        exp_ymd = None
        if isinstance(exp, (int,float)):
            exp_ymd = ms_to_ymd(exp)
        else:
            try:
                exp_ymd = str(exp).split("T")[0]
            except:
                exp_ymd = None
        if exp_ymd != expiry_yyyy_mm_dd:
            continue
        name = str(row.get("name","")).lower()
        ts = str(row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol") or "").lower()
        if term in name or term in ts:
            # ensure CE or PE
            if (" ce " in ts) or (" pe " in ts) or ts.strip().endswith("ce") or ts.strip().endswith("pe") or "call" in ts or "put" in ts:
                out.append(row)
    def _strike_key(r):
        try:
            return float(r.get("strike_price") or r.get("strike") or 0)
        except:
            return 0
    return sorted(out, key=_strike_key)

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ------------- Market quote: batched, retry, throttle -------------
def fetch_quotes_for_instrument_keys(instrument_keys):
    out = {}
    if not instrument_keys:
        return out
    batch_size = 100
    retry_delay = 1.0
    max_retries = 2
    for batch in chunk_list(instrument_keys, batch_size):
        params = "&".join(f"instrument_key={quote_plus(str(k))}" for k in batch)
        url = UPSTOX_MARKET_QUOTE_URL + "?" + params
        success = False
        tries = 0
        while not success and tries <= max_retries:
            tries += 1
            try:
                logging.info("Market-quote fetch URL (batch size %d): %.200s", len(batch), url)
                r = requests.get(url, headers=HEADERS, timeout=25)
                r.raise_for_status()
                j = r.json()
                data = j.get("data") if isinstance(j, dict) and "data" in j else j
                if isinstance(data, list):
                    for item in data:
                        key = item.get("instrument_key") or item.get("instrumentKey") or item.get("instrument_token")
                        if key:
                            out[key] = item
                elif isinstance(data, dict):
                    key = data.get("instrument_key") or data.get("instrumentKey")
                    if key:
                        out[key] = data
                success = True
            except Exception as e:
                logging.warning("Market-quote batch fetch failed (attempt %d): %s", tries, e)
                time.sleep(retry_delay * tries)
        time.sleep(0.08)
    return out

# ------------- Build option chain from rows + quotes -------------
def build_option_chain_from_instruments(instrument_rows, quotes_map):
    strikes = {}
    for row in instrument_rows:
        ik = row.get("instrument_key") or row.get("instrumentKey") or row.get("instrumentToken") or row.get("instrument_token")
        ts = row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol") or ""
        strike = row.get("strike_price") or row.get("strike") or row.get("strikePrice")
        try:
            if strike is None:
                # try parsing last numeric in symbol
                parts = str(ts).split()
                found = None
                for p in parts:
                    if p.replace('.','',1).isdigit():
                        found = p
                if found:
                    strike = float(found)
            strike = float(strike)
        except:
            continue
        q = quotes_map.get(ik) or {}
        ltp = q.get("last_traded_price") or q.get("ltp") or q.get("lastPrice")
        oi = q.get("open_interest") or q.get("oi") or q.get("openInterest")
        iv = q.get("iv") or q.get("implied_volatility") or q.get("IV")
        tsl = str(ts).lower()
        side = "ce" if (" ce" in tsl or tsl.endswith("ce") or "call" in tsl) else ("pe" if (" pe" in tsl or tsl.endswith("pe") or "put" in tsl) else None)
        rec = strikes.setdefault(strike, {"strike": strike, "ce": None, "pe": None})
        info = {"instrument_key": ik, "trading_symbol": ts, "ltp": ltp, "oi": oi, "iv": iv}
        if side == "ce":
            rec["ce"] = info
        elif side == "pe":
            rec["pe"] = info
    return [strikes[k] for k in sorted(strikes.keys())]

def short_text_for_side(side):
    if not side:
        return "NA"
    ltp = side.get("ltp")
    oi = side.get("oi")
    iv = side.get("iv")
    try: l = f"{float(ltp):,.2f}" if ltp is not None else "NA"
    except: l = str(ltp) if ltp is not None else "NA"
    try: o = f"{int(float(oi)):,}" if oi not in (None,"") else "NA"
    except: o = str(oi) if oi not in (None,"") else "NA"
    try: v = f"{float(iv):.2f}" if iv not in (None,"") else "NA"
    except: v = str(iv) if iv not in (None,"") else "NA"
    return f"{l} / {o} / {v}"

def build_summary_text_from_strikes(label, strikes_list, atm_strike=None, window=5):
    lines = []
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    lines.append(f"📊 <b>Option Chain — {html.escape(label)}</b> — {ts}")
    if not strikes_list:
        lines.append("No option chain data available.")
        return "\n".join(lines)
    try: atm = float(atm_strike) if atm_strike is not None else None
    except: atm = None
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

# ------------- Orchestration (optimized selection + batched fetch) -------------
def poll_once_and_send():
    instruments = fetch_and_cache_instruments(force_refresh=False)
    if not instruments:
        logging.warning("No instruments available — skipping this cycle.")
        return

    nifty_rows = resolve_option_instruments_for(instruments, "nifty", OPTION_EXPIRY_NIFTY)
    tcs_rows   = resolve_option_instruments_for(instruments, "tcs", OPTION_EXPIRY_TCS)
    logging.info("Found option rows: Nifty=%d, TCS=%d", len(nifty_rows), len(tcs_rows))

    # estimate ATM using underlying LTP if possible, else median strike
    def estimate_atm(option_rows, instruments, underlying_term):
        # attempt to find underlying instrument in instruments list and fetch its quote
        underlying_ltp = None
        try:
            for r in instruments:
                ks = (r.get("instrument_key") or r.get("instrumentKey") or "")
                ts = str(r.get("tradingsymbol") or r.get("symbol") or "").lower()
                name = str(r.get("name") or "").lower()
                if underlying_term in ks.lower() or underlying_term in ts or underlying_term in name:
                    q = fetch_quotes_for_instrument_keys([ks])
                    if q and q.get(ks):
                        underlying_ltp = q[ks].get("last_traded_price") or q[ks].get("ltp")
                        break
        except Exception:
            underlying_ltp = None
        if underlying_ltp:
            try:
                return float(underlying_ltp)
            except:
                pass
        # fallback median strike
        try:
            mid = option_rows[len(option_rows)//2]
            return float(mid.get("strike_price") or mid.get("strike") or 0)
        except:
            return None

    nifty_atm = estimate_atm(nifty_rows, instruments, "nifty")
    tcs_atm   = estimate_atm(tcs_rows, instruments, "tcs")
    logging.info("Estimated ATMs: Nifty= %s, TCS= %s", nifty_atm, tcs_atm)

    # select window keys (ATM +/- STRIKE_WINDOW). returns list of instrument_keys (CE+PE)
    def select_window_keys(option_rows, atm_val, window=STRIKE_WINDOW):
        strike_map = {}
        for r in option_rows:
            try:
                strike = float(r.get("strike_price") or r.get("strike") or 0)
            except:
                continue
            ik = r.get("instrument_key") or r.get("instrumentToken") or r.get("instrument_token")
            ts = str(r.get("tradingsymbol") or r.get("trading_symbol") or r.get("symbol") or "")
            side = "ce" if (" ce" in ts.lower() or ts.lower().endswith("ce") or "call" in ts.lower()) else ("pe" if (" pe" in ts.lower() or ts.lower().endswith("pe") or "put" in ts.lower()) else None)
            if strike not in strike_map:
                strike_map[strike] = {"strike": strike, "ce": None, "pe": None}
            if side == "ce":
                strike_map[strike]["ce"] = ik
            elif side == "pe":
                strike_map[strike]["pe"] = ik
        strikes_sorted = sorted(strike_map.keys())
        if not strikes_sorted:
            return []
        if atm_val is None:
            center_idx = len(strikes_sorted)//2
        else:
            center_idx = min(range(len(strikes_sorted)), key=lambda i: abs(strikes_sorted[i]-atm_val))
        start = max(0, center_idx - window)
        end = min(len(strikes_sorted)-1, center_idx + window)
        chosen = []
        for s in strikes_sorted[start:end+1]:
            rec = strike_map[s]
            if rec.get("ce"):
                chosen.append(rec["ce"])
            if rec.get("pe"):
                chosen.append(rec["pe"])
        return chosen

    nifty_keys = select_window_keys(nifty_rows, nifty_atm)
    tcs_keys   = select_window_keys(tcs_rows, tcs_atm)
    all_keys = list(dict.fromkeys((nifty_keys or []) + (tcs_keys or [])))  # unique maintain order
    logging.info("Selected keys counts: Nifty=%d, TCS=%d, total=%d", len(nifty_keys), len(tcs_keys), len(all_keys))

    quotes_map = fetch_quotes_for_instrument_keys(all_keys)

    nifty_strikes = build_option_chain_from_instruments([r for r in nifty_rows if (r.get("instrument_key") in nifty_keys)], quotes_map)
    tcs_strikes   = build_option_chain_from_instruments([r for r in tcs_rows if (r.get("instrument_key") in tcs_keys)], quotes_map)

    if nifty_strikes:
        atm_n = None
        if nifty_atm:
            atm_n = min([s["strike"] for s in nifty_strikes], key=lambda x: abs(x - nifty_atm))
        summary = build_summary_text_from_strikes("Nifty 50", nifty_strikes, atm_n, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent Nifty summary (ATM approx %s)", atm_n)
    else:
        logging.info("No Nifty strikes to send.")

    if tcs_strikes:
        atm_t = None
        if tcs_atm:
            atm_t = min([s["strike"] for s in tcs_strikes], key=lambda x: abs(x - tcs_atm))
        summary = build_summary_text_from_strikes("TCS", tcs_strikes, atm_t, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent TCS summary (ATM approx %s)", atm_t)
    else:
        logging.info("No TCS strikes to send.")

# ------------- Main -------------
def main():
    logging.info("Starting Option Chain poller. Poll interval: %ss. STRIKE_WINDOW=%s", POLL_INTERVAL, STRIKE_WINDOW)
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
