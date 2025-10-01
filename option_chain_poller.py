#!/usr/bin/env python3
"""
Option Chain poller — tuned for 429 avoidance with rotation of keys per poll.
"""
import os, time, logging, requests, json, gzip, io, html, datetime, math
from urllib.parse import quote_plus

# -------- CONFIG from env ----------
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN','').strip()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN','').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID','').strip()

OPTION_SYMBOL_NIFTY = os.getenv('OPTION_SYMBOL_NIFTY') or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv('OPTION_EXPIRY_NIFTY') or "2025-10-07"

OPTION_SYMBOL_TCS = os.getenv('OPTION_SYMBOL_TCS') or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv('OPTION_EXPIRY_TCS') or "2025-10-28"

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 10)
INSTRUMENTS_TTL_SECONDS = int(os.getenv('INSTRUMENTS_TTL_SECONDS') or 24*3600)

UPSTOX_INSTRUMENTS_JSON_URL = os.getenv('UPSTOX_INSTRUMENTS_JSON_URL') or "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
UPSTOX_MARKET_QUOTE_URL = os.getenv('UPSTOX_MARKET_QUOTE_URL') or "https://api.upstox.com/v2/market-quote/quotes"

# new tuning envs
MAX_KEYS_PER_POLL = int(os.getenv('MAX_KEYS_PER_POLL') or 30)
BATCH_SIZE = int(os.getenv('BATCH_SIZE') or 20)
BASE_DELAY = float(os.getenv('BASE_DELAY') or 2.0)
MAX_RETRIES = int(os.getenv('MAX_RETRIES') or 4)

# -------- logging & headers ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)
HEADERS = {"Accept":"application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

# -------- cache ----------
CACHE_DIR = "./cache"
os.makedirs(CACHE_DIR, exist_ok=True)
INSTR_CACHE_PATH = os.path.join(CACHE_DIR, "instruments_complete.json")

# -------- helpers ----------
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
        try:
            dec = gzip.decompress(data).decode('utf-8')
            js = json.loads(dec)
            return js
        except Exception:
            pass
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
        return datetime.datetime.fromtimestamp(ms/1000.0, datetime.timezone.utc).strftime("%Y-%m-%d")
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

# -------- robust fetch with backoff & respect Retry-After ----------
def fetch_quotes_for_instrument_keys(instrument_keys):
    out = {}
    if not instrument_keys:
        return out

    # use configured batch size
    for batch in chunk_list(instrument_keys, BATCH_SIZE):
        params = "&".join(f"instrument_key={quote_plus(str(k))}" for k in batch)
        url = UPSTOX_MARKET_QUOTE_URL + "?" + params
        attempt = 0
        while attempt <= MAX_RETRIES:
            attempt += 1
            try:
                logging.info("Market-quote fetch (batch size %d) attempt %d: %.200s", len(batch), attempt, url)
                r = requests.get(url, headers=HEADERS, timeout=30)
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    try:
                        wait = float(ra) if ra is not None else (BASE_DELAY * (2 ** (attempt-1)))
                    except:
                        wait = BASE_DELAY * (2 ** (attempt-1))
                    logging.warning("Received 429. Waiting %.1fs before retry (Retry-After=%s).", wait, ra)
                    time.sleep(wait)
                    continue
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
                # polite pause
                time.sleep(0.15)
                break
            except requests.exceptions.RequestException as e:
                wait = BASE_DELAY * (2 ** (attempt-1))
                logging.warning("Market-quote batch fetch failed (attempt %d): %s. Backing off %.1fs", attempt, e, wait)
                time.sleep(wait)
        else:
            logging.error("Batch failed after %d attempts; skipping these keys.", MAX_RETRIES)
    return out

# -------- build chain ----------
def build_option_chain_from_instruments(instrument_rows, quotes_map):
    strikes = {}
    for row in instrument_rows:
        ik = row.get("instrument_key") or row.get("instrumentKey") or row.get("instrumentToken") or row.get("instrument_token")
        ts = row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol") or ""
        strike = row.get("strike_price") or row.get("strike") or row.get("strikePrice")
        try:
            if strike is None:
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

# -------- select window keys (unchanged) ----------
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

# -------- orchestration with per-poll rotation ----------
def poll_once_and_send():
    instruments = fetch_and_cache_instruments(force_refresh=False)
    if not instruments:
        logging.warning("No instruments available — skipping this cycle.")
        return

    nifty_rows = resolve_option_instruments_for(instruments, "nifty", OPTION_EXPIRY_NIFTY)
    tcs_rows   = resolve_option_instruments_for(instruments, "tcs", OPTION_EXPIRY_TCS)
    logging.info("Found option rows: Nifty=%d, TCS=%d", len(nifty_rows), len(tcs_rows))

    # find underlying keys
    def find_underlying_key(instruments, term):
        term = term.lower()
        for r in instruments:
            ks = (r.get("instrument_key") or r.get("instrumentKey") or "")
            ts = str(r.get("tradingsymbol") or r.get("symbol") or "").lower()
            name = str(r.get("name") or "").lower()
            if term in ks.lower() or term in ts or term in name:
                return ks
        return None

    nifty_under_key = find_underlying_key(instruments, "nifty")
    tcs_under_key   = find_underlying_key(instruments, "tcs")

    # initial median-based keys (small)
    nifty_keys_initial = select_window_keys(nifty_rows, None)
    tcs_keys_initial   = select_window_keys(tcs_rows, None)

    # compose candidate keys (include underlying)
    candidate_keys = []
    if nifty_under_key:
        candidate_keys.append(nifty_under_key)
    if tcs_under_key and tcs_under_key != nifty_under_key:
        candidate_keys.append(tcs_under_key)
    candidate_keys.extend(k for k in (nifty_keys_initial + tcs_keys_initial) if k)

    # dedupe
    seen = set(); cand = []
    for k in candidate_keys:
        if k and k not in seen:
            seen.add(k); cand.append(k)

    # rotate / slice so we don't fetch all keys every poll
    total = len(cand)
    if total == 0:
        logging.info("No candidate keys.")
        quotes_map = {}
    else:
        # slice size = MAX_KEYS_PER_POLL; use time-based offset to rotate
        slice_size = min(MAX_KEYS_PER_POLL, total)
        polls_since_epoch = int(time.time() // POLL_INTERVAL)
        pages = math.ceil(total / slice_size)
        offset = polls_since_epoch % pages
        start = offset * slice_size
        end = min(start + slice_size, total)
        keys_this_poll = cand[start:end]
        logging.info("Rotation slicing: total=%d pages=%d offset=%d -> fetching %d keys (idx %d..%d)", total, pages, offset, len(keys_this_poll), start, end-1)
        quotes_map = fetch_quotes_for_instrument_keys(keys_this_poll)

    # attempt to estimate ATM from underlying if present in quotes_map
    nifty_atm = None
    tcs_atm = None
    if nifty_under_key and quotes_map.get(nifty_under_key):
        try:
            nifty_atm = float(quotes_map[nifty_under_key].get("last_traded_price") or quotes_map[nifty_under_key].get("ltp"))
        except: pass
    if tcs_under_key and quotes_map.get(tcs_under_key):
        try:
            tcs_atm = float(quotes_map[tcs_under_key].get("last_traded_price") or quotes_map[tcs_under_key].get("ltp"))
        except: pass

    logging.info("Estimated ATMs: Nifty=%s, TCS=%s", nifty_atm, tcs_atm)

    # now select final keys around ATM (refined); but only request additional if necessary and within limits
    nifty_keys = select_window_keys(nifty_rows, nifty_atm)
    tcs_keys   = select_window_keys(tcs_rows, tcs_atm)
    # ensure we only use keys we already fetched (to avoid new big burst)
    nifty_keys = [k for k in nifty_keys if k in quotes_map]
    tcs_keys   = [k for k in tcs_keys if k in quotes_map]

    logging.info("Using keys fetched for summary: Nifty=%d, TCS=%d", len(nifty_keys), len(tcs_keys))

    # build strikes and send
    nifty_strikes = build_option_chain_from_instruments([r for r in nifty_rows if (r.get("instrument_key") in nifty_keys)], quotes_map)
    tcs_strikes   = build_option_chain_from_instruments([r for r in tcs_rows if (r.get("instrument_key") in tcs_keys)], quotes_map)

    if nifty_strikes:
        atm_n = None
        if nifty_atm:
            try: atm_n = min([s["strike"] for s in nifty_strikes], key=lambda x: abs(x - nifty_atm))
            except: atm_n = None
        summary = build_summary_text_from_strikes("Nifty 50", nifty_strikes, atm_n, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent Nifty summary (ATM approx %s)", atm_n)
    else:
        logging.info("No Nifty strikes to send (possibly due to 429s).")

    if tcs_strikes:
        atm_t = None
        if tcs_atm:
            try: atm_t = min([s["strike"] for s in tcs_strikes], key=lambda x: abs(x - tcs_atm))
            except: atm_t = None
        summary = build_summary_text_from_strikes("TCS", tcs_strikes, atm_t, window=STRIKE_WINDOW)
        send_telegram(summary)
        logging.info("Sent TCS summary (ATM approx %s)", atm_t)
    else:
        logging.info("No TCS strikes to send (possibly due to 429s).")

# ----- main -----
def main():
    logging.info("Starting Option Chain poller. Poll interval: %ss. STRIKE_WINDOW=%s MAX_KEYS_PER_POLL=%s BATCH_SIZE=%s", POLL_INTERVAL, STRIKE_WINDOW, MAX_KEYS_PER_POLL, BATCH_SIZE)
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
