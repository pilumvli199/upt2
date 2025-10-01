#!/usr/bin/env python3
"""
Option Chain poller (single file):
- Downloads Upstox instrument master (JSON preferred) from assets.upstox.com, caches it.
- Resolves instrument_key for Nifty / TCS, then fetches option chain using instrument_key + expiry.
- Sends compact summary to Telegram.
- Robust fallbacks & verbose logging for UDAPI100060 debugging.
"""
import os
import time
import logging
import requests
import gzip
import json
import csv
import io
import html
from urllib.parse import quote_plus

# ---------- CONFIG / ENV ----------
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN') or ""
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or ""
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID') or ""

OPTION_SYMBOL_NIFTY = os.getenv('OPTION_SYMBOL_NIFTY') or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv('OPTION_EXPIRY_NIFTY') or "2025-10-07"

OPTION_SYMBOL_TCS = os.getenv('OPTION_SYMBOL_TCS') or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv('OPTION_EXPIRY_TCS') or "2025-10-28"

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 5)

# Instruments download/caching
INSTRUMENTS_CACHE_DIR = os.getenv('INSTRUMENTS_CACHE_DIR') or "./cache"
INSTRUMENTS_TTL_SECONDS = int(os.getenv('INSTRUMENTS_TTL_SECONDS') or 24*3600)

# Candidate instruments URLs (you can override via env)
CANDIDATE_INSTRUMENT_URLS = [
    os.getenv('UPSTOX_INSTRUMENTS_JSON_URL') or "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
    os.getenv('UPSTOX_INSTRUMENTS_JSON_URL2') or "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
    os.getenv('UPSTOX_INSTRUMENTS_CSV_URL') or "https://assets.upstox.com/instruments/NSE_EQ.csv",
]

# Option chain URL (default)
UPSTOX_OPTION_CHAIN_URL = os.getenv('UPSTOX_OPTION_CHAIN_URL') or "https://api.upstox.com/v3/option/chain"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)

HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

os.makedirs(INSTRUMENTS_CACHE_DIR, exist_ok=True)

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

# ---------- Instruments fetch / cache / lookup ----------
def _cache_path():
    return os.path.join(INSTRUMENTS_CACHE_DIR, "instruments_complete.json")

def _is_cache_fresh(path):
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age < INSTRUMENTS_TTL_SECONDS

def _write_cache_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)

def _read_cache_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _try_download_gz_json(url):
    logging.info("Trying instruments URL: %s", url)
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        content = r.content
        # If gzipped JSON, gunzip
        try:
            decompressed = gzip.decompress(content).decode('utf-8')
            # parse JSON
            js = json.loads(decompressed)
            logging.info("Downloaded and parsed gz JSON from %s (items=%d)", url, len(js) if isinstance(js, list) else 1)
            return js
        except (OSError, gzip.BadGzipFile):
            # maybe JSON plain
            try:
                text = content.decode('utf-8')
                js = json.loads(text)
                return js
            except Exception:
                pass
        # If raw CSV
        try:
            text = content.decode('utf-8')
            if "\n" in text and "," in text.splitlines()[0]:
                rows = list(csv.DictReader(io.StringIO(text)))
                logging.info("Downloaded CSV instruments with %d rows from %s", len(rows), url)
                return rows
        except Exception:
            pass
    except Exception as e:
        logging.warning("Failed to download instruments from %s: %s", url, e)
    return None

def fetch_and_cache_instruments(force_refresh=False):
    cache_path = _cache_path()
    if not force_refresh and _is_cache_fresh(cache_path):
        try:
            cached = _read_cache_json(cache_path)
            logging.info("Using cached instruments (%d items)", len(cached) if isinstance(cached, list) else 1)
            return cached
        except Exception:
            logging.warning("Failed to read cached instruments, will re-download.")
    # try candidate urls
    last_err = None
    for url in CANDIDATE_INSTRUMENT_URLS:
        if not url:
            continue
        js = _try_download_gz_json(url)
        if js:
            # normalize: if dict with keys, convert to list
            if isinstance(js, dict):
                # some JSON files might be object with multiple arrays; try to find list
                for v in js.values():
                    if isinstance(v, list):
                        js = v
                        break
            # write cache
            try:
                _write_cache_json(cache_path, js)
                logging.info("Cached instruments to %s", cache_path)
            except Exception as e:
                logging.warning("Failed to write cache: %s", e)
            return js
    # fallback: try reading existing cache even if stale
    if os.path.exists(cache_path):
        try:
            cached = _read_cache_json(cache_path)
            logging.warning("Using stale cached instruments (%d items)", len(cached) if isinstance(cached, list) else 1)
            return cached
        except Exception as e:
            last_err = e
    logging.error("Unable to fetch instruments and no cache available. Last err: %s", last_err)
    return []

def find_instrument_key(instruments, match_term):
    """Match by symbol or name. Returns first found instrument_key / instrument_token / token."""
    if not instruments:
        return None
    term = str(match_term).strip().lower()
    candidate_keys = ("instrument_key","instrumentToken","instrument_token","token","id","instrument_id","instrumentId")
    symbol_fields = ("tradingsymbol","trading_symbol","symbol","trade_symbol","display_symbol")
    name_fields = ("name","instrumentName","display_name")
    for row in instruments:
        # row may be dict with varied keys
        row_lower = {}
        for k,v in row.items():
            try:
                row_lower[k] = str(v).lower()
            except Exception:
                row_lower[k] = ""
        # check symbol fields
        for sf in symbol_fields:
            if sf in row_lower and term in row_lower[sf]:
                # get key
                for k in candidate_keys:
                    if k in row and row[k]:
                        return row[k]
        # check name fields
        for nf in name_fields:
            if nf in row_lower and term in row_lower[nf]:
                for k in candidate_keys:
                    if k in row and row[k]:
                        return row[k]
    # fallback: partial match in any field
    for row in instruments:
        for v in row.values():
            try:
                if term in str(v).lower():
                    for k in candidate_keys:
                        if k in row and row[k]:
                            return row[k]
            except Exception:
                continue
    return None

# ---------- Option-chain fetch using instrument_key ----------
def fetch_option_chain_by_instrument_key(instrument_key, expiry_date):
    if not instrument_key:
        logging.warning("No instrument_key provided.")
        return None
    params = f"instrument_key={quote_plus(str(instrument_key))}"
    if expiry_date:
        params += "&expiry_date=" + quote_plus(expiry_date)
    url = UPSTOX_OPTION_CHAIN_URL + "?" + params
    logging.info("Fetching option chain URL: %s", url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as he:
        body = he.response.text if he.response is not None else ""
        logging.warning("Option chain fetch HTTPError %s for instrument_key %s %s: %s", he.response.status_code if he.response is not None else "?", instrument_key, expiry_date, body[:1000])
        return None
    except Exception as e:
        logging.warning("Option chain fetch failed for %s: %s", instrument_key, e)
        return None

# ---------- Reuse earlier helper functions: extract strikes, find atm, build summary ----------
def extract_strikes_from_chain(chain_json):
    if not chain_json:
        return []
    data = None
    if isinstance(chain_json, dict):
        if 'data' in chain_json:
            data = chain_json['data']
        elif 'results' in chain_json:
            data = chain_json['results']
        else:
            for v in chain_json.values():
                if isinstance(v, list):
                    data = v
                    break
    elif isinstance(chain_json, list):
        data = chain_json
    if not isinstance(data, list):
        return []
    strikes = []
    for item in data:
        try:
            strike_price = item.get('strike_price') or item.get('strike') or item.get('strikePrice')
            ce = item.get('ce') or item.get('CE') or item.get('call') or None
            pe = item.get('pe') or item.get('PE') or item.get('put') or None
            strikes.append({'strike': strike_price, 'ce': ce, 'pe': pe})
        except Exception:
            continue
    strikes_sorted = sorted([s for s in strikes if s.get('strike') is not None], key=lambda x: float(x['strike']))
    return strikes_sorted

def find_atm_strike(strikes):
    if not strikes:
        return None
    try:
        for s in strikes:
            ce = s.get('ce')
            pe = s.get('pe')
            cand = None
            if ce and isinstance(ce, dict):
                cand = ce.get('underlying') or ce.get('underlying_price') or ce.get('underlyingPrice')
            if cand is None and pe and isinstance(pe, dict):
                cand = pe.get('underlying') or pe.get('underlying_price') or pe.get('underlyingPrice')
            if cand:
                try:
                    up = float(cand)
                    nearest = min(strikes, key=lambda x: abs(float(x['strike']) - up))
                    return nearest['strike']
                except Exception:
                    pass
        mid = strikes[len(strikes)//2]['strike']
        return mid
    except Exception:
        return strikes[0]['strike']

def build_summary_text(symbol_label, strikes, atm_strike, window=5):
    lines = []
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    lines.append(f"📊 <b>Option Chain — {html.escape(symbol_label)}</b> — {ts}")
    if not strikes:
        lines.append("No option chain data available.")
        return "\n".join(lines)
    try:
        atm = float(atm_strike) if atm_strike is not None else None
    except Exception:
        atm = None
    idx = None
    for i,s in enumerate(strikes):
        try:
            if atm is not None and float(s['strike']) == atm:
                idx = i
                break
        except Exception:
            continue
    if idx is None:
        idx = min(range(len(strikes)), key=lambda i: abs(float(strikes[i]['strike']) - (atm or float(strikes[len(strikes)//2]['strike']))))
    start = max(0, idx - window)
    end = min(len(strikes)-1, idx + window)
    lines.append("<code>Strike    CE(LTP / OI / IV)       |      PE(LTP / OI / IV)</code>")
    for i in range(start, end+1):
        s = strikes[i]
        strike = s.get('strike')
        ce = s.get('ce') or {}
        pe = s.get('pe') or {}
        def short_info(side):
            if not side:
                return "NA"
            ltp = side.get('ltp') or side.get('last_traded_price') or side.get('lastPrice') or side.get('lastTradedPrice')
            oi = side.get('open_interest') or side.get('oi') or side.get('openInterest')
            iv = side.get('iv') or side.get('implied_volatility') or side.get('IV')
            try:
                l = f"{float(ltp):,.2f}" if ltp is not None else "NA"
            except Exception:
                l = str(ltp) if ltp is not None else "NA"
            try:
                o = f"{int(oi):,}" if oi not in (None, "") and str(oi).isdigit() else (str(oi) if oi not in (None,"") else "NA")
            except Exception:
                o = str(oi) if oi not in (None,"") else "NA"
            try:
                v = f"{float(iv):.2f}" if iv not in (None,"") else "NA"
            except Exception:
                v = str(iv) if iv not in (None,"") else "NA"
            return f"{l} / {o} / {v}"
        ce_info = short_info(ce)
        pe_info = short_info(pe)
        atm_mark = " ⭑" if (atm is not None and float(strike) == atm) else ""
        lines.append(f"<code>{str(int(float(strike))).rjust(6)}{atm_mark}   {ce_info.ljust(20)} | {pe_info}</code>")
    return "\n".join(lines)

# ---------- Polling logic ----------
def poll_once_and_send():
    # load instruments (cached)
    instruments = fetch_and_cache_instruments()
    # resolve instrument keys
    nifty_key = find_instrument_key(instruments, "nifty 50") or find_instrument_key(instruments, "nifty")
    tcs_key = find_instrument_key(instruments, "tcs") or find_instrument_key(instruments, "tata consultancy services")
    logging.info("Resolved instrument keys: Nifty=%s, TCS=%s", nifty_key, tcs_key)

    # fetch option chains by instrument key
    chain_nifty = fetch_option_chain_by_instrument_key(nifty_key, OPTION_EXPIRY_NIFTY)
    strikes_nifty = extract_strikes_from_chain(chain_nifty)
    atm_nifty = find_atm_strike(strikes_nifty) if strikes_nifty else None

    chain_tcs = fetch_option_chain_by_instrument_key(tcs_key, OPTION_EXPIRY_TCS)
    strikes_tcs = extract_strikes_from_chain(chain_tcs)
    atm_tcs = find_atm_strike(strikes_tcs) if strikes_tcs else None

    if strikes_nifty:
        text = build_summary_text("Nifty 50", strikes_nifty, atm_nifty, window=STRIKE_WINDOW)
        ok = send_telegram(text)
        logging.info("Sent Nifty option chain summary (ATM %s). Telegram ok=%s", atm_nifty, ok)
    else:
        logging.info("No Nifty option chain to send.")

    if strikes_tcs:
        text = build_summary_text("TCS", strikes_tcs, atm_tcs, window=STRIKE_WINDOW)
        ok = send_telegram(text)
        logging.info("Sent TCS option chain summary (ATM %s). Telegram ok=%s", atm_tcs, ok)
    else:
        logging.info("No TCS option chain to send.")

def main():
    logging.info("Starting Option Chain poller. Interval: %ss. Nifty symbol=%s expiry=%s | TCS symbol=%s expiry=%s",
                 POLL_INTERVAL, OPTION_SYMBOL_NIFTY, OPTION_EXPIRY_NIFTY, OPTION_SYMBOL_TCS, OPTION_EXPIRY_TCS)
    # quick health check: try to download instruments once at start
    instruments = fetch_and_cache_instruments(force_refresh=False)
    logging.info("Initial instruments loaded: %d items", len(instruments) if instruments else 0)
    while True:
        try:
            poll_once_and_send()
        except Exception as e:
            logging.exception("Unhandled error during poll: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
