#!/usr/bin/env python3
"""
Option Chain poller for TCS (equity) and Nifty 50 (index).
- Polls Upstox option/chain endpoint for two configured symbols+expiries
- Sends a compact summary (ATM +/- STRIKE_WINDOW strikes) to Telegram every POLL_INTERVAL seconds
- Configure via .env (see .env.example)
"""
import os
import time
import logging
import requests
import html
from urllib.parse import quote_plus

# Logging
logging.basicConfig(level=logging.INFO, format='%(Y-%m-%d %H:%M:%S')  # fallback format replaced below
)
# fix logging format properly
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Config from env
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Symbols + expiries (must be provided)
OPTION_SYMBOL_NIFTY = os.getenv('OPTION_SYMBOL_NIFTY') or "NSE_INDEX|Nifty 50"
OPTION_EXPIRY_NIFTY = os.getenv('OPTION_EXPIRY_NIFTY') or ""  # YYYY-MM-DD or empty
OPTION_SYMBOL_TCS = os.getenv('OPTION_SYMBOL_TCS') or "NSE_EQ|INE467B01029"
OPTION_EXPIRY_TCS = os.getenv('OPTION_EXPIRY_TCS') or ""     # YYYY-MM-DD or empty

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL') or 60)  # seconds
STRIKE_WINDOW = int(os.getenv('STRIKE_WINDOW') or 5)   # ATM +/- window
TOP_N_STRIKES = int(os.getenv('TOP_N_STRIKES') or 10)  # fallback limit to show

UPSTOX_OPTION_CHAIN_URL = os.getenv('UPSTOX_OPTION_CHAIN_URL') or "https://api.upstox.com/v3/option/chain"
# Optional: an instruments/lookup endpoint if you have one for resolving instrument tokens
UPSTOX_INSTRUMENTS_URL = os.getenv('UPSTOX_INSTRUMENTS_URL') or ""

# Basic validation
if not UPSTOX_ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Set UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID in env.")
    raise SystemExit(1)

if not OPTION_EXPIRY_NIFTY or not OPTION_EXPIRY_TCS:
    logging.warning("OPTION_EXPIRY_NIFTY or OPTION_EXPIRY_TCS not set. You must set expiry dates (YYYY-MM-DD) for accurate results. Script will still try fallback without expiry.")

HEADERS = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}

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

def health_check_token():
    """Optional lightweight health-check: try a simple GET to the option endpoint (no params) or instruments URL."""
    logging.info("Performing token health check...")
    # Prefer instruments URL if provided
    test_url = UPSTOX_INSTRUMENTS_URL or (UPSTOX_OPTION_CHAIN_URL + "?symbol=" + quote_plus(OPTION_SYMBOL_NIFTY))
    try:
        r = requests.get(test_url, headers=HEADERS, timeout=12)
        logging.info("Health check status: %s", r.status_code)
        # don't raise; just return status and body snippet
        return r.status_code, r.text[:800]
    except Exception as e:
        logging.warning("Health check failed: %s", e)
        return None, str(e)

def fetch_option_chain(symbol, expiry_date):
    """
    Fetch option chain. Verbose: logs URL, status, body. If 404 with expiry, tries without expiry as a fallback.
    Returns parsed JSON or None.
    """
    if not symbol:
        logging.warning("No symbol provided — skipping fetch.")
        return None

    base = UPSTOX_OPTION_CHAIN_URL
    params = f"symbol={quote_plus(symbol)}"
    if expiry_date:
        params_exp = params + "&expiry_date=" + quote_plus(expiry_date)
    else:
        params_exp = params

    url_with_exp = base + "?" + params_exp
    logging.info("Fetching option chain URL: %s", url_with_exp)

    try:
        r = requests.get(url_with_exp, headers=HEADERS, timeout=25)
        try:
            r.raise_for_status()
            logging.info("Option chain fetch OK (%s) for %s %s", r.status_code, symbol, expiry_date)
            return r.json()
        except requests.exceptions.HTTPError:
            code = r.status_code
            body = r.text
            logging.warning("Option chain fetch HTTPError %s for %s %s: %s", code, symbol, expiry_date, body[:1000])
            # If 404 and we sent an expiry, try again without expiry to see what's available
            if code == 404 and expiry_date:
                logging.info("Received 404 with expiry; trying without expiry to inspect available expiries.")
                url_no_exp = base + "?" + "symbol=" + quote_plus(symbol)
                logging.info("Fetching option chain URL (no expiry): %s", url_no_exp)
                try:
                    r2 = requests.get(url_no_exp, headers=HEADERS, timeout=25)
                    r2.raise_for_status()
                    logging.info("Fallback (no expiry) OK (%s) for %s", r2.status_code, symbol)
                    return r2.json()
                except Exception as e2:
                    body2 = getattr(e2, 'response', None).text if getattr(e2, 'response', None) is not None else str(e2)
                    logging.warning("Fallback without expiry failed: %s | body: %s", e2, str(body2)[:1000])
            return None
    except Exception as e:
        logging.warning("Option chain fetch failed for %s %s: %s", symbol, expiry_date, e)
        return None

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
        # Try to read underlying price from CE or PE
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
                    # nearest strike
                    nearest = min(strikes, key=lambda x: abs(float(x['strike']) - up))
                    return nearest['strike']
                except Exception:
                    pass
        # fallback: median or nearest by position
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
            if float(s['strike']) == float(atm_strike):
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

def poll_once_and_send():
    chain_nifty = fetch_option_chain(OPTION_SYMBOL_NIFTY, OPTION_EXPIRY_NIFTY)
    strikes_nifty = extract_strikes_from_chain(chain_nifty)
    atm_nifty = find_atm_strike(strikes_nifty) if strikes_nifty else None

    chain_tcs = fetch_option_chain(OPTION_SYMBOL_TCS, OPTION_EXPIRY_TCS)
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
    # health-check at start
    status, body_snip = health_check_token()
    logging.info("Initial health-check returned status %s, body_snip: %s", status, body_snip[:500] if body_snip else "")
    while True:
        try:
            poll_once_and_send()
        except Exception as e:
            logging.exception("Unhandled error during poll: %s", e)
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
