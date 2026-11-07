"""
CE + EMA + QQE MOD signal dashboard for NSE stocks.
Python port of the TradingView indicator "Chandelier Exit + EMA + QQE MOD Signals".

Setup:   pip install fastapi uvicorn pandas numpy yfinance
Run:     python signal_dashboard.py
Open:    http://127.0.0.1:8000
Online:  see Dockerfile; set DASH_PASSWORD on a server.

Optional env vars:  INTERVAL=5m|15m|1d (default timeframe)   REFRESH_SECONDS=300   PORT=8000   HOST=0.0.0.0
Optional file:      symbols.txt  (one NSE symbol per line, e.g. RELIANCE) overrides the default list.

This dashboard only reads market data and shows signals. It never places orders.
"""
import base64
import csv
import io
import json
import os
import re
import secrets
import threading
import time
from datetime import datetime, time as dtime

import numpy as np
import pandas as pd
import requests
import uvicorn
from fastapi import Body, FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

# ── Settings: keep these equal to the indicator's inputs ──
INTERVALS = ["5m", "15m", "1d"]   # timeframes offered on the dashboard
DEFAULT_INTERVAL = os.getenv("INTERVAL", "15m")
if DEFAULT_INTERVAL not in INTERVALS:
    DEFAULT_INTERVAL = "15m"
ATR_LEN, ATR_MULT = 22, 3.0           # Chandelier Exit (close-only extremes)
EMA_LEN = 20                          # trend filter
QQE_P = dict(rsi_len=6, smooth=5, factor=3.0)    # QQE primary
QQE_S = dict(rsi_len=6, smooth=5, factor=1.61)   # QQE secondary
QQE_THRESHOLD = 3.0
BB_LEN, BB_MULT = 50, 0.35
QQE_LOOKBACK = 0                      # 0 = QQE must be green/red on the CE flip candle
R_MULTIPLES = (1.0, 2.0, 3.0, 4.0)    # T1..T4
MIN_RISK_ATR, MAX_RISK_ATR = 1.0, 3.0

FRESH_BARS = 3            # a signal shows as BUY/SELL for this many candles, then as "Open" while its trade is live
CONFIRMED_ONLY = True     # ignore the candle that is still forming
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "300"))
PORT = int(os.getenv("PORT", "8000"))
HOST = os.getenv("HOST", "127.0.0.1")
OPTION_TTL = int(os.getenv("OPTION_TTL", "120"))                 # seconds an option chain stays cached
STRIKES_EACH_SIDE = int(os.getenv("STRIKES_EACH_SIDE", "8"))       # strikes shown above and below the money
PASSWORD = os.getenv("DASH_PASSWORD")  # set this when the dashboard is on the internet

PERIODS = {"5m": "30d", "15m": "60d", "1d": "2y"}
STEP = {"5m": pd.Timedelta(minutes=5), "15m": pd.Timedelta(minutes=15), "1h": pd.Timedelta(hours=1)}
IST = "Asia/Kolkata"
MIN_BARS = 150

DEFAULT_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", "LT", "ITC", "AXISBANK", "KOTAKBANK",
    "BHARTIARTL", "HINDUNILVR", "BAJFINANCE", "MARUTI", "SUNPHARMA", "TATAMOTORS", "TATASTEEL", "NTPC",
    "POWERGRID", "ONGC", "ADANIENT", "ADANIPORTS", "ASIANPAINT", "ULTRACEMCO", "TITAN", "WIPRO",
    "HCLTECH", "TECHM", "M&M", "COALINDIA", "JSWSTEEL", "HINDALCO", "GRASIM", "CIPLA", "DRREDDY",
    "EICHERMOT", "BAJAJ-AUTO", "HEROMOTOCO", "BRITANNIA", "NESTLEIND",
]


def load_symbols():
    if os.path.exists("symbols.txt"):
        with open("symbols.txt") as f:
            syms = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
        if syms:
            return syms
    return DEFAULT_SYMBOLS


SYMBOLS = load_symbols()

# Indices that have futures and options. Yahoo is the price source; contract details come from Angel One's list.
INDEX_YF = {"NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK", "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
            "MIDCPNIFTY": "NIFTY_MID_SELECT.NS", "NIFTYNXT50": "^NSMIDCP", "SENSEX": "^BSESN", "BANKEX": "BSE-BANK.BO"}
BSE_INDICES = {"SENSEX", "BANKEX"}
INDICES = list(INDEX_YF)
UNIVERSE = SYMBOLS + INDICES


def yf_ticker(s):
    return INDEX_YF.get(s, s + ".NS")



# ── Indicator maths (ported from the Pine script) ──
def chandelier(df):
    """Close-only Chandelier Exit. Returns long stop, short stop, direction and plain ATR."""
    c = df["Close"].to_numpy(float)
    h = df["High"].to_numpy(float)
    l = df["Low"].to_numpy(float)
    n = len(c)
    prev_c = np.roll(c, 1)
    prev_c[0] = c[0]
    tr = np.maximum.reduce([h - l, np.abs(h - prev_c), np.abs(l - prev_c)])
    atr = pd.Series(tr).ewm(alpha=1 / ATR_LEN, adjust=False).mean().to_numpy()  # Wilder RMA
    hh = pd.Series(c).rolling(ATR_LEN).max().to_numpy()
    ll = pd.Series(c).rolling(ATR_LEN).min().to_numpy()

    ls = np.full(n, np.nan)
    ss = np.full(n, np.nan)
    d = np.ones(n, dtype=int)
    for i in range(n):
        if np.isnan(hh[i]):
            continue
        lraw = hh[i] - ATR_MULT * atr[i]
        sraw = ll[i] + ATR_MULT * atr[i]
        lp = ls[i - 1] if i > 0 and not np.isnan(ls[i - 1]) else lraw
        sp = ss[i - 1] if i > 0 and not np.isnan(ss[i - 1]) else sraw
        ls[i] = max(lraw, lp) if c[i - 1] > lp else lraw
        ss[i] = min(sraw, sp) if c[i - 1] < sp else sraw
        d[i] = 1 if c[i] > sp else (-1 if c[i] < lp else d[i - 1])
    return ls, ss, d, atr


def qqe(close, rsi_len, smooth, factor):
    """QQE as written in the indicator. Returns (trend line, smoothed RSI)."""
    wilders = rsi_len * 2 - 1
    delta = close.diff()
    up = delta.clip(lower=0)
    dn = (-delta).clip(lower=0)
    rma = lambda s: s.ewm(alpha=1 / rsi_len, adjust=False).mean()
    with np.errstate(divide="ignore", invalid="ignore"):
        rsi = 100 - 100 / (1 + rma(up) / rma(dn))
    sm = rsi.ewm(span=smooth, adjust=False).mean()
    dyn = (sm.shift(1) - sm).abs().ewm(span=wilders, adjust=False).mean() * factor
    sm, dyn = sm.to_numpy(), dyn.to_numpy()

    n = len(sm)
    lb, sb = np.full(n, np.nan), np.full(n, np.nan)
    lbp, sbp = np.full(n, np.nan), np.full(n, np.nan)
    trend = np.full(n, np.nan)
    tdir = 0
    for i in range(n):
        if np.isnan(sm[i]) or np.isnan(dyn[i]):
            continue
        nl, ns = sm[i] - dyn[i], sm[i] + dyn[i]
        lp = lb[i - 1] if i > 0 and not np.isnan(lb[i - 1]) else nl
        sp = sb[i - 1] if i > 0 and not np.isnan(sb[i - 1]) else ns
        lbp[i], sbp[i] = lp, sp
        prev = sm[i - 1] if i > 0 else np.nan
        lb[i] = max(lp, nl) if (prev > lp and sm[i] > lp) else nl
        sb[i] = min(sp, ns) if (prev < sp and sm[i] < sp) else ns

        cross_short = cross_long = False
        if i > 0 and not (np.isnan(lbp[i - 1]) or np.isnan(sbp[i - 1]) or np.isnan(sm[i - 1])):
            # ta.cross(a, b): crossover or crossunder
            cross_short = (sm[i] > sp and sm[i - 1] <= sbp[i - 1]) or (sm[i] < sp and sm[i - 1] >= sbp[i - 1])
            cross_long = (lp > sm[i] and lbp[i - 1] <= sm[i - 1]) or (lp < sm[i] and lbp[i - 1] >= sm[i - 1])
        if cross_short:
            tdir = 1
        elif cross_long:
            tdir = -1
        trend[i] = lb[i] if tdir == 1 else sb[i]
    return trend, sm


def qqe_states(close):
    """Boolean arrays: QQE green (up) and QQE red (down) per candle."""
    trend_p, rsi_p = qqe(close, **QQE_P)
    _, rsi_s = qqe(close, **QQE_S)
    x = pd.Series(trend_p - 50)
    basis = x.rolling(BB_LEN).mean()
    dev = BB_MULT * x.rolling(BB_LEN).std(ddof=0)  # ta.stdev is the population deviation
    upper, lower = (basis + dev).to_numpy(), (basis - dev).to_numpy()
    with np.errstate(invalid="ignore"):
        green = (rsi_s - 50 > QQE_THRESHOLD) & (rsi_p - 50 > upper)
        red = (rsi_s - 50 < -QQE_THRESHOLD) & (rsi_p - 50 < lower)
    return green, red


def within_lookback(flags):
    w = QQE_LOOKBACK + 1
    return pd.Series(flags.astype(float)).rolling(w, min_periods=1).max().to_numpy() > 0


def drop_forming(df, interval):
    """Remove the last candle if it has not closed yet."""
    if df.empty:
        return df
    now = pd.Timestamp.now(tz=IST)
    last = df.index[-1]
    if interval == "1d":
        forming = last.date() == now.date() and now.time() < dtime(15, 30)
    else:
        forming = last + STEP[interval] > now
    return df.iloc[:-1] if forming else df


def fmt_time(ts, interval):
    return ts.strftime("%d %b %H:%M") if interval != "1d" else ts.strftime("%d %b")


def analyse(symbol, df, interval):
    df = df.dropna(subset=["Close", "High", "Low"]).copy()
    if df.empty:
        return None
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    if CONFIRMED_ONLY:
        df = drop_forming(df, interval)
    n = len(df)
    if n < MIN_BARS:
        return None

    close = df["Close"]
    c = close.to_numpy(float)
    hi, lo = df["High"].to_numpy(float), df["Low"].to_numpy(float)
    ls, ss, d, atr = chandelier(df)
    ema = close.ewm(span=EMA_LEN, adjust=False).mean().to_numpy()
    green, red = qqe_states(close)
    green_ok, red_ok = within_lookback(green), within_lookback(red)

    flip_up = np.zeros(n, bool)
    flip_dn = np.zeros(n, bool)
    flip_up[1:] = (d[1:] == 1) & (d[:-1] == -1)
    flip_dn[1:] = (d[1:] == -1) & (d[:-1] == 1)
    buy = flip_up & (c > ema) & green_ok
    sell = flip_dn & (c < ema) & red_ok
    sig_idx = np.nonzero(buy | sell)[0]

    base = {"symbol": symbol, "type": "index" if symbol in INDEX_YF else "stock", "close": round(float(c[-1]), 2), "signal": "–", "entry": None, "sl": None,
            "tp1": None, "tp2": None, "tp3": None, "tp4": None, "hits": 0, "sl_hit": False,
            "status": "No signal yet", "bars_ago": None, "signal_time": "–"}
    if len(sig_idx) == 0:
        return base

    j = int(sig_idx[-1])
    side = 1 if buy[j] else -1
    entry = float(c[j])
    raw_stop = ls[j] if side == 1 else ss[j]
    raw_risk = abs(entry - raw_stop)
    risk = max(MIN_RISK_ATR * atr[j], min(raw_risk, MAX_RISK_ATR * atr[j]))
    sl = entry - side * risk
    tps = [float(entry + side * risk * r) for r in R_MULTIPLES]

    # walk the candles after the signal, same order as the indicator: SL first, then targets
    hits, sl_hit = 0, False
    for k in range(j + 1, n):
        if (lo[k] <= sl) if side == 1 else (hi[k] >= sl):
            sl_hit = True
            break
        reached = sum(1 for t in tps if ((hi[k] >= t) if side == 1 else (lo[k] <= t)))
        hits = max(hits, reached)
        if hits == 4:
            break

    bars_ago = n - 1 - j
    name = "BUY" if side == 1 else "SELL"
    live = not sl_hit and hits < 4
    if bars_ago <= FRESH_BARS:
        signal = name
    elif live:
        signal = "Open " + name
    else:
        signal = "–"

    if sl_hit:
        status = f"T{hits} then SL" if hits else "SL hit"
    elif hits == 4:
        status = "T4 hit"
    elif hits:
        status = f"T{hits} hit"
    else:
        status = "Open"

    base.update(signal=signal, entry=round(entry, 2), sl=round(float(sl), 2),
                tp1=round(tps[0], 2), tp2=round(tps[1], 2), tp3=round(tps[2], 2), tp4=round(tps[3], 2),
                hits=hits, sl_hit=sl_hit, status=status, bars_ago=int(bars_ago), signal_time=fmt_time(df.index[j], interval))
    base["side"] = name
    return base


# ── Data + cache (one cache per timeframe) ──
STATES = {tf: {"rows": [], "updated": None, "errors": [], "refreshing": False, "checked": 0.0} for tf in INTERVALS}
LOCK = threading.Lock()


def fetch_all(interval):
    import yfinance as yf  # imported lazily so the module loads without it

    tickers = [yf_ticker(s) for s in UNIVERSE]
    data = yf.download(tickers, period=PERIODS[interval], interval=interval, group_by="ticker",
                       auto_adjust=False, threads=True, progress=False)
    rows, errors = [], []
    for s in UNIVERSE:
        try:
            row = analyse(s, data[yf_ticker(s)].dropna(how="all"), interval)
            if row:
                rows.append(row)
            else:
                errors.append(f"{s}: not enough data")
        except Exception as e:  # keep going if one symbol fails
            errors.append(f"{s}: {e}")
    return rows, errors


def refresh(interval):
    st = STATES[interval]
    with LOCK:
        if st["refreshing"]:
            return
        st["refreshing"] = True
    try:
        rows, errors = fetch_all(interval)
        with LOCK:
            st.update(rows=rows, errors=errors, updated=datetime.now().strftime("%d %b %H:%M:%S"))
    except Exception as e:
        with LOCK:
            st["errors"] = [f"Data fetch failed: {e}"]
    finally:
        with LOCK:
            st["refreshing"] = False
            st["checked"] = time.time()


def ensure_fresh(interval):
    """Rescan a timeframe in the background when its data is older than REFRESH_SECONDS."""
    st = STATES[interval]
    with LOCK:
        stale = not st["refreshing"] and time.time() - st["checked"] > REFRESH_SECONDS
    if stale:
        threading.Thread(target=refresh, args=(interval,), daemon=True).start()


# ── Option chain (NSE public website, unofficial) ──
NSE_BASE = "https://www.nseindia.com"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": NSE_BASE + "/option-chain",
}
EXPIRY_FMT = "%d-%b-%Y"
BLOCKED_MSG = ("NSE did not return data. NSE often blocks servers in data centres and sometimes rejects requests "
               "for a few minutes. Try again shortly, or run the dashboard from your own PC.")


class NSEClient:
    """Small polite client: one session, cookies from the option-chain page, one request at a time."""

    def __init__(self):
        self.session = None
        self.lock = threading.Lock()
        self.last = 0.0

    def _prime(self):
        self.session = requests.Session()
        self.session.headers.update(NSE_HEADERS)
        self.session.get(NSE_BASE + "/option-chain", timeout=10)

    def get_json(self, path, params=None):
        with self.lock:
            for _ in range(2):  # second try starts a fresh session
                wait = 0.5 - (time.time() - self.last)
                if wait > 0:
                    time.sleep(wait)
                try:
                    if self.session is None:
                        self._prime()
                    r = self.session.get(NSE_BASE + path, params=params, timeout=10)
                    self.last = time.time()
                    if r.status_code == 200 and r.text.strip():
                        return r.json()
                except Exception:
                    pass
                self.session = None
            return None


nse = NSEClient()
CHAIN_CACHE = {}


def _num(x):
    try:
        v = float(x)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def _leg(d):
    d = d or {}
    return {"oi": _num(d.get("openInterest")), "doi": _num(d.get("changeinOpenInterest")),
            "vol": _num(d.get("totalTradedVolume")), "iv": _num(d.get("impliedVolatility")),
            "ltp": _num(d.get("lastPrice")), "chg": _num(d.get("pChange"))}


def _sorted_expiries(dates):
    out = []
    for d in set(dates or []):
        try:
            out.append((datetime.strptime(d, EXPIRY_FMT).date(), d))
        except ValueError:
            pass
    return [d for _, d in sorted(out)]


def parse_chain(raw, expiry):
    """Turn NSE's option chain JSON into strikes around the money plus a few summary numbers."""
    rec = raw.get("records") or raw
    underlying = _num(rec.get("underlyingValue"))
    rows = []
    for e in rec.get("data") or []:
        exp = e.get("expiryDate") or e.get("expiryDates")
        if expiry and exp and exp != expiry:
            continue
        k = _num(e.get("strikePrice"))
        if k is None:
            continue
        ce, pe = e.get("CE"), e.get("PE")
        if underlying is None:
            underlying = _num((ce or pe or {}).get("underlyingValue"))
        rows.append({"strike": k, "ce": _leg(ce), "pe": _leg(pe)})
    if not rows or underlying is None:
        return None
    rows.sort(key=lambda r: r["strike"])
    ce_oi = sum(r["ce"]["oi"] or 0 for r in rows)
    pe_oi = sum(r["pe"]["oi"] or 0 for r in rows)
    max_ce = max(rows, key=lambda r: r["ce"]["oi"] or 0)["strike"]
    max_pe = max(rows, key=lambda r: r["pe"]["oi"] or 0)["strike"]
    i = min(range(len(rows)), key=lambda n: abs(rows[n]["strike"] - underlying))
    return {"underlying": underlying, "timestamp": rec.get("timestamp"), "atm": rows[i]["strike"],
            "rows": rows[max(0, i - STRIKES_EACH_SIDE): i + STRIKES_EACH_SIDE + 1],
            "pcr": round(pe_oi / ce_oi, 2) if ce_oi > 0 else None, "max_ce": max_ce, "max_pe": max_pe}


def build_chain(symbol, expiry=None):
    key = (symbol, expiry or "")
    hit = CHAIN_CACHE.get(key)
    if hit and time.time() - hit["t"] < OPTION_TTL:
        return hit["v"]

    today = datetime.now().date()
    info = nse.get_json("/api/option-chain-contract-info", {"symbol": symbol})
    expiries = _sorted_expiries((info or {}).get("expiryDates"))
    upcoming = [d for d in expiries if datetime.strptime(d, EXPIRY_FMT).date() >= today]
    if expiry not in upcoming:
        expiry = upcoming[0] if upcoming else None

    parsed = None
    if expiry:
        raw = nse.get_json("/api/option-chain-v3", {"type": "Indices" if symbol in INDEX_YF else "Equity", "symbol": symbol, "expiry": expiry})
        parsed = parse_chain(raw, expiry) if raw else None
    if not parsed:  # older endpoint as a fallback
        raw = nse.get_json("/api/option-chain-indices" if symbol in INDEX_YF else "/api/option-chain-equities", {"symbol": symbol})
        if raw:
            exps = _sorted_expiries((raw.get("records") or {}).get("expiryDates"))
            expiries = exps or expiries
            if expiry not in expiries:
                expiry = expiries[0] if expiries else None
            parsed = parse_chain(raw, expiry)

    if not parsed:
        if hit:  # serve the last good copy rather than nothing
            return {**hit["v"], "stale": True}
        return {"error": BLOCKED_MSG}
    out = {**parsed, "symbol": symbol, "expiry": expiry, "expiries": expiries[:4], "stale": False}
    CHAIN_CACHE[key] = {"t": time.time(), "v": out}
    return out


# ── Order placement (Angel One SmartAPI) ──
# Safe by default: orders are only simulated ("paper") unless live trading is switched on with environment variables.
# Live orders also need an Angel One API key tied to a registered static IP (SEBI rule, enforced since 1 April 2026).
LIVE_REQUESTED = os.getenv("LIVE_TRADING", "0") == "1"
ANGEL = {"api_key": os.getenv("ANGEL_API_KEY", ""), "client_id": os.getenv("ANGEL_CLIENT_ID", ""),
         "pin": os.getenv("ANGEL_PIN", ""), "totp": os.getenv("ANGEL_TOTP_SECRET", "")}
DHAN = {"client_id": os.getenv("DHAN_CLIENT_ID", ""), "token": os.getenv("DHAN_ACCESS_TOKEN", "")}
BROKER_PREF = os.getenv("BROKER", "").strip().lower()               # default broker when both are set up: angel or dhan
ALLOW_OPTION_SELLING = os.getenv("ALLOW_OPTION_SELLING", "1") == "1"
ORDER_PIN = os.getenv("ORDER_PIN", "")                              # typed before every live order
MAX_ORDER_VALUE = float(os.getenv("MAX_ORDER_VALUE", "50000"))      # rupees per order
MAX_QTY = int(os.getenv("MAX_QTY", "500"))                          # shares per order, or lots for options
MAX_ORDERS_PER_DAY = int(os.getenv("MAX_ORDERS_PER_DAY", "10"))
MAX_FUT_LOTS = int(os.getenv("MAX_FUT_LOTS", "1"))                  # lots per futures order
MAX_FUT_VALUE = float(os.getenv("MAX_FUT_VALUE", "2500000"))        # notional rupees per futures order
EQ_PRICE_BAND, OPT_PRICE_BAND = 5.0, 30.0                           # limit price may differ from last price by this %
ORDER_LOG = os.getenv("ORDER_LOG", "orders_log.jsonl")
MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
DHAN_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
DHAN_BASE = "https://api.dhan.co/v2"


def available_brokers():
    out = []
    if all(ANGEL.values()):
        out.append("angel")
    if all(DHAN.values()):
        out.append("dhan")
    return out


LIVE_BROKERS = available_brokers()
DEFAULT_BROKER = BROKER_PREF if BROKER_PREF in LIVE_BROKERS else (LIVE_BROKERS[0] if LIVE_BROKERS else "angel")
BROKER_NAMES = {"angel": "Angel One", "dhan": "Dhan", "paper": "Paper"}


def decide_mode():
    if not LIVE_REQUESTED:
        return "paper", "Paper mode: orders are simulated and nothing is sent to your broker."
    missing = [n for n, v in [("ORDER_PIN", ORDER_PIN), ("DASH_PASSWORD", PASSWORD or "")] if not v]
    if not LIVE_BROKERS:
        missing.append("a broker login (Angel One: ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET. "
                       "Dhan: DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)")
    if missing:
        return "paper", "Live trading is off because these settings are missing: " + ", ".join(missing) + ". Orders are simulated."
    if len(ORDER_PIN) < 4:
        return "paper", "Live trading is off because ORDER_PIN needs at least 4 characters. Orders are simulated."
    return "live", ""


MODE, MODE_NOTE = decide_mode()
ORDERS, PENDING, PIN_STATE = [], {}, {"fails": 0, "until": 0.0}
ORDER_LOCK = threading.Lock()
SYNC = {"t": 0.0}


def ist_now():
    return pd.Timestamp.now(tz=IST)


def market_open_now():
    n = ist_now()
    return n.weekday() < 5 and dtime(9, 15) <= n.time() <= dtime(15, 30)


def append_log(o):
    try:
        with open(ORDER_LOG, "a") as f:
            f.write(json.dumps(o) + "\n")
    except OSError:
        pass


def load_orders():
    today = ist_now().strftime("%Y-%m-%d")
    latest = {}
    try:
        with open(ORDER_LOG) as f:
            for ln in f:
                try:
                    o = json.loads(ln)
                except ValueError:
                    continue
                if o.get("date") == today:
                    o.setdefault("broker", "angel" if o.get("mode") == "live" else "paper")
                    latest[o["id"]] = o
    except OSError:
        pass
    ORDERS.extend(latest.values())


load_orders()


class OrderRejected(RuntimeError):
    """The broker answered and refused the order, so no order exists."""


class Broker:
    """Angel One session. Reads retry once; order writes are never retried, so an order cannot be sent twice."""

    def __init__(self):
        self.obj = None
        self.lock = threading.Lock()

    def _login(self):
        from SmartApi import SmartConnect  # pip install smartapi-python pyotp
        import pyotp
        obj = SmartConnect(api_key=ANGEL["api_key"])
        res = obj.generateSession(ANGEL["client_id"], ANGEL["pin"], pyotp.TOTP(ANGEL["totp"]).now())
        if not res or not res.get("status"):
            raise RuntimeError("Angel One login failed: " + str((res or {}).get("message", "no response")))
        self.obj = obj

    def read(self, fn, *a):
        with self.lock:
            for i in (1, 2):
                try:
                    if self.obj is None:
                        self._login()
                    return getattr(self.obj, fn)(*a)
                except Exception:
                    self.obj = None
                    if i == 2:
                        raise

    def write(self, names, *a):
        with self.lock:
            if self.obj is None:
                self._login()
            fn = next((getattr(self.obj, n) for n in names if hasattr(self.obj, n)), None)
            if fn is None:
                raise RuntimeError("Installed SmartApi library has no " + names[-1])
            try:
                return fn(*a)
            except Exception:
                self.obj = None
                raise


broker = Broker()
MASTERS = {n: {"day": None, "eq": {}, "opt": {}, "fut": {}} for n in ("angel", "dhan")}


def M(name):
    return MASTERS[name]


def master_name(broker_id):
    """Which instrument list to use. Paper mode uses Angel One's unless BROKER=dhan."""
    if broker_id in MASTERS:
        return broker_id
    return "dhan" if BROKER_PREF == "dhan" else "angel"


def _angel_master():
    r = requests.get(MASTER_URL, timeout=90)
    r.raise_for_status()
    names, eq, opt, fut = set(UNIVERSE), {}, {}, {}
    for it in r.json():
        name = it.get("name")
        if name not in names:
            continue
        seg, typ, sym = it.get("exch_seg"), it.get("instrumenttype"), it.get("symbol", "")
        lot = int(float(it.get("lotsize") or 0))
        if seg == "NSE" and sym.endswith("-EQ"):
            eq[name] = (it["token"], sym)
        elif seg in ("NFO", "BFO") and typ in ("FUTSTK", "FUTIDX") and sym.endswith("FUT"):
            try:
                exp = datetime.strptime(str(it.get("expiry", "")), "%d%b%Y").date()
            except ValueError:
                continue
            fut.setdefault(name, []).append((exp, it["token"], sym, lot, seg))
        elif seg in ("NFO", "BFO") and typ in ("OPTSTK", "OPTIDX") and sym[-2:] in ("CE", "PE"):
            try:
                strike = float(it["strike"]) / 100
            except (KeyError, ValueError):
                continue
            opt[(name, str(it.get("expiry", "")).upper(), strike, sym[-2:])] = (it["token"], sym, lot, seg)
    return eq, opt, fut


def _parse_date(v):
    v = str(v or "").strip()[:10]
    for f in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, f).date()
        except ValueError:
            pass
    return None


def _dhan_master():
    names, eq, opt, fut = set(UNIVERSE), {}, {}, {}
    g = lambda row, *keys: next((str(row[k]).strip() for k in keys if row.get(k) not in (None, "")), "")
    with requests.get(DHAN_MASTER_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        for row in csv.DictReader(ln.decode("utf-8", "ignore") for ln in r.iter_lines()):
            sym = g(row, "UNDERLYING_SYMBOL", "SEM_TRADING_SYMBOL").upper()
            if sym not in names:
                continue
            inst = g(row, "INSTRUMENT", "SEM_INSTRUMENT_NAME").upper()
            exch = g(row, "EXCH_ID", "SEM_EXM_EXCH_ID").upper()
            sid = g(row, "SECURITY_ID", "SEM_SMST_SECURITY_ID")
            if not sid:
                continue
            try:
                lot = int(float(g(row, "LOT_SIZE", "SEM_LOT_UNITS") or 0))
            except ValueError:
                lot = 0
            label = g(row, "DISPLAY_NAME", "SEM_CUSTOM_SYMBOL") or sym
            seg = "NSE_FNO" if exch == "NSE" else "BSE_FNO" if exch == "BSE" else ""
            if inst == "EQUITY" and exch == "NSE" and g(row, "SERIES", "SEM_SERIES").upper() in ("EQ", ""):
                eq[sym] = (sid, label)
            elif inst in ("FUTSTK", "FUTIDX") and seg:
                exp = _parse_date(g(row, "SM_EXPIRY_DATE", "SEM_EXPIRY_DATE"))
                if exp:
                    fut.setdefault(sym, []).append((exp, sid, label, lot, seg))
            elif inst in ("OPTSTK", "OPTIDX") and seg:
                right = g(row, "OPTION_TYPE", "SEM_OPTION_TYPE").upper()[:2]
                exp = _parse_date(g(row, "SM_EXPIRY_DATE", "SEM_EXPIRY_DATE"))
                try:
                    strike = float(g(row, "STRIKE_PRICE", "SEM_STRIKE_PRICE"))
                except ValueError:
                    continue
                if right in ("CE", "PE") and exp:
                    opt[(sym, exp.strftime("%d%b%Y").upper(), strike, right)] = (sid, label, lot, seg)
    return eq, opt, fut


def load_master(name="angel"):
    """Instrument list: ids, lot sizes, futures and option contracts for the dashboard's symbols. Cached for the day."""
    today = ist_now().date()
    m = MASTERS[name]
    if m["day"] == today:
        return True
    try:
        eq, opt, fut = _dhan_master() if name == "dhan" else _angel_master()
    except Exception:
        return m["day"] is not None
    for lst in fut.values():
        lst.sort()
    m.update(day=today, eq=eq, opt=opt, fut=fut)
    return True


def near_future(symbol, name="angel"):
    today = ist_now().date()
    return next((f for f in MASTERS[name]["fut"].get(symbol, []) if f[0] >= today), None)


def row_for(symbol, tf):
    with LOCK:
        for t in [tf] + [x for x in INTERVALS if x != tf]:
            row = next((r for r in STATES[t]["rows"] if r["symbol"] == symbol), None)
            if row:
                return row, t
    return None, None


def count_today():
    with ORDER_LOCK:
        return sum(1 for o in ORDERS if o["mode"] == MODE and not o.get("is_exit"))


def inr(x):
    return "₹" + f"{x:,.2f}"


def make_order(p):
    """Check an order request against every limit. Returns (order, error, warnings, lines)."""
    kind = p.get("kind")
    symbol = str(p.get("symbol", "")).upper().strip()
    side = str(p.get("side", "")).upper()
    otype = str(p.get("order_type", "MARKET")).upper()
    product = str(p.get("product", "INTRADAY")).upper()
    tf = p.get("tf") if p.get("tf") in INTERVALS else DEFAULT_INTERVAL
    fail = lambda m: (None, m, [], [])
    broker_id = str(p.get("broker") or DEFAULT_BROKER).lower() if MODE == "live" else "paper"
    if MODE == "live" and broker_id not in LIVE_BROKERS:
        return fail("That broker is not set up.")
    mname = master_name(broker_id)
    if kind not in ("equity", "option", "future"):
        return fail("Unknown order type.")
    if symbol not in UNIVERSE:
        return fail("This symbol is not on the dashboard list, so it cannot be traded here.")
    if kind == "equity" and symbol in INDEX_YF:
        return fail("An index cannot be bought as shares. Use futures or options.")
    if side not in ("BUY", "SELL") or otype not in ("MARKET", "LIMIT"):
        return fail("Side must be BUY or SELL and the order type Market or Limit.")
    try:
        qty = int(p.get("qty"))
    except (TypeError, ValueError):
        return fail("Quantity must be a whole number.")
    cap = MAX_FUT_LOTS if (kind == "future" or (kind == "option" and side == "SELL")) else MAX_QTY
    if qty < 1 or qty > cap:
        return fail(f"Quantity must be between 1 and {cap}" + (" shares." if kind == "equity" else " lots."))
    if MODE == "live" and not market_open_now():
        return fail("The market is closed. Live orders are accepted 9:15 to 15:30 IST, Monday to Friday.")
    if count_today() >= MAX_ORDERS_PER_DAY:
        return fail(f"Daily limit of {MAX_ORDERS_PER_DAY} orders reached.")

    row, _ = row_for(symbol, tf)
    warnings = []
    strike = right = expiry = None
    if kind == "equity":
        if product not in ("INTRADAY", "DELIVERY"):
            return fail("Product must be Intraday or Delivery.")
        if side == "SELL" and product == "DELIVERY":
            return fail("Delivery sells need shares in your demat account, which this dashboard cannot check. Use Intraday.")
        if not row:
            return fail("No price yet for this stock. Wait for the first scan to finish.")
        if not load_master(mname):
            return fail("Could not load Angel One's instrument list. Try again in a minute.")
        inst = M(mname)["eq"].get(symbol)
        if not inst:
            return fail("This stock was not found in Angel One's instrument list.")
        ref, units, lot, band = row["close"], qty, 1, EQ_PRICE_BAND
        token, tsym, exch, label = inst[0], inst[1], ("NSE_EQ" if mname == "dhan" else "NSE"), symbol
    elif kind == "future":
        if product not in ("INTRADAY", "CARRYFORWARD"):
            return fail("Product must be Intraday or Carry forward.")
        if not row:
            return fail("No price yet for this symbol. Wait for the first scan to finish.")
        if not load_master(mname):
            return fail("Could not load Angel One's instrument list. Try again in a minute.")
        near = near_future(symbol, mname)
        if not near or not near[3]:
            return fail("No futures contract was found for this symbol in Angel One's instrument list.")
        ref, lot, band = row["close"], near[3], EQ_PRICE_BAND
        units = qty * lot
        token, tsym, exch = near[1], near[2], near[4]
        expiry = near[0].strftime(EXPIRY_FMT)
        label = f"{symbol} futures {expiry}"
    else:
        right = str(p.get("right", "")).upper()
        expiry = str(p.get("expiry", ""))
        if symbol in BSE_INDICES:
            return fail("Option chains for Sensex and Bankex are not available here, so they cannot be traded from the chain.")
        try:
            strike = float(p.get("strike"))
            exp_key = datetime.strptime(expiry, EXPIRY_FMT).strftime("%d%b%Y").upper()
        except (TypeError, ValueError):
            return fail("The option contract details are not valid.")
        if right not in ("CE", "PE"):
            return fail("Option type must be CE or PE.")
        if side == "SELL" and not ALLOW_OPTION_SELLING:
            return fail("Selling options is switched off. Set ALLOW_OPTION_SELLING=1 to allow it.")
        if otype != "LIMIT":
            return fail("Option orders must be limit orders, because option markets can be thin.")
        if product not in ("INTRADAY", "CARRYFORWARD"):
            return fail("Product must be Intraday or Carry forward.")
        if not load_master(mname):
            return fail("Could not load Angel One's instrument list. Try again in a minute.")
        inst = M(mname)["opt"].get((symbol, exp_key, strike, right))
        if not inst or not inst[2]:
            return fail("This contract was not found in Angel One's instrument list.")
        chain = build_chain(symbol, expiry)
        leg = next((r[right.lower()] for r in chain.get("rows", []) if r["strike"] == strike), None)
        if not leg or not leg.get("ltp"):
            return fail("No current price for this contract. Reload the option chain and try again.")
        ref, lot, band = leg["ltp"], inst[2], OPT_PRICE_BAND
        units = qty * lot
        token, tsym, exch = inst[0], inst[1], inst[3]
        label = f"{symbol} {int(strike) if strike == int(strike) else strike} {right} {expiry}"

    price = 0.0
    if otype == "LIMIT":
        try:
            price = round(round(float(p.get("price")) / 0.05) * 0.05, 2)
        except (TypeError, ValueError):
            return fail("Enter a limit price.")
        if price <= 0:
            return fail("Enter a limit price.")
        if abs(price / ref - 1) * 100 > band:
            return fail(f"The limit price is more than {band:.0f}% away from the last price {inr(ref)}. Check it for a typo.")
    est = (price or ref) * units
    selling_opt = kind == "option" and side == "SELL"
    exposure = strike * units if selling_opt else None
    if selling_opt:
        if exposure > MAX_FUT_VALUE:
            return fail(f"Selling this option puts about {inr(exposure)} of the underlying at stake, above your limit of {inr(MAX_FUT_VALUE)}.")
    else:
        value_cap = MAX_FUT_VALUE if kind == "future" else MAX_ORDER_VALUE
        if est > value_cap:
            return fail(f"This order is worth about {inr(est)}, above your limit of {inr(value_cap)} per order.")
    key = (kind, symbol, side, strike, right)
    now = time.time()
    with ORDER_LOCK:
        if any(o.get("key") == key and not o.get("is_exit") and now - o["t"] < 60 for o in ORDERS):
            return fail("An identical order was sent less than a minute ago. Check the Orders tab before sending another.")

    sl = risk = None
    signal_row = next((r for r in STATES[tf]["rows"] if r["symbol"] == symbol), None) if tf in STATES else None
    if signal_row and signal_row["signal"] != "–":
        sig_side = "BUY" if signal_row["signal"].endswith("BUY") else "SELL"
        if kind in ("equity", "future"):
            if side != sig_side:
                warnings.append(f"This order is against the current {sig_side} signal.")
            elif signal_row.get("sl") and signal_row.get("entry"):
                sl = round(round(signal_row["sl"] / 0.05) * 0.05, 2)
                risk = round(abs(signal_row["entry"] - signal_row["sl"]), 2)
        elif ((sig_side == "BUY") == (right == "CE")) != (side == "BUY"):
            warnings.append(f"This option {side.lower()} is against the current {sig_side} signal.")
        if signal_row["bars_ago"] is not None and signal_row["bars_ago"] > FRESH_BARS:
            warnings.append(f"The signal is {signal_row['bars_ago']} candles old.")
        if signal_row.get("sl_hit"):
            warnings.append("The stop for this signal has already been hit.")
        elif signal_row.get("hits"):
            warnings.append(f"Target T{signal_row['hits']} has already been reached, so the signal entry price is behind the market.")
    elif signal_row is not None or kind in ("equity", "future"):
        warnings.append("This symbol has no active signal on this timeframe.")
    if kind == "future":
        warnings.append("Futures are leveraged and the stop-loss is not automatic. Place it from the Orders tab once the entry fills.")
    if selling_opt:
        warnings.append("Selling an option can lose far more than the premium you receive. It needs margin, and no stop-loss is placed for you. "
                        "If you are closing an option you bought, this is a sell to close.")

    lines = [["Instrument", label], ["Side", side],
             ["Quantity", f"{qty} lot{'s' if qty > 1 else ''} x {lot} = {units} units" if kind != "equity" else f"{qty} shares"],
             ["Order type", otype.title()], ["Price", inr(price) if otype == "LIMIT" else f"Market (last price {inr(ref)})"],
             ["Product", product.title()], ["Estimated value", inr(est)], ["Mode", "LIVE, real money" if MODE == "live" else "Paper, simulated"]]
    if kind == "equity" and sl:
        lines.append(["Stop-loss from the signal", inr(sl) + " (placed from the Orders tab once filled)"])
    if selling_opt:
        lines.append(["Exposure (strike x units)", inr(exposure)])
    if kind == "future" and risk:
        lines.append(["Stop-loss distance", f"{risk:.2f} points from your fill price (placed from the Orders tab)"])
    order = {"kind": kind, "symbol": symbol, "label": label, "tradingsymbol": tsym, "token": token, "exchange": exch,
             "side": side, "qty": units, "lots": qty, "order_type": otype, "price": price, "ref": ref, "product": product,
             "expiry": expiry, "strike": strike, "right": right, "sl": sl, "risk": risk, "key": key, "broker": broker_id}
    return order, None, warnings, lines


def place_live(o, variety="NORMAL", ordertype=None, trigger=None, side=None, qty=None):
    """Angel One."""
    otype = ordertype or o["order_type"]
    params = {"variety": variety, "tradingsymbol": o["tradingsymbol"], "symboltoken": o["token"],
              "transactiontype": side or o["side"], "exchange": o["exchange"], "ordertype": otype,
              "producttype": o["product"], "duration": "DAY", "price": f'{o["price"]:.2f}' if otype == "LIMIT" else "0",
              "squareoff": "0", "stoploss": "0", "quantity": str(qty or o["qty"])}
    if trigger is not None:
        params["triggerprice"] = f"{trigger:.2f}"
    res = broker.write(("placeOrderFullResponse", "placeOrder"), params)
    if isinstance(res, dict):
        if not res.get("status"):
            raise OrderRejected(str(res.get("message") or "The order was not accepted."))
        oid = (res.get("data") or {}).get("orderid")
    else:
        oid = res
    if not oid:
        raise RuntimeError("The broker did not return an order id.")
    return str(oid)


DHAN_PRODUCT = {"INTRADAY": "INTRADAY", "DELIVERY": "CNC", "CARRYFORWARD": "MARGIN"}
DHAN_ORDER_TYPE = {"MARKET": "MARKET", "LIMIT": "LIMIT", "STOPLOSS_MARKET": "STOP_LOSS_MARKET"}
DHAN_STATUS = {"TRADED": "complete", "PENDING": "open", "TRANSIT": "open pending", "PART_TRADED": "open",
               "REJECTED": "rejected", "CANCELLED": "cancelled", "EXPIRED": "expired"}


def dhan_request(method, path, body=None):
    r = requests.request(method, DHAN_BASE + path, json=body, timeout=15,
                         headers={"access-token": DHAN["token"], "Content-Type": "application/json", "Accept": "application/json"})
    try:
        data = r.json()
    except ValueError:
        data = None
    if 400 <= r.status_code < 500:  # Dhan answered and refused, so nothing was placed
        msg = data.get("errorMessage") if isinstance(data, dict) else None
        raise OrderRejected(msg or f"HTTP {r.status_code}")
    if r.status_code >= 500:
        raise RuntimeError(f"Dhan server error (HTTP {r.status_code})")
    return data


def place_dhan(o, variety="NORMAL", ordertype=None, trigger=None, side=None, qty=None):
    """Dhan."""
    ot = ordertype or o["order_type"]
    body = {"dhanClientId": DHAN["client_id"], "correlationId": "DB" + secrets.token_hex(8),
            "transactionType": side or o["side"], "exchangeSegment": o["exchange"],
            "productType": DHAN_PRODUCT[o["product"]], "orderType": DHAN_ORDER_TYPE[ot], "validity": "DAY",
            "securityId": str(o["token"]), "quantity": int(qty or o["qty"]), "disclosedQuantity": "",
            "price": float(o["price"]) if ot == "LIMIT" else "",
            "triggerPrice": float(trigger) if trigger is not None else "", "afterMarketOrder": False}
    res = dhan_request("POST", "/orders", body)
    oid = str((res or {}).get("orderId") or "")
    if not oid:
        raise RuntimeError("Dhan did not return an order id.")
    if str(res.get("orderStatus", "")).upper() == "REJECTED":
        raise OrderRejected("Dhan rejected the order.")
    return oid


def place_order(o, **kw):
    return (place_dhan if o.get("broker") == "dhan" else place_live)(o, **kw)


def new_record(o, oid, status, variety="NORMAL", is_exit=False, note=""):
    n = ist_now()
    rec = {"id": oid, "t": time.time(), "date": n.strftime("%Y-%m-%d"), "time": n.strftime("%H:%M:%S"), "mode": MODE,
           "kind": o["kind"], "symbol": o["symbol"], "label": o["label"], "tradingsymbol": o["tradingsymbol"],
           "token": o["token"], "exchange": o["exchange"], "side": o["side"], "qty": o["qty"], "order_type": o["order_type"],
           "price": o["price"], "product": o["product"], "status": status, "avg_price": None, "sl": o.get("sl"),
           "sl_order_id": None, "risk": o.get("risk"), "variety": variety, "broker": o.get("broker", "paper"), "is_exit": is_exit, "key": o.get("key"), "note": note}
    with ORDER_LOCK:
        ORDERS.append(rec)
    append_log(rec)
    return rec


def paper_id():
    with ORDER_LOCK:
        return f"PAPER-{sum(1 for o in ORDERS if str(o['id']).startswith('PAPER-')) + 1:04d}"


def public(o):
    return {k: v for k, v in o.items() if k not in ("token", "key", "t")}


def pin_error(pin):
    """Live orders need the order PIN. Five wrong tries lock it for five minutes."""
    if MODE != "live":
        return None
    if time.time() < PIN_STATE["until"]:
        return f"Too many wrong PINs. Try again in {int((PIN_STATE['until'] - time.time()) / 60) + 1} minutes."
    if ORDER_PIN and secrets.compare_digest(str(pin or ""), ORDER_PIN):
        PIN_STATE["fails"] = 0
        return None
    PIN_STATE["fails"] += 1
    if PIN_STATE["fails"] >= 5:
        PIN_STATE.update(fails=0, until=time.time() + 300)
    return "Wrong order PIN."


def angel_book():
    out = {}
    for x in (broker.read("orderBook") or {}).get("data") or []:
        st = str(x.get("orderstatus") or "").lower()
        out[str(x.get("orderid"))] = {"status": st, "avg_price": _num(x.get("averageprice")), "filled": _num(x.get("filledshares")),
                                      "note": str(x.get("text") or "") if st == "rejected" else ""}
    return out


def dhan_book():
    res = dhan_request("GET", "/orders")
    out = {}
    for x in res if isinstance(res, list) else []:
        raw = str(x.get("orderStatus", "")).upper()
        st = DHAN_STATUS.get(raw, raw.lower())
        out[str(x.get("orderId"))] = {"status": st, "avg_price": _num(x.get("averageTradedPrice")), "filled": _num(x.get("filledQty")),
                                      "note": str(x.get("omsErrorDescription") or "") if st == "rejected" else ""}
    return out


def sync_status():
    """Copy order statuses from each broker's order book (at most every 5 seconds)."""
    if MODE != "live" or time.time() - SYNC["t"] < 5:
        return
    SYNC["t"] = time.time()
    with ORDER_LOCK:
        used = {o.get("broker") for o in ORDERS if o["mode"] == "live"}
    books = {}
    for b in used:
        try:
            books[b] = angel_book() if b == "angel" else dhan_book() if b == "dhan" else {}
        except Exception:
            continue
    with ORDER_LOCK:
        for o in ORDERS:
            info = books.get(o.get("broker"), {}).get(str(o["id"]))
            if not info:
                continue
            if info["status"]:
                o["status"] = info["status"]
            if info["avg_price"]:
                o["avg_price"] = info["avg_price"]
            o["filled"] = info["filled"]
            if info["note"]:
                o["note"] = info["note"]


# ── Web app ──
app = FastAPI(title="Signal dashboard")


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    if PASSWORD:
        ok = False
        h = request.headers.get("authorization", "")
        if h.startswith("Basic "):
            try:
                _, _, pw = base64.b64decode(h[6:]).decode().partition(":")
                ok = secrets.compare_digest(pw, PASSWORD)
            except Exception:
                ok = False
        if not ok:
            return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="Signal board"'})
    return await call_next(request)


@app.on_event("startup")
def start_worker():
    threading.Thread(target=refresh, args=(DEFAULT_INTERVAL,), daemon=True).start()


@app.get("/api/signals")
def api_signals(tf: str = DEFAULT_INTERVAL):
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    ensure_fresh(tf)  # scans on demand, so a timeframe nobody is viewing costs nothing
    with LOCK:
        return JSONResponse({
            **STATES[tf],
            "interval": tf,
            "intervals": INTERVALS,
            "settings": f"Chandelier {ATR_LEN}x{ATR_MULT} + EMA {EMA_LEN} + QQE MOD",
            "fresh_bars": FRESH_BARS,
            "symbols": SYMBOLS,
            "indices": INDICES,
        })


@app.post("/api/refresh")
def api_refresh(tf: str = DEFAULT_INTERVAL):
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    threading.Thread(target=refresh, args=(tf,), daemon=True).start()
    return {"ok": True}


@app.get("/api/options/chain")
def api_chain(symbol: str, expiry: str = "", tf: str = DEFAULT_INTERVAL):
    symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9&\-]{1,20}", symbol):
        return JSONResponse({"error": "Enter a valid NSE stock symbol, for example RELIANCE."})
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    if symbol in BSE_INDICES:
        return JSONResponse({"error": "Option chains for Sensex and Bankex are not available here. NSE's site does not carry BSE contracts. "
                                      "Futures signals for them are on the Futures tab."})
    chain = build_chain(symbol, expiry or None)
    if "error" in chain:
        return JSONResponse(chain)

    with LOCK:
        row = next((r for r in STATES[tf]["rows"] if r["symbol"] == symbol), None)
    signal = row["signal"] if row else "–"
    side = "CE" if signal.endswith("BUY") else "PE" if signal.endswith("SELL") else None
    out = {**chain, "signal": signal, "bias": side, "suggested": None, "levels": None}
    if side:
        atm_row = next((r for r in chain["rows"] if r["strike"] == chain["atm"]), None)
        if atm_row:
            out["suggested"] = {"side": side, "strike": chain["atm"], "ltp": atm_row[side.lower()]["ltp"]}
    if row and row.get("entry") is not None:
        out["levels"] = {k: row[k] for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4")}
    return JSONResponse(out)


@app.get("/api/futures/info")
def api_fut_info():
    mn = master_name(DEFAULT_BROKER if MODE == "live" else "paper")
    ok = load_master(mn)
    out = {}
    for name in UNIVERSE:
        near = near_future(name, mn)
        if near:
            out[name] = {"lot": near[3], "expiry": near[0].strftime(EXPIRY_FMT)}
    return {"ok": ok, "info": out}


@app.get("/api/trading")
def api_trading():
    return {"mode": MODE, "note": MODE_NOTE, "market_open": market_open_now(),
            "brokers": LIVE_BROKERS if MODE == "live" else [], "default_broker": DEFAULT_BROKER, "broker_names": BROKER_NAMES,
            "caps": {"max_value": MAX_ORDER_VALUE, "max_qty": MAX_QTY, "max_orders": MAX_ORDERS_PER_DAY,
                     "max_fut_lots": MAX_FUT_LOTS, "max_fut_value": MAX_FUT_VALUE}}


@app.get("/api/trading/check")
def api_trading_check(which: str = ""):
    """Read-only test of the broker connection and the instrument list. Places no order."""
    if MODE != "live":
        return {"ok": False, "message": "Paper mode: no broker is connected. " + MODE_NOTE}
    b = which if which in LIVE_BROKERS else DEFAULT_BROKER
    name = BROKER_NAMES[b]
    try:
        angel_book() if b == "angel" else dhan_book()
    except Exception as e:
        return {"ok": False, "broker": name, "message": f"Could not connect to {name}: {str(e)[:200]}"}
    loaded = load_master(b)
    m = M(b)
    miss = [x for x in SYMBOLS if x not in m["eq"]]
    msg = (f"Connected to {name}. Instrument list: {len(SYMBOLS) - len(miss)} of {len(SYMBOLS)} stocks, "
           f"{len(m['fut'])} futures symbols, {len(m['opt'])} option contracts.")
    if not loaded or not m["eq"]:
        msg += " The instrument list did not load or did not match, so orders will be refused until it does."
    elif miss:
        msg += " Not found: " + ", ".join(miss[:5]) + ("..." if len(miss) > 5 else "") + "."
    return {"ok": bool(loaded and m["eq"]), "broker": name, "message": msg}


@app.post("/api/orders/preview")
def api_preview(payload: dict = Body(...)):
    order, err, warnings, lines = make_order(payload)
    if err:
        return {"error": err}
    token = secrets.token_urlsafe(16)
    now = time.time()
    with ORDER_LOCK:
        for k in [k for k, v in PENDING.items() if v["exp"] < now]:
            PENDING.pop(k, None)
        PENDING[token] = {"order": order, "exp": now + 90}
    return {"token": token, "lines": lines, "warnings": warnings, "mode": MODE}


@app.post("/api/orders/confirm")
def api_confirm(payload: dict = Body(...)):
    token = str(payload.get("token", ""))
    with ORDER_LOCK:
        item = PENDING.get(token)
    if not item or item["exp"] < time.time():
        return {"error": "This review expired or was already used. Review the order again."}
    if str(payload.get("confirm", "")).strip().upper() != "YES":
        return {"error": "Type YES to confirm."}
    err = pin_error(payload.get("pin"))
    if err:
        return {"error": err}
    if MODE == "live" and not market_open_now():
        return {"error": "The market is closed."}
    if count_today() >= MAX_ORDERS_PER_DAY:
        return {"error": f"Daily limit of {MAX_ORDERS_PER_DAY} orders reached."}
    with ORDER_LOCK:
        item = PENDING.pop(token, None)  # single use: a double click cannot send two orders
    if not item:
        return {"error": "This review was already used. Check the Orders tab."}
    o = item["order"]
    if MODE == "paper":
        status = "complete" if o["order_type"] == "MARKET" else "open"
        rec = new_record(o, paper_id(), status)
        if status == "complete":
            rec["avg_price"] = o["ref"]
        return {"ok": True, "order": public(rec)}
    bname = BROKER_NAMES.get(o.get("broker"), "broker")
    try:
        oid = place_order(o)
    except OrderRejected as e:
        return {"error": f"{bname} rejected the order: {e}"}
    except Exception as e:
        new_record(o, "UNKNOWN-" + secrets.token_hex(3), "unknown",
                   note=f"{bname} did not confirm this order. Check your {bname} app before sending it again.")
        return {"error": f"{bname} did not confirm the order ({e}). Check your {bname} app before trying again."}
    return {"ok": True, "order": public(new_record(o, oid, "open"))}


@app.get("/api/orders")
def api_orders():
    sync_status()
    with ORDER_LOCK:
        return {"mode": MODE, "orders": [public(o) for o in reversed(ORDERS)]}


def find_order(oid):
    with ORDER_LOCK:
        return next((o for o in ORDERS if str(o["id"]) == str(oid)), None)


@app.post("/api/orders/cancel")
def api_cancel(payload: dict = Body(...)):
    o = find_order(payload.get("order_id"))
    if not o:
        return {"error": "Order not found."}
    err = pin_error(payload.get("pin"))
    if err:
        return {"error": err}
    if o["mode"] == "live":
        try:
            if o.get("broker") == "dhan":
                dhan_request("DELETE", f"/orders/{o['id']}")
            else:
                broker.write(("cancelOrder",), o["id"], o.get("variety", "NORMAL"))
        except Exception as e:
            return {"error": f"The broker did not confirm the cancel ({e}). Check your {BROKER_NAMES.get(o.get('broker'), 'broker')} app."}
    o["status"] = "cancelled"
    append_log(o)
    return {"ok": True}


@app.post("/api/orders/protect")
def api_protect(payload: dict = Body(...)):
    """Place the stop-loss as a stop-market order, only after the entry order is complete."""
    if MODE == "live":
        sync_status()
    o = find_order(payload.get("order_id"))
    if not o or o.get("is_exit") or o["kind"] not in ("equity", "future"):
        return {"error": "A stop-loss can only be added to a stock or futures entry order."}
    if o["mode"] != MODE:
        return {"error": "This order was placed in a different mode."}
    if not (o.get("risk") if o["kind"] == "future" else o.get("sl")):
        return {"error": "This order has no stop-loss level from a signal."}
    if o.get("sl_order_id"):
        return {"error": "A stop-loss was already placed for this order."}
    if o["status"] != "complete":
        return {"error": "The entry order is not filled yet. Place the stop-loss after it shows complete."}
    err = pin_error(payload.get("pin"))
    if err:
        return {"error": err}
    qty = int(o.get("filled") or o["qty"])
    exit_side = "SELL" if o["side"] == "BUY" else "BUY"
    fill = o.get("avg_price") or o["price"]
    if o["kind"] == "future":
        # futures trade at a different price from the index, so the stop is measured from the actual fill
        if not fill:
            return {"error": "The fill price is not known yet. Try again in a few seconds."}
        sl = round(round((fill - o["risk"] if o["side"] == "BUY" else fill + o["risk"]) / 0.05) * 0.05, 2)
    else:
        sl = o["sl"]
        if fill and ((o["side"] == "BUY" and sl >= fill) or (o["side"] == "SELL" and sl <= fill)):
            return {"error": "The stop-loss level is on the wrong side of the entry price, so it was not placed."}
    exit_o = {**o, "side": exit_side, "qty": qty, "order_type": "STOPLOSS_MARKET", "price": 0.0, "key": None, "sl": sl}
    if MODE == "paper":
        rec = new_record(exit_o, paper_id(), "open", variety="STOPLOSS", is_exit=True, note=f"Trigger {sl:.2f}")
    else:
        try:
            oid = place_order(o, variety="STOPLOSS", ordertype="STOPLOSS_MARKET", trigger=sl, side=exit_side, qty=qty)
        except Exception as e:
            return {"error": f"The broker did not confirm the stop-loss ({e}). Check your {BROKER_NAMES.get(o.get('broker'), 'broker')} app."}
        rec = new_record(exit_o, oid, "open", variety="STOPLOSS", is_exit=True, note=f"Trigger {sl:.2f}")
    o["sl_order_id"] = rec["id"]
    append_log(o)
    return {"ok": True, "order": public(rec)}


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signal board</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Schibsted+Grotesk:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;
--buy:#0b7a63;--buy-bg:#d8f0e8;--sell:#b3261e;--sell-bg:#f9dcd9;--flat:#e6eaee}
@media(prefers-color-scheme:dark){:root{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;
--buy:#4cc9a8;--buy-bg:#16372f;--sell:#ff8a80;--sell-bg:#42221f;--flat:#252f38}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 "Schibsted Grotesk",system-ui,sans-serif;font-variant-numeric:tabular-nums}
main{max-width:1180px;margin:0 auto;padding:28px 20px 60px}
header{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap;margin-bottom:20px}
h1{margin:0;font-size:26px;font-weight:700;letter-spacing:-.01em}
.sub{color:var(--mute);font-size:13px;margin-top:2px}
button,input{font:inherit;color:inherit}
.btn{background:var(--ink);color:var(--bg);border:0;border-radius:8px;padding:8px 14px;cursor:pointer;font-weight:500}
.btn:disabled{opacity:.5;cursor:wait}
:focus-visible{outline:2px solid var(--buy);outline-offset:2px}
.ctl{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.seg{display:flex;border:1px solid var(--line);border-radius:8px;overflow:hidden;background:var(--panel)}
.seg button{background:none;border:0;padding:8px 14px;cursor:pointer;font-weight:500;color:var(--mute)}
.seg button[aria-pressed=true]{background:var(--ink);color:var(--bg)}
.tally{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line);border-radius:10px;overflow:hidden;background:var(--panel);margin-bottom:14px}
.tally button{background:none;border:0;border-right:1px solid var(--line);padding:12px 14px;text-align:left;cursor:pointer}
.tally button:last-child{border-right:0}
.tally button[aria-pressed=true]{background:var(--flat)}
.tally b{display:block;font-size:24px;font-weight:700}
.tally span{color:var(--mute);font-size:13px}
.buy b{color:var(--buy)}.sell b{color:var(--sell)}
.bar{display:flex;gap:10px;margin-bottom:10px}
.bar input{flex:1;max-width:260px;background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:8px 12px}
.wrap{background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:900px}
th,td{padding:10px 12px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}
th:first-child,td:first-child,th:nth-child(2),td:nth-child(2),th:nth-child(10),td:nth-child(10){text-align:left}
th{font-weight:500;color:var(--mute);font-size:13px;cursor:pointer;user-select:none}
tr:last-child td{border-bottom:0}
.pill{display:inline-block;min-width:84px;text-align:center;padding:2px 10px;border-radius:999px;font-size:13px;font-weight:500;background:var(--flat);color:var(--mute)}
.pill.BUY{background:var(--buy-bg);color:var(--buy);font-weight:700}
.pill.SELL{background:var(--sell-bg);color:var(--sell);font-weight:700}
.pill.Open_BUY{color:var(--buy)}.pill.Open_SELL{color:var(--sell)}
.up{color:var(--buy)}.dn{color:var(--sell)}
.note{color:var(--mute);font-size:13px;margin-top:12px;max-width:80ch}
.empty{padding:40px;text-align:center;color:var(--mute)}
.mode{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid var(--line)}
.mode button{background:none;border:0;border-bottom:2px solid transparent;padding:8px 14px;margin-bottom:-1px;cursor:pointer;font-weight:500;color:var(--mute)}
.mode button[aria-pressed=true]{color:var(--ink);border-bottom-color:var(--ink)}
.obar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
.obar input{flex:1;min-width:220px;max-width:320px;background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:8px 12px}
select{font:inherit;color:inherit;background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:3px 6px}
.chips{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:14px}
.chip{display:inline-flex;align-items:center;gap:8px;background:var(--panel);border:1px solid var(--line);border-radius:999px;padding:5px 12px;cursor:pointer}
.chip[aria-pressed=true]{border-color:var(--ink)}
.chip .pill{min-width:0;padding:0 8px;font-size:12px}
.osum{display:flex;flex-wrap:wrap;gap:4px 28px;background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 16px;margin-bottom:12px}
.osum:not([hidden]){display:flex}
.stat{display:flex;flex-direction:column;gap:2px}
.stat span.l{color:var(--mute);font-size:13px}
.stat .lv{margin-right:12px;font-weight:400}
table.otab{min-width:760px}
table.otab th,table.otab td{text-align:right}
table.otab td.k,table.otab th.k{text-align:center;font-weight:700}
table.otab th.g{text-align:center;color:var(--ink);cursor:default}
tr.atm td{background:var(--flat)}
td.bias-ce{background:color-mix(in srgb,var(--buy-bg) 55%,transparent)}
td.bias-pe{background:color-mix(in srgb,var(--sell-bg) 55%,transparent)}
td.pick-ce{box-shadow:inset 0 0 0 2px var(--buy);font-weight:700}
td.pick-pe{box-shadow:inset 0 0 0 2px var(--sell);font-weight:700}
.banner{border-radius:8px;padding:8px 14px;margin-bottom:14px;font-size:14px;border:1px solid var(--line);background:var(--panel);color:var(--mute)}
.banner.live{background:var(--sell-bg);color:var(--sell);border-color:var(--sell);font-weight:500}
.mini{background:none;border:1px solid var(--ink);border-radius:6px;padding:2px 10px;cursor:pointer;font-size:13px}
.ob{border:0;border-radius:4px;width:20px;height:20px;font-size:11px;font-weight:700;cursor:pointer;margin-left:4px;color:var(--bg)}
.ob.b{background:var(--buy)}.ob.s{background:var(--sell)}
.px{display:inline-block;min-width:52px}
dialog{background:var(--panel);color:var(--ink);border:1px solid var(--line);border-radius:12px;padding:0;width:min(460px,92vw)}
dialog::backdrop{background:rgba(0,0,0,.45)}
.tk{padding:20px}.tk h2{margin:0 0 4px;font-size:20px}
.tkgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:14px 0}
.tkgrid label:first-child{grid-column:1/-1}
.tk label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:var(--mute)}
.tk input,.tk select{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:7px 9px}
.tkreview{border:1px solid var(--line);border-radius:8px;padding:8px 12px;margin-bottom:10px}
.tkreview div{display:flex;justify-content:space-between;gap:12px;padding:3px 0}
.tkreview span{color:var(--mute)}
.warn{color:var(--sell);margin:6px 0 0;font-size:13px}
.tkmsg{min-height:20px;font-size:14px;margin:8px 0}
.tkbtns{display:flex;gap:8px;justify-content:flex-end}
.btn.ghost{background:none;color:var(--ink);border:1px solid var(--line)}
.btn.go{background:var(--buy);color:#fff}
#ordTable{min-width:760px}#ordTable th,#ordTable td{text-align:left}
.tag{font-size:11px;color:var(--mute);border:1px solid var(--line);border-radius:4px;padding:0 4px;margin-left:6px;font-weight:400}
@media(max-width:640px){.tally{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main>
<header>
 <div><h1>Signal board</h1><div class="sub" id="meta">Loading first scan…</div></div>
 <div class="ctl"><div class="seg" id="tfs" role="group" aria-label="Timeframe"></div><button class="btn" id="refresh">Refresh now</button></div>
</header>
<div class="banner paper" id="banner"></div>
<nav class="mode" id="mode" aria-label="View"><button data-m="stocks">Stocks</button><button data-m="futures">Futures</button><button data-m="options">Options</button><button data-m="orders">Orders</button></nav>
<section id="view-stocks">
<div class="tally" id="tally"></div>
<div class="bar"><input id="q" placeholder="Search symbol" aria-label="Search symbol"></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table></div>
<div class="note" id="note"></div>
</section>
<section id="view-options" hidden>
<div class="obar">
 <input id="osym" list="fno" placeholder="Stock or index, e.g. RELIANCE or NIFTY" aria-label="Stock symbol" autocomplete="off">
 <datalist id="fno"></datalist>
 <button class="btn" id="oload">Load chain</button>
</div>
<div class="chips" id="chips"></div>
<div class="osum" id="osum" hidden></div>
<div class="wrap"><table class="otab" id="otable"></table></div>
<div class="note" id="onote">Pick a symbol with a signal above, or type any F&amp;O stock or index (NIFTY, BANKNIFTY and so on). Sensex and Bankex option chains are not available here.</div>
</section>
<section id="view-orders" hidden>
<div class="note" id="ordNote" style="margin:0 0 10px">Orders placed from this dashboard today.</div>
<div class="wrap"><table id="ordTable"></table></div>
<div style="margin-top:12px"><button class="mini" id="chk">Check broker connection</button> <span class="sub" id="chkMsg"></span></div>
</section>
</main>
<dialog id="ticket" aria-labelledby="tkTitle"><div class="tk">
 <h2 id="tkTitle">Order ticket</h2>
 <div class="sub" id="tkMode"></div>
 <div class="tkgrid">
  <label>Instrument<input id="tkInst" readonly></label>
  <label>Side<select id="tkSide"></select></label>
  <label><span id="tkQtyT">Quantity</span><input id="tkQty" type="number" min="1" step="1" value="1"></label>
  <label>Order type<select id="tkType"></select></label>
  <label>Limit price<input id="tkPrice" type="number" step="0.05" min="0"></label>
  <label>Product<select id="tkProd"></select></label>
  <label id="tkBrokerL" hidden>Broker<select id="tkBroker"></select></label>
 </div>
 <div class="tkreview" id="tkReview" hidden></div>
 <div class="tkgrid" id="tkConfirm" hidden>
  <label>Type YES to confirm<input id="tkYes" autocomplete="off"></label>
  <label id="tkPinL" hidden>Order PIN<input id="tkPin" type="password" autocomplete="off"></label>
 </div>
 <div class="tkmsg" id="tkMsg" role="status"></div>
 <div class="tkbtns"><button class="btn ghost" id="tkClose">Close</button><button class="btn" id="tkReviewBtn">Review order</button><button class="btn go" id="tkPlace" hidden>Place order</button></div>
</div></dialog>
<script>
const colsStocks=[["symbol","Symbol"],["signal","Signal"],["close","Close"],["entry","Entry"],["sl","SL"],["tp1","T1"],["tp2","T2"],["tp3","T3"],["tp4","T4"],["status","Status"],["signal_time","Signal candle"],["_t",""]];
const colsFut=[["symbol","Symbol"],["signal","Signal"],["close","Spot"],["entry","Entry"],["sl","SL"],["tp1","T1"],["tp2","T2"],["tp3","T3"],["tp4","T4"],["status","Status"],["signal_time","Signal candle"],["lot","Lot"],["fexp","Expiry"],["_t",""]];
let data=null,filter="ALL",sortKey="signal",sortDir=1,q="",timer=null,tf=null,futInfo={},futLoaded=false;
try{tf=localStorage.getItem("tf")}catch(e){}
const tfName={"5m":"5 min","15m":"15 min","1d":"Daily"};
const rank={BUY:0,SELL:1,"Open BUY":2,"Open SELL":2,"–":3};
const fmt=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const curCols=()=>mode==="futures"?colsFut:colsStocks;
function renderHead(){document.getElementById("head").innerHTML=curCols().map(c=>`<th data-k="${c[0]}">${c[1]}</th>`).join("")}
document.getElementById("head").onclick=e=>{const k=e.target.dataset.k;if(!k||k==="_t")return;sortDir=sortKey===k?-sortDir:1;sortKey=k;render()};
document.getElementById("q").oninput=e=>{q=e.target.value.trim().toUpperCase();render()};
document.getElementById("refresh").onclick=async()=>{const b=document.getElementById("refresh");b.disabled=true;await fetch("/api/refresh?tf="+(tf||""),{method:"POST"});setTimeout(()=>{load();b.disabled=false},4000)};
document.getElementById("tfs").onclick=e=>{const b=e.target.closest("button");if(!b||b.dataset.tf===tf)return;
 tf=b.dataset.tf;try{localStorage.setItem("tf",tf)}catch(x){}
 data=null;document.getElementById("body").innerHTML=`<tr><td colspan="14" class="empty">Scanning ${tfName[tf]} candles…</td></tr>`;
 document.querySelectorAll("#tfs button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.tf===tf));load();if(osym)loadChain()};
async function load(){clearTimeout(timer);const asked=tf;
 try{const d=await (await fetch("/api/signals"+(asked?"?tf="+asked:""))).json();if(asked!==tf)return;data=d;tf=d.interval;render();
  timer=setTimeout(load,(d.refreshing||!d.updated)?3000:20000)}
 catch(e){document.getElementById("meta").textContent="Cannot reach the server.";timer=setTimeout(load,10000)}}
async function loadFutInfo(){if(futLoaded)return;futLoaded=true;
 try{const d=await (await fetch("/api/futures/info")).json();futInfo=d.info||{};if(!d.ok)futLoaded=false;render()}catch(e){futLoaded=false}}
const isOpen=r=>r.signal.startsWith("Open");
function cellFor(r,k){
 const tp=n=>`<td class="${r.hits>=n?"up":""}">${fmt(r["tp"+n])}${r.hits>=n?" ✓":""}</td>`;
 switch(k){
  case "symbol":return `<td><b>${esc(r.symbol)}</b>${r.type==="index"?'<span class="tag">Index</span>':""}</td>`;
  case "signal":return `<td><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span></td>`;
  case "close":case "entry":return `<td>${fmt(r[k])}</td>`;
  case "sl":return `<td class="${r.sl_hit?"dn":""}">${fmt(r.sl)}${r.sl_hit?" ✖":""}</td>`;
  case "tp1":return tp(1);case "tp2":return tp(2);case "tp3":return tp(3);case "tp4":return tp(4);
  case "status":return `<td>${esc(r.status)}</td>`;
  case "signal_time":return `<td>${esc(r.signal_time)}</td>`;
  case "lot":return `<td>${r.lot??"–"}</td>`;
  case "fexp":return `<td>${r.fexp??"–"}</td>`;
  default:return `<td>${r.entry!=null&&r.signal!=="–"?`<button class="mini" data-trade="${esc(r.symbol)}">Trade</button>`:""}</td>`}}
function render(){
 if(!data)return;
 document.getElementById("tfs").innerHTML=data.intervals.map(x=>`<button data-tf="${x}" aria-pressed="${x===tf}">${tfName[x]||x}</button>`).join("");
 renderChips();
 const fut=mode==="futures";
 const rows=fut?data.rows:data.rows.filter(r=>r.type!=="index");
 rows.forEach(r=>{const f=futInfo[r.symbol];r.lot=f?f.lot:null;r.fexp=f?f.expiry:null});
 const cnt=s=>rows.filter(r=>r.signal===s).length;
 const tabs=[["ALL","All symbols",rows.length,""],["BUY","Buy",cnt("BUY"),"buy"],["SELL","Sell",cnt("SELL"),"sell"],["OPEN","Open trades",rows.filter(isOpen).length,""]];
 const t=document.getElementById("tally");
 t.innerHTML=tabs.map(x=>`<button class="${x[3]}" data-f="${x[0]}" aria-pressed="${filter===x[0]}"><b>${x[2]}</b><span>${x[1]}</span></button>`).join("");
 t.onclick=e=>{const b=e.target.closest("button");if(b){filter=b.dataset.f;render()}};
 document.getElementById("meta").textContent=(data.updated?`Updated ${data.updated} · `:"")+`${tfName[data.interval]} candles · ${data.settings}`+(data.refreshing?" · scanning…":"");
 let v=rows.filter(r=>(filter==="ALL"||(filter==="OPEN"?isOpen(r):r.signal===filter))&&(!q||r.symbol.includes(q)));
 v.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];if(sortKey==="signal"){x=rank[x];y=rank[y];if(x===y){x=a.bars_ago??999;y=b.bars_ago??999}}
  if(x==null)x=1e9;if(y==null)y=1e9;return (x>y?1:x<y?-1:0)*sortDir});
 const cs=curCols();
 document.getElementById("body").innerHTML=v.length?v.map(r=>`<tr>${cs.map(c=>cellFor(r,c[0])).join("")}</tr>`).join(""):`<tr><td colspan="${cs.length}" class="empty">${data.updated?"No symbols match this filter.":"First scan is running. This page updates on its own."}</td></tr>`;
 document.getElementById("note").textContent=(data.errors.length?`${data.errors.length} symbol(s) skipped: ${data.errors.slice(0,3).join("; ")}${data.errors.length>3?"…":""}. `:"")+`BUY and SELL show for ${data.fresh_bars} candles after a signal. Open trades are older signals whose stop and T4 have not been reached. Entry, SL and targets come from the latest signal for each symbol.`+(fut?" Futures signals come from the same indicator on the stock's or index's own candles (spot prices). The lot size and expiry shown are for the nearest-month contract, from Angel One's instrument list.":"");
}
// ── Options tab ──
let mode="stocks",osym="",oexp="",oreq=0;
try{mode=localStorage.getItem("mode")||"stocks"}catch(e){}
const esc=s=>String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
const fk=n=>{if(n==null)return "–";const a=Math.abs(n),g=n<0?"-":"";return g+(a>=1e7?(a/1e7).toFixed(2)+" Cr":a>=1e5?(a/1e5).toFixed(2)+" L":a>=1e3?(a/1e3).toFixed(1)+" K":String(Math.round(a)))};
const f2=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const sgn=n=>n==null?"–":`<span class="${n>=0?"up":"dn"}">${n>0?"+":""}${fk(n)}</span>`;
function setMode(m){mode=m;try{localStorage.setItem("mode",m)}catch(e){}
 const vm={stocks:"stocks",futures:"stocks",options:"options",orders:"orders"};
 ["stocks","options","orders"].forEach(x=>document.getElementById("view-"+x).hidden=x!==vm[m]);
 document.querySelectorAll("#mode button").forEach(b=>b.setAttribute("aria-pressed",b.dataset.m===m));
 if(m==="orders")loadOrders();if(m==="options")renderChips();
 if(m==="stocks"||m==="futures"){filter="ALL";q="";document.getElementById("q").value="";renderHead();if(m==="futures")loadFutInfo();render()}}
document.getElementById("mode").onclick=e=>{const b=e.target.closest("button");if(b)setMode(b.dataset.m)};
function renderChips(){
 if(!data)return;
 const dl=document.getElementById("fno");if(!dl.children.length&&data.symbols)dl.innerHTML=(data.symbols||[]).concat(data.indices||[]).map(s=>`<option value="${esc(s)}">`).join("");
 const sig=data.rows.filter(r=>r.signal!=="–").sort((a,b)=>rank[a.signal]-rank[b.signal]||(a.bars_ago??999)-(b.bars_ago??999));
 document.getElementById("chips").innerHTML=sig.length?sig.map(r=>`<button class="chip" data-s="${esc(r.symbol)}" aria-pressed="${r.symbol===osym}"><b>${esc(r.symbol)}</b><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span></button>`).join(""):`<span class="sub">No stocks have a signal on this timeframe. Type any F&amp;O stock or index symbol to see its chain.</span>`;
}
document.getElementById("chips").onclick=e=>{const b=e.target.closest(".chip");if(b){osym=b.dataset.s;document.getElementById("osym").value=osym;oexp="";loadChain()}};
const startLoad=()=>{osym=document.getElementById("osym").value.trim().toUpperCase();oexp="";loadChain()};
document.getElementById("oload").onclick=startLoad;
document.getElementById("osym").onkeydown=e=>{if(e.key==="Enter")startLoad()};
async function loadChain(quiet){
 if(!osym)return;const my=++oreq,box=document.getElementById("onote");
 if(!quiet){document.getElementById("otable").innerHTML="";document.getElementById("osum").hidden=true;box.textContent=`Loading the ${osym} option chain…`}
 renderChips();
 try{const u=`/api/options/chain?symbol=${encodeURIComponent(osym)}&tf=${tf||""}`+(oexp?`&expiry=${encodeURIComponent(oexp)}`:"");
  const d=await (await fetch(u)).json();if(my!==oreq)return;renderChain(d)}
 catch(e){if(my===oreq)box.textContent="Cannot reach the server."}
}
function renderChain(d){
 const box=document.getElementById("onote"),sum=document.getElementById("osum"),tb=document.getElementById("otable");
 if(d.error){sum.hidden=true;tb.innerHTML="";box.textContent=d.error;return}
 oexp=d.expiry;lastChain=d;const sg=d.signal||"–";
 const stat=(l,v)=>`<div class="stat"><span class="l">${l}</span><b>${v}</b></div>`;
 const lv=d.levels?["entry","sl","tp1","tp2","tp3","tp4"].map((k,i)=>`<span class="lv">${["Entry","SL","T1","T2","T3","T4"][i]} ${f2(d.levels[k])}</span>`).join(""):"";
 sum.hidden=false;
 sum.innerHTML=stat(esc(d.symbol)+" price",f2(d.underlying))
  +`<div class="stat"><span class="l">Expiry</span><select id="oexp" aria-label="Expiry">${d.expiries.map(x=>`<option ${x===d.expiry?"selected":""}>${x}</option>`).join("")}</select></div>`
  +stat("Put/call ratio (OI)",d.pcr??"–")+stat("Most call OI at",d.max_ce??"–")+stat("Most put OI at",d.max_pe??"–")
  +stat("Stock signal",`<span class="pill ${sg.replace(" ","_")}">${sg}</span>`)
  +(d.suggested?stat("Option bias",`ATM ${d.suggested.strike} ${d.suggested.side}${d.suggested.ltp!=null?" at ₹"+f2(d.suggested.ltp):""}`):"")
  +(lv?stat("Stock levels",lv):"");
 document.getElementById("oexp").onchange=e=>{oexp=e.target.value;loadChain()};
 const cls=(...a)=>a.filter(Boolean).join(" ");
 const pickCe=(atm,i)=>d.suggested&&d.suggested.side==="CE"&&atm&&i===4?"pick-ce":"";
 const pickPe=(atm,i)=>d.suggested&&d.suggested.side==="PE"&&atm&&i===0?"pick-pe":"";
 const iv=l=>l.iv==null?"–":l.iv.toFixed(1);
 const rows=d.rows.map(r=>{const atm=r.strike===d.atm;
  const ce=[fk(r.ce.oi),sgn(r.ce.doi),fk(r.ce.vol),iv(r.ce),ltpBtn(r,"CE")].map((c,i)=>`<td class="${cls(d.bias==="CE"&&"bias-ce",pickCe(atm,i))}">${c}</td>`).join("");
  const pe=[ltpBtn(r,"PE"),iv(r.pe),fk(r.pe.vol),sgn(r.pe.doi),fk(r.pe.oi)].map((c,i)=>`<td class="${cls(d.bias==="PE"&&"bias-pe",pickPe(atm,i))}">${c}</td>`).join("");
  return `<tr class="${atm?"atm":""}">${ce}<td class="k">${r.strike.toLocaleString("en-IN")}</td>${pe}</tr>`}).join("");
 tb.innerHTML=`<thead><tr><th class="g" colspan="5">Calls</th><th class="k" rowspan="2">Strike</th><th class="g" colspan="5">Puts</th></tr><tr><th>OI</th><th>Change in OI</th><th>Volume</th><th>IV</th><th>LTP</th><th>LTP</th><th>IV</th><th>Volume</th><th>Change in OI</th><th>OI</th></tr></thead><tbody>${rows}</tbody>`;
 box.textContent=(d.timestamp?`NSE data as of ${d.timestamp}. `:"")+(d.stale?"NSE did not respond just now, so this is the last copy received. ":"")+"Calls are tinted when the stock has a BUY signal and puts when it has a SELL signal. The indicator runs on the stock's own candles, not on option prices, because NSE does not provide option candle history. The at-the-money strike is shaded. Click B or S beside a price to buy or sell that option.";
}
setInterval(()=>{if(mode==="options"&&osym)loadChain(true)},60000);
// ── Orders and order ticket ──
let tk=null,tkToken=null,tradeInfo={mode:"paper",caps:{}},lastChain=null;
const $=id=>document.getElementById(id);
const post=async(u,b)=>(await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)})).json();
const ltpBtn=(r,s)=>{const l=r[s.toLowerCase()].ltp;if(l==null)return "–";const d=`data-strike="${r.strike}" data-right="${s}" data-ltp="${l}"`;
 return `<span class="px">${f2(l)}</span><button class="ob b" ${d} data-side="BUY" title="Buy ${s}" aria-label="Buy ${r.strike} ${s}">B</button><button class="ob s" ${d} data-side="SELL" title="Sell ${s}" aria-label="Sell ${r.strike} ${s}">S</button>`};
async function loadTrading(){try{tradeInfo=await (await fetch("/api/trading")).json();const b=$("banner");b.className="banner "+tradeInfo.mode;
 b.textContent=tradeInfo.mode==="live"?`LIVE trading is on. Orders go to your ${(tradeInfo.brokers||[]).map(x=>tradeInfo.broker_names[x]).join(" or ")} account. Limits: ₹${tradeInfo.caps.max_value.toLocaleString("en-IN")} per order and ${tradeInfo.caps.max_orders} orders a day. Futures: up to ${tradeInfo.caps.max_fut_lots} lot(s) and ₹${tradeInfo.caps.max_fut_value.toLocaleString("en-IN")} notional.`:tradeInfo.note}catch(e){}}
function resetReview(){tkToken=null;$("tkReview").hidden=true;$("tkConfirm").hidden=true;$("tkPlace").hidden=true;$("tkReviewBtn").hidden=false;$("tkMsg").textContent=""}
function openTicket(s){tk=s;const opt=s.kind==="option",fut=s.kind==="future";
 $("tkTitle").textContent=opt?"Option order":fut?"Futures order":"Order ticket";$("tkInst").value=s.label;
 $("tkSide").innerHTML="<option>BUY</option><option>SELL</option>";$("tkSide").value=s.side;
 $("tkQtyT").textContent=opt||fut?"Lots":"Quantity (shares)";$("tkQty").value=1;
 $("tkType").innerHTML=opt?'<option value="LIMIT">Limit</option>':'<option value="MARKET">Market</option><option value="LIMIT">Limit</option>';
 $("tkPrice").value=s.price??"";$("tkPrice").disabled=$("tkType").value!=="LIMIT";
 $("tkProd").innerHTML=opt||fut?'<option value="INTRADAY">Intraday</option><option value="CARRYFORWARD">Carry forward</option>':'<option value="INTRADAY">Intraday</option><option value="DELIVERY">Delivery</option>';
 $("tkMode").textContent=tradeInfo.mode==="live"?"LIVE: this sends a real order to your broker.":"Paper mode: this order is only simulated.";
 const bl=tradeInfo.brokers||[];$("tkBroker").innerHTML=bl.map(x=>`<option value="${x}">${tradeInfo.broker_names[x]}</option>`).join("");
 $("tkBrokerL").hidden=bl.length<2;if(tradeInfo.default_broker&&bl.length)$("tkBroker").value=tradeInfo.default_broker;
 $("tkYes").value="";$("tkPin").value="";resetReview();$("ticket").showModal()}
["tkSide","tkQty","tkType","tkPrice","tkProd","tkBroker"].forEach(id=>{$(id).addEventListener("input",resetReview);$(id).addEventListener("change",resetReview)});
$("tkType").addEventListener("change",()=>{$("tkPrice").disabled=$("tkType").value!=="LIMIT"});
$("tkClose").onclick=()=>$("ticket").close();
$("tkReviewBtn").onclick=async()=>{
 const body={kind:tk.kind,symbol:tk.symbol,side:$("tkSide").value,qty:+$("tkQty").value,order_type:$("tkType").value,price:$("tkPrice").value===""?null:+$("tkPrice").value,product:$("tkProd").value,broker:$("tkBrokerL").hidden?"":$("tkBroker").value,tf:tf||"",expiry:tk.expiry,strike:tk.strike,right:tk.right};
 $("tkMsg").textContent="Checking the order…";
 const d=await post("/api/orders/preview",body);
 if(d.error){$("tkMsg").textContent=d.error;return}
 tkToken=d.token;$("tkReview").hidden=false;
 $("tkReview").innerHTML=d.lines.map(l=>`<div><span>${esc(l[0])}</span><b>${esc(l[1])}</b></div>`).join("")+d.warnings.map(w=>`<p class="warn">${esc(w)}</p>`).join("");
 $("tkConfirm").hidden=false;$("tkPinL").hidden=d.mode!=="live";$("tkPlace").hidden=false;$("tkReviewBtn").hidden=true;
 $("tkMsg").textContent="Check the details, type YES and place the order. This review is valid for 90 seconds."};
$("tkPlace").onclick=async()=>{const b=$("tkPlace");b.disabled=true;
 try{const d=await post("/api/orders/confirm",{token:tkToken,confirm:$("tkYes").value.trim(),pin:$("tkPin").value});
  if(d.error){$("tkMsg").textContent=d.error;return}
  $("tkMsg").textContent=`Order sent. ID ${d.order.id}, status ${d.order.status}.`+(d.order.sl?" Place the stop-loss from the Orders tab once it is filled.":"");
  tkToken=null;$("tkPlace").hidden=true;$("tkConfirm").hidden=true;$("tkYes").value="";$("tkPin").value="";loadOrders()}
 finally{b.disabled=false}};
$("body").onclick=e=>{const b=e.target.closest("[data-trade]");if(!b||!data)return;const r=data.rows.find(x=>x.symbol===b.dataset.trade);if(!r)return;
 const side=r.signal.endsWith("SELL")?"SELL":"BUY";
 openTicket(mode==="futures"?{kind:"future",symbol:r.symbol,label:r.symbol+" futures, nearest month",side,price:r.close}:{kind:"equity",symbol:r.symbol,label:r.symbol,side,price:r.close})};
$("otable").onclick=e=>{const b=e.target.closest(".ob");if(!b||!lastChain)return;
 openTicket({kind:"option",symbol:lastChain.symbol,expiry:lastChain.expiry,strike:+b.dataset.strike,right:b.dataset.right,side:b.dataset.side,price:+b.dataset.ltp,label:`${lastChain.symbol} ${+b.dataset.strike} ${b.dataset.right} ${lastChain.expiry}`})};
$("chk").onclick=async()=>{$("chkMsg").textContent="Checking…";const bs=(tradeInfo.brokers||[]).length?tradeInfo.brokers:[""];const out=[];
 try{for(const b of bs){out.push((await (await fetch("/api/trading/check?which="+b)).json()).message)}$("chkMsg").textContent=out.join(" ")}catch(e){$("chkMsg").textContent="Cannot reach the server."}};
const cancelable=new Set(["open","trigger pending","open pending","validation pending","put order req received","modified","after market order req received"]);
async function loadOrders(){try{renderOrders(await (await fetch("/api/orders")).json())}catch(e){}}
function renderOrders(d){const t=$("ordTable");
 if(!d.orders.length){t.innerHTML="";$("ordNote").textContent="No orders have been placed from this dashboard today.";return}
 $("ordNote").textContent=(d.mode==="live"?"Live orders, with status copied from Angel One.":"Paper orders, simulated.")+" A stop-loss can be placed once its entry order shows complete.";
 t.innerHTML=`<thead><tr><th>Time</th><th>Instrument</th><th>Side</th><th>Qty</th><th>Type</th><th>Price</th><th>Status</th><th>Order ID</th><th></th></tr></thead><tbody>`+d.orders.map(o=>{
  const act=[];
  if(cancelable.has(o.status))act.push(`<button class="mini" data-cancel="${esc(o.id)}">Cancel</button>`);
  if(!o.is_exit&&(o.kind==="equity"||o.kind==="future")&&(o.sl||o.risk)&&!o.sl_order_id&&o.status==="complete")act.push(`<button class="mini" data-protect="${esc(o.id)}">Place stop-loss${o.kind==="future"?"":" "+f2(o.sl)}</button>`);
  const price=o.order_type==="LIMIT"?f2(o.price):o.is_exit?(o.note||""):o.avg_price?f2(o.avg_price):"Market";
  return `<tr><td>${o.time}</td><td>${esc(o.label)}${o.is_exit?" (stop-loss)":""}${o.broker&&o.broker!=="paper"?`<span class="tag">${esc((tradeInfo.broker_names||{})[o.broker]||o.broker)}</span>`:""}</td><td>${o.side}</td><td>${o.qty}</td><td>${o.order_type.replace("_"," ").toLowerCase()}</td><td>${price}</td><td title="${esc(o.note||"")}">${esc(o.status)}${o.sl_order_id?", SL placed":""}</td><td>${esc(o.id)}</td><td>${act.join(" ")}</td></tr>`}).join("")+"</tbody>"}
$("ordTable").onclick=async e=>{const c=e.target.closest("[data-cancel]"),p=e.target.closest("[data-protect]");if(!c&&!p)return;
 if(!confirm(c?"Cancel this order?":"Place the stop-loss order now?"))return;
 const live=tradeInfo.mode==="live",pin=live?prompt("Order PIN"):"";if(live&&pin===null)return;
 const d=await post(c?"/api/orders/cancel":"/api/orders/protect",{order_id:c?c.dataset.cancel:p.dataset.protect,pin});
 if(d.error)alert(d.error);loadOrders()};
setInterval(()=>{if(mode==="orders")loadOrders()},10000);
loadTrading();
setMode(mode);
load();
</script></body></html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
