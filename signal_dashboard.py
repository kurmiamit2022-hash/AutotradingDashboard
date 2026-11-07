"""
CE + EMA + QQE MOD signal dashboard for NSE stocks, futures and options, with sign-in for several people.
Python port of the TradingView indicator "Chandelier Exit + EMA + QQE MOD Signals".

Setup:   pip install -r requirements.txt            (live trading also: pip install -r requirements-live.txt)
Run:     python signal_dashboard.py
Open:    http://127.0.0.1:8000
First sign-in: set ADMIN_USER and ADMIN_PASSWORD, or read the one-time password printed in the console.
Locked out: set ADMIN_RESET=1 with ADMIN_PASSWORD, restart once, sign in, then remove ADMIN_RESET.

Optional env vars:  INTERVAL=5m|15m|1d  REFRESH_SECONDS=300  PORT=8000  HOST=0.0.0.0
  DATABASE_URL=<postgres address, keeps users across redeploys; also set SECRETS_KEY>
  DB_PATH=dashboard.db  ORDER_LOG=orders_log.jsonl  SECRETS_KEY=<Fernet key>  COOKIE_SECURE=1 (when served over https)
  LIVE_TRADING=1 (switch live trading on for the server; an admin must still enable it for each person)
  MAX_ORDER_VALUE, MAX_QTY, MAX_ORDERS_PER_DAY, MAX_FUT_LOTS, MAX_FUT_VALUE, ALLOW_OPTION_SELLING  (default limits per person)
Optional file:      symbols.txt  (one NSE symbol per line, e.g. RELIANCE) overrides the default stock list.

Orders are simulated ("paper") unless live trading is on for the server and enabled for that person.
Back up the database and the secret.key file together. Without the key, saved broker details cannot be read.
"""
import csv
import hashlib
import hmac
import json
import sqlite3
import os
import queue
import re
import secrets
import threading
import time
from datetime import datetime, timedelta, time as dtime

import numpy as np
import pandas as pd
import requests
import uvicorn
from fastapi import Body, FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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

PERIODS = {"5m": "30d", "15m": "60d", "1d": "2y"}
STEP = {"5m": pd.Timedelta(minutes=5), "15m": pd.Timedelta(minutes=15), "1h": pd.Timedelta(hours=1)}
IST = "Asia/Kolkata"
MIN_BARS = 150

DEFAULT_SYMBOLS = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT",
    "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ALKEM", "AMBER",
    "AMBUJACEM", "ANANDRATHI", "ANGELONE", "APLAPOLLO", "APOLLOHOSP",
    "ASHOKLEY", "ASIANPAINT", "ASTRAL", "ATHERENERG", "AUBANK",
    "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL",
    "BHARATFORG", "BHARTIARTL", "BHEL", "BIOCON", "BLUESTARCO",
    "BOSCHLTD", "BPCL", "BRITANNIA", "BSE", "CAMS",
    "CANBK", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA",
    "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR",
    "COROMANDEL", "CROMPTON", "CUB", "CUMMINSIND", "DABUR",
    "DATAPATTNS", "DELHIVERY", "DIVISLAB", "DIXON", "DLF",
    "DMART", "DRREDDY", "EICHERMOT", "ENRIN", "ETERNAL",
    "FEDERALBNK", "FORCEMOT", "FORTIS", "GAIL", "GLENMARK",
    "GMRAIRPORT", "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM",
    "GVT&D", "HAL", "HAVELLS", "HCLTECH", "HDFCAMC",
    "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDPETRO",
    "HINDUNILVR", "HINDZINC", "HYUNDAI", "ICICIBANK", "ICICIGI",
    "ICICILIFE", "IDEA", "IDFCFIRSTB", "IEX", "INDHOTEL",
    "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY",
    "INOXWIND", "IOC", "IREDA", "IRFC", "ITC",
    "JINDALSTEL", "JIOFIN", "JSWENERGY", "JSWSTEEL", "JUBLFOOD",
    "KALYANKJIL", "KARURVYSYA", "KAYNES", "KEI", "KFINTECH",
    "KOTAKBANK", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI",
    "LODHA", "LT", "LTF", "LTM", "LUPIN",
    "M&M", "MAHABANK", "MANAPPURAM", "MANKIND", "MARICO",
    "MARUTI", "MAXHEALTH", "MAZDOCK", "MCX", "MFSL",
    "MOTHERSON", "MOTILALOFS", "MPHASIS", "MUTHOOTFIN", "NAM-INDIA",
    "NATIONALUM", "NAUKRI", "NAVINFLUOR", "NBCC", "NESTLEIND",
    "NHPC", "NMDC", "NTPC", "NYKAA", "OBEROIRLTY",
    "OFSS", "OIL", "ONGC", "PAGEIND", "PATANJALI",
    "PAYTM", "PERSISTENT", "PETRONET", "PFC", "PGEL",
    "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING",
    "POLICYBZR", "POLYCAB", "POWERGRID", "POWERINDIA", "PREMIERENE",
    "PRESTIGE", "RADICO", "RBLBANK", "RECLTD", "RELIANCE",
    "RVNL", "SAGILITY", "SAIL", "SBICARD", "SBILIFE",
    "SBIN", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SOLARINDS",
    "SONACOMS", "SRF", "SUNPHARMA", "SUPREMEIND", "SUZLON",
    "SWIGGY", "SYRMA", "TATACONSUM", "TATAELXSI", "TATAPOWER",
    "TATASTEEL", "TCS", "TECHM", "TIINDIA", "TITAN",
    "TMPV", "TORNTPHARM", "TRENT", "TVSMOTOR", "UJJIVANSFB",
    "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL",
    "VBL", "VEDL", "VMM", "VOLTAS", "WAAREEENER",
    "WIPRO", "YESBANK", "ZYDUSLIFE",
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


# Sector for each symbol (NSE-style groupings). Anything not listed falls under "Others".
_SECTOR_LISTS = {
    "Banking": "AUBANK AXISBANK BANDHANBNK BANKBARODA BANKINDIA CANBK CUB FEDERALBNK HDFCBANK ICICIBANK IDFCFIRSTB INDIANB INDUSINDBK KARURVYSYA KOTAKBANK MAHABANK PNB RBLBANK SBIN UJJIVANSFB UNIONBANK YESBANK",
    "Financial Services": "360ONE ABCAPITAL ANANDRATHI ANGELONE BAJAJFINSV BAJFINANCE BSE CAMS CDSL CHOLAFIN HDFCAMC HDFCLIFE ICICIGI ICICILIFE IEX IRFC IREDA JIOFIN KFINTECH LICHSGFIN LICI LTF MANAPPURAM MCX MFSL MOTILALOFS MUTHOOTFIN NAM-INDIA PAYTM PFC PNBHOUSING POLICYBZR RECLTD SBICARD SBILIFE SHRIRAMFIN",
    "IT": "COFORGE HCLTECH INFY KPITTECH LTM MPHASIS OFSS PERSISTENT SAGILITY TATAELXSI TCS TECHM WIPRO",
    "Auto": "ASHOKLEY ATHERENERG BAJAJ-AUTO BHARATFORG BOSCHLTD EICHERMOT FORCEMOT HEROMOTOCO HYUNDAI M&M MARUTI MOTHERSON SONACOMS TIINDIA TMPV TVSMOTOR UNOMINDA",
    "Pharma & Healthcare": "ALKEM APOLLOHOSP AUROPHARMA BIOCON CIPLA DIVISLAB DRREDDY FORTIS GLENMARK LAURUSLABS LUPIN MANKIND MAXHEALTH SUNPHARMA TORNTPHARM ZYDUSLIFE",
    "FMCG": "BRITANNIA COLPAL DABUR GODFRYPHLP GODREJCP HINDUNILVR ITC MARICO NESTLEIND PATANJALI RADICO TATACONSUM UNITDSPR VBL",
    "Metals & Mining": "COALINDIA HINDALCO HINDZINC JINDALSTEL JSWSTEEL NATIONALUM NMDC SAIL TATASTEEL VEDL",
    "Oil, Gas & Energy": "BPCL GAIL HINDPETRO IOC OIL ONGC PETRONET RELIANCE",
    "Power": "ADANIENSOL ADANIGREEN ADANIPOWER INOXWIND JSWENERGY NHPC NTPC POWERGRID PREMIERENE SUZLON TATAPOWER WAAREEENER",
    "Capital Goods & Defence": "ABB BDL BEL BHEL CGPOWER COCHINSHIP CUMMINSIND DATAPATTNS ENRIN GVT&D HAL MAZDOCK POWERINDIA SIEMENS SOLARINDS",
    "Consumer Durables": "AMBER BLUESTARCO CROMPTON DIXON HAVELLS KAYNES KEI PGEL POLYCAB SYRMA VOLTAS",
    "Retail & Consumer": "DMART ETERNAL INDHOTEL JUBLFOOD KALYANKJIL NAUKRI NYKAA PAGEIND SWIGGY TITAN TRENT VMM",
    "Infra & Logistics": "ADANIENT ADANIPORTS CONCOR DELHIVERY GMRAIRPORT INDIGO LT NBCC RVNL",
    "Telecom": "BHARTIARTL IDEA INDUSTOWER",
    "Cement & Building": "AMBUJACEM APLAPOLLO ASTRAL GRASIM SHREECEM SUPREMEIND ULTRACEMCO",
    "Chemicals & Paints": "ASIANPAINT COROMANDEL NAVINFLUOR PIDILITIND PIIND SRF UPL",
    "Realty": "DLF GODREJPROP LODHA OBEROIRLTY PHOENIXLTD PRESTIGE",
}
SECTOR_OF = {sym: sec for sec, names in _SECTOR_LISTS.items() for sym in names.split()}


def sector_of(symbol):
    return "Indices" if symbol in INDEX_YF else SECTOR_OF.get(symbol, "Others")



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


def eval_signals(symbol, df, interval, idxs, buy, c, hi, lo, atr, ls, ss):
    """Every signal in the loaded candles with what happened next: which targets were reached before the stop-loss.
    Same rules as the live trade tracking: entry at the signal candle's close, stop-loss checked before targets in a candle."""
    out = []
    sector = sector_of(symbol)
    for j in idxs:
        j = int(j)
        side = 1 if buy[j] else -1
        entry = float(c[j])
        raw = ls[j] if side == 1 else ss[j]
        risk = max(MIN_RISK_ATR * atr[j], min(abs(entry - raw), MAX_RISK_ATR * atr[j]))
        sl = entry - side * risk
        tps = [entry + side * risk * r for r in R_MULTIPLES]
        h, l = hi[j + 1:], lo[j + 1:]
        slm = (l <= sl) if side == 1 else (h >= sl)
        sl_i = int(slm.argmax()) if slm.size and slm.any() else None
        hits = 0
        for t in tps:
            m = (h >= t) if side == 1 else (l <= t)
            if m.size and m.any():
                ti = int(m.argmax())
                if sl_i is None or ti < sl_i:
                    hits += 1
        ts = df.index[j]
        out.append({"symbol": symbol, "sector": sector, "side": "BUY" if side == 1 else "SELL", "t": int(ts.timestamp()),
                    "time": fmt_time(ts, interval), "hour": int(ts.hour) if interval != "1d" else None,
                    "entry": round(entry, 2), "sl": round(float(sl), 2), "hits": hits, "sl_hit": sl_i is not None})
    return out


def analyse(symbol, df, interval):
    df = df.dropna(subset=["Close", "High", "Low"]).copy()
    if df.empty:
        return None
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    chg = None  # change since the previous session's close, using the latest price (even a candle that is still forming)
    prev_close = None
    try:
        cl = df["Close"].to_numpy(float)
        if interval == "1d":
            if len(cl) >= 2 and cl[-2] > 0:
                chg = (cl[-1] / cl[-2] - 1) * 100
                prev_close = float(cl[-2])
        else:
            days = np.array([t.date() for t in df.index])
            before = np.nonzero(days != days[-1])[0]
            if len(before) and cl[before[-1]] > 0:
                chg = (cl[-1] / cl[before[-1]] - 1) * 100
                prev_close = float(cl[before[-1]])
    except Exception:
        chg = None
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

    hist = eval_signals(symbol, df, interval, sig_idx, buy, c, hi, lo, atr, ls, ss)

    base = {"symbol": symbol, "hist": hist, "type": "index" if symbol in INDEX_YF else "stock", "close": round(float(c[-1]), 2), "signal": "–", "entry": None, "sl": None,
            "sector": sector_of(symbol), "chg": None if chg is None else round(float(chg), 2),
            "prev_close": None if prev_close is None else round(prev_close, 2),
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
HIST = {tf: [] for tf in INTERVALS}   # every past signal per timeframe, with its outcome
STATES = {tf: {"rows": [], "updated": None, "errors": [], "refreshing": False, "checked": 0.0, "progress": None} for tf in INTERVALS}
LOCK = threading.Lock()


YF_BATCH = int(os.getenv("YF_BATCH", "30"))   # symbols per Yahoo request; one giant request is often throttled


def _one(data, ticker, n_tickers):
    """The candles of one ticker from a yfinance download (its shape depends on how many tickers were asked for)."""
    cols = data.columns
    if getattr(cols, "nlevels", 1) > 1:
        return data[ticker]
    return data if n_tickers == 1 else None


def fetch_all(interval, progress=None):
    """Download in small batches, retry a failed batch once, and report partial results as they arrive."""
    import yfinance as yf  # imported lazily so the module loads without it

    rows, errors, hist = [], [], []
    for i in range(0, len(UNIVERSE), YF_BATCH):
        names = UNIVERSE[i:i + YF_BATCH]
        tickers = [yf_ticker(n) for n in names]
        data = None
        for attempt in range(2):
            try:
                data = yf.download(tickers, period=PERIODS[interval], interval=interval, group_by="ticker",
                                   auto_adjust=False, threads=True, progress=False)
            except Exception as e:
                data = None
                errors.append(f"batch {i // YF_BATCH + 1}: {e}")
            if data is not None and not data.empty:
                break
            time.sleep(3 * (attempt + 1))
        if data is None or data.empty:
            errors.append(f"batch {i // YF_BATCH + 1}: Yahoo returned nothing for {names[0]}…{names[-1]}")
        else:
            for sname, t in zip(names, tickers):
                try:
                    df = _one(data, t, len(tickers))
                    row = analyse(sname, df.dropna(how="all"), interval) if df is not None else None
                    if row:
                        hist.extend(row.pop("hist", []))
                        rows.append(row)
                    else:
                        errors.append(f"{sname}: not enough data")
                except Exception as e:  # keep going if one symbol fails
                    errors.append(f"{sname}: {e}")
        if progress:
            progress(list(rows), list(errors), min(i + YF_BATCH, len(UNIVERSE)))
        time.sleep(0.5)
    return rows, errors, hist


def refresh(interval):
    st = STATES[interval]
    with LOCK:
        if st["refreshing"]:
            return
        st["refreshing"] = True
        st["progress"] = [0, len(UNIVERSE)]

    def partial(rows, errors, done):
        with LOCK:
            st["progress"] = [done, len(UNIVERSE)]
            if not st["rows"] or not st["updated"]:  # first scan: show results as they arrive
                st.update(rows=rows, errors=errors)

    try:
        rows, errors, hist = fetch_all(interval, partial)
        with LOCK:
            if rows:  # never replace good data with an empty result
                st.update(rows=rows, errors=errors, updated=datetime.now().strftime("%d %b %H:%M:%S"))
                HIST[interval] = hist
            else:
                st["errors"] = errors or ["Yahoo returned no data."]
        if rows:
            try:
                check_alerts(interval, rows)
            except Exception as e:
                print("alert check failed:", e, flush=True)
    except Exception as e:
        with LOCK:
            st["errors"] = [f"Data fetch failed: {e}"]
    finally:
        with LOCK:
            st["refreshing"] = False
            st["progress"] = None
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


# ── Accounts, sign-in and order placement ──
# Several people can sign in. Each has their own broker accounts, order PIN and limits.
# Everything is simulated ("paper") unless live trading is switched on for the server AND enabled for that person by an admin.
# Brokers accept API orders only from a static IP registered to that client. Check each broker's sharing rules first.
LIVE_REQUESTED = os.getenv("LIVE_TRADING", "0") == "1"
BROKER_PREF = os.getenv("BROKER", "").strip().lower()               # preferred broker for people who set up both
DB_PATH = os.getenv("DB_PATH", "dashboard.db")
SESSION_HOURS = float(os.getenv("SESSION_HOURS", "12"))
ALLOW_SIGNUP = os.getenv("ALLOW_SIGNUP", "1") == "1"                # anyone can create an account (paper mode only until an admin enables live)
SIGNUP_CODE = os.getenv("SIGNUP_CODE", "").strip()                   # optional invite code required to sign up
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "0") == "1"              # set to 1 when the site is served over https
ORDER_LOG = os.getenv("ORDER_LOG", "orders_log.jsonl")
# Set DATABASE_URL (a Postgres address, e.g. a free Neon or Supabase database) and users, broker details and orders live there
# instead of on the server's disk, so they survive every redeploy. Needs psycopg2-binary.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]
USE_PG = bool(DATABASE_URL)
MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
DHAN_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
DHAN_BASE = "https://api.dhan.co/v2"
DEFAULT_CAPS = {                                                    # an admin can override these for each person
    "max_order_value": float(os.getenv("MAX_ORDER_VALUE", "50000")),       # rupees per stock or option-buy order
    "max_qty": int(os.getenv("MAX_QTY", "500")),                           # shares per order, or lots for option buys
    "max_orders": int(os.getenv("MAX_ORDERS_PER_DAY", "10")),
    "max_fut_lots": int(os.getenv("MAX_FUT_LOTS", "1")),                   # lots per futures order or option sell
    "max_fut_value": float(os.getenv("MAX_FUT_VALUE", "2500000")),         # notional rupees per futures order or option sell
    "allow_option_selling": os.getenv("ALLOW_OPTION_SELLING", "1") == "1",
}
EQ_PRICE_BAND, OPT_PRICE_BAND = 5.0, 30.0                           # limit price may differ from last price by this %
BROKER_NAMES = {"angel": "Angel One", "dhan": "Dhan", "paper": "Paper"}
BROKER_FIELDS = {"angel": [("api_key", "API key"), ("client_id", "Client ID"), ("pin", "Login PIN"), ("totp_secret", "TOTP secret")],
                 "dhan": [("client_id", "Client ID"), ("access_token", "Access token")]}
ORDERS, PENDING, PIN_STATE, LOGIN_FAILS, SYNC, ACCOUNTS = [], {}, {}, {}, {}, {}
SIGNUP_LOG = {}
DEFAULT_ADMIN_PW = "Admin@12345"   # used only when ADMIN_PASSWORD is not set; the sign-in page warns until it is changed
DEFAULT_PW_USERS = set()           # ids of users who signed in with that default password
ORDER_LOCK = threading.Lock()
DB_LOCK = threading.Lock()


def ist_now():
    return pd.Timestamp.now(tz=IST)


def market_open_now():
    n = ist_now()
    return n.weekday() < 5 and dtime(9, 15) <= n.time() <= dtime(15, 30)


def append_log(o):
    if USE_PG:
        try:
            x("INSERT INTO order_log(day, rec) VALUES(?,?)", (o.get("date") or ist_now().strftime("%Y-%m-%d"), json.dumps(o)))
        except Exception as e:
            print("order log write failed:", e, flush=True)
        return
    try:
        with open(ORDER_LOG, "a") as f:
            f.write(json.dumps(o) + "\n")
    except OSError:
        pass


def load_orders():
    today = ist_now().strftime("%Y-%m-%d")
    latest = {}
    try:
        lines = [r["rec"] for r in q("SELECT rec FROM order_log WHERE day=? ORDER BY n", (today,))] if USE_PG else open(ORDER_LOG)
        if True:
            for ln in lines:
                try:
                    o = json.loads(ln)
                except ValueError:
                    continue
                if o.get("date") == today:
                    o.setdefault("broker", "angel" if o.get("mode") == "live" else "paper")
                    o.setdefault("user", 1)
                    latest[o["id"]] = o
    except OSError:
        pass
    ORDERS.extend(latest.values())


if not USE_PG:
    load_orders()  # with Postgres this happens after the tables exist (init_db)


# ── database ──
def _con():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    return con


_PG = {"con": None}


def _pg_con():
    import psycopg2  # pip install psycopg2-binary
    c = _PG["con"]
    if c is None or c.closed:
        c = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        c.autocommit = True
        _PG["con"] = c
    return c


def _pg_sql(sql):
    sql = sql.replace("?", "%s")
    return sql.replace("username=%s", "lower(username)=lower(%s)")  # names are not case sensitive


def _pg_run(sql, args, fetch):
    import psycopg2
    import psycopg2.extras
    last = None
    for _ in range(2):  # one retry when the connection has dropped
        try:
            cur = _pg_con().cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(_pg_sql(sql), tuple(args))
            if fetch == "all":
                return [dict(r) for r in cur.fetchall()]
            if fetch == "id":
                return cur.fetchone()["id"]
            return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last = e
            try:
                _PG["con"].close()
            except Exception:
                pass
            _PG["con"] = None
    raise last


ALERT_DDL = [
    "CREATE TABLE IF NOT EXISTS alerts(user_id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0, telegram_chat TEXT, ntfy_topic TEXT, "
    "tfs TEXT NOT NULL DEFAULT '15m', sides TEXT NOT NULL DEFAULT 'BUY,SELL', sectors TEXT NOT NULL DEFAULT '', updated TEXT)",
    "CREATE TABLE IF NOT EXISTS alert_seen(k TEXT PRIMARY KEY, ts TEXT NOT NULL)",
]


class DuplicateUser(Exception):
    pass


def q(sql, args=()):
    with DB_LOCK:
        if USE_PG:
            return _pg_run(sql, args, "all")
        con = _con()
        try:
            return [dict(r) for r in con.execute(sql, args).fetchall()]
        finally:
            con.close()


def x(sql, args=()):
    with DB_LOCK:
        if USE_PG:
            import psycopg2
            try:
                if sql.lstrip().upper().startswith("INSERT INTO USERS"):
                    return _pg_run(sql + " RETURNING id", args, "id")
                return _pg_run(sql, args, None)
            except psycopg2.errors.UniqueViolation:
                raise DuplicateUser()
        con = _con()
        try:
            cur = con.execute(sql, args)
            con.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            raise DuplicateUser()
        finally:
            con.close()


def init_db():
    if USE_PG:
        with DB_LOCK:
            c = _pg_con().cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY, username TEXT NOT NULL,
              pw_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'user', active INTEGER NOT NULL DEFAULT 1,
              live_allowed INTEGER NOT NULL DEFAULT 0, pin_hash TEXT, caps TEXT, created TEXT, recovery_hash TEXT)""")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS users_name ON users(lower(username))")
            c.execute("CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, csrf TEXT NOT NULL, expires DOUBLE PRECISION NOT NULL)")
            c.execute("""CREATE TABLE IF NOT EXISTS creds(user_id INTEGER NOT NULL, broker TEXT NOT NULL, blob TEXT NOT NULL, hint TEXT, updated TEXT,
              PRIMARY KEY(user_id, broker))""")
            for ddl in ALERT_DDL:
                c.execute(ddl)
            c.execute("DELETE FROM alert_seen WHERE ts < %s", ((ist_now() - timedelta(days=7)).isoformat(),))
            c.execute("CREATE TABLE IF NOT EXISTS order_log(n SERIAL PRIMARY KEY, day TEXT NOT NULL, rec TEXT NOT NULL)")
            c.execute("CREATE INDEX IF NOT EXISTS order_log_day ON order_log(day)")
            c.execute("DELETE FROM order_log WHERE day < %s", ((ist_now() - timedelta(days=14)).strftime("%Y-%m-%d"),))
        ORDERS.clear()
        load_orders()
        ensure_admin()
        return
    new = not os.path.exists(DB_PATH)
    with DB_LOCK:
        con = _con()
        try:
            con.executescript("""
            CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE COLLATE NOCASE NOT NULL,
              pw_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'user', active INTEGER NOT NULL DEFAULT 1,
              live_allowed INTEGER NOT NULL DEFAULT 0, pin_hash TEXT, caps TEXT, created TEXT);
            CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, csrf TEXT NOT NULL, expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS creds(user_id INTEGER NOT NULL, broker TEXT NOT NULL, blob TEXT NOT NULL, hint TEXT, updated TEXT,
              PRIMARY KEY(user_id, broker));""")
            for ddl in ALERT_DDL:
                con.execute(ddl)
            con.execute("DELETE FROM alert_seen WHERE ts < ?", ((ist_now() - timedelta(days=7)).isoformat(),))
            if "recovery_hash" not in [r[1] for r in con.execute("PRAGMA table_info(users)")]:
                con.execute("ALTER TABLE users ADD COLUMN recovery_hash TEXT")
            con.commit()
        finally:
            con.close()
    if new:
        try:
            os.chmod(DB_PATH, 0o600)
        except OSError:
            pass
    ensure_admin()


def ensure_admin():
    """The first admin comes from ADMIN_USER / ADMIN_PASSWORD. Without them a random password is printed once."""
    name = os.getenv("ADMIN_USER", "admin")
    if q("SELECT COUNT(*) AS c FROM users")[0]["c"]:
        # Locked out? Set ADMIN_RESET=1 with ADMIN_PASSWORD, restart once, sign in, then remove ADMIN_RESET.
        if os.getenv("ADMIN_RESET") == "1" and os.getenv("ADMIN_PASSWORD"):
            row = q("SELECT id FROM users WHERE username=?", (name,))
            if row:
                x("UPDATE users SET pw_hash=?, role='admin', active=1 WHERE id=?", (hash_secret(os.environ["ADMIN_PASSWORD"]), row[0]["id"]))
                x("DELETE FROM sessions WHERE user_id=?", (row[0]["id"],))
            else:
                x("INSERT INTO users(username, pw_hash, role, active, live_allowed, created) VALUES(?,?,?,?,?,?)",
                  (name, hash_secret(os.environ["ADMIN_PASSWORD"]), "admin", 1, 0, ist_now().isoformat()))
            print(f"\n  Admin password reset for '{name}'. Remove ADMIN_RESET now.\n", flush=True)
        return
    pw = os.getenv("ADMIN_PASSWORD") or os.getenv("DASH_PASSWORD") or ""
    generated = not pw
    if generated and not LIVE_REQUESTED:
        pw = DEFAULT_ADMIN_PW
        generated = False
        print(f"\n  No ADMIN_PASSWORD set, so the default admin sign-in is in use.\n    username: {name}\n    password: {DEFAULT_ADMIN_PW}\n"
              "  Change it under Account right after signing in. Set ADMIN_PASSWORD to choose your own from the start.\n", flush=True)
    elif generated:
        pw = secrets.token_urlsafe(12)
    x("INSERT INTO users(username, pw_hash, role, active, live_allowed, created) VALUES(?,?,?,?,?,?)",
      (name, hash_secret(pw), "admin", 1, 0, ist_now().isoformat()))
    if generated:
        print(f"\n  First sign-in\n    username: {name}\n    password: {pw}\n  This password is shown once. Change it under Account.\n", flush=True)


# ── passwords, sessions and stored broker details ──
def hash_secret(secret):
    salt = os.urandom(16)
    h = hashlib.scrypt(secret.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)
    return "scrypt$" + salt.hex() + "$" + h.hex()


def check_secret(secret, stored):
    try:
        _, salt, h = stored.split("$")
        calc = hashlib.scrypt(secret.encode(), salt=bytes.fromhex(salt), n=2 ** 14, r=8, p=1, dklen=32)
        return hmac.compare_digest(calc.hex(), h)
    except Exception:
        return False


DUMMY_HASH = hash_secret("not-a-real-password")


def sha(t):
    return hashlib.sha256(t.encode()).hexdigest()


def new_session(uid):
    token = secrets.token_urlsafe(32)
    x("INSERT INTO sessions(token_hash, user_id, csrf, expires) VALUES(?,?,?,?)",
      (sha(token), uid, secrets.token_urlsafe(24), time.time() + SESSION_HOURS * 3600))
    return token


def build_user(row):
    caps = dict(DEFAULT_CAPS)
    try:
        caps.update(json.loads(row.get("caps") or "{}"))
    except ValueError:
        pass
    have = {r["broker"] for r in q("SELECT broker FROM creds WHERE user_id=?", (row["id"],))}
    brokers = [b for b in ("angel", "dhan") if b in have]
    default = BROKER_PREF if BROKER_PREF in brokers else (brokers[0] if brokers else "angel")
    return {"id": row["id"], "username": row["username"], "role": row["role"], "active": bool(row["active"]),
            "live_allowed": bool(row["live_allowed"]), "pin_hash": row.get("pin_hash"), "recovery_set": bool(row.get("recovery_hash")), "pin_set": bool(row.get("pin_hash")),
            "caps": caps, "brokers": brokers, "default_broker": default, "csrf": row.get("csrf")}


def session_user(request):
    tok = request.cookies.get("sid")
    if not tok:
        return None
    rows = q("SELECT s.csrf, s.expires, u.* FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash=?", (sha(tok),))
    if not rows:
        return None
    r = rows[0]
    if r["expires"] < time.time() or not r["active"]:
        x("DELETE FROM sessions WHERE token_hash=?", (sha(tok),))
        return None
    return build_user(r)


def public_user(u):
    return {k: u[k] for k in ("id", "username", "role", "active", "live_allowed", "pin_set", "recovery_set", "brokers", "caps")}


def user_mode(u):
    if not LIVE_REQUESTED:
        return "paper", "Paper mode: orders are simulated. Live trading is switched off on this server."
    if not u["live_allowed"]:
        return "paper", "Paper mode: live trading is not enabled for your account. Ask the admin."
    if not u["pin_set"]:
        return "paper", "Paper mode: set your order PIN under Account to enable live trading."
    if not u["brokers"]:
        return "paper", "Paper mode: add your broker details under Account to enable live trading."
    return "live", ""


RECOVERY_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def new_recovery_key():
    k = "".join(secrets.choice(RECOVERY_CHARS) for _ in range(16))
    return "-".join(k[i:i + 4] for i in range(0, 16, 4))


def norm_key(k):
    return re.sub(r"[^A-Z0-9]", "", str(k).upper())


def set_recovery(uid):
    key = new_recovery_key()
    x("UPDATE users SET recovery_hash=? WHERE id=?", (hash_secret(norm_key(key)), uid))
    return key


def valid_new_password(name, pw):
    if len(pw) < 10:
        return "Use at least 10 characters."
    if len(pw) > 200:
        return "Use at most 200 characters."
    if pw.lower() == name.lower():
        return "The password cannot be the same as the username."
    if pw == DEFAULT_ADMIN_PW:
        return "Choose a password other than the default one."
    return None


def client_ip(request):
    return request.client.host if request.client else "?"


def session_cookie(resp, request, uid):
    resp.set_cookie("sid", new_session(uid), httponly=True, samesite="lax", path="/",
                    secure=COOKIE_SECURE or request.url.scheme == "https", max_age=int(SESSION_HOURS * 3600))


_FERNET = {}


def fernet():
    """Broker details are encrypted with SECRETS_KEY, or with a key file kept next to the database."""
    if "f" not in _FERNET:
        from cryptography.fernet import Fernet  # pip install cryptography
        key = os.getenv("SECRETS_KEY", "").strip()
        if not key and USE_PG:
            raise RuntimeError("Set SECRETS_KEY (a Fernet key) in the environment so broker details can be stored safely. "
                               "Make one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")
        if not key:
            path = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "secret.key")
            if os.path.exists(path):
                key = open(path).read().strip()
            else:
                key = Fernet.generate_key().decode()
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w") as f:
                    f.write(key)
        _FERNET["f"] = Fernet(key.encode())
    return _FERNET["f"]


def save_creds(uid, broker_id, fields):
    blob = fernet().encrypt(json.dumps(fields).encode()).decode()
    hint = "…" + fields["client_id"][-4:]
    x("INSERT INTO creds(user_id, broker, blob, hint, updated) VALUES(?,?,?,?,?) "
      "ON CONFLICT(user_id, broker) DO UPDATE SET blob=excluded.blob, hint=excluded.hint, updated=excluded.updated",
      (uid, broker_id, blob, hint, ist_now().isoformat()))
    ACCOUNTS.pop((uid, broker_id), None)


def delete_creds(uid, broker_id):
    x("DELETE FROM creds WHERE user_id=? AND broker=?", (uid, broker_id))
    ACCOUNTS.pop((uid, broker_id), None)


def get_account(uid, broker_id):
    acct = ACCOUNTS.get((uid, broker_id))
    if acct:
        return acct
    rows = q("SELECT blob FROM creds WHERE user_id=? AND broker=?", (uid, broker_id))
    if not rows:
        raise RuntimeError("Broker details are missing.")
    creds = json.loads(fernet().decrypt(rows[0]["blob"].encode()))
    acct = AngelAccount(creds) if broker_id == "angel" else DhanAccount(creds)
    ACCOUNTS[(uid, broker_id)] = acct
    return acct


class OrderRejected(RuntimeError):
    """The broker answered and refused the order, so no order exists."""


DHAN_PRODUCT = {"INTRADAY": "INTRADAY", "DELIVERY": "CNC", "CARRYFORWARD": "MARGIN"}
DHAN_ORDER_TYPE = {"MARKET": "MARKET", "LIMIT": "LIMIT", "STOPLOSS_MARKET": "STOP_LOSS_MARKET"}
DHAN_STATUS = {"TRADED": "complete", "PENDING": "open", "TRANSIT": "open pending", "PART_TRADED": "open",
               "REJECTED": "rejected", "CANCELLED": "cancelled", "EXPIRED": "expired"}


class AngelAccount:
    """One person's Angel One session. Reads retry once; order writes are never retried, so an order cannot be sent twice."""

    def __init__(self, creds):
        self.c = creds
        self.obj = None
        self.lock = threading.Lock()

    def _login(self):
        from SmartApi import SmartConnect  # pip install smartapi-python pyotp
        import pyotp
        obj = SmartConnect(api_key=self.c["api_key"])
        res = obj.generateSession(self.c["client_id"], self.c["pin"], pyotp.TOTP(self.c["totp_secret"]).now())
        if not res or not res.get("status"):
            raise RuntimeError("Angel One login failed: " + str((res or {}).get("message", "no response")))
        self.obj = obj

    def _read(self, fn, *a):
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

    def _write(self, names, *a):
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

    def book(self):
        out = {}
        for x_ in (self._read("orderBook") or {}).get("data") or []:
            st = str(x_.get("orderstatus") or "").lower()
            out[str(x_.get("orderid"))] = {"status": st, "avg_price": _num(x_.get("averageprice")), "filled": _num(x_.get("filledshares")),
                                           "note": str(x_.get("text") or "") if st == "rejected" else ""}
        return out

    def place(self, o, variety="NORMAL", ordertype=None, trigger=None, side=None, qty=None):
        otype = ordertype or o["order_type"]
        params = {"variety": variety, "tradingsymbol": o["tradingsymbol"], "symboltoken": o["token"],
                  "transactiontype": side or o["side"], "exchange": o["exchange"], "ordertype": otype,
                  "producttype": o["product"], "duration": "DAY", "price": f'{o["price"]:.2f}' if otype == "LIMIT" else "0",
                  "squareoff": "0", "stoploss": "0", "quantity": str(qty or o["qty"])}
        if trigger is not None:
            params["triggerprice"] = f"{trigger:.2f}"
        res = self._write(("placeOrderFullResponse", "placeOrder"), params)
        if isinstance(res, dict):
            if not res.get("status"):
                raise OrderRejected(str(res.get("message") or "The order was not accepted."))
            oid = (res.get("data") or {}).get("orderid")
        else:
            oid = res
        if not oid:
            raise RuntimeError("The broker did not return an order id.")
        return str(oid)

    def cancel(self, o):
        self._write(("cancelOrder",), o["id"], o.get("variety", "NORMAL"))

    def ltp(self, items):
        """Last traded prices for [(exchange, token)]. Returns {(exchange, token): price}."""
        by = {}
        for ex, tok in items:
            by.setdefault(ex, []).append(str(tok))
        out = {}
        for ex, toks in by.items():
            for i in range(0, len(toks), 50):
                res = self._read("getMarketData", "LTP", {ex: toks[i:i + 50]}) or {}
                for it in (res.get("data") or {}).get("fetched") or []:
                    price = _num(it.get("ltp"))
                    if price:
                        out[(str(it.get("exchange") or ex), str(it.get("symbolToken")))] = price
                if len(toks) > 50:
                    time.sleep(1.1)  # Angel One limits market data calls to about one a second
        return out

    def modify_stop(self, o, rec, trigger):
        """Move the trigger of a stop-loss order that is already placed. Safe to repeat: it sets the trigger, it does not add to it."""
        params = {"variety": rec.get("variety", "STOPLOSS"), "orderid": str(rec["id"]), "ordertype": "STOPLOSS_MARKET", "producttype": o["product"],
                  "duration": "DAY", "price": "0", "quantity": str(rec["qty"]), "triggerprice": f"{trigger:.2f}",
                  "tradingsymbol": o["tradingsymbol"], "symboltoken": o["token"], "exchange": o["exchange"]}
        res = self._write(("modifyOrder",), params)
        if isinstance(res, dict) and not res.get("status"):
            raise OrderRejected(str(res.get("message") or "The change was not accepted."))


class DhanAccount:
    """One person's Dhan account."""

    def __init__(self, creds):
        self.c = creds

    def request(self, method, path, body=None, extra=None):
        r = requests.request(method, DHAN_BASE + path, json=body, timeout=15,
                             headers={"access-token": self.c["access_token"], "Content-Type": "application/json", "Accept": "application/json", **(extra or {})})
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

    def book(self):
        res = self.request("GET", "/orders")
        out = {}
        for x_ in res if isinstance(res, list) else []:
            raw = str(x_.get("orderStatus", "")).upper()
            st = DHAN_STATUS.get(raw, raw.lower())
            out[str(x_.get("orderId"))] = {"status": st, "avg_price": _num(x_.get("averageTradedPrice")), "filled": _num(x_.get("filledQty")),
                                           "note": str(x_.get("omsErrorDescription") or "") if st == "rejected" else ""}
        return out

    def place(self, o, variety="NORMAL", ordertype=None, trigger=None, side=None, qty=None):
        ot = ordertype or o["order_type"]
        body = {"dhanClientId": self.c["client_id"], "correlationId": "DB" + secrets.token_hex(8),
                "transactionType": side or o["side"], "exchangeSegment": o["exchange"],
                "productType": DHAN_PRODUCT[o["product"]], "orderType": DHAN_ORDER_TYPE[ot], "validity": "DAY",
                "securityId": str(o["token"]), "quantity": int(qty or o["qty"]), "disclosedQuantity": "",
                "price": float(o["price"]) if ot == "LIMIT" else "",
                "triggerPrice": float(trigger) if trigger is not None else "", "afterMarketOrder": False}
        res = self.request("POST", "/orders", body)
        oid = str((res or {}).get("orderId") or "")
        if not oid:
            raise RuntimeError("Dhan did not return an order id.")
        if str(res.get("orderStatus", "")).upper() == "REJECTED":
            raise OrderRejected("Dhan rejected the order.")
        return oid

    def cancel(self, o):
        self.request("DELETE", f"/orders/{o['id']}")

    def ltp(self, items):
        """Last traded prices for [(exchange segment, security id)]. Returns {(segment, id): price}."""
        by = {}
        for seg, sid in items:
            by.setdefault(seg, []).append(int(sid))
        if not by:
            return {}
        res = self.request("POST", "/marketfeed/ltp", by, extra={"client-id": str(self.c["client_id"])})
        out = {}
        for seg, d in ((res or {}).get("data") or {}).items():
            for sid, v in (d or {}).items():
                price = _num((v or {}).get("last_price"))
                if price:
                    out[(seg, str(sid))] = price
        return out

    def modify_stop(self, o, rec, trigger):
        body = {"dhanClientId": self.c["client_id"], "orderId": str(rec["id"]), "orderType": "STOP_LOSS_MARKET", "legName": "",
                "quantity": int(rec["qty"]), "price": "", "disclosedQuantity": "", "triggerPrice": float(trigger), "validity": "DAY"}
        self.request("PUT", f"/orders/{rec['id']}", body)


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


def count_today(uid, mode):
    with ORDER_LOCK:
        return sum(1 for o in ORDERS if o.get("user") == uid and o["mode"] == mode and not o.get("is_exit"))


def inr(x):
    return "₹" + f"{x:,.2f}"


def make_order(p, u):
    """Check an order request against every limit. Returns (order, error, warnings, lines)."""
    kind = p.get("kind")
    symbol = str(p.get("symbol", "")).upper().strip()
    side = str(p.get("side", "")).upper()
    otype = str(p.get("order_type", "MARKET")).upper()
    product = str(p.get("product", "INTRADAY")).upper()
    tf = p.get("tf") if p.get("tf") in INTERVALS else DEFAULT_INTERVAL
    fail = lambda m: (None, m, [], [])
    mode, _ = user_mode(u)
    caps = u["caps"]
    broker_id = str(p.get("broker") or u["default_broker"]).lower() if mode == "live" else "paper"
    if mode == "live" and broker_id not in u["brokers"]:
        return fail("That broker is not set up on your account.")
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
        raw_qty = p.get("qty")
        if isinstance(raw_qty, bool):
            raise ValueError
        qty = int(float(raw_qty))
        if qty != float(raw_qty):  # 1.5 is not a quantity
            raise ValueError
    except (TypeError, ValueError, OverflowError):
        return fail("Quantity must be a whole number.")
    cap = caps["max_fut_lots"] if (kind == "future" or (kind == "option" and side == "SELL")) else caps["max_qty"]
    if qty < 1 or qty > cap:
        return fail(f"Quantity must be between 1 and {cap}" + (" shares." if kind == "equity" else " lots."))
    if mode == "live" and not market_open_now():
        return fail("The market is closed. Live orders are accepted 9:15 to 15:30 IST, Monday to Friday.")
    if count_today(u["id"], mode) >= caps["max_orders"]:
        return fail(f"Daily limit of {caps['max_orders']} orders reached.")

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
        ref, units, lot, band = live_price(row), qty, 1, EQ_PRICE_BAND
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
        ref, lot, band = live_price(row), near[3], EQ_PRICE_BAND
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
        if side == "SELL" and not caps["allow_option_selling"]:
            return fail("Selling options is switched off for your account.")
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
            raw_price = float(p.get("price"))
            if raw_price != raw_price or raw_price in (float("inf"), float("-inf")):
                raise ValueError
            price = round(round(raw_price / 0.05) * 0.05, 2)
        except (TypeError, ValueError, OverflowError):
            return fail("Enter a limit price.")
        if price <= 0:
            return fail("Enter a limit price.")
        if abs(price / ref - 1) * 100 > band:
            return fail(f"The limit price is more than {band:.0f}% away from the last price {inr(ref)}. Check it for a typo.")
    est = (price or ref) * units
    selling_opt = kind == "option" and side == "SELL"
    exposure = strike * units if selling_opt else None
    if selling_opt:
        if exposure > caps["max_fut_value"]:
            return fail(f"Selling this option puts about {inr(exposure)} of the underlying at stake, above your limit of {inr(caps['max_fut_value'])}.")
    else:
        value_cap = caps["max_fut_value"] if kind == "future" else caps["max_order_value"]
        if est > value_cap:
            return fail(f"This order is worth about {inr(est)}, above your limit of {inr(value_cap)} per order.")
    key = (kind, symbol, side, strike, right)
    now = time.time()
    with ORDER_LOCK:
        if any(o.get("user") == u["id"] and o.get("key") == key and not o.get("is_exit") and now - o["t"] < 60 for o in ORDERS):
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
    auto_sl = bool(p.get("auto_sl")) and kind in ("equity", "future")
    if auto_sl and not (sl if kind == "equity" else risk):
        warnings.append("No signal stop-loss is available for this order, so no automatic stop-loss will be placed.")
        auto_sl = False
    trail = bool(p.get("trail")) and auto_sl
    if kind == "future" and not auto_sl:
        warnings.append("Futures are leveraged and the stop-loss is not automatic. Place it from the Orders tab once the entry fills.")
    if selling_opt:
        warnings.append("Selling an option can lose far more than the premium you receive. It needs margin, and no stop-loss is placed for you. "
                        "If you are closing an option you bought, this is a sell to close.")

    lines = [["Instrument", label], ["Side", side],
             ["Quantity", f"{qty} lot{'s' if qty > 1 else ''} x {lot} = {units} units" if kind != "equity" else f"{qty} shares"],
             ["Order type", otype.title()], ["Price", inr(price) if otype == "LIMIT" else f"Market (last price {inr(ref)})"],
             ["Product", product.title()], ["Estimated value", inr(est)], ["Mode", "LIVE, real money" if mode == "live" else "Paper, simulated"]]
    if auto_sl:
        lines.append(["Automatic stop-loss", "ON: placed for you as soon as the entry fills" + (", and trailed as targets are reached" if trail else "")])
        if mode == "live":
            warnings.append("Automatic stop-loss is switched on. The dashboard server places it for you after the fill, so it must be running. Check the Orders tab and your broker app.")
    if kind == "equity" and sl and not auto_sl:
        lines.append(["Stop-loss from the signal", inr(sl) + " (placed from the Orders tab once filled)"])
    if selling_opt:
        lines.append(["Exposure (strike x units)", inr(exposure)])
    if kind == "future" and risk:
        lines.append(["Stop-loss distance", f"{risk:.2f} points from your fill price" + ("" if auto_sl else " (placed from the Orders tab)")])
    order = {"kind": kind, "symbol": symbol, "label": label, "tradingsymbol": tsym, "token": token, "exchange": exch,
             "side": side, "qty": units, "lots": qty, "order_type": otype, "price": price, "ref": ref, "product": product,
             "expiry": expiry, "strike": strike, "right": right, "sl": sl, "risk": risk, "key": key, "broker": broker_id, "mode": mode, "user": u["id"],
             "auto_sl": auto_sl, "trail": trail}
    return order, None, warnings, lines


def new_record(o, oid, status, variety="NORMAL", is_exit=False, note=""):
    n = ist_now()
    rec = {"id": oid, "t": time.time(), "date": n.strftime("%Y-%m-%d"), "time": n.strftime("%H:%M:%S"), "mode": o["mode"],
           "user": o["user"], "kind": o["kind"], "symbol": o["symbol"], "label": o["label"], "tradingsymbol": o["tradingsymbol"],
           "token": o["token"], "exchange": o["exchange"], "side": o["side"], "qty": o["qty"], "order_type": o["order_type"],
           "price": o["price"], "product": o["product"], "status": status, "avg_price": None, "sl": o.get("sl"),
           "sl_order_id": None, "risk": o.get("risk"), "variety": variety, "broker": o.get("broker", "paper"),
           "auto_sl": bool(o.get("auto_sl")) and not is_exit, "trail": bool(o.get("trail")) and not is_exit,
           "sl_state": None, "trail_level": 0, "trail_fail": 0, "trail_done": False,
           "is_exit": is_exit, "key": o.get("key"), "note": note}
    with ORDER_LOCK:
        ORDERS.append(rec)
    append_log(rec)
    return rec


def paper_id():
    with ORDER_LOCK:
        return f"PAPER-{sum(1 for o in ORDERS if str(o['id']).startswith('PAPER-')) + 1:04d}"


def public(o):
    return {k: v for k, v in o.items() if k not in ("token", "key", "t", "user")}


def pin_error(u, pin, live):
    """Live orders need the person's order PIN. Five wrong tries lock it for five minutes."""
    if not live:
        return None
    if not u["pin_hash"]:
        return "Set your order PIN under Account first."
    st = PIN_STATE.setdefault(u["id"], {"fails": 0, "until": 0.0})
    if time.time() < st["until"]:
        return f"Too many wrong PINs. Try again in {int((st['until'] - time.time()) / 60) + 1} minutes."
    if check_secret(str(pin or ""), u["pin_hash"]):
        st["fails"] = 0
        return None
    st["fails"] += 1
    if st["fails"] >= 5:
        st.update(fails=0, until=time.time() + 300)
    return "Wrong order PIN."


def find_order(oid, uid):
    with ORDER_LOCK:
        return next((o for o in ORDERS if str(o["id"]) == str(oid) and o.get("user") == uid), None)


def sync_status(u):
    """Copy this person's order statuses from their brokers' order books (at most every 5 seconds)."""
    if time.time() - SYNC.get(u["id"], 0) < 5:
        return
    SYNC[u["id"]] = time.time()
    with ORDER_LOCK:
        mine = [o for o in ORDERS if o.get("user") == u["id"] and o["mode"] == "live"]
    for b in {o["broker"] for o in mine}:
        try:
            book = get_account(u["id"], b).book()
        except Exception:
            continue
        with ORDER_LOCK:
            for o in mine:
                info = book.get(str(o["id"])) if o["broker"] == b else None
                if not info:
                    continue
                if info["status"]:
                    o["status"] = info["status"]
                if info["avg_price"]:
                    o["avg_price"] = info["avg_price"]
                o["filled"] = info["filled"]
                if info["note"]:
                    o["note"] = info["note"]


# ── Live prices from a broker, automatic stop-loss and trailing ──
# DATA_BROKER=angel|dhan with that broker's credentials in the environment turns on a live price feed for the dashboard.
# Without it the dashboard uses Yahoo's candle closes, which can lag. Automatic stop-loss and trailing work either way.
DATA_BROKER = os.getenv("DATA_BROKER", "").strip().lower()
FEED_SECONDS = max(5, int(os.getenv("FEED_SECONDS", "10")))
AUTOPILOT_SECONDS = max(5, int(os.getenv("AUTOPILOT_SECONDS", "10")))
LTP_MAX_AGE = 60            # seconds a live price stays usable
LTP_MAX_JUMP = 0.25         # a live price more than 25% from the candle close is treated as bad data
TRAIL_MAX_STEP = 3          # trailing moves the stop to breakeven at 1R, then +1R at 2R, then +2R at 3R
LTP = {}                    # symbol -> {"p": price, "t": epoch seconds}
FEED = {"source": None, "error": None, "count": 0, "t": None, "acct": None}
AUTO = {"last": None, "error": None}
INDEX_FEED = {
    "angel": {"NIFTY": ("NSE", "99926000"), "BANKNIFTY": ("NSE", "99926009"), "FINNIFTY": ("NSE", "99926037"),
              "MIDCPNIFTY": ("NSE", "99926074"), "NIFTYNXT50": ("NSE", "99926013"), "SENSEX": ("BSE", "99919000"), "BANKEX": ("BSE", "99919012")},
    "dhan": {"NIFTY": ("IDX_I", "13"), "BANKNIFTY": ("IDX_I", "25"), "FINNIFTY": ("IDX_I", "27"), "MIDCPNIFTY": ("IDX_I", "442"), "SENSEX": ("IDX_I", "51")},
}


def fresh_ltp(symbol):
    v = LTP.get(symbol)
    return v["p"] if v and time.time() - v["t"] < LTP_MAX_AGE else None


def candle_close(symbol):
    best = None
    for t in INTERVALS:
        with LOCK:
            row = next((r for r in STATES[t]["rows"] if r["symbol"] == symbol), None)
            upd = STATES[t]["updated"]
        if row and (best is None or str(upd) > str(best[1])):
            best = (row["close"], upd)
    return best[0] if best else None


def live_price(row):
    """The live broker price when there is a fresh one, otherwise the candle close."""
    p = fresh_ltp(row["symbol"])
    return p if p else row["close"]


def feed_items(broker):
    """[(symbol, exchange, token)] for every dashboard symbol the broker's instrument list knows."""
    out = []
    eq = M(broker)["eq"]
    seg = "NSE" if broker == "angel" else "NSE_EQ"
    for name in SYMBOLS:
        if name in eq:
            out.append((name, seg, str(eq[name][0])))
    for name, (ex, tok) in INDEX_FEED.get(broker, {}).items():
        out.append((name, ex, tok))
    return out


def feed_account():
    if FEED["acct"] is None:
        if DATA_BROKER == "angel":
            keys = {"api_key": "ANGEL_API_KEY", "client_id": "ANGEL_CLIENT_ID", "pin": "ANGEL_PIN", "totp_secret": "ANGEL_TOTP_SECRET"}
            creds = {k: os.getenv(v, "").strip() for k, v in keys.items()}
            FEED["acct"] = AngelAccount(creds)
        else:
            creds = {"client_id": os.getenv("DHAN_CLIENT_ID", "").strip(), "access_token": os.getenv("DHAN_ACCESS_TOKEN", "").strip()}
            FEED["acct"] = DhanAccount(creds)
        if not all(creds.values()):
            FEED["acct"] = None
            raise RuntimeError("The data feed credentials are not all set in the environment.")
    return FEED["acct"]


def feed_once():
    """One pass over every symbol. Prices far from the last candle close are dropped as bad data."""
    if not load_master(DATA_BROKER):
        raise RuntimeError("The instrument list could not be loaded.")
    items = feed_items(DATA_BROKER)
    prices = feed_account().ltp([(ex, tok) for _, ex, tok in items])
    now, n = time.time(), 0
    for sym, ex, tok in items:
        p = prices.get((ex, tok))
        if not p:
            continue
        ref = candle_close(sym)
        if ref and abs(p / ref - 1) > LTP_MAX_JUMP:
            continue
        LTP[sym] = {"p": float(p), "t": now}
        n += 1
    FEED.update(count=n, t=now, error=None)
    return n


def market_window():
    n = ist_now()
    return n.weekday() < 5 and dtime(9, 0) <= n.time() <= dtime(15, 45)


def feed_loop():
    while True:
        try:
            if market_window():
                feed_once()
            else:
                time.sleep(30)
        except Exception as e:
            FEED["error"] = f"{type(e).__name__}: {str(e)[:120]}"
            if hasattr(FEED["acct"], "obj"):
                FEED["acct"].obj = None
            time.sleep(30)
        time.sleep(FEED_SECONDS)


def apply_live(rows):
    """Copies of the rows with the live price and the change since the previous close, when a fresh price exists."""
    out = []
    for r in rows:
        p = fresh_ltp(r["symbol"])
        if p:
            r = {**r, "ltp": round(p, 2)}
            if r.get("prev_close"):
                r["ltp_chg"] = round((p / r["prev_close"] - 1) * 100, 2)
        out.append(r)
    return out


def user_by_id(uid):
    rows = q("SELECT * FROM users WHERE id=?", (uid,))
    return build_user(rows[0]) if rows and rows[0]["active"] else None


def stop_price_now(u, o):
    """Current price of the instrument the order trades: from the owner's broker for live orders, else the best price we have."""
    if o["mode"] == "live":
        try:
            got = get_account(u["id"], o["broker"]).ltp([(o["exchange"], o["token"])])
            p = got.get((o["exchange"], str(o["token"])))
            if p:
                return p
        except Exception:
            pass
        return None
    return fresh_ltp(o["symbol"]) or candle_close(o["symbol"])


def trail_target(o, rec, price):
    """The stop trigger the trailing rule asks for at this price, or None if the stop should stay where it is."""
    fill = o.get("avg_price") or o.get("price") or o.get("ref")
    risk = o.get("risk")
    if not fill or not risk or not price or abs(price / fill - 1) > LTP_MAX_JUMP:
        return None
    buy = o["side"] == "BUY"
    steps = int(((price - fill) if buy else (fill - price)) // risk)
    if steps < 1:
        return None
    steps = min(steps, TRAIL_MAX_STEP)
    trig = fill + (steps - 1) * risk if buy else fill - (steps - 1) * risk
    trig = round(round(trig / 0.05) * 0.05, 2)
    cur = rec.get("sl")
    if cur is not None and ((buy and trig <= cur) or (not buy and trig >= cur)):
        return None
    if (buy and trig >= price) or (not buy and trig <= price):
        return None  # a stop at or beyond the market would fire at once
    return trig, steps


def autopilot_once():
    """Place stop-losses for filled entry orders that asked for one, and trail them. Never retries a broker order."""
    if not market_open_now():
        return 0
    with ORDER_LOCK:
        pend = [o for o in ORDERS if o.get("auto_sl") and not o.get("is_exit") and o.get("kind") in ("equity", "future")
                and o.get("sl_state") in (None, "placed") and not o.get("trail_done")]
    acts = 0
    for uid in {o["user"] for o in pend}:
        u = user_by_id(uid)
        mine = [o for o in pend if o["user"] == uid]
        if not u:
            continue
        if any(o["mode"] == "live" for o in mine):
            sync_status(u)
        for o in mine:
            try:
                acts += _auto_one(u, o)
            except Exception as e:
                AUTO["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    AUTO["last"] = time.time()
    return acts


def _auto_one(u, o):
    live = o["mode"] == "live"
    if o["sl_state"] is None:
        if o["status"] in ("rejected", "cancelled", "expired", "unknown"):
            o["sl_state"] = "skipped"
            append_log(o)
            return 0
        if o["status"] != "complete":
            return 0
        o["sl_state"] = "placing"  # recorded first, so a crash mid-call is never followed by a second order
        append_log(o)
        rec, err = place_stop(u, o, live)
        if err:
            o["sl_state"] = "failed"
            o["sl_note"] = err
            append_log(o)
            notify_user(u["id"], f"STOP-LOSS NOT PLACED: {o['symbol']}", f"{err}\nYour {o['side']} position in {o['label']} has no automatic stop-loss. Place one yourself now.")
            return 1
        notify_user(u["id"], f"Stop-loss placed: {o['symbol']}", f"Trigger {rec['sl']:.2f} for {rec['qty']} ({'live' if live else 'paper'}).")
        return 1
    if not o.get("trail") or o["sl_state"] != "placed":
        return 0
    rec = find_order(o["sl_order_id"], u["id"])
    if not rec:
        return 0
    if rec["status"] in ("complete", "cancelled", "rejected", "expired"):
        o["trail_done"] = True
        append_log(o)
        if rec["status"] == "complete":
            notify_user(u["id"], f"Stop-loss hit: {o['symbol']}", f"Your stop-loss for {o['label']} was triggered.")
        return 0
    price = stop_price_now(u, o)
    got = trail_target(o, rec, price)
    if not got:
        return 0
    trig, steps = got
    try:
        if live:
            get_account(u["id"], o["broker"]).modify_stop(o, rec, trig)
        rec["sl"] = trig
        rec["note"] = f"Trigger {trig:.2f} (trailed)"
        o["trail_level"] = steps
        o["trail_fail"] = 0
        append_log(rec)
        append_log(o)
        notify_user(u["id"], f"Stop-loss moved: {o['symbol']}", f"Trigger is now {trig:.2f} (price {price:.2f}).")
        return 1
    except Exception as e:
        o["trail_fail"] = o.get("trail_fail", 0) + 1
        if o["trail_fail"] == 3:
            notify_user(u["id"], f"Could not move stop-loss: {o['symbol']}", f"{str(e)[:120]}\nYour existing stop-loss is still in place at {rec.get('sl')}.")
        return 0


def autopilot_loop():
    while True:
        try:
            autopilot_once()
        except Exception as e:
            AUTO["error"] = f"{type(e).__name__}: {str(e)[:100]}"
        time.sleep(AUTOPILOT_SECONDS)


def start_engine():
    threading.Thread(target=autopilot_loop, daemon=True).start()
    if DATA_BROKER in ("angel", "dhan"):
        FEED["source"] = DATA_BROKER
        threading.Thread(target=feed_loop, daemon=True).start()


# ── Web app ──
app = FastAPI(title="Signal dashboard", docs_url=None, redoc_url=None, openapi_url=None)


PUBLIC_PATHS = {"/login", "/api/login", "/signup", "/api/signup", "/forgot", "/api/forgot", "/admin"}


# ── Signal history and win rates ──
def hist_stats(recs):
    """Hit rates per target. A signal counts for target k once it reached that target (win) or hit the stop first (loss);
    signals that have done neither yet are still open and are left out of that target's rate."""
    n = len(recs)
    out = {"n": n, "open": sum(1 for r in recs if not r["sl_hit"] and r["hits"] < 4),
           "sl_first": sum(1 for r in recs if r["sl_hit"] and r["hits"] == 0), "t": []}
    for k in range(1, 5):
        wins = sum(1 for r in recs if r["hits"] >= k)
        losses = sum(1 for r in recs if r["sl_hit"] and r["hits"] < k)
        done = wins + losses
        out["t"].append({"k": k, "wins": wins, "losses": losses, "done": done,
                         "rate": round(100 * wins / done, 1) if done else None,
                         "exp": round((wins * k - losses) / done, 2) if done else None})
    out["sl_rate"] = round(100 * out["sl_first"] / n, 1) if n else None
    return out


def hist_groups(recs, key, min_n=1):
    g = {}
    for r in recs:
        g.setdefault(r[key], []).append(r)
    rows = []
    for k, v in g.items():
        if len(v) >= min_n:
            st = hist_stats(v)
            rows.append({"key": k, "n": st["n"], "t1": st["t"][0]["rate"], "t2": st["t"][1]["rate"], "t3": st["t"][2]["rate"],
                         "sl_rate": st["sl_rate"], "exp1": st["t"][0]["exp"], "exp2": st["t"][1]["exp"], "open": st["open"]})
    return rows


def hist_status(r):
    if r["sl_hit"]:
        return f"T{r['hits']} then SL" if r["hits"] else "SL hit"
    return "T4 hit" if r["hits"] == 4 else (f"T{r['hits']} hit, open" if r["hits"] else "Open")


@app.get("/api/history")
def api_history(tf: str = DEFAULT_INTERVAL, symbol: str = "", sector: str = "", side: str = "", days: int = 0):
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    ensure_fresh(tf)
    with LOCK:
        recs = list(HIST[tf])
        scanned = STATES[tf]["updated"]
        progress = STATES[tf]["progress"]
    symbol = symbol.upper().strip()
    if symbol:
        recs = [r for r in recs if symbol in r["symbol"]]
    if sector and sector != "ALL":
        recs = [r for r in recs if r["sector"] == sector]
    if side in ("BUY", "SELL"):
        recs = [r for r in recs if r["side"] == side]
    if days and days > 0:
        cut = time.time() - days * 86400
        recs = [r for r in recs if r["t"] >= cut]
    recs.sort(key=lambda r: r["t"], reverse=True)
    oldest = min((r["t"] for r in recs), default=None)
    sym_rows = hist_groups(recs, "symbol", 3)
    by_rate = sorted([r for r in sym_rows if r["exp2"] is not None], key=lambda r: (r["exp2"], r["t2"] or 0, r["n"]), reverse=True)
    half = len(by_rate) // 2  # the two lists never share a symbol
    return {"tf": tf, "scanned": scanned, "progress": progress, "stats": hist_stats(recs),
            "by_side": hist_groups(recs, "side"), "by_sector": sorted(hist_groups(recs, "sector"), key=lambda r: -r["n"]),
            "best": [r for r in by_rate[:min(8, half)] if r["exp2"] > 0], "worst": [r for r in reversed(by_rate[len(by_rate) - min(8, half):]) if r["exp2"] < 0] if half else [],
            "by_hour": sorted(hist_groups([r for r in recs if r["hour"] is not None], "hour"), key=lambda r: r["key"]) if tf != "1d" else [],
            "recent": [{**r, "status": hist_status(r)} for r in recs[:300]],
            "since": pd.Timestamp(oldest, unit="s", tz="UTC").tz_convert(IST).strftime("%d %b %Y") if oldest else None,
            "sector_names": sector_names()}


# ── Signal alerts: Telegram and ntfy push ──
# Each person chooses where to be told about new signals. One server-wide Telegram bot (TELEGRAM_BOT_TOKEN) serves everyone;
# ntfy.sh needs no account: the person installs the ntfy app and subscribes to a private topic name.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
ALERT_MAX_AGE_BARS = 1      # only candles this fresh are announced
ALERT_DIGEST_AT = 5         # more new signals than this at once are sent as one summary
ALERT_HOURLY_CAP = 80       # most messages one person can receive per hour
ALERT_SYNC = False          # tests set this so messages are sent immediately
TF_NAMES = {"5m": "5 min", "15m": "15 min", "1d": "Daily"}
TG = {"name": None}
LINK_CODES = {}             # code -> (user id, expires)
ALERT_STATE = {"baseline": set(), "sent": {}, "last": {}, "test": {}}
ALERT_Q = queue.Queue()
NTFY_TOPIC_RE = re.compile(r"[A-Za-z0-9_-]{6,64}")


def sector_names():
    return sorted(set(SECTOR_OF.values()) | {"Indices"})


def alert_row(uid):
    rows = q("SELECT * FROM alerts WHERE user_id=?", (uid,))
    r = rows[0] if rows else {"enabled": 0, "telegram_chat": None, "ntfy_topic": None, "tfs": "15m", "sides": "BUY,SELL", "sectors": ""}
    return {"enabled": bool(r["enabled"]), "telegram_chat": r["telegram_chat"], "ntfy_topic": r["ntfy_topic"],
            "tfs": [t for t in str(r["tfs"] or "").split(",") if t in INTERVALS],
            "sides": [t for t in str(r["sides"] or "").split(",") if t in ("BUY", "SELL")],
            "sectors": [t for t in str(r["sectors"] or "").split(",") if t]}


def ensure_alert_row(uid):
    x("INSERT INTO alerts(user_id, updated) VALUES(?,?) ON CONFLICT(user_id) DO NOTHING", (uid, ist_now().isoformat()))


def _tg(method, **params):
    r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", json=params, timeout=35)
    return r.json() if r.content else {}


def send_telegram(chat, text):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": chat, "text": text, "disable_web_page_preview": True}, timeout=10)
        return (True, "sent") if r.status_code == 200 else (False, f"Telegram refused it (HTTP {r.status_code}). Link Telegram again.")
    except Exception as e:
        return False, f"Telegram could not be reached: {type(e).__name__}"


def send_ntfy(topic, title, text, tag):
    try:
        r = requests.post(f"{NTFY_SERVER}/{topic}", data=text.encode("utf-8"), timeout=10,
                          headers={"Title": title.encode("ascii", "ignore").decode(), "Tags": tag, "Priority": "high"})
        return (True, "sent") if r.status_code == 200 else (False, f"ntfy refused it (HTTP {r.status_code}).")
    except Exception as e:
        return False, f"ntfy could not be reached: {type(e).__name__}"


def deliver(uid, title, text, tag, force=False):
    """Send one message to every channel the person set up. Returns {channel: result text}."""
    now = time.time()
    sent = [t for t in ALERT_STATE["sent"].get(uid, []) if now - t < 3600]
    ALERT_STATE["sent"][uid] = sent
    if len(sent) >= ALERT_HOURLY_CAP and not force:
        return {"limit": "hourly limit reached"}
    pr = alert_row(uid)
    out = {}
    if pr["telegram_chat"] and TELEGRAM_BOT_TOKEN:
        out["telegram"] = send_telegram(pr["telegram_chat"], title + "\n" + text)[1]
    if pr["ntfy_topic"]:
        out["ntfy"] = send_ntfy(pr["ntfy_topic"], title, text, tag)[1]
    if out:
        sent.append(now)
        bad = [v for v in out.values() if v != "sent"]
        ALERT_STATE["last"][uid] = (ist_now().strftime("%d %b %H:%M") + (" sent" if not bad else " problem: " + "; ".join(bad)))
    return out


def alert_worker():
    while True:
        item = ALERT_Q.get()
        try:
            deliver(*item)
        except Exception as e:
            print("alert delivery failed:", e, flush=True)


def notify_user(uid, title, text):
    """Important messages (a stop-loss that was not placed) bypass the hourly limit."""
    item = (uid, title, text, "warning", True)
    if ALERT_SYNC:
        deliver(*item)
    else:
        ALERT_Q.put(item)


def queue_alert(uid, title, text, tag):
    if ALERT_SYNC:
        deliver(uid, title, text, tag)
    else:
        ALERT_Q.put((uid, title, text, tag))


def _n(v):
    return "–" if v is None else f"{v:,.2f}"


def fmt_alert(tf, r):
    chg = r.get("chg")
    lines = [f"Entry {_n(r['entry'])}   SL {_n(r['sl'])}",
             f"T1 {_n(r['tp1'])}   T2 {_n(r['tp2'])}   T3 {_n(r['tp3'])}   T4 {_n(r['tp4'])}",
             f"{r.get('sector', '')}" + (f" · today {chg:+.2f}%" if chg is not None else ""),
             f"Signal candle {r['signal_time']}", "Review it on the dashboard before placing any order."]
    return f"{r['signal']} {r['symbol']} ({TF_NAMES.get(tf, tf)})", "\n".join(lines)


def check_alerts(interval, rows):
    """Called after every scan. Tells people about signals that appeared since the last scan, each only once."""
    fresh = [r for r in rows if r["signal"] in ("BUY", "SELL") and r.get("bars_ago") is not None and r["bars_ago"] <= ALERT_MAX_AGE_BARS]
    new = []
    for r in fresh:
        k = f"{interval}|{r['symbol']}|{r['signal_time']}|{r['signal']}"
        if not q("SELECT 1 FROM alert_seen WHERE k=?", (k,)):
            x("INSERT INTO alert_seen(k, ts) VALUES(?,?) ON CONFLICT(k) DO NOTHING", (k, ist_now().isoformat()))
            new.append(r)
    if interval not in ALERT_STATE["baseline"]:  # first scan after a restart: note what exists, announce nothing
        ALERT_STATE["baseline"].add(interval)
        return 0
    if not new:
        return 0
    users = q("SELECT a.user_id FROM alerts a JOIN users u ON u.id = a.user_id WHERE a.enabled=1 AND u.active=1")
    count = 0
    for u in users:
        pr = alert_row(u["user_id"])
        if interval not in pr["tfs"] or not (pr["telegram_chat"] or pr["ntfy_topic"]):
            continue
        mine = [r for r in new if r["signal"] in pr["sides"] and (not pr["sectors"] or r.get("sector") in pr["sectors"])]
        if not mine:
            continue
        if len(mine) > ALERT_DIGEST_AT:
            b = [r["symbol"] for r in mine if r["signal"] == "BUY"]
            sl = [r["symbol"] for r in mine if r["signal"] == "SELL"]
            text = (f"BUY ({len(b)}): " + ", ".join(b[:25]) if b else "") + ("\n" if b and sl else "") + (f"SELL ({len(sl)}): " + ", ".join(sl[:25]) if sl else "")
            queue_alert(u["user_id"], f"{len(mine)} new signals ({TF_NAMES.get(interval, interval)})", text, "chart_with_upwards_trend")
            count += 1
        else:
            for r in mine:
                title, text = fmt_alert(interval, r)
                queue_alert(u["user_id"], title, text, "green_circle" if r["signal"] == "BUY" else "red_circle")
                count += 1
    return count


def handle_tg_update(upd):
    msg = upd.get("message") or {}
    chat = msg.get("chat") or {}
    if chat.get("type") != "private":
        return
    text = (msg.get("text") or "").strip()
    m = re.fullmatch(r"/start(?:@\w+)?\s+([0-9A-Fa-f]{8})", text)
    reply = None
    if m:
        ent = LINK_CODES.pop(m.group(1).upper(), None)
        if ent and ent[1] > time.time():
            ensure_alert_row(ent[0])
            x("UPDATE alerts SET telegram_chat=? WHERE user_id=?", (str(chat["id"]), ent[0]))
            reply = "Linked. New signal alerts will arrive in this chat. You can change what you receive in the dashboard under Account."
        else:
            reply = "That link code is wrong or has expired. Create a new one in the dashboard under Account, Signal alerts."
    elif text.startswith("/start"):
        reply = "Open the dashboard, go to Account, Signal alerts, and press Link Telegram. It gives you a link that connects this chat."
    if reply:
        send_telegram(chat["id"], reply)


def telegram_poll():
    offset = None
    while True:
        try:
            if not TG["name"]:
                TG["name"] = (_tg("getMe").get("result") or {}).get("username")
            for c in [c for c, v in LINK_CODES.items() if v[1] < time.time()]:
                LINK_CODES.pop(c, None)
            if not LINK_CODES:  # only listen while someone is waiting to link
                time.sleep(3)
                continue
            params = {"timeout": 20, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset
            for upd in _tg("getUpdates", **params).get("result", []):
                offset = upd["update_id"] + 1
                handle_tg_update(upd)
        except Exception as e:
            print("telegram poll:", type(e).__name__, flush=True)
            time.sleep(10)


def start_alerts():
    threading.Thread(target=alert_worker, daemon=True).start()
    if TELEGRAM_BOT_TOKEN:
        threading.Thread(target=telegram_poll, daemon=True).start()


@app.get("/api/alerts")
def api_alerts(request: Request):
    u = request.state.user
    pr = alert_row(u["id"])
    chat = pr.pop("telegram_chat")
    return {**pr, "telegram_linked": bool(chat), "telegram_configured": bool(TELEGRAM_BOT_TOKEN), "bot": TG["name"],
            "ntfy_server": NTFY_SERVER, "sector_names": sector_names(), "tf_names": TF_NAMES, "last": ALERT_STATE["last"].get(u["id"])}


@app.put("/api/alerts")
def api_alerts_save(request: Request, payload: dict = Body(...)):
    u = request.state.user
    tfs = [t for t in INTERVALS if t in (payload.get("tfs") or [])]
    sides = [t for t in ("BUY", "SELL") if t in (payload.get("sides") or [])]
    names = set(sector_names())
    sectors = [t for t in (payload.get("sectors") or []) if t in names]
    topic = str(payload.get("ntfy_topic") or "").strip()
    if topic and not NTFY_TOPIC_RE.fullmatch(topic):
        return {"error": "The ntfy topic must be 6 to 64 letters, digits, dashes or underscores."}
    enabled = bool(payload.get("enabled"))
    if enabled and not tfs:
        return {"error": "Pick at least one timeframe."}
    if enabled and not sides:
        return {"error": "Pick BUY, SELL or both."}
    ensure_alert_row(u["id"])
    x("UPDATE alerts SET enabled=?, ntfy_topic=?, tfs=?, sides=?, sectors=?, updated=? WHERE user_id=?",
      (int(enabled), topic or None, ",".join(tfs), ",".join(sides), ",".join(sectors), ist_now().isoformat(), u["id"]))
    return {"ok": True}


@app.post("/api/alerts/telegram/link")
def api_tg_link(request: Request):
    u = request.state.user
    if not TELEGRAM_BOT_TOKEN:
        return {"error": "Telegram alerts are not set up on this server. The admin must set TELEGRAM_BOT_TOKEN."}
    if not TG["name"]:
        try:
            TG["name"] = (_tg("getMe").get("result") or {}).get("username")
        except Exception:
            pass
    if not TG["name"]:
        return {"error": "Could not reach Telegram just now. Try again in a minute."}
    for c in [c for c, v in LINK_CODES.items() if v[0] == u["id"]]:
        LINK_CODES.pop(c, None)
    code = secrets.token_hex(4).upper()
    LINK_CODES[code] = (u["id"], time.time() + 600)
    return {"code": code, "bot": TG["name"], "url": f"https://t.me/{TG['name']}?start={code}", "minutes": 10}


@app.post("/api/alerts/telegram/unlink")
def api_tg_unlink(request: Request):
    u = request.state.user
    ensure_alert_row(u["id"])
    x("UPDATE alerts SET telegram_chat=NULL WHERE user_id=?", (u["id"],))
    return {"ok": True}


@app.post("/api/alerts/test")
def api_alert_test(request: Request):
    u = request.state.user
    if time.time() - ALERT_STATE["test"].get(u["id"], 0) < 10:
        return {"error": "Wait a few seconds before sending another test."}
    ALERT_STATE["test"][u["id"]] = time.time()
    out = deliver(u["id"], "Test alert", "If you can read this, signal alerts will reach you here.", "white_check_mark", force=True)
    if not out:
        return {"error": "Nothing is set up yet. Link Telegram or enter an ntfy topic, save, then test again."}
    return {"ok": all(v == "sent" for v in out.values()), "results": out}


CSP = ("default-src 'self'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
       "font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    h = resp.headers
    h.setdefault("X-Frame-Options", "DENY")
    h.setdefault("X-Content-Type-Options", "nosniff")
    h.setdefault("Referrer-Policy", "same-origin")
    h.setdefault("Content-Security-Policy", CSP)
    h.setdefault("Cache-Control", "no-store")
    if request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https":
        h.setdefault("Strict-Transport-Security", "max-age=31536000")
    return resp


@app.middleware("http")
async def auth_mw(request: Request, call_next):
    path = request.url.path
    request.state.user = session_user(request)
    if path in PUBLIC_PATHS:
        return await call_next(request)
    u = request.state.user
    if not u:
        if path.startswith("/api/"):
            return JSONResponse({"error": "Please sign in."}, status_code=401)
        return RedirectResponse("/login")
    if request.method in ("POST", "PUT", "PATCH", "DELETE") and not hmac.compare_digest(request.headers.get("x-csrf-token", ""), u["csrf"] or ""):
        return JSONResponse({"error": "Your session is out of date. Reload the page."}, status_code=403)
    return await call_next(request)


@app.on_event("startup")
def start_worker():
    init_db()
    start_alerts()
    start_engine()
    threading.Thread(target=refresh, args=(DEFAULT_INTERVAL,), daemon=True).start()


@app.post("/api/login")
def api_login(request: Request, payload: dict = Body(...)):
    name = str(payload.get("username", "")).strip()
    pw = str(payload.get("password", ""))
    key = (name.lower(), request.client.host if request.client else "?")
    st = LOGIN_FAILS.get(key, {"n": 0, "until": 0.0})
    if time.time() < st["until"]:
        return JSONResponse({"error": "Too many attempts. Try again in a few minutes."}, status_code=429)
    pw = pw[:300]  # never spend time hashing a huge input
    rows = q("SELECT * FROM users WHERE username=?", (name,))
    ok = bool(rows) and bool(rows[0]["active"]) and check_secret(pw, rows[0]["pw_hash"])
    if not rows:
        check_secret(pw, DUMMY_HASH)  # same work whether or not the name exists
    if not ok:
        st["n"] += 1
        if st["n"] >= 5:
            st = {"n": 0, "until": time.time() + 300}
        LOGIN_FAILS[key] = st
        return JSONResponse({"error": "Wrong username or password."}, status_code=401)
    if payload.get("admin") and rows[0]["role"] != "admin":
        st["n"] += 1
        LOGIN_FAILS[key] = st
        return JSONResponse({"error": "This sign-in is for admins only. Regular users sign in on the main page."}, status_code=403)
    LOGIN_FAILS.pop(key, None)
    if pw == DEFAULT_ADMIN_PW:
        if LIVE_REQUESTED:
            return JSONResponse({"error": "The default password is not accepted while live trading is switched on. Set ADMIN_PASSWORD on the server."}, status_code=403)
        DEFAULT_PW_USERS.add(rows[0]["id"])
    else:
        DEFAULT_PW_USERS.discard(rows[0]["id"])
    resp = JSONResponse({"ok": True})
    resp.set_cookie("sid", new_session(rows[0]["id"]), httponly=True, samesite="lax", path="/",
                    secure=COOKIE_SECURE or request.url.scheme == "https", max_age=int(SESSION_HOURS * 3600))
    return resp


@app.post("/api/logout")
def api_logout(request: Request):
    tok = request.cookies.get("sid")
    if tok:
        x("DELETE FROM sessions WHERE token_hash=?", (sha(tok),))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("sid", path="/")
    return resp


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.state.user:
        return RedirectResponse("/")
    return auth_page("login")


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    u = request.state.user
    if u and u["role"] == "admin":
        return RedirectResponse("/?m=admin")
    return auth_page("admin")


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    if request.state.user:
        return RedirectResponse("/")
    return auth_page("signup")


@app.get("/forgot", response_class=HTMLResponse)
def forgot_page(request: Request):
    if request.state.user:
        return RedirectResponse("/")
    return auth_page("forgot")


@app.post("/api/signup")
def api_signup(request: Request, payload: dict = Body(...)):
    if not ALLOW_SIGNUP:
        return JSONResponse({"error": "Sign-up is closed. Ask the admin for an account."}, status_code=403)
    ip = client_ip(request)
    now = time.time()
    recent = [t for t in SIGNUP_LOG.get(ip, []) if now - t < 3600]
    if len(recent) >= 5:
        return JSONResponse({"error": "Too many sign-ups from this address. Try again later."}, status_code=429)
    SIGNUP_LOG[ip] = recent + [now]
    if SIGNUP_CODE and not hmac.compare_digest(str(payload.get("code", "")), SIGNUP_CODE):
        return JSONResponse({"error": "The invite code is wrong."}, status_code=400)
    name = str(payload.get("username", "")).strip()
    pw = str(payload.get("password", ""))
    if not re.fullmatch(r"[A-Za-z0-9_.-]{3,32}", name):
        return JSONResponse({"error": "Username: 3 to 32 letters, digits, dots, dashes or underscores."}, status_code=400)
    bad = valid_new_password(name, pw)
    if bad:
        return JSONResponse({"error": bad}, status_code=400)
    if q("SELECT 1 FROM users WHERE username=?", (name,)):
        return JSONResponse({"error": "That username is taken."}, status_code=400)
    try:
        uid = x("INSERT INTO users(username, pw_hash, role, active, live_allowed, created) VALUES(?,?,?,?,?,?)",
                (name, hash_secret(pw), "user", 1, 0, ist_now().isoformat()))
    except DuplicateUser:
        return JSONResponse({"error": "That username is taken."}, status_code=400)
    resp = JSONResponse({"ok": True, "recovery_key": set_recovery(uid)})
    session_cookie(resp, request, uid)
    return resp


@app.post("/api/forgot")
def api_forgot(request: Request, payload: dict = Body(...)):
    name = str(payload.get("username", "")).strip()
    key = norm_key(payload.get("key", ""))
    pw = str(payload.get("password", ""))
    fk = ("forgot", name.lower(), client_ip(request))
    st = LOGIN_FAILS.get(fk, {"n": 0, "until": 0.0})
    if time.time() < st["until"]:
        return JSONResponse({"error": "Too many attempts. Try again in a few minutes."}, status_code=429)
    rows = q("SELECT * FROM users WHERE username=?", (name,))
    stored = rows[0].get("recovery_hash") if rows else None
    ok = bool(rows) and bool(rows[0]["active"]) and bool(stored) and check_secret(key, stored)
    if not stored:
        check_secret(key, DUMMY_HASH)  # same work whether or not the name or key exists
    if not ok:
        st["n"] += 1
        if st["n"] >= 5:
            st = {"n": 0, "until": time.time() + 900}
        LOGIN_FAILS[fk] = st
        return JSONResponse({"error": "The username or recovery key is wrong. If you have no recovery key, ask the admin to reset your password."}, status_code=400)
    bad = valid_new_password(name, pw)
    if bad:
        return JSONResponse({"error": bad}, status_code=400)
    LOGIN_FAILS.pop(fk, None)
    uid = rows[0]["id"]
    x("UPDATE users SET pw_hash=? WHERE id=?", (hash_secret(pw), uid))
    x("DELETE FROM sessions WHERE user_id=?", (uid,))
    PIN_STATE.pop(uid, None)
    DEFAULT_PW_USERS.discard(uid)
    return {"ok": True, "recovery_key": set_recovery(uid)}  # a recovery key works once; here is the next one


@app.get("/api/signals")
def api_signals(tf: str = DEFAULT_INTERVAL):
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    ensure_fresh(tf)  # scans on demand, so a timeframe nobody is viewing costs nothing
    with LOCK:
        snap = dict(STATES[tf])
        snap["rows"] = apply_live(snap["rows"])
        return JSONResponse({
            **snap,
            "feed": {"source": FEED["source"], "age": None if not FEED["t"] else int(time.time() - FEED["t"]), "count": FEED["count"], "error": FEED["error"]},
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


# ── Chart popup: candles, EMA, Chandelier line, signal markers and the latest signal's levels ──
CHART_CACHE = {}
CHART_BARS = {"5m": 400, "15m": 400, "1d": 500}


def chart_data(symbol, tf):
    import yfinance as yf
    df = yf.download(yf_ticker(symbol), period=PERIODS[tf], interval=tf, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return None
    if getattr(df.columns, "nlevels", 1) > 1:
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close", "High", "Low", "Open"]).copy()
    row = analyse(symbol, df.copy(), tf)  # same rules as the signal board, so the chart always agrees with it
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    if CONFIRMED_ONLY:
        df = drop_forming(df, tf)
    n = len(df)
    if n < MIN_BARS or not row:
        return None
    c = df["Close"].to_numpy(float)
    ls, ss, d, atr = chandelier(df)
    ema = df["Close"].ewm(span=EMA_LEN, adjust=False).mean().to_numpy()
    green, red = qqe_states(df["Close"])
    flip_up = np.zeros(n, bool)
    flip_dn = np.zeros(n, bool)
    flip_up[1:] = (d[1:] == 1) & (d[:-1] == -1)
    flip_dn[1:] = (d[1:] == -1) & (d[:-1] == 1)
    buy = flip_up & (c > ema) & within_lookback(green)
    sell = flip_dn & (c < ema) & within_lookback(red)

    def tm(ts):
        return ts.strftime("%Y-%m-%d") if tf == "1d" else int(ts.timestamp()) + 19800  # IST on the axis

    start = max(0, n - CHART_BARS[tf])
    t = [tm(df.index[i]) for i in range(start, n)]

    def val(a, i):
        v = a[i]
        return None if np.isnan(v) else round(float(v), 2)

    out = {"symbol": symbol, "tf": tf, "intraday": tf != "1d",
           "candles": [{"time": t[k], "open": round(float(df["Open"].iloc[i]), 2), "high": round(float(df["High"].iloc[i]), 2),
                        "low": round(float(df["Low"].iloc[i]), 2), "close": round(float(c[i]), 2)} for k, i in enumerate(range(start, n))],
           "ema": [{"time": t[k], "value": val(ema, i)} for k, i in enumerate(range(start, n)) if val(ema, i) is not None],
           "ce_long": [{"time": t[k], "value": val(ls, i)} for k, i in enumerate(range(start, n)) if d[i] == 1 and val(ls, i) is not None],
           "ce_short": [{"time": t[k], "value": val(ss, i)} for k, i in enumerate(range(start, n)) if d[i] == -1 and val(ss, i) is not None],
           "markers": [{"time": t[i - start], "side": "BUY" if buy[i] else "SELL"} for i in range(start, n) if buy[i] or sell[i]],
           "ema_len": EMA_LEN, "signal": None}
    if row.get("entry") is not None:
        j = max((i for i in range(n) if buy[i] or sell[i]), default=None)
        if j is not None:
            out["signal"] = {"side": row.get("side"), "state": row["signal"], "status": row["status"], "time": row["signal_time"],
                             "from": t[max(j, start) - start], "entry": row["entry"], "sl": row["sl"], "sl_hit": row["sl_hit"], "hits": row["hits"],
                             "tps": [row["tp1"], row["tp2"], row["tp3"], row["tp4"]]}
    return out


@app.get("/api/chart")
def api_chart(symbol: str, tf: str = DEFAULT_INTERVAL):
    symbol = symbol.upper().strip()
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    if not re.fullmatch(r"[A-Z0-9&\-]{1,20}", symbol) or (symbol not in UNIVERSE and symbol not in INDEX_YF):
        return JSONResponse({"error": "Unknown symbol."}, status_code=400)
    key = (symbol, tf)
    hit = CHART_CACHE.get(key)
    if hit and time.time() - hit[0] < 30:
        return JSONResponse(hit[1])
    try:
        d = chart_data(symbol, tf)
    except Exception as e:
        return JSONResponse({"error": f"Could not load candles ({type(e).__name__})."})
    if not d:
        return JSONResponse({"error": "Not enough candle data for this symbol and timeframe."})
    if len(CHART_CACHE) > 200:
        CHART_CACHE.clear()
    CHART_CACHE[key] = (time.time(), d)
    return JSONResponse(d)


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
    sig_info = {"status": row["status"], "signal_time": row["signal_time"], "bars_ago": row["bars_ago"], "side": row.get("side")} if row else None
    all_tf = {}
    with LOCK:
        for t_ in INTERVALS:
            r_ = next((r for r in STATES[t_]["rows"] if r["symbol"] == symbol), None)
            if r_:
                all_tf[t_] = {"signal": r_["signal"], "status": r_["status"], "signal_time": r_["signal_time"]}
    out = {**chain, "signal": signal, "bias": side, "suggested": None, "levels": None,
           "tf": tf, "sig_info": sig_info, "all_tf": all_tf,
           "chg": (apply_live([row])[0].get("ltp_chg", row.get("chg"))) if row else None, "sector": row.get("sector") if row else sector_of(symbol)}
    if side:
        atm_row = next((r for r in chain["rows"] if r["strike"] == chain["atm"]), None)
        if atm_row:
            out["suggested"] = {"side": side, "strike": chain["atm"], "ltp": atm_row[side.lower()]["ltp"]}
    if row and row.get("entry") is not None:
        out["levels"] = {k: row[k] for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4")}
    return JSONResponse(out)


@app.get("/api/futures/info")
def api_fut_info():
    mn = master_name("paper")
    ok = load_master(mn)
    out = {}
    for name in UNIVERSE:
        near = near_future(name, mn)
        if near:
            out[name] = {"lot": near[3], "expiry": near[0].strftime(EXPIRY_FMT)}
    return {"ok": ok, "info": out}


@app.get("/api/trading")
def api_trading(request: Request):
    u = request.state.user
    mode, note = user_mode(u)
    c = u["caps"]
    return {"mode": mode, "note": note, "market_open": market_open_now(),
            "brokers": u["brokers"] if mode == "live" else [], "default_broker": u["default_broker"], "broker_names": BROKER_NAMES,
            "caps": {"max_value": c["max_order_value"], "max_qty": c["max_qty"], "max_orders": c["max_orders"],
                     "max_fut_lots": c["max_fut_lots"], "max_fut_value": c["max_fut_value"]}}


@app.get("/api/trading/check")
def api_trading_check(request: Request, which: str = ""):
    """Read-only test of the broker connection and the instrument list. Places no order."""
    u = request.state.user
    if not u["brokers"]:
        return {"ok": False, "message": "No broker details saved yet. Add them under Account."}
    b = which if which in u["brokers"] else u["default_broker"]
    name = BROKER_NAMES[b]
    try:
        get_account(u["id"], b).book()
    except Exception as e:
        return {"ok": False, "broker": name, "message": f"Could not connect to {name}: {str(e)[:200]}"}
    loaded = load_master(b)
    m = M(b)
    miss = [t for t in SYMBOLS if t not in m["eq"]]
    msg = (f"Connected to {name}. Instrument list: {len(SYMBOLS) - len(miss)} of {len(SYMBOLS)} stocks, "
           f"{len(m['fut'])} futures symbols, {len(m['opt'])} option contracts.")
    if not loaded or not m["eq"]:
        msg += " The instrument list did not load or did not match, so orders will be refused until it does."
    elif miss:
        msg += " Not found: " + ", ".join(miss[:5]) + ("..." if len(miss) > 5 else "") + "."
    return {"ok": bool(loaded and m["eq"]), "broker": name, "message": msg}


@app.post("/api/orders/preview")
def api_preview(request: Request, payload: dict = Body(...)):
    u = request.state.user
    order, err, warnings, lines = make_order(payload, u)
    if err:
        return {"error": err}
    token = secrets.token_urlsafe(16)
    now = time.time()
    with ORDER_LOCK:
        for k in [k for k, v in PENDING.items() if v["exp"] < now]:
            PENDING.pop(k, None)
        PENDING[token] = {"order": order, "exp": now + 90, "user": u["id"]}
    return {"token": token, "lines": lines, "warnings": warnings, "mode": order["mode"]}


@app.post("/api/orders/confirm")
def api_confirm(request: Request, payload: dict = Body(...)):
    u = request.state.user
    token = str(payload.get("token", ""))
    with ORDER_LOCK:
        item = PENDING.get(token)
    if not item or item["exp"] < time.time() or item["user"] != u["id"]:
        return {"error": "This review expired or was already used. Review the order again."}
    if str(payload.get("confirm", "")).strip().upper() != "YES":
        return {"error": "Type YES to confirm."}
    live = item["order"]["mode"] == "live"
    err = pin_error(u, payload.get("pin"), live)
    if err:
        return {"error": err}
    if live and user_mode(u)[0] != "live":
        return {"error": "Live trading is no longer enabled for your account."}
    if live and not market_open_now():
        return {"error": "The market is closed."}
    if count_today(u["id"], item["order"]["mode"]) >= u["caps"]["max_orders"]:
        return {"error": f"Daily limit of {u['caps']['max_orders']} orders reached."}
    with ORDER_LOCK:
        item = PENDING.pop(token, None)  # single use: a double click cannot send two orders
    if not item:
        return {"error": "This review was already used. Check the Orders tab."}
    o = item["order"]
    if not live:
        status = "complete" if o["order_type"] == "MARKET" else "open"
        rec = new_record(o, paper_id(), status)
        if status == "complete":
            rec["avg_price"] = o["ref"]
        return {"ok": True, "order": public(rec)}
    bname = BROKER_NAMES.get(o.get("broker"), "broker")
    try:
        oid = get_account(u["id"], o["broker"]).place(o)
    except OrderRejected as e:
        return {"error": f"{bname} rejected the order: {e}"}
    except Exception as e:
        new_record(o, "UNKNOWN-" + secrets.token_hex(3), "unknown",
                   note=f"{bname} did not confirm this order. Check your {bname} app before sending it again.")
        return {"error": f"{bname} did not confirm the order ({e}). Check your {bname} app before trying again."}
    return {"ok": True, "order": public(new_record(o, oid, "open"))}


@app.get("/api/orders")
def api_orders(request: Request):
    u = request.state.user
    sync_status(u)
    with ORDER_LOCK:
        mine = [public(o) for o in reversed(ORDERS) if o.get("user") == u["id"]]
    return {"mode": user_mode(u)[0], "orders": mine}


@app.post("/api/orders/cancel")
def api_cancel(request: Request, payload: dict = Body(...)):
    u = request.state.user
    o = find_order(payload.get("order_id"), u["id"])
    if not o:
        return {"error": "Order not found."}
    err = pin_error(u, payload.get("pin"), o["mode"] == "live")
    if err:
        return {"error": err}
    if o["mode"] == "live":
        try:
            get_account(u["id"], o["broker"]).cancel(o)
        except Exception as e:
            return {"error": f"The broker did not confirm the cancel ({e}). Check your {BROKER_NAMES.get(o.get('broker'), 'broker')} app."}
    o["status"] = "cancelled"
    append_log(o)
    return {"ok": True}


def place_stop(u, o, live):
    """Place the stop-loss for a filled entry order. Used by the Orders tab button and by automatic stop-loss.
    Returns (record, error). The broker call is made once and never retried."""
    qty = int(o.get("filled") or o["qty"])
    exit_side = "SELL" if o["side"] == "BUY" else "BUY"
    fill = o.get("avg_price") or o["price"]
    if o["kind"] == "future":
        # futures trade at a different price from the index, so the stop is measured from the actual fill
        if not fill:
            return None, "The fill price is not known yet. Try again in a few seconds."
        sl = round(round((fill - o["risk"] if o["side"] == "BUY" else fill + o["risk"]) / 0.05) * 0.05, 2)
    else:
        sl = o["sl"]
        if fill and ((o["side"] == "BUY" and sl >= fill) or (o["side"] == "SELL" and sl <= fill)):
            return None, "The stop-loss level is on the wrong side of the entry price, so it was not placed."
    exit_o = {**o, "side": exit_side, "qty": qty, "order_type": "STOPLOSS_MARKET", "price": 0.0, "key": None, "sl": sl}
    if not live:
        rec = new_record(exit_o, paper_id(), "open", variety="STOPLOSS", is_exit=True, note=f"Trigger {sl:.2f}")
    else:
        try:
            oid = get_account(u["id"], o["broker"]).place(o, variety="STOPLOSS", ordertype="STOPLOSS_MARKET", trigger=sl, side=exit_side, qty=qty)
        except Exception as e:
            return None, f"The broker did not confirm the stop-loss ({e}). Check your {BROKER_NAMES.get(o.get('broker'), 'broker')} app."
        rec = new_record(exit_o, oid, "open", variety="STOPLOSS", is_exit=True, note=f"Trigger {sl:.2f}")
    o["sl_order_id"] = rec["id"]
    o["sl_state"] = "placed"
    append_log(o)
    return rec, None


@app.post("/api/orders/protect")
def api_protect(request: Request, payload: dict = Body(...)):
    """Place the stop-loss as a stop-market order, only after the entry order is complete."""
    u = request.state.user
    o = find_order(payload.get("order_id"), u["id"])
    if o and o["mode"] == "live":
        sync_status(u)
    if not o or o.get("is_exit") or o["kind"] not in ("equity", "future"):
        return {"error": "A stop-loss can only be added to a stock or futures entry order."}
    if not (o.get("risk") if o["kind"] == "future" else o.get("sl")):
        return {"error": "This order has no stop-loss level from a signal."}
    if o.get("sl_order_id"):
        return {"error": "A stop-loss was already placed for this order."}
    if o["status"] != "complete":
        return {"error": "The entry order is not filled yet. Place the stop-loss after it shows complete."}
    live = o["mode"] == "live"
    err = pin_error(u, payload.get("pin"), live)
    if err:
        return {"error": err}
    rec, err = place_stop(u, o, live)
    if err:
        return {"error": err}
    return {"ok": True, "order": public(rec)}


# ── your account ──
@app.get("/api/me")
def api_me(request: Request):
    u = request.state.user
    mode, note = user_mode(u)
    hints = {r["broker"]: r["hint"] for r in q("SELECT broker, hint FROM creds WHERE user_id=?", (u["id"],))}
    return {"user": public_user(u), "csrf": u["csrf"], "mode": mode, "note": note, "hints": hints, "default_password": u["id"] in DEFAULT_PW_USERS,
            "broker_fields": BROKER_FIELDS, "broker_names": BROKER_NAMES}


@app.post("/api/account/password")
def api_password(request: Request, payload: dict = Body(...)):
    u = request.state.user
    row = q("SELECT pw_hash FROM users WHERE id=?", (u["id"],))[0]
    if not check_secret(str(payload.get("current", "")), row["pw_hash"]):
        return {"error": "Your current password is wrong."}
    new = str(payload.get("new", ""))
    bad = valid_new_password(u["username"], new)
    if bad:
        return {"error": bad}
    x("UPDATE users SET pw_hash=? WHERE id=?", (hash_secret(new), u["id"]))
    DEFAULT_PW_USERS.discard(u["id"])
    x("DELETE FROM sessions WHERE user_id=? AND token_hash!=?", (u["id"], sha(request.cookies.get("sid", ""))))
    return {"ok": True}


@app.post("/api/account/pin")
def api_pin(request: Request, payload: dict = Body(...)):
    u = request.state.user
    row = q("SELECT pw_hash FROM users WHERE id=?", (u["id"],))[0]
    if not check_secret(str(payload.get("password", "")), row["pw_hash"]):
        return {"error": "Your account password is wrong."}
    pin = str(payload.get("new_pin", ""))
    if not 4 <= len(pin) <= 32:
        return {"error": "The order PIN needs 4 to 32 characters."}
    x("UPDATE users SET pin_hash=? WHERE id=?", (hash_secret(pin), u["id"]))
    PIN_STATE.pop(u["id"], None)
    return {"ok": True}


@app.post("/api/account/recovery")
def api_recovery(request: Request, payload: dict = Body(...)):
    u = request.state.user
    row = q("SELECT pw_hash FROM users WHERE id=?", (u["id"],))[0]
    if not check_secret(str(payload.get("password", "")), row["pw_hash"]):
        return {"error": "Your account password is wrong."}
    return {"ok": True, "recovery_key": set_recovery(u["id"])}


@app.put("/api/account/broker")
def api_broker_save(request: Request, payload: dict = Body(...)):
    u = request.state.user
    if u["id"] in DEFAULT_PW_USERS:
        return {"error": "Change the default admin password before saving broker details."}
    b = str(payload.get("broker", ""))
    fields = payload.get("fields") or {}
    if b not in BROKER_FIELDS:
        return {"error": "Unknown broker."}
    clean = {}
    for key, label in BROKER_FIELDS[b]:
        v = str(fields.get(key, "")).strip()
        if not v or len(v) > 300:
            return {"error": f"{label} is required."}
        clean[key] = v
    try:
        save_creds(u["id"], b, clean)
    except ImportError:
        return {"error": "The 'cryptography' package is missing on the server. Run: pip install cryptography"}
    except RuntimeError as e:
        return {"error": str(e)}
    return {"ok": True}


@app.delete("/api/account/broker/{b}")
def api_broker_delete(request: Request, b: str):
    if b in BROKER_FIELDS:
        delete_creds(request.state.user["id"], b)
    return {"ok": True}


# ── admin ──
def need_admin(request):
    return None if request.state.user["role"] == "admin" else JSONResponse({"error": "Admins only."}, status_code=403)


CAP_TYPES = {"max_order_value": float, "max_qty": int, "max_orders": int, "max_fut_lots": int, "max_fut_value": float,
             "allow_option_selling": bool}


def active_admins():
    return q("SELECT COUNT(*) AS c FROM users WHERE role='admin' AND active=1")[0]["c"]


@app.get("/api/admin/overview")
def api_admin_overview(request: Request):
    denied = need_admin(request)
    if denied:
        return denied
    users = q("SELECT id, username, role, active, live_allowed FROM users")
    names = {r["id"]: r["username"] for r in users}
    with ORDER_LOCK:
        orders = [{**public(o), "username": names.get(o.get("user"), "?")} for o in reversed(ORDERS)][:300]
    sig = {}
    with LOCK:
        for t_ in INTERVALS:
            rows = STATES[t_]["rows"]
            if rows:
                sig[t_] = {"buy": sum(1 for r in rows if r["signal"] == "BUY"), "sell": sum(1 for r in rows if r["signal"] == "SELL"),
                           "open": sum(1 for r in rows if r["signal"].startswith("Open")), "updated": STATES[t_]["updated"]}
    return {"users": len(users), "active": sum(1 for r in users if r["active"]), "admins": sum(1 for r in users if r["role"] == "admin"),
            "live_enabled": sum(1 for r in users if r["live_allowed"]), "orders": orders,
            "live_server": LIVE_REQUESTED, "signup": ALLOW_SIGNUP, "invite": bool(SIGNUP_CODE),
            "storage": "postgres" if USE_PG else DB_PATH, "signals": sig,
            "feed": {"source": FEED["source"], "age": None if not FEED["t"] else int(time.time() - FEED["t"]), "count": FEED["count"], "error": FEED["error"]},
            "auto": {"last_age": None if not AUTO["last"] else int(time.time() - AUTO["last"]), "error": AUTO["error"]}}


@app.get("/api/admin/users")
def api_admin_users(request: Request):
    denied = need_admin(request)
    if denied:
        return denied
    out = []
    for r in q("SELECT * FROM users ORDER BY id"):
        r["csrf"] = None
        u = build_user(r)
        out.append({**public_user(u), "created": r["created"]})
    return {"users": out}


@app.post("/api/admin/users")
def api_admin_create(request: Request, payload: dict = Body(...)):
    denied = need_admin(request)
    if denied:
        return denied
    name = str(payload.get("username", "")).strip()
    pw = str(payload.get("password", ""))
    role = payload.get("role", "user")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{3,32}", name):
        return {"error": "Username: 3 to 32 letters, digits, dots, dashes or underscores."}
    if len(pw) < 10:
        return {"error": "The temporary password needs at least 10 characters."}
    if role not in ("user", "admin"):
        return {"error": "Role must be user or admin."}
    if q("SELECT 1 FROM users WHERE username=?", (name,)):
        return {"error": "That username is taken."}
    try:
        x("INSERT INTO users(username, pw_hash, role, active, live_allowed, created) VALUES(?,?,?,?,?,?)",
          (name, hash_secret(pw), role, 1, 0, ist_now().isoformat()))
    except DuplicateUser:
        return {"error": "That username is taken."}
    return {"ok": True}


@app.patch("/api/admin/users/{uid}")
def api_admin_update(request: Request, uid: int, payload: dict = Body(...)):
    denied = need_admin(request)
    if denied:
        return denied
    me_ = request.state.user
    rows = q("SELECT * FROM users WHERE id=?", (uid,))
    if not rows:
        return {"error": "User not found."}
    t = rows[0]
    if "active" in payload or "role" in payload:
        new_active = bool(payload.get("active", t["active"]))
        new_role = payload.get("role", t["role"])
        if new_role not in ("user", "admin"):
            return {"error": "Role must be user or admin."}
        if uid == me_["id"] and (not new_active or new_role != "admin"):
            return {"error": "You cannot remove your own admin access."}
        if t["role"] == "admin" and t["active"] and (not new_active or new_role != "admin") and active_admins() <= 1:
            return {"error": "There must be at least one active admin."}
        x("UPDATE users SET active=?, role=? WHERE id=?", (int(new_active), new_role, uid))
        if not new_active:
            x("DELETE FROM sessions WHERE user_id=?", (uid,))
    if "live_allowed" in payload:
        if me_["id"] in DEFAULT_PW_USERS:
            return {"error": "Change the default admin password before enabling live trading for anyone."}
        x("UPDATE users SET live_allowed=? WHERE id=?", (int(bool(payload["live_allowed"])), uid))
    if "password" in payload:
        pw = str(payload["password"])
        if len(pw) < 10:
            return {"error": "The password needs at least 10 characters."}
        if pw == DEFAULT_ADMIN_PW:
            return {"error": "Choose a password other than the default one."}
        x("UPDATE users SET pw_hash=? WHERE id=?", (hash_secret(pw), uid))
        DEFAULT_PW_USERS.discard(uid)
        x("DELETE FROM sessions WHERE user_id=?", (uid,))
    if "caps" in payload:
        caps = {}
        try:
            for k, v in (payload["caps"] or {}).items():
                if k in CAP_TYPES:
                    caps[k] = bool(v) if CAP_TYPES[k] is bool else CAP_TYPES[k](v)
                    if CAP_TYPES[k] is not bool and caps[k] < 0:
                        raise ValueError
        except (TypeError, ValueError):
            return {"error": "Limits must be positive numbers."}
        x("UPDATE users SET caps=? WHERE id=?", (json.dumps(caps), uid))
    return {"ok": True}


@app.delete("/api/admin/users/{uid}")
def api_admin_delete(request: Request, uid: int):
    denied = need_admin(request)
    if denied:
        return denied
    if uid == request.state.user["id"]:
        return {"error": "You cannot delete your own account."}
    rows = q("SELECT role, active FROM users WHERE id=?", (uid,))
    if not rows:
        return {"error": "User not found."}
    if rows[0]["role"] == "admin" and rows[0]["active"] and active_admins() <= 1:
        return {"error": "There must be at least one active admin."}
    for t in ("sessions", "creds", "alerts"):
        x(f"DELETE FROM {t} WHERE user_id=?", (uid,))
    x("DELETE FROM users WHERE id=?", (uid,))
    for k in [k for k in ACCOUNTS if k[0] == uid]:
        ACCOUNTS.pop(k, None)
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


AUTH_CSS = """
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;--sell:#b3261e;--buy:#0b7a63}
@media(prefers-color-scheme:dark){:root{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80;--buy:#4cc9a8}}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,sans-serif;padding:16px}
form,.box{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:26px;width:min(380px,100%)}
h1{margin:0 0 4px;font-size:24px}p{margin:0 0 16px;color:var(--mute);font-size:14px}
label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:var(--mute);margin-bottom:12px}
input{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:9px 11px}
button{width:100%;font:inherit;font-weight:500;background:var(--ink);color:var(--bg);border:0;border-radius:8px;padding:10px;cursor:pointer}
button:disabled{opacity:.5}
#msg{min-height:20px;margin:10px 0 0;color:var(--sell);font-size:14px}
.alt{margin:14px 0 0;font-size:14px;display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}
a{color:var(--ink)}
.key{font:700 20px/1.3 ui-monospace,Menlo,monospace;letter-spacing:.05em;text-align:center;border:2px dashed var(--ink);border-radius:8px;padding:14px;margin:12px 0;user-select:all;word-break:break-all}
.warn{color:var(--sell);font-size:14px}
:focus-visible{outline:2px solid var(--ink);outline-offset:2px}
:root{--bg:#f3f1fb;--panel:#fff;--ink:#1c1b33;--mute:#6b6a86;--line:#e3e0f3;--sell:#e0264b;--buy:#00a77a;--acc:#6c4cf5;--g:linear-gradient(135deg,#6c4cf5,#00b8d9)}
@media(prefers-color-scheme:dark){:root{--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;--sell:#ff5c7c;--buy:#27f0b0;--acc:#8b6cff;--g:linear-gradient(135deg,#8b6cff,#2de2ff)}}
body{background:radial-gradient(900px 500px at 10% -10%,rgba(108,76,245,.28),transparent 60%),radial-gradient(800px 460px at 100% 100%,rgba(0,184,217,.22),transparent 55%),var(--bg)}
form,.box{border:0;border-radius:20px;box-shadow:0 20px 60px rgba(60,40,160,.25)}
h1{background:var(--g);-webkit-background-clip:text;background-clip:text;color:transparent;font-size:28px}
h1::before{content:"";display:inline-block;width:11px;height:24px;border-radius:4px;margin-right:10px;vertical-align:-4px;background:linear-gradient(var(--buy) 50%,var(--sell) 50%)}
button{background:var(--g);color:#fff;border-radius:10px;box-shadow:0 4px 14px rgba(108,76,245,.4)}
input:focus{outline:2px solid var(--acc)}
"""

AUTH_BODIES = {
 "login": """<form id="f"><h1>Signal board</h1><p>Sign in to continue.</p>
<label>Username<input id="u" autocomplete="username" required autofocus></label>
<label>Password<input id="p" type="password" autocomplete="current-password" required></label>
<button id="go">Sign in</button><div id="msg" role="alert"></div>
<div class="alt"><a href="/forgot">Forgot password?</a><!--SIGNUP--></div>
<div class="alt"><a href="/admin">Admin sign-in</a></div></form>""",
 "admin": """<form id="f"><h1>Admin console</h1><p>Sign in with an admin account.</p>
<label>Admin username<input id="u" autocomplete="username" required autofocus></label>
<label>Password<input id="p" type="password" autocomplete="current-password" required></label>
<button id="go">Sign in to the console</button><div id="msg" role="alert"></div>
<div class="alt"><a href="/login">Regular sign-in</a></div></form>""",
 "signup": """<form id="f"><h1>Create your account</h1><p>You start in paper mode. Live trading is switched on later by an admin.</p>
<label>Username<input id="u" autocomplete="username" required autofocus></label>
<label>Password (at least 10 characters)<input id="p" type="password" autocomplete="new-password" minlength="10" required></label>
<!--CODE-->
<button id="go">Create account</button><div id="msg" role="alert"></div>
<div class="alt"><a href="/login">I already have an account</a></div></form>
<div class="box" id="done" hidden><h1>Save your recovery key</h1>
<p>This key is the only way to reset your own password if you forget it. It is shown once. Write it down or store it in a password manager.</p>
<div class="key" id="key"></div><p class="warn">Anyone with this key and your username can reset your password.</p>
<button id="cont">I have saved it, continue</button></div>""",
 "forgot": """<form id="f"><h1>Reset your password</h1><p>Enter your username and the recovery key you saved when you signed up.</p>
<label>Username<input id="u" autocomplete="username" required autofocus></label>
<label>Recovery key<input id="k" autocomplete="off" placeholder="XXXX-XXXX-XXXX-XXXX" required></label>
<label>New password (at least 10 characters)<input id="p" type="password" autocomplete="new-password" minlength="10" required></label>
<button id="go">Reset password</button><div id="msg" role="alert"></div>
<div class="alt"><a href="/login">Back to sign in</a></div></form>
<div class="box" id="done" hidden><h1>Password changed</h1>
<p>Your old recovery key has been used up. Here is your new one. It is shown once.</p>
<div class="key" id="key"></div><button id="cont">I have saved it, sign in</button></div>""",
}

AUTH_JS = {
 "login": """const post=async(u,b)=>{const r=await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return [r.ok,await r.json()]};
f.onsubmit=async e=>{e.preventDefault();go.disabled=true;msg.textContent="";
 try{const [ok,d]=await post("/api/login",{username:u.value,password:p.value});if(ok)location.href="/";else msg.textContent=d.error||"Could not sign in."}
 catch(x){msg.textContent="Cannot reach the server."}finally{go.disabled=false}};""",
 "admin": """const post=async(u,b)=>{const r=await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return [r.ok,await r.json()]};
f.onsubmit=async e=>{e.preventDefault();go.disabled=true;msg.textContent="";
 try{const [ok,d]=await post("/api/login",{username:u.value,password:p.value,admin:true});if(ok)location.href="/?m=admin";else msg.textContent=d.error||"Could not sign in."}
 catch(x){msg.textContent="Cannot reach the server."}finally{go.disabled=false}};""",
 "signup": """const post=async(u,b)=>{const r=await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return [r.ok,await r.json()]};
f.onsubmit=async e=>{e.preventDefault();go.disabled=true;msg.textContent="";
 try{const c=document.getElementById("c");const [ok,d]=await post("/api/signup",{username:u.value,password:p.value,code:c?c.value:""});
  if(ok){f.hidden=true;key.textContent=d.recovery_key;done.hidden=false}else msg.textContent=d.error||"Could not sign up."}
 catch(x){msg.textContent="Cannot reach the server."}finally{go.disabled=false}};
cont.onclick=()=>{location.href="/"};""",
 "forgot": """const post=async(u,b)=>{const r=await fetch(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)});return [r.ok,await r.json()]};
f.onsubmit=async e=>{e.preventDefault();go.disabled=true;msg.textContent="";
 try{const [ok,d]=await post("/api/forgot",{username:u.value,key:k.value,password:p.value});
  if(ok){f.hidden=true;key.textContent=d.recovery_key;done.hidden=false}else msg.textContent=d.error||"Could not reset."}
 catch(x){msg.textContent="Cannot reach the server."}finally{go.disabled=false}};
cont.onclick=()=>{location.href="/login"};""",
}


def auth_page(kind):
    body = AUTH_BODIES[kind]
    if kind == "login":
        body = body.replace("<!--SIGNUP-->", '<a href="/signup">Create an account</a>' if ALLOW_SIGNUP else "")
    if kind == "signup":
        if not ALLOW_SIGNUP:
            body = '<div class="box"><h1>Sign-up is closed</h1><p>Ask the admin to create an account for you.</p><a href="/login">Back to sign in</a></div>'
        else:
            body = body.replace("<!--CODE-->", '<label>Invite code<input id="c" autocomplete="off" required></label>' if SIGNUP_CODE else "")
    title = {"login": "Sign in", "admin": "Admin console", "signup": "Create account", "forgot": "Reset password"}[kind]
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>{title}</title><style>{AUTH_CSS}</style></head><body>{body}<script>{AUTH_JS[kind]}</script></body></html>')


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in</title>
<style>
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;--sell:#b3261e}
@media(prefers-color-scheme:dark){:root{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80}}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,sans-serif}
form{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:26px;width:min(360px,92vw)}
h1{margin:0 0 4px;font-size:24px}p{margin:0 0 16px;color:var(--mute);font-size:14px}
label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:var(--mute);margin-bottom:12px}
input{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:9px 11px}
button{width:100%;font:inherit;font-weight:500;background:var(--ink);color:var(--bg);border:0;border-radius:8px;padding:10px;cursor:pointer}
button:disabled{opacity:.5}
#msg{min-height:20px;margin:10px 0 0;color:var(--sell);font-size:14px}
:focus-visible{outline:2px solid var(--ink);outline-offset:2px}
</style></head><body>
<form id="f"><h1>Signal board</h1><p>Sign in to continue.</p>
<label>Username<input id="u" autocomplete="username" required autofocus></label>
<label>Password<input id="p" type="password" autocomplete="current-password" required></label>
<button id="go">Sign in</button><div id="msg" role="alert"></div></form>
<script>
document.getElementById("f").onsubmit=async e=>{e.preventDefault();const b=document.getElementById("go"),m=document.getElementById("msg");b.disabled=true;m.textContent="";
 try{const r=await fetch("/api/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:document.getElementById("u").value,password:document.getElementById("p").value})});
  const d=await r.json();if(r.ok){location.href="/"}else{m.textContent=d.error||"Could not sign in."}}
 catch(x){m.textContent="Cannot reach the server."}finally{b.disabled=false}};
</script></body></html>
"""


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
.sectors{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px;margin-bottom:14px}
.sectors button{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:8px 12px;text-align:left;cursor:pointer;color:var(--ink);font:inherit;display:flex;flex-direction:column;gap:2px}
.sectors button[aria-pressed=true]{border-color:var(--ink);box-shadow:inset 0 0 0 1px var(--ink)}
.sectors .sn{font-size:13px;color:var(--mute);line-height:1.25}
.sectors .sc{font-size:18px;font-weight:700}
.sectors .ss{font-size:12px;color:var(--mute)}
.idx{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-bottom:10px}
.idx div{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:8px 12px;display:flex;flex-direction:column;gap:2px}
.idx .sn{font-size:13px;color:var(--mute);font-weight:600}.idx .sc{font-size:20px;font-weight:700}.idx .ss{font-size:13px;font-weight:600}
.idx:empty{display:none}
.sechead{display:flex;justify-content:space-between;align-items:baseline;margin:0 0 8px}
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
.ogrid{display:grid;grid-template-columns:230px minmax(0,1fr);gap:16px;align-items:start}
.olist{position:sticky;top:8px;display:flex;flex-direction:column;gap:8px}
.olist input,.olist select{background:var(--panel);color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:8px 12px;font:inherit;width:100%}
.orows{display:flex;flex-direction:column;border:1px solid var(--line);border-radius:10px;background:var(--panel);max-height:70vh;overflow-y:auto}
.orow{display:flex;justify-content:space-between;align-items:center;gap:8px;background:none;border:0;border-bottom:1px solid var(--line);padding:7px 12px;cursor:pointer;color:var(--ink);font:inherit;text-align:left}
.orow:last-child{border-bottom:0}
.orow[aria-pressed=true]{background:var(--flat);font-weight:700}
.orow .pill{min-width:0;padding:0 8px;font-size:12px}
.orow .none{color:var(--mute)}
.hgrid{display:grid;grid-template-columns:minmax(0,5fr) minmax(0,4fr);gap:14px;margin-bottom:14px}.hgrid.even{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.hgrid .card{margin:0;min-width:0}
@media(max-width:900px){.hgrid,.hgrid.even{grid-template-columns:minmax(0,1fr)}}
table.gt{min-width:0;width:100%;font-size:13px}table.gt th,table.gt td{padding:7px 8px;white-space:nowrap}
.hbar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
.hbar input,.hbar select{background:var(--panel);color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:8px 12px;font:inherit}
.hbar input{flex:1;min-width:160px;max-width:260px}
.hkpi{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-bottom:14px}
.hkpi div{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px;display:flex;flex-direction:column;gap:2px}
.hkpi .l{font-size:13px;color:var(--mute)}.hkpi .v{font-size:24px;font-weight:700}.hkpi .s{font-size:12px;color:var(--mute)}
.hkpi .bar2{height:6px;border-radius:3px;background:var(--line);overflow:hidden;margin-top:4px}.hkpi .bar2 i{display:block;height:100%;background:var(--buy)}
.chks{display:flex;flex-wrap:wrap;gap:4px 14px;margin:4px 0 10px}.chks.sec{max-height:150px;overflow:auto;border:1px solid var(--line);border-radius:8px;padding:6px 10px}
label.chk{display:inline-flex!important;flex-direction:row!important;gap:6px;align-items:center;font-size:14px;color:var(--ink);margin:0}
label.chk input{width:auto;margin:0}
.orow .ochg{margin-left:auto;font-size:12px}
.omain .more{display:inline-flex;gap:6px;align-items:center;margin:0 0 8px;color:var(--mute);font-size:13px}
.osig{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:8px 16px;margin-bottom:12px;display:flex;flex-direction:column;gap:6px}
.osl{display:flex;flex-wrap:wrap;gap:4px 14px;align-items:center;font-size:14px}
.osl .pill{min-width:0;padding:0 10px}
@media(max-width:800px){.ogrid{grid-template-columns:1fr}.olist{position:static}.orows{max-height:240px}}
.osum{display:flex;flex-wrap:wrap;gap:6px 24px;background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 16px;margin-bottom:12px}
.osum:not([hidden]){display:flex}
.stat{display:flex;flex-direction:column;gap:2px}
.stat span.l{color:var(--mute);font-size:13px}
.stat .lv{margin-right:12px;font-weight:400}
table.otab{min-width:0}
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
#chartDlg{width:min(1100px,96vw);max-width:96vw}
.chbox{padding:14px 16px}.chhead{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:8px}.chhead b{font-size:18px}
.chtfs{display:flex;gap:4px;margin-left:auto}.chtfs button{border:1px solid var(--line);background:var(--panel);color:var(--ink);border-radius:8px;padding:4px 12px;cursor:pointer;font:inherit}
.chtfs button[aria-pressed=true]{background:var(--ink);color:var(--panel)}
.chleg{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:12.5px;color:var(--mute);margin-bottom:6px}.chleg i{display:inline-block;width:14px;height:3px;border-radius:2px;margin-right:5px;vertical-align:middle}
#chart{height:min(62vh,560px);width:100%}
td.act{white-space:nowrap}
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
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px 18px;margin-bottom:14px}
.card h3{margin:0 0 8px;font-size:17px}.card h4{margin:0 0 8px;font-size:15px}
.card label{display:flex;flex-direction:column;gap:4px;font-size:13px;color:var(--mute);margin-bottom:8px}
.card input,.card select{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:7px 9px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.bcard{border-top:1px solid var(--line);padding-top:12px;margin-top:12px}
#usrTable{min-width:820px}#usrTable th,#usrTable td{text-align:left}
#who{font-size:13px}
@media(max-width:640px){.grid2{grid-template-columns:1fr}}
@media(max-width:640px){.tally{grid-template-columns:repeat(2,1fr)}}
/* ── Theme: vivid accents, glass cards, live signal flashing ── */
:root{--bg:#f3f1fb;--panel:#ffffff;--ink:#1c1b33;--mute:#6b6a86;--line:#e3e0f3;
--buy:#00a77a;--buy-bg:#d3f7ea;--sell:#e0264b;--sell-bg:#ffe0e7;--flat:#ece9f8;
--acc:#6c4cf5;--acc2:#00b8d9;--hot:#ff9f1a;--glowb:0,196,140;--glows:255,59,95;
--shadow:0 6px 20px rgba(76,56,180,.09);--g:linear-gradient(135deg,#6c4cf5,#00b8d9)}
@media(prefers-color-scheme:dark){:root{--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;
--buy:#27f0b0;--buy-bg:#0c3b31;--sell:#ff5c7c;--sell-bg:#451526;--flat:#201f3d;
--acc:#8b6cff;--acc2:#2de2ff;--hot:#ffb347;--shadow:0 6px 24px rgba(0,0,0,.45);--g:linear-gradient(135deg,#8b6cff,#2de2ff)}}
body{background:radial-gradient(1000px 520px at 8% -8%,rgba(108,76,245,.20),transparent 60%),radial-gradient(900px 480px at 100% 0,rgba(0,184,217,.16),transparent 55%),var(--bg);background-attachment:fixed}
h1{font-size:30px;font-weight:700;background:var(--g);-webkit-background-clip:text;background-clip:text;color:transparent;display:flex;align-items:center;gap:10px}
h1::before{content:"";width:12px;height:26px;border-radius:4px;background:linear-gradient(var(--buy) 50%,var(--sell) 50%);box-shadow:0 0 14px rgba(var(--glowb),.55)}
.btn{background:var(--g);color:#fff;border-radius:10px;padding:9px 16px;box-shadow:0 4px 14px rgba(108,76,245,.35);transition:transform .12s,box-shadow .12s}
.btn:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 8px 20px rgba(108,76,245,.45)}
.btn.go{background:linear-gradient(135deg,#00b37e,#00d4a0)}.btn.ghost{background:var(--panel);color:var(--ink);box-shadow:none;border:1px solid var(--line)}
.seg{border-radius:12px;box-shadow:var(--shadow)}
.seg button[aria-pressed=true]{background:var(--g);color:#fff}
.mode{border-bottom:0;gap:6px;background:var(--panel);padding:5px;border-radius:14px;box-shadow:var(--shadow);width:fit-content;max-width:100%;overflow-x:auto}
.mode button{border-radius:10px;border-bottom:0;margin:0;padding:8px 16px}
.mode button[aria-pressed=true]{background:var(--g);color:#fff}
.mode button:hover{background:var(--flat)}.mode button[aria-pressed=true]:hover{background:var(--g)}
.idx div{border:0;box-shadow:var(--shadow);position:relative;overflow:hidden;background:linear-gradient(160deg,var(--panel),var(--flat))}
.idx div::after{content:"";position:absolute;right:-18px;top:-18px;width:70px;height:70px;border-radius:50%;background:var(--g);opacity:.18}
.sectors button{border:0;box-shadow:var(--shadow);transition:transform .12s,box-shadow .12s}
.sectors button:hover{transform:translateY(-2px)}
.sectors button.pos{background:linear-gradient(160deg,var(--panel) 40%,rgba(var(--glowb),.16))}
.sectors button.neg{background:linear-gradient(160deg,var(--panel) 40%,rgba(var(--glows),.14))}
.sectors button[aria-pressed=true]{box-shadow:0 0 0 2px var(--acc),0 8px 22px rgba(108,76,245,.28)}
.tally{border:0;box-shadow:var(--shadow);border-radius:14px}
.tally button{border-right-color:var(--line)}
.tally button[aria-pressed=true]{background:linear-gradient(160deg,var(--flat),transparent)}
.tally .buy b{text-shadow:0 0 16px rgba(var(--glowb),.4)}.tally .sell b{text-shadow:0 0 16px rgba(var(--glows),.4)}
.wrap,.card,.hkpi div,.osig,.orows,.stat{border:0!important;box-shadow:var(--shadow);border-radius:14px}
.wrap{overflow-x:auto}
thead th{background:linear-gradient(180deg,var(--flat),transparent);color:var(--mute);font-weight:700;text-transform:uppercase;font-size:11.5px;letter-spacing:.06em}
tbody tr{transition:background .15s}tbody tr:hover td{background:rgba(108,76,245,.07)}
td{border-bottom-color:var(--line)}
.pill{font-weight:700}
.pill.BUY{background:linear-gradient(135deg,#00b37e,#00dba5);color:#fff;box-shadow:0 0 14px rgba(var(--glowb),.55)}
.pill.SELL{background:linear-gradient(135deg,#e0264b,#ff6a86);color:#fff;box-shadow:0 0 14px rgba(var(--glows),.55)}
.pill.Open_BUY{background:var(--buy-bg)}.pill.Open_SELL{background:var(--sell-bg)}
.mini{border-radius:8px;border:1px solid var(--acc);color:var(--acc);background:transparent;padding:3px 10px;cursor:pointer;font-weight:600;transition:background .12s,color .12s}
.mini:hover{background:var(--acc);color:#fff}
button.mini[data-trade]{background:var(--g);color:#fff;border-color:transparent}
.banner.paper{background:linear-gradient(90deg,rgba(108,76,245,.14),rgba(0,184,217,.12));border:0}
.banner.live{background:linear-gradient(90deg,rgba(255,59,95,.18),rgba(255,159,26,.16));border:0}
input:focus,select:focus{outline:2px solid var(--acc);outline-offset:1px}
dialog{border:0;border-radius:18px;box-shadow:0 24px 70px rgba(30,20,90,.45)}
dialog::backdrop{background:rgba(14,10,40,.6);backdrop-filter:blur(3px)}
.tkreview{background:var(--flat);border-radius:10px}
.tag{background:var(--g);color:#fff!important;border-radius:6px}
/* signal flashing */
.newb{display:inline-block;margin-left:8px;padding:1px 7px;border-radius:999px;font-size:10.5px;font-weight:800;letter-spacing:.08em;color:#fff;background:linear-gradient(135deg,var(--hot),#ff4d6d);animation:blink 1s steps(2,start) infinite;vertical-align:middle}
tr.fresh.buy td:first-child{box-shadow:inset 4px 0 0 var(--buy)}tr.fresh.sell td:first-child{box-shadow:inset 4px 0 0 var(--sell)}
tr.fresh.buy td{animation:pulseb 2.2s ease-in-out infinite}tr.fresh.sell td{animation:pulses 2.2s ease-in-out infinite}
tr.fresh .pill{animation:glow 1.6s ease-in-out infinite}
tr.flash td{animation:flashrow .6s ease-in-out 12!important;background:rgba(255,159,26,.30)}
tr.flash td:first-child{box-shadow:inset 6px 0 0 var(--hot)}
@keyframes blink{50%{opacity:.25}}
@keyframes pulseb{0%,100%{background-color:transparent}50%{background-color:rgba(var(--glowb),.13)}}
@keyframes pulses{0%,100%{background-color:transparent}50%{background-color:rgba(var(--glows),.12)}}
@keyframes flashrow{0%,100%{background-color:rgba(255,159,26,.10)}50%{background-color:rgba(255,159,26,.55)}}
@keyframes glow{50%{filter:brightness(1.25);transform:scale(1.05)}}
#toast{position:fixed;top:14px;left:50%;transform:translate(-50%,-140%);z-index:50;max-width:min(560px,94vw);padding:12px 18px;border-radius:14px;color:#fff;font-weight:600;background:linear-gradient(135deg,#6c4cf5,#ff4d6d);box-shadow:0 12px 34px rgba(80,40,160,.5);cursor:pointer;transition:transform .35s cubic-bezier(.2,1.2,.4,1)}
#toast.show{transform:translate(-50%,0)}
#toast small{display:block;font-weight:500;opacity:.92;margin-top:2px}
#snd{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:6px 10px;cursor:pointer}
#snd[aria-pressed=true]{background:var(--g);color:#fff;border-color:transparent}
@media(prefers-reduced-motion:reduce){.newb,tr.fresh td,tr.fresh .pill,tr.flash td{animation:none!important}#toast{transition:none}.btn,.sectors button{transition:none}}
</style></head><body><main>
<header>
 <div><h1>Signal board</h1><div class="sub" id="meta">Loading first scan…</div></div>
 <div class="ctl"><div class="seg" id="tfs" role="group" aria-label="Timeframe"></div><button class="btn" id="refresh">Refresh now</button><button id="snd" aria-pressed="false" title="Play a sound when a new signal arrives">🔔 Sound off</button><span class="sub" id="who"></span><button class="mini" id="logout">Sign out</button></div>
</header>
<div id="toast" role="status" aria-live="polite"></div>
<div class="banner paper" id="banner"></div>
<div class="banner live" id="dpBanner" hidden>You are signed in with the default admin password. Change it under Account now. Anyone who knows it can sign in as admin.</div>
<nav class="mode" id="mode" aria-label="View"><button data-m="stocks">Stocks</button><button data-m="futures">Futures</button><button data-m="options">Options</button><button data-m="history">History</button><button data-m="orders">Orders</button><button data-m="account">Account</button><button data-m="admin" id="navAdmin" hidden>Admin</button></nav>
<section id="view-stocks">
<div class="idx" id="idxTiles"></div>
<div class="sechead"><b>Sectors</b><span class="sub" id="secnote"></span></div>
<div class="sectors" id="sectors"></div>
<div class="tally" id="tally"></div>
<div class="bar"><input id="q" placeholder="Search symbol" aria-label="Search symbol"></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table></div>
<div class="note" id="note"></div>
</section>
<section id="view-options" hidden>
<div class="ogrid">
 <aside class="olist" aria-label="Scripts">
  <input id="osym" placeholder="Search a stock or index" aria-label="Search scripts" autocomplete="off">
  <select id="osec" aria-label="Sector"><option value="ALL">All sectors</option></select>
  <select id="ofilt" aria-label="Show"><option value="all">All scripts</option><option value="sig">With a signal</option><option value="BUY">Buy signals</option><option value="SELL">Sell signals</option></select>
  <div class="orows" id="chips"></div>
 </aside>
 <div class="omain">
  <div class="osum" id="osum" hidden></div>
  <label class="more"><input type="checkbox" id="omore"> Show volume and IV</label>
  <div class="wrap"><table class="otab" id="otable"></table></div>
  <div class="note" id="onote">Pick a script on the left. Every F&amp;O stock and index has a chain. Sensex and Bankex option chains are not available here.</div>
 </div>
</div>
</section>
<section id="view-history" hidden>
<div class="hbar">
 <input id="hSym" placeholder="Search symbol" aria-label="Search symbol">
 <select id="hSec" aria-label="Sector"><option value="ALL">All sectors</option></select>
 <select id="hSide" aria-label="Side"><option value="">Buy and sell</option><option value="BUY">Buy only</option><option value="SELL">Sell only</option></select>
 <select id="hDays" aria-label="Period"><option value="7">Last 7 days</option><option value="30">Last 30 days</option><option value="0" selected>All loaded candles</option></select>
</div>
<div class="note" id="hNote" style="margin:0 0 10px"></div>
<div class="hkpi" id="hKpi"></div>
<div class="hgrid">
 <div class="card"><h3>By sector</h3><div class="wrap"><table id="hSector"></table></div></div>
 <div class="card"><h3>By side</h3><div class="wrap"><table id="hSideT"></table></div><h3 style="margin-top:14px" id="hHourH">By hour of day</h3><div class="wrap"><table id="hHour"></table></div></div>
</div>
<div class="hgrid even">
 <div class="card"><h3>Best symbols <span class="sub">(3 or more signals, ranked by R at T2)</span></h3><div class="wrap"><table id="hBest"></table></div></div>
 <div class="card"><h3>Weakest symbols</h3><div class="wrap"><table id="hWorst"></table></div></div>
</div>
<div class="card"><h3>Every signal</h3><div class="wrap"><table id="hList"></table></div></div>
<div class="note" id="hFoot">How to read this: each signal is followed from its candle's close. It counts as a win for a target if price reached that target before the stop-loss. If the stop and a target are touched in the same candle, the stop is counted first. Signals that have reached neither yet are open and left out of that target's rate. "R per trade" is the average result if you always exited at that target: a win is worth that many R, a loss is minus 1R. Slippage, brokerage and gaps are not included, signals that overlap are each counted on their own, and the history only goes as far back as Yahoo's candles (about 30 days on 5 min, 60 days on 15 min, 2 years on Daily). Past results do not promise future ones.</div>
</section>
<section id="view-orders" hidden>
<div class="note" id="ordNote" style="margin:0 0 10px">Orders placed from this dashboard today.</div>
<div class="wrap"><table id="ordTable"></table></div>
<div style="margin-top:12px"><button class="mini" id="chk">Check broker connection</button> <span class="sub" id="chkMsg"></span></div>
</section>
<section id="view-account" hidden>
 <div class="card"><h3>Your account</h3><div class="sub" id="acctInfo"></div></div>
 <div class="grid2">
  <div class="card"><h3>Change password</h3>
   <label>Current password<input id="pwCur" type="password" autocomplete="current-password"></label>
   <label>New password (at least 10 characters)<input id="pwNew" type="password" autocomplete="new-password"></label>
   <div class="tkbtns"><span class="sub" id="pwMsg"></span><button class="btn" id="pwBtn">Change password</button></div></div>
  <div class="card"><h3>Order PIN</h3>
   <div class="sub" style="margin-bottom:8px">Asked before every live order, and when you cancel one. It is separate from your password.</div>
   <label>Your account password<input id="pinPw" type="password" autocomplete="current-password"></label>
   <label>New order PIN<input id="pinNew" type="password" autocomplete="off"></label>
   <div class="tkbtns"><span class="sub" id="pinMsg"></span><button class="btn" id="pinBtn">Save PIN</button></div></div>
 </div>
 <div class="card"><h3>Recovery key</h3>
  <div class="sub" style="margin-bottom:8px" id="recInfo"></div>
  <label>Your account password<input id="recPw" type="password" autocomplete="current-password"></label>
  <div class="tkbtns"><span class="sub" id="recMsg"></span><button class="btn" id="recBtn">Create a new recovery key</button></div></div>
 <div class="card" id="alertCard"><h3>Signal alerts</h3>
  <div class="sub" style="margin-bottom:10px">Get a message on your phone when a new BUY or SELL signal appears, so you do not have to watch the screen. Messages say what the indicator saw. They are not advice, so review each one before trading.</div>
  <label class="chk"><input type="checkbox" id="alOn"> Send me alerts</label>
  <div class="grid2">
   <div><b class="sub">Timeframes</b><div id="alTf" class="chks"></div>
    <b class="sub">Signals</b><div id="alSide" class="chks"><label class="chk"><input type="checkbox" value="BUY"> Buy</label><label class="chk"><input type="checkbox" value="SELL"> Sell</label></div></div>
   <div><b class="sub">Sectors <span style="font-weight:400">(none ticked means all)</span></b><div id="alSec" class="chks sec"></div></div>
  </div>
  <div class="grid2">
   <div class="bcard"><h4>Telegram <span class="tag" id="alTgTag"></span></h4>
    <div class="sub" id="alTgInfo"></div>
    <div class="tkbtns"><button class="mini" id="alTgUnlink" hidden>Unlink</button><button class="btn" id="alTgLink">Link Telegram</button></div></div>
   <div class="bcard"><h4>ntfy app <span class="tag">No account needed</span></h4>
    <div class="sub">Install the free ntfy app, subscribe to a topic name only you know, and type it here. Use a long, hard-to-guess name: anyone who knows it can read your alerts.</div>
    <label>Topic<input id="alNtfy" autocomplete="off" placeholder="e.g. amit-signals-7f3k9x"></label>
    <div class="tkbtns"><button class="mini" id="alRand">Make a random topic</button></div></div>
  </div>
  <div class="tkbtns"><span class="sub" id="alMsg"></span><button class="btn ghost" id="alTest">Send a test</button><button class="btn" id="alSave">Save alerts</button></div></div>
 <div class="card"><h3>Broker accounts</h3>
  <div class="note" style="margin:0 0 6px">Your broker details are stored encrypted on this server and are never shown again. Brokers accept API orders only from a static IP registered to your own client account, so register this server's IP with your broker. Sharing one IP between different clients is restricted: check your broker's rules.</div>
  <div id="brokerCards"></div></div>
</section>
<section id="view-admin" hidden>
 <div class="card"><h3>Overview</h3><div class="osum" id="admOv" style="margin-bottom:0"></div></div>
 <div class="card"><h3>Everyone's orders today</h3><div class="wrap"><table id="admOrd"></table></div></div>
 <div class="card"><h3>Add a user</h3>
  <div class="grid2" style="margin-bottom:0"><label>Username<input id="nuName" autocomplete="off"></label><label>Temporary password (at least 10 characters)<input id="nuPw" type="password" autocomplete="new-password"></label></div>
  <div class="tkbtns"><select id="nuRole" aria-label="Role"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn" id="addUser">Add user</button></div></div>
 <div class="wrap"><table id="usrTable"></table></div>
 <div class="note" id="admNote">Anyone can create their own account unless you set ALLOW_SIGNUP=0 (or require an invite code with SIGNUP_CODE). New accounts start in paper mode. Live trading is off for every new user until you enable it here, and it also needs LIVE_TRADING=1 on the server. Brokers allow one static IP per client, and sharing it is limited to family members. A platform that trades for unrelated clients has to be an exchange-empanelled algo provider. Enable live trading only where that is allowed, and check with your brokers.</div>
</section>
</main>
<dialog id="limDlg"><div class="tk">
 <h2 id="limTitle">Limits</h2>
 <div class="tkgrid">
  <label>Most rupees per stock order or option buy<input id="limValue" type="number" min="0" step="1000"></label>
  <label>Most shares per order<input id="limQty" type="number" min="0" step="1"></label>
  <label>Most orders per day<input id="limOrders" type="number" min="0" step="1"></label>
  <label>Most lots per futures order or option sell<input id="limLots" type="number" min="0" step="1"></label>
  <label>Most rupees per futures order or option sell (notional)<input id="limFut" type="number" min="0" step="10000"></label>
  <label>Allow selling options<select id="limSell"><option value="1">Yes</option><option value="0">No</option></select></label>
 </div>
 <div class="tkmsg" id="limMsg" role="status"></div>
 <div class="tkbtns"><button class="btn ghost" id="limClose">Close</button><button class="btn" id="limSave">Save</button></div>
</div></dialog>
<dialog id="chartDlg"><div class="chbox">
 <div class="chhead"><b id="chTitle"></b><div class="chtfs" id="chTfs"></div><button class="btn ghost" id="chClose">Close</button></div>
 <div class="chleg" id="chLeg"></div>
 <div id="chMsg" class="tkmsg" role="status"></div>
 <div id="chart"></div>
</div></dialog>
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
 <div class="tkgrid" id="tkAutoBox" hidden>
  <label class="chk"><input type="checkbox" id="tkAuto"> Place the stop-loss automatically after the fill</label>
  <label class="chk" id="tkTrailL"><input type="checkbox" id="tkTrail"> Trail it up as targets are reached</label>
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
let csrf="",me=null;
const api=async(u,o={})=>{o.credentials="same-origin";o.headers={...(o.headers||{}),"X-CSRF-Token":csrf};const r=await fetch(u,o);if(r.status===401){location.href="/login";throw new Error("signed out")}return r};
const colsStocks=[["symbol","Symbol"],["signal","Signal"],["status","Status"],["signal_time","Signal candle"],["_t",""],["close","Close"],["chg","Day %"],["entry","Entry"],["sl","SL"],["tp1","T1"],["tp2","T2"],["tp3","T3"],["tp4","T4"]];
const colsFut=[["symbol","Symbol"],["signal","Signal"],["status","Status"],["signal_time","Signal candle"],["_t",""],["close","Spot"],["chg","Day %"],["entry","Entry"],["sl","SL"],["tp1","T1"],["tp2","T2"],["tp3","T3"],["tp4","T4"],["lot","Lot"],["fexp","Expiry"]];
let data=null,sector="ALL",filter="ALL",sortKey="signal",sortDir=1,q="",timer=null,tf=null,futInfo={},futLoaded=false;
try{tf=localStorage.getItem("tf")}catch(e){}
const tfName={"5m":"5 min","15m":"15 min","1d":"Daily"};
const rank={BUY:0,SELL:1,"Open BUY":2,"Open SELL":2,"–":3};
const fmt=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const curCols=()=>mode==="futures"?colsFut:colsStocks;
function renderHead(){document.getElementById("head").innerHTML=curCols().map(c=>`<th data-k="${c[0]}">${c[1]}</th>`).join("")}
document.getElementById("head").onclick=e=>{const k=e.target.dataset.k;if(!k||k==="_t")return;sortDir=sortKey===k?-sortDir:1;sortKey=k;render()};
document.getElementById("q").oninput=e=>{q=e.target.value.trim().toUpperCase();render()};
document.getElementById("refresh").onclick=async()=>{const b=document.getElementById("refresh");b.disabled=true;await api("/api/refresh?tf="+(tf||""),{method:"POST"});setTimeout(()=>{load();b.disabled=false},4000)};
document.getElementById("tfs").onclick=e=>{const b=e.target.closest("button");if(!b||b.dataset.tf===tf)return;
 tf=b.dataset.tf;try{localStorage.setItem("tf",tf)}catch(x){}
 data=null;document.getElementById("body").innerHTML=`<tr><td colspan="14" class="empty">Scanning ${tfName[tf]} candles…</td></tr>`;
 document.querySelectorAll("#tfs button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.tf===tf));load();if(osym)loadChain();if(mode==="history")loadHistory()};
async function load(){clearTimeout(timer);const asked=tf;
 try{const d=await (await api("/api/signals"+(asked?"?tf="+asked:""))).json();if(asked!==tf)return;data=d;tf=d.interval;detectNew(d);render();
  timer=setTimeout(load,(d.refreshing||!d.updated)?3000:20000)}
 catch(e){document.getElementById("meta").textContent="Cannot reach the server.";timer=setTimeout(load,10000)}}
async function loadFutInfo(){if(futLoaded)return;futLoaded=true;
 try{const d=await (await api("/api/futures/info")).json();futInfo=d.info||{};if(!d.ok)futLoaded=false;render()}catch(e){futLoaded=false}}
const isOpen=r=>r.signal.startsWith("Open");
function cellFor(r,k){
 const tp=n=>`<td class="${r.hits>=n?"up":""}">${fmt(r["tp"+n])}${r.hits>=n?" ✓":""}</td>`;
 switch(k){
  case "symbol":return `<td><b>${esc(r.symbol)}</b>${r.type==="index"?'<span class="tag">Index</span>':""}${r.signal==="BUY"||r.signal==="SELL"?'<span class="newb">NEW</span>':""}</td>`;
  case "sector":return `<td class="sub">${esc(r.sector||"")}</td>`;
  case "chg":{const c=r.ltp_chg!=null?r.ltp_chg:r.chg;return c==null?"<td>–</td>":`<td class="${c>=0?"up":"dn"}">${c>0?"+":""}${c.toFixed(2)}%</td>`}
  case "signal":return `<td><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span></td>`;
  case "close":return `<td>${r.ltp!=null?`<b title="Live broker price. Candle close ${fmt(r.close)}">${fmt(r.ltp)}</b>`:fmt(r.close)}</td>`;
  case "entry":return `<td>${fmt(r[k])}</td>`;
  case "sl":return `<td class="${r.sl_hit?"dn":""}">${fmt(r.sl)}${r.sl_hit?" ✖":""}</td>`;
  case "tp1":return tp(1);case "tp2":return tp(2);case "tp3":return tp(3);case "tp4":return tp(4);
  case "status":return `<td>${esc(r.status)}</td>`;
  case "signal_time":return `<td>${esc(r.signal_time)}</td>`;
  case "lot":return `<td>${r.lot??"–"}</td>`;
  case "fexp":return `<td>${r.fexp??"–"}</td>`;
  default:return `<td class="act"><button class="mini" data-chart="${esc(r.symbol)}" title="Open chart">Chart</button> ${r.entry!=null&&r.signal!=="–"?`<button class="mini" data-trade="${esc(r.symbol)}">Trade</button>`:""}</td>`}}
function render(){
 if(!data)return;
 document.getElementById("tfs").innerHTML=data.intervals.map(x=>`<button data-tf="${x}" aria-pressed="${x===tf}">${tfName[x]||x}</button>`).join("");
 renderChips();
 const fut=mode==="futures";
 const rows=fut?data.rows:data.rows.filter(r=>r.type!=="index");
 rows.forEach(r=>{const f=futInfo[r.symbol];r.lot=f?f.lot:null;r.fexp=f?f.expiry:null});
 // sector tiles: average day change of the symbols in each sector
 const grp={};rows.forEach(r=>{const k=r.sector||"Others";(grp[k]=grp[k]||[]).push(r)});
 if(sector!=="ALL"&&!grp[sector])sector="ALL";
 const avg=a=>{const v=a.filter(r=>r.chg!=null).map(r=>r.chg);return v.length?v.reduce((x,y)=>x+y,0)/v.length:null};
 const pc=n=>n==null?"–":(n>0?"+":"")+n.toFixed(2)+"%";
 const tile=(key,name,a)=>{const m=avg(a),up=a.filter(r=>r.chg>0).length,dn=a.filter(r=>r.chg<0).length,b=a.filter(r=>r.signal==="BUY").length,se=a.filter(r=>r.signal==="SELL").length;
  return `<button class="${m==null?"":m>=0?"pos":"neg"}" data-sec="${esc(key)}" aria-pressed="${sector===key}"><span class="sn" title="${esc(name)}">${esc(name)}</span><span class="sc ${m==null?"":m>=0?"up":"dn"}">${pc(m)}</span><span class="ss">${a.length} · ${up} up, ${dn} down</span>${b||se?`<span class="ss">${b} buy, ${se} sell</span>`:""}</button>`};
 const order=Object.keys(grp).sort((a,b)=>(avg(grp[b])??-1e9)-(avg(grp[a])??-1e9));
 const sc=document.getElementById("sectors");
 sc.innerHTML=order.map(k=>tile(k,k,grp[k])).join("");
 sc.onclick=e=>{const b=e.target.closest("button");if(b){sector=sector===b.dataset.sec?"ALL":b.dataset.sec;render()}};
 const ix=["NIFTY","BANKNIFTY","SENSEX"].map(n=>data.rows.find(r=>r.symbol===n)).filter(Boolean);
 document.getElementById("idxTiles").innerHTML=ix.map(r=>{const c=r.ltp_chg!=null?r.ltp_chg:r.chg,px=r.ltp!=null?r.ltp:r.close;
  return `<div><span class="sn">${esc(r.symbol)}</span><span class="sc ${c==null?"":c>=0?"up":"dn"}">${fmt(px)}</span><span class="ss ${c==null?"":c>=0?"up":"dn"}">${c==null?"–":(c>0?"+":"")+c.toFixed(2)+"%"}${r.signal&&r.signal!=="–"?` · ${esc(r.signal)}`:""}</span></div>`}).join("");
 document.getElementById("secnote").textContent="Average change since the previous session's close, best sector first. Click a sector to filter, click again to clear.";
 const srows=sector==="ALL"?rows:grp[sector];
 const cnt=s=>srows.filter(r=>r.signal===s).length;
 const tabs=[["ALL","All symbols",srows.length,""],["BUY","Buy",cnt("BUY"),"buy"],["SELL","Sell",cnt("SELL"),"sell"],["OPEN","Open trades",srows.filter(isOpen).length,""]];
 const t=document.getElementById("tally");
 t.innerHTML=tabs.map(x=>`<button class="${x[3]}" data-f="${x[0]}" aria-pressed="${filter===x[0]}"><b>${x[2]}</b><span>${x[1]}</span></button>`).join("");
 t.onclick=e=>{const b=e.target.closest("button");if(b){filter=b.dataset.f;render()}};
 document.getElementById("meta").textContent=(data.updated?`Updated ${data.updated} · `:"")+`${tfName[data.interval]} candles · ${data.settings}`+(data.refreshing?(data.progress?` · scanning ${data.progress[0]} of ${data.progress[1]}…`:" · scanning…"):"");
 let v=rows.filter(r=>(filter==="ALL"||(filter==="OPEN"?isOpen(r):r.signal===filter))&&(!q||r.symbol.includes(q))&&(sector==="ALL"||(r.sector||"Others")===sector));
 v.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];if(sortKey==="signal"){x=rank[x];y=rank[y];if(x===y){x=a.bars_ago??999;y=b.bars_ago??999}}
  if(x==null)x=1e9;if(y==null)y=1e9;return (x>y?1:x<y?-1:0)*sortDir});
 const cs=curCols();
 document.getElementById("body").innerHTML=v.length?v.map(r=>`<tr${rowCls(r)}>${cs.map(c=>cellFor(r,c[0])).join("")}</tr>`).join(""):`<tr><td colspan="${cs.length}" class="empty">${data.updated?"No symbols match this filter.":"First scan is running. This page updates on its own."}</td></tr>`;
 document.getElementById("note").textContent=(data.errors.length?`${data.errors.length} symbol(s) skipped: ${data.errors.slice(0,3).join("; ")}${data.errors.length>3?"…":""}. `:"")+`BUY and SELL show for ${data.fresh_bars} candles after a signal. Open trades are older signals whose stop and T4 have not been reached. Entry, SL and targets come from the latest signal for each symbol.`+(fut?" Futures signals come from the same indicator on the stock's or index's own candles (spot prices). The lot size and expiry shown are for the nearest-month contract, from Angel One's instrument list.":"");
}
// ── Options tab ──
let mode="stocks",osym="",oexp="",oreq=0;
try{mode=localStorage.getItem("mode")||"stocks"}catch(e){}
const esc=s=>String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
const fk=n=>{if(n==null)return "–";const a=Math.abs(n),g=n<0?"-":"";return g+(a>=1e7?(a/1e7).toFixed(2)+" Cr":a>=1e5?(a/1e5).toFixed(2)+" L":a>=1e3?(a/1e3).toFixed(1)+" K":String(Math.round(a)))};
const f2=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const sgn=n=>n==null?"–":`<span class="${n>=0?"up":"dn"}">${n>0?"+":""}${fk(n)}</span>`;
function setMode(m){if(m==="admin"&&!(me&&me.user.role==="admin"))m="stocks";mode=m;try{localStorage.setItem("mode",m)}catch(e){}
 const vm={stocks:"stocks",futures:"stocks",options:"options",history:"history",orders:"orders",account:"account",admin:"admin"};
 ["stocks","options","history","orders","account","admin"].forEach(x=>document.getElementById("view-"+x).hidden=x!==vm[m]);
 document.querySelectorAll("#mode button").forEach(b=>b.setAttribute("aria-pressed",b.dataset.m===m));
 if(m==="orders")loadOrders();if(m==="options")renderChips();if(m==="history")loadHistory();if(m==="account"){loadMe();loadAlerts()}if(m==="admin")loadUsers();
 if(m==="stocks"||m==="futures"){filter="ALL";q="";document.getElementById("q").value="";renderHead();if(m==="futures")loadFutInfo();render()}}
document.getElementById("mode").onclick=e=>{const b=e.target.closest("button");if(b)setMode(b.dataset.m)};
function renderChips(){
 if(!data)return;
 const box=document.getElementById("chips"),keep=box.scrollTop;
 const by={};data.rows.forEach(r=>by[r.symbol]=r);
 const all=(data.indices||[]).concat(data.symbols||[]);
 const text=document.getElementById("osym").value.trim().toUpperCase(),fl=document.getElementById("ofilt").value;
 const ss=document.getElementById("osec"),sec=ss.value;
 const secs=[...new Set(data.rows.map(r=>r.sector||"Others"))].sort();
 if(ss.options.length!==secs.length+1){ss.innerHTML='<option value="ALL">All sectors</option>'+secs.map(x=>`<option>${esc(x)}</option>`).join("");ss.value=secs.includes(sec)?sec:"ALL"}
 const list=all.filter(n=>{const sg=(by[n]||{}).signal||"–";
  if(text&&!n.includes(text))return false;
  if(sec!=="ALL"&&((by[n]||{}).sector||(data.indices||[]).includes(n)&&"Indices"||"Others")!==sec)return false;
  return fl==="all"||(fl==="sig"?sg!=="–":sg.includes(fl))});
 list.sort((a,b)=>{const ra=by[a]?rank[by[a].signal]:99,rb=by[b]?rank[by[b].signal]:99;return (ra??99)-(rb??99)||a.localeCompare(b)});
 box.innerHTML=list.length?list.map(n=>{const sg=(by[n]||{}).signal||"–";
  const c=(by[n]||{}).chg;
  return `<button class="orow" data-s="${esc(n)}" aria-pressed="${n===osym}"><span>${esc(n)}</span><span class="ochg ${c==null?"":c>=0?"up":"dn"}">${c==null?"":(c>0?"+":"")+c.toFixed(2)+"%"}</span>${sg==="–"?'<span class="none">–</span>':`<span class="pill ${sg.replace(" ","_")}">${sg}</span>`}</button>`}).join(""):`<div class="sub" style="padding:12px">No scripts match.</div>`;
 box.scrollTop=keep;
}
document.getElementById("chips").onclick=e=>{const b=e.target.closest(".orow");if(b){osym=b.dataset.s;oexp="";loadChain()}};
document.getElementById("osym").oninput=renderChips;
document.getElementById("ofilt").onchange=renderChips;
document.getElementById("osec").onchange=renderChips;
document.getElementById("osym").onkeydown=e=>{if(e.key!=="Enter")return;
 const t=e.target.value.trim().toUpperCase();if(!t)return;
 const all=((data&&data.indices)||[]).concat((data&&data.symbols)||[]);
 osym=all.includes(t)?t:(all.find(n=>n.startsWith(t))||t);oexp="";loadChain()};
document.getElementById("omore").onchange=()=>{if(lastChain)renderChain(lastChain)};
async function loadChain(quiet){
 if(!osym)return;const my=++oreq,box=document.getElementById("onote");
 if(!quiet){document.getElementById("otable").innerHTML="";document.getElementById("osum").hidden=true;box.textContent=`Loading the ${osym} option chain…`}
 renderChips();
 try{const u=`/api/options/chain?symbol=${encodeURIComponent(osym)}&tf=${tf||""}`+(oexp?`&expiry=${encodeURIComponent(oexp)}`:"");
  const d=await (await api(u)).json();if(my!==oreq)return;renderChain(d)}
 catch(e){if(my===oreq)box.textContent="Cannot reach the server."}
}
// ── new-signal flashing, toast, page title and optional sound ──
const seenSig={},flashUntil={};let newCount=0,sndOn=false,aud=null;
try{sndOn=localStorage.getItem("snd")==="1"}catch(e){}
const sigKey=r=>r.symbol+"|"+r.signal+"|"+r.signal_time;
const isFresh=r=>r.signal==="BUY"||r.signal==="SELL";
function rowCls(r){if(!isFresh(r))return "";const f=(flashUntil[tf+"|"+sigKey(r)]||0)>Date.now();return ` class="fresh ${r.signal==="BUY"?"buy":"sell"}${f?" flash":""}"`}
function beep(){try{aud=aud||new (window.AudioContext||window.webkitAudioContext)();[880,1175].forEach((f,i)=>{const o=aud.createOscillator(),g=aud.createGain();o.frequency.value=f;o.connect(g);g.connect(aud.destination);const t=aud.currentTime+i*.16;g.gain.setValueAtTime(.12,t);g.gain.exponentialRampToValueAtTime(.001,t+.14);o.start(t);o.stop(t+.15)})}catch(e){}}
function detectNew(d){const fresh=d.rows.filter(isFresh);const first=!seenSig[d.interval];const known=seenSig[d.interval]=seenSig[d.interval]||new Set();const add=[];
 fresh.forEach(r=>{const k=sigKey(r);if(!known.has(k)){known.add(k);if(!first)add.push(r)}});
 if(!add.length)return;add.forEach(r=>{flashUntil[d.interval+"|"+sigKey(r)]=Date.now()+8000});setTimeout(render,8200);
 const t=$("toast");t.innerHTML=`${add.length} new signal${add.length>1?"s":""} · ${tfName[d.interval]||d.interval}<small>${add.slice(0,6).map(r=>r.signal+" "+esc(r.symbol)).join(", ")}${add.length>6?" …":""}</small>`;
 t.classList.add("show");clearTimeout(t._h);t._h=setTimeout(()=>t.classList.remove("show"),9000);
 if(document.hidden){newCount+=add.length;document.title=`(${newCount}) New signals · Signal board`}
 if(sndOn)beep()}
function initFlash(){const b=$("snd");const paint=()=>{b.setAttribute("aria-pressed",String(sndOn));b.textContent=sndOn?"🔔 Sound on":"🔕 Sound off"};paint();
 b.onclick=()=>{sndOn=!sndOn;try{localStorage.setItem("snd",sndOn?"1":"0")}catch(e){}paint();if(sndOn)beep()};
 $("toast").onclick=()=>$("toast").classList.remove("show");
 document.addEventListener("visibilitychange",()=>{if(!document.hidden){newCount=0;document.title="Signal board"}})}
// ── chart popup (TradingView's open-source Lightweight Charts) ──
let lwPromise=null,chSym="",chTf="",chObj=null;
function loadLW(){if(window.LightweightCharts)return Promise.resolve();
 return lwPromise||(lwPromise=new Promise((ok,no)=>{const s=document.createElement("script");s.src="https://cdnjs.cloudflare.com/ajax/libs/lightweight-charts/4.1.3/lightweight-charts.standalone.production.js";s.onload=ok;s.onerror=()=>{lwPromise=null;no()};document.head.appendChild(s)}))}
function openChart(sym,t){chSym=sym;chTf=t||tf||"15m";$("chTitle").textContent=sym;$("chTfs").innerHTML=(data?data.intervals:["5m","15m","1d"]).map(x=>`<button data-ct="${x}" aria-pressed="${x===chTf}">${tfName[x]||x}</button>`).join("");
 if(!$("chartDlg").open)$("chartDlg").showModal();drawChart()}
function initChart(){
 $("chTfs").onclick=e=>{const b=e.target.closest("button");if(b){chTf=b.dataset.ct;$("chTfs").querySelectorAll("button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.ct===chTf));drawChart()}};
 $("chClose").onclick=()=>$("chartDlg").close();
 $("chartDlg").addEventListener("close",()=>{if(chObj){chObj.remove();chObj=null}})}
async function drawChart(){
 const my=chSym+chTf;$("chMsg").textContent="Loading chart…";$("chLeg").innerHTML="";
 if(chObj){chObj.remove();chObj=null}
 let d;try{await loadLW();d=await (await api(`/api/chart?symbol=${encodeURIComponent(chSym)}&tf=${chTf}`)).json()}catch(e){$("chMsg").textContent="Could not load the chart. Check your connection.";return}
 if(my!==chSym+chTf)return;
 if(d.error){$("chMsg").textContent=d.error;return}
 $("chMsg").textContent="";
 const css=getComputedStyle(document.documentElement),col=(n,f)=>css.getPropertyValue(n).trim()||f;
 const ink=col("--ink","#222"),line=col("--line","#ddd"),up=col("--buy","#0a7d5a"),dn=col("--sell","#c0392b");
 const ch=LightweightCharts.createChart($("chart"),{autoSize:true,layout:{background:{color:"transparent"},textColor:ink},grid:{vertLines:{color:line},horzLines:{color:line}},rightPriceScale:{borderColor:line},timeScale:{borderColor:line,timeVisible:d.intraday,secondsVisible:false},crosshair:{mode:0}});
 chObj=ch;
 const cs=ch.addCandlestickSeries({upColor:up,downColor:dn,borderUpColor:up,borderDownColor:dn,wickUpColor:up,wickDownColor:dn});cs.setData(d.candles);
 const ln=(arr,color,w,style)=>{const s=ch.addLineSeries({color,lineWidth:w,lineStyle:style||0,priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});s.setData(arr)};
 ln(d.ema,"#2962ff",1);ln(d.ce_long,up,1,2);ln(d.ce_short,dn,1,2);
 cs.setMarkers(d.markers.map(m=>m.side==="BUY"?{time:m.time,position:"belowBar",color:up,shape:"arrowUp",text:"BUY"}:{time:m.time,position:"aboveBar",color:dn,shape:"arrowDown",text:"SELL"}));
 const sg=d.signal,last=d.candles[d.candles.length-1].time,leg=[`<span><i style="background:#2962ff"></i>EMA ${d.ema_len}</span>`,`<span><i style="background:${up}"></i>Chandelier long stop</span>`,`<span><i style="background:${dn}"></i>Chandelier short stop</span>`];
 if(sg){const lv=(price,color,title,style)=>{const s=ch.addLineSeries({color,lineWidth:2,lineStyle:style,priceLineVisible:false,lastValueVisible:true,crosshairMarkerVisible:false,title});s.setData([{time:sg.from,value:price},{time:last,value:price}])};
  lv(sg.entry,ink,"Entry",0);lv(sg.sl,dn,"SL"+(sg.sl_hit?" ✖":""),0);sg.tps.forEach((p,i)=>lv(p,up,"T"+(i+1)+(sg.hits>i?" ✓":""),2));
  leg.unshift(`<b class="${sg.side==="BUY"?"up":"dn"}">${esc(sg.state)}</b> <span>signal candle ${esc(sg.time)}, ${esc(sg.status)}</span>`)}
 else leg.unshift("<span>No signal to show levels for on this timeframe.</span>");
 $("chLeg").innerHTML=leg.join("");
 ch.timeScale().fitContent();if(d.candles.length>120)ch.timeScale().setVisibleLogicalRange({from:d.candles.length-120,to:d.candles.length+4})}
function renderChain(d){
 const box=document.getElementById("onote"),sum=document.getElementById("osum"),tb=document.getElementById("otable");
 if(d.error){sum.hidden=true;tb.innerHTML="";box.textContent=d.error;return}
 oexp=d.expiry;lastChain=d;const sg=d.signal||"–";
 const stat=(l,v)=>`<div class="stat"><span class="l">${l}</span><b>${v}</b></div>`;
 const lv=d.levels?["entry","sl","tp1","tp2","tp3","tp4"].map((k,i)=>`<span>${["Entry","SL","T1","T2","T3","T4"][i]} <b>${f2(d.levels[k])}</b></span>`).join(""):"";
 sum.hidden=false;
 sum.innerHTML=stat(esc(d.symbol),f2(d.underlying))
  +`<div class="stat"><span class="l">Expiry</span><select id="oexp" aria-label="Expiry">${d.expiries.map(x=>`<option ${x===d.expiry?"selected":""}>${x}</option>`).join("")}</select></div>`
  +(d.chg!=null?stat("Today",`<span class="${d.chg>=0?"up":"dn"}">${d.chg>0?"+":""}${d.chg.toFixed(2)}%</span>`):"")
  +(d.sector?stat("Sector",esc(d.sector)):"")
  +stat("PCR (OI)",d.pcr??"–")+stat("Max call OI",d.max_ce??"–")+stat("Max put OI",d.max_pe??"–")
  +stat("Signal",`<span class="pill ${sg.replace(" ","_")}">${sg}</span>`)
  +(d.suggested?stat("Bias",`ATM ${d.suggested.strike} ${d.suggested.side}${d.suggested.ltp!=null?" ₹"+f2(d.suggested.ltp):""}`):"");
 let ol=document.getElementById("olv");if(!ol){ol=document.createElement("div");ol.id="olv";ol.className="osig";sum.after(ol)}
 const si=d.sig_info,tn=tfName[d.tf]||d.tf;let h="";
 if(sg!=="–"){h=`<div class="osl"><span class="pill ${sg.replace(" ","_")}">${sg}</span><b>${esc(d.symbol)} on ${tn} candles</b><span>signal candle ${esc(si.signal_time)}${si.bars_ago!=null?", "+(si.bars_ago===0?"latest candle":si.bars_ago+(si.bars_ago===1?" candle ago":" candles ago")):""} · ${esc(si.status)}</span></div>`;
  if(d.suggested)h+=`<div class="osl"><span>Option idea:</span><button class="btn" id="osug" data-strike="${d.suggested.strike}" data-right="${d.suggested.side}" data-ltp="${d.suggested.ltp??""}" ${d.suggested.ltp==null?"disabled":""}>Buy ATM ${d.suggested.strike} ${d.suggested.side}${d.suggested.ltp!=null?" at ₹"+f2(d.suggested.ltp):""}</button><span class="sub">${d.suggested.side==="CE"?"Buy signal, so a call":"Sell signal, so a put"}. The shaded column is the side that matches the signal.</span></div>`;
  if(lv)h+=`<div class="osl"><span>Stock levels:</span>${lv}</div>`}
 else{h=`<div class="osl"><span class="pill">–</span><b>No active signal on ${tn} candles</b><span>${si&&si.signal_time!=="–"?"The last signal ("+esc(si.signal_time)+") ended: "+esc(si.status)+".":"No signal in the loaded candles."}</span></div>`}
 const others=Object.entries(d.all_tf||{}).filter(([k])=>k!==d.tf).map(([k,v])=>`<span>${tfName[k]||k}: <span class="pill ${v.signal.replace(" ","_")}">${v.signal}</span></span>`).join("");
 if(others)h+=`<div class="osl"><span>Other timeframes:</span>${others}<span class="sub">(only those already scanned)</span></div>`;
 ol.innerHTML=h;
 const sb=document.getElementById("osug");if(sb)sb.onclick=()=>openTicket({kind:"option",symbol:d.symbol,expiry:d.expiry,strike:+sb.dataset.strike,right:sb.dataset.right,side:"BUY",price:+sb.dataset.ltp,label:`${d.symbol} ${+sb.dataset.strike} ${sb.dataset.right} ${d.expiry}`});
 document.getElementById("oexp").onchange=e=>{oexp=e.target.value;loadChain()};
 const more=document.getElementById("omore").checked,cls=(...a)=>a.filter(Boolean).join(" ");
 const iv=l=>l.iv==null?"–":l.iv.toFixed(1);
 const rows=d.rows.map(r=>{const atm=r.strike===d.atm;
  const ce=(more?[fk(r.ce.oi),sgn(r.ce.doi),fk(r.ce.vol),iv(r.ce),ltpBtn(r,"CE")]:[fk(r.ce.oi),sgn(r.ce.doi),ltpBtn(r,"CE")]);
  const pe=(more?[ltpBtn(r,"PE"),iv(r.pe),fk(r.pe.vol),sgn(r.pe.doi),fk(r.pe.oi)]:[ltpBtn(r,"PE"),sgn(r.pe.doi),fk(r.pe.oi)]);
  const pickC=d.suggested&&d.suggested.side==="CE"&&atm,pickP=d.suggested&&d.suggested.side==="PE"&&atm;
  const ch=ce.map((c,i)=>`<td class="${cls(d.bias==="CE"&&"bias-ce",pickC&&i===ce.length-1&&"pick-ce")}">${c}</td>`).join("");
  const ph=pe.map((c,i)=>`<td class="${cls(d.bias==="PE"&&"bias-pe",pickP&&i===0&&"pick-pe")}">${c}</td>`).join("");
  return `<tr class="${atm?"atm":""}">${ch}<td class="k">${r.strike.toLocaleString("en-IN")}</td>${ph}</tr>`}).join("");
 const cH=more?["OI","Chg OI","Volume","IV","LTP"]:["OI","Chg OI","LTP"],pH=more?["LTP","IV","Volume","Chg OI","OI"]:["LTP","Chg OI","OI"];
 tb.innerHTML=`<thead><tr><th class="g" colspan="${cH.length}">Calls</th><th class="k" rowspan="2">Strike</th><th class="g" colspan="${pH.length}">Puts</th></tr><tr>${cH.map(h=>`<th>${h}</th>`).join("")}${pH.map(h=>`<th>${h}</th>`).join("")}</tr></thead><tbody>${rows}</tbody>`;
 box.textContent=(d.timestamp?`NSE data as of ${d.timestamp}. `:"")+(d.stale?"NSE did not respond just now, so this is the last copy received. ":"")+"Calls are tinted when the stock has a BUY signal and puts when it has a SELL signal. The indicator runs on the stock's own candles, not on option prices, because NSE does not provide option candle history. The at-the-money strike is shaded. Click B or S beside a price to buy or sell that option.";
}
setInterval(()=>{if(mode==="options"&&osym)loadChain(true)},60000);
// ── Orders and order ticket ──
let tk=null,tkToken=null,tradeInfo={mode:"paper",caps:{}},lastChain=null;
const $=id=>document.getElementById(id);
const post=async(u,b)=>(await api(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)})).json();
const ltpBtn=(r,s)=>{const l=r[s.toLowerCase()].ltp;if(l==null)return "–";const d=`data-strike="${r.strike}" data-right="${s}" data-ltp="${l}"`;
 return `<span class="px">${f2(l)}</span><button class="ob b" ${d} data-side="BUY" title="Buy ${s}" aria-label="Buy ${r.strike} ${s}">B</button><button class="ob s" ${d} data-side="SELL" title="Sell ${s}" aria-label="Sell ${r.strike} ${s}">S</button>`};
async function loadTrading(){try{tradeInfo=await (await api("/api/trading")).json();const b=$("banner");b.className="banner "+tradeInfo.mode;
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
 $("tkAutoBox").hidden=opt;$("tkAuto").checked=!opt;$("tkTrail").checked=false;$("tkTrailL").hidden=false;
 $("tkYes").value="";$("tkPin").value="";resetReview();$("ticket").showModal()}
["tkSide","tkQty","tkType","tkPrice","tkProd","tkBroker","tkAuto","tkTrail"].forEach(id=>{$(id).addEventListener("input",resetReview);$(id).addEventListener("change",resetReview)});
$("tkType").addEventListener("change",()=>{$("tkPrice").disabled=$("tkType").value!=="LIMIT"});
$("tkClose").onclick=()=>$("ticket").close();
initChart();initFlash();
$("tkReviewBtn").onclick=async()=>{
 const body={kind:tk.kind,symbol:tk.symbol,side:$("tkSide").value,qty:+$("tkQty").value,order_type:$("tkType").value,price:$("tkPrice").value===""?null:+$("tkPrice").value,product:$("tkProd").value,broker:$("tkBrokerL").hidden?"":$("tkBroker").value,tf:tf||"",expiry:tk.expiry,strike:tk.strike,right:tk.right,auto_sl:!$("tkAutoBox").hidden&&$("tkAuto").checked,trail:!$("tkAutoBox").hidden&&$("tkAuto").checked&&$("tkTrail").checked};
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
$("body").onclick=e=>{const cb=e.target.closest("[data-chart]");if(cb){openChart(cb.dataset.chart);return}
 const b=e.target.closest("[data-trade]");if(!b||!data)return;const r=data.rows.find(x=>x.symbol===b.dataset.trade);if(!r)return;
 const side=r.signal.endsWith("SELL")?"SELL":"BUY";
 openTicket(mode==="futures"?{kind:"future",symbol:r.symbol,label:r.symbol+" futures, nearest month",side,price:r.close}:{kind:"equity",symbol:r.symbol,label:r.symbol,side,price:r.close})};
$("otable").onclick=e=>{const b=e.target.closest(".ob");if(!b||!lastChain)return;
 openTicket({kind:"option",symbol:lastChain.symbol,expiry:lastChain.expiry,strike:+b.dataset.strike,right:b.dataset.right,side:b.dataset.side,price:+b.dataset.ltp,label:`${lastChain.symbol} ${+b.dataset.strike} ${b.dataset.right} ${lastChain.expiry}`})};
$("chk").onclick=async()=>{$("chkMsg").textContent="Checking…";const bs=(tradeInfo.brokers||[]).length?tradeInfo.brokers:[""];const out=[];
 try{for(const b of bs){out.push((await (await api("/api/trading/check?which="+b)).json()).message)}$("chkMsg").textContent=out.join(" ")}catch(e){$("chkMsg").textContent="Cannot reach the server."}};
const cancelable=new Set(["open","trigger pending","open pending","validation pending","put order req received","modified","after market order req received"]);
async function loadOrders(){try{renderOrders(await (await api("/api/orders")).json())}catch(e){}}
function renderOrders(d){const t=$("ordTable");
 if(!d.orders.length){t.innerHTML="";$("ordNote").textContent="No orders have been placed from this dashboard today.";return}
 $("ordNote").textContent=(d.mode==="live"?"Live orders, with status copied from Angel One.":"Paper orders, simulated.")+" A stop-loss can be placed once its entry order shows complete.";
 t.innerHTML=`<thead><tr><th>Time</th><th>Instrument</th><th>Side</th><th>Qty</th><th>Type</th><th>Price</th><th>Status</th><th>Order ID</th><th></th></tr></thead><tbody>`+d.orders.map(o=>{
  const act=[];
  if(cancelable.has(o.status))act.push(`<button class="mini" data-cancel="${esc(o.id)}">Cancel</button>`);
  if(!o.is_exit&&(o.kind==="equity"||o.kind==="future")&&(o.sl||o.risk)&&!o.sl_order_id&&o.status==="complete")act.push(`<button class="mini" data-protect="${esc(o.id)}">Place stop-loss${o.kind==="future"?"":" "+f2(o.sl)}</button>`);
  const price=o.order_type==="LIMIT"?f2(o.price):o.is_exit?(o.note||""):o.avg_price?f2(o.avg_price):"Market";
  return `<tr><td>${o.time}</td><td>${esc(o.label)}${o.is_exit?" (stop-loss)":""}${o.broker&&o.broker!=="paper"?`<span class="tag">${esc((tradeInfo.broker_names||{})[o.broker]||o.broker)}</span>`:""}</td><td>${o.side}</td><td>${o.qty}</td><td>${o.order_type.replace("_"," ").toLowerCase()}</td><td>${price}</td><td title="${esc(o.note||"")}">${esc(o.status)}${o.sl_order_id?", SL placed":""}${o.auto_sl&&!o.sl_order_id?(o.sl_state==="failed"?", <b class=\"dn\">auto SL FAILED</b>":o.sl_state==="skipped"?"":", auto SL waiting"):""}${o.trail_level?", trailed x"+o.trail_level:""}</td><td>${esc(o.id)}</td><td>${act.join(" ")}</td></tr>`}).join("")+"</tbody>"}
$("ordTable").onclick=async e=>{const c=e.target.closest("[data-cancel]"),p=e.target.closest("[data-protect]");if(!c&&!p)return;
 if(!confirm(c?"Cancel this order?":"Place the stop-loss order now?"))return;
 const live=tradeInfo.mode==="live",pin=live?prompt("Order PIN"):"";if(live&&pin===null)return;
 const d=await post(c?"/api/orders/cancel":"/api/orders/protect",{order_id:c?c.dataset.cancel:p.dataset.protect,pin});
 if(d.error)alert(d.error);loadOrders()};
setInterval(()=>{if(mode==="orders")loadOrders()},10000);
loadTrading();
// ── Account and admin ──
const send=async(m,u,b)=>(await api(u,{method:m,headers:{"Content-Type":"application/json"},body:b===undefined?undefined:JSON.stringify(b)})).json();
async function loadMe(){const d=await (await api("/api/me")).json();me=d;csrf=d.csrf;
 $("dpBanner").hidden=!d.default_password;$("who").textContent=d.user.username+(d.user.role==="admin"?" (admin)":"");$("navAdmin").hidden=d.user.role!=="admin";renderAccount(d)}
function renderAccount(d){
 $("acctInfo").textContent=d.mode==="live"?"Live trading is enabled for your account.":d.note;
 $("recInfo").textContent=d.user.recovery_set?"You have a recovery key. If you forget your password, the Forgot password page accepts it once. Creating a new key cancels the old one.":"You have no recovery key yet, so you could not reset your own password. Create one and keep it somewhere safe.";
 $("brokerCards").innerHTML=Object.keys(d.broker_fields).map(b=>{const f=d.broker_fields[b],hint=d.hints[b];
  return `<div class="bcard" data-b="${b}"><h4>${esc(d.broker_names[b])} <span class="tag">${hint?"Saved "+esc(hint):"Not set up"}</span></h4><div class="tkgrid">${f.map(x=>`<label>${esc(x[1])}<input data-f="${x[0]}" type="${x[0]==="client_id"?"text":"password"}" autocomplete="off" placeholder="${hint?"Enter again to replace":""}"></label>`).join("")}</div><div class="tkbtns"><span class="sub" data-msg></span>${hint?'<button class="mini" data-rm>Remove</button>':""}<button class="btn" data-save>Save</button></div></div>`}).join("")}
$("brokerCards").onclick=async e=>{const card=e.target.closest(".bcard");if(!card)return;const b=card.dataset.b,msg=card.querySelector("[data-msg]");
 if(e.target.closest("[data-save]")){const fields={};card.querySelectorAll("[data-f]").forEach(i=>fields[i.dataset.f]=i.value);
  const d=await send("PUT","/api/account/broker",{broker:b,fields});if(d.error){msg.textContent=d.error;return}await loadMe();loadTrading()}
 if(e.target.closest("[data-rm]")){if(!confirm("Remove these broker details?"))return;await send("DELETE","/api/account/broker/"+b);await loadMe();loadTrading()}};
$("pwBtn").onclick=async()=>{const d=await send("POST","/api/account/password",{current:$("pwCur").value,new:$("pwNew").value});$("pwMsg").textContent=d.error||"Password changed.";if(!d.error){$("pwCur").value="";$("pwNew").value="";loadMe()}};
$("pinBtn").onclick=async()=>{const d=await send("POST","/api/account/pin",{password:$("pinPw").value,new_pin:$("pinNew").value});$("pinMsg").textContent=d.error||"Order PIN saved.";if(!d.error){$("pinPw").value="";$("pinNew").value="";await loadMe();loadTrading()}};
$("logout").onclick=async()=>{try{await send("POST","/api/logout",{})}catch(e){}location.href="/login"};
let users=[],limUser=null;
async function loadOverview(){const d=await (await api("/api/admin/overview")).json();if(d.error)return;
 const st=(l,v)=>`<div class="stat"><span class="l">${l}</span><b>${v}</b></div>`;
 const sg=Object.entries(d.signals).map(([k,v])=>`${tfName[k]||k}: ${v.buy} buy, ${v.sell} sell, ${v.open} open`).join("<br>")||"Not scanned yet";
 const live=d.orders.filter(o=>o.mode==="live").length;
 $("admOv").innerHTML=st("Users",`${d.active} active of ${d.users}`)+st("Admins",d.admins)+st("Live enabled for",d.live_enabled)
  +st("Orders today",`${d.orders.length} (${live} live)`)+st("Live trading on server",d.live_server?"On":"Off")
  +st("Live price feed",d.feed&&d.feed.source?(d.feed.error?"Error: "+esc(d.feed.error):d.feed.age==null?"Waiting for the market":esc(d.feed.source)+", "+d.feed.count+" prices, "+d.feed.age+"s old"):"Off (using Yahoo closes)")+st("Auto stop-loss",d.auto&&d.auto.error?"Error: "+esc(d.auto.error):"Running")+st("Sign-up",d.signup?(d.invite?"Open, invite code":"Open"):"Closed")+st("User storage",d.storage==="postgres"?"Postgres":esc(d.storage)+" (needs a persistent disk or users vanish on redeploy)")
  +st("Signals now",sg);
 const t=$("admOrd");if(!d.orders.length){t.innerHTML=`<tbody><tr><td class="empty" style="text-align:left">No orders have been placed today.</td></tr></tbody>`;return}
 t.innerHTML=`<thead><tr><th>Time</th><th>User</th><th>Instrument</th><th>Side</th><th>Qty</th><th>Price</th><th>Status</th><th>Broker</th></tr></thead><tbody>`+d.orders.map(o=>`<tr><td>${esc(o.time)}</td><td>${esc(o.username)}</td><td>${esc(o.label)}${o.is_exit?" (stop-loss)":""}</td><td>${esc(o.side)}</td><td>${o.qty}</td><td>${o.order_type==="LIMIT"?f2(o.price):o.avg_price?f2(o.avg_price):"Market"}</td><td>${esc(o.status)}</td><td>${esc(o.broker||"paper")}</td></tr>`).join("")+"</tbody>"}
async function loadUsers(){loadOverview();const d=await (await api("/api/admin/users")).json();if(d.error){$("admNote").textContent=d.error;return}users=d.users;renderUsers()}
function renderUsers(){const bn=(me&&me.broker_names)||{};
 $("usrTable").innerHTML=`<thead><tr><th>User</th><th>Role</th><th>Active</th><th>Live trading</th><th>Brokers</th><th>Order PIN</th><th></th></tr></thead><tbody>`+users.map(u=>`<tr><td><b>${esc(u.username)}</b></td><td>${u.role}</td><td><input type="checkbox" data-act="active" data-id="${u.id}" ${u.active?"checked":""} aria-label="Active"></td><td><input type="checkbox" data-act="live" data-id="${u.id}" ${u.live_allowed?"checked":""} aria-label="Live trading"></td><td>${u.brokers.map(b=>esc(bn[b]||b)).join(", ")||"–"}</td><td>${u.pin_set?"Set":"Not set"}</td><td><button class="mini" data-act="limits" data-id="${u.id}">Limits</button> <button class="mini" data-act="role" data-id="${u.id}">Make ${u.role==="admin"?"user":"admin"}</button> <button class="mini" data-act="reset" data-id="${u.id}">Reset password</button> <button class="mini" data-act="del" data-id="${u.id}">Delete</button></td></tr>`).join("")+"</tbody>"}
$("usrTable").onchange=async e=>{const c=e.target.closest("input[data-act]");if(!c)return;
 if(c.dataset.act==="live"&&c.checked&&!confirm("Enable live trading for this user? Their orders will go to their real broker account. Check that their broker allows this from your server's IP."))  {c.checked=false;return}
 const d=await send("PATCH","/api/admin/users/"+c.dataset.id,c.dataset.act==="active"?{active:c.checked}:{live_allowed:c.checked});$("admNote").textContent=d.error||"Saved.";loadUsers()};
$("usrTable").onclick=async e=>{const b=e.target.closest("button[data-act]");if(!b)return;const id=+b.dataset.id,u=users.find(x=>x.id===id);let d;
 if(b.dataset.act==="reset"){const pw=prompt("New temporary password for "+u.username+" (at least 10 characters)");if(!pw)return;d=await send("PATCH","/api/admin/users/"+id,{password:pw});if(!d.error)d={error:"Password reset. Ask them to change it after signing in."}}
 else if(b.dataset.act==="role"){d=await send("PATCH","/api/admin/users/"+id,{role:u.role==="admin"?"user":"admin"})}
 else if(b.dataset.act==="del"){if(!confirm("Delete "+u.username+" and their saved broker details?"))return;d=await send("DELETE","/api/admin/users/"+id)}
 else if(b.dataset.act==="limits"){limUser=u;const c=u.caps;$("limTitle").textContent="Limits for "+u.username;$("limValue").value=c.max_order_value;$("limQty").value=c.max_qty;$("limOrders").value=c.max_orders;$("limLots").value=c.max_fut_lots;$("limFut").value=c.max_fut_value;$("limSell").value=c.allow_option_selling?"1":"0";$("limMsg").textContent="";$("limDlg").showModal();return}
 if(d)$("admNote").textContent=d.error||"Saved.";loadUsers()};
$("limClose").onclick=()=>$("limDlg").close();
$("limSave").onclick=async()=>{const d=await send("PATCH","/api/admin/users/"+limUser.id,{caps:{max_order_value:$("limValue").value,max_qty:$("limQty").value,max_orders:$("limOrders").value,max_fut_lots:$("limLots").value,max_fut_value:$("limFut").value,allow_option_selling:$("limSell").value==="1"}});
 if(d.error){$("limMsg").textContent=d.error;return}$("limDlg").close();loadUsers()};
$("addUser").onclick=async()=>{const d=await send("POST","/api/admin/users",{username:$("nuName").value,password:$("nuPw").value,role:$("nuRole").value});$("admNote").textContent=d.error||"User added. Live trading is off for them until you enable it.";if(!d.error){$("nuName").value="";$("nuPw").value=""}loadUsers()};
let hReq=0,hTimer=null,hDeb=null;
const pct=n=>n==null?"–":n.toFixed(1)+"%";
const rr=n=>n==null?"–":`<span class="${n>=0?"up":"dn"}">${n>0?"+":""}${n.toFixed(2)}R</span>`;
async function loadHistory(){clearTimeout(hTimer);const my=++hReq;
 const p=new URLSearchParams({tf:tf||"",symbol:$("hSym").value.trim(),sector:$("hSec").value,side:$("hSide").value,days:$("hDays").value});
 try{const d=await (await api("/api/history?"+p)).json();if(my!==hReq)return;renderHistory(d);
  if(mode==="history"&&(!d.scanned||d.progress))hTimer=setTimeout(loadHistory,3000)}
 catch(e){$("hNote").textContent="Cannot reach the server."}}
["hSec","hSide","hDays"].forEach(i=>$(i).onchange=loadHistory);$("hSym").oninput=()=>{clearTimeout(hDeb);hDeb=setTimeout(loadHistory,300)};
function grpTable(id,rows,label,fmt){const t=$(id);t.className="gt";
 if(!rows.length){t.innerHTML=`<tbody><tr><td class="empty" style="text-align:left">Nothing to show yet.</td></tr></tbody>`;return}
 t.innerHTML=`<thead><tr><th>${label}</th><th>Signals</th><th>T1</th><th>T2</th><th>T3</th><th>Stop first</th><th>R at T2</th></tr></thead><tbody>`+rows.map(r=>`<tr><td><b>${fmt?fmt(r.key):esc(r.key)}</b></td><td>${r.n}</td><td>${pct(r.t1)}</td><td>${pct(r.t2)}</td><td>${pct(r.t3)}</td><td>${pct(r.sl_rate)}</td><td>${rr(r.exp2)}</td></tr>`).join("")+"</tbody>"}
function renderHistory(d){
 const ss=$("hSec");if(ss.options.length!==d.sector_names.length+1){const v=ss.value;ss.innerHTML='<option value="ALL">All sectors</option>'+d.sector_names.map(n=>`<option>${esc(n)}</option>`).join("");ss.value=v}
 const st=d.stats;
 $("hNote").textContent=!d.scanned?(d.progress?`Scanning ${d.progress[0]} of ${d.progress[1]} symbols…`:"The first scan is running. This page updates on its own."):
  st.n?`${tfName[d.tf]||d.tf} candles · ${st.n} signals${d.since?" since "+d.since:""} · ${st.open} still open`:"No signals match these filters.";
 const tile=(l,v,s,w)=>`<div><span class="l">${l}</span><span class="v">${v}</span><span class="s">${s}</span>${w!=null?`<span class="bar2"><i style="width:${w}%"></i></span>`:""}</div>`;
 $("hKpi").innerHTML=st.n?tile("Signals",st.n,`${st.open} open`)+st.t.map(t=>tile(`Reached T${t.k}`,pct(t.rate),`${t.wins} of ${t.done} closed · ${t.exp==null?"–":(t.exp>0?"+":"")+t.exp.toFixed(2)+"R"} per trade`,t.rate)).join("")+tile("Stopped out first",pct(st.sl_rate),`${st.sl_first} of ${st.n}`):"";
 grpTable("hSector",d.by_sector,"Sector");grpTable("hSideT",d.by_side,"Side");
 $("hHourH").hidden=$("hHour").hidden=!d.by_hour.length;if(d.by_hour.length)grpTable("hHour",d.by_hour,"Hour",k=>String(k).padStart(2,"0")+":00");
 grpTable("hBest",d.best,"Symbol");grpTable("hWorst",d.worst,"Symbol");
 const t=$("hList");
 t.innerHTML=d.recent.length?`<thead><tr><th>Signal candle</th><th>Symbol</th><th>Side</th><th>Sector</th><th>Entry</th><th>SL</th><th>Result</th></tr></thead><tbody>`+d.recent.map(r=>`<tr><td>${esc(r.time)}</td><td><b>${esc(r.symbol)}</b></td><td><span class="pill ${r.side}">${r.side}</span></td><td class="sub">${esc(r.sector)}</td><td>${f2(r.entry)}</td><td>${f2(r.sl)}</td><td class="${r.sl_hit&&!r.hits?"dn":r.hits?"up":""}">${esc(r.status)}</td></tr>`).join("")+"</tbody>":"";
 if(st.n>d.recent.length)$("hNote").textContent+=` · showing the latest ${d.recent.length}`}
let alTimer=null;
async function loadAlerts(){const d=await (await api("/api/alerts")).json();if(d.error)return;
 $("alOn").checked=d.enabled;
 $("alTf").innerHTML=Object.entries(d.tf_names).map(([k,v])=>`<label class="chk"><input type="checkbox" value="${k}" ${d.tfs.includes(k)?"checked":""}> ${esc(v)}</label>`).join("");
 document.querySelectorAll("#alSide input").forEach(i=>i.checked=d.sides.includes(i.value));
 $("alSec").innerHTML=d.sector_names.map(n=>`<label class="chk"><input type="checkbox" value="${esc(n)}" ${d.sectors.includes(n)?"checked":""}> ${esc(n)}</label>`).join("");
 if(document.activeElement!==$("alNtfy"))$("alNtfy").value=d.ntfy_topic||"";
 $("alTgTag").textContent=!d.telegram_configured?"Not set up on this server":d.telegram_linked?"Linked":"Not linked";
 $("alTgInfo").textContent=d.telegram_configured?(d.telegram_linked?"Alerts go to your Telegram chat.":"Press Link Telegram, open the link it gives you and press Start in Telegram."):"The admin has not set TELEGRAM_BOT_TOKEN, so only ntfy works for now.";
 $("alTgLink").hidden=!d.telegram_configured||d.telegram_linked;$("alTgUnlink").hidden=!d.telegram_linked;
 if(d.last)$("alMsg").textContent="Last alert: "+d.last;
 if(d.telegram_linked&&alTimer){clearInterval(alTimer);alTimer=null}}
const alBody=()=>({enabled:$("alOn").checked,tfs:[...document.querySelectorAll("#alTf input:checked")].map(i=>i.value),sides:[...document.querySelectorAll("#alSide input:checked")].map(i=>i.value),sectors:[...document.querySelectorAll("#alSec input:checked")].map(i=>i.value),ntfy_topic:$("alNtfy").value.trim()});
$("alSave").onclick=async()=>{const d=await send("PUT","/api/alerts",alBody());$("alMsg").textContent=d.error||"Saved.";loadAlerts()};
$("alTest").onclick=async()=>{const s=await send("PUT","/api/alerts",alBody());if(s.error){$("alMsg").textContent=s.error;return}
 const d=await send("POST","/api/alerts/test",{});$("alMsg").textContent=d.error||Object.entries(d.results).map(([k,v])=>k+": "+v).join(" · ")};
$("alRand").onclick=()=>{const a=new Uint8Array(9);crypto.getRandomValues(a);$("alNtfy").value="signals-"+[...a].map(b=>"abcdefghjkmnpqrstuvwxyz23456789"[b%31]).join("")};
$("alTgLink").onclick=async()=>{const d=await send("POST","/api/alerts/telegram/link",{});if(d.error){$("alMsg").textContent=d.error;return}
 $("alTgInfo").innerHTML=`Open <a href="${esc(d.url)}" target="_blank" rel="noopener">${esc(d.url)}</a> and press Start. Or send <b>/start ${esc(d.code)}</b> to @${esc(d.bot)}. The code works for ${d.minutes} minutes.`;
 if(alTimer)clearInterval(alTimer);let n=0;alTimer=setInterval(()=>{loadAlerts();if(++n>150){clearInterval(alTimer);alTimer=null}},4000)};
$("alTgUnlink").onclick=async()=>{await send("POST","/api/alerts/telegram/unlink",{});loadAlerts()};
$("recBtn").onclick=async()=>{const d=await send("POST","/api/account/recovery",{password:$("recPw").value});
 if(d.error){$("recMsg").textContent=d.error;return}$("recPw").value="";$("recMsg").innerHTML="Your new key (shown once, it replaces the old one): <b>"+esc(d.recovery_key)+"</b>";loadMe()};
loadMe().then(()=>{if(new URLSearchParams(location.search).get("m")==="admin"){history.replaceState(null,"","/");setMode("admin")}else if(mode==="admin")setMode("admin")}).catch(()=>{});
setMode(mode);
load();
</script></body></html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
