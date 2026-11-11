"""
CE + EMA + QQE MOD signal dashboard for NSE stocks, futures and options, with sign-in for several people.
Python port of the TradingView indicator "Chandelier Exit + EMA + QQE MOD Signals".

Setup:   pip install -r requirements.txt            (live trading also: pip install -r requirements-live.txt)
Run:     python signal_dashboard.py
Open:    http://127.0.0.1:8000
First sign-in: set ADMIN_USER and ADMIN_PASSWORD, or read the one-time password printed in the console.
Locked out: set ADMIN_RESET=1 with ADMIN_PASSWORD, restart once, sign in, then remove ADMIN_RESET.

Optional env vars:  INTERVAL=5m|15m|1d  REFRESH_SECONDS=300  PORT=8000  HOST=0.0.0.0
                     DATA_BROKER=dhan (one source: Dhan live stream + Dhan history)  CANDLE_SOURCE=dhan|yahoo  YAHOO_FALLBACK=0|1  LIVE_RESYNC_SECONDS=1800
  DATABASE_URL=<postgres address, keeps users across redeploys; also set SECRETS_KEY>
  DB_PATH=dashboard.db  ORDER_LOG=orders_log.jsonl  SECRETS_KEY=<Fernet key>  COOKIE_SECURE=1 (when served over https)
  DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN (first token), DHAN_PIN, DHAN_TOTP_SECRET (optional: with DHAN_AUTO_RENEW=1 the server signs in by itself every day at DHAN_REFRESH_AT=06:00 IST; off by default)
  LIVE_TRADING=1 (switch live trading on for the server; an admin must still enable it for each person)
  MAX_ORDER_VALUE, MAX_QTY, MAX_ORDERS_PER_DAY, MAX_FUT_LOTS, MAX_FUT_VALUE, ALLOW_OPTION_SELLING  (default limits per person)
Optional file:      symbols.txt  (one NSE symbol per line, e.g. RELIANCE) overrides the default stock list.

Orders are simulated ("paper") unless live trading is on for the server and enabled for that person.
Back up the database and the secret.key file together. Without the key, saved broker details cannot be read.
"""
import base64
import csv
import math
import hashlib
import hmac
import json
import sqlite3
import os
import queue
import re
import secrets
import struct
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
STRIKES_EACH_SIDE = int(os.getenv("STRIKES_EACH_SIDE", "25"))       # strikes shown above and below the money (set STRIKES_EACH_SIDE in Render to change)

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


INTRA_CHG = {}   # symbol -> today's change from the latest 5 or 15 minute candles, used for the daily view when Dhan's daily history has no candle for today
INTERNAL = {}   # (symbol, timeframe) -> values the options autopilot needs; never sent to the browser


def analyse(symbol, df, interval):
    df = df.dropna(subset=["Close", "High", "Low"]).copy()
    if df.empty:
        return None
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    chg = None  # change since the previous session's close, using the latest price (even a candle that is still forming)
    prev_close = None
    stale_day = False
    vol = val_cr = None
    try:
        if interval != "1d" and "Volume" in df:
            day = np.array([t.date() for t in df.index]) == df.index[-1].date()
            v = df["Volume"].to_numpy(float)[day]
            vol = int(np.nansum(v))
            val_cr = round(float(np.nansum(v * df["Close"].to_numpy(float)[day])) / 1e7, 2)
    except Exception:
        vol = val_cr = None
    try:
        cl = df["Close"].to_numpy(float)
        if interval == "1d":
            if len(cl) >= 2 and cl[-2] > 0:
                chg = (cl[-1] / cl[-2] - 1) * 100
                prev_close = float(cl[-2])
            n_ = ist_now()
            if len(cl) and cl[-1] > 0 and df.index[-1].date() < n_.date() and n_.time() >= dtime(9, 15) and (n_.weekday() < 5 or special_session_now()):
                prev_close = float(cl[-1])   # Dhan's daily history stops at yesterday: today's change is measured from that last candle, not the one before it
                chg, stale_day = None, True   # and the last candle's own move is yesterday's, not today's
        else:
            days = np.array([t.date() for t in df.index])
            before = np.nonzero(days != days[-1])[0]
            if len(before) and cl[before[-1]] > 0:
                chg = (cl[-1] / cl[before[-1]] - 1) * 100
                prev_close = float(cl[before[-1]])
    except Exception:
        chg = None
    if interval != "1d" and chg is not None:
        INTRA_CHG[symbol] = chg
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

    INTERNAL[(symbol, interval)] = {"ema": float(ema[-1]), "atr": float(atr[-1]) if len(atr) and atr[-1] == atr[-1] else None}
    base = {"symbol": symbol, "hist": hist, "type": "index" if symbol in INDEX_YF else "stock", "close": round(float(c[-1]), 2), "signal": "–", "entry": None, "sl": None,
            "sector": sector_of(symbol), "chg": None if chg is None else round(float(chg), 2),
            "prev_close": None if prev_close is None else round(prev_close, 2), "stale_day": stale_day, "vol": vol, "val_cr": val_cr,
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
                hits=hits, sl_hit=sl_hit, status=status, bars_ago=int(bars_ago), signal_time=fmt_time(df.index[j], interval), signal_date=df.index[j].strftime("%Y-%m-%d"))
    base["side"] = name
    return base


# ── Live candles: with a live price feed, the 5 and 15 minute candles are completed from the feed the moment each one closes,
#    so signals appear within seconds instead of waiting for the data provider to publish the candle. ──
LIVE_CANDLES = os.getenv("LIVE_CANDLES", "1") != "0"
LIVE_TFS = ("5m", "15m")
LIVE_MINUTES = {"5m": 5, "15m": 15}
TAIL_BARS = 1500
LIVE_RESYNC_SECONDS = int(os.getenv("LIVE_RESYNC_SECONDS", "1800"))   # with the Dhan stream running, a full re-read is only a safety net
TAILS = {}     # (symbol, tf) -> the last closed candles from the data provider
BUILD = {}     # (symbol, tf) -> the candle being built from feed prices
CLOSED = {}    # (symbol, tf) -> {candle start: candle} finished from feed prices
LIVEC = {tf: {"bucket": None, "at": None, "n": 0, "busy": False} for tf in LIVE_TFS}


def live_active():
    return LIVE_CANDLES and bool(FEED["t"]) and time.time() - FEED["t"] < 30 and market_window()


def live_bucket(now, tf):
    m = LIVE_MINUTES[tf]
    mins = now.hour * 60 + now.minute
    return now.normalize() + pd.Timedelta(minutes=(mins // m) * m)


def live_tail(df):
    """The closed candles worth keeping for the live engine, in India time."""
    d = df.dropna(subset=["Close", "High", "Low", "Open"]).copy()
    if d.empty:
        return None
    d.index = d.index.tz_localize("UTC").tz_convert(IST) if d.index.tz is None else d.index.tz_convert(IST)
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in d]
    return d[keep].tail(TAIL_BARS + 1)


LIVE_LOCK = threading.RLock()
LASTP = {}     # symbol -> price of the last tick since the feed (re)connected
LASTV = {}     # symbol -> cumulative day volume at the last tick (Dhan's quote packets carry it)
REPAIR = {tf: set() for tf in LIVE_TFS}   # symbols whose candle was only partly seen: re-read from the broker's history


def live_tick(sym, price, now, cum=None):
    """Feed one price (and the day's cumulative volume, when the feed has it) into the 5 and 15 minute candles."""
    with LIVE_LOCK:
        first = sym not in LASTP
        prev_v = LASTV.get(sym)
        LASTP[sym] = price
        if cum is not None:
            LASTV[sym] = cum
        for tf in LIVE_TFS:
            b = live_bucket(now, tf)
            cur = BUILD.get((sym, tf))
            if cur and cur["b"] == b:
                cur["h"], cur["l"], cur["c"] = max(cur["h"], price), min(cur["l"], price), price
                if cum is not None:
                    cur["v"] = cum
                continue
            if cur:
                _live_store(sym, tf, cur, b)
            if cum is None:
                v0 = None
            elif prev_v is None:
                v0 = cum
            else:
                v0 = 0 if cum < prev_v else prev_v   # the day's total starts again from zero each morning
            late = first and (now - b).total_seconds() > 20   # connected mid-candle: its open and extremes were not all seen
            BUILD[(sym, tf)] = {"b": b, "o": price, "h": price, "l": price, "c": price, "v0": v0, "v": cum, "part": bool(late and use_dhan())}


def _live_store(sym, tf, cur, upto):
    """Keep a finished candle, and fill candles in which nothing traded with flat ones, so no hole stops the scan."""
    if cur.get("part"):
        REPAIR[tf].add(sym)
        return
    got = CLOSED.setdefault((sym, tf), {})
    got[cur["b"]] = cur
    step, k = STEP[tf], cur["b"] + STEP[tf]
    while k < upto and k.date() == cur["b"].date():
        got[k] = {"b": k, "o": cur["c"], "h": cur["c"], "l": cur["c"], "c": cur["c"], "v0": 0, "v": 0}
        k += step


def live_reset():
    """Forget the candles built so far: used when the feed connection starts again, as ticks may have been missed."""
    with LIVE_LOCK:
        BUILD.clear()
        CLOSED.clear()
        LASTP.clear()
        LASTV.clear()


def live_frame(sym, tf):
    """Provider candles up to the last closed one, then the candles finished from the feed. None if there is a hole in between."""
    base = TAILS.get((sym, tf))
    if base is None or base.empty:
        return None
    base = drop_forming(base, tf)
    if base.empty:
        return None
    last, step = base.index[-1], STEP[tf]
    with LIVE_LOCK:
        got = dict(CLOSED.get((sym, tf)) or {})
    keys = sorted(k for k in got if k > last)
    now = pd.Timestamp.now(tz=IST)
    prev, rows = last, []
    for k in keys:
        if k.date() == prev.date() and k - prev != step:
            return None
        if k + step > now:
            break
        c = got[k]
        vol = (c["v"] - c["v0"]) if c.get("v") is not None and c.get("v0") is not None else np.nan
        rows.append((k, c["o"], c["h"], c["l"], c["c"], vol))
        prev = k
    if use_dhan() and last.date() == now.date() and prev < live_bucket(now, tf) - step:
        return None   # candles are missing up to the one that just closed: keep the previous result until they are repaired
    if not rows:
        return base
    add = pd.DataFrame([r[1:5] for r in rows], index=pd.DatetimeIndex([r[0] for r in rows]), columns=["Open", "High", "Low", "Close"])
    if "Volume" in base:
        add["Volume"] = [r[5] for r in rows]
    return pd.concat([base, add])


def live_rescan(tf):
    """Recompute every symbol of one timeframe from provider candles plus feed-built candles, and publish the result."""
    if not LIVE_CANDLES or tf not in LIVE_TFS or LIVEC[tf]["busy"]:
        return 0
    LIVEC[tf]["busy"] = True
    try:
        with LOCK:
            old = list(STATES[tf]["rows"])
        if not old:
            return 0
        out, n = [], 0
        for r in old:
            try:
                df = live_frame(r["symbol"], tf)
                row = analyse(r["symbol"], df, tf) if df is not None else None
            except Exception:
                row = None
            if not row:
                out.append(r)
                continue
            row.pop("hist", None)
            row["vol"], row["val_cr"] = r.get("vol"), r.get("val_cr")   # traded value is carried from the last full scan
            out.append(row)
            n += 1
        with LOCK:
            STATES[tf].update(rows=out, updated=ist_now().strftime("%d %b %H:%M:%S"))
        LIVEC[tf].update(at=ist_now().strftime("%H:%M:%S"), n=n)
        try:
            check_alerts(tf, out)
        except Exception as e:
            print("alert check failed:", e, flush=True)
        return n
    finally:
        LIVEC[tf]["busy"] = False


def live_repair(tf):
    """Re-read the last few days of the symbols whose candle was only partly seen, straight from Dhan's history."""
    with LIVE_LOCK:
        todo = sorted(REPAIR[tf])
        REPAIR[tf].clear()
    if not todo or not use_dhan():
        return 0
    got, _ = dhan_frames(todo, tf, 3)
    for s, df in got.items():
        tl = live_tail(df)
        if tl is None:
            continue
        with LIVE_LOCK:
            old = TAILS.get((s, tf))
            if old is not None and len(old):
                tl = pd.concat([old[old.index < tl.index[0]], tl])   # the top-up is joined onto the longer history already kept
            TAILS[(s, tf)] = tl.tail(TAIL_BARS + 1)
            d = CLOSED.get((s, tf))
            if d:
                for k in [k for k in d if k <= tl.index[-1]]:
                    d.pop(k, None)
    return len(got)


def live_cycle(tf):
    try:
        live_repair(tf)
    except Exception as e:
        DHAN_STAT["hist_error"] = f"{type(e).__name__}: {str(e)[:100]}"
    live_rescan(tf)


def live_boundary(now):
    """Called about every second: when a candle has just closed, finish it and rescan that timeframe in the background."""
    for tf in LIVE_TFS:
        b = live_bucket(now, tf)
        with LIVE_LOCK:
            was = LIVEC[tf]["bucket"]
            LIVEC[tf]["bucket"] = b
            if was is None or b <= was:
                continue
            for k in [k for k, v in BUILD.items() if k[1] == tf and v["b"] < b]:
                _live_store(k[0], tf, BUILD.pop(k), b)
            if use_dhan():
                for s in {s for (s, t) in TAILS if t == tf}:
                    if s not in LASTP:
                        REPAIR[tf].add(s)   # nothing has traded or arrived for it yet: take its candles from the history
            cut = now - pd.Timedelta(days=3)
            for k, d in CLOSED.items():
                if k[1] == tf:
                    for t in [t for t in d if t < cut]:
                        d.pop(t, None)
        threading.Thread(target=live_cycle, args=(tf,), daemon=True).start()


# ── Dhan as the single data source: live prices over its WebSocket, candles from its historical API ──
# With DATA_BROKER=dhan the dashboard reads everything from Dhan: the WebSocket feed gives every price and the day's running
# volume, the historical API fills in the candles before the market opens, and each new 5 and 15 minute candle is built from the
# stream. Yahoo is not used unless YAHOO_FALLBACK=1 or an admin switches the data source to Yahoo on the Admin page.
CANDLE_SOURCE = os.getenv("CANDLE_SOURCE", "").strip().lower()      # "dhan" or "yahoo"; empty means dhan when DATA_BROKER=dhan
YAHOO_FALLBACK = os.getenv("YAHOO_FALLBACK", "0") == "1"           # when on, a symbol Dhan cannot supply is read from Yahoo
SRC = {"override": None}                                            # set from the Admin page: "dhan" or "yahoo"
DHAN_STAT = {"ws": "off", "last": 0.0, "error": None, "frames": 0, "connects": 0, "subscribed": 0,
             "hist_error": None, "hist_ok": 0, "hist_at": None, "tz": None}
DHAN_WS_URL = "wss://api-feed.dhan.co"
DHAN_SEG = {"IDX_I": 0, "NSE_EQ": 1, "NSE_FNO": 2, "NSE_CURRENCY": 3, "BSE_EQ": 4, "MCX_COMM": 5, "BSE_CURRENCY": 7, "BSE_FNO": 8}
DHAN_PKT = {2: 16, 4: 50, 6: 16, 5: 12, 8: 162, 7: 10}             # packet sizes in bytes, header included
DHAN_DATA_ERRORS = {
    "800": "Dhan had an internal error", "804": "too many instruments requested", "805": "too many requests or connections",
    "806": "Data APIs are not subscribed on this Dhan account (subscribe under web.dhan.co, My Profile, DhanHQ Trading APIs)",
    "807": "the Dhan access token has expired", "808": "Dhan could not authenticate the token", "809": "the Dhan access token is invalid",
    "810": "the Dhan client ID is invalid", "811": "invalid expiry", "812": "invalid date format", "813": "invalid security id",
    "814": "invalid request", "DH-901": "the Dhan access token is invalid or expired", "DH-902": "Data APIs are not subscribed on this Dhan account",
    "DH-904": "Dhan rate limit reached", "DH-905": "Dhan rejected the request"}
DHAN_AUTH_CODES = {"807", "808", "809", "810", "DH-901", "DH-902", "806"}


class DhanDataError(Exception):
    def __init__(self, msg, auth=False):
        super().__init__(msg)
        self.auth = auth


def data_source():
    if SRC["override"] in ("dhan", "yahoo"):
        return SRC["override"]
    if CANDLE_SOURCE in ("dhan", "yahoo"):
        return CANDLE_SOURCE
    return "dhan" if DATA_BROKER == "dhan" else "yahoo"


def use_dhan():
    return DATA_BROKER == "dhan" and data_source() == "dhan"


def _dhan_reason(res, status):
    """A plain sentence for a refused Dhan data request. Returns (text, is_a_sign_in_problem)."""
    codes = []
    if isinstance(res, dict):
        d = res.get("data")
        if isinstance(d, dict):
            codes += [str(k) for k in d]
        for k in ("errorCode", "internalErrorCode"):
            if res.get(k):
                codes.append(str(res[k]))
    if status in (401, 403) and not codes:
        codes.append("808")
    for c in codes:
        if c in DHAN_DATA_ERRORS:
            return f"{DHAN_DATA_ERRORS[c]} ({c})", c in DHAN_AUTH_CODES
    msg = (res.get("errorMessage") or res.get("internalErrorMessage")) if isinstance(res, dict) else None
    return (str(msg)[:120] if msg else f"HTTP {status}"), False


# ── Dhan access token: the server renews it, so nobody has to paste a new one every day.
# Needs DHAN_CLIENT_ID and a first DHAN_ACCESS_TOKEN. With DHAN_PIN and DHAN_TOTP_SECRET as well, it signs in again by itself every morning.
# Without them it can still extend the token it has (Dhan's renew call) as long as that token has not yet expired.
DHAN_AUTH_URL = os.getenv("DHAN_AUTH_URL", "https://auth.dhan.co/app/generateAccessToken")
DHAN_REFRESH_AT = os.getenv("DHAN_REFRESH_AT", "06:00")           # IST, the daily sign-in time
DHAN_AUTO_RENEW = os.getenv("DHAN_AUTO_RENEW", "0") == "1"   # off unless DHAN_AUTO_RENEW=1
DHAN_RENEW_EVERY_H = float(os.getenv("DHAN_RENEW_EVERY_H", "12") or 12)   # renew-only mode: extend the token this often
TOKEN = {"lock": threading.Lock(), "expiry": 0.0, "issued": 0.0, "generated": 0.0, "try": 0.0, "ok": 0.0, "error": None, "how": None,
         "alerted": 0.0, "loaded": False}
_TOKEN_SEED = hashlib.sha256(os.getenv("DHAN_ACCESS_TOKEN", "").encode()).hexdigest()[:16]   # the token that was pasted into the settings


def _token_auto():
    return bool(os.getenv("DHAN_PIN", "").strip() and os.getenv("DHAN_TOTP_SECRET", "").strip())


def totp_code(secret, at=None, step=30, digits=6):
    """The 6 digit code an authenticator app would show (RFC 6238)."""
    sec = re.sub(r"\s+", "", secret).upper()
    key = base64.b32decode(sec + "=" * (-len(sec) % 8))
    n = int((time.time() if at is None else at) // step)
    h = hmac.new(key, struct.pack(">Q", n), hashlib.sha1).digest()
    o = h[-1] & 15
    return str((struct.unpack(">I", h[o:o + 4])[0] & 0x7FFFFFFF) % (10 ** digits)).zfill(digits)


def _jwt_exp(tok):
    try:
        part = str(tok).split(".")[1]
        part += "=" * (-len(part) % 4)
        return float(json.loads(base64.urlsafe_b64decode(part))["exp"])
    except Exception:
        return 0.0


def _scrub(text):
    """Nothing secret may end up in a message: the PIN, the code, and any token are masked."""
    t = str(text)
    for v in (os.getenv("DHAN_PIN", ""), os.getenv("DHAN_TOTP_SECRET", ""), os.getenv("DHAN_ACCESS_TOKEN", ""), (FEED.get("acct").c.get("access_token") if FEED.get("acct") else "")):
        if v and len(str(v)) >= 4:
            t = t.replace(str(v), "…")
    return re.sub(r"eyJ[\w-]{10,}\.[\w-]{10,}\.[\w-]{5,}", "…", t)[:160]


def _token_result(res):
    d = res.get("data") if isinstance(res, dict) and isinstance(res.get("data"), dict) else res
    tok = (d or {}).get("accessToken") or (d or {}).get("access_token") if isinstance(d, dict) else None
    if not tok:
        msg = None
        if isinstance(res, dict):
            msg = res.get("message") or res.get("errorMessage") or res.get("remarks") or res.get("status")
            if isinstance(msg, dict):
                msg = msg.get("message") or json.dumps(msg)
        raise RuntimeError(_scrub(msg or "Dhan did not return a token"))
    exp = _jwt_exp(tok)
    if not exp:
        try:
            exp = pd.Timestamp(str(d.get("expiryTime")), tz=IST).timestamp()
        except Exception:
            exp = time.time() + 23 * 3600
    return str(tok), float(exp)


def dhan_generate(c):
    """Sign in with the PIN and the authenticator code, the same as the Dhan web page does."""
    pin, sec = os.getenv("DHAN_PIN", "").strip(), os.getenv("DHAN_TOTP_SECRET", "").strip()
    if not (pin and sec):
        raise RuntimeError("DHAN_PIN and DHAN_TOTP_SECRET are not set")
    if time.time() % 30 > 27:
        time.sleep(30 - time.time() % 30 + 0.5)     # do not send a code that is about to change
    try:
        r = requests.post(DHAN_AUTH_URL, params={"dhanClientId": c["client_id"], "pin": pin, "totp": totp_code(sec)}, timeout=20)
        res = r.json()
    except requests.RequestException as e:
        raise RuntimeError("could not reach Dhan's sign-in page (" + type(e).__name__ + ")")
    except ValueError:
        raise RuntimeError(f"Dhan's sign-in page gave an unreadable answer (HTTP {r.status_code})")
    return _token_result(res)


def dhan_renew_call(c):
    """Ask Dhan to extend the token that is still valid."""
    try:
        r = requests.get(DHAN_BASE + "/RenewToken", timeout=20, headers={"access-token": c["access_token"], "dhanClientId": str(c["client_id"])})
        res = r.json()
    except requests.RequestException as e:
        raise RuntimeError("could not reach Dhan (" + type(e).__name__ + ")")
    except ValueError:
        raise RuntimeError(f"Dhan's renew call gave an unreadable answer (HTTP {r.status_code})")
    return _token_result(res)


def token_save(tok, exp, issued, generated):
    try:
        blob = fernet().encrypt(json.dumps({"token": tok, "expiry": exp, "issued": issued, "generated": generated, "seed": _TOKEN_SEED}).encode()).decode()
        x("INSERT INTO creds(user_id, broker, blob, hint, updated) VALUES(?,?,?,?,?) "
          "ON CONFLICT(user_id, broker) DO UPDATE SET blob=excluded.blob, hint=excluded.hint, updated=excluded.updated",
          (0, "dhan-feed-token", blob, "", ist_now().isoformat()))
    except Exception:
        pass    # a failed save only means the next restart signs in again


def token_load(c):
    """Use the token this server last renewed (kept encrypted in the database) unless a newer one was pasted into the settings."""
    if TOKEN["loaded"] or not DHAN_AUTO_RENEW:
        return
    TOKEN["loaded"] = True
    try:
        rows = q("SELECT blob FROM creds WHERE user_id=? AND broker=?", (0, "dhan-feed-token"))
        if rows:
            d = json.loads(fernet().decrypt(rows[0]["blob"].encode()).decode())
            if d.get("seed") == _TOKEN_SEED and d.get("token"):
                c["access_token"] = d["token"]
                TOKEN.update(expiry=float(d.get("expiry") or 0), issued=float(d.get("issued") or 0), generated=float(d.get("generated") or 0), how="saved")
                os.environ["DHAN_ACCESS_TOKEN"] = d["token"]
                return
    except Exception:
        pass
    exp = _jwt_exp(c.get("access_token", ""))
    TOKEN.update(expiry=exp, issued=(exp - 24 * 3600) if exp else 0.0)


def alert_admins(title, text):
    try:
        for r in q("SELECT id FROM users WHERE role='admin' AND active=1"):
            notify_user(r["id"], title, text)
    except Exception:
        pass


def dhan_token_refresh(why="", force=False):
    """Get a fresh token now. Returns True when the server has a working token afterwards."""
    if DATA_BROKER != "dhan" or not DHAN_AUTO_RENEW:
        return False
    with TOKEN["lock"]:
        now = time.time()
        if not force and now - TOKEN["try"] < 130:      # Dhan allows a sign-in only once in a couple of minutes
            return TOKEN["error"] is None
        acct = FEED.get("acct")
        if acct is None:
            return False
        c = acct.c
        token_load(c)
        TOKEN["try"] = now
        valid = bool(c.get("access_token")) and (not TOKEN["expiry"] or now < TOKEN["expiry"] - 30)
        order = (["generate"] if _token_auto() else []) + (["renew"] if valid else [])
        if not order:
            TOKEN["error"] = "no way to renew: set DHAN_PIN and DHAN_TOTP_SECRET, or paste a new DHAN_ACCESS_TOKEN"
        errs = []
        for how in order:
            try:
                tok, exp = dhan_generate(c) if how == "generate" else dhan_renew_call(c)
            except Exception as e:
                errs.append(f"{'sign-in' if how == 'generate' else 'renew'}: {_scrub(e)}")
                continue
            c["access_token"] = tok
            os.environ["DHAN_ACCESS_TOKEN"] = tok
            TOKEN.update(expiry=exp, issued=time.time(), ok=time.time(), error=None, how="signed in" if how == "generate" else "renewed",
                         generated=time.time() if how == "generate" else TOKEN["generated"])
            token_save(tok, exp, TOKEN["issued"], TOKEN["generated"])
            DHAN_STAT["error"] = None
            FEED["error"] = None
            return True
        if errs:
            TOKEN["error"] = "; ".join(errs)[:220]
        if now - TOKEN["alerted"] > 6 * 3600:
            TOKEN["alerted"] = now
            alert_admins("Dhan token could not be renewed", f"{TOKEN['error']}. Prices and orders will stop when the current token runs out. "
                                                              "Check DHAN_PIN and DHAN_TOTP_SECRET, or paste a new DHAN_ACCESS_TOKEN in Render.")
        return False


def dhan_token_due():
    """Why the token should be renewed right now, or None."""
    acct = FEED.get("acct")
    if acct is None:
        return None
    c = acct.c
    token_load(c)
    now = time.time()
    if not c.get("access_token"):
        return "no token"
    if TOKEN["expiry"] and now >= TOKEN["expiry"] - 600:
        return "about to expire"
    if _token_auto():
        try:
            hh, mm = [int(v) for v in DHAN_REFRESH_AT.split(":")]
        except ValueError:
            hh, mm = 6, 0
        at = ist_now().replace(hour=hh, minute=mm, second=0, microsecond=0).timestamp()
        if now >= at and TOKEN["generated"] < at:
            return "daily sign-in"
    elif not TOKEN["issued"] or now - TOKEN["issued"] > DHAN_RENEW_EVERY_H * 3600:
        return "extend"
    return None


def dhan_token_loop():
    while True:
        try:
            if FEED.get("acct") is None and os.getenv("DHAN_CLIENT_ID", "").strip():
                try:
                    feed_account()      # builds the account (and signs in if there is no token yet)
                except Exception:
                    pass
            why = dhan_token_due()
            if why:
                dhan_token_refresh(why)
        except Exception as e:
            TOKEN["error"] = _scrub(f"{type(e).__name__}: {e}")
        time.sleep(60)


def token_report():
    now = time.time()
    return {"enabled": DHAN_AUTO_RENEW, "auto": _token_auto(), "at": DHAN_REFRESH_AT, "how": TOKEN["how"], "error": TOKEN["error"],
            "expires_in": int(TOKEN["expiry"] - now) if TOKEN["expiry"] else None,
            "last_ok_age": int(now - TOKEN["ok"]) if TOKEN["ok"] else None}


_HTHROT = {"lock": threading.Lock(), "last": 0.0}


def dhan_post(path, body):
    """A Dhan data request. When Dhan says the token has expired, the server gets a new one and asks again."""
    try:
        return _dhan_post_once(path, body)
    except DhanDataError as e:
        if e.auth and "subscribed" not in str(e) and dhan_token_refresh("a data request was refused"):
            return _dhan_post_once(path, body)
        raise


def _dhan_post_once(path, body):
    """One Dhan data request, spaced to stay under the 5 a second limit that all threads share."""
    with _HTHROT["lock"]:
        wait = 0.21 - (time.time() - _HTHROT["last"])
        if wait > 0:
            time.sleep(wait)
        _HTHROT["last"] = time.time()
    c = feed_account().c
    for attempt in range(2):
        r = requests.post(DHAN_BASE + path, json=body, timeout=20,
                          headers={"access-token": c["access_token"], "client-id": str(c["client_id"]),
                                   "Content-Type": "application/json", "Accept": "application/json"})
        if r.status_code == 429 and attempt == 0:
            time.sleep(1.2)
            continue
        break
    try:
        res = r.json()
    except ValueError:
        res = None
    if r.status_code != 200:
        text, auth = _dhan_reason(res, r.status_code)
        raise DhanDataError(text, auth)
    if isinstance(res, dict) and res.get("status") == "failed":
        text, auth = _dhan_reason(res, 200)
        raise DhanDataError(text, auth)
    return res


def dhan_inst(sym):
    """(security id, exchange segment, instrument) for a dashboard symbol, or None if Dhan's list does not have it."""
    ix = INDEX_FEED["dhan"].get(sym)
    if ix:
        return ix[1], ix[0], "INDEX"
    eq = M("dhan")["eq"].get(sym)
    return (str(eq[0]), "NSE_EQ", "EQUITY") if eq else None


def dhan_to_df(res, daily):
    """Dhan's parallel arrays as a candle table. The time base of the epoch is read from the data itself."""
    if not isinstance(res, dict) or not res.get("close"):
        return None
    ts = res.get("timestamp") or res.get("start_Time") or []
    n = min(len(ts), len(res["close"]), len(res.get("open", [])), len(res.get("high", [])), len(res.get("low", [])))
    if n == 0:
        return None
    vol = res.get("volume") or [0] * n
    df = pd.DataFrame({"Open": res["open"][:n], "High": res["high"][:n], "Low": res["low"][:n], "Close": res["close"][:n],
                       "Volume": (list(vol) + [0] * n)[:n]}, index=pd.to_datetime(list(ts)[:n], unit="s"))
    if daily:
        if (df.index.hour >= 12).any():   # midnight India time written as an epoch lands at 18:30 the evening before
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
        df.index = pd.DatetimeIndex(df.index.date)
        DHAN_STAT["tz"] = DHAN_STAT["tz"] or "daily"
    else:
        first = pd.Series(df.index).groupby(df.index.date).min()
        mins = [t.hour * 60 + t.minute for t in first]
        med = sorted(mins)[len(mins) // 2]
        if 9 * 60 <= med <= 9 * 60 + 40 or med >= 15 * 60:   # the epoch already holds the India clock time (a first candle near 09:15)
            df.index = df.index.tz_localize(IST)
            DHAN_STAT["tz"] = "ist-clock"
        else:
            df.index = df.index.tz_localize("UTC").tz_convert(IST)
            DHAN_STAT["tz"] = "utc"
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df.dropna(subset=["Close", "High", "Low", "Open"])


def dhan_candles(sym, interval, days):
    """Candles of one symbol from Dhan: intraday in pieces of up to 80 days, daily in one request."""
    inst = dhan_inst(sym)
    if not inst:
        raise DhanDataError(f"{sym} is not in Dhan's instrument list")
    sid, seg, kind = inst
    now = ist_now()
    end = now.date() + timedelta(days=1)
    parts = []
    if interval == "1d":
        res = dhan_post("/charts/historical", {"securityId": sid, "exchangeSegment": seg, "instrument": kind, "expiryCode": 0, "oi": False,
                                               "fromDate": (end - timedelta(days=days + 2)).isoformat(), "toDate": end.isoformat()})
        df = dhan_to_df(res, True)
        return df
    mins = {"5m": 5, "15m": 15, "1h": 60}[interval]
    start = end - timedelta(days=days)
    while start < end:
        stop = min(end, start + timedelta(days=80))
        res = dhan_post("/charts/intraday", {"securityId": sid, "exchangeSegment": seg, "instrument": kind, "interval": mins, "oi": False,
                                             "fromDate": f"{start.isoformat()} 00:00:00", "toDate": f"{stop.isoformat()} 00:00:00"})
        d = dhan_to_df(res, False)
        if d is not None and not d.empty:
            parts.append(d)
        start = stop
    if not parts:
        return None
    df = pd.concat(parts)
    return df[~df.index.duplicated(keep="last")].sort_index()


def _days(period):
    return int(str(period).rstrip("dy")) * (365 if str(period).endswith("y") else 1)


def yahoo_frames(symbols, interval, days):
    """Candles per symbol from Yahoo (only used as the optional fallback)."""
    import yfinance as yf
    out = {}
    span = {"period": f"{min(days, 58)}d"} if interval != "1d" else {"start": (ist_now().date() - timedelta(days=days + 5)).isoformat()}
    tk = [yf_ticker(s) for s in symbols]
    try:
        data = yf.download(tk, interval=interval, group_by="ticker", auto_adjust=False, threads=True, progress=False, **span)
    except Exception:
        return out
    for s, t in zip(symbols, tk):
        try:
            d = _one(data, t, len(tk)) if data is not None and not data.empty else None
            if d is not None and not d.dropna(how="all").empty:
                out[s] = d.dropna(how="all")
        except Exception:
            continue
    return out


def dhan_frames(symbols, interval, days, progress=None):
    """({symbol: candles}, [problems]) from Dhan, with Yahoo filling the gaps only when YAHOO_FALLBACK is on."""
    out, errs = {}, []
    DHAN_STAT["step"] = "loading Dhan's instrument list"
    if not load_master("dhan"):
        DHAN_STAT["step"] = "Dhan's instrument list could not be loaded"
        return out, ["Dhan's instrument list could not be loaded."]
    for i, s in enumerate(symbols):
        DHAN_STAT["step"] = f"asking Dhan for {s} ({interval})"
        try:
            d = dhan_candles(s, interval, days)
            if d is None or d.empty:
                errs.append(f"{s}: Dhan returned no candles")
            else:
                out[s] = d
                DHAN_STAT.update(hist_ok=DHAN_STAT["hist_ok"] + 1, hist_at=time.time())
        except DhanDataError as e:
            errs.append(f"{s}: {e}")
            if e.auth:   # the same sign-in problem would repeat for every symbol
                DHAN_STAT["hist_error"] = str(e)
                errs.append("Stopped: " + str(e) + ". Put a fresh Dhan access token in the DHAN_ACCESS_TOKEN setting.")
                break
        except Exception as e:
            errs.append(f"{s}: {type(e).__name__}: {str(e)[:80]}")
        if progress:
            progress(i + 1, len(symbols))
    DHAN_STAT["step"] = None
    if out:
        DHAN_STAT["hist_error"] = None
    elif errs:
        DHAN_STAT["hist_error"] = errs[-1] if len(errs) == 1 else errs[0]
    if YAHOO_FALLBACK:
        miss = [s for s in symbols if s not in out]
        if miss:
            got = yahoo_frames(miss, interval, days)
            out.update(got)
            errs = [e for e in errs if e.split(":")[0] not in got]
    return out, errs


def candle_frames(symbols, interval, days):
    """One place to get candles for a few symbols from whichever source is active."""
    if use_dhan():
        return dhan_frames(symbols, interval, days)
    got = yahoo_frames(symbols, interval, days)
    return got, ([] if got else ["The data provider returned no candles."])


# ── The WebSocket client ──
def dhan_parse(buf):
    """Packets of one WebSocket message (little endian, 8 byte header: code, length, segment, security id).
    Returns [{"code", "seg", "sid", "ltp", "vol"?, "disc"?}]. Unknown packets are skipped."""
    out, i, n = [], 0, len(buf)
    while i + 8 <= n:
        code, length, seg, sid = struct.unpack_from("<BHBi", buf, i)
        size = DHAN_PKT.get(code) or (length if length >= 8 else 8)
        if i + size > n and code not in DHAN_PKT:
            size = n - i
        pkt = {"code": code, "seg": seg, "sid": sid}
        try:
            if code in (1, 2) and i + 12 <= n:
                pkt["ltp"] = struct.unpack_from("<f", buf, i + 8)[0]
            elif code == 4 and i + 50 <= n:
                pkt["ltp"] = struct.unpack_from("<f", buf, i + 8)[0]
                pkt["vol"] = struct.unpack_from("<i", buf, i + 22)[0]
            elif code == 8 and i + 62 <= n:
                pkt["ltp"] = struct.unpack_from("<f", buf, i + 8)[0]
                pkt["vol"] = struct.unpack_from("<i", buf, i + 22)[0]
            elif code == 6 and i + 12 <= n:
                pkt["prev"] = struct.unpack_from("<f", buf, i + 8)[0]   # the exchange's previous close, sent when a script is subscribed
            elif code == 50 and i + 10 <= n:
                pkt["disc"] = struct.unpack_from("<h", buf, i + 8)[0]
        except struct.error:
            break
        out.append(pkt)
        i += max(size, 8)
    return out


PREV_CLOSE = {}   # symbol -> {"p": the exchange's previous close, "d": the day it was received}


def dhan_on_packets(pkts, lookup, now_ts=None):
    """Apply parsed packets: update the live prices and feed the candle builder."""
    now_ts = now_ts or time.time()
    now = pd.Timestamp.fromtimestamp(now_ts, tz=IST)
    today = now.strftime("%Y-%m-%d")
    n = 0
    for p in pkts:
        if "prev" in p:
            sym = lookup.get((p["seg"], p["sid"]))
            if sym and p["prev"] == p["prev"] and p["prev"] > 0:
                PREV_CLOSE[sym] = {"p": float(p["prev"]), "d": today}
            continue
        if "ltp" not in p:
            continue
        sym = lookup.get((p["seg"], p["sid"]))
        price = p["ltp"]
        if not sym or not price or price != price or price <= 0:
            continue
        ref = candle_close(sym)
        if ref and abs(price / ref - 1) > LTP_MAX_JUMP:
            continue
        LTP[sym] = {"p": float(price), "t": now_ts, "d": today}
        n += 1
        if LIVE_CANDLES:
            live_tick(sym, float(price), now, p.get("vol") if (p.get("vol") or 0) > 0 else None)
    if n:
        DHAN_STAT["frames"] += n
        DHAN_STAT["last"] = now_ts
        FEED.update(count=len(LTP), t=now_ts, error=None)
    return n


def dhan_subscriptions(items):
    """Subscribe messages, at most 100 instruments each: quotes (with volume) for shares, ticker for indices."""
    msgs = []
    for code, group in ((17, [x for x in items if x[1] != "IDX_I"]), (15, [x for x in items if x[1] == "IDX_I"])):
        for i in range(0, len(group), 100):
            part = group[i:i + 100]
            msgs.append(json.dumps({"RequestCode": code, "InstrumentCount": len(part),
                                    "InstrumentList": [{"ExchangeSegment": ex, "SecurityId": str(tok)} for _, ex, tok in part]}))
    return msgs


def dhan_ws_healthy():
    return DHAN_STAT["ws"] == "connected" and time.time() - DHAN_STAT["last"] < 20


def dhan_housekeeping(last_touch):
    """About once a second while connected: finish candles that have just closed, and keep quiet shares' last prices valid."""
    now = time.time()
    if LIVE_CANDLES:
        live_boundary(pd.Timestamp.now(tz=IST))
    if now - last_touch >= 5:
        today = ist_now().strftime("%Y-%m-%d")
        for v in LTP.values():
            if v.get("d") == today:   # no new trade is not a stale price while the connection is alive
                v["t"] = now
        return now
    return last_touch


def dhan_ws_session(connect=None, stop=None):
    """One connection from connect to disconnect. Returns the reason it ended."""
    load_master("dhan")
    items = feed_items("dhan")
    lookup = {(DHAN_SEG[ex], int(tok)): name for name, ex, tok in items}
    c = feed_account().c
    url = f"{DHAN_WS_URL}?version=2&token={c['access_token']}&clientId={c['client_id']}&authType=2"
    if connect is None:
        from websockets.sync.client import connect as ws_connect
        connect = lambda u: ws_connect(u, open_timeout=15, max_size=2 ** 20, ping_interval=None)
    first = DHAN_STAT["connects"] == 0
    with connect(url) as ws:
        DHAN_STAT.update(ws="connected", error=None, connects=DHAN_STAT["connects"] + 1, subscribed=len(items), last=time.time())
        live_reset()
        for m in dhan_subscriptions(items):
            ws.send(m)
        if not first:
            for tf in LIVE_TFS:
                if STATES[tf]["rows"]:
                    threading.Thread(target=refresh, args=(tf,), daemon=True).start()   # fill what was missed while offline
        touch = heard = time.time()
        while market_window() and use_dhan() and not (stop and stop()):
            try:
                msg = ws.recv(timeout=1)
            except TimeoutError:
                msg = None
            if isinstance(msg, (bytes, bytearray)):
                heard = time.time()
                pkts = dhan_parse(bytes(msg))
                for p in pkts:
                    if "disc" in p:
                        code = str(p["disc"])
                        text = DHAN_DATA_ERRORS.get(code, f"Dhan closed the live feed (code {code})")
                        DHAN_STAT["error"] = text + f" ({code})"
                        return "auth" if code in DHAN_AUTH_CODES else "refused"
                dhan_on_packets(pkts, lookup)
            touch = dhan_housekeeping(touch)
            if time.time() - heard > 60:
                return "silent"
    return "closed"


def dhan_ws_loop():
    wait = 3
    while True:
        try:
            if not (use_dhan() and market_window()):
                DHAN_STAT["ws"] = "idle" if use_dhan() else "off"
                time.sleep(15)
                continue
            why = dhan_ws_session()
            DHAN_STAT["ws"] = "closed"
            if why == "auth":
                dhan_token_refresh("the live feed refused the token")
            wait = 3 if why == "closed" else 60 if why in ("auth", "refused") else 5
        except Exception as e:
            DHAN_STAT["ws"] = "error"
            text = f"{type(e).__name__}: {str(e)[:100]}"
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)
            if status in (401, 403):
                text = "Dhan refused the live feed sign-in (the access token has probably expired)"
                dhan_token_refresh("the live feed refused the token")
            elif status == 429:
                text = "Dhan allows 5 live feed connections: close the others"
            DHAN_STAT["error"] = text
            FEED["error"] = text
            wait = min(60, wait * 2)
        time.sleep(wait)


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
    rows, errors, hist = [], [], []
    for i in range(0, len(UNIVERSE), YF_BATCH):
        names = UNIVERSE[i:i + YF_BATCH]
        tickers = [yf_ticker(n) for n in names]
        if use_dhan():
            frames, errs = dhan_frames(names, interval, _days(PERIODS[interval]),
                                       progress=(lambda a, b, i=i: progress(list(rows), list(errors), i + a)) if progress else None)
            errors.extend(errs)
            for sname in names:
                df = frames.get(sname)
                try:
                    row = analyse(sname, df, interval) if df is not None else None
                    if row and LIVE_CANDLES and interval in LIVE_TFS:
                        tl = live_tail(df)
                        if tl is not None:
                            with LIVE_LOCK:
                                TAILS[(sname, interval)] = tl
                    if row:
                        hist.extend(row.pop("hist", []))
                        rows.append(row)
                    elif df is not None:
                        errors.append(f"{sname}: not enough data")
                except Exception as e:
                    errors.append(f"{sname}: {e}")
            if DHAN_STAT["hist_error"] and not frames and any("Stopped:" in e for e in errs):
                if progress:
                    progress(list(rows), list(errors), len(UNIVERSE))
                break
            if progress:
                progress(list(rows), list(errors), min(i + YF_BATCH, len(UNIVERSE)))
            continue
        import yfinance as yf  # imported lazily so the module loads without it
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
                    if row and LIVE_CANDLES and interval in LIVE_TFS:
                        tl = live_tail(df)
                        if tl is not None:
                            TAILS[(sname, interval)] = tl
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
                st.update(rows=rows, errors=errors, updated=ist_now().strftime("%d %b %H:%M:%S"))
                HIST[interval] = hist
            else:
                st["errors"] = errors or [f"{'Dhan' if use_dhan() else 'Yahoo'} returned no data."]
        if rows:
            try:
                check_alerts(interval, rows)
            except Exception as e:
                print("alert check failed:", e, flush=True)
            if interval in LIVE_TFS and live_active():
                live_rescan(interval)   # keep the candles the feed already finished if the provider is a candle behind
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
        ttl = LIVE_RESYNC_SECONDS if (use_dhan() and interval in LIVE_TFS and dhan_ws_healthy()) else REFRESH_SECONDS
        stale = not st["refreshing"] and time.time() - st["checked"] > ttl
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

    today = ist_now().date()
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


def _parse_sessions(text):
    """'2026-11-08 18:00-19:15; 2026-11-09 09:15-10:00' -> [(date string, start, end)]"""
    out = []
    for part in re.split(r"[;,]\s*(?=\d{4}-)", text or ""):
        m = re.match(r"\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$", part)
        if m:
            out.append((m.group(1), dtime(int(m.group(2)), int(m.group(3))), dtime(int(m.group(4)), int(m.group(5)))))
    return out


SPECIAL_SESSIONS = _parse_sessions(os.getenv("SPECIAL_SESSIONS", ""))   # e.g. Muhurat trading or a Saturday/Sunday special session
BUILD = "2026-11-11k"   # shown next to the user name so it is clear which version a server is running
SESSION_OVERRIDE = {}   # set by an admin from the Admin page for one day: {"date": ..., "start": time, "end": time}


def special_session_now():
    n = ist_now()
    td = n.strftime("%Y-%m-%d")
    spans = list(SPECIAL_SESSIONS) + ([(SESSION_OVERRIDE["date"], SESSION_OVERRIDE["start"], SESSION_OVERRIDE["end"])] if SESSION_OVERRIDE else [])
    return any(d == td and a <= n.time() <= b for d, a, b in spans)


def market_open_now():
    n = ist_now()
    return (n.weekday() < 5 and dtime(9, 15) <= n.time() <= dtime(15, 30)) or special_session_now()


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


MODE_PREF = {}   # user id -> "live" once they switched live on; cleared at sign-in, sign-out and server restart


def live_blocker(u):
    """Why this person cannot trade live right now, or None."""
    if not LIVE_REQUESTED:
        return "Live trading is switched off on this server."
    if not u["live_allowed"]:
        return "Live trading is not enabled for your account. Ask the admin."
    if not u["pin_set"]:
        return "Set your order PIN under Account to enable live trading."
    if not u["brokers"]:
        return "Add your broker details under Account to enable live trading."
    return None


def user_mode(u):
    why = live_blocker(u)
    if why:
        return "paper", "Paper mode: " + why
    if MODE_PREF.get(u["id"]) == "live":
        return "live", ""
    return "paper", "Paper mode: orders are simulated. Switch to Live with the toggle at the top when you are ready."


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
    ACCT_BAD.pop((uid, broker_id), None)


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

    def funds(self):
        d = (self._read("rmsLimit") or {}).get("data") or {}
        return _num(d.get("availablecash")) if d.get("availablecash") is not None else _num(d.get("net"))

    def margin_for(self, o, qty):
        return None  # Angel One's margin calculator is not used; the estimate falls back to the full order value

    def positions(self):
        out = []
        for x_ in (self._read("position") or {}).get("data") or []:
            net = _num(x_.get("netqty")) or 0
            avg = _num(x_.get("buyavgprice")) if net > 0 else _num(x_.get("sellavgprice")) if net < 0 else None
            un, re_ = _num(x_.get("unrealised")) or 0, _num(x_.get("realised")) or 0
            out.append({"label": str(x_.get("tradingsymbol") or x_.get("symbolname")), "qty": net, "avg": avg, "ltp": _num(x_.get("ltp")),
                        "realized": re_, "unrealized": un, "product": str(x_.get("producttype") or "")})
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
        for attempt in range(2):
            r = requests.request(method, DHAN_BASE + path, json=body, timeout=15,
                                 headers={"access-token": self.c["access_token"], "Content-Type": "application/json", "Accept": "application/json", **(extra or {})})
            if r.status_code == 401 and attempt == 0 and self.c.get("_feed") and dhan_token_refresh("an order request was refused"):
                continue    # the token had expired: nothing was placed, so it is safe to ask again with the new one
            break
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

    def funds(self):
        res = self.request("GET", "/fundlimit") or {}
        v = res.get("availabelBalance", res.get("availableBalance"))  # Dhan spells it "availabel"
        return _num(v)

    def margin_for(self, o, qty):
        """Margin Dhan wants for this order, or None when it cannot say."""
        body = {"dhanClientId": self.c["client_id"], "exchangeSegment": o["exchange"], "transactionType": o["side"], "quantity": int(qty),
                "productType": DHAN_PRODUCT[o["product"]], "securityId": str(o["token"]), "price": float(o["price"] or o["ref"]), "triggerPrice": 0}
        res = self.request("POST", "/margincalculator", body) or {}
        return _num(res.get("totalMargin"))

    def positions(self):
        out = []
        for x_ in self.request("GET", "/positions") or []:
            net = _num(x_.get("netQty")) or 0
            un, re_ = _num(x_.get("unrealizedProfit")) or 0, _num(x_.get("realizedProfit")) or 0
            cost = _num(x_.get("costPrice")) or 0
            ltp = cost + un / net if net and cost else None
            label = str(x_.get("tradingSymbol") or x_.get("securityId"))
            out.append({"label": label, "qty": net, "avg": cost or (_num(x_.get("buyAvg")) or _num(x_.get("sellAvg"))), "ltp": ltp,
                        "realized": re_, "unrealized": un, "product": str(x_.get("productType") or "")})
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


MASTER_LOCK = threading.Lock()


def opt_find(opt, symbol, exp_key, strike, right):
    """A contract from a broker's list. When the exact expiry is missing, the same strike a few days either side is used: when the exchange
    moves an expiry for a holiday, the option chain shows the new day while a broker's list can still carry the original day."""
    hit = opt.get((symbol, exp_key, strike, right))
    if hit:
        return hit
    try:
        want = datetime.strptime(exp_key, "%d%b%Y").date()
    except ValueError:
        return None
    best = None
    for (sym, ek, k, r), v in opt.items():
        if sym != symbol or r != right or abs(k - strike) > 1e-6:
            continue
        try:
            gap = abs((datetime.strptime(ek, "%d%b%Y").date() - want).days)
        except ValueError:
            continue
        if gap <= 4 and (best is None or gap < best[0]):
            best = (gap, v)
    return best[1] if best else None


def load_master(name="angel"):
    """Instrument list: ids, lot sizes, futures and option contracts for the dashboard's symbols. Cached for the day."""
    today = ist_now().date()
    m = MASTERS[name]
    if m["day"] == today:
        return True
    with MASTER_LOCK:   # the list is a large download: one thread fetches it while the others wait, instead of every thread fetching its own copy
        if m["day"] == today:
            return True
        if time.time() - m.get("tried", 0) < 60:   # a failed or partial download is not repeated on every call
            return m["day"] is not None
        m["tried"] = time.time()
        try:
            eq, opt, fut = _dhan_master() if name == "dhan" else _angel_master()
        except Exception:
            return m["day"] is not None
        if not eq or not opt:   # an interrupted download gives a list with the equities but no contracts: do not keep it for the day
            print(f"{name} instrument list looks incomplete ({len(eq)} shares, {len(opt)} option contracts); it will be fetched again", flush=True)
            if m["day"] is None:
                m.update(eq=eq, opt=opt, fut=fut)
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
        td = ist_now().strftime("%Y-%m-%d")
        return sum(1 for o in ORDERS if o.get("user") == uid and o["mode"] == mode and not o.get("is_exit") and o.get("date") == td)


def inr(x):
    return "₹" + f"{x:,.2f}"


def make_order(p, u, dry=False, force=None):
    """Check an order request against every limit. Returns (order, error, warnings, lines)."""
    kind = p.get("kind")
    symbol = str(p.get("symbol", "")).upper().strip()
    side = str(p.get("side", "")).upper()
    otype = str(p.get("order_type", "MARKET")).upper()
    product = str(p.get("product", "INTRADAY")).upper()
    tf = p.get("tf") if p.get("tf") in INTERVALS else DEFAULT_INTERVAL
    fail = lambda m: (None, m, [], [])
    mode, _ = user_mode(u)
    if force:
        mode = force
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
    if mode == "live" and not dry and not market_open_now():
        return fail("The market is closed. Live orders are accepted 9:15 to 15:30 IST, Monday to Friday, and in special sessions an admin has opened (Admin page).")
    if not dry and count_today(u["id"], mode) >= caps["max_orders"]:
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
        try:
            strike = float(p.get("strike"))
            exp_key = datetime.strptime(expiry, EXPIRY_FMT).strftime("%d%b%Y").upper()
        except (TypeError, ValueError):
            return fail("The option contract details are not valid.")
        if right not in ("CE", "PE"):
            return fail("Option type must be CE or PE.")
        if side == "SELL" and not caps["allow_option_selling"]:
            return fail("Selling options is switched off for your account.")
        if product not in ("INTRADAY", "CARRYFORWARD"):
            return fail("Product must be Intraday or Carry forward.")
        if not load_master(mname):
            return fail("Could not load Angel One's instrument list. Try again in a minute.")
        inst = M(mname)["opt"].get((symbol, exp_key, strike, right))   # exact day only: a real order must never go to a different expiry
        if not inst or not inst[2]:
            return fail("This contract was not found in Angel One's instrument list.")
        chain = chain_for(symbol, expiry, u)
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
        if not dry and any(o.get("user") == u["id"] and o.get("key") == key and not o.get("is_exit") and now - o["t"] < 60 for o in ORDERS):
            return fail("An identical order was sent less than a minute ago. Check the Portfolio tab before sending another.")

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
        warnings.append("Futures are leveraged and the stop-loss is not automatic. Place it from the Portfolio tab once the entry fills.")
    if selling_opt:
        warnings.append("Selling an option can lose far more than the premium you receive. It needs margin, and no stop-loss is placed for you. "
                        "If you are closing an option you bought, this is a sell to close.")

    if kind == "option" and otype == "MARKET":
        warnings.append("Market order on an option: it fills at whatever the best price is, which can be well away from the last price when the contract is thinly traded. A limit order is safer.")
    lines = [["Instrument", label], ["Side", side],
             ["Quantity", f"{qty} lot{'s' if qty > 1 else ''} x {lot} = {units} units" if kind != "equity" else f"{qty} shares"],
             ["Order type", otype.title()], ["Price", inr(price) if otype == "LIMIT" else f"Market (last price {inr(ref)})"],
             ["Product", product.title()], ["Estimated value", inr(est)], ["Mode", "LIVE, real money" if mode == "live" else "Paper, simulated"]]
    if auto_sl:
        lines.append(["Automatic stop-loss", "ON: placed for you as soon as the entry fills" + (", and trailed as targets are reached" if trail else "")])
        if mode == "live":
            warnings.append("Automatic stop-loss is switched on. The dashboard server places it for you after the fill, so it must be running. Check the Portfolio tab and your broker app.")
    if kind == "equity" and sl and not auto_sl:
        lines.append(["Stop-loss from the signal", inr(sl) + " (placed from the Portfolio tab once filled)"])
    if selling_opt:
        lines.append(["Exposure (strike x units)", inr(exposure)])
    if kind == "future" and risk:
        lines.append(["Stop-loss distance", f"{risk:.2f} points from your fill price" + ("" if auto_sl else " (placed from the Portfolio tab)")])
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
FEED_SECONDS = max(1, int(os.getenv("FEED_SECONDS", "5")))   # one call fetches every symbol; Dhan allows about one such call a second
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
            token_load(creds)
            if creds["client_id"] and not creds["access_token"] and _token_auto():
                dhan_token_refresh("no token yet", force=True)
        if not all(creds.values()):
            FEED["acct"] = None
            raise RuntimeError("The data feed credentials are not all set in the environment.")
        if DATA_BROKER == "dhan":
            creds["_feed"] = True
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
        if LIVE_CANDLES:
            live_tick(sym, float(p), pd.Timestamp.now(tz=IST))
    FEED.update(count=n, t=now, error=None)
    if LIVE_CANDLES:
        live_boundary(pd.Timestamp.now(tz=IST))
    return n


def market_window():
    n = ist_now()
    return (n.weekday() < 5 and dtime(9, 0) <= n.time() <= dtime(15, 45)) or special_session_now()


def feed_loop():
    while True:
        try:
            if market_window():
                if not (use_dhan() and dhan_ws_healthy()):   # the live stream does the work while it is healthy
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
            pc = r.get("prev_close")
            ex = PREV_CLOSE.get(r["symbol"])
            if ex and ex["d"] == ist_now().strftime("%Y-%m-%d") and (not pc or abs(ex["p"] / pc - 1) < 0.2):
                pc = r["prev_close"] = round(ex["p"], 2)   # the exchange's own previous close, the figure every broker app uses
            if pc:
                r["ltp_chg"] = round((p / pc - 1) * 100, 2)
        elif r.get("stale_day") and INTRA_CHG.get(r["symbol"]) is not None:
            r = {**r, "chg": round(float(INTRA_CHG[r["symbol"]]), 2)}   # no live price: today's change from the intraday candles
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
        try:
            optbot_once()
        except Exception as e:
            AUTO["error"] = f"optbot {type(e).__name__}: {str(e)[:100]}"
        try:
            if time.time() - AUTO.get("purged", 0) > 3600:
                AUTO["purged"] = time.time()
                x("DELETE FROM sessions WHERE expires<?", (time.time(),))
        except Exception:
            pass
        time.sleep(AUTOPILOT_SECONDS)


def start_engine():
    threading.Thread(target=autopilot_loop, daemon=True).start()
    if DATA_BROKER in ("angel", "dhan"):
        FEED["source"] = DATA_BROKER
        threading.Thread(target=feed_loop, daemon=True).start()
    if DATA_BROKER == "dhan":
        threading.Thread(target=dhan_ws_loop, daemon=True).start()
        if DHAN_AUTO_RENEW:
            threading.Thread(target=dhan_token_loop, daemon=True).start()


# ── BSE index options (Sensex, Bankex): NSE's site has no BSE contracts, so the chain is built from the broker's instrument list
# and priced with the broker's last-traded-price call. It shows prices only: open interest and IV are not available this way.
ACCT_BAD = {}   # (user id, broker) -> when that broker last refused the person's own sign-in (an expired token), so it is skipped for a few minutes


def _auth_problem(e):
    t = str(e).lower()
    return any(w in t for w in ("access token", "invalid or expired", "authenticat", "unauthor", "dh-901", "token is invalid", "token has expired", "invalid token", "session expired"))


def chain_accounts(u):
    """Every broker account that can price contracts: the person's own (unless it was refused lately), then the shared data feed account."""
    out, seen = [], set()
    for b in ([u.get("default_broker")] if u and u.get("default_broker") else []) + [x for x in (u or {}).get("brokers", [])]:
        if b in seen or time.time() - ACCT_BAD.get((u["id"], b), 0) < 300:
            continue
        seen.add(b)
        try:
            out.append((b, get_account(u["id"], b)))
        except Exception:
            continue
    if DATA_BROKER in ("angel", "dhan"):
        try:
            out.append((DATA_BROKER, feed_account()))
        except Exception:
            pass
    return out


def chain_account(u):
    """A broker account that can price contracts: the person's own, else the shared data feed account. Returns (broker id, account)."""
    accts = chain_accounts(u)
    return accts[0] if accts else (None, None)


def broker_chain(symbol, expiry, u):
    bid, acct = chain_account(u)
    if not acct and u and any(k[0] == u["id"] and time.time() - t < 300 for k, t in ACCT_BAD.items()):
        return {"error": "Your broker refused the access token (it has probably expired). Paste a fresh one under Account."}
    if not acct:
        return {"error": f"{symbol} option prices come from your broker. Add Angel One or Dhan details under Account first (or set DATA_BROKER on the server)."}
    if not load_master(bid):
        return {"error": "Could not load the broker's instrument list. Try again in a minute."}
    opt = M(bid)["opt"]
    exps = {}
    for (sym, ek, strike, right) in opt:
        if sym == symbol:
            try:
                exps[datetime.strptime(ek, "%d%b%Y").date()] = ek
            except ValueError:
                pass
    today = ist_now().date()
    upcoming = sorted(d for d in exps if d >= today)
    if not upcoming:
        return {"error": f"No {symbol} option contracts were found in {BROKER_NAMES[bid]}'s instrument list."}
    want = None
    if expiry:
        try:
            want = datetime.strptime(expiry, EXPIRY_FMT).date()
        except ValueError:
            want = None
    exp = want if want in upcoming else upcoming[0]
    ek = exps[exp]
    strikes = sorted({k for (sym, e, k, r) in opt if sym == symbol and e == ek})
    spot = None
    with LOCK:
        for t_ in INTERVALS:
            row = next((r for r in STATES[t_]["rows"] if r["symbol"] == symbol), None)
            if row:
                spot = apply_live([row])[0].get("ltp", row["close"])
                break
    if spot is None:
        spot = strikes[len(strikes) // 2]
    i = min(range(len(strikes)), key=lambda n: abs(strikes[n] - spot))
    window = strikes[max(0, i - STRIKES_EACH_SIDE): i + STRIKES_EACH_SIDE + 1]
    items, ref = [], {}
    for k in window:
        for right in ("CE", "PE"):
            inst = opt.get((symbol, ek, k, right))
            if inst:
                items.append((inst[3], str(inst[0])))
                ref[(k, right)] = (inst[3], str(inst[0]))
    key = ("B", symbol, ek, bid)
    hit = CHAIN_CACHE.get(key)
    if hit and time.time() - hit["t"] < 5:
        prices = hit["v"]
    else:
        try:
            prices = acct.ltp(items)
        except Exception as e:
            if _auth_problem(e) and u and bid in (u.get("brokers") or []) and acct is not FEED.get("acct"):
                ACCT_BAD[(u["id"], bid)] = time.time()
                return broker_chain(symbol, expiry, u)   # tries the shared data feed account next, or says the token expired
            return {"error": f"{BROKER_NAMES[bid]} did not return prices ({str(e)[:120]})."}
        CHAIN_CACHE[key] = {"t": time.time(), "v": prices}
    leg = lambda k, r: {"oi": None, "doi": None, "vol": None, "iv": None, "ltp": prices.get(ref.get((k, r)))}
    rows = [{"strike": k, "ce": leg(k, "CE"), "pe": leg(k, "PE")} for k in window]
    return {"underlying": round(float(spot), 2), "timestamp": None, "atm": strikes[i], "rows": rows, "pcr": None, "max_ce": None, "max_pe": None,
            "symbol": symbol, "expiry": exp.strftime(EXPIRY_FMT), "expiries": [d.strftime(EXPIRY_FMT) for d in upcoming[:4]], "stale": False,
            "source": "broker", "note": f"Prices from {BROKER_NAMES[bid]}. Open interest and IV are not available for {symbol}."}


def chain_for(symbol, expiry, u):
    """Option chain from NSE. When NSE blocks us, the broker's own prices are used instead (no open interest or IV)."""
    if symbol in BSE_INDICES:
        return broker_chain(symbol, expiry, u)
    ch = build_chain(symbol, expiry)
    if ch.get("error") or not ch.get("rows"):
        alt = broker_chain(symbol, expiry, u)
        if not alt.get("error"):
            return alt
    return ch


# ── Options autopilot: buys an at-the-money option in paper or live mode. Two autopilots with the same rules: one for the Intraday Scanner's
# stocks, one for NIFTY (see AUTO_RULES below for the entry and exit rules). Live mode must be armed with the order PIN each day.
# Orders are never retried. Positions are kept in the order records, so a restart keeps managing them. ──
OPTBOT_FIRST_ENTRY = dtime(9, 20)
OPTBOT_LAST_ENTRY = dtime(*map(int, os.getenv("OPTBOT_LAST_ENTRY", "14:30").split(":")))
OPTBOT_SQUAREOFF = dtime(*map(int, os.getenv("OPTBOT_SQUAREOFF", "15:15").split(":")))
OPTBOT_NEAR_ATR = float(os.getenv("OPTBOT_NEAR_ATR", "0.6"))   # price must be within this many ATRs of the 20 EMA
OPTBOT_MAX_CHG = float(os.getenv("OPTBOT_MAX_CHG", "2"))       # day change limit, percent, each side
OPTBOT_MAX_LOSS_PCT = float(os.getenv("OPTBOT_MAX_LOSS_PCT", "50"))   # the option stop is never set further than this percent below the price paid
OPTBOT = {}   # (user id, "scanner" or "nifty") -> settings. Two separate autopilots with their own mode, limits and Play/Pause/Stop. Kept in memory: after a restart the autopilot is Off until you switch it on again
OPTBOT_LOG = []
BOT_DIAG = {}      # user id -> (time, text) of the last 'why nothing was bought' note
OPTBOT_SKIP = {}   # (user id, symbol) -> time until which a stock that could not be traded is left alone
OPTBOT_DEFAULTS = {"mode": "off", "strategy": "scanner", "paused": False, "last_mode": "paper", "max_trades": 3, "lots": 1, "loss_limit": 3000.0, "armed_day": None, "entries": "both", "min_grade": "any"}


BOT_KINDS = ("scanner", "nifty")


def bot_kind(v):
    return "nifty" if v == "nifty" else "scanner"


def bot_cfg(uid, kind="scanner"):
    kind = bot_kind(kind)
    return OPTBOT.setdefault((uid, kind), dict(OPTBOT_DEFAULTS, strategy=kind, **({"max_trades": 2} if kind == "nifty" else {})))


def bot_log(uid, text, bot=None):
    OPTBOT_LOG.append({"uid": uid, "bot": bot, "t": ist_now().strftime("%H:%M:%S"), "text": text})
    del OPTBOT_LOG[:-200]


def bot_positions(uid, open_only=True, kind=None):
    with ORDER_LOCK:
        td = ist_now().strftime("%Y-%m-%d")
        return [o for o in ORDERS if o.get("user") == uid and o.get("bot") and (kind is None or bot_kind(o.get("bot_strategy")) == kind)
                and ((o.get("bot_state") == "open") if open_only else o.get("date") == td)]


def bot_pnl(o):
    """Profit so far on one autopilot trade: from the exit price once closed."""
    if o.get("bot_state") == "closed" and o.get("bot_exit_px") and o.get("avg_price"):
        return round((o["bot_exit_px"] - o["avg_price"]) * o["qty"], 2)
    return None


def bot_day_loss(uid, kind=None):
    """Net loss today across closed autopilot trades of one bot (0 or less means no loss)."""
    return -sum(bot_pnl(o) or 0 for o in bot_positions(uid, open_only=False, kind=kind))


def bot_place(u, o, entry_rec=None, exit_px=None):
    """Send one order (paper or live) and record it. Returns (record, error). Never retried."""
    live = o["mode"] == "live"
    if not live:
        rec = new_record(o, paper_id(), "complete", is_exit=entry_rec is not None)
        rec["avg_price"] = exit_px if entry_rec is not None else o["ref"]
        if entry_rec is not None:
            rec["bot_exit"] = True
        return rec, None
    bname = BROKER_NAMES.get(o.get("broker"), "broker")
    try:
        oid = get_account(u["id"], o["broker"]).place(o)
    except OrderRejected as e:
        return None, f"{bname} rejected the order: {e}"
    except Exception as e:
        new_record(o, "UNKNOWN-" + secrets.token_hex(3), "unknown", is_exit=entry_rec is not None,
                   note=f"{bname} did not confirm this order. Check your {bname} app.")
        return None, f"UNSURE: {bname} did not confirm the order ({str(e)[:100]}). Check your {bname} app."
    return new_record(o, oid, "open", is_exit=entry_rec is not None), None


def est_delta(spot, strike, right, expiry, iv):
    """Rough option delta (Black-Scholes, no interest). Used only to turn stock moves into option price moves. 0.5 when IV or the expiry is unknown."""
    try:
        t = max(0.5, (datetime.strptime(expiry, EXPIRY_FMT).date() - ist_now().date()).days + 0.5) / 365
        sig = float(iv) / 100
        if sig <= 0:
            raise ValueError
        d1 = (math.log(spot / strike) + 0.5 * sig * sig * t) / (sig * math.sqrt(t))
        nd = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
        return round(min(0.85, max(0.25, nd if right == "CE" else 1 - nd)), 3)
    except Exception:
        return 0.5


def bot_opt_ltp(u, pos):
    try:
        ch = chain_for(pos["symbol"], pos["bot_expiry"], u)
    except Exception:
        return None
    leg = next((r.get(pos["bot_right"].lower()) for r in (ch or {}).get("rows", []) if r["strike"] == pos["bot_strike"]), None)
    return (leg or {}).get("ltp")


# Entry rules, the same for the stock autopilot (the scanner's stocks) and the NIFTY autopilot, from 9:20 until OPTBOT_LAST_ENTRY:
#  1. FRESH: the indicator gives a signal on the 5 minute or the 15 minute chart (the candle has just closed), nothing has been hit yet,
#     and the price is still between the signal's stop and first target. The trade is taken at once.
#  2. CONTINUATION: a signal from an earlier day is still open (stop and first target not hit) and the price has come back to within
#     OPTBOT_NEAR_ATR of its 20 EMA on the right side. The trade is taken then.
# Both buy one at-the-money CE (BUY signal) or PE (SELL signal). The stop-loss and T1 to T4 are the indicator's own levels on the stock or index:
# the stop moves to the entry at T1, to T1 at T2, to T2 at T3, and everything is sold at T4 or at the stop. Closed by OPTBOT_SQUAREOFF.
OPTBOT_FRESH_BARS = int(os.getenv("OPTBOT_FRESH_BARS", "1"))   # a signal counts as fresh while it is at most this many candles old
AUTO_TFS = ("5m", "15m")
AUTO_ENTRY_TFS = tuple(t for t in os.getenv("AUTO_ENTRY_TFS", "5m").replace(" ", "").split(",") if t in AUTO_TFS) or ("5m",)   # entries are only taken on these charts; the other chart is still read for the same-direction zone check
AUTO_CONT_START = os.getenv("AUTO_CONT_START", "09:15")   # continuation trades (yesterday's signal still open, price back near the 20 EMA) are only taken in this window
AUTO_CONT_END = os.getenv("AUTO_CONT_END", "09:45")


def cont_window_open():
    t = ist_now().strftime("%H:%M")
    return AUTO_CONT_START <= t < AUTO_CONT_END


AUTO_MOVE_MIN = float(os.getenv("AUTO_MOVE_MIN", "1"))   # the scanner autopilot only trades stocks whose move today (up or down) is between these percentages
AUTO_MOVE_MAX = float(os.getenv("AUTO_MOVE_MAX", "3"))
AUTO_SEEN = {}   # (user id, bot, symbol, timeframe, signal time, side) -> True: each signal is acted on once
AUTO_RULES = ("Buys an at-the-money CE (BUY signal) or PE (SELL signal) when the indicator gives a fresh signal on the 5 minute chart (entries are taken on the 5 minute chart only; the 15 minute chart must be in the same direction: a BUY only while the other chart is also in a BUY zone, a SELL only while it is in a SELL zone, for fresh and continuation trades alike), "
              f"or, only from {AUTO_CONT_START} to {AUTO_CONT_END}, when a signal from an earlier day is still open and the price is back near its trend line. Stop-loss and targets T1 to T4 are the indicator's own: "
              "the stop moves to the entry at T1, to T1 at T2, to T2 at T3, and it sells everything at T4 or at the stop.")


def auto_rows(kind, tf):
    """The rows this autopilot may trade on one timeframe, with live prices and grades. None when there is no data yet."""
    if kind == "nifty":
        ensure_fresh(tf)
        names = ["NIFTY"]
    else:
        ensure_fresh(tf)
        with LOCK:
            names = [r["symbol"] for r in STATES[tf]["rows"] if r.get("type") != "index" and r["symbol"] not in INDICES and r["symbol"] not in SCANNER_EXCLUDE]
    with LOCK:
        by = {r["symbol"]: r for r in STATES[tf]["rows"]}
    rows = [by[s] for s in names if s in by]
    return add_conf(apply_live(rows), tf)


def auto_candidates(u, cfg, kind, why=None, manual=False, day=False):
    """Signals that qualify right now, fresh ones first and then by grade and closeness to the trend line. `why` collects the reason a live signal was left out.
    With day=True (the list for manual trading) every signal from today that fits the rules is returned, with a status: taken, open or passed."""
    ent = cfg.get("entries", "both")
    today = ist_now().strftime("%Y-%m-%d")
    out = []

    def no(label, text):
        if why is not None:
            why.append(f"{label}: {text}")
    cont_window, cont_noted = cont_window_open(), False
    tfrows = {tf: auto_rows(kind, tf) for tf in AUTO_TFS}
    zones = {tf: {r["symbol"]: r.get("side") for r in (tfrows[tf] or []) if (r.get("signal") or "–") != "–" and not r.get("sl_hit")} for tf in AUTO_TFS}   # who is in a BUY or SELL zone on each chart
    for tf in AUTO_TFS:
        if tf not in AUTO_ENTRY_TFS:
            continue
        rows = tfrows[tf]
        other = "15m" if tf == "5m" else "5m"
        zone = zones[other]
        if rows is None:
            no(tf, "no data yet")
            continue
        for r in rows:
            sym, side, sig = r["symbol"], r.get("side"), r.get("signal") or "–"
            label = f"{sym} {tf}"
            if sig == "–" or side not in ("BUY", "SELL") or not r.get("entry") or not r.get("sl") or (r.get("sl_hit") and not day):
                continue
            if kind == "scanner":
                mv = r.get("ltp_chg", r.get("chg"))
                if mv is None or not (AUTO_MOVE_MIN <= abs(mv) <= AUTO_MOVE_MAX):
                    no(label, f"{side} skipped: moved {'%+.2f%%' % mv if mv is not None else 'unknown'} today, needs {AUTO_MOVE_MIN:g}% to {AUTO_MOVE_MAX:g}% either way")
                    continue
            if zone.get(sym) != side:   # the 5 and the 15 minute charts must point the same way, for fresh and continuation entries alike
                z = zone.get(sym)
                no(label, f"{side} on {tf[:-1]} min skipped: the {other[:-1]} minute chart is " + (f"in a {z} zone" if z else "not in any zone"))
                continue
            sdate = r.get("signal_date")
            fresh = r.get("bars_ago") is not None and (day or r["bars_ago"] <= OPTBOT_FRESH_BARS) and sdate == today
            cont = not fresh and sig.startswith("Open") and bool(sdate) and sdate < today
            if not fresh and not cont:
                continue   # a signal from earlier today that is no longer fresh is not traded
            if cont and not cont_window:
                if not cont_noted:
                    no("continuation", f"yesterday's open signals are only traded from {AUTO_CONT_START} to {AUTO_CONT_END}")
                    cont_noted = True
                continue
            if (fresh and ent == "gap") or (cont and ent == "fresh"):
                continue
            passed = None
            if r.get("sl_hit"):
                passed = "the stop was hit"
            elif r.get("hits"):
                passed = f"already reached T{r['hits']}"
            px = float(r.get("ltp") or r["close"])
            lv = {k: r.get(k) for k in ("sl", "tp1", "tp2", "tp3", "tp4")}
            if not passed and ((side == "BUY" and not (lv["sl"] < px < lv["tp1"])) or (side == "SELL" and not (lv["tp1"] < px < lv["sl"]))):
                passed = "the price is already past T1 or the stop"
            if passed and not day:
                no(label, f"{side} signal {passed}")
                continue
            it = INTERNAL.get((sym, tf)) or {}
            dist = abs(px - it["ema"]) / it["atr"] if it.get("ema") and it.get("atr") else None
            if cont and not passed:
                if dist is None:
                    no(label, "no trend line yet")
                    continue
                if (side == "BUY" and px < it["ema"]) or (side == "SELL" and px > it["ema"]):
                    no(label, f"continuing {side} from {sdate}: waiting, price is on the wrong side of its trend line")
                    continue
                if dist > OPTBOT_NEAR_ATR:
                    no(label, f"continuing {side} from {sdate}: waiting for a pullback to its trend line ({dist:.1f} away, needs {OPTBOT_NEAR_ATR:g})")
                    continue
            g = (r.get("conf") or {}).get("g")
            if not grade_ok(cfg, g):
                no(label, f"{side} grade {g or 'none'} is below your minimum ({cfg.get('min_grade')})")
                continue
            key = (u["id"], kind, sym, tf, r.get("signal_time"), side)
            skey = f"{tf} {r.get('signal_time')} {side}"
            with ORDER_LOCK:
                done = any(o.get("user") == u["id"] and o.get("bot") and o["symbol"] == sym and o.get("date") == today and o.get("bot_sigkey") == skey for o in ORDERS)
            if done:
                AUTO_SEEN[key] = True
            if (AUTO_SEEN.get(key) or done) and not manual:
                continue
            cf = r.get("conf") or {}
            out.append({"symbol": sym, "side": side, "px": px, "levels": lv, "tf": tf, "etype": "fresh" if fresh else "continuation", "grade": g, "score": cf.get("s"),
                        "dist": dist if dist is not None else 9.9, "key": key, "sigkey": skey, "taken": bool(done), "time": r.get("signal_time"),
                        "status": "taken" if done else "passed" if passed else "open", "reason": passed,
                        "note": (f"fresh {tf} {side} signal at {r.get('signal_time')}" if fresh else f"{side} signal from {sdate} still open, back near its trend line on {tf}")
                                + (f", grade {g} ({cf.get('s')})" if g else "")})
    out.sort(key=lambda c: (c["etype"] != "fresh", -(c["score"] or 0), c["dist"]))
    return out


def auto_enter(u, cfg, kind):
    """Look at every qualifying signal and buy the best one. At most one purchase a look."""
    mode = "live" if cfg["mode"] == "live" else "paper"
    why = []
    cands = auto_candidates(u, cfg, kind, why)
    if why and not cands:   # say why, once per change or every 10 minutes, so a quiet autopilot can be understood
        text = "No trade: " + "; ".join(why[:6]) + (f"; and {len(why) - 6} more" if len(why) > 6 else "")
        last = BOT_DIAG.get((u["id"], kind))
        if not last or last[1] != text or time.time() - last[0] > 600:
            BOT_DIAG[(u["id"], kind)] = (time.time(), text)
            bot_log(u["id"], text, bot=kind)
    for c in cands:
        if OPTBOT_SKIP.get((u["id"], c["symbol"]), 0) > time.time():
            continue
        if any(p["symbol"] == c["symbol"] for p in bot_positions(u["id"], kind=kind)):
            continue   # one open trade on a stock at a time
        r = bot_buy(u, cfg, c, mode, strategy=kind, skip_expiry_day=(kind == "nifty"), etype=c["etype"])
        if r == "skip":
            continue
        AUTO_SEEN[c["key"]] = True
        return r != "changed"
    return False


def bot_buy(u, cfg, c, mode, strategy="scanner", skip_expiry_day=False, etype="fresh"):
    """Buy one at-the-money option for candidate c = {symbol, side, px, levels{sl, tp1, tp2}, note}. Returns "skip", "done", "changed" or "stop"."""
    right = "CE" if c["side"] == "BUY" else "PE"
    try:
        ch = chain_for(c["symbol"], None, u)
        if skip_expiry_day and ch and ch.get("expiry") == ist_now().strftime(EXPIRY_FMT) and len(ch.get("expiries") or []) > 1:
            ch = chain_for(c["symbol"], ch["expiries"][1], u)   # expiry day: the next expiry, because prices swing too wildly on the day
    except Exception:
        ch = None
    rows = (ch or {}).get("rows") or []
    if not rows or not ch.get("expiry"):
        OPTBOT_SKIP[(u["id"], c["symbol"])] = time.time() + 300
        bot_log(u["id"], f"{c['symbol']}: no option chain, skipped for 5 minutes.", bot=strategy)
        return "skip"
    row = min(rows, key=lambda r: abs(r["strike"] - c["px"]))
    ltp = (row.get(right.lower()) or {}).get("ltp")
    if not ltp:
        OPTBOT_SKIP[(u["id"], c["symbol"])] = time.time() + 300
        bot_log(u["id"], f"{c['symbol']}: no price for the {int(row['strike'])} {right}, skipped for 5 minutes.", bot=strategy)
        return "skip"
    limit = round(round(ltp * 1.01 / 0.05) * 0.05, 2)
    payload = {"kind": "option", "symbol": c["symbol"], "side": "BUY", "qty": cfg["lots"], "order_type": "LIMIT", "price": limit,
               "product": "INTRADAY", "tf": c.get("tf", "5m"), "expiry": ch["expiry"], "strike": row["strike"], "right": right}
    order, err, _, _ = make_order(payload, u, force=mode)
    if err:
        OPTBOT_SKIP[(u["id"], c["symbol"])] = time.time() + 300
        bot_log(u["id"], f"{c['symbol']} {right}: {err} (skipped for 5 minutes)", bot=strategy)
        return "skip"
    if order["mode"] != mode:
        bot_log(u["id"], "Your trading mode changed, so no order was sent.", bot=strategy)
        return "changed"
    rec, err = bot_place(u, order)
    if err:
        bot_log(u["id"], f"{c['symbol']} {right}: {err}", bot=strategy)
        notify_user(u["id"], f"Autopilot order failed: {c['symbol']}", err)
        if err.startswith("UNSURE"):
            cfg["mode"] = "off"
        return "stop"  # an order may have reached the broker, so stop for this round
    lv = c["levels"]
    delta = est_delta(c["px"], row["strike"], right, ch["expiry"], (row.get(right.lower()) or {}).get("iv"))
    p0 = float(ltp)
    p_sl = round(max(p0 - delta * abs(c["px"] - lv["sl"]), p0 * (1 - OPTBOT_MAX_LOSS_PCT / 100)), 2)
    p_t1 = round(p0 + delta * abs(lv["tp1"] - c["px"]), 2)
    p_t2 = round(p0 + delta * abs(lv["tp2"] - c["px"]), 2)
    rec.update(bot_p0=p0, bot_delta=delta, bot_p_sl=p_sl, bot_p_t1=p_t1, bot_p_t2=p_t2, bot_strategy=strategy, bot_etype=etype)
    rec.update(bot=True, bot_right=right, bot_strike=row["strike"], bot_expiry=ch["expiry"], bot_state="open", bot_dir=c["side"], bot_sl=lv["sl"], bot_t1=lv["tp1"], bot_t2=lv["tp2"],
               bot_t3=lv.get("tp3"), bot_t4=lv.get("tp4"), bot_step=0, bot_stop=lv["sl"], bot_tf=c.get("tf", "5m"), bot_sigkey=c.get("sigkey"), bot_entry_under=c["px"],
               bot_t1_done=False, bot_exit_px=None, bot_reason=None)
    append_log(rec)
    bot_log(u["id"], f"{'LIVE' if mode == 'live' else 'Paper'} BUY {rec['label']} at about {limit:.2f}: {c['symbol']} {c['px']:.2f}, {c['note']}. "
                     f"{c['symbol']} stop {lv['sl']:.2f}, T1 {lv['tp1']:.2f}, T2 {lv['tp2']:.2f}, T3 {lv['tp3']:.2f}, T4 {lv['tp4']:.2f}; option safety stop {p_sl:.2f} (delta {delta:.2f}).", bot=strategy)
    notify_user(u["id"], f"Autopilot bought {rec['label']}", f"{mode} order at about {limit:.2f}. {c['symbol']} stop {lv['sl']:.2f}, final target {lv['tp4']:.2f}.")
    return "done"


# ── NIFTY gap continuation: when yesterday's 5 min signal is still open and NIFTY opens the same way, buy a CE or PE at 9:20 ──
# Entry: yesterday's last 5 min signal is still open (BUY or SELL), NIFTY gaps the same way, and the first 5 min candle closes in that direction.
# Target: the next classic pivot (from yesterday's high, low and close). Stop: today's open or the nearest pivot behind price, whichever is closer.
NIFTY_GAP_MIN = float(os.getenv("NIFTY_GAP_MIN", "0.05"))          # percent, the smallest gap that counts
NIFTY_GAP_MAX = float(os.getenv("NIFTY_GAP_MAX", "1.5"))           # percent, bigger gaps are skipped (often exhausted)
NIFTY_MIN_TARGET_PCT = float(os.getenv("NIFTY_MIN_TARGET_PCT", "0.15"))
NIFTY_MIN_SL_PCT = float(os.getenv("NIFTY_MIN_SL_PCT", "0.12"))
NIFTY_MIN_RR = float(os.getenv("NIFTY_MIN_RR", "1.0"))             # reward must be at least this many times the risk
NIFTY_LAST_ENTRY = dtime(9, 45)


def classic_pivots(h, l, c):
    pp = (h + l + c) / 3
    return {"S3": l - 2 * (h - pp), "S2": pp - (h - l), "S1": 2 * pp - h, "PP": pp, "R1": 2 * pp - l, "R2": pp + (h - l), "R3": h + 2 * (pp - l)}


def nifty_plan(prev_side, prev, first, params=None):
    """prev = (high, low, close) of yesterday. first = (open, high, low, close) of today's first 5 min candle.
    Returns (plan, None), or (None, why not)."""
    P = {"gap_min": NIFTY_GAP_MIN, "gap_max": NIFTY_GAP_MAX, "min_tgt": NIFTY_MIN_TARGET_PCT, "min_sl": NIFTY_MIN_SL_PCT, "min_rr": NIFTY_MIN_RR, **(params or {})}
    if prev_side not in ("BUY", "SELL"):
        return None, "yesterday's 5 min signal is not open"
    up = prev_side == "BUY"
    ph, pl, pc = prev
    o, h, l, c = first
    gap = (o / pc - 1) * 100
    if (up and gap < P["gap_min"]) or (not up and gap > -P["gap_min"]):
        return None, f"gap {gap:+.2f}% is not in the direction of yesterday's {prev_side} signal"
    if abs(gap) > P["gap_max"]:
        return None, f"gap {gap:+.2f}% is bigger than {P['gap_max']:g}%"
    if (up and not (c > o and c > pc)) or (not up and not (c < o and c < pc)):
        return None, "the first 5 min candle did not confirm the gap"
    piv = classic_pivots(ph, pl, pc)
    entry = c
    ahead = sorted((v, k) for k, v in piv.items() if (v > entry * (1 + P["min_tgt"] / 100) if up else v < entry * (1 - P["min_tgt"] / 100)))
    if not ahead:
        return None, "no pivot level ahead to aim for"
    tgt, tname = ahead[0] if up else ahead[-1]
    behind = [("today's open", o)] + [(k, v) for k, v in piv.items() if (v < entry if up else v > entry)]
    behind = [(k, v) for k, v in behind if abs(entry - v) / entry * 100 >= P["min_sl"] and (v < entry if up else v > entry)]
    if behind:
        sname, sl = max(behind, key=lambda kv: kv[1]) if up else min(behind, key=lambda kv: kv[1])
    else:
        sname, sl = "a small fixed buffer", entry * (1 - 0.002) if up else entry * (1 + 0.002)
    risk, reward = abs(entry - sl), abs(tgt - entry)
    rr = reward / risk if risk else 0
    if rr < P["min_rr"]:
        return None, f"reward/risk {rr:.2f} is below {P['min_rr']:g}"
    tp1 = entry + (tgt - entry) / 2
    return {"side": prev_side, "entry": round(entry, 2), "sl": round(sl, 2), "tp1": round(tp1, 2), "tp2": round(tgt, 2), "gap": round(gap, 2), "rr": round(rr, 2),
            "sl_name": sname, "target_name": tname, "pivots": {k: round(v, 2) for k, v in piv.items()}}, None


def nifty_inputs(df, today, sym="NIFTY"):
    """From 5 min candles (IST index): yesterday's signal state and range, and today's first candle. Returns (prev_side, prev, first, None) or (..., why not)."""
    days = sorted(set(df.index.date))
    if today not in days:
        return None, None, None, "today's candles are not available yet"
    before = [d for d in days if d < today]
    if not before:
        return None, None, None, "no earlier day to compare with"
    y = before[-1]
    dy = df[df.index.date == y]
    prev = (float(dy["High"].max()), float(dy["Low"].min()), float(dy["Close"].iloc[-1]))
    row = analyse(sym, df[df.index.date <= y].copy(), "5m")
    side = row["side"] if row and row["signal"] != "–" else None
    dt = df[df.index.date == today]
    f = dt.iloc[0]
    if dt.index[0].time() != dtime(9, 15):
        return side, prev, None, "the 9:15 candle is missing"
    return side, prev, (float(f["Open"]), float(f["High"]), float(f["Low"]), float(f["Close"])), None


def bot_exit(u, pos, reason):
    if pos.get("bot_state") != "open" or pos["status"] in ("rejected", "cancelled", "expired", "unknown"):
        pos["bot_state"] = "closed"
        append_log(pos)
        return
    if pos["mode"] == "live" and pos["status"] != "complete":
        return  # wait for the entry to fill
    ch = None
    try:
        ch = chain_for(pos["symbol"], pos["bot_expiry"], u)
    except Exception:
        pass
    leg = next((r.get(pos["bot_right"].lower()) for r in (ch or {}).get("rows", []) if r["strike"] == pos["bot_strike"]), None)
    ltp = (leg or {}).get("ltp")
    if not ltp:
        if not pos.get("bot_warned"):
            pos["bot_warned"] = True
            notify_user(u["id"], f"Autopilot cannot price {pos['label']}", f"Exit wanted ({reason}) but no option price is available. Close it yourself if needed.")
        return
    limit = round(round(ltp * 0.99 / 0.05) * 0.05, 2)
    exit_o = {**pos, "side": "SELL", "order_type": "LIMIT", "price": limit, "ref": ltp, "key": None, "sl": None, "risk": None, "auto_sl": False, "trail": False}
    for k in [k for k in exit_o if k.startswith("bot")] + ["avg_price", "filled", "status", "id", "time", "date", "t"]:
        exit_o.pop(k, None)
    pos["bot_state"] = "closing"   # recorded first so a crash mid-call is never followed by a second sell
    append_log(pos)
    rec, err = bot_place(u, exit_o, entry_rec=pos, exit_px=ltp)
    if err:
        unsure = err.startswith("UNSURE")
        pos["bot_state"] = "unconfirmed" if unsure else "open"   # an unconfirmed sell is never sent again
        pos["bot_warned"] = True
        append_log(pos)
        if unsure:
            bot_cfg(u["id"], pos.get("bot_strategy"))["mode"] = "off"
        notify_user(u["id"], f"AUTOPILOT EXIT FAILED: {pos['label']}", f"{err}\nYour option position may still be open or may already be sold. Check your broker app now.")
        bot_log(u["id"], f"Exit of {pos['label']} failed: {err}", bot=bot_kind(pos.get("bot_strategy")))
        return
    pos.update(bot_state="closed", bot_exit_px=ltp, bot_reason=reason)
    append_log(pos)
    pnl = bot_pnl(pos)
    bot_log(u["id"], f"Sold {pos['label']} at about {limit:.2f} ({reason})" + (f", profit/loss {pnl:+.0f}." if pnl is not None else "."), bot=bot_kind(pos.get("bot_strategy")))
    notify_user(u["id"], f"Autopilot sold {pos['label']}", f"{reason}. Sold at about {limit:.2f}.")


def bot_manage(u, pos):
    """Follow the indicator's levels on the stock or index itself: stop-loss first, then T1 to T4 with the stop moving up behind them."""
    n = ist_now().time()
    kind = bot_kind(pos.get("bot_strategy"))
    if n >= OPTBOT_SQUAREOFF:
        return bot_exit(u, pos, "end of day")
    row, _ = row_for(pos["symbol"], "5m")
    up = pos["bot_dir"] == "BUY"
    if row:
        px = live_price(row)
        lv = [pos[k] for k in ("bot_t1", "bot_t2", "bot_t3", "bot_t4") if pos.get(k)]
        step = pos.get("bot_step", 1 if pos.get("bot_t1_done") else 0)
        stop = pos.get("bot_stop") or pos["bot_sl"]
        if (up and px <= stop) or (not up and px >= stop):
            return bot_exit(u, pos, "stock hit the signal stop-loss" if step == 0 else f"price came back to the stop after T{step}")
        moved = False
        while step < len(lv) and ((up and px >= lv[step]) or (not up and px <= lv[step])):
            step += 1
            moved = True
        if moved:
            entry = pos["bot_entry_under"]
            stop = entry if step == 1 else lv[step - 2]
            pos.update(bot_step=step, bot_stop=stop, bot_t1_done=True)
            append_log(pos)
            bot_log(u["id"], f"{pos['label']}: T{step} reached ({px:.2f}), stop moved to {stop:.2f}" + (" (the entry)." if step == 1 else f" (T{step - 1})."), bot=kind)
            if step >= len(lv):
                return bot_exit(u, pos, f"T{step} reached, all targets done")
    ltp = bot_opt_ltp(u, pos)
    if ltp and ltp <= pos["bot_p_sl"]:
        return bot_exit(u, pos, "option safety stop")


def optbot_once():
    if not market_open_now():
        return
    today = ist_now().strftime("%Y-%m-%d")
    now_t = ist_now().time()
    uids = {k[0] for k, c in OPTBOT.items() if c["mode"] != "off"} | {o["user"] for o in list(ORDERS) if o.get("bot") and o.get("bot_state") == "open"}
    for o in list(ORDERS):
        if o.get("bot") and o.get("bot_state") in ("open", "unconfirmed") and o.get("date") != today:
            o["bot_state"] = "closed"   # an intraday position from an earlier day is closed by the broker, so it is no longer managed
            o["bot_reason"] = "carried over from an earlier day"
            append_log(o)
    for uid in uids:
        u = user_by_id(uid)
        if not u:
            continue
        for kind in BOT_KINDS:
            cfg = bot_cfg(uid, kind)
            try:
                if cfg["mode"] == "live" and (cfg["armed_day"] != today or user_mode(u)[0] != "live"):
                    cfg["mode"] = "off"
                    bot_log(uid, f"Live {kind} autopilot was switched off because it was not armed today or your account is no longer in live mode.", bot=kind)
                    notify_user(uid, "Autopilot switched off", "Live autopilot needs your PIN each day, and your account must be in live mode.")
                for pos in bot_positions(uid, kind=kind):
                    if pos["mode"] == "live":
                        sync_status(u)
                    bot_manage(u, pos)
                if cfg["mode"] == "off" or cfg.get("paused") or not (OPTBOT_FIRST_ENTRY <= now_t <= OPTBOT_LAST_ENTRY):
                    continue
                made = bot_positions(uid, open_only=False, kind=kind)
                if len(made) >= cfg["max_trades"] or len(bot_positions(uid, kind=kind)) >= 2:
                    continue
                if bot_day_loss(uid, kind) >= cfg["loss_limit"]:
                    cfg["mode"] = "off"
                    bot_log(uid, f"Daily loss limit of {cfg['loss_limit']:.0f} reached. {kind.title()} autopilot switched off for today.", bot=kind)
                    notify_user(uid, "Autopilot stopped: loss limit", f"The daily loss limit of {cfg['loss_limit']:.0f} was reached.")
                    continue
                cfg["last_scan"] = now_t.strftime("%H:%M:%S")
                auto_enter(u, cfg, kind)
            except Exception as e:
                bot_log(uid, f"Error: {type(e).__name__}: {str(e)[:100]}", bot=kind)


# ── Confidence grade: how many independent checks agree with a signal. A, B or C. Never a promise: compare it with the History and Backtest win rates ──
CONF_A = float(os.getenv("CONF_A", "75"))
CONF_B = float(os.getenv("CONF_B", "55"))
CONF_MAX = {"htf": 20, "mkt": 15, "fresh": 15, "stretch": 15, "liq": 10, "stop": 10, "time": 10, "record": 5}
GRADE_RANK = {"A": 2, "B": 1, "C": 0, None: 0}
GRADE_NEED = {"any": 0, "B": 1, "A": 2}


def cf_htf(side, others):
    """others: {timeframe name: 'agree' | 'against' | None}"""
    pts = sum(10 if v == "agree" else 0 if v == "against" else 4 for v in others.values())
    agree = [k for k, v in others.items() if v == "agree"]
    against = [k for k, v in others.items() if v == "against"]
    if against:
        return pts, "Against it on " + " and ".join(against), False
    if agree:
        return pts, "Agrees on " + " and ".join(agree), len(agree) == len(others)
    return pts, "No signal on the other timeframes", False


def cf_mkt(side, idx_chg, sec_chg):
    def lean(c):
        if c is None:
            return None
        return "agree" if (c > 0.1 if side == "BUY" else c < -0.1) else "against" if (c < -0.1 if side == "BUY" else c > 0.1) else "flat"
    a, b = lean(idx_chg), lean(sec_chg)
    pts = {"agree": 1.0, "flat": 0.4, "against": 0.0, None: 0.4}
    total = round(7 * pts[a] + 8 * pts[b])
    word = "up" if side == "BUY" else "down"
    if a == "agree" and b == "agree":
        return total, f"NIFTY and its sector are both {word}", True
    if a == "against" or b == "against":
        who = "NIFTY" if a == "against" else "Its sector"
        return total, f"{who} is moving the other way", False
    return total, "NIFTY and the sector are flat or mixed", False


def cf_fresh(bars_ago):
    if bars_ago is None:
        return None
    if bars_ago <= 1:
        return 15, "Brand new signal" if bars_ago == 0 else "Signal is 1 candle old", True
    if bars_ago <= FRESH_BARS:
        return 10, f"Signal is {bars_ago} candles old", True
    if bars_ago <= 10:
        return 5, f"Signal is {bars_ago} candles old, the move has started", False
    return 0, f"Signal is {bars_ago} candles old", False


def cf_stretch(dist_atr, chg, side):
    pts, bits, ok = 0, [], True
    if dist_atr is None:
        pts += 5
    elif dist_atr <= 0.6:
        pts += 10
        bits.append("close to its trend line")
    elif dist_atr <= 1.2:
        pts += 6
        bits.append("a little away from its trend line")
        ok = False
    else:
        pts += 2
        bits.append("stretched from its trend line")
        ok = False
    if chg is not None:
        m = chg if side == "BUY" else -chg
        if 0 <= m <= 2:
            pts += 5
        elif m <= 3.5 and m > 2:
            pts += 3
            ok = False
            bits.append("already moved a lot today")
        elif m > 3.5:
            pts += 1
            ok = False
            bits.append("already moved a lot today")
        else:
            pts += 1
            ok = False
            bits.append("day is moving against it")
    return pts, ("Not stretched, " + ", ".join(bits) if ok and bits else "Not stretched" if ok else (", ".join(bits)[:1].upper() + ", ".join(bits)[1:])), ok


def cf_liq(val_cr, is_index):
    if is_index:
        return 8, "Index, always liquid", True
    if val_cr is None:
        return None
    if val_cr >= 100:
        return 10, f"High traded value (₹{val_cr:.0f} Cr so far)", True
    if val_cr >= 25:
        return 7, f"Good traded value (₹{val_cr:.0f} Cr so far)", True
    if val_cr >= 5:
        return 4, f"Modest traded value (₹{val_cr:.0f} Cr so far)", False
    return 1, f"Thin traded value (₹{val_cr:.1f} Cr so far)", False


def cf_stop(risk_pct):
    if risk_pct is None:
        return None
    if risk_pct <= 0.6:
        return 10, f"Small stop ({risk_pct:.2f}% of price)", True
    if risk_pct <= 1.2:
        return 7, f"Reasonable stop ({risk_pct:.2f}% of price)", True
    if risk_pct <= 2.5:
        return 4, f"Wide stop ({risk_pct:.2f}% of price)", False
    return 1, f"Very wide stop ({risk_pct:.2f}% of price)", False


def cf_time(hour, minute):
    if hour is None:
        return None
    t = hour * 60 + minute
    if t < 570:
        return 4, "Opening minutes are choppy", False
    if t < 690:
        return 10, "Good time of day (9:30 to 11:30)", True
    if t < 840:
        return 6, "Midday is usually slow", False
    if t < 900:
        return 7, "Afternoon session", True
    return 3, "Close to the end of the day", False


def cf_record(wins, total):
    if total < 5:
        return 2, "Too little history on this stock yet", False
    wr = wins / total
    if wr >= 0.55:
        return 5, f"Its past signals worked {wr * 100:.0f}% of the time ({total} signals)", True
    if wr >= 0.40:
        return 3, f"Its past signals worked {wr * 100:.0f}% of the time ({total} signals)", False
    return 1, f"Its past signals worked only {wr * 100:.0f}% of the time ({total} signals)", False


def conf_assemble(parts):
    """parts: {factor: (points, text, ok) or None}. Score is scaled to the factors that could be checked."""
    got = {k: v for k, v in parts.items() if v}
    avail = sum(CONF_MAX[k] for k in got)
    if not avail:
        return None
    score = round(sum(v[0] for v in got.values()) / avail * 100)
    return {"s": score, "g": "A" if score >= CONF_A else "B" if score >= CONF_B else "C", "w": [[bool(v[2]), v[1]] for v in got.values()], "n": len(got)}


def conf_context(tf):
    with LOCK:
        snaps = {t: list(STATES[t]["rows"]) for t in INTERVALS}
        hist = list(HIST.get(tf, []))
    cur = snaps[tf]
    nifty = next((r for r in cur if r["symbol"] == "NIFTY"), None)
    sec = {}
    for r in cur:
        if r.get("type") != "index" and r.get("chg") is not None:
            sec.setdefault(r.get("sector"), []).append(r["chg"])
    rec = {}
    for h in hist:
        if h.get("hits") or h.get("sl_hit"):
            b = rec.setdefault(h["symbol"], [0, 0])
            b[1] += 1
            b[0] += 1 if h.get("hits") else 0
    return {"others": {t: {r["symbol"]: r for r in snaps[t]} for t in INTERVALS if t != tf}, "nifty": (nifty or {}).get("chg"),
            "sec": {k: sum(v) / len(v) for k, v in sec.items() if v}, "rec": rec}


def conf_row(r, tf, ctx):
    side = r.get("side")
    if side not in ("BUY", "SELL") or not r.get("entry") or not r.get("sl"):
        return None
    others = {}
    for t, rows in ctx["others"].items():
        sg = (rows.get(r["symbol"]) or {}).get("signal") or "–"
        others[t] = "agree" if sg.endswith(side) else "against" if sg.endswith("BUY") or sg.endswith("SELL") else None
    it = INTERNAL.get((r["symbol"], tf)) or {}
    dist = abs(r["close"] - it["ema"]) / it["atr"] if it.get("ema") and it.get("atr") else None
    chg = r.get("ltp_chg", r.get("chg"))
    hh = mm = None
    try:
        if tf != "1d":
            hh, mm = map(int, r["signal_time"][-5:].split(":"))
    except Exception:
        hh = mm = None
    w, t = ctx["rec"].get(r["symbol"], [0, 0])
    idx = r.get("type") == "index"
    parts = {"htf": cf_htf(side, others), "mkt": cf_mkt(side, ctx["nifty"], None if idx else ctx["sec"].get(r.get("sector"))),
             "fresh": cf_fresh(r.get("bars_ago")), "stretch": cf_stretch(dist, chg, side), "liq": cf_liq(r.get("val_cr"), idx),
             "stop": cf_stop(abs(r["entry"] - r["sl"]) / r["entry"] * 100), "time": cf_time(hh, mm), "record": cf_record(w, t)}
    return conf_assemble(parts)


def add_conf(rows, tf):
    """Copies of the rows with a 'conf' grade on every row that has a signal."""
    ctx = conf_context(tf)
    out = []
    for r in rows:
        try:
            c = conf_row(r, tf, ctx)
        except Exception:
            c = None
        out.append({**r, "conf": c} if c else r)
    return out


def grade_ok(cfg, grade):
    return GRADE_RANK.get(grade, 0) >= GRADE_NEED.get(cfg.get("min_grade", "any"), 0)


# ── Backtest: replay the signals (or the gap-continuation rule) over past candles, then size the trades with the user's funds ──
BT_MAX_SYMBOLS = 10
BT_MAX_DAYS = {"5m": 58, "15m": 58, "1d": 3650}
BT_CACHE = {}   # (symbol, interval, days) -> (time, candles)
BT_JOBS = {}    # job id -> {"user", "state", "result", "error", "t"}
BT_LOT_FALLBACK = {"NIFTY": 75, "BANKNIFTY": 35, "FINNIFTY": 65, "MIDCPNIFTY": 140, "SENSEX": 20, "BANKEX": 30, "NIFTYNXT50": 25}
BT_LOT_MAP = {"day": None, "lots": {}}


def bt_lot_for(symbol):
    """Lot size from a loaded broker instrument master when there is one, else a built-in value for the indices, else None."""
    today = ist_now().date()
    if BT_LOT_MAP["day"] != today:
        lots = {}
        for name in ("dhan", "angel"):
            m = MASTERS.get(name) or {}
            for k, v in list((m.get("opt") or {}).items()):
                try:
                    lots.setdefault(k[0], int(v[2]))
                except Exception:
                    pass
        BT_LOT_MAP.update(day=today, lots=lots)
    return BT_LOT_MAP["lots"].get(symbol) or BT_LOT_FALLBACK.get(symbol)


def bt_norm(df):
    df = df.dropna(subset=["Close", "High", "Low", "Open"]).copy()
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    return df


def bt_fetch(symbols, interval, days, progress=None):
    """Candles per symbol from Yahoo, 10 minutes of caching. Returns ({symbol: candles}, [problems])."""
    out, bad, need, now = {}, [], [], time.time()
    for s in symbols:
        hit = BT_CACHE.get((s, interval, days))
        if hit and now - hit[0] < 600:
            out[s] = hit[1]
        else:
            need.append(s)
    if use_dhan():
        got, errs = dhan_frames(need, interval, days, progress=(lambda a, b: progress(a, b)) if progress else None)
        for s, d in got.items():
            d = bt_norm(d)
            if not d.empty:
                out[s] = d
                BT_CACHE[(s, interval, days)] = (time.time(), d)
        bad.extend(errs)
        return out, bad
    import yfinance as yf
    # Yahoo refuses intraday requests that reach back more than 60 days, so those use a period; daily candles use a start date
    span = {"period": f"{min(days, 58)}d"} if interval != "1d" else {"start": (ist_now().date() - timedelta(days=days + 5)).isoformat()}
    errs = []
    for i in range(0, len(need), 4):
        part = need[i:i + 4]
        tk = [yf_ticker(s) for s in part]
        data = None
        for attempt in range(2):
            try:
                data = yf.download(tk, interval=interval, group_by="ticker", auto_adjust=False, threads=True, progress=False, **span)
                if data is not None and not data.empty:
                    break
            except Exception as e:
                data = None
                errs.append(f"{type(e).__name__}: {str(e)[:80]}")
            time.sleep(1.0)
        for s, t in zip(part, tk):
            d = None
            try:
                d = _one(data, t, len(tk)) if data is not None and not data.empty else None
                d = bt_norm(d) if d is not None else None
            except Exception:
                d = None
            if d is None or d.empty:   # one more try for this symbol on its own
                try:
                    one = yf.download(t, interval=interval, auto_adjust=False, progress=False, **span)
                    if one is not None and not one.empty:
                        if getattr(one.columns, "nlevels", 1) > 1:
                            one.columns = one.columns.get_level_values(0)
                        d = bt_norm(one)
                except Exception as e:
                    errs.append(f"{type(e).__name__}: {str(e)[:80]}")
            if d is None or d.empty:
                bad.append(f"{s}: no candles from the data provider")
            else:
                out[s] = d
                BT_CACHE[(s, interval, days)] = (time.time(), d)
        if progress:
            progress(min(len(need), i + 4), len(need))
    if errs and not out:
        bad.append("Data provider said: " + errs[-1])
    return out, bad


def bt_arrays(df):
    """The signal series, computed exactly as the live scanner does it."""
    close = df["Close"]
    c, hi, lo = close.to_numpy(float), df["High"].to_numpy(float), df["Low"].to_numpy(float)
    ls, ss, d, atr = chandelier(df)
    ema = close.ewm(span=EMA_LEN, adjust=False).mean().to_numpy()
    green, red = qqe_states(close)
    n = len(c)
    up, dn = np.zeros(n, bool), np.zeros(n, bool)
    up[1:] = (d[1:] == 1) & (d[:-1] == -1)
    dn[1:] = (d[1:] == -1) & (d[:-1] == 1)
    buy = up & (c > ema) & within_lookback(green)
    sell = dn & (c < ema) & within_lookback(red)
    vol = df["Volume"].to_numpy(float) if "Volume" in df else np.zeros(n)
    return {"c": c, "ema": ema, "v": np.nan_to_num(vol), "o": df["Open"].to_numpy(float), "h": hi, "l": lo, "atr": atr, "ls": ls, "ss": ss, "buy": buy, "sell": sell}


def bt_levels(a, j):
    side = 1 if a["buy"][j] else -1
    entry = float(a["c"][j])
    raw = a["ls"][j] if side == 1 else a["ss"][j]
    risk = float(max(MIN_RISK_ATR * a["atr"][j], min(abs(entry - raw), MAX_RISK_ATR * a["atr"][j])))
    return side, entry, risk


def bt_walk(a, idx, j, k0, side, entry, sl, tp, intraday, sq):
    """Candles k0 onward: stop first, then target; intraday trades are closed at the square-off time. Returns (exit index, exit price, reason)."""
    n = len(a["c"])
    day = idx[j].date()
    for k in range(k0, n):
        if intraday and (idx[k].date() != day or idx[k].time() >= sq):
            if idx[k].date() != day:
                return k - 1, float(a["c"][k - 1]), "square-off"
            return k, float(a["o"][k]), "square-off"
        if side == 1:
            if a["l"][k] <= sl:
                return k, float(min(sl, a["o"][k])), "stop-loss"
            if a["h"][k] >= tp:
                return k, tp, "target"
        else:
            if a["h"][k] >= sl:
                return k, float(max(sl, a["o"][k])), "stop-loss"
            if a["l"][k] <= tp:
                return k, tp, "target"
    return n - 1, float(a["c"][n - 1]), "still open at the end of the data"


def bt_signal_trades(sym, df, interval, p):
    """Every signal in the period as one trade: entry at the signal candle's close, stop and target from the signal."""
    df = bt_norm(df) if df.index.tz is None else df
    if CONFIRMED_ONLY:
        df = drop_forming(df, interval)
    if len(df) < MIN_BARS:
        return [], f"{sym}: only {len(df)} candles, at least {MIN_BARS} are needed"
    a, idx = bt_arrays(df), df.index
    intraday = bool(p.get("intraday")) and interval != "1d"
    t_from, t_to, sq = p["entry_from"], p["entry_to"], p["squareoff"]
    out, free = [], -1
    dates = np.array([t.date() for t in idx])
    last_close = {}
    for k_ in range(len(idx)):
        last_close[dates[k_]] = a["c"][k_]
    ordered = sorted(last_close)
    prev_of = {d_: last_close[ordered[i_ - 1]] for i_, d_ in enumerate(ordered) if i_ > 0}
    day_val = pd.Series(a["c"] * a["v"], index=idx).groupby(dates).cumsum().to_numpy() / 1e7
    is_idx = sym in INDEX_YF
    need = GRADE_NEED.get(p.get("min_grade", "any"), 0)
    for j in np.nonzero(a["buy"] | a["sell"])[0]:
        j = int(j)
        if j <= free or j >= len(idx) - 1:
            continue
        side, entry, risk = bt_levels(a, j)
        sname = "BUY" if side == 1 else "SELL"
        pc = prev_of.get(dates[j])
        cf = conf_assemble({"fresh": cf_fresh(0), "stretch": cf_stretch(abs(a["c"][j] - a["ema"][j]) / a["atr"][j] if a["atr"][j] else None, (a["c"][j] / pc - 1) * 100 if pc else None, sname),
                            "liq": cf_liq(float(day_val[j]) if day_val[j] > 0 else None, is_idx), "stop": cf_stop(risk / entry * 100),
                            "time": cf_time(idx[j].hour, idx[j].minute) if interval != "1d" else None})
        if need and GRADE_RANK.get((cf or {}).get("g"), 0) < need:
            continue
        if side == -1 and not p.get("sell_ok", True):
            continue
        if intraday and not (t_from <= idx[j].time() <= t_to):
            continue
        sl, tp = entry - side * risk, entry + side * risk * p["target"]
        k, px, why = bt_walk(a, idx, j, j + 1, side, entry, sl, tp, intraday, sq)
        free = k
        out.append({"sym": sym, "side": side, "t_in": idx[j], "t_out": idx[k], "entry": entry, "exit": px, "risk": risk, "sl": sl, "tp": tp, "why": why, "kind": "signal", "grade": (cf or {}).get("g"), "score": (cf or {}).get("s")})
    return out, None


def bt_open_after(a, j, last):
    """Is the signal at candle j still alive at candle `last` (no stop-loss, not all four targets yet)? Same walk as the scanner's status."""
    side, entry, risk = bt_levels(a, j)
    sl, tps, hits = entry - side * risk, [entry + side * risk * r for r in R_MULTIPLES], 0
    for k in range(j + 1, last + 1):
        if (a["l"][k] <= sl) if side == 1 else (a["h"][k] >= sl):
            return False, side
        hits = max(hits, sum(1 for t in tps if ((a["h"][k] >= t) if side == 1 else (a["l"][k] <= t))))
        if hits == 4:
            return False, side
    return True, side


def bt_gap_trades(sym, df, interval, p):
    """Gap continuation: yesterday's signal still open, today gaps the same way and the first candle confirms: enter at its close, target the next pivot."""
    df = bt_norm(df) if df.index.tz is None else df
    if interval == "1d":
        return [], f"{sym}: gap continuation needs 5 or 15 minute candles"
    if len(df) < MIN_BARS:
        return [], f"{sym}: only {len(df)} candles, at least {MIN_BARS} are needed"
    a, idx = bt_arrays(df), df.index
    days = sorted(set(idx.date))
    sig = np.nonzero(a["buy"] | a["sell"])[0]
    params = {"gap_max": p["gap_max"], "min_tgt": p["min_tgt"], "min_sl": p["min_sl"]}
    out, whys = [], {}
    for d in days[1:]:
        before = [x for x in days if x < d]
        y = before[-1]
        iy = np.nonzero(idx.date == y)[0]
        idd = np.nonzero(idx.date == d)[0]
        if not len(iy) or not len(idd) or idx[idd[0]].time() != dtime(9, 15):
            continue
        last = int(iy[-1])
        js = sig[sig <= last]
        side = None
        if len(js):
            alive, sd_ = bt_open_after(a, int(js[-1]), last)
            if alive or last - int(js[-1]) <= FRESH_BARS:
                side = "BUY" if sd_ == 1 else "SELL"
        prev = (float(a["h"][iy].max()), float(a["l"][iy].min()), float(a["c"][last]))
        f = int(idd[0])
        plan, why = nifty_plan(side, prev, (float(a["o"][f]), float(a["h"][f]), float(a["l"][f]), float(a["c"][f])), params)
        if not plan:
            whys[re.sub(r"[-+]?\d+(\.\d+)?", "#", why)] = whys.get(re.sub(r"[-+]?\d+(\.\d+)?", "#", why), 0) + 1
            continue
        sd_ = 1 if plan["side"] == "BUY" else -1
        k, px, w = bt_walk(a, idx, f, f + 1, sd_, plan["entry"], plan["sl"], plan["tp2"], True, p["squareoff"])
        out.append({"sym": sym, "side": sd_, "t_in": idx[f], "t_out": idx[k], "entry": plan["entry"], "exit": px, "risk": abs(plan["entry"] - plan["sl"]),
                    "sl": plan["sl"], "tp": plan["tp2"], "why": w, "kind": "gap"})
    return out, (f"{sym}: {sum(whys.values())} days had no trade, mostly: " + max(whys, key=whys.get) if whys and not out else None)


def bs_price(S, K, T, sig, right):
    if T <= 0 or sig <= 0:
        return max(0.0, S - K) if right == "CE" else max(0.0, K - S)
    d1 = (math.log(S / K) + 0.5 * sig * sig * T) / (sig * math.sqrt(T))
    d2 = d1 - sig * math.sqrt(T)
    N = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return S * N(d1) - K * N(d2) if right == "CE" else K * N(-d2) - S * N(-d1)


def bt_strike_step(sym, px):
    if sym in ("NIFTY", "FINNIFTY", "NIFTYNXT50"):
        return 50
    if sym in ("BANKNIFTY", "BANKEX"):
        return 100
    if sym == "SENSEX":
        return 100
    return 2.5 if px < 250 else 5 if px < 500 else 10 if px < 1000 else 20 if px < 2500 else 50 if px < 5000 else 100


def bt_money(trades, kind, p):
    """Size and cost every trade with the user's funds, in time order. Returns (closed trades with P&L, skipped reasons, notes)."""
    cap = p["capital"]
    trades = sorted(trades, key=lambda t: t["t_in"])
    cash, openp, done, skipped, notes = cap, [], [], {}, []
    slip, other, brk = p["slippage"] / 100, p["other_pct"] / 100, p["brokerage"]
    lots_note = set()
    over_risk = 0

    def settle(upto):
        nonlocal cash
        for o in sorted([o for o in openp if o["t_out"] <= upto], key=lambda o: o["t_out"]):
            openp.remove(o)
            cash += o["pnl"]
            done.append(o)

    for t in trades:
        settle(t["t_in"])
        used = sum(o["margin"] for o in openp)
        free = cash - used
        risk_amt = cash * p["risk_pct"] / 100
        side, S0, S1, sym = t["side"], t["entry"], t["exit"], t["sym"]
        label, lot = sym, 1
        if kind in ("futures", "options"):
            lot = p.get("lot_size") if (p.get("lot_size") and len(p["symbols"]) == 1) else bt_lot_for(sym)
            if not lot:
                lot = p.get("lot_size") or 1
                lots_note.add(sym)
        if kind == "stock":
            risk_u = t["risk"]
            qty = int(risk_amt // risk_u) if risk_u > 0 else 0
            qty = min(qty, int(free * p["leverage"] // S0))
            if qty < 1:
                skipped["not enough funds for one share"] = skipped.get("not enough funds for one share", 0) + 1
                continue
            margin = S0 * qty / p["leverage"]
            ein, eout = S0 * (1 + side * slip), S1 * (1 - side * slip)
            gross = side * (eout - ein) * qty
            turn = (ein + eout) * qty
        elif kind == "futures":
            lots = int(risk_amt // (t["risk"] * lot)) if t["risk"] > 0 else 0
            per = S0 * lot * p["margin_pct"] / 100
            if per > free:
                skipped["not enough margin for one lot"] = skipped.get("not enough margin for one lot", 0) + 1
                continue
            if lots < 1:
                lots, over_risk = 1, over_risk + 1
            lots = min(lots, int(free // per))
            qty = lots * lot
            margin = per * lots
            ein, eout = S0 * (1 + side * slip), S1 * (1 - side * slip)
            gross = side * (eout - ein) * qty
            turn = (ein + eout) * qty
            label = f"{sym} futures"
        else:
            right = "CE" if side == 1 else "PE"
            step = bt_strike_step(sym, S0)
            K = round(S0 / step) * step
            sig = p["iv"] / 100
            T0 = p["dte"] / 365
            hold = max(0.0, (t["t_out"] - t["t_in"]).total_seconds() / 86400)
            T1 = max(p["dte"] - hold, 0.25) / 365
            p0 = max(0.05, bs_price(S0, K, T0, sig, right))
            p1 = max(0.0, bs_price(S1, K, T1, sig, right))
            p_sl = max(0.0, bs_price(t["sl"], K, T0, sig, right))
            buy_px, sell_px = p0 * (1 + slip), p1 * (1 - slip)
            loss_lot = max(buy_px - p_sl, 0.05 * buy_px) * lot
            lots = int(risk_amt // loss_lot)
            cost_lot = buy_px * lot
            if cost_lot > free:
                skipped["not enough funds for one lot"] = skipped.get("not enough funds for one lot", 0) + 1
                continue
            if lots < 1:
                lots, over_risk = 1, over_risk + 1
            lots = min(lots, int(free // cost_lot))
            qty = lots * lot
            margin = cost_lot * lots
            gross = (sell_px - buy_px) * qty
            turn = (buy_px + sell_px) * qty
            label = f"{sym} {int(K) if K == int(K) else K} {right}"
            ein, eout = buy_px, sell_px
        cost = 2 * brk + turn * other
        pnl = gross - cost
        slipc = (abs(ein - (p0 if kind == "options" else S0)) + abs(eout - (p1 if kind == "options" else S1))) * qty
        openp.append({**t, "qty": qty, "margin": margin, "pnl": pnl, "cost": cost + slipc, "label": label, "p_in": ein, "p_out": eout})
    if openp:
        settle(max(o["t_out"] for o in openp))
    if lots_note:
        notes.append("Lot size is not known for " + ", ".join(sorted(lots_note)) + ", so the lot size you entered (or 1) was used.")
    if over_risk:
        notes.append(f"{over_risk} trade(s) risked more than your limit because one lot was already bigger than it (a lot is the smallest you can trade).")
    return sorted(done, key=lambda o: o["t_out"]), skipped, notes


def bt_stats(done, p):
    cap = p["capital"]
    eq, peak, mdd, mdd_pct, curve, streak, worst_streak = cap, cap, 0.0, 0.0, [], 0, 0
    wins, losses = [], []
    months, by_sym, by_grade = {}, {}, {}
    for o in done:
        eq += o["pnl"]
        peak = max(peak, eq)
        dd = peak - eq
        mdd = max(mdd, dd)
        mdd_pct = max(mdd_pct, dd / peak * 100 if peak else 0)
        o["equity"] = eq
        curve.append([int(o["t_out"].timestamp()), round(eq, 2), round(dd / peak * 100 if peak else 0, 2)])
        (wins if o["pnl"] > 0 else losses).append(o["pnl"])
        streak = 0 if o["pnl"] > 0 else streak + 1
        worst_streak = max(worst_streak, streak)
        m = o["t_out"].strftime("%Y-%m")
        months[m] = months.get(m, 0) + o["pnl"]
        gb = by_grade.setdefault(o.get("grade") or "–", {"trades": 0, "wins": 0, "pnl": 0.0})
        gb["trades"] += 1
        gb["wins"] += 1 if o["pnl"] > 0 else 0
        gb["pnl"] += o["pnl"]
        b = by_sym.setdefault(o["sym"], {"trades": 0, "wins": 0, "pnl": 0.0})
        b["trades"] += 1
        b["wins"] += 1 if o["pnl"] > 0 else 0
        b["pnl"] += o["pnl"]
    n = len(done)
    net = eq - cap
    span = (done[-1]["t_out"] - done[0]["t_in"]).days if n else 0
    gw, gl = sum(wins), -sum(losses)
    if len(curve) > 400:
        step = len(curve) / 400
        curve = [curve[int(i * step)] for i in range(400)] + [curve[-1]]
    return {"capital": cap, "final": round(eq, 2), "net": round(net, 2), "growth_pct": round(net / cap * 100, 2),
            "cagr_pct": round(((eq / cap) ** (365 / span) - 1) * 100, 1) if span >= 60 and eq > 0 else None, "days": span,
            "max_dd": round(mdd, 2), "max_dd_pct": round(mdd_pct, 2), "trades": n, "win_rate": round(len(wins) / n * 100, 1) if n else 0,
            "avg_win": round(gw / len(wins), 2) if wins else 0, "avg_loss": round(-gl / len(losses), 2) if losses else 0,
            "profit_factor": round(gw / gl, 2) if gl else None, "expectancy": round(net / n, 2) if n else 0,
            "best": round(max((o["pnl"] for o in done), default=0), 2), "worst": round(min((o["pnl"] for o in done), default=0), 2),
            "losing_streak": worst_streak, "costs": round(sum(o["cost"] for o in done), 2), "curve": curve,
            "months": [{"m": k, "pnl": round(v, 2), "pct": round(v / cap * 100, 2)} for k, v in sorted(months.items())],
            "by_grade": [{"grade": k, "trades": v["trades"], "win_rate": round(v["wins"] / v["trades"] * 100, 1), "pnl": round(v["pnl"], 2)} for k, v in sorted(by_grade.items())],
            "by_symbol": [{"sym": k, "trades": v["trades"], "win_rate": round(v["wins"] / v["trades"] * 100, 1), "pnl": round(v["pnl"], 2)} for k, v in sorted(by_sym.items(), key=lambda kv: -kv[1]["pnl"])]}


def bt_run(params, candles):
    """candles: {symbol: DataFrame}. Returns the full result."""
    p = params
    trades, problems = [], []
    for sym, df in candles.items():
        try:
            tr, why = (bt_gap_trades if p["strategy"] == "gap" else bt_signal_trades)(sym, df, p["interval"], p)
        except Exception as e:
            tr, why = [], f"{sym}: could not be tested ({type(e).__name__})"
        trades += tr
        if why:
            problems.append(why)
    done, skipped, notes = bt_money(trades, p["kind"], p)
    res = bt_stats(done, p)
    res["found"] = len(trades)
    if p.get("min_grade") != "any":
        notes.append("Grades in the backtest use only what a past candle can show (freshness, stretch from the trend line, stop size, traded value, time of day). The live badge also checks NIFTY, the sector and higher timeframes, which a backtest cannot replay." if p["strategy"] == "signal" else "Grades do not apply to gap continuation trades, so the minimum grade was ignored.")
    res["skipped"] = [{"why": k, "n": v} for k, v in skipped.items()]
    res["problems"] = problems
    res["notes"] = notes
    res["list"] = [{"in": o["t_in"].strftime("%d %b %Y %H:%M"), "out": o["t_out"].strftime("%d %b %Y %H:%M"), "label": o["label"], "side": "BUY" if o["side"] == 1 else "SELL",
                    "qty": o["qty"], "entry": round(o["p_in"], 2), "exit": round(o["p_out"], 2), "why": o["why"], "kind": o["kind"], "grade": o.get("grade"), "pnl": round(o["pnl"], 2), "equity": round(o["equity"], 2)}
                   for o in done[-300:]][::-1]
    res["listed_of"] = len(done)
    return res


def bt_params(payload):
    """Validate the request. Returns (params, None) or (None, error)."""
    def num(k, lo, hi, d):
        try:
            v = float(payload.get(k, d))
        except (TypeError, ValueError):
            raise ValueError(f"{k} must be a number")
        if not (lo <= v <= hi):
            raise ValueError(f"{k} must be between {lo:g} and {hi:g}")
        return v
    def tm(k, d):
        try:
            h, m = str(payload.get(k, d)).split(":")
            return dtime(int(h), int(m))
        except Exception:
            raise ValueError(f"{k} must look like 09:20")
    try:
        interval = str(payload.get("interval", "15m"))
        if interval not in BT_MAX_DAYS:
            raise ValueError("Choose a timeframe of 5m, 15m or 1d")
        kind = str(payload.get("kind", "stock"))
        if kind not in ("stock", "futures", "options"):
            raise ValueError("Choose stock, futures or options")
        strategy = str(payload.get("strategy", "signal"))
        if strategy not in ("signal", "gap"):
            raise ValueError("Choose the signal or the gap continuation strategy")
        raw = payload.get("symbols", "")
        syms = [s.strip().upper() for s in (raw.split(",") if isinstance(raw, str) else raw) if str(s).strip()]
        syms = list(dict.fromkeys(syms))
        if not syms:
            raise ValueError("Enter at least one symbol, for example NIFTY or RELIANCE")
        if len(syms) > BT_MAX_SYMBOLS:
            raise ValueError(f"Test at most {BT_MAX_SYMBOLS} symbols at a time")
        unknown = [s for s in syms if s not in UNIVERSE]
        if unknown:
            raise ValueError("Not in the list of tracked symbols: " + ", ".join(unknown[:5]))
        mx = BT_MAX_DAYS[interval]
        days = int(num("days", 10 if interval != "1d" else 200, mx, min(mx, 58 if interval != "1d" else 730)))
        p = {"interval": interval, "kind": kind, "strategy": strategy, "symbols": syms, "days": days,
             "capital": num("capital", 10000, 1e9, 500000), "risk_pct": num("risk_pct", 0.1, 10, 1.0), "target": int(num("target", 1, 4, 2)),
             "intraday": bool(payload.get("intraday", interval != "1d")), "sell_ok": bool(payload.get("sell_ok", True)),
             "entry_from": tm("entry_from", "09:20"), "entry_to": tm("entry_to", "14:30"), "squareoff": tm("squareoff", "15:15"),
             "leverage": num("leverage", 1, 20, 5 if interval != "1d" else 1), "margin_pct": num("margin_pct", 3, 100, 12),
             "lot_size": int(num("lot_size", 0, 100000, 0)) or None, "iv": num("iv", 5, 150, 15), "dte": num("dte", 1, 60, 4 if "NIFTY" in "".join(syms) else 15),
             "brokerage": num("brokerage", 0, 500, 20), "other_pct": num("other_pct", 0, 1, 0.05), "slippage": num("slippage", 0, 2, 0.03),
             "min_grade": str(payload.get("min_grade", "any")) if str(payload.get("min_grade", "any")) in GRADE_NEED else "any",
             "gap_max": num("gap_max", 0.2, 5, 1.5 if all(s in INDEX_YF for s in syms) else 2.0), "min_tgt": NIFTY_MIN_TARGET_PCT, "min_sl": NIFTY_MIN_SL_PCT}
        if strategy == "gap" and interval == "1d":
            raise ValueError("Gap continuation needs a 5 or 15 minute timeframe")
        if interval == "1d" and p["intraday"]:
            p["intraday"] = False
        if strategy == "gap":
            p["intraday"] = True
        return p, None
    except ValueError as e:
        return None, str(e)


def bt_job(jid, user_id, p):
    job = BT_JOBS[jid]
    try:
        job["note"] = "Downloading price history"
        candles, bad = bt_fetch(p["symbols"], p["interval"], p["days"], lambda a, b: job.update(note=f"Downloaded {a} of {b} symbols"))
        if not candles:
            job.update(state="error", error="The data provider returned no candles. Try again in a minute, or test fewer symbols." + ((" " + " ".join(bad[-2:])) if bad else ""))
            return
        job["note"] = "Replaying the trades"
        res = bt_run(p, candles)
        res["problems"] = bad + res["problems"]
        res["params"] = {k: (v.strftime("%H:%M") if isinstance(v, dtime) else v) for k, v in p.items()}
        job.update(state="done", result=res)
    except Exception as e:
        job.update(state="error", error=f"The backtest failed ({type(e).__name__}). Try again.")


# ── Web app ──
app = FastAPI(title="SignalDesk", docs_url=None, redoc_url=None, openapi_url=None)


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
    return {"tf": tf, "scanned": scanned, "progress": progress, "step": DHAN_STAT.get("step") if progress else None, "stats": hist_stats(recs),
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

# ── Mobile app (installable web app) ──
# The dashboard installs on a phone from the browser: Android Chrome shows "Install app", iPhone Safari uses Share > Add to Home Screen.
BRAND = "SignalDesk"   # the name written into the pages below. Set APP_NAME on the server to show a different name everywhere.
APP_NAME = os.getenv("APP_NAME", BRAND).strip() or BRAND
APP_ICONS = {"192": "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",
             "512": "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",
             "mask": "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",
             "180": "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"}
PWA_PATHS = {"/healthz", "/manifest.webmanifest", "/sw.js", "/icon-192.png", "/icon-512.png", "/icon-maskable.png", "/apple-touch-icon.png", "/offline"}
SW_JS = """const V="v1";
self.addEventListener("install",e=>self.skipWaiting());
self.addEventListener("activate",e=>e.waitUntil(self.clients.claim()));
// Pages and data always come fresh from the server. Nothing private is ever stored. Offline, a short notice is shown instead.
self.addEventListener("fetch",e=>{
 if(e.request.mode==="navigate"){
  e.respondWith(fetch(e.request).catch(()=>new Response('<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><body style="font-family:system-ui;background:#12181e;color:#e6ebf0;display:grid;place-items:center;min-height:100vh;margin:0;text-align:center"><div><h2>You are offline</h2><p>Connect to the internet and open the app again.</p></div>',{headers:{"Content-Type":"text/html"}})));
 }
});"""
PWA_HEAD = ('<link rel="manifest" href="/manifest.webmanifest"><meta name="theme-color" content="#12181e">'
            '<meta name="mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-capable" content="yes">'
            '<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="%s">'
            '<link rel="apple-touch-icon" href="/apple-touch-icon.png"><link rel="icon" type="image/png" href="/icon-192.png">'
            '<script>if("serviceWorker" in navigator)addEventListener("load",function(){navigator.serviceWorker.register("/sw.js").catch(function(){})})</script>')


def brand(html):
    """Shows APP_NAME in place of the built-in name when the server sets a different one."""
    return html if APP_NAME == BRAND else html.replace(BRAND, APP_NAME.replace("&", "&amp;").replace("<", "&lt;"))


def pwa_head():
    return PWA_HEAD % APP_NAME.replace("&", "&amp;").replace("<", "&lt;").replace('"', "&quot;")


def _png(key):
    import base64
    return Response(base64.b64decode(APP_ICONS[key]), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/healthz")
def healthz():
    """Cheap check for Render's health check or an uptime pinger. Reveals nothing."""
    return {"ok": True}


@app.get("/manifest.webmanifest")
def pwa_manifest():
    return JSONResponse({"name": APP_NAME, "short_name": APP_NAME[:12], "description": "AI powered trade scanner", "start_url": "/", "scope": "/",
                         "display": "standalone", "orientation": "portrait", "background_color": "#12181e", "theme_color": "#12181e",
                         "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
                                   {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
                                   {"src": "/icon-maskable.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"}]},
                        media_type="application/manifest+json", headers={"Cache-Control": "public, max-age=3600"})


@app.get("/sw.js")
def pwa_sw():
    return Response(SW_JS, media_type="application/javascript", headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})


@app.get("/icon-192.png")
def pwa_i192():
    return _png("192")


@app.get("/icon-512.png")
def pwa_i512():
    return _png("512")


@app.get("/icon-maskable.png")
def pwa_imask():
    return _png("mask")


@app.get("/apple-touch-icon.png")
def pwa_i180():
    return _png("180")



CSP = ("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
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
    if path in PUBLIC_PATHS or path in PWA_PATHS:
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
    threading.Thread(target=warm_scans, daemon=True).start()


def warm_scans():
    """Scan the chart timeframes the autopilot and the page start on, one after another, so nobody has to press Scan after a restart."""
    order = []
    for tf in (DEFAULT_INTERVAL, *AUTO_TFS):
        if tf in INTERVALS and tf not in order:
            order.append(tf)
    for tf in order:
        try:
            refresh(tf)
        except Exception as e:
            print("start-up scan failed:", tf, type(e).__name__, flush=True)


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
    MODE_PREF.pop(rows[0]["id"], None)  # every sign-in starts in paper mode
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
        try:
            MODE_PREF.pop(request.state.user["id"], None)
        except Exception:
            pass
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
        snap["step"] = DHAN_STAT.get("step") if snap.get("refreshing") else None
    snap["rows"] = add_conf(snap["rows"], tf)
    return JSONResponse({
        **snap,
        "feed": {"source": FEED["source"], "age": None if not FEED["t"] else int(time.time() - FEED["t"]), "count": FEED["count"], "error": FEED["error"]},
        "interval": tf,
        "live": {"on": bool(live_active() and tf in LIVE_TFS), "at": LIVEC.get(tf, {}).get("at"), "source": data_source() if DATA_BROKER == "dhan" else "yahoo",
                 "stream": dhan_ws_healthy() if DATA_BROKER == "dhan" else False},
        "intervals": INTERVALS,
        "fresh_bars": FRESH_BARS,
        "symbols": SYMBOLS,
        "indices": INDICES,
    })


@app.get("/api/ticks")
def api_ticks(tf: str = DEFAULT_INTERVAL):
    """Just the live prices (a few KB), so the page can update every couple of seconds without reloading every row."""
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    with LOCK:
        rows = list(STATES[tf]["rows"])
    out = {r["symbol"]: [r["ltp"], r.get("ltp_chg")] for r in apply_live(rows) if r.get("ltp")}
    return {"p": out, "age": None if not FEED["t"] else int(time.time() - FEED["t"]), "source": FEED["source"]}


@app.post("/api/refresh")
def api_refresh(tf: str = DEFAULT_INTERVAL):
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    threading.Thread(target=refresh, args=(tf,), daemon=True).start()
    return {"ok": True}


# TradingView Lightweight Charts 4.1.3 (Apache-2.0), served from this app so the chart needs no outside website.
LW_GZ_B64 = (
    "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"
    "ZVZZ7FqcNFiTXgwiX8WqyfY56CIgW66NuTKsG6ISeWhL8tCmtQK9a8IyOEcgp8zpL1l8KzKlvkhOZZqKTJqKFPTP3Jb5F1d6/Oid"
    "7qRU9xUd/Rp9EWGnraNKdeXIoU3uInI5T2utVvmNOKPhSRETNxPZYfJNO5f4PN52AH1OV6URJMs/jPyo+K57ZdgMfiFVO2fp/I1G"
    "LmjQqRV3k7a5CdLej81K+SzzUpjUwU98147Mh/r3XDXiNtXA1WclRZoTS30UuRoiCVaNbFysNTX7FPTcGUeNaJ2EQ2osA1qnYBeU"
    "m05l1CSUcxkZjTHCqaudq4UGCYtcyMBg0zHPmEZ1EYUdrqC/pgJKGaKyr1iP4uEJNjs9rofZCnnYRY/psRAglTCHstx10IxuJA2o"
    "KeBbkzJYGPTkDwCGvGlJaYDuLACliaPeQJQ2O7+McmbnN9mRt0gIn30gsKZVC46XjpDE5Naj6TW/Fzn23wmHNGC5Q0s6AGQ8Bk82"
    "qvrlZJeJdFbLgXS+kUaozifSc0B/QbwQV4aOGsxwhgsU36lAAC0ZX0/PMX6yTx/FRYN8A//QJzRYBE1Yp/mmK3RjFlG6Bh4PY3uK"
    "i2lUIZcYRnUhvlODmErzsRIdl+ZqwN5oZ1MBiohsomuQOQm7MjbXKIaRd0iMImkyv+R00BsMmfkUEORfbdjranW3hfL9Im/Nr3OS"
    "aR03a7XwMkKE7LAbNfXc9cxvKb6SkOG3Ci9TSVq/5osAwx0BqWY/jnSEMgbQVhgLeXFUClD5NadLgmIlXoC+gmhgWBVsOD2yMfYx"
    "zZ0IzoGF3BK3CV5aSOhSAt0VVMJl1Poy/0zyOQ7bcdr8UTdi2VD8fNEeJoNb+cDbLAijqLkOv30r7yyC0eTXKW4Bv2mSvWuopSBq"
    "rnmWQy8tDD2BL3idygie5yYa5pffRKBGO+bgMP2tSjVtnJtlULqCQRbP0/AkDU/TcC8NjysAF49Tvkbtp10GXTSPAjjxOH2LHp23"
    "xwy7qJ8AmXhOxj3nCjKRu79dYYwg29SJtEL/xvw0BgpXkSWsOYpsK4HvztOZnfE2KiMSQs0vrQSuxYf7tsvIjYgi/F35R/MtUIC7"
    "WZoobTSprfBmYgdMcvWdjmUkM+yKyi7IXdB/dy7e7epKb0DRJsSKKQQCzfV0IrJZgmvKRvxUQV9JkKC9qK4wCo5NEqKnUmxhMcrb"
    "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"
    "n0ODFc8tAD+NTR9s/j//Hx1/kF2vdAIA"
)
_LW = {}


@app.get("/lw.js")
def lw_js(request: Request):
    import base64, gzip
    if "js" not in _LW:
        _LW["js"] = gzip.decompress(base64.b64decode(LW_GZ_B64))
    return Response(_LW["js"], media_type="application/javascript", headers={"Cache-Control": "private, max-age=86400"})


# ── Chart popup: candles, EMA, Chandelier line, signal markers and the latest signal's levels ──
CHART_CACHE = {}
CHART_BARS = {"5m": 400, "15m": 400, "1d": 500}


CHART_INDICATOR = os.getenv("CHART_INDICATOR", "1") != "0"   # draw the indicator's lines on charts (set 0 to show only the latest signal and its levels)


def indicator_pack(symbol, df, tf, stamp, lo_t=None, minbars=60):
    """The indicator drawn on a chart, from a candle table with an India-time index: its two lines and the latest buy or sell signal with
    stop-loss and targets. `stamp(ts)` gives the chart's time for a candle; only candles from `lo_t` on are returned (the earlier ones are warm-up)."""
    n = len(df)
    if n < minbars:
        return None
    c = df["Close"].to_numpy(float)
    ls, ss, d, atr = chandelier(df)
    ema = df["Close"].ewm(span=EMA_LEN, adjust=False).mean().to_numpy()
    green, red = qqe_states(df["Close"])
    up = np.zeros(n, bool)
    dn = np.zeros(n, bool)
    up[1:] = (d[1:] == 1) & (d[:-1] == -1)
    dn[1:] = (d[1:] == -1) & (d[:-1] == 1)
    buy = up & (c > ema) & within_lookback(green)
    sell = dn & (c < ema) & within_lookback(red)
    start = 0 if lo_t is None else next((i for i in range(n) if int(df.index[i].timestamp()) >= lo_t), n)
    out = {"stop": [], "ema": [], "sig": None}
    if CHART_INDICATOR:
        for i in range(start, n):
            v = ls[i] if d[i] == 1 else ss[i]
            if np.isfinite(v):
                out["stop"].append([stamp(df.index[i]), round(float(v), 2), int(d[i])])
            if np.isfinite(ema[i]):
                out["ema"].append([stamp(df.index[i]), round(float(ema[i]), 2)])
    js = np.flatnonzero(buy | sell)
    if len(js):
        j = int(js[-1])
        rec = eval_signals(symbol, df, tf, [j], buy, c, df["High"].to_numpy(float), df["Low"].to_numpy(float), atr, ls, ss)[0]
        sgn = 1 if rec["side"] == "BUY" else -1
        risk = abs(rec["entry"] - rec["sl"])
        out["sig"] = {"side": rec["side"], "t": stamp(df.index[j]), "visible": j >= start, "entry": rec["entry"], "sl": rec["sl"], "sl_hit": rec["sl_hit"], "hits": rec["hits"],
                      "tps": [round(rec["entry"] + sgn * risk * r, 2) for r in R_MULTIPLES], "time": rec["time"]}
    return out


def chart_data(symbol, tf, at=0):
    if use_dhan():
        df = dhan_frames([symbol], tf, _days(PERIODS[tf]))[0].get(symbol)
    else:
        import yfinance as yf
        df = yf.download(yf_ticker(symbol), period=PERIODS[tf], interval=tf, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return None
    if getattr(df.columns, "nlevels", 1) > 1:
        df.columns = df.columns.get_level_values(0)
    df = df.dropna(subset=["Close", "High", "Low", "Open"]).copy()
    row = analyse(symbol, df.copy(), tf)  # same rules as the main signal list, so the chart always agrees with it
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

    past = next((i for i in range(n) if at and int(df.index[i].timestamp()) == at and (buy[i] or sell[i])), None) if at else None
    start = max(0, n - CHART_BARS[tf])
    if past is not None:
        start = min(start, max(0, past - 60))   # an older signal opened from a list or a replay is shown with some candles before it
    t = [tm(df.index[i]) for i in range(start, n)]

    out = {"symbol": symbol, "tf": tf, "intraday": tf != "1d", "index": symbol in INDEX_YF,
           "candles": [{"time": t[k], "open": round(float(df["Open"].iloc[i]), 2), "high": round(float(df["High"].iloc[i]), 2),
                        "low": round(float(df["Low"].iloc[i]), 2), "close": round(float(c[i]), 2)} for k, i in enumerate(range(start, n))],
           "markers": [{"time": t[i - start], "side": "BUY" if buy[i] else "SELL"} for i in range(start, n) if buy[i] or sell[i]][-1:],
           "lines": None, "signal": None}
    if CHART_INDICATOR:
        out["lines"] = {"stop": [[t[i - start], round(float(ls[i] if d[i] == 1 else ss[i]), 2), int(d[i])] for i in range(start, n) if np.isfinite(ls[i] if d[i] == 1 else ss[i])],
                        "ema": [[t[i - start], round(float(ema[i]), 2)] for i in range(start, n)]}
    if past is not None:
        rec = eval_signals(symbol, df, tf, [past], buy, c, df["High"].to_numpy(float), df["Low"].to_numpy(float), atr, ls, ss)[0]
        out["markers"] = [{"time": t[past - start], "side": rec["side"]}] if past >= start else []   # a past signal opened from the history list is the one marked
        sgn = 1 if rec["side"] == "BUY" else -1
        risk = abs(rec["entry"] - rec["sl"])
        out["signal"] = {"side": rec["side"], "state": rec["side"] + " (past signal)", "status": hist_status(rec), "time": rec["time"],
                         "from": t[max(past, start) - start], "entry": rec["entry"], "sl": rec["sl"], "sl_hit": rec["sl_hit"], "hits": rec["hits"],
                         "tps": [round(rec["entry"] + sgn * risk * r, 2) for r in R_MULTIPLES]}
    elif row.get("entry") is not None:
        j = max((i for i in range(n) if buy[i] or sell[i]), default=None)
        if j is not None:
            out["signal"] = {"side": row.get("side"), "state": row["signal"], "status": row["status"], "time": row["signal_time"],
                             "from": t[max(j, start) - start], "entry": row["entry"], "sl": row["sl"], "sl_hit": row["sl_hit"], "hits": row["hits"],
                             "tps": [row["tp1"], row["tp2"], row["tp3"], row["tp4"]]}
    return out


@app.get("/api/chart")
def api_chart(symbol: str, tf: str = DEFAULT_INTERVAL, at: int = 0):
    symbol = symbol.upper().strip()
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    if not re.fullmatch(r"[A-Z0-9&\-]{1,20}", symbol) or (symbol not in UNIVERSE and symbol not in INDEX_YF):
        return JSONResponse({"error": "Unknown symbol."}, status_code=400)
    key = (symbol, tf, at)
    hit = CHART_CACHE.get(key)
    if hit and time.time() - hit[0] < 30:
        return JSONResponse(hit[1])
    try:
        d = chart_data(symbol, tf, at)
    except Exception as e:
        return JSONResponse({"error": f"Could not load candles ({type(e).__name__})."})
    if not d:
        return JSONResponse({"error": "Not enough candle data for this symbol and timeframe."})
    if len(CHART_CACHE) > 200:
        CHART_CACHE.clear()
    CHART_CACHE[key] = (time.time(), d)
    return JSONResponse(d)


OPT_HIST = {}   # (symbol, expiry, strike, right, minutes, days) -> (time, candles): option price history kept for a short while


def dhan_option_candles(symbol, expiry, strike, right, minutes, days):
    """Intraday candles of one option contract from Dhan: [{t, lbl, d, o, h, l, c, v}] for the last `days` trading days."""
    ek = datetime.strptime(expiry, EXPIRY_FMT).strftime("%d%b%Y").upper()
    if not load_master("dhan"):
        raise DhanDataError("Dhan's instrument list could not be loaded")
    inst = opt_find(M("dhan")["opt"], symbol, ek, float(strike), right)
    if not inst:
        raise DhanDataError(f"{symbol} {strike:g} {right} is not in Dhan's instrument list")
    end = ist_now().date() + timedelta(days=1)
    res = dhan_post("/charts/intraday", {"securityId": str(inst[0]), "exchangeSegment": inst[3], "instrument": "OPTIDX" if symbol in INDICES or symbol in BSE_INDICES else "OPTSTK",
                                         "interval": minutes, "oi": False, "fromDate": f"{(end - timedelta(days=7)).isoformat()} 00:00:00", "toDate": f"{end.isoformat()} 00:00:00"})
    df = dhan_to_df(res, False)
    if df is None or df.empty:
        return []
    keep = sorted({t.date() for t in df.index})[-days:]
    df = df[[t.date() in keep for t in df.index]]
    return [{"t": int(t.timestamp()), "lbl": t.strftime("%H:%M"), "d": t.strftime("%d %b"), "o": float(r.Open), "h": float(r.High), "l": float(r.Low), "c": float(r.Close), "v": int(r.Volume or 0)}
            for t, r in df.iterrows()]


def dhan_underlying_candles(symbol, minutes, days):
    """Candles of the option's parent stock or index from Dhan, in the same shape and for the same days as the option candles."""
    inst = dhan_inst(symbol)
    if not inst:
        raise DhanDataError(f"{symbol} is not in Dhan's instrument list")
    sid, seg, kind = inst
    end = ist_now().date() + timedelta(days=1)
    res = dhan_post("/charts/intraday", {"securityId": sid, "exchangeSegment": seg, "instrument": kind, "interval": minutes, "oi": False,
                                         "fromDate": f"{(end - timedelta(days=7)).isoformat()} 00:00:00", "toDate": f"{end.isoformat()} 00:00:00"})
    df = dhan_to_df(res, False)
    if df is None or df.empty:
        return []
    keep = sorted({t.date() for t in df.index})[-days:]
    df = df[[t.date() in keep for t in df.index]]
    return [{"t": int(t.timestamp()), "lbl": t.strftime("%H:%M"), "d": t.strftime("%d %b"), "o": float(r.Open), "h": float(r.High), "l": float(r.Low), "c": float(r.Close), "v": int(r.Volume or 0)}
            for t, r in df.iterrows()]


def candle_last_days(rows, days):
    """The rows of the last `days` trading days of a candle list."""
    seen = []
    for r in rows:
        if r["d"] not in seen:
            seen.append(r["d"])
    keep = set(seen[-days:])
    return [r for r in rows if r["d"] in keep]


def candle_rows_df(rows):
    """A candle list as the table the indicator reads (India-time index)."""
    df = pd.DataFrame({"Open": [r["o"] for r in rows], "High": [r["h"] for r in rows], "Low": [r["l"] for r in rows], "Close": [r["c"] for r in rows], "Volume": [r["v"] for r in rows]},
                      index=pd.to_datetime([r["t"] for r in rows], unit="s", utc=True).tz_convert(IST))
    return df


@app.get("/api/options/history")
def api_option_history(request: Request, symbol: str, expiry: str, strike: float, tf: str = "5m", days: int = 1):
    """Price history of the call and the put of one strike, from Dhan's intraday data."""
    symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9&\-]{1,20}", symbol):
        return {"error": "Enter a valid symbol."}
    minutes = {"1m": 1, "5m": 5, "15m": 15}.get(tf)
    if not minutes:
        return {"error": "Choose 1, 5 or 15 minutes."}
    days = 3 if days >= 3 else 1
    if DATA_BROKER != "dhan":
        return {"error": "Option price history comes from Dhan's data feed. Set DATA_BROKER=dhan on the server."}
    out = {"symbol": symbol, "expiry": expiry, "strike": strike, "tf": tf, "days": days}
    full = {}   # a week of candles per series: the indicator needs the earlier days to warm up, the chart shows only the last `days`
    for side in ("CE", "PE"):
        key = (symbol, expiry, float(strike), side, minutes, 7)
        hit = OPT_HIST.get(key)
        if hit and time.time() - hit[0] < 20:
            full[side.lower()] = hit[1]
            continue
        try:
            rows = dhan_option_candles(symbol, expiry, strike, side, minutes, 7)
        except DhanDataError as e:
            return {"error": f"Dhan: {e}"}
        except ValueError:
            return {"error": "The expiry date is not valid."}
        except Exception as e:
            return {"error": f"Could not read the price history ({type(e).__name__}: {str(e)[:100]})."}
        OPT_HIST[key] = (time.time(), rows)
        full[side.lower()] = rows
    ukey = (symbol, "UND", 0.0, "", minutes, 7)   # the parent script, on the same candle size and days, for a like-for-like comparison
    hit = OPT_HIST.get(ukey)
    if hit and time.time() - hit[0] < 20:
        full["und"] = hit[1]
    else:
        try:
            full["und"] = dhan_underlying_candles(symbol, minutes, 7)
            OPT_HIST[ukey] = (time.time(), full["und"])
        except Exception as e:
            full["und"] = []
            out["und_error"] = f"The {symbol} chart could not be loaded ({str(e)[:90]})."
    ind = {}
    for k, rows in full.items():
        shown = candle_last_days(rows, days)
        out[k] = shown
        ind[k] = None
        if shown:
            try:
                ind[k] = indicator_pack(symbol, candle_rows_df(rows), "5m", lambda ts: int(ts.timestamp()), lo_t=shown[0]["t"])
            except Exception as e:
                print("option indicator:", e, flush=True)
    out["ind"] = ind
    if not out["ce"] and not out["pe"]:
        out["error"] = "Dhan returned no candles for these contracts. They may not have traded yet, or the Data API is not subscribed."
    return out


@app.get("/api/options/chain")
def api_chain(request: Request, symbol: str, expiry: str = "", tf: str = DEFAULT_INTERVAL):
    symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9&\-]{1,20}", symbol):
        return JSONResponse({"error": "Enter a valid NSE stock symbol, for example RELIANCE."})
    if tf not in INTERVALS:
        tf = DEFAULT_INTERVAL
    chain = chain_for(symbol, expiry or None, request.state.user)
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


IDX_OPT = ("NIFTY", "BANKNIFTY", "SENSEX")
IDX_TFS = ("5m", "15m")


@app.get("/api/index/overview")
def api_index_overview():
    """The latest signal for NIFTY and BANKNIFTY on 5 min and 15 min. Opening the tab also starts any scan that is out of date."""
    for t_ in IDX_TFS:
        ensure_fresh(t_)
    out, scanning = {}, False
    for t_ in IDX_TFS:
        with LOCK:
            st = STATES[t_]
            rows = apply_live(list(st["rows"]))
            busy = st["refreshing"] or not st["updated"]
        scanning = scanning or busy
        out[t_] = {}
        for sym in IDX_OPT:
            r = next((x for x in rows if x["symbol"] == sym), None)
            out[t_][sym] = None if not r else {
                "signal": r["signal"], "status": r["status"], "signal_time": r["signal_time"], "bars_ago": r["bars_ago"],
                "price": r.get("ltp", r["close"]), "chg": r.get("ltp_chg", r.get("chg")),
                "levels": {k: r[k] for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4")} if r.get("entry") is not None else None}
    return {"symbols": list(IDX_OPT), "tfs": list(IDX_TFS), "data": out, "scanning": scanning}


@app.get("/api/index/signals")
def api_index_signals(symbol: str, tf: str = "15m", n: int = 10):
    """The most recent signals of one index on one timeframe, newest first, with what each one did."""
    symbol = symbol.upper().strip()
    if symbol not in IDX_OPT or tf not in IDX_TFS:
        return JSONResponse({"error": "Unknown index or timeframe."}, status_code=400)
    ensure_fresh(tf)
    with LOCK:
        recs = [r for r in HIST[tf] if r["symbol"] == symbol]
        scanned = STATES[tf]["updated"]
    recs.sort(key=lambda r: r["t"], reverse=True)
    out = []
    for r in recs[:max(1, min(n, 30))]:
        sg = 1 if r["side"] == "BUY" else -1
        risk = abs(r["entry"] - r["sl"])
        out.append({"t": r["t"], "time": r["time"], "side": r["side"], "entry": r["entry"], "sl": r["sl"],
                    "tps": [round(r["entry"] + sg * risk * m, 2) for m in R_MULTIPLES], "hits": r["hits"], "sl_hit": r["sl_hit"], "status": hist_status(r)})
    return {"symbol": symbol, "tf": tf, "signals": out, "scanned": bool(scanned)}


# ── Intraday scanner: NSE's most active stocks, with the signal on 5 min / 15 min ──
ACTIVE_CACHE = {}
# Large and mid caps only. Large = NIFTY 100, mid = NIFTY MIDCAP 150, read from NSE and kept for 12 hours.
# If NSE does not answer, the scanner drops the F&O stocks listed here, which are my best guess of the smaller ones, and says so.
SMALLCAP_FALLBACK = set("AMBER ANANDRATHI ANGELONE CAMS CDSL CROMPTON CUB DATAPATTNS FORCEMOT IEX INOXWIND KARURVYSYA KFINTECH MANAPPURAM NAVINFLUOR PGEL RBLBANK SAGILITY SYRMA UJJIVANSFB".split())
SCANNER_EXCLUDE = {x.strip().upper() for x in os.getenv("SCANNER_EXCLUDE", "").split(",") if x.strip()}   # always leave these out
SCANNER_INCLUDE = {x.strip().upper() for x in os.getenv("SCANNER_INCLUDE", "").split(",") if x.strip()}   # always keep these
CAP_CACHE = {"t": 0.0, "map": None}


def cap_classes():
    """{symbol: "Large" | "Mid"} from NSE's NIFTY 100 and NIFTY MIDCAP 150 lists, or None when NSE does not answer for both."""
    if CAP_CACHE["map"] is not None and time.time() - CAP_CACHE["t"] < 12 * 3600:
        return CAP_CACHE["map"]
    out = {}
    for idx, label in (("NIFTY 100", "Large"), ("NIFTY MIDCAP 150", "Mid")):
        raw = nse.get_json("/api/equity-stockIndices", {"index": idx})
        names = [str(r.get("symbol", "")).upper() for r in ((raw or {}).get("data") or []) if isinstance(r, dict)]
        names = [x for x in names if x and x != idx]
        if len(names) < 30:   # an answer that small is not a real index list
            return CAP_CACHE["map"]
        for x in names:
            out.setdefault(x, label)
    CAP_CACHE.update(t=time.time(), map=out)
    return out


# Intraday focus: rank by traded value (what NSE's most-active page sorts by) and skip cheap shares. Raw volume is dominated by low-priced
# stocks such as IDEA, YESBANK and SUZLON, which are hard to trade as futures or options. Both can be changed with environment variables.
SCANNER_INDICES = [x.strip().upper() for x in os.getenv("SCANNER_INDICES", "NIFTY,SENSEX").split(",") if x.strip()]   # indices shown at the top of the scanner
SCANNER_RANK = os.getenv("SCANNER_RANK", "value").strip().lower()
SCANNER_RANK = SCANNER_RANK if SCANNER_RANK in ("value", "volume") else "value"
SCANNER_MIN_PRICE = float(os.getenv("SCANNER_MIN_PRICE", "150") or 0)
SCANNER_TOP = max(1, int(os.getenv("SCANNER_TOP", "15") or 15))   # how many of the day's most active F&O stocks the scanner follows


def nse_most_active(by):
    """Symbols in NSE's most-active list (by value or volume), most active first. None if NSE does not answer."""
    hit = ACTIVE_CACHE.get(by)
    if hit and time.time() - hit[0] < 60:
        return hit[1]
    raw = nse.get_json("/api/live-analysis-most-active-securities", {"index": by})
    items = None
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        for v in [raw.get("data")] + list(raw.values()):
            if isinstance(v, list) and v and isinstance(v[0], dict) and "symbol" in v[0]:
                items = v
                break
    if not items:
        return hit[1] if hit else None
    names = [str(i.get("symbol", "")).upper() for i in items if i.get("symbol")]
    ACTIVE_CACHE[by] = (time.time(), names)
    return names


@app.get("/api/scanner")
def api_scanner(tf: str = "15m", only: int = 0):
    """Today's most active large and mid cap stocks by traded value, priced above the minimum, with their signal on 5 min or 15 min."""
    if tf not in ("5m", "15m"):
        return JSONResponse({"error": "Choose 5 min or 15 min."}, status_code=400)
    by, n = SCANNER_RANK, SCANNER_TOP
    ensure_fresh(tf)
    with LOCK:
        all_rows = apply_live(list(STATES[tf]["rows"]))
    rows = add_conf([r for r in all_rows if r.get("type") != "index"], tf)
    sec = {}
    for r in rows:   # how each sector is doing today: the average day change of its stocks
        c_ = r.get("ltp_chg", r.get("chg"))
        if c_ is not None:
            sec.setdefault(r.get("sector"), []).append(c_)
    sec_chg = {k: round(sum(v) / len(v), 2) for k, v in sec.items() if v}
    idx_rows = add_conf([r for r in all_rows if r.get("symbol") in SCANNER_INDICES], tf)
    with LOCK:
        upd, busy = STATES[tf]["updated"], STATES[tf]["refreshing"] or not STATES[tf]["updated"]
    by_sym = {r["symbol"]: r for r in rows}
    caps = cap_classes()
    cheap = []

    def allowed(sym):
        if sym in SCANNER_EXCLUDE:
            return False
        r_ = by_sym.get(sym)
        if r_ is not None and SCANNER_MIN_PRICE and r_.get("ltp", r_["close"]) < SCANNER_MIN_PRICE:
            cheap.append(sym)
            return False
        if sym in SCANNER_INCLUDE:
            return True
        return sym in caps if caps is not None else sym not in SMALLCAP_FALLBACK
    names = nse_most_active(by)
    source, skipped = "nse", 0
    if names:
        ranked = [x for x in names if x in by_sym and allowed(x)]
        skipped = len([x for x in names if x not in by_sym])
    else:
        source = "computed"
        key = "val_cr" if by == "value" else "vol"
        ranked = [r["symbol"] for r in sorted((r for r in rows if r.get(key) and allowed(r["symbol"])), key=lambda r: -r[key])]
    out = []

    def item(r, rank, is_index=False):
        return {"rank": rank, "symbol": r["symbol"], "cap": None if is_index else (caps or {}).get(r["symbol"]), "index": is_index,
                "sector": "Index" if is_index else r.get("sector"), "sector_chg": None if is_index else sec_chg.get(r.get("sector")),
                "price": r.get("ltp", r["close"]), "chg": r.get("ltp_chg", r.get("chg")), "vol": None if is_index else r.get("vol"),
                "val_cr": None if is_index else r.get("val_cr"), "signal": r["signal"], "status": r["status"],
                "signal_time": r["signal_time"], "bars_ago": r["bars_ago"], "side": r.get("side"), "conf": r.get("conf"),
                "levels": {k: r[k] for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4")} if r.get("entry") is not None else None}
    for r in sorted(idx_rows, key=lambda r: SCANNER_INDICES.index(r["symbol"])):   # the indices come first, unranked
        if not (only and r["signal"] == "–"):
            out.append(item(r, None, True))
    stocks = 0
    for sym in ranked:
        r = by_sym[sym]
        if only and r["signal"] == "–":
            continue
        stocks += 1
        out.append(item(r, stocks))
        if stocks >= n:
            break
    return {"tf": tf, "by": by, "source": source, "cap_source": "nse" if caps is not None else "approx", "rows": out, "skipped": skipped, "top": n, "cheap": len(set(cheap)), "min_price": SCANNER_MIN_PRICE, "rank": by, "scanning": busy, "updated": upd,
            "note": (f"Today's ten most active large-cap and mid-cap F&O stocks by traded {by}, from NSE's most active list. Large cap = NIFTY 100, mid cap = NIFTY MIDCAP 150. Stocks priced under ₹{SCANNER_MIN_PRICE:g} are left out. Only F&O stocks are shown, because those are the ones that can be traded as futures or options."
                     if source == "nse" else
                     f"NSE's list did not load, so today's ten highest-{by} large-cap and mid-cap F&O stocks above ₹{SCANNER_MIN_PRICE:g} are taken from the candle data instead. Open this page again later to use NSE's own list.") +
                    ("" if caps is not None else " NSE's NIFTY 100 / MIDCAP 150 lists did not load either, so the smaller F&O stocks are dropped using a built-in list that may be a little out of date.")}


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
    return {"mode": mode, "note": note, "market_open": market_open_now(), "live_blocker": live_blocker(u),
            "brokers": u["brokers"] if mode == "live" else [], "default_broker": u["default_broker"], "broker_names": BROKER_NAMES,
            "caps": {"max_value": c["max_order_value"], "max_qty": c["max_qty"], "max_orders": c["max_orders"],
                     "max_fut_lots": c["max_fut_lots"], "max_fut_value": c["max_fut_value"]}}


@app.post("/api/trading/mode")
def api_trading_mode(request: Request, payload: dict = Body(...)):
    """Switch between paper and live. Going live needs the order PIN; going back to paper never does."""
    u = request.state.user
    want = str(payload.get("mode", ""))
    if want == "paper":
        MODE_PREF.pop(u["id"], None)
        return {"ok": True, "mode": "paper"}
    if want != "live":
        return JSONResponse({"error": "Choose paper or live."}, status_code=400)
    why = live_blocker(u)
    if why:
        return {"error": why}
    err = pin_error(u, payload.get("pin"), True)
    if err:
        return {"error": err}
    MODE_PREF[u["id"]] = "live"
    return {"ok": True, "mode": "live"}


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
        return {"ok": False, "broker": name, "message": f"Could not connect to {name}: {str(e)[:200]}" + (". Paste a fresh access token under Account, then check again." if _auth_problem(e) else "")}
    ACCT_BAD.pop((u["id"], b), None)
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


@app.post("/api/orders/funds")
def api_order_funds(request: Request, payload: dict = Body(...)):
    """Broker balance, and what one share or lot costs, so the ticket can show how much can be bought. Places nothing.
    The balance is read first and on its own, so it still shows when the sizing part cannot be worked out."""
    u = request.state.user
    if user_mode(u)[0] != "live":
        return {"live": False}
    bid = str(payload.get("broker") or "").strip() or u.get("default_broker") or (u.get("brokers") or [""])[0]
    if bid not in (u.get("brokers") or []):
        return {"live": True, "error": "Add your Dhan or Angel One details under Account first."}
    name = BROKER_NAMES.get(bid, bid)
    out = {"live": True, "broker": name}
    try:
        acct = get_account(u["id"], bid)
        out["balance"] = acct.funds()
    except Exception as e:
        return {**out, "error": f"Could not read your {name} balance: {str(e)[:140]}"}
    if out["balance"] is None:
        return {**out, "error": f"{name} did not report a balance."}
    order, err, _, _ = make_order({**payload, "qty": 1, "broker": bid}, u, dry=True)
    if err:
        return {**out, "sizing_error": err}
    value = (order["price"] or order["ref"]) * order["qty"]
    per, basis = value, "full order value"
    if order["product"] != "DELIVERY" or order["kind"] != "equity":
        try:
            m = acct.margin_for(order, order["qty"])
            if m:
                per, basis = m, f"{name} margin"
        except Exception:
            pass
    caps = u["caps"]
    big = order["kind"] == "future" or (order["kind"] == "option" and order["side"] == "SELL")
    cap_qty = caps["max_fut_lots"] if big else caps["max_qty"]
    by_value = cap_qty if (big or not value) else int(caps["max_order_value"] // value)
    out.update(per_qty=round(per, 2), basis=basis, unit="lot" if order["kind"] != "equity" else "share",
               max_qty=max(0, int(min(out["balance"] // per if per else 0, cap_qty, by_value))))
    return out


PF_OPT = {}   # (symbol, expiry) -> (time, chain rows): option prices for the paper portfolio, kept for a few seconds


DHAN_OC = {}   # (symbol, expiry) -> (time, {(strike, right): last price}): Dhan's option chain, one request per contract list


def dhan_oc_price(symbol, expiry, strike, right):
    """Last price of one option contract from Dhan's option chain call (asked once per symbol and expiry, kept for 10 seconds).
    It needs no instrument-list lookup of the contract. Returns None when Dhan has no price for it."""
    key = (symbol, expiry)
    hit = DHAN_OC.get(key)
    if not hit or time.time() - hit[0] > 10:
        inst = dhan_inst(symbol) if load_master("dhan") else None
        if not inst:
            raise DhanDataError(f"{symbol} is not in Dhan's instrument list")
        d = datetime.strptime(expiry, EXPIRY_FMT).date().isoformat()
        res = dhan_post("/optionchain", {"UnderlyingScrip": int(inst[0]), "UnderlyingSeg": inst[1], "Expiry": d})
        oc = ((res or {}).get("data") or {}).get("oc") or {}
        prices = {}
        for k, v in oc.items():
            try:
                kf = round(float(k), 4)
            except ValueError:
                continue
            for r_ in ("ce", "pe"):
                lp = ((v or {}).get(r_) or {}).get("last_price")
                if lp:
                    prices[(kf, r_.upper())] = float(lp)
        hit = DHAN_OC[key] = (time.time(), prices)
    return hit[1].get((round(float(strike), 4), right))


def paper_option_prices(u, orders):
    """({order label: last price}, [problems]) for the option contracts held in paper orders.
    The held contracts are priced directly from a broker (one small request). The person's own account is tried first; when it is refused
    (an expired token, say) the shared data feed account is used, and the option chain is the last resort."""
    out, errs, want, why = {}, [], {}, []
    for o in orders:
        want.setdefault(o["label"], o)
    if not want:
        return out, errs
    own = {b for b in ((u or {}).get("brokers") or [])}
    for bid, acct in chain_accounts(u):
        pending = {l: o for l, o in want.items() if l not in out}
        if not pending:
            break
        name = BROKER_NAMES.get(bid, bid)
        if not load_master(bid):
            why.append(f"{name}'s instrument list could not be loaded")
            continue
        opt, ref = M(bid)["opt"], {}
        for label, o in pending.items():
            try:
                ek = datetime.strptime(o["expiry"], EXPIRY_FMT).strftime("%d%b%Y").upper()
                inst = opt_find(opt, o["symbol"], ek, float(o["strike"]), o["right"])
            except (TypeError, ValueError, KeyError):
                inst = None
            if inst:
                ref[label] = (inst[3], str(inst[0]))
        for label in pending:
            if label not in ref:
                o_ = pending[label]
                have = sorted({e for (sy, e, k, r_) in opt if sy == o_["symbol"]}, key=lambda e: datetime.strptime(e, "%d%b%Y"))[:4]
                ek_ = datetime.strptime(o_["expiry"], EXPIRY_FMT).strftime("%d%b%Y").upper()
                ks = sorted({k for (sy, e, k, r_) in opt if sy == o_["symbol"] and e == ek_ and r_ == o_["right"]}, key=lambda k: abs(k - float(o_["strike"])))[:3]
                why.append(f"{label} is not in {name}'s instrument list ("
                           + (f"it has {o_['symbol']} contracts expiring " + ", ".join(have) + (f"; strikes nearest {o_['strike']:g} on that day: " + (", ".join(f"{k:g}" for k in ks) if ks else "none") if have else "") if have else f"it has no {o_['symbol']} option contracts at all; {sum(1 for _ in opt)} option contracts loaded") + ")")
        if not ref:
            continue
        items = sorted(set(ref.values()))
        hit = PF_OPT.get(("direct", bid, id(acct), tuple(items)))
        if hit and time.time() - hit[0] < 4:
            prices = hit[1]
        else:
            try:
                prices = acct.ltp(items)
                PF_OPT[("direct", bid, id(acct), tuple(items))] = (time.time(), prices)
            except Exception as e:
                if _auth_problem(e):
                    if bid in own and acct is not (FEED.get("acct")):
                        ACCT_BAD[(u["id"], bid)] = time.time()
                    why.append(f"{name} refused the access token: update it" + (" under Account" if bid in own and acct is not FEED.get("acct") else " in the server settings"))
                else:
                    why.append(f"{name} did not return option prices ({str(e)[:90]})")
                continue
        for label, k in ref.items():
            if prices.get(k):
                out[label] = prices[k]
            else:
                why.append(f"{name} sent no price for {label} (id {k[1]}, {k[0]})")
    for label, o in want.items():   # anything not priced above: Dhan's option chain call
        if label in out:
            continue
        try:
            px = dhan_oc_price(o["symbol"], o["expiry"], o["strike"], o["right"])
            if px:
                out[label] = px
            else:
                why.append(f"Dhan's option chain has no price for {label}")
        except Exception as e:
            why.append(f"Dhan's option chain call failed for {o.get('symbol')} ({str(e)[:90]})")
    for label, o in want.items():   # anything still not priced: NSE's option chain
        if label in out:
            continue
        k = (o["symbol"], o.get("expiry"))
        hit = PF_OPT.get(k)
        if not hit or time.time() - hit[0] > 10:
            try:
                rows = (chain_for(o["symbol"], o.get("expiry"), u) or {}).get("rows") or []
            except Exception:
                rows = []
            hit = PF_OPT[k] = (time.time(), rows)
        leg = next((r.get(str(o.get("right", "")).lower()) for r in hit[1] if r["strike"] == o.get("strike")), None)
        if (leg or {}).get("ltp"):
            out[label] = leg["ltp"]
    miss = [l for l in want if l not in out]
    if miss:
        errs.append("No live price for " + ", ".join(miss[:3]) + (f" and {len(miss) - 3} more" if len(miss) > 3 else "") + ". "
                    + ("Why: " + "; ".join(list(dict.fromkeys(why))[:3]) + ". " if why else "No broker account could be used to price them. ") + "Profit and loss show once a price is available.")
    return out, errs


@app.get("/api/portfolio")
def api_portfolio(request: Request):
    """Positions with live profit and loss, and the broker balance. Live mode reads the broker; paper mode is worked out from paper orders."""
    u = request.state.user
    mode = user_mode(u)[0]
    pos, funds, errors = [], [], []
    if mode == "live":
        for b in u["brokers"]:
            try:
                acct = get_account(u["id"], b)
                for r in acct.positions():
                    pos.append({**r, "broker": BROKER_NAMES.get(b, b)})
                try:
                    funds.append({"broker": BROKER_NAMES.get(b, b), "balance": acct.funds()})
                except Exception:
                    pass
            except Exception as e:
                errors.append(f"{BROKER_NAMES.get(b, b)}: {str(e)[:140]}")
    else:
        with ORDER_LOCK:
            recs = [o for o in ORDERS if o.get("user") == u["id"] and o["mode"] == "paper" and o["status"] == "complete" and o.get("date") == ist_now().strftime("%Y-%m-%d") and (not o.get("is_exit") or o.get("bot_exit"))]
        groups = {}
        held = {}
        for o in recs:
            if o["kind"] == "option":
                held[o["label"]] = held.get(o["label"], 0) + (o["qty"] if o["side"] == "BUY" else -o["qty"])
        oprices, oerrs = paper_option_prices(u, [o for o in recs if o["kind"] == "option" and held.get(o["label"])])
        errors.extend(oerrs)
        for o in recs:
            g = groups.setdefault(o["label"], {"o": o, "bq": 0, "bv": 0.0, "sq": 0, "sv": 0.0})
            px = o.get("avg_price") or o["price"]
            if o["side"] == "BUY":
                g["bq"] += o["qty"]; g["bv"] += o["qty"] * px
            else:
                g["sq"] += o["qty"]; g["sv"] += o["qty"] * px
        for label, g in groups.items():
            o = g["o"]
            matched = min(g["bq"], g["sq"])
            ba = g["bv"] / g["bq"] if g["bq"] else 0
            sa = g["sv"] / g["sq"] if g["sq"] else 0
            real = matched * (sa - ba)
            net = g["bq"] - g["sq"]
            avg = ba if net > 0 else sa if net < 0 else None
            ltp = None
            if o["kind"] in ("equity", "future"):
                row, _ = row_for(o["symbol"], DEFAULT_INTERVAL)
                ltp = live_price(row) if row else None
            elif o["kind"] == "option" and net:
                ltp = oprices.get(label)
            unreal = net * (ltp - avg) if net and ltp is not None else 0.0
            pos.append({"label": label, "qty": net, "avg": avg, "ltp": ltp, "realized": round(real, 2), "unrealized": round(unreal, 2),
                        "product": o["product"], "broker": "Paper"})
    for r in pos:
        r["pnl"] = round((r["realized"] or 0) + (r["unrealized"] or 0), 2)
    pos.sort(key=lambda r: (r["qty"] == 0, -abs(r["pnl"])))
    tot = {"realized": round(sum(r["realized"] or 0 for r in pos), 2), "unrealized": round(sum(r["unrealized"] or 0 for r in pos), 2)}
    tot["pnl"] = round(tot["realized"] + tot["unrealized"], 2)
    return {"mode": mode, "positions": pos, "totals": tot, "funds": funds, "errors": errors}


PAST_BOT = {}   # (user id, bot) -> (time, trades from earlier days). Read from the order log at most once a minute.
PAST_BOT_DAYS = int(os.getenv("PAST_BOT_DAYS", "30"))


def bot_past_trades(uid, kind):
    """The autopilot's trades from earlier days, newest first, from the order log (the latest record of each order wins)."""
    k = (uid, kind)
    hit = PAST_BOT.get(k)
    if hit and time.time() - hit[0] < 60:
        return hit[1]
    today = ist_now().strftime("%Y-%m-%d")
    since = (ist_now() - timedelta(days=PAST_BOT_DAYS)).strftime("%Y-%m-%d")
    latest = {}
    try:
        lines = [r["rec"] for r in q("SELECT rec FROM order_log WHERE day>=? AND day<? ORDER BY n", (since, today))] if USE_PG else (open(ORDER_LOG) if os.path.exists(ORDER_LOG) else [])
        for ln in lines:
            try:
                o = json.loads(ln)
            except ValueError:
                continue
            if o.get("bot") and o.get("user") == uid and bot_kind(o.get("bot_strategy")) == kind and since <= (o.get("date") or "") < today:
                latest[o["id"]] = o
    except Exception as e:
        print("past autopilot trades:", e, flush=True)
    out = []
    for o in sorted(latest.values(), key=lambda o: (o.get("date"), o.get("time")), reverse=True):
        st = o.get("bot_state")
        out.append({"date": o["date"], "time": o["time"], "label": o["label"], "mode": o["mode"], "entry": o.get("avg_price") or o["price"], "state": "closed" if st in ("open", "unconfirmed", "closing") else st,
                    "exit": o.get("bot_exit_px"), "pnl": bot_pnl(o), "reason": o.get("bot_reason") or ("Closed at the end of that day" if st in ("open", "unconfirmed", "closing") else None),
                    "dir": o.get("bot_dir"), "etype": o.get("bot_etype") or "signal", "past": True})
    PAST_BOT[k] = (time.time(), out)
    return out


def bot_view(u, kind="scanner"):
    kind = bot_kind(kind)
    cfg = bot_cfg(u["id"], kind)
    today = ist_now().strftime("%Y-%m-%d")
    trades = []
    for o in sorted(bot_positions(u["id"], open_only=False, kind=kind), key=lambda o: o["time"], reverse=True):
        trades.append({"date": today, "time": o["time"], "label": o["label"], "mode": o["mode"], "entry": o.get("avg_price") or o["price"], "state": o.get("bot_state"),
                       "exit": o.get("bot_exit_px"), "pnl": bot_pnl(o), "reason": o.get("bot_reason"), "p_sl": o.get("bot_p_sl"), "p_t1": o.get("bot_p_t1"), "p_t2": o.get("bot_p_t2"), "dir": o.get("bot_dir"), "etype": o.get("bot_etype") or "signal"})
    pnl = round(sum(t["pnl"] or 0 for t in trades), 2)
    trades = trades + bot_past_trades(u["id"], kind)
    why = live_blocker(u) or ("Switch your account to Live with the toggle at the top first." if user_mode(u)[0] != "live" else None)
    state = "stopped" if cfg["mode"] == "off" else "paused" if cfg.get("paused") else "running"
    return {"mode": cfg["mode"], "state": state, "choice": cfg["last_mode"], "strategy": kind, "bot": kind, "entries": cfg.get("entries", "both"), "min_grade": cfg.get("min_grade", "any"), "open": len(bot_positions(u["id"], kind=kind)), "max_trades": cfg["max_trades"], "lots": cfg["lots"], "loss_limit": cfg["loss_limit"], "live_blocker": why,
            "armed": cfg["armed_day"] == today, "trades": trades, "pnl": pnl, "last_scan": cfg.get("last_scan"), "market_open": market_open_now(),
            "rules": AUTO_RULES + (" Trades NIFTY only." if kind == "nifty" else f" Looks at every F&O stock (not only the intraday scanner's list) whose move today is between {AUTO_MOVE_MIN:g}% and {AUTO_MOVE_MAX:g}%, up or down.") + f" Entries from 9:20 to {OPTBOT_LAST_ENTRY.strftime('%H:%M')}, a continuation needs the price near its trend line, everything is sold by {OPTBOT_SQUAREOFF.strftime('%H:%M')}.",
            "signals": manual_signals(u, kind),
            "log": [l for l in OPTBOT_LOG if l["uid"] == u["id"] and (l.get("bot") in (None, kind))][-25:][::-1]}


# ── Autopilot replay: what the rules would have done on a past day ──
# It runs the same entry rules over that day's 5 and 15 minute candles, one 5 minute step at a time, and follows each trade on the stock or index itself
# (stop-loss, T1 to T4 with the stop moving up behind them, square-off at 15:15). Results are in R, the stop-loss distance, not in option rupees.
REPLAY_DATA = {}   # bot -> (time, {symbol: {"5m": series, "15m": series}}, errors, loaded days)
REPLAY_JOB = {}    # (user id, bot) -> {"status", "msg", "done", "total", "result"}
REPLAY_LOCK = threading.Lock()


def _hm(ts):
    return pd.Timestamp(int(ts), unit="s", tz="UTC").tz_convert(IST).strftime("%H:%M")


def replay_series(df, tf):
    """Everything the replay needs from one symbol's candles: the indicator's signals with where each one's stop and targets were first reached."""
    df = df.dropna(subset=["Close", "High", "Low", "Open"]).copy()
    if df.empty:
        return None
    df.index = df.index.tz_localize("UTC").tz_convert(IST) if df.index.tz is None else df.index.tz_convert(IST)
    if CONFIRMED_ONLY:
        df = drop_forming(df, tf)
    n = len(df)
    if n < MIN_BARS:
        return None
    c, hi, lo = df["Close"].to_numpy(float), df["High"].to_numpy(float), df["Low"].to_numpy(float)
    ls, ss, d, atr = chandelier(df)
    ema = df["Close"].ewm(span=EMA_LEN, adjust=False).mean().to_numpy()
    green, red = qqe_states(df["Close"])
    up = np.zeros(n, bool)
    dn = np.zeros(n, bool)
    up[1:] = (d[1:] == 1) & (d[:-1] == -1)
    dn[1:] = (d[1:] == -1) & (d[:-1] == 1)
    buy = up & (c > ema) & within_lookback(green)
    sell = dn & (c < ema) & within_lookback(red)
    mins = 5 if tf == "5m" else 15
    ts = np.array([int(t.timestamp()) for t in df.index])
    sg = []
    for j in np.nonzero(buy | sell)[0]:
        j = int(j)
        side = 1 if buy[j] else -1
        entry = float(c[j])
        raw = ls[j] if side == 1 else ss[j]
        risk = max(MIN_RISK_ATR * atr[j], min(abs(entry - raw), MAX_RISK_ATR * atr[j]))
        sl = entry - side * risk
        tps = [entry + side * risk * r for r in R_MULTIPLES]
        sl_i, tidx, hits = None, [None] * 4, 0
        for k in range(j + 1, n):   # same order as the live indicator: the stop first, then the targets
            if (lo[k] <= sl) if side == 1 else (hi[k] >= sl):
                sl_i = k
                break
            reached = sum(1 for t in tps if ((hi[k] >= t) if side == 1 else (lo[k] <= t)))
            for r in range(hits, reached):
                tidx[r] = k
            hits = max(hits, reached)
            if hits == 4:
                break
        sg.append({"j": j, "side": "BUY" if side == 1 else "SELL", "dir": side, "entry": entry, "sl": float(sl), "tps": [float(t) for t in tps], "risk": float(risk), "sl_i": sl_i, "tidx": tidx,
                   "date": df.index[j].strftime("%Y-%m-%d"), "time": df.index[j].strftime("%H:%M")})
    day0 = {}
    for k, dstr in enumerate(t.strftime("%Y-%m-%d") for t in df.index):
        day0.setdefault(dstr, k)
    return {"day0": day0, "n": n, "ts": ts, "close_ts": ts + mins * 60, "c": c, "hi": hi, "lo": lo, "ema": ema, "atr": atr, "sg": sg, "sig_idx": [g["j"] for g in sg],
            "dates": [t.strftime("%Y-%m-%d") for t in df.index], "tm": [t.strftime("%H:%M") for t in df.index]}


def replay_state(S, i):
    """The last signal on this chart as of candle i, and how far it had got by then."""
    from bisect import bisect_right
    k = bisect_right(S["sig_idx"], i) - 1
    if k < 0:
        return None
    g = S["sg"][k]
    sl_hit = g["sl_i"] is not None and g["sl_i"] <= i
    hits = sum(1 for x in g["tidx"] if x is not None and x <= i)
    return dict(g, sl_hit=sl_hit, hits=hits, open=not sl_hit and hits < 4, bars_ago=i - g["j"])


def replay_fetch(names, tf, progress):
    out, errors = {}, []
    for i in range(0, len(names), YF_BATCH):
        part = names[i:i + YF_BATCH]
        try:
            if use_dhan():
                frames, errs = dhan_frames(part, tf, _days(PERIODS[tf]))
                errors.extend(errs)
                out.update({k: v for k, v in frames.items() if v is not None})
            else:
                import yfinance as yf
                data = yf.download([yf_ticker(x) for x in part], period=PERIODS[tf], interval=tf, group_by="ticker", auto_adjust=False, threads=True, progress=False)
                for x in part:
                    try:
                        df = data[yf_ticker(x)] if len(part) > 1 else data
                        if df is not None and not df.empty:
                            out[x] = df
                    except Exception:
                        errors.append(f"{x}: no data")
        except Exception as e:
            errors.append(f"batch {i // YF_BATCH + 1}: {e}")
        progress(min(i + YF_BATCH, len(names)))
    return out, errors


def replay_names(kind):
    return ["NIFTY"] if kind == "nifty" else [s for s in UNIVERSE if s not in INDICES and s not in SCANNER_EXCLUDE and s not in INDEX_YF]


def replay_load(kind, set_progress):
    hit = REPLAY_DATA.get(kind)
    if hit and time.time() - hit[0] < 600:
        return hit
    names = replay_names(kind)
    total = len(names) * 2
    base = [0]
    data, errors = {}, []
    for tf in ("5m", "15m"):
        frames, errs = replay_fetch(names, tf, lambda n, b=base: set_progress(b[0] + n, total))
        base[0] += len(names)
        errors.extend(errs)
        for sname, df in frames.items():
            try:
                S = replay_series(df, tf)
            except Exception as e:
                errors.append(f"{sname} {tf}: {e}")
                continue
            if S:
                data.setdefault(sname, {})[tf] = S
    days = sorted({d for v in data.values() if "5m" in v for d in v["5m"]["dates"]})[-30:]
    REPLAY_DATA[kind] = hit = (time.time(), data, errors, days)
    return hit


def replay_run(data, kind, day, cfg):
    """One day, one 5 minute step at a time. Returns the signals that qualified, the trades the autopilot would have taken, and why the others were left out."""
    ent = cfg.get("entries", "both")
    max_trades = int(cfg.get("max_trades", 3))
    syms = [s for s, v in data.items() if "5m" in v and "15m" in v]
    steps = {}
    for s in syms:
        S5 = data[s]["5m"]
        idx = [k for k in range(S5["n"]) if S5["dates"][k] == day]
        if idx:
            steps[s] = idx
    if not steps:
        return {"error": f"No candles for {day} in the data loaded."}
    ref = max(steps, key=lambda s: len(steps[s]))
    grid = [(data[ref]["5m"]["close_ts"][k], data[ref]["5m"]["tm"][k]) for k in steps[ref]]
    first = OPTBOT_FIRST_ENTRY.strftime("%H:%M")
    last = OPTBOT_LAST_ENTRY.strftime("%H:%M")
    seen, signals, skips, trades, taken_keys = set(), [], {}, [], set()
    pos = {}
    from bisect import bisect_right

    def skip(why, key):
        if (why, key) not in skips_seen:
            skips_seen.add((why, key))
            skips[why] = skips.get(why, 0) + 1
    skips_seen = set()
    for T, hhmm in grid:
        endt = _hm(T)
        if endt < first or endt > last:
            continue
        cands = []
        for s in syms:
            S5, S15 = data[s]["5m"], data[s]["15m"]
            k5 = bisect_right(S5["close_ts"], T) - 1
            k15 = bisect_right(S15["close_ts"], T) - 1
            if k5 < 0 or k15 < 0 or S5["dates"][k5] != day:
                continue
            st = {"5m": replay_state(S5, k5), "15m": replay_state(S15, k15)}
            ser = {"5m": (S5, k5), "15m": (S15, k15)}
            ltp = float(S5["c"][k5])
            d0 = S5["day0"].get(day, 0)
            prev = float(S5["c"][d0 - 1]) if d0 > 0 else None
            chg = (ltp / prev - 1) * 100 if prev else None
            for tf in ("5m", "15m"):
                if tf not in AUTO_ENTRY_TFS:
                    continue
                g = st[tf]
                if not g or not g["open"]:
                    continue
                other = st["15m" if tf == "5m" else "5m"]
                S, k = ser[tf]
                fresh = g["bars_ago"] <= OPTBOT_FRESH_BARS and g["date"] == day
                cont = not fresh and g["date"] < day
                if not fresh and not cont:
                    continue
                key = (s, tf, g["date"], g["time"], g["side"])
                if (fresh and ent == "gap") or (cont and ent == "fresh"):
                    continue
                if cont and not (AUTO_CONT_START <= endt < AUTO_CONT_END):
                    skip(f"continuation outside {AUTO_CONT_START}-{AUTO_CONT_END}", key)
                    continue
                if kind == "scanner" and (chg is None or not (AUTO_MOVE_MIN <= abs(chg) <= AUTO_MOVE_MAX)):
                    skip(f"stock moved outside {AUTO_MOVE_MIN:g}-{AUTO_MOVE_MAX:g}% today", key)
                    continue
                if not other or not other["open"] or other["side"] != g["side"]:
                    skip("5 and 15 minute charts not in the same direction", key)
                    continue
                if g["hits"]:
                    skip("already reached T1", key)
                    continue
                if (g["dir"] == 1 and not (g["sl"] < ltp < g["tps"][0])) or (g["dir"] == -1 and not (g["tps"][0] < ltp < g["sl"])):
                    skip("price already past T1 or the stop", key)
                    continue
                dist = None
                if cont:
                    em, at = float(S["ema"][k]), float(S["atr"][k])
                    dist = abs(ltp - em) / at if at == at and at else None
                    if dist is None or (g["dir"] == 1 and ltp < em) or (g["dir"] == -1 and ltp > em):
                        skip("continuation: price on the wrong side of its trend line", key)
                        continue
                    if dist > OPTBOT_NEAR_ATR:
                        skip("continuation: not back near its trend line", key)
                        continue
                cands.append({"symbol": s, "tf": tf, "g": g, "px": ltp, "key": key, "etype": "fresh" if fresh else "continuation", "dist": dist if dist is not None else 9.9, "at": endt, "chg": chg, "S5": S5, "k5": k5, "sts": int(S["ts"][g["j"]])})
        cands.sort(key=lambda c: (c["etype"] != "fresh", c["dist"]))
        for c in cands:
            g = c["g"]
            if c["key"] not in seen:
                seen.add(c["key"])
                signals.append({"symbol": c["symbol"], "side": g["side"], "right": "CE" if g["side"] == "BUY" else "PE", "tf": c["tf"], "type": c["etype"], "at": c["at"], "signal_time": f"{g['date']} {g['time']}",
                                "price": round(c["px"], 2), "sl": round(g["sl"], 2), "tps": [round(t, 2) for t in g["tps"]], "move": None if c["chg"] is None else round(c["chg"], 2), "taken": False, "why_not": None, "ts": c["sts"], "day": day})
            sig = next(x for x in signals if (x["symbol"], x["tf"], x["signal_time"], x["side"]) == (c["symbol"], c["tf"], f"{g['date']} {g['time']}", g["side"]))
            if c["key"] in taken_keys or sig["taken"]:
                continue
            if c["symbol"] in pos and pos[c["symbol"]]["open"]:
                sig["why_not"] = "already in a trade on this stock"
                continue
            if len(trades) >= max_trades:
                sig["why_not"] = f"daily limit of {max_trades} trades reached"
                continue
            tr = replay_trade(c, day)
            tr["open"] = True
            pos[c["symbol"]] = tr
            sig["taken"] = True
            taken_keys.add(c["key"])
            trades.append(tr)
        for s, tr in pos.items():   # a trade is "open" until its exit time has passed
            if tr["open"] and tr["exit_hhmm"] <= endt:
                tr["open"] = False
    for sg_ in signals:
        if not sg_["taken"] and not sg_["why_not"]:
            sg_["why_not"] = "a better-ranked trade was taken first or the limit was reached"
    for tr in trades:
        tr.pop("open", None)
    tot = round(sum(t["r"] for t in trades), 2)
    win = [t for t in trades if t["r"] > 0]
    summ = {"signals": len(signals), "trades": len(trades), "wins": len(win), "losses": sum(1 for t in trades if t["r"] < 0), "flat": sum(1 for t in trades if t["r"] == 0),
            "total_r": tot, "avg_r": round(tot / len(trades), 2) if trades else 0.0, "win_rate": round(len(win) / len(trades) * 100) if trades else 0,
            "best": max((t["r"] for t in trades), default=0), "worst": min((t["r"] for t in trades), default=0)}
    eq, run = [], 0.0
    for t in trades:
        run += t["r"]
        eq.append(round(run, 2))
    return {"day": day, "bot": kind, "equity": eq, "summary": summ, "trades": trades, "signals": signals, "skips": sorted(skips.items(), key=lambda x: -x[1]), "max_trades": max_trades, "entries": ent}


def replay_run_range(data, kind, days, cfg):
    """Several days in a row, each with its own daily trade limit. Adds the equity curve, daily results and drawdown."""
    trades, signals, skips, per_day = [], [], {}, []
    for day in days:
        r = replay_run(data, kind, day, cfg)
        if r.get("error"):
            continue
        trades += r["trades"]
        signals += r["signals"]
        for k, n in r["skips"]:
            skips[k] = skips.get(k, 0) + n
        per_day.append({"day": day, "trades": r["summary"]["trades"], "wins": r["summary"]["wins"], "r": r["summary"]["total_r"], "signals": r["summary"]["signals"]})
    if not per_day:
        return {"error": "No candles for those days in the data loaded."}
    rs = [t["r"] for t in trades]
    eq, run, peak, dd = [], 0.0, 0.0, 0.0
    for t in trades:
        run += t["r"]
        peak = max(peak, run)
        dd = max(dd, peak - run)
        eq.append(round(run, 2))
    gain, loss = sum(x for x in rs if x > 0), -sum(x for x in rs if x < 0)
    win = [x for x in rs if x > 0]
    summ = {"signals": len(signals), "trades": len(trades), "wins": len(win), "losses": sum(1 for x in rs if x < 0), "flat": sum(1 for x in rs if x == 0), "total_r": round(sum(rs), 2),
            "avg_r": round(sum(rs) / len(rs), 2) if rs else 0.0, "win_rate": round(len(win) / len(rs) * 100) if rs else 0, "best": max(rs, default=0), "worst": min(rs, default=0),
            "profit_factor": round(gain / loss, 2) if loss else None, "max_drawdown": round(dd, 2), "days": len(per_day), "green_days": sum(1 for d in per_day if d["r"] > 0), "red_days": sum(1 for d in per_day if d["r"] < 0),
            "best_day": max(per_day, key=lambda d: d["r"]), "worst_day": min(per_day, key=lambda d: d["r"])}
    return {"mode": "range", "day": f"{per_day[0]['day']} to {per_day[-1]['day']}", "bot": kind, "summary": summ, "trades": trades, "signals": signals[:4000], "signals_total": len(signals),
            "skips": sorted(skips.items(), key=lambda x: -x[1]), "per_day": per_day, "equity": eq, "max_trades": int(cfg.get("max_trades", 3)), "entries": cfg.get("entries", "both")}


def _bs_call_put(spot, strike, t, vol, right, rate=0.07):
    """Black-Scholes price of a European option."""
    if t <= 0 or vol <= 0:
        return max(0.0, (spot - strike) if right == "CE" else (strike - spot))
    d1 = (math.log(spot / strike) + (rate + vol * vol / 2) * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)
    cdf = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    if right == "CE":
        return spot * cdf(d1) - strike * math.exp(-rate * t) * cdf(d2)
    return strike * math.exp(-rate * t) * cdf(-d2) - spot * cdf(-d1)


def _expiry_after(symbol, d):
    """The next NSE expiry on or after date d: every Tuesday for NIFTY, the last Tuesday of the month for stocks."""
    if symbol == "NIFTY":
        return d + timedelta(days=(1 - d.weekday()) % 7)
    for add in (0, 1):
        y, m = d.year + (d.month + add - 1) // 12, (d.month + add - 1) % 12 + 1
        last = (datetime(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)).date()
        last -= timedelta(days=(last.weekday() - 1) % 7)
        if last >= d:
            return last
    return d


def replay_option(symbol, S, k0, k1, day, entry, exit_px, dr):
    """An estimate of what the at-the-money option bought at the signal would have been worth at the exit.
    Past option prices are not available here, so this prices the option with Black-Scholes: strike = the entry price,
    volatility = the stock's own recent 5 minute volatility, the nearest expiry, and time passing between entry and exit."""
    try:
        c = np.asarray(S["c"][max(0, k0 - 375):k0 + 1], dtype=float)
        rets = np.diff(np.log(c[c > 0]))
        vol = float(np.std(rets) * math.sqrt(75 * 252)) if len(rets) > 20 else 0.3
        vol = min(0.9, max(0.15, vol))
        d = datetime.strptime(str(day)[:10], "%Y-%m-%d").date()
        days = (_expiry_after(symbol, d) - d).days
        t_in = (S["close_ts"][k0]); t_out = (S["close_ts"][k1])
        mins = max(0.0, (float(t_out) - float(t_in)) / 60.0)
        hr = pd.Timestamp(t_in, unit="s", tz="UTC").tz_convert(IST)
        left = max(0.0, (15.5 - (hr.hour + hr.minute / 60)) / 24)
        t0 = max(0.5, days + left) / 365.0
        t1 = max(0.25 / 365.0, t0 - mins / 1440.0 / 365.0)
        right = "CE" if dr == 1 else "PE"
        p0 = _bs_call_put(entry, entry, t0, vol, right)
        p1 = _bs_call_put(exit_px, entry, t1, vol, right)
        if p0 <= 0:
            return {}
        return {"opt_in": round(p0, 2), "opt_out": round(p1, 2), "opt_pct": round((p1 / p0 - 1) * 100, 1), "opt_iv": round(vol * 100)}
    except Exception:
        return {}


def replay_trade(c, day):
    """Follow one entry on the stock or index itself: stop-loss first, then T1 to T4 with the stop moving up behind them, out at 15:15."""
    g, S = c["g"], c["S5"]
    dr, entry, risk = g["dir"], c["px"], g["risk"]
    lv = g["tps"]
    # the signal's own stop and targets, measured from the signal candle as the indicator does
    stop, step = g["sl"], 0
    start = c["k5"]
    exit_px, exit_at, why = None, None, None
    kx = start
    sq = OPTBOT_SQUAREOFF.strftime("%H:%M")
    for k in range(start + 1, S["n"]):
        if S["dates"][k] != day:
            break
        ck = _hm(S["close_ts"][k])
        kx = k
        hi, lo = S["hi"][k], S["lo"][k]
        if (dr == 1 and lo <= stop) or (dr == -1 and hi >= stop):
            exit_px, exit_at, why = stop, ck, "stop-loss" if step == 0 else ("back to the entry after T1" if step == 1 else f"back to the stop after T{step}")
            break
        while step < 4 and ((dr == 1 and hi >= lv[step]) or (dr == -1 and lo <= lv[step])):
            step += 1
            stop = entry if step == 1 else lv[step - 2]
        if step >= 4:
            exit_px, exit_at, why = lv[3], ck, "T4 reached"
            break
        if ck >= sq:
            exit_px, exit_at, why = float(S["c"][k]), ck, "end of day"
            break
    if exit_px is None:
        k = max(k for k in range(S["n"]) if S["dates"][k] == day)
        exit_px, exit_at, why = float(S["c"][k]), _hm(S["close_ts"][k]), "end of data"
        kx = k
    op = replay_option(c["symbol"], S, start, kx, day, entry, exit_px, dr)
    r = round((exit_px - entry) * dr / risk, 2) if risk else 0.0
    return {"symbol": c["symbol"], "side": g["side"], "right": "CE" if g["side"] == "BUY" else "PE", "tf": c["tf"], "type": c["etype"], "entered": c["at"], "entry": round(entry, 2),
            "sl": round(g["sl"], 2), "t1": round(lv[0], 2), "t4": round(lv[3], 2), "exit": round(exit_px, 2), "exit_at": exit_at, "exit_hhmm": exit_at, "reason": why, "targets": step,
            "r": r, "pct": round((exit_px / entry - 1) * 100 * dr, 2), "ts": c["sts"], "day": day, **op}


def replay_job(uid, kind, day):
    j = REPLAY_JOB[(uid, kind)]
    try:
        def prog(done, total):
            j["done"], j["total"] = done, total
        j["status"], j["msg"] = "running", "Loading the 5 and 15 minute candles…"
        _, data, errors, days = replay_load(kind, prog)
        if not data:
            j.update(status="error", msg="No candles could be loaded. " + ("; ".join(errors[:3]) if errors else "Check the data feed."))
            return
        today = ist_now().strftime("%Y-%m-%d")
        if day == "month":
            span = [d for d in days[1:] if d < today or ist_now().strftime("%H:%M") > "15:35"]   # the first day is warm-up for the indicator
            j["msg"] = f"Replaying {len(span)} trading days…"
            res = replay_run_range(data, kind, span, bot_cfg(uid, kind))
        else:
            day = day if day in days else (days[-2] if len(days) > 1 and days[-1] == today else days[-1])
            j["msg"] = f"Replaying {day}…"
            res = replay_run(data, kind, day, bot_cfg(uid, kind))
        res["days"] = days
        res["loaded"] = len(data)
        res["errors"] = errors[:5]
        j.update(status="error" if res.get("error") else "done", msg=res.get("error", ""), result=res)
    except Exception as e:
        j.update(status="error", msg=f"The replay failed: {type(e).__name__}: {str(e)[:120]}")


@app.post("/api/optbot/replay")
def api_replay_start(request: Request, payload: dict = Body(...)):
    u = request.state.user
    kind = bot_kind(payload.get("bot"))
    day = str(payload.get("day") or "")
    cur = REPLAY_JOB.get((u["id"], kind))
    if cur and cur["status"] == "running":
        return {"status": "running", "msg": cur["msg"], "done": cur.get("done", 0), "total": cur.get("total", 0)}
    if not REPLAY_LOCK.acquire(blocking=False):
        return {"error": "Another replay is loading data. Try again in a minute."}
    REPLAY_JOB[(u["id"], kind)] = job = {"status": "running", "msg": "Starting…", "done": 0, "total": 0, "result": None}

    def go():
        try:
            replay_job(u["id"], kind, day)
        finally:
            REPLAY_LOCK.release()
    threading.Thread(target=go, daemon=True).start()
    return {"status": "running", "msg": job["msg"], "done": 0, "total": 0}


@app.get("/api/optbot/replay")
def api_replay_get(request: Request, bot: str = "scanner"):
    u = request.state.user
    j = REPLAY_JOB.get((u["id"], bot_kind(bot)))
    if not j:
        return {"status": "idle"}
    return {"status": j["status"], "msg": j["msg"], "done": j.get("done", 0), "total": j.get("total", 0), "result": j.get("result") if j["status"] == "done" else None}


MANUAL_SIG = {}   # (user id, bot) -> (time, list). The signal list for manual trading is recomputed at most every 10 seconds.


MANUAL_DAY = {}   # (user id, bot) -> {"day": date, "rows": {key: row}}. Every signal seen today stays on the list, even after it stops qualifying.
MANUAL_ORDER = {"open": 0, "taken": 1, "passed": 2, "earlier": 3}


def manual_signals(u, kind):
    """Every signal from today that fits this autopilot's rules, for someone trading by hand, with what happened to it.
    Shown whether or not the autopilot is running. Signals stay on the list for the rest of the day."""
    k = (u["id"], kind)
    hit = MANUAL_SIG.get(k)
    if hit and time.time() - hit[0] < 10:
        return hit[1]
    cfg = bot_cfg(u["id"], kind)
    today = ist_now().strftime("%Y-%m-%d")
    try:
        cands = auto_candidates(u, cfg, kind, manual=True, day=True)
    except Exception as e:
        print("manual signals:", e)
        return hit[1] if hit else []
    store = MANUAL_DAY.get(k)
    if not store or store["day"] != today:
        store = MANUAL_DAY[k] = {"day": today, "rows": {}}
    rows, now = store["rows"], time.time()
    seen = set()
    for c in cands:
        key = f"{c['sigkey']} {c['symbol']}"
        seen.add(key)
        old = rows.get(key)
        rows[key] = {"symbol": c["symbol"], "side": c["side"], "right": "CE" if c["side"] == "BUY" else "PE", "tf": c["tf"], "type": c["etype"], "px": round(c["px"], 2),
                     "time": c.get("time"), "sl": c["levels"]["sl"], "t1": c["levels"]["tp1"], "t2": c["levels"]["tp2"], "t3": c["levels"]["tp3"], "t4": c["levels"]["tp4"],
                     "grade": c.get("grade"), "score": c.get("score"), "taken": c.get("taken", False), "status": c["status"], "reason": c.get("reason"), "note": c["note"],
                     "ts": old["ts"] if old else now}
        if old and old["status"] == "taken":
            rows[key]["status"], rows[key]["taken"] = "taken", True
    for key, row in rows.items():
        if key not in seen and row["status"] == "open":
            row["status"], row["reason"] = "earlier", "no longer meets the rules"
    with ORDER_LOCK:
        mine = [o for o in ORDERS if o.get("user") == u["id"] and o.get("bot") and o.get("bot_strategy", "scanner") == kind and o.get("date") == today]
    for o in mine:   # a trade the autopilot made always stays on the list, even if the signal behind it no longer qualifies
        key = f"{o.get('bot_sigkey')} {o['symbol']}"
        if key in rows:
            rows[key]["status"], rows[key]["taken"] = "taken", True
            continue
        dr = o.get("bot_dir") or "BUY"
        rows[key] = {"symbol": o["symbol"], "side": dr, "right": o.get("bot_right") or ("CE" if dr == "BUY" else "PE"), "tf": o.get("bot_tf", "5m"), "type": o.get("bot_etype", "fresh"),
                     "px": round(float(o.get("bot_entry_under") or 0), 2), "time": o.get("time"), "sl": o.get("bot_sl"), "t1": o.get("bot_t1"), "t2": o.get("bot_t2"),
                     "t3": o.get("bot_t3"), "t4": o.get("bot_t4"), "grade": None, "score": None, "taken": True, "status": "taken", "reason": None, "note": "", "ts": now}
    out = sorted(rows.values(), key=lambda r: (MANUAL_ORDER.get(r["status"], 9), -r["ts"]))
    MANUAL_SIG[k] = (time.time(), out)
    return out


@app.get("/api/optbot")
def api_optbot(request: Request, bot: str = "scanner"):
    return bot_view(request.state.user, bot)


def bot_apply(u, payload, mode, kind="scanner"):
    """Check the settings and, for Live, the PIN. Returns an error text or None. Applies nothing until everything passes."""
    cfg = bot_cfg(u["id"], kind)
    try:
        mt, lots, ll = int(payload.get("max_trades", cfg["max_trades"])), int(payload.get("lots", cfg["lots"])), float(payload.get("loss_limit", cfg["loss_limit"]))
    except (TypeError, ValueError):
        return "Trades, lots and the loss limit must be numbers."
    if not (1 <= mt <= 10) or not (1 <= lots <= u["caps"]["max_qty"]) or ll < 0:
        return f"Use 1 to 10 trades, 1 to {u['caps']['max_qty']} lots, and a loss limit of 0 or more."
    if mode == "live" and (cfg["mode"] != "live" or cfg["armed_day"] != ist_now().strftime("%Y-%m-%d")):
        why = live_blocker(u) or (None if user_mode(u)[0] == "live" else "Switch your account to Live with the toggle at the top first.")
        if why:
            return why
        err = pin_error(u, payload.get("pin"), True)
        if err:
            return err
        cfg["armed_day"] = ist_now().strftime("%Y-%m-%d")
    ent = payload.get("entries", cfg.get("entries", "both"))
    if ent not in ("both", "fresh", "gap"):
        return "Choose both entry types, fresh signals only, or continuation only."
    mg = payload.get("min_grade", cfg.get("min_grade", "any"))
    if mg not in GRADE_NEED:
        return "Choose any grade, B or better, or A only."
    cfg.update(max_trades=mt, lots=lots, loss_limit=ll, entries=ent, min_grade=mg)
    return None


@app.post("/api/optbot")
def api_optbot_set(request: Request, payload: dict = Body(...)):
    """Settings and mode in one call (Off, Paper or Live)."""
    u = request.state.user
    kind = bot_kind(payload.get("bot") or payload.get("strategy"))
    cfg = bot_cfg(u["id"], kind)
    mode = str(payload.get("mode", cfg["mode"]))
    if mode not in ("off", "paper", "live"):
        return {"error": "Choose Off, Paper or Live."}
    err = bot_apply(u, payload, mode, kind)
    if err:
        return {"error": err}
    cfg.update(mode=mode, paused=False)
    if mode != "off":
        cfg["last_mode"] = mode
    bot_log(u["id"], f"{kind.title()} autopilot set to {mode.upper()}: up to {cfg['max_trades']} trades, {cfg['lots']} lot(s), daily loss limit {cfg['loss_limit']:.0f}.", bot=kind)
    return {"ok": True, **bot_view(u, kind)}


@app.post("/api/optbot/control")
def api_optbot_control(request: Request, payload: dict = Body(...)):
    """Play, pause and stop. Play starts or resumes. Pause stops new trades but keeps managing open ones. Stop sells everything and switches off."""
    u = request.state.user
    kind = bot_kind(payload.get("bot") or payload.get("strategy"))
    cfg = bot_cfg(u["id"], kind)
    action = str(payload.get("action", ""))
    if action == "play":
        mode = str(payload.get("mode") or "")
        if cfg["mode"] != "off" and not mode:
            mode = cfg["mode"]
        if mode not in ("paper", "live"):
            return {"error": "Choose Paper or Live, then press Play."}
        if cfg["mode"] != "off" and mode != cfg["mode"]:
            return {"error": f"The autopilot is running in {cfg['mode'].title()} mode. Press Stop before changing mode."}
        err = bot_apply(u, payload, mode, kind)
        if err:
            return {"error": err}
        cfg.update(mode=mode, paused=False, last_mode=mode)
        bot_log(u["id"], f"Play: {'NIFTY' if kind == 'nifty' else 'F&O stocks'} autopilot running in {mode.upper()} mode.", bot=kind)
    elif action == "pause":
        if cfg["mode"] == "off":
            return {"error": "The autopilot is not running."}
        cfg["paused"] = True
        bot_log(u["id"], "Paused: no new trades. Open positions are still managed.", bot=kind)
    elif action == "stop":
        for pos in bot_positions(u["id"], kind=kind):
            bot_exit(u, pos, "stopped by you")
        left = len(bot_positions(u["id"], kind=kind))
        cfg.update(mode="off", paused=False)
        bot_log(u["id"], f"{kind.title()} stopped: " + ("all positions closed." if not left else f"{left} position(s) could not be closed. Check them in your broker app."), bot=kind)
        return {"ok": True, "left": left, **bot_view(u, kind)}
    elif action == "save":
        err = bot_apply(u, payload, cfg["mode"], kind)
        if err:
            return {"error": err}
    else:
        return {"error": "Unknown action."}
    return {"ok": True, **bot_view(u, kind)}


@app.post("/api/optbot/exit_all")
def api_optbot_exit(request: Request, payload: dict = Body(default={})):
    u = request.state.user
    n = 0
    k = payload.get("bot") if isinstance(payload, dict) else None
    for pos in bot_positions(u["id"], kind=bot_kind(k) if k else None):
        bot_exit(u, pos, "closed by you")
        n += 1
    return {"ok": True, "closed": n}


BT_LOCK = threading.Lock()


@app.post("/api/backtest")
def api_backtest(request: Request, payload: dict = Body(...)):
    u = request.state.user
    p, err = bt_params(payload)
    if err:
        return {"error": err}
    with BT_LOCK:
        if any(j["user"] == u["id"] and j["state"] == "running" for j in BT_JOBS.values()):
            return {"error": "A backtest of yours is still running. Wait for it to finish."}
        if sum(1 for j in BT_JOBS.values() if j["state"] == "running") >= 3:
            return {"error": "Several backtests are running right now. Try again in a minute."}
        for k in sorted(BT_JOBS, key=lambda k: BT_JOBS[k]["t"])[:-30]:
            BT_JOBS.pop(k, None)
        jid = secrets.token_urlsafe(9)
        BT_JOBS[jid] = {"user": u["id"], "state": "running", "t": time.time(), "note": "Starting"}
    threading.Thread(target=bt_job, args=(jid, u["id"], p), daemon=True).start()
    return {"ok": True, "id": jid}


@app.get("/api/backtest/{jid}")
def api_backtest_get(jid: str, request: Request):
    j = BT_JOBS.get(jid)
    if not j or j["user"] != request.state.user["id"]:
        return {"error": "That backtest was not found. Run it again."}
    return {"state": j["state"], "note": j.get("note"), "error": j.get("error"), "result": j.get("result")}


@app.post("/api/orders/preview")
def api_preview(request: Request, payload: dict = Body(...)):
    u = request.state.user
    order, err, warnings, lines = make_order(payload, u)
    if err:
        return {"error": err}
    if order["mode"] != "live":
        why = live_blocker(u)
        warnings = list(warnings) + ["PAPER ORDER: nothing is sent to your broker. " + (why if why else "To send real orders, switch the toggle at the top of the page from Paper to Live (it asks for your order PIN), then review the order again. Signing in always starts in Paper.")]
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
        return {"error": "This review was already used. Check the Portfolio tab."}
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
    td = ist_now().strftime("%Y-%m-%d")
    with ORDER_LOCK:
        mine = [public(o) for o in reversed(ORDERS) if o.get("user") == u["id"] and o.get("date") == td]
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
            "broker_fields": BROKER_FIELDS, "broker_names": BROKER_NAMES, "build": BUILD}


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
        if v[:7].lower() == "bearer ":
            v = v[7:]
        v = "".join(v.split())  # drop spaces and line breaks picked up when pasting
        if not v:
            return {"error": f"{label} is required."}
        if len(v) > (4000 if key == "access_token" else 300):  # Dhan tokens are long JWTs, 500+ characters
            return {"error": f"{label} looks too long. Paste only the token itself."}
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


def source_report():
    on = DATA_BROKER == "dhan"
    return {"dhan_available": on, "source": data_source(), "fallback": YAHOO_FALLBACK, "stream": DHAN_STAT["ws"], "healthy": dhan_ws_healthy(),
            "stream_error": DHAN_STAT["error"], "hist_error": DHAN_STAT["hist_error"], "subscribed": DHAN_STAT["subscribed"],
            "age": None if not DHAN_STAT["last"] else int(time.time() - DHAN_STAT["last"]), "connects": DHAN_STAT["connects"],
            "hist_ok": DHAN_STAT["hist_ok"], "clock": DHAN_STAT["tz"], "token": token_report()}


@app.get("/api/admin/source")
def api_admin_source(request: Request):
    denied = need_admin(request)
    if denied:
        return denied
    return source_report()


@app.post("/api/admin/dhan-token")
def api_admin_dhan_token(request: Request):
    """Renew the Dhan token now."""
    denied = need_admin(request)
    if denied:
        return denied
    if DATA_BROKER != "dhan":
        return {"error": "Dhan is not set up on this server."}
    if FEED.get("acct") is None:
        try:
            feed_account()
        except Exception as e:
            return {"error": _scrub(e)}
    ok = dhan_token_refresh("asked by the admin", force=True)
    return {"ok": ok, **token_report()}


@app.post("/api/admin/source")
def api_admin_source_set(request: Request, payload: dict = Body(...)):
    """Switch where candles and prices come from: Dhan (the default with a Dhan feed) or Yahoo."""
    denied = need_admin(request)
    if denied:
        return denied
    want = str(payload.get("source", "")).lower()
    if want not in ("dhan", "yahoo"):
        return {"error": "Choose dhan or yahoo."}
    if want == "dhan" and DATA_BROKER != "dhan":
        return {"error": "Dhan cannot be the data source until DATA_BROKER=dhan and the Dhan details are set on the server."}
    SRC["override"] = want
    live_reset()
    for tf in INTERVALS:
        STATES[tf]["checked"] = 0.0    # every timeframe is read again from the new source the next time someone looks
    return {"ok": True, **source_report()}


@app.get("/api/admin/session")
def api_admin_session(request: Request):
    denied = need_admin(request)
    if denied:
        return denied
    o = SESSION_OVERRIDE
    return {"open_now": market_open_now(), "special": special_session_now(),
            "override": ({"date": o["date"], "start": o["start"].strftime("%H:%M"), "end": o["end"].strftime("%H:%M")} if o else None),
            "env": [f"{d} {a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for d, a, b in SPECIAL_SESSIONS]}


@app.post("/api/admin/session")
def api_admin_session_set(request: Request, payload: dict = Body(...)):
    """Open a special trading session for today (Muhurat trading, weekend session), or clear it."""
    denied = need_admin(request)
    if denied:
        return denied
    if payload.get("clear"):
        SESSION_OVERRIDE.clear()
        return {"ok": True, "open_now": market_open_now(), "override": None}
    got = _parse_sessions(f"{ist_now().strftime('%Y-%m-%d')} {payload.get('start', '')}-{payload.get('end', '')}")
    if not got or got[0][1] >= got[0][2]:
        return {"error": "Enter a start and an end time like 18:00 and 19:15, with the end after the start."}
    d, a, b = got[0]
    SESSION_OVERRIDE.update(date=d, start=a, end=b)
    return {"ok": True, "open_now": market_open_now(), "override": {"date": d, "start": a.strftime("%H:%M"), "end": b.strftime("%H:%M")}}


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
    return brand(PAGE.replace("<head>", "<head>" + pwa_head(), 1)).replace("initial-scale=1\">", "initial-scale=1,viewport-fit=cover\">", 1)


EARLY_THEME = '<script>try{var t=localStorage.getItem("theme");if(t==="dark"||t==="light")document.documentElement.setAttribute("data-theme",t)}catch(e){}</script>'

AUTH_CSS = """
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;--sell:#b3261e;--buy:#0b7a63}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80;--buy:#4cc9a8}}
:root[data-theme=dark]{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80;--buy:#4cc9a8;color-scheme:dark}
:root[data-theme=light]{color-scheme:light}
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
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;--sell:#ff5c7c;--buy:#27f0b0;--acc:#8b6cff;--g:linear-gradient(135deg,#8b6cff,#2de2ff)}}
:root[data-theme=dark]{--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;--sell:#ff5c7c;--buy:#27f0b0;--acc:#8b6cff;--g:linear-gradient(135deg,#8b6cff,#2de2ff);color-scheme:dark}
:root[data-theme=light]{color-scheme:light}
body{background:radial-gradient(900px 500px at 10% -10%,rgba(108,76,245,.28),transparent 60%),radial-gradient(800px 460px at 100% 100%,rgba(0,184,217,.22),transparent 55%),var(--bg)}
form,.box{border:0;border-radius:20px;box-shadow:0 20px 60px rgba(60,40,160,.25)}
h1{background:var(--g);-webkit-background-clip:text;background-clip:text;color:transparent;font-size:28px}
h1::before{content:"";display:inline-block;width:11px;height:24px;border-radius:4px;margin-right:10px;vertical-align:-4px;background:linear-gradient(var(--buy) 50%,var(--sell) 50%)}
button{background:var(--g);color:#fff;border-radius:10px;box-shadow:0 4px 14px rgba(108,76,245,.4)}
input:focus{outline:2px solid var(--acc)}
"""

AUTH_BODIES = {
 "login": """<form id="f"><h1>SignalDesk</h1><p>Sign in to continue.</p>
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
    return brand(f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
                 f'{pwa_head()}{EARLY_THEME}<title>{title} · {BRAND}</title><style>{AUTH_CSS}</style></head><body>{body}<script>{AUTH_JS[kind]}</script></body></html>')


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in · SignalDesk</title>
<style>
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;--sell:#b3261e}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80}}
:root[data-theme=dark]{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;--sell:#ff8a80;color-scheme:dark}
:root[data-theme=light]{color-scheme:light}
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
<form id="f"><h1>SignalDesk</h1><p>Sign in to continue.</p>
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
<title>SignalDesk</title>
<script>try{var t=localStorage.getItem("theme");if(t==="dark"||t==="light")document.documentElement.setAttribute("data-theme",t)}catch(e){}</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Schibsted+Grotesk:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#eef1f4;--panel:#fff;--ink:#16202a;--mute:#6a7783;--line:#d9dfe5;
--buy:#0b7a63;--buy-bg:#d8f0e8;--sell:#b3261e;--sell-bg:#f9dcd9;--flat:#e6eaee}
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;
--buy:#4cc9a8;--buy-bg:#16372f;--sell:#ff8a80;--sell-bg:#42221f;--flat:#252f38}}
:root[data-theme=dark]{--bg:#12181e;--panel:#1a222a;--ink:#e6ebf0;--mute:#8a98a6;--line:#2a343e;
--buy:#4cc9a8;--buy-bg:#16372f;--sell:#ff8a80;--sell-bg:#42221f;--flat:#252f38;color-scheme:dark}
:root[data-theme=light]{color-scheme:light}
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
.ixrow{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
.rpch .ax{stroke:var(--mute);stroke-opacity:.35;stroke-width:1}.rpch{margin:10px 0}.rpcap{margin:10px 0;padding:10px 12px;border:1px solid var(--line,#8884);border-radius:8px}.rpbig{font-size:18px}.rpbig b{font-size:22px}.rpch svg{display:block;max-width:720px;width:100%;height:auto}.rpch text{font-size:10px;fill:var(--mute)}.rpscroll{max-height:420px;overflow:auto}
.chtrade{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0}.chtrade button{border:0;border-radius:10px;padding:8px 14px;font:inherit;font-weight:700;cursor:pointer;color:#fff}.chtrade .b{background:var(--buy)}.chtrade .s{background:var(--sell)}.chtrade button:disabled{opacity:.5}.chtrade .sub{align-self:center}
.botbox .ixrow+.wrap,.botbox .wrap+.ixrow{margin-top:14px}.botbox tr.taken,.botbox tr.past{opacity:.6}.botbox td.act{white-space:nowrap}
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
.cg{display:inline-block;margin-left:6px;padding:1px 7px;border-radius:999px;font-size:11px;font-weight:700;cursor:help;border:1px solid var(--line);vertical-align:middle}.cgA{background:rgba(34,197,94,.18);color:var(--buy);border-color:var(--buy)}.cgB{background:rgba(234,179,8,.15);color:#d4a017;border-color:#d4a017}.cgC{color:var(--mute)}.btgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr));gap:12px;margin-bottom:6px}.btgrid>.card,#btOut{min-width:0}#btOut .wrap{max-width:100%}.btadv{margin:6px 0}.btadv summary{cursor:pointer;color:var(--mute);font-size:13px;margin-bottom:6px}.btsvg{width:100%;height:auto;display:block}.btsvg .eq{fill:none;stroke:var(--buy);stroke-width:2}.btsvg .ar{fill:var(--buy);opacity:.12}.btsvg .dd{fill:var(--sell);opacity:.35}.btsvg .base{stroke:var(--mute);stroke-dasharray:4 4;fill:none}.btsvg text{fill:var(--mute);font-size:11px}
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
#chartDlg,#optDlg{width:min(1100px,96vw);max-width:96vw}.kb{background:none;border:0;color:inherit;font:inherit;font-weight:700;cursor:pointer;padding:2px 4px;border-radius:6px}.kb:hover{background:var(--line)}.kb span{font-size:11px;opacity:.7}.ogtip{font-size:13px;min-height:20px;margin:6px 0;color:var(--ink)}.ogsum{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-top:10px;font-size:13px}.ogsum div{padding:8px 10px;border:1px solid var(--line);border-radius:10px}#ogSvg{overflow-x:auto;max-width:100%}#ogSvg svg{display:block;touch-action:pan-x pan-y}#ogSvg text{fill:var(--mute);font-size:10px}#ogSvg .ax{stroke:var(--line);stroke-width:1}#ogSvg .xh{stroke:var(--acc);stroke-width:1;stroke-dasharray:3 3}
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
.botctl{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:4px 0 8px}.botctl .btn{min-width:96px}.btn.stop{background:var(--sell);color:#fff;border-color:var(--sell)}.btn:disabled{opacity:.45;cursor:not-allowed}
.botpill{padding:4px 12px;border-radius:999px;font-weight:600;font-size:13px;background:var(--line);color:var(--ink)}.botpill.running{background:#1f9d62;color:#fff}.botpill.paused{background:#d99a00;color:#111}.botpill.live{outline:2px solid var(--sell)}
.botbox{margin-bottom:12px}.botrow{display:flex;flex-wrap:wrap;gap:10px;align-items:end;margin:8px 0}.botrow label{display:flex;flex-direction:column;font-size:12px;color:var(--mute);gap:2px}.botrow input,.botrow select{width:130px}
.tkfunds{border:1px solid var(--line);border-radius:8px;padding:8px 12px;margin-bottom:10px}
.tkfunds div{display:flex;justify-content:space-between;gap:12px;padding:3px 0}.tkfunds span{color:var(--mute)}.tkfunds p{margin:6px 0 0}
.tkfunds.bad{border-color:var(--sell)}.tkfunds.bad b{color:var(--sell)}
.pnlup{color:var(--buy)!important}.pnldn{color:var(--sell)!important}
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
@media(prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;
--buy:#27f0b0;--buy-bg:#0c3b31;--sell:#ff5c7c;--sell-bg:#451526;--flat:#201f3d;
--acc:#8b6cff;--acc2:#2de2ff;--hot:#ffb347;--shadow:0 6px 24px rgba(0,0,0,.45);--g:linear-gradient(135deg,#8b6cff,#2de2ff)}}
:root[data-theme=dark]{--bg:#0b0b1a;--panel:#15152b;--ink:#ecebff;--mute:#9a98c0;--line:#2a2950;
--buy:#27f0b0;--buy-bg:#0c3b31;--sell:#ff5c7c;--sell-bg:#451526;--flat:#201f3d;
--acc:#8b6cff;--acc2:#2de2ff;--hot:#ffb347;--shadow:0 6px 24px rgba(0,0,0,.45);--g:linear-gradient(135deg,#8b6cff,#2de2ff);color-scheme:dark}
:root[data-theme=light]{color-scheme:light}
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
.ai{font-weight:700;background:var(--g);-webkit-background-clip:text;background-clip:text;color:transparent;letter-spacing:.02em}
#themeBtn{background:var(--panel);color:var(--ink);border:1px solid var(--line);border-radius:10px;padding:6px 10px;cursor:pointer;font:inherit}
.seg.tm button[data-tm=live][aria-pressed=true]{background:linear-gradient(135deg,#e0264b,#ff9f1a);color:#fff;box-shadow:0 0 14px rgba(var(--glows),.55)}
.seg.tm button[data-tm=paper][aria-pressed=true]{background:var(--g);color:#fff}
.seg.tm button:disabled{opacity:.5;cursor:not-allowed}
.scttl{font-size:16px}
#scTable td,#scTable th{text-align:left}#scTable td:nth-child(n+8),#scTable th:nth-child(n+8){text-align:right}.omain{min-width:0}.ochart{margin:10px 0;padding:10px 12px;max-width:100%;overflow:hidden}#ocSvg{overflow-x:auto;max-width:100%}.ochart svg{display:block}.ochart svg text{fill:var(--mute);font-size:10px}.ochart svg .ax{stroke:var(--line);stroke-width:1}.ochart svg .sp{stroke:var(--acc);stroke-dasharray:4 3;stroke-width:1.2}.ochart svg .atmb{fill:var(--acc);opacity:.10}.oleg{font-size:12px;color:var(--mute)}.oleg i{display:inline-block;width:10px;height:10px;border-radius:2px;margin:0 4px 0 10px;vertical-align:-1px}#scTable th.srt{cursor:pointer;user-select:none;white-space:nowrap}#scTable th.srt:hover{color:var(--acc)}#scTable th .ar{font-size:10px;margin-left:3px;opacity:.9}
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
[hidden]{display:none!important}
@media(max-width:640px){
 body{padding-left:env(safe-area-inset-left);padding-right:env(safe-area-inset-right);padding-bottom:env(safe-area-inset-bottom)}
 main{padding-left:10px;padding-right:10px}
 input,select,textarea{font-size:16px!important}
 .btn,.mini,.mode button,.seg button{min-height:40px}
 .mode{position:sticky;top:0;z-index:20;margin-left:-4px;margin-right:-4px}
 .mode button{white-space:nowrap;padding:10px 14px}
 header{flex-direction:column;align-items:flex-start;gap:8px}
 .ctl{flex-wrap:wrap;gap:6px}
 dialog{width:100vw!important;max-width:100vw!important;max-height:100dvh;margin:0;border-radius:18px 18px 0 0;position:fixed;top:auto;bottom:0}
 .tkgrid{grid-template-columns:1fr}
 .botrow label,.botrow input,.botrow select{width:100%}
 .hkpi{grid-template-columns:repeat(2,1fr)}
 table{font-size:13px}
 #chart{height:55vh!important}
}
@media(display-mode:standalone){body{-webkit-user-select:none;user-select:none}input,textarea{-webkit-user-select:text;user-select:text}}
</style></head><body><main>
<header>
 <div><h1>SignalDesk</h1><div class="sub" id="meta">Loading first scan…</div></div>
 <div class="ctl"><div class="seg" id="tfs" role="group" aria-label="Timeframe"></div><button class="btn" id="installBtn" hidden>Install app</button><button class="btn" id="refresh">Refresh now</button><div class="seg tm" id="modeSw" role="group" aria-label="Trading mode"><button data-tm="paper" aria-pressed="true">Paper</button><button data-tm="live" aria-pressed="false">Live</button></div><button id="themeBtn" title="Switch between automatic, light and dark">◐ Auto</button><button id="snd" aria-pressed="false" title="Play a sound when a new signal arrives">🔔 Sound off</button><span class="sub" id="who"></span><button class="mini" id="logout">Sign out</button></div>
</header>
<div id="toast" role="status" aria-live="polite"></div>
<div class="banner paper" id="banner"></div>
<div class="banner live" id="dpBanner" hidden>You are signed in with the default admin password. Change it under Account now. Anyone who knows it can sign in as admin.</div>
<nav class="mode" id="mode" aria-label="View"><button data-m="stocks">Stocks</button><button data-m="futures">Futures</button><button data-m="options">Options</button><button data-m="scanner">Intraday Scanner</button><button data-m="autopilot">Autopilot</button><button data-m="history">History</button><button data-m="backtest">Backtest</button><button data-m="orders">Portfolio</button><button data-m="account">Account</button><button data-m="admin" id="navAdmin" hidden>Admin</button></nav>
<section id="view-stocks">
<div class="idx" id="idxTiles"></div>
<div class="sechead"><b>Sectors</b><span class="sub" id="secnote"></span></div>
<div class="sectors" id="sectors"></div>
<div class="tally" id="tally"></div>
<div class="bar"><input id="q" placeholder="Search symbol" aria-label="Search symbol"><label class="chk" title="Show only signals where most checks agree. Hover a grade badge to see why."><input type="checkbox" id="confOnly"> Grade A only</label></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table></div>
<div class="note" id="note"></div>
</section>
<section id="view-options" hidden>
<div class="ogrid" id="ogrid">
 <aside class="olist" aria-label="Scripts">
  <input id="osym" placeholder="Search a stock or index" aria-label="Search scripts" autocomplete="off">
  <select id="osec" aria-label="Sector"><option value="ALL">All sectors</option></select>
  <select id="ofilt" aria-label="Show"><option value="all">All scripts</option><option value="sig">With a signal</option><option value="BUY">Buy signals</option><option value="SELL">Sell signals</option></select>
  <select id="osort" aria-label="Sort"><option value="sig">Sort: signal</option><option value="name">Sort: name A to Z</option><option value="up">Sort: gainers first</option><option value="down">Sort: losers first</option><option value="move">Sort: biggest move, up or down</option></select>
  <div class="orows" id="chips"></div>
 </aside>
 <div class="omain">
  <div class="osum" id="osum" hidden></div>
  <div class="card ochart" id="ochartc" hidden>
   <div class="ixrow"><div class="seg" id="ocSeg" role="group" aria-label="Graph type"></div><label class="chk"><input type="checkbox" id="ocOn" checked> Show graph</label>
    <span class="oleg"><i style="background:#ff9f43"></i>Calls <i style="background:#35c9ff"></i>Puts</span><span class="sub" id="ocNote"></span></div>
   <div class="wrap" id="ocSvg"></div></div>
  <label class="more"><input type="checkbox" id="omore"> Show volume and IV</label>
  <div class="wrap"><table class="otab" id="otable"></table></div>
  <div class="note" id="onote">Pick a script on the left. Every F&amp;O stock and index has a chain. Sensex and Bankex chains are priced by your broker and show no open interest.</div>
 </div>
</div>
</section>
<section id="view-scanner" hidden>
<div class="ixrow"><div class="seg" id="scTfs" role="group" aria-label="Timeframe"></div>
 <b class="scttl" id="scTtl">Top 15 by traded value today</b>
 <label class="chk"><input type="checkbox" id="scOnly"> Only with a signal</label> <label class="chk"><input type="checkbox" id="scConf"> Grade A only</label><span class="sub" id="scMeta"></span></div>
<div class="wrap"><table id="scTable"></table></div>

</section>
<section id="view-autopilot" hidden>
<div id="botHost"></div>
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
<section id="view-backtest" hidden>
<div class="card botbox">
 <div class="ixrow"><b>Backtest</b><span class="sub">See how a strategy would have grown your funds on past candles. Results are estimates, not promises.</span></div>
 <div class="botrow">
  <label>Strategy<select id="btStrategy"><option value="signal">Signals (fresh and ongoing)</option><option value="gap">Gap continuation at the open</option></select></label>
  <label>Trade<select id="btKind"><option value="stock">Stocks / index (cash)</option><option value="futures">Futures</option><option value="options">Options (buying)</option></select></label>
  <label>Symbols (up to 10, comma separated)<input id="btSyms" value="NIFTY" placeholder="NIFTY, RELIANCE, TCS"></label>
  <label>Timeframe<select id="btTf"><option value="5m">5 minute (last ~2 months)</option><option value="15m" selected>15 minute (last ~2 months)</option><option value="1d">Daily (years)</option></select></label>
  <label>Days of history<input id="btDays" type="number" min="10" step="1" value="58"></label>
 </div>
 <div class="botrow">
  <label>Starting funds ₹<input id="btCap" type="number" min="10000" step="10000" value="500000"></label>
  <label>Risk per trade (% of funds)<input id="btRisk" type="number" min="0.1" max="10" step="0.1" value="1"></label>
  <label>Exit at<select id="btTarget"><option value="1">1R target</option><option value="2" selected>2R target</option><option value="3">3R target</option><option value="4">4R target</option></select></label>
  <label>Only trade grade<select id="btGrade"><option value="any">Any (A, B or C)</option><option value="B">B or better</option><option value="A">A only</option></select></label>
  <label class="chk"><input type="checkbox" id="btIntra" checked> Close at 15:15 (intraday)</label>
  <label class="chk"><input type="checkbox" id="btSell" checked> Include SELL signals</label>
 </div>
 <details class="btadv"><summary>More settings: costs, margin, options</summary>
  <div class="botrow">
   <label>Leverage (stocks)<input id="btLev" type="number" min="1" max="20" step="1" value="5"></label>
   <label>Futures margin %<input id="btMargin" type="number" min="3" max="100" step="1" value="12"></label>
   <label>Lot size (blank = automatic)<input id="btLot" type="number" min="0" step="1" placeholder="auto"></label>
   <label>Option IV % (assumed)<input id="btIv" type="number" min="5" max="150" step="1" value="15"></label>
   <label>Days to expiry (options)<input id="btDte" type="number" min="1" max="60" step="1" value="4"></label>
   <label>Brokerage ₹ per order<input id="btBrk" type="number" min="0" step="1" value="20"></label>
   <label>Taxes and charges % of turnover<input id="btOth" type="number" min="0" max="1" step="0.01" value="0.05"></label>
   <label>Slippage % each side<input id="btSlip" type="number" min="0" max="2" step="0.01" value="0.03"></label>
   <label>Entries from<input id="btFrom" value="09:20" size="5"></label>
   <label>to<input id="btTo" value="14:30" size="5"></label>
   <label>Gap: largest gap %<input id="btGap" type="number" min="0.2" max="5" step="0.1" value="1.5"></label>
  </div>
 </details>
 <div class="botctl"><button class="btn go" id="btRun">▶ Run backtest</button><span class="sub" id="btNote"></span></div>
 <div class="tkmsg" id="btMsg" role="status"></div>
</div>
<div id="btOut" hidden>
 <div class="hkpi" id="btKpi"></div>
 <div class="card"><h3 style="margin:0 0 6px">Your funds over time</h3><div id="btChart"></div></div>
 <div class="note" id="btNotes" style="margin:10px 0"></div>
 <div class="btgrid">
  <div class="card"><h3>By month</h3><div class="wrap"><table id="btMonths"></table></div></div>
  <div class="card"><h3>By grade</h3><div class="wrap"><table id="btGradeT"></table></div></div>
  <div class="card"><h3>By symbol</h3><div class="wrap"><table id="btSymT"></table></div></div>
 </div>
 <h3 style="margin:14px 0 6px">Trades <span class="sub" id="btListNote"></span></h3>
 <div class="wrap"><table id="btTrades"></table></div>
</div>
</section>
<section id="view-orders" hidden>
<div class="hkpi" id="pfKpi"></div>
<div class="note" id="pfNote" style="margin:0 0 10px"></div>
<h3 style="margin:14px 0 6px">Positions</h3>
<div class="wrap"><table id="pfTable"></table></div>
<h3 style="margin:18px 0 6px">Orders today</h3>
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
 <div class="card"><h3>Special trading session</h3>
  <div class="sub">On a day the exchange opens outside the usual hours (Muhurat trading, a weekend session), live orders stay blocked until you open it here. It lasts for today only and is forgotten on restart. To keep it through restarts, set SPECIAL_SESSIONS (for example 2026-11-08 18:00-19:15) in Render.</div>
  <div class="botrow"><label>From<input id="ssFrom" value="09:15" size="5"></label><label>To<input id="ssTo" value="15:30" size="5"></label><button class="btn go" id="ssOpen">Open for today</button><button class="btn" id="ssClear">Clear</button></div>
  <div class="sub" id="ssNote"></div></div>
 <div class="card"><h3>Data source</h3>
  <div class="sub">Where candles and live prices come from. With Dhan, prices stream over Dhan's WebSocket and the candles come from Dhan's historical data, so there is a single source. Yahoo is slower and can lag, so it is only a fallback.</div>
  <div class="botrow"><button class="btn go" id="srcDhan">Use Dhan</button><button class="btn" id="srcYahoo">Use Yahoo</button></div>
  <div class="sub" id="srcNote"></div></div>
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
 <div class="chtrade" id="chTrade"></div>
 <div id="chMsg" class="tkmsg" role="status"></div>
 <div id="chart"></div>
</div></dialog>
<dialog id="optDlg" aria-labelledby="ogTitle"><div class="chbox">
 <div class="chhead"><b id="ogTitle"></b><div class="seg" id="ogView" role="group" aria-label="Show"></div><div class="seg" id="ogTf" role="group" aria-label="Candle size"></div><div class="seg" id="ogDays" role="group" aria-label="Days"></div><button class="btn ghost" id="ogClose">Close</button></div>
 <div class="oleg"><i style="background:#ff9f43"></i>Call (CE) <i style="background:#35c9ff"></i>Put (PE)</div>
 <div id="ogTip" class="ogtip" aria-live="polite">Move over the chart to read prices.</div>
 <div id="ogMsg" class="tkmsg" role="status"></div>
 <div class="wrap" id="ogSvg"></div>
 <div class="ogsum" id="ogSum"></div>
 <div class="chtrade" id="ogTrade"></div>
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
 <div class="tkfunds" id="tkFunds" hidden></div>
 <div class="tkreview" id="tkReview" hidden></div>
 <div class="tkgrid" id="tkConfirm" hidden>
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
 optOverride="";document.querySelectorAll("#tfs button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.tf===tf));load();if(osym)loadChain();if(mode==="history")loadHistory()};
let tickT=null,tickBusy=false;
async function tick(){clearTimeout(tickT);
 try{if(mode==="stocks"&&data&&data.feed&&data.feed.source&&!document.hidden&&!tickBusy){tickBusy=true;const asked=tf;const d=await (await api("/api/ticks?tf="+asked)).json();tickBusy=false;
  if(asked===tf&&data&&d.p&&d.age!=null&&d.age<30){let ch=false;for(const r of data.rows){const x=d.p[r.symbol];if(x&&(r.ltp!==x[0]||r.ltp_chg!==x[1])){r.ltp=x[0];if(x[1]!=null)r.ltp_chg=x[1];ch=true}}if(ch)render()}}}
 catch(e){tickBusy=false}
 tickT=setTimeout(tick,2000)}
async function load(){clearTimeout(timer);const asked=tf;
 try{const d=await (await api("/api/signals"+(asked?"?tf="+asked:""))).json();if(asked!==tf)return;data=d;tf=d.interval;detectNew(d);render();
  timer=setTimeout(load,(d.refreshing||!d.updated)?3000:20000);if(!tickT)tick()}
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
  case "signal":return `<td><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span>${confBadge(r)}</td>`;
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
 document.getElementById("meta").innerHTML=`<span class="ai">✦ AI-Powered Trade Scanner</span> · `+(data.live&&data.live.on?`<b title="Candles are completed from the live feed the moment they close">Live candles${data.live.at?" "+esc(data.live.at):""}</b> · `:(data.updated?`Updated ${esc(data.updated)} · `:""))+`${tfName[data.interval]} candles`+(data.live?` · <span title="Where candles and prices come from">${data.live.source==="dhan"?(data.live.stream?"Dhan live stream":"Dhan"):"Yahoo"}</span>`:"")+(data.refreshing?(data.progress?` · scanning ${data.progress[0]} of ${data.progress[1]}…`:" · scanning…"):"");
 let v=rows.filter(r=>(filter==="ALL"||(filter==="OPEN"?isOpen(r):r.signal===filter))&&(!q||r.symbol.includes(q))&&(!$("confOnly").checked||(r.conf&&r.conf.g==="A"))&&(sector==="ALL"||(r.sector||"Others")===sector));
 v.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];if(sortKey==="signal"){x=rank[x];y=rank[y];if(x===y){x=a.bars_ago??999;y=b.bars_ago??999}}
  if(x==null)x=1e9;if(y==null)y=1e9;return (x>y?1:x<y?-1:0)*sortDir});
 const cs=curCols();
 document.getElementById("body").innerHTML=v.length?v.map(r=>`<tr${rowCls(r)}>${cs.map(c=>cellFor(r,c[0])).join("")}</tr>`).join(""):`<tr><td colspan="${cs.length}" class="empty">${data.updated?"No symbols match this filter.":"First scan is running. This page updates on its own."}</td></tr>`;
 document.getElementById("note").textContent=(data.errors.length?`${data.errors.length} symbol(s) skipped: ${data.errors.slice(0,3).join("; ")}${data.errors.length>3?"…":""}. `:"")+`BUY and SELL show for ${data.fresh_bars} candles after a signal. Open trades are older signals whose stop and T4 have not been reached. Entry, SL and targets come from the latest signal for each symbol.`+(fut?" Futures signals come from the same indicator on the stock's or index's own candles (spot prices). The lot size and expiry shown are for the nearest-month contract, from Angel One's instrument list.":"");
}
// ── Options tab ──
let mode="stocks",osym="",oexp="",oreq=0;
let optOverride="";
const optTf=()=>optOverride||tf||"";
try{mode=localStorage.getItem("mode")||"stocks"}catch(e){}
const esc=s=>String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
const fk=n=>{if(n==null)return "–";const a=Math.abs(n),g=n<0?"-":"";return g+(a>=1e7?(a/1e7).toFixed(2)+" Cr":a>=1e5?(a/1e5).toFixed(2)+" L":a>=1e3?(a/1e3).toFixed(1)+" K":String(Math.round(a)))};
const f2=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const sgn=n=>n==null?"–":`<span class="${n>=0?"up":"dn"}">${n>0?"+":""}${fk(n)}</span>`;
function setMode(m){if(m==="index")m="options";if(m==="admin"&&!(me&&me.user.role==="admin"))m="stocks";mode=m;try{localStorage.setItem("mode",m)}catch(e){}
 const vm={stocks:"stocks",futures:"stocks",options:"options",scanner:"scanner",autopilot:"autopilot",history:"history",backtest:"backtest",orders:"orders",account:"account",admin:"admin"};
 ["stocks","options","scanner","autopilot","history","backtest","orders","account","admin"].forEach(x=>document.getElementById("view-"+x).hidden=x!==vm[m]);
 document.querySelectorAll("#mode button").forEach(b=>b.setAttribute("aria-pressed",b.dataset.m===m));
 if(m==="scanner"){scPaint();scLoad()}else clearTimeout(scTimer);
 if(m==="autopilot")botLoad();else clearTimeout(botTimer);
 if(m==="orders")loadOrders();if(m==="options")renderChips();if(m==="history")loadHistory();if(m==="account"){loadMe();loadAlerts()}if(m==="admin")loadUsers();
 if(m==="stocks"||m==="futures"){filter="ALL";q="";document.getElementById("q").value="";renderHead();if(m==="futures")loadFutInfo();render()}}
function initConf(){$("confOnly").onchange=()=>render()}
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
 const so=document.getElementById("osort").value,ch=n=>{const r=by[n];if(!r)return null;const c=r.ltp_chg!=null?r.ltp_chg:r.chg;return c==null?null:c};
 list.sort((a,b)=>{
  if(so!=="sig"&&so!=="name"){const ca=ch(a),cb=ch(b);if(ca==null||cb==null){if(ca!==cb)return ca==null?1:-1}else{const d=so==="up"?cb-ca:so==="down"?ca-cb:Math.abs(cb)-Math.abs(ca);if(d)return d}return a.localeCompare(b)}
  if(so==="name")return a.localeCompare(b);
  const ra=by[a]?rank[by[a].signal]:99,rb=by[b]?rank[by[b].signal]:99;return (ra??99)-(rb??99)||a.localeCompare(b)});
 box.innerHTML=list.length?list.map(n=>{const sg=(by[n]||{}).signal||"–";
  const c=by[n]?(by[n].ltp_chg!=null?by[n].ltp_chg:by[n].chg):null;
  return `<button class="orow" data-s="${esc(n)}" aria-pressed="${n===osym}"><span>${esc(n)}</span><span class="ochg ${c==null?"":c>=0?"up":"dn"}">${c==null?"":(c>0?"+":"")+c.toFixed(2)+"%"}</span>${sg==="–"?'<span class="none">–</span>':`<span class="pill ${sg.replace(" ","_")}">${sg}</span>`}</button>`}).join(""):`<div class="sub" style="padding:12px">No scripts match.</div>`;
 box.scrollTop=keep;
}
document.getElementById("chips").onclick=e=>{const b=e.target.closest(".orow");if(b){optOverride="";osym=b.dataset.s;oexp="";loadChain()}};
document.getElementById("osym").oninput=renderChips;
document.getElementById("ofilt").onchange=renderChips;
document.getElementById("osec").onchange=renderChips;
try{const sv=localStorage.getItem("osort");if(sv&&[...document.getElementById("osort").options].some(o=>o.value===sv))document.getElementById("osort").value=sv}catch(e){}
document.getElementById("osort").onchange=()=>{try{localStorage.setItem("osort",document.getElementById("osort").value)}catch(e){}renderChips()};
document.getElementById("osym").onkeydown=e=>{if(e.key!=="Enter")return;
 const t=e.target.value.trim().toUpperCase();if(!t)return;
 const all=((data&&data.indices)||[]).concat((data&&data.symbols)||[]);
 osym=all.includes(t)?t:(all.find(n=>n.startsWith(t))||t);oexp="";loadChain()};
document.getElementById("omore").onchange=()=>{if(lastChain)renderChain(lastChain)};
async function loadChain(quiet){
 if(!osym)return;const my=++oreq,box=document.getElementById("onote");
 if(!quiet){document.getElementById("otable").innerHTML="";document.getElementById("osum").hidden=true;box.textContent=`Loading the ${osym} option chain…`}
 renderChips();
 try{const u=`/api/options/chain?symbol=${encodeURIComponent(osym)}&tf=${optTf()}`+(oexp?`&expiry=${encodeURIComponent(oexp)}`:"");
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
 if(document.hidden){newCount+=add.length;document.title=`(${newCount}) New signals · SignalDesk`}
 if(sndOn)beep()}
function initFlash(){const b=$("snd");const paint=()=>{b.setAttribute("aria-pressed",String(sndOn));b.textContent=sndOn?"🔔 Sound on":"🔕 Sound off"};paint();
 b.onclick=()=>{sndOn=!sndOn;try{localStorage.setItem("snd",sndOn?"1":"0")}catch(e){}paint();if(sndOn)beep()};
 $("toast").onclick=()=>$("toast").classList.remove("show");
 document.addEventListener("visibilitychange",()=>{if(!document.hidden){newCount=0;document.title="SignalDesk"}})}
// ── theme (auto / light / dark) and paper / live switch ──
let themePref="auto";try{themePref=localStorage.getItem("theme")||"auto"}catch(e){}
function paintTheme(){const r=document.documentElement;if(themePref==="auto")r.removeAttribute("data-theme");else r.setAttribute("data-theme",themePref);
 $("themeBtn").textContent=themePref==="dark"?"🌙 Dark":themePref==="light"?"☀️ Light":"◐ Auto";if($("chartDlg").open&&chSym)drawChart()}
function paintMode(){const live=tradeInfo&&tradeInfo.mode==="live";$("modeSw").querySelectorAll("button").forEach(b=>b.setAttribute("aria-pressed",String((b.dataset.tm==="live")===!!live)));
 const lb=$("modeSw").querySelector('[data-tm=live]');lb.title=tradeInfo&&tradeInfo.live_blocker?tradeInfo.live_blocker:"Send real orders to your broker";lb.classList.toggle("off",!!(tradeInfo&&tradeInfo.live_blocker))}
function initModes(){paintTheme();
 $("themeBtn").onclick=()=>{themePref=themePref==="auto"?"dark":themePref==="dark"?"light":"auto";try{localStorage.setItem("theme",themePref)}catch(e){}paintTheme()};
 $("modeSw").onclick=async e=>{const b=e.target.closest("button");if(!b||!tradeInfo)return;const want=b.dataset.tm;if(want===tradeInfo.mode)return;
  if(want==="live"){if(tradeInfo.live_blocker){alert(tradeInfo.live_blocker);return}
   const pin=prompt("Switching to LIVE trading. Orders will go to your real broker account.\nEnter your order PIN to continue.");if(pin===null)return;
   const d=await post("/api/trading/mode",{mode:"live",pin});if(d.error){alert(d.error);return}}
  else{const d=await post("/api/trading/mode",{mode:"paper"});if(d.error){alert(d.error);return}}
  await loadTrading();if(typeof loadOrders==="function")try{loadOrders()}catch(x){}}}
// ── Intraday Scanner tab ──
let scTf="15m",scTimer=null,scData=null;
try{scTf=localStorage.getItem("scTf")==="5m"?"5m":"15m"}catch(e){}
function scPaint(){$("scTfs").innerHTML=["5m","15m"].map(x=>`<button data-t="${x}" aria-pressed="${x===scTf}">${tfName[x]||x}</button>`).join("");
}
async function scLoad(){clearTimeout(scTimer);if(mode!=="scanner")return;const my=scTf;
 try{const d=await (await api(`/api/scanner?tf=${scTf}&only=${$("scOnly").checked?1:0}`)).json();if(my!==scTf)return;scData=d;scRender()}
 catch(e){$("scMeta").textContent="Cannot reach the server."}
 scTimer=setTimeout(scLoad,scData&&scData.scanning?3000:20000)}
let scSort={k:null,d:1};try{const q=JSON.parse(localStorage.getItem("scSort")||"null");if(q&&q.k)scSort=q}catch(e){}
const SC_COLS=[["#","rank"],["Symbol","symbol"],["Signal","signal"],["Status","status"],["Signal candle","age"],["",null],["Sector","sector"],["Sector %","sector_chg"],["Price","price"],["Day %","chg"],["Volume","vol"],["Value ₹ Cr","val_cr"],["Entry","entry"],["SL","sl"],["T1","tp1"],["T2","tp2"],["T3","tp3"],["T4","tp4"]];
const SC_TEXT=["symbol","status","sector","rank","age","signal"];
function scVal(r,k){if(k==="rank")return r.rank==null?-1:r.rank;if(k==="age")return r.bars_ago;
 if(k==="signal")return r.signal==="–"?null:(r.signal.startsWith("Open")?1000:0)-((r.conf&&r.conf.s)||0);
 if(["entry","sl","tp1","tp2","tp3","tp4"].includes(k))return r.levels?r.levels[k]:null;return r[k]===undefined?null:r[k]}
function scSorted(rows){if(!scSort.k)return rows;const k=scSort.k,d=scSort.d;
 return rows.map((r,i)=>[r,i]).sort((a,b)=>{const x=scVal(a[0],k),y=scVal(b[0],k);if(x==null&&y==null)return a[1]-b[1];if(x==null)return 1;if(y==null)return -1;
  const c=typeof x==="string"?x.localeCompare(y):x-y;return c?c*d:a[1]-b[1]}).map(x=>x[0])}
function scRender(){const d=scData,tb=$("scTable");if(!d||d.error){tb.innerHTML=`<tbody><tr><td class="empty">${d?esc(d.error):""}</td></tr></tbody>`;return}
 $("scMeta").textContent=(d.source==="nse"?"NSE most active":"Computed from candle data")+(d.updated?` · candles updated ${d.updated}`:"")+(d.scanning?" · scanning…":"");
 $("scTtl").textContent=`NIFTY, SENSEX and the top ${d.top||15} stocks by traded ${d.rank||"value"} today · large and mid caps · price ₹${d.min_price||0}+`;
 const head="<thead><tr>"+SC_COLS.map(([n,k])=>k?`<th class="srt" data-k="${k}" aria-sort="${scSort.k===k?(scSort.d>0?"ascending":"descending"):"none"}" title="Sort by ${esc(n)}">${n}<span class="ar">${scSort.k===k?(scSort.d>0?"▲":"▼"):""}</span></th>`:`<th>${n}</th>`).join("")+"</tr></thead>";
 if(!d.rows.length){tb.innerHTML=head+`<tbody><tr><td colspan="${SC_COLS.length}" class="empty">${d.scanning?"First scan is running. This page updates on its own.":"Nothing matches."}</td></tr></tbody>`;return}
 tb.innerHTML=head+"<tbody>"+scSorted(d.rows.filter(r=>!$("scConf").checked||(r.conf&&r.conf.g==="A"))).map(r=>{const L=r.levels,fr=r.signal==="BUY"||r.signal==="SELL",lv=(k,i)=>L?`<td class="${r.status&&r.status.startsWith("T")&&+r.status[1]>i?"up":""}">${f2(L[k])}</td>`:"<td>–</td>";
  const c=r.chg==null?"<td>–</td>":`<td class="${r.chg>=0?"up":"dn"}">${r.chg>0?"+":""}${r.chg.toFixed(2)}%</td>`;
  const sc=r.sector_chg==null?"<td>–</td>":`<td class="${r.sector_chg>=0?"up":"dn"}">${r.sector_chg>0?"+":""}${r.sector_chg.toFixed(2)}%</td>`;
  const side=r.signal.endsWith("SELL")?"SELL":"BUY",has=r.signal!=="–"&&L;
  return `<tr${fr?` class="fresh ${r.signal==="BUY"?"buy":"sell"}"`:""}><td>${r.rank==null?"–":r.rank}</td><td><b>${esc(r.symbol)}</b>${r.index?'<span class="tag">Index</span>':r.cap?`<span class="tag">${r.cap}</span>`:""}${fr?'<span class="newb">NEW</span>':""}</td>
   <td><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span>${confBadge(r)}</td><td>${esc(r.status)}</td><td>${esc(r.signal_time)}</td>
   <td class="act"><button class="mini" data-sc="chart" data-s="${esc(r.symbol)}">Chart</button> ${has?`<button class="mini" data-sc="fut" data-s="${esc(r.symbol)}" data-side="${side}" data-p="${r.price}">Futures</button> `:""}<button class="mini" data-sc="opt" data-s="${esc(r.symbol)}">Options</button></td>
   <td>${esc(r.sector||"–")}</td>${sc}<td>${f2(r.price)}</td>${c}<td>${r.vol!=null?fk(r.vol):"–"}</td><td>${r.val_cr!=null?f2(r.val_cr):"–"}</td>${L?`<td>${f2(L.entry)}</td><td>${f2(L.sl)}</td>`+lv("tp1",0)+lv("tp2",1)+lv("tp3",2)+lv("tp4",3):"<td>–</td>".repeat(6)}</tr>`}).join("")+"</tbody>"}
let botTimer=null;const BOTS={bot:{kind:"scanner",title:"Autopilot 1: F&O stocks"},nbot:{kind:"nifty",title:"Autopilot 2: NIFTY"}};
window.botD={};
function botHtml(p,t){return `<div class="card botbox" id="${p}Box">
 <div class="ixrow"><b>${t}</b><span class="sub" id="${p}State"></span></div>
 <div class="sub" id="${p}Rules"></div>
 <div class="botrow">
  <label>Mode<select id="${p}Mode"><option value="paper">Paper</option><option value="live">Live</option></select></label>
  <label>Only trade grade<select id="${p}Grade"><option value="any">Any (A, B or C)</option><option value="B">B or better</option><option value="A">A only</option></select></label>
  <label>Entries<select id="${p}Ent"><option value="both">Fresh + continuation</option><option value="fresh">Fresh 5/15 min signals only</option><option value="gap">Continuation only (yesterday's signal near its trend line)</option></select></label>
  <label>Trades a day<input id="${p}Max" type="number" min="1" max="10" step="1"></label>
  <label>Lots each<input id="${p}Lots" type="number" min="1" step="1"></label>
  <label>Stop for the day at a loss of ₹<input id="${p}Loss" type="number" min="0" step="500"></label>
  <button class="btn" id="${p}Save">Save settings</button>
 </div>
 <div class="botctl">
  <button class="btn go" id="${p}Play" title="Start or resume">▶ Play</button>
  <button class="btn" id="${p}Pause" title="No new trades. Open positions stay managed">⏸ Pause</button>
  <button class="btn stop" id="${p}Stop" title="Sell this autopilot's positions and switch it off">■ Stop</button>
  <span class="botpill" id="${p}Pill"></span>
 </div>
 <div class="tkmsg" id="${p}Msg" role="status"></div>
 <div class="wrap"><table id="${p}Trades"></table></div>
 <label class="sub" style="display:flex;gap:6px;align-items:center;margin:6px 0"><input type="checkbox" id="${p}Past"> Show trades from earlier days</label>
 <div class="ixrow"><b>Signals for manual trades</b><span class="sub" id="${p}SigNote">Every signal from today that fits this autopilot's rules, and what happened to it. Shown even while it is off.</span></div>
 <div class="wrap"><table id="${p}Sig"></table></div>
 <div class="ixrow"><b>Replay past days</b><select id="${p}RDay" aria-label="Day to replay"></select><button class="btn" id="${p}RRun">Replay</button><label class="sub">Capital ₹ <input id="${p}RCap" type="number" min="1000" step="1000" value="100000" style="width:90px"></label><label class="sub">Charges per trade (%) <input id="${p}RFee" type="number" min="0" max="2" step="0.01" value="0.05" style="width:60px"></label><span class="sub" id="${p}RMsg">Runs the rules above over the last month of 5 and 15 minute candles, or one day. Nothing is traded.</span></div>
 <div id="${p}RRes"></div>
 <div class="sub" id="${p}Log"></div>
</div>`}
async function botLoad(){clearTimeout(botTimer);if(mode!=="autopilot")return;
 for(const p in BOTS){try{const d=await (await api("/api/optbot?bot="+BOTS[p].kind)).json();window.botD[p]=d;
  if(document.activeElement&&[p+"Mode",p+"Ent",p+"Grade",p+"Max",p+"Lots",p+"Loss"].includes(document.activeElement.id)){}else{$(p+"Mode").value=d.mode==="off"?d.choice:d.mode;$(p+"Ent").value=d.entries;$(p+"Grade").value=d.min_grade;$(p+"Max").value=d.max_trades;$(p+"Lots").value=d.lots;$(p+"Loss").value=d.loss_limit}
  $(p+"Rules").textContent=d.rules;
  $(p+"State").textContent=(d.mode==="off"?"":d.mode==="live"?"LIVE, real money":"Paper, simulated")+(d.last_scan&&d.mode!=="off"?` · last check ${d.last_scan}`:"")+` · today ${d.pnl>=0?"+":"-"}₹${Math.abs(d.pnl).toLocaleString("en-IN")}`;
  const pill=$(p+"Pill");pill.className="botpill "+d.state+(d.mode==="live"?" live":"");pill.textContent=d.state==="running"?"Running":d.state==="paused"?"Paused":"Stopped";const mm=$(p+"Msg");if(mm.textContent==="Running."&&d.state!=="running")mm.textContent="The autopilot is stopped (the server restarted or it was stopped elsewhere). Press Play to start it.";
  $(p+"Play").disabled=d.state==="running";$(p+"Pause").disabled=d.state!=="running";$(p+"Stop").disabled=d.state==="stopped"&&!d.open;$(p+"Mode").disabled=d.state!=="stopped";
  const showPast=$(p+"Past").checked,tr=d.trades.filter(t=>showPast||!t.past),np=d.trades.length-tr.length;$(p+"Past").nextSibling.textContent=" Show trades from earlier days"+(np?` (${np})`:"");
  $(p+"Trades").innerHTML=tr.length?`<thead><tr><th>Date</th><th>Time</th><th>Option</th><th>Entry</th><th>Mode</th><th>Bought</th><th>Sold</th><th>P&amp;L</th><th>Status</th></tr></thead><tbody>`+tr.map(t=>`<tr class="${t.past?"past":""}"><td>${t.past?esc(t.date||""):"Today"}</td><td>${t.time}</td><td>${esc(t.label)}</td><td>${t.etype==="gap"||t.etype==="continuation"?"Continuation":t.etype==="fresh"?"Fresh":"Signal"}</td><td>${t.mode}</td><td>${t.entry?f2(t.entry):"–"}</td><td>${t.exit?f2(t.exit):"–"}</td><td class="${pnlCls(t.pnl||0)}">${t.pnl==null?"–":money(t.pnl)}</td><td>${t.state==="open"?"Open · stop "+f2(t.p_sl)+" · T1 "+f2(t.p_t1)+" · T2 "+f2(t.p_t2):esc(t.reason||t.state)}</td></tr>`).join("")+"</tbody>":`<tbody><tr><td class="sub">No trades today.</td></tr></tbody>`;
  const sg=d.signals||[];$(p+"Sig").innerHTML=sg.length?`<thead><tr><th>Stock</th><th></th><th>Status</th><th>Why</th><th>Price</th><th>Stop</th><th>T1</th><th>T2</th><th>T3</th><th>T4</th><th>Grade</th><th></th></tr></thead><tbody>`+sg.map(x=>`<tr class="${x.status==="passed"||x.status==="earlier"?"past":""}"><td><b>${esc(x.symbol)}</b></td><td class="act"><button class="mini" data-ap="chart" data-s="${esc(x.symbol)}" data-tf="${x.tf}">Chart</button> <button class="mini" data-ap="ochart" data-s="${esc(x.symbol)}" data-r="${x.right}" data-tf="${x.tf}">Option chart</button> <button class="mini go" data-ap="trade" data-s="${esc(x.symbol)}" data-r="${x.right}" data-tf="${x.tf}">Trade ${x.right}</button></td><td>${x.status==="taken"?"<b>Bought by the autopilot</b>":x.status==="open"?"<b>Open now</b>":x.status==="passed"?"Passed: "+esc(x.reason||""):"Earlier today: "+esc(x.reason||"")}</td><td>${x.type==="fresh"?"Fresh "+esc(x.time||""):"Continuation"}</td><td>${x.px}</td><td>${x.sl}</td><td>${x.t1}</td><td>${x.t2}</td><td>${x.t3}</td><td>${x.t4}</td><td>${x.grade?`<span class="cg cg${x.grade}">${x.grade}</span>`:"–"}</td></tr>`).join("")+"</tbody>":`<tbody><tr><td class="sub">No signal that fits the rules yet today.</td></tr></tbody>`;
  $(p+"Sig").parentElement.hidden=false;
  $(p+"Log").innerHTML=d.log.slice(0,6).map(l=>`${l.t} ${esc(l.text)}`).join("<br>")}catch(e){}}
 botTimer=setTimeout(botLoad,8000)}
async function botCtl(p,action,extra){const D=window.botD[p]||{};const body={action,bot:BOTS[p].kind,mode:$(p+"Mode").value,entries:$(p+"Ent").value,min_grade:$(p+"Grade").value,max_trades:+$(p+"Max").value,lots:+$(p+"Lots").value,loss_limit:+$(p+"Loss").value,...extra};
 if(action==="play"&&body.mode==="live"&&!(D.mode==="live"&&D.armed)){const pin=prompt("Order PIN, to arm this live autopilot for today");if(pin===null)return;body.pin=pin}
 if(action==="stop"&&!confirm(D.open?`Stop this autopilot and sell ${D.open} open position(s) now?`:"Stop this autopilot?"))return;
 const d=await post("/api/optbot/control",body);$(p+"Msg").textContent=d.error||({play:"Running.",pause:"Paused. No new trades; open positions are still managed.",stop:d.left?`Stopped, but ${d.left} position(s) could not be closed. Check your broker app.`:"Stopped. All positions closed.",save:"Saved."})[action];botLoad()}
let installEv=null;
function initInstall(){const b=$("installBtn"),standalone=matchMedia("(display-mode: standalone)").matches||navigator.standalone;if(standalone)return;
 addEventListener("beforeinstallprompt",e=>{e.preventDefault();installEv=e;b.hidden=false});
 if(/iphone|ipad|ipod/i.test(navigator.userAgent)){b.hidden=false}
 b.onclick=async()=>{if(installEv){installEv.prompt();await installEv.userChoice;installEv=null;b.hidden=true}else alert("On iPhone: tap the Share button in Safari, then Add to Home Screen. On Android: open the browser menu and choose Install app.")};
 addEventListener("appinstalled",()=>{b.hidden=true})}
function rpDays(p,days,sel){const el=$(p+"RDay"),cur=sel||el.value;if(!days){days=[];const d=new Date();while(days.length<6){d.setDate(d.getDate()-1);if(d.getDay()%6)days.unshift(d.toISOString().slice(0,10))}}
 el.innerHTML=`<option value="month"${cur==="month"||!cur?" selected":""}>Whole month (all days)</option>`+days.slice().reverse().map(x=>`<option value="${x}"${x===cur?" selected":""}>${x}</option>`).join("")}
async function rpRun(p){const b=$(p+"RRun"),m=$(p+"RMsg");b.disabled=true;$(p+"RRes").innerHTML="";m.textContent="Starting…";
 try{let r=await post("/api/optbot/replay",{bot:BOTS[p].kind,day:$(p+"RDay").value});if(r.error){m.textContent=r.error;b.disabled=false;return}
  const poll=async()=>{const g=await (await api("/api/optbot/replay?bot="+BOTS[p].kind)).json();
   if(g.status==="running"){m.textContent=(g.msg||"Working…")+(g.total?` ${g.done} of ${g.total} candle sets`:"");setTimeout(poll,2000);return}
   b.disabled=false;if(g.status==="done"){m.textContent=`Replayed ${g.result.day}.`;rpDays(p,g.result.days,$(p+"RDay").value);rpShow(p,g.result)}else m.textContent=g.msg||"The replay could not run."};
  poll()}catch(e){m.textContent="The replay could not run.";b.disabled=false}}
const rpInr=v=>(v<0?"-":"")+"₹"+Math.abs(Math.round(v)).toLocaleString("en-IN");
function rpSim(r,cap,feePct){const tr=r.trades.slice().sort((a,b)=>(a.day||r.day)<(b.day||r.day)?-1:(a.day||r.day)>(b.day||r.day)?1:a.entered<b.entered?-1:1),out={};
 for(const comp of [true,false]){let eq=cap,peak=cap,dd=0,ddp=0;const curve=[cap],days={};
  for(const t of tr){const pnl=(comp?eq:cap)*(((t.opt_pct!=null?t.opt_pct:(t.pct||0)))-feePct)/100;eq+=pnl;const d=t.day||r.day;days[d]=(days[d]||0)+pnl;curve.push(eq);peak=Math.max(peak,eq);dd=Math.max(dd,peak-eq);ddp=Math.max(ddp,(peak-eq)/peak*100)}
  out[comp?"c":"f"]={end:eq,curve,dd,ddp,days:Object.entries(days).map(([day,v])=>({day,v}))}}
 return out}
function rpCharts(eq,pd,fmt,base,title){if(eq.length<2)return "";const W=640,H=150,pl=52,pr=8,pt=8,pb=18,n=eq.length,lo=Math.min(base,...eq),hi=Math.max(base,...eq),sp=(hi-lo)||1,x=i=>pl+i/(n-1)*(W-pl-pr),y=v=>pt+(hi-v)/sp*(H-pt-pb);
 let g=`<line class="ax" x1="${pl}" x2="${W-pr}" y1="${y(base)}" y2="${y(base)}"/>`;for(const v of [lo,(lo+hi)/2,hi])g+=`<text x="${pl-4}" y="${y(v)+3}" text-anchor="end">${fmt(v)}</text>`;
 const pts=eq.map((v,i)=>`${x(i)},${y(v)}`).join(" "),end=eq[n-1],cl=end>=base?"var(--buy)":"var(--sell)";
 const svg=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${title}">${g}<polygon points="${x(0)},${y(base)} ${pts} ${x(n-1)},${y(base)}" fill="${cl}" opacity=".12"/><polyline fill="none" stroke="${cl}" stroke-width="2" points="${pts}"/><text x="${W-pr}" y="${H-4}" text-anchor="end">${n-1} trade${n>2?"s":""}</text></svg>`;
 let bars="";if(pd&&pd.length>1){const H2=130,m=Math.max(1e-9,...pd.map(d=>Math.abs(d.v))),bw=(W-pl-pr)/pd.length,mid=pt+(H2-pt-pb)/2,sc=(H2-pt-pb)/2/m;
  bars=`<svg viewBox="0 0 ${W} ${H2}" role="img" aria-label="Result of each day"><line class="ax" x1="${pl}" x2="${W-pr}" y1="${mid}" y2="${mid}"/><text x="${pl-4}" y="${pt+8}" text-anchor="end">+${fmt(m,1)}</text><text x="${pl-4}" y="${H2-pb}" text-anchor="end">-${fmt(m,1)}</text>`+pd.map((d,i)=>{const h=Math.abs(d.v)*sc,yy=d.v>=0?mid-h:mid;return `<rect x="${pl+i*bw+1}" y="${yy}" width="${Math.max(2,bw-2)}" height="${Math.max(1,h)}" fill="${d.v>=0?"var(--buy)":"var(--sell)"}"><title>${d.day}: ${d.v>0?"+":""}${fmt(d.v,1)}</title></rect>`}).join("")+`<text x="${pl}" y="${H2-4}">${pd[0].day.slice(5)}</text><text x="${W-pr}" y="${H2-4}" text-anchor="end">${pd[pd.length-1].day.slice(5)}</text></svg>`}
 return `<div class="rpch"><div class="sub">${title}</div>${svg}${bars?`<div class="sub">Result of each day</div>${bars}`:""}</div>`}
function rpTrade(r,x){return x.taken?r.trades.find(t=>(t.day||r.day)===(x.day||r.day)&&t.symbol===x.symbol&&t.tf===x.tf&&t.entered===x.at):null}
function rpByTf(r){const o={};for(const t of r.trades){const k=o[t.tf]=o[t.tf]||{n:0,w:0,l:0,tr:0,pct:0,op:0,gw:0,gl:0};k.n++;k.op+=t.opt_pct||0;k.tr+=t.r;k.pct+=t.pct||0;if(t.r>0){k.w++;k.gw+=t.r}else if(t.r<0){k.l++;k.gl-=t.r}}
 const ks=Object.keys(o).sort();if(!ks.length)return "";const f=(v,d=2)=>(v>0?"+":"")+v.toFixed(d);
 return `<div class="wrap"><table><thead><tr><th>Chart</th><th>Trades</th><th>Win rate</th><th>Total R</th><th>Avg R / trade</th><th>Profit factor</th><th>Avg stock move</th><th>Avg option P&amp;L</th></tr></thead><tbody>`+ks.map(k=>{const a=o[k];return `<tr><td><b>${k==="5m"?"5 min":"15 min"}</b></td><td>${a.n}</td><td>${(a.w/a.n*100).toFixed(0)}%</td><td class="${a.tr>=0?"up":"dn"}">${f(a.tr,1)}R</td><td>${f(a.tr/a.n)}R</td><td>${a.gl?(a.gw/a.gl).toFixed(2):"–"}</td><td>${f(a.pct/a.n)}%</td><td class="${a.op>=0?"up":"dn"}">${f(a.op/a.n,1)}%</td></tr>`}).join("")+`</tbody></table></div>`}
function rpShow(p,r){window.rpLast=window.rpLast||{};window.rpLast[p]=r;const cap=Math.max(1000,+$(p+"RCap").value||100000),fee=+$(p+"RFee").value||0,sim=rpSim(r,cap,fee),sc=sim.c,sf=sim.f,pc=(sc.end/cap-1)*100,when=r.mode==="range"?"by the end of the month":"by the end of the day";
 const money=`<div class="rpcap"><div class="rpbig"><span class="sub">Starting with ${rpInr(cap)}, buying the at-the-money call (CE) or put (PE) with the whole account on each trade, exiting at the stop-loss or target the system sets${fee?`, less ${fee}% charges and slippage per trade`:""}</span><div>${rpInr(cap)} → <b class="${sc.end>=cap?"up":"dn"}">${rpInr(sc.end)}</b> <span class="${pc>=0?"up":"dn"}">${pc>=0?"+":""}${pc.toFixed(1)}%</span> <span class="sub">${when}</span></div></div>
  <div class="sub">Profit ${rpInr(sc.end-cap)} · biggest fall from a peak ${rpInr(-sc.dd)} (${sc.ddp.toFixed(1)}%) · without compounding (${rpInr(cap)} in every trade): ${rpInr(sf.end)} (${((sf.end/cap-1)*100).toFixed(1)}%)</div></div>`;
 const s=r.summary,rg=r.mode==="range",R=x=>`<span class="${x>0?"up":x<0?"dn":""}">${x>0?"+":""}${x}R</span>`,why=t=>t==="fresh"?"Fresh":"Continuation",pill=(sd,rt)=>`<span class="pill ${sd}">${rt==="CE"?"CALL (CE)":"PUT (PE)"}</span>`,cb=(x)=>`<button class="mini" data-rc="${esc(x.symbol)}|${x.tf}|${x.ts}">Chart</button>`;
 const dy=x=>rg?`<td>${x.day.slice(5)}</td>`:"",dh=rg?"<th>Day</th>":"";
 const tr=r.trades.length?`<div class="wrap rpscroll"><table><thead><tr>${dh}<th>Stock</th><th>Buy</th><th>Chart</th><th>Why</th><th>In</th><th>Price</th><th>Stop</th><th>T1</th><th>T4</th><th>Out</th><th>Price</th><th>Result</th><th>Targets</th><th>Stock moved</th><th>Option bought</th><th>Option sold</th><th>Option P&amp;L</th><th>R</th><th></th></tr></thead><tbody>`+r.trades.map(t=>`<tr>${dy(t)}<td><b>${esc(t.symbol)}</b></td><td>${pill(t.side,t.right)}</td><td>${t.tf==="5m"?"5 min":"15 min"}</td><td>${why(t.type)}</td><td>${t.entered}</td><td>${t.entry}</td><td>${t.sl}</td><td>${t.t1}</td><td>${t.t4}</td><td>${t.exit_at}</td><td>${t.exit}</td><td>${esc(t.reason)}</td><td>${t.targets}</td><td class="${t.pct>=0?"up":"dn"}">${t.pct>0?"+":""}${t.pct}%</td>${t.opt_pct==null?"<td>–</td><td>–</td><td>–</td>":`<td>${t.opt_in}</td><td>${t.opt_out}</td><td class="${t.opt_pct>=0?"up":"dn"}">${t.opt_pct>0?"+":""}${t.opt_pct}%</td>`}<td>${R(t.r)}</td><td>${cb(t)}</td></tr>`).join("")+"</tbody></table></div>":`<div class="sub">The rules would not have taken any trade on ${esc(r.day)}.</div>`;
 const shown=r.signals.slice(0,300),tot=r.signals_total||r.signals.length;
 const sg=r.signals.length?`<details><summary>All ${tot} signal${tot>1?"s":""} that qualified${tot>shown.length?` (first ${shown.length} shown, the CSV has them all)`:""}</summary><div class="wrap rpscroll"><table><thead><tr>${dh}<th>Stock</th><th>Buy</th><th>Chart</th><th>Why</th><th>Signal candle</th><th>Qualified at</th><th>Price</th><th>Stop</th><th>T1</th><th>T2</th><th>T3</th><th>T4</th><th>Day move</th><th>Taken</th><th></th></tr></thead><tbody>`+shown.map(x=>`<tr class="${x.taken?"":"past"}">${dy(x)}<td><b>${esc(x.symbol)}</b></td><td>${pill(x.side,x.right)}</td><td>${x.tf==="5m"?"5 min":"15 min"}</td><td>${why(x.type)}</td><td>${esc(x.signal_time)}</td><td>${x.at}</td><td>${x.price}</td><td>${x.sl}</td>${x.tps.map(v=>`<td>${v}</td>`).join("")}<td>${x.move==null?"–":(x.move>0?"+":"")+x.move+"%"}</td><td>${x.taken?"Yes":esc(x.why_not||"No")}</td><td>${cb(x)}</td></tr>`).join("")+"</tbody></table></div></details>":"";
 const sk=r.skips.length?`<details><summary>Why other signals were left out</summary><ul>${r.skips.map(([k,n])=>`<li>${esc(k)}: ${n}</li>`).join("")}</ul></details>`:"";
 const head=rg?`<div><b>${esc(r.day)}</b> · ${s.days} trading days<br>${s.trades} trades from ${tot} qualifying signals · ${s.wins} won, ${s.losses} lost, ${s.flat} flat · win rate ${s.win_rate}%</div><div>Total <b>${R(s.total_r)}</b> · average ${R(s.avg_r)} a trade · profit factor ${s.profit_factor==null?"–":s.profit_factor}<br>max drawdown ${s.max_drawdown}R · ${s.green_days} green and ${s.red_days} red days · best day ${R(s.best_day.r)} (${s.best_day.day.slice(5)}), worst ${R(s.worst_day.r)} (${s.worst_day.day.slice(5)})</div>`
  :`<div><b>${r.day}</b> · ${s.trades} trade${s.trades===1?"":"s"} from ${s.signals} qualifying signal${s.signals===1?"":"s"}<br>${s.wins} won, ${s.losses} lost, ${s.flat} flat · win rate ${s.win_rate}%</div><div>Total <b>${R(s.total_r)}</b> · average ${R(s.avg_r)} a trade<br>best ${R(s.best)} · worst ${R(s.worst)}</div>`;
 $(p+"RRes").innerHTML=`<div class="ogsum">${head}</div>${money}<div class="sub">5 min against 15 min, for the trades taken</div>${rpByTf(r)}${rpCharts(sc.curve,sc.days,(v,d)=>d?rpInr(v).replace("₹","₹"):rpInr(v),cap,"Account value, trade by trade")}${tr}${sg}${sk}
 <div class="sub">The stock columns follow the stock or index price. The option prices are estimated, not taken from the exchange: each at-the-money option is priced with Black-Scholes using the stock's own recent volatility, the nearest expiry and the time between entry and exit, so real premiums, spreads and slippage will differ. R is the stop-loss distance (a stop-out is −1R, T4 is +4R), the stock's stop distance. The grade filter is not applied. Daily limit ${r.max_trades} trade${r.max_trades===1?"":"s"} a day, ${r.entries==="both"?"fresh and continuation entries":r.entries==="fresh"?"fresh entries only":"continuation only"}.${r.loaded?` ${r.loaded} stocks loaded`:""}${r.errors&&r.errors.length?`; some failed: ${esc(r.errors.slice(0,2).join("; "))}`:""}. Each Chart button opens that stock with the signal's levels.</div><button class="btn ghost" id="${p}RCsv">Download the signals as CSV</button>`;
 $(p+"RRes").onclick=e=>{const b=e.target.closest("[data-rc]");if(!b)return;const [sy,t,ts]=b.dataset.rc.split("|");openChart(sy,t,+ts)};
 $(p+"RCsv").onclick=()=>{const h=["day","stock","buy","chart","type","signal candle","qualified at","price","stop","T1","T2","T3","T4","day move %","taken","why not","result R","price move %","option bought","option sold","option P&L %","how it ended"],rows=r.signals.map(x=>{const t=rpTrade(r,x);return [x.day||r.day,x.symbol,x.right,x.tf,x.type,x.signal_time,x.at,x.price,x.sl,...x.tps,x.move,x.taken?"yes":"no",x.why_not||"",t?t.r:"",t?t.pct:"",t&&t.opt_in!=null?t.opt_in:"",t&&t.opt_out!=null?t.opt_out:"",t&&t.opt_pct!=null?t.opt_pct:"",t?t.reason:""]});
  const csv=[h,...rows].map(l=>l.map(v=>`"${String(v==null?"":v).replace(/"/g,'""')}"`).join(",")).join("\n"),a=document.createElement("a");a.href=URL.createObjectURL(new Blob([csv],{type:"text/csv"}));a.download=`autopilot_replay_${r.bot}_${r.day.replace(/ /g,"_")}.csv`;a.click()}}
async function atmOption(sy,right,tf,m,what){m.textContent=`Loading the ${sy} option chain…`;
 try{const d=await (await api(`/api/options/chain?symbol=${encodeURIComponent(sy)}&tf=${tf||"15m"}`)).json();
  if(d.error||!d.rows||!d.rows.length){m.textContent=d.error||"No option chain for "+sy+" right now.";return}
  const row=d.rows.reduce((a,r)=>Math.abs(r.strike-d.atm)<Math.abs(a.strike-d.atm)?r:a,d.rows[0]),leg=right==="CE"?row.ce:row.pe;
  lastChain=d;m.textContent="";
  if(what==="ochart"){openOptChart(row.strike);return}
  if(!leg||!leg.ltp){m.textContent=`No price for the ${sy} ${row.strike} ${right} yet, so no order was opened.`;return}
  openTicket({kind:"option",symbol:d.symbol,expiry:d.expiry,strike:row.strike,right,side:"BUY",price:+leg.ltp,label:`${d.symbol} ${row.strike} ${right} ${d.expiry}`})
 }catch(e){m.textContent="Could not load the option chain."}}
async function apOption(b,p,what){b.disabled=true;try{await atmOption(b.dataset.s,b.dataset.r,b.dataset.tf,$(p+"Msg"),what)}finally{b.disabled=false}}
function initBot(){$("botHost").innerHTML=Object.entries(BOTS).map(([p,v])=>botHtml(p,v.title)).join("");
 for(const p in BOTS){rpDays(p);for(const k of ["RCap","RFee"])$(p+k).onchange=()=>{const l=window.rpLast&&window.rpLast[p];if(l)rpShow(p,l)};$(p+"RRun").onclick=()=>rpRun(p);$(p+"Past").onchange=()=>botLoad();$(p+"Sig").onclick=e=>{const b=e.target.closest("[data-ap]");if(!b)return;if(b.dataset.ap==="chart")openChart(b.dataset.s,b.dataset.tf);else apOption(b,p,b.dataset.ap)};$(p+"Save").onclick=()=>botCtl(p,"save");$(p+"Play").onclick=()=>botCtl(p,"play");$(p+"Pause").onclick=()=>botCtl(p,"pause");$(p+"Stop").onclick=()=>botCtl(p,"stop")}}
function confBadge(r){const c=r&&r.conf;if(!c)return "";const tip=`Grade ${c.g}, score ${c.s} of 100\n`+c.w.map(x=>(x[0]?"✓ ":"✗ ")+x[1]).join("\n")+(c.n<8?"\n(Some checks could not be made)":"");return `<span class="cg cg${c.g}" title="${esc(tip)}">${c.g}<small> ${c.s}</small></span>`}
let btTimer=null;
const inr=x=>(x<0?"-":"")+"₹"+Math.abs(Math.round(x)).toLocaleString("en-IN");
function btChart(curve,cap){if(!curve.length)return "<div class='sub'>No trades to draw.</div>";
 const W=800,H=230,D=60,pad=44,t0=curve[0][0],t1=curve[curve.length-1][0]||t0+1;const ys=curve.map(c=>c[1]).concat([cap]);let lo=Math.min(...ys),hi=Math.max(...ys);if(hi===lo){hi+=1;lo-=1}
 const X=t=>pad+(t1===t0?0:(t-t0)/(t1-t0))*(W-pad-8),Y=v=>8+(hi-v)/(hi-lo)*(H-22);
 const pts=curve.map(c=>X(c[0]).toFixed(1)+","+Y(c[1]).toFixed(1)).join(" ");
 const mdd=Math.max(1,...curve.map(c=>c[2]));
 const dd=curve.map(c=>X(c[0]).toFixed(1)+","+(D*(c[2]/mdd)).toFixed(1)).join(" ");
 const d0=new Date(t0*1000).toLocaleDateString("en-IN",{day:"numeric",month:"short"}),d1=new Date(t1*1000).toLocaleDateString("en-IN",{day:"numeric",month:"short",year:"2-digit"});
 return `<svg class="btsvg" viewBox="0 0 ${W} ${H+D+30}" role="img" aria-label="Funds over time and drawdown">
  <polygon class="ar" points="${X(t0)},${Y(lo)} ${pts} ${X(t1)},${Y(lo)}"/><line class="base" x1="${pad}" x2="${W-8}" y1="${Y(cap)}" y2="${Y(cap)}"/><polyline class="eq" points="${pts}"/>
  <text x="2" y="${Y(hi)+8}">${inr(hi)}</text><text x="2" y="${Y(lo)}">${inr(lo)}</text><text x="${pad+4}" y="${Y(cap)-4}">start ${inr(cap)}</text>
  <text x="${pad}" y="${H+4}">${d0}</text><text x="${W-8}" y="${H+4}" text-anchor="end">${d1}</text>
  <g transform="translate(0,${H+12})"><polygon class="dd" points="${pad},0 ${dd} ${W-8},0"/><text x="2" y="12">drawdown</text><text x="2" y="${D}">-${mdd.toFixed(1)}%</text></g></svg>`}
function btPaint(r){$("btOut").hidden=false;
 const k=(l,v,s,c)=>`<div><span class="l">${l}</span><span class="v ${c||""}">${v}</span><span class="s">${s||""}</span></div>`;
 $("btKpi").innerHTML=k("Starting funds",inr(r.capital),"")+k("Ending funds",inr(r.final),r.days?`after ${r.days} days`:"",r.net>=0?"pnlup":"pnldn")+
  k("Growth",(r.growth_pct>=0?"+":"")+r.growth_pct+"%",inr(r.net),r.net>=0?"pnlup":"pnldn")+(r.cagr_pct!=null?k("Per year (if it repeated)",(r.cagr_pct>=0?"+":"")+r.cagr_pct+"%","not a forecast"):"")+
  k("Max drawdown",r.max_dd_pct+"%",inr(-r.max_dd)+" from a peak","pnldn")+k("Win rate",r.win_rate+"%",`${r.trades} trades`)+
  k("Profit factor",r.profit_factor==null?"–":r.profit_factor,"gains ÷ losses")+k("Average win / loss",inr(r.avg_win),"/ "+inr(-r.avg_loss))+
  k("Best / worst trade",inr(r.best),"/ "+inr(r.worst))+k("Longest losing streak",r.losing_streak,"trades in a row")+k("Costs and slippage",inr(r.costs),"already deducted");
 $("btChart").innerHTML=btChart(r.curve,r.capital);
 const notes=[...(r.notes||[]),...(r.skipped||[]).map(x=>`${x.n} trade(s) skipped: ${x.why}.`),...(r.problems||[]),
  `${r.found} trades found in the period${r.trades<r.found?`, ${r.trades} taken with your funds`:""}.`,
  r.trades<30?"Fewer than 30 trades is too few to judge a strategy. Treat this as a first look only.":"",
  r.params&&r.params.kind==="options"?"Option prices are estimated from the index or stock move with an assumed volatility. Real prices, spreads and expiry-day moves will differ.":"",
  r.params&&r.params.kind==="futures"?"Futures are priced as the underlying (no basis or roll cost).":"",
  "Past results do not predict future ones. Orders fill at the signal candle's close, with the slippage you set."].filter(Boolean);
 $("btNotes").innerHTML=notes.map(esc).join("<br>");
 $("btMonths").innerHTML=r.months.length?`<thead><tr><th>Month</th><th>P&amp;L</th><th>% of start</th></tr></thead><tbody>`+r.months.map(m=>`<tr><td>${m.m}</td><td class="${pnlCls(m.pnl)}">${inr(m.pnl)}</td><td class="${pnlCls(m.pnl)}">${m.pct}%</td></tr>`).join("")+"</tbody>":"";
 $("btGradeT").innerHTML=r.by_grade&&r.by_grade.some(g=>g.grade!=="–")?`<thead><tr><th>Grade</th><th>Trades</th><th>Win rate</th><th>P&amp;L</th></tr></thead><tbody>`+r.by_grade.map(m=>`<tr><td><b>${m.grade}</b></td><td>${m.trades}</td><td>${m.win_rate}%</td><td class="${pnlCls(m.pnl)}">${inr(m.pnl)}</td></tr>`).join("")+"</tbody>":"<tbody><tr><td class='empty'>Grades apply to signal strategies.</td></tr></tbody>";
 $("btSymT").innerHTML=r.by_symbol.length?`<thead><tr><th>Symbol</th><th>Trades</th><th>Win rate</th><th>P&amp;L</th></tr></thead><tbody>`+r.by_symbol.map(m=>`<tr><td><b>${esc(m.sym)}</b></td><td>${m.trades}</td><td>${m.win_rate}%</td><td class="${pnlCls(m.pnl)}">${inr(m.pnl)}</td></tr>`).join("")+"</tbody>":"";
 $("btListNote").textContent=r.listed_of>r.list.length?`(latest ${r.list.length} of ${r.listed_of})`:"";
 $("btTrades").innerHTML=r.list.length?`<thead><tr><th>In</th><th>Out</th><th>Instrument</th><th>Side</th><th>Grade</th><th>Qty</th><th>Entry</th><th>Exit</th><th>Why</th><th>P&amp;L</th><th>Funds after</th></tr></thead><tbody>`+r.list.map(t=>`<tr><td>${t.in}</td><td>${t.out}</td><td>${esc(t.label)}</td><td>${t.side}</td><td>${t.grade||"–"}</td><td>${t.qty}</td><td>${f2(t.entry)}</td><td>${f2(t.exit)}</td><td>${esc(t.why)}</td><td class="${pnlCls(t.pnl)}">${inr(t.pnl)}</td><td>${inr(t.equity)}</td></tr>`).join("")+"</tbody>":""}
async function btRun(){clearTimeout(btTimer);
 const b={strategy:$("btStrategy").value,kind:$("btKind").value,symbols:$("btSyms").value,interval:$("btTf").value,days:+$("btDays").value,capital:+$("btCap").value,risk_pct:+$("btRisk").value,target:+$("btTarget").value,
  intraday:$("btIntra").checked,sell_ok:$("btSell").checked,min_grade:$("btGrade").value,leverage:+$("btLev").value,margin_pct:+$("btMargin").value,lot_size:+$("btLot").value||0,iv:+$("btIv").value,dte:+$("btDte").value,
  brokerage:+$("btBrk").value,other_pct:+$("btOth").value,slippage:+$("btSlip").value,entry_from:$("btFrom").value,entry_to:$("btTo").value,gap_max:+$("btGap").value};
 $("btRun").disabled=true;$("btMsg").textContent="";$("btNote").textContent="Starting…";
 const d=await post("/api/backtest",b);if(d.error){$("btMsg").textContent=d.error;$("btRun").disabled=false;$("btNote").textContent="";return}
 const poll=async()=>{try{const g=await (await api("/api/backtest/"+d.id)).json();
  if(g.state==="running"){$("btNote").textContent=g.note||"Working…";btTimer=setTimeout(poll,1200);return}
  $("btRun").disabled=false;$("btNote").textContent="";
  if(g.state==="done")btPaint(g.result);else $("btMsg").textContent=g.error||"The backtest failed."}catch(e){$("btRun").disabled=false;$("btMsg").textContent="Lost contact with the server. Try again."}};
 poll()}
function initBacktest(){$("btRun").onclick=btRun;
 $("btTf").onchange=()=>{const dly=$("btTf").value==="1d";$("btDays").value=dly?730:58;$("btIntra").checked=!dly;$("btIntra").disabled=dly;$("btLev").value=dly?1:5};
 $("btStrategy").onchange=()=>{const g=$("btStrategy").value==="gap";if(g&&$("btTf").value==="1d"){$("btTf").value="5m";$("btTf").onchange()}$("btIntra").disabled=g||$("btTf").value==="1d";if(g)$("btIntra").checked=true}}
function initScanner(){
 $("scTfs").onclick=e=>{const b=e.target.closest("button");if(b){scTf=b.dataset.t;try{localStorage.setItem("scTf",scTf)}catch(x){}scData=null;scPaint();scLoad()}};
 $("scOnly").onchange=()=>{scLoad()};$("scConf").onchange=()=>{scRender()};
 $("scTable").onclick=e=>{const th=e.target.closest("th[data-k]");if(th){const k=th.dataset.k;scSort=scSort.k===k?{k,d:-scSort.d}:{k,d:SC_TEXT.includes(k)?1:-1};try{localStorage.setItem("scSort",JSON.stringify(scSort))}catch(x){}scRender();return}
  const b=e.target.closest("[data-sc]");if(!b)return;const sy=b.dataset.s;
  if(b.dataset.sc==="chart")openChart(sy,scTf);
  else if(b.dataset.sc==="fut")openTicket({kind:"future",symbol:sy,label:sy+" futures, nearest month",side:b.dataset.side,price:+b.dataset.p,tf:scTf});
  else{optOverride=scTf;osym=sy;oexp="";setMode("options");loadChain()}}}
// ── chart popup (TradingView's open-source Lightweight Charts) ──
let lwPromise=null,chSym="",chTf="",chAt=0,chObj=null;
function loadLW(){if(window.LightweightCharts)return Promise.resolve();
 return lwPromise||(lwPromise=new Promise((ok,no)=>{const s=document.createElement("script");s.src="/lw.js";s.onload=ok;s.onerror=()=>{lwPromise=null;no()};document.head.appendChild(s)}))}
function openChart(sym,t,at){chSym=sym;chTf=t||tf||"15m";chAt=at||0;$("chTitle").textContent=sym;$("chTfs").innerHTML=(data?data.intervals:["5m","15m","1d"]).map(x=>`<button data-ct="${x}" aria-pressed="${x===chTf}">${tfName[x]||x}</button>`).join("");
 if(!$("chartDlg").open)$("chartDlg").showModal();drawChart()}
function chTradeBar(d){const last=d.candles[d.candles.length-1].close,idx=d.index,sy=d.symbol,bt=(c,a,t)=>`<button class="${c}" data-ct2="${a}">${t}</button>`;
 $("chTrade").innerHTML=(idx?"":bt("b","eq-BUY","Buy shares")+bt("s","eq-SELL","Sell shares")+bt("b","fut-BUY","Buy futures")+bt("s","fut-SELL","Sell futures"))+bt("b","opt-CE","Buy Call (CE)")+bt("s","opt-PE","Buy Put (PE)")+'<span class="sub">Opens the order ticket to review. Nothing is sent yet.</span>';
 $("chTrade").onclick=async e=>{const b=e.target.closest("button[data-ct2]");if(!b)return;const [k,v]=b.dataset.ct2.split("-");
  if(k==="eq")openTicket({kind:"equity",symbol:sy,label:sy,side:v,price:last});
  else if(k==="fut")openTicket({kind:"future",symbol:sy,label:sy+" futures, nearest month",side:v,price:last,tf:chTf});
  else{b.disabled=true;try{await atmOption(sy,v,chTf,$("chMsg"),"trade")}finally{b.disabled=false}}}}
function initChart(){
 $("chTfs").onclick=e=>{const b=e.target.closest("button");if(b){chTf=b.dataset.ct;chAt=0;$("chTfs").querySelectorAll("button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.ct===chTf));drawChart()}};
 $("chClose").onclick=()=>$("chartDlg").close();
 $("chartDlg").addEventListener("close",()=>{if(chObj){chObj.remove();chObj=null}})}
async function drawChart(){
 const my=chSym+chTf+chAt;$("chMsg").textContent="Loading chart…";$("chLeg").innerHTML="";
 if(chObj){chObj.remove();chObj=null}
 try{await loadLW()}catch(e){$("chMsg").textContent="The chart library did not load. Reload the page and try again.";return}
 let d;try{const r=await api(`/api/chart?symbol=${encodeURIComponent(chSym)}&tf=${chTf}${chAt?'&at='+chAt:''}`);d=await r.json()}catch(e){$("chMsg").textContent="The server did not return chart data. Try again in a moment.";return}
 if(my!==chSym+chTf+chAt)return;
 if(d.error){$("chMsg").textContent=d.error;return}
 $("chMsg").textContent="";
 const css=getComputedStyle(document.documentElement),col=(n,f)=>css.getPropertyValue(n).trim()||f;
 const ink=col("--ink","#222"),line=col("--line","#ddd"),up=col("--buy","#0a7d5a"),dn=col("--sell","#c0392b");
 const ch=LightweightCharts.createChart($("chart"),{autoSize:true,layout:{background:{color:"transparent"},textColor:ink},grid:{vertLines:{color:line},horzLines:{color:line}},rightPriceScale:{borderColor:line},timeScale:{borderColor:line,timeVisible:d.intraday,secondsVisible:false},crosshair:{mode:0}});
 chObj=ch;
 if(d.lines){const sp=ch.addLineSeries({lineWidth:2,priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});sp.setData(d.lines.stop.map(([t,v,dr])=>({time:t,value:v,color:dr===1?up:dn})));
  const ep=ch.addLineSeries({color:ink,lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false,crosshairMarkerVisible:false});ep.setData(d.lines.ema.map(([t,v])=>({time:t,value:v})))}
 const cs=ch.addCandlestickSeries({upColor:up,downColor:dn,borderUpColor:up,borderDownColor:dn,wickUpColor:up,wickDownColor:dn});cs.setData(d.candles);
 cs.setMarkers(d.markers.map(m=>m.side==="BUY"?{time:m.time,position:"belowBar",color:up,shape:"arrowUp",text:"BUY"}:{time:m.time,position:"aboveBar",color:dn,shape:"arrowDown",text:"SELL"}));
 const sg=d.signal,last=d.candles[d.candles.length-1].time,leg=[];
 if(sg){const lv=(price,color,title,style)=>{const s=ch.addLineSeries({color,lineWidth:2,lineStyle:style,priceLineVisible:false,lastValueVisible:true,crosshairMarkerVisible:false,title});s.setData([{time:sg.from,value:price},{time:last,value:price}])};
  lv(sg.entry,ink,"Entry",0);lv(sg.sl,dn,"SL"+(sg.sl_hit?" ✖":""),0);sg.tps.forEach((p,i)=>lv(p,up,"T"+(i+1)+(sg.hits>i?" ✓":""),2));
  leg.unshift(`<b class="${sg.side==="BUY"?"up":"dn"}">${esc(sg.state)}</b> <span>signal candle ${esc(sg.time)}, ${esc(sg.status)}</span>`)}
 else leg.unshift("<span>No signal to show levels for on this timeframe.</span>");
 $("chLeg").innerHTML=leg.join("");chTradeBar(d);
 ch.timeScale().fitContent();if(d.candles.length>120)ch.timeScale().setVisibleLogicalRange({from:d.candles.length-120,to:d.candles.length+4})}
const OC_M={oi:["Open interest","oi","n"],doi:["Change in OI","doi","n"],vol:["Volume","vol","n"],ltp:["Premium","ltp","p"]};
let ocm="oi";try{ocm=localStorage.getItem("ocm")||"oi"}catch(e){}
function drawChain(d){
 const card=$("ochartc");if(!d||d.error||!d.rows||!d.rows.length){card.hidden=true;return}
 card.hidden=false;
 const has=k=>d.rows.some(r=>r.ce[k]!=null||r.pe[k]!=null),ok=Object.keys(OC_M).filter(k=>has(OC_M[k][1]));
 if(!ok.includes(ocm))ocm=ok.includes("oi")?"oi":(ok[0]||"ltp");
 $("ocSeg").innerHTML=Object.entries(OC_M).map(([k,[n]])=>`<button data-m="${k}" aria-pressed="${k===ocm}" ${ok.includes(k)?"":"disabled"}>${n}</button>`).join("");
 $("ocNote").textContent=ok.includes("oi")?"":"Open interest and volume are not available for this chain (the broker's prices only).";
 if(!$("ocOn").checked||!ok.length){$("ocSvg").innerHTML="";return}
 const [name,key,kind]=OC_M[ocm],rows=d.rows,n=rows.length,gw=n>24?30:36,H=250,pl=48,pr=10,pt=22,pb=30,W=pl+n*gw+pr;
 const val=(r,s)=>{const v=r[s][key];return v==null?null:+v};
 const all=rows.flatMap(r=>[val(r,"ce"),val(r,"pe")]).filter(v=>v!=null);
 const mx=Math.max(0,...all),mn=Math.min(0,...all),span=(mx-mn)||1,y=v=>pt+(mx-v)/span*(H-pt-pb);
 const fmt=v=>kind==="p"?f2(v):fk(Math.abs(v)).replace(/^/,v<0?"-":"");
 const step=(()=>{const raw=span/4,p=Math.pow(10,Math.floor(Math.log10(raw||1))),m=raw/p;return (m<=1?1:m<=2?2:m<=5?5:10)*p})();
 let g="";for(let t=Math.ceil(mn/step)*step;t<=mx+1e-9;t+=step)g+=`<line class="ax" x1="${pl}" x2="${W-pr}" y1="${y(t)}" y2="${y(t)}"/><text x="${pl-5}" y="${y(t)+3}" text-anchor="end">${fmt(t)}</text>`;
 const ai=rows.findIndex(r=>r.strike===d.atm);if(ai>=0)g=`<rect class="atmb" x="${pl+ai*gw}" y="${pt}" width="${gw}" height="${H-pt-pb}"/>`+g+`<text x="${pl+ai*gw+gw/2}" y="${pt-8}" text-anchor="middle">ATM</text>`;
 let body="";const bw=(gw-8)/2,zero=y(0);
 if(kind==="p"){
  for(const [s,c] of [["ce","#ff9f43"],["pe","#35c9ff"]]){const pts=rows.map((r,i)=>[pl+i*gw+gw/2,val(r,s)]).filter(p=>p[1]!=null);
   body+=`<polyline fill="none" stroke="${c}" stroke-width="2" points="${pts.map(p=>p[0]+","+y(p[1])).join(" ")}"/>`+pts.map((p,i)=>`<circle cx="${p[0]}" cy="${y(p[1])}" r="2.6" fill="${c}"><title>${rows.find((r,j)=>pl+j*gw+gw/2===p[0]).strike} ${s.toUpperCase()} ${name}: ${f2(p[1])}</title></circle>`).join("")}
 }else{
  rows.forEach((r,i)=>{for(const [s,c,o] of [["ce","#ff9f43",0],["pe","#35c9ff",1]]){const v=val(r,s);if(v==null)continue;const x=pl+i*gw+4+o*bw,yv=y(v);
   body+=`<rect x="${x}" y="${Math.min(yv,zero)}" width="${bw}" height="${Math.max(1,Math.abs(zero-yv))}" fill="${c}" rx="1.5"><title>${r.strike} ${s.toUpperCase()} ${name}: ${v.toLocaleString("en-IN")}</title></rect>`}})
 }
 let xl="";rows.forEach((r,i)=>{if(n>24&&i%2)return;xl+=`<text x="${pl+i*gw+gw/2}" y="${H-pb+14}" text-anchor="middle" ${r.strike===d.atm?'style="font-weight:700"':""}>${r.strike.toLocaleString("en-IN")}</text>`});
 let sp="";const sk=rows.findIndex((r,i)=>i<n-1&&rows[i].strike<=d.underlying&&d.underlying<rows[i+1].strike);
 if(sk>=0){const f=(d.underlying-rows[sk].strike)/(rows[sk+1].strike-rows[sk].strike),x=pl+(sk+f)*gw+gw/2;sp=`<line class="sp" x1="${x}" x2="${x}" y1="${pt}" y2="${H-pb}"/><text x="${Math.min(x+4,W-60)}" y="${pt+10}">Spot ${f2(d.underlying)}</text>`}
 $("ocSvg").innerHTML=`<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(d.symbol)} ${name} by strike, calls and puts">${g}<line class="ax" x1="${pl}" x2="${W-pr}" y1="${zero}" y2="${zero}" style="stroke-width:1.5"/>${body}${sp}${xl}</svg>`}
let og={k:null,tf:"5m",days:1,view:"both",data:null,geom:null};try{const q=JSON.parse(localStorage.getItem("og")||"null");if(q)Object.assign(og,{tf:q.tf||"5m",days:q.days||1,view:q.view||"both"})}catch(e){}
function ogSeg(){const seg=(id,items,cur)=>$(id).innerHTML=items.map(([v,n])=>`<button data-v="${v}" aria-pressed="${String(v)===String(cur)}">${n}</button>`).join("");
 seg("ogView",[["both","Both"],["ce","Call"],["pe","Put"]],og.view);seg("ogTf",[["1m","1 min"],["5m","5 min"],["15m","15 min"]],og.tf);seg("ogDays",[[1,"Latest day"],[3,"3 days"]],og.days)}
async function openOptChart(k){og.k=k;og.data=null;ogSeg();$("ogTitle").textContent=`${lastChain.symbol} ${k.toLocaleString("en-IN")} · ${lastChain.expiry}`;if(!$("optDlg").open)$("optDlg").showModal();await ogLoad()}
async function ogLoad(){const my=++oreq2;$("ogMsg").textContent="Loading prices…";$("ogSvg").innerHTML="";$("ogSum").innerHTML="";
 try{const d=await (await api(`/api/options/history?symbol=${encodeURIComponent(lastChain.symbol)}&expiry=${encodeURIComponent(lastChain.expiry)}&strike=${og.k}&tf=${og.tf}&days=${og.days}`)).json();if(my!==oreq2)return;
  if(d.error){$("ogMsg").textContent=d.error;return}$("ogMsg").textContent="";og.data=d;ogDraw()}catch(e){if(my===oreq2)$("ogMsg").textContent="Cannot reach the server."}}
let oreq2=0;
function indSvg(pk,o){if(!pk)return "";const idx=new Map(o.tl.map((t,i)=>[t,i])),sg=pk.sig;let r="";
 if(o.lines){const seg=[];let cur=null;for(const [t,v,dr] of pk.stop){const i=idx.get(t);if(i==null)continue;if(!cur||cur.dr!==dr){cur={dr,pts:[]};seg.push(cur)}cur.pts.push(`${o.x(i)},${o.y(v)}`)}
  for(const g of seg)if(g.pts.length>1)r+=`<polyline fill="none" stroke="${g.dr===1?"var(--buy)":"var(--sell)"}" stroke-width="1.4" opacity=".8" points="${g.pts.join(" ")}"/>`;
  const ep=pk.ema.map(([t,v])=>{const i=idx.get(t);return i==null?null:`${o.x(i)},${o.y(v)}`}).filter(Boolean);if(ep.length>1)r+=`<polyline fill="none" stroke="var(--mute)" stroke-width="1.2" stroke-dasharray="3 3" points="${ep.join(" ")}"/>`}
 if(sg){const i0=idx.has(sg.t)?idx.get(sg.t):0,buy=sg.side==="BUY";
  if(o.levels){const L=[["Entry",sg.entry,"var(--ink)",""],["SL"+(sg.sl_hit?" ✖":""),sg.sl,"var(--sell)",""],...sg.tps.map((q,k)=>["T"+(k+1)+(sg.hits>k?" ✓":""),q,"var(--buy)","4 3"])];
   for(const [nm,v,c,da] of L){if(v<o.lo||v>o.hi)continue;r+=`<line x1="${o.x(i0)}" x2="${o.W-o.pr}" y1="${o.y(v)}" y2="${o.y(v)}" stroke="${c}" stroke-width="1.2" ${da?`stroke-dasharray="${da}"`:""}/><text x="${o.W-o.pr-3}" y="${o.y(v)-3}" text-anchor="end" fill="${c}">${nm}</text>`}}
  if(sg.visible&&idx.has(sg.t)){const c=o.cand.get(sg.t);if(c){const px=o.x(idx.get(sg.t)),py=buy?o.y(c.l)+4:o.y(c.h)-4,cl=buy?"var(--buy)":"var(--sell)",dy=buy?9:-9;
   r+=`<polygon points="${px},${py} ${px-6},${py+dy} ${px+6},${py+dy}" fill="${cl}"/><text x="${px}" y="${py+(buy?21:-13)}" text-anchor="middle" fill="${cl}" style="font-weight:700">${buy?"BUY":"SELL"}</text>`}}}
 return `<g clip-path="url(#${o.clip})">${r}</g>`}
function ogDraw(){const d=og.data;if(!d)return;og.und=null;const sides=og.view==="both"?["ce","pe"]:[og.view],col={ce:"#ff9f43",pe:"#35c9ff"};
 const maps={};for(const s of ["ce","pe"]){maps[s]=new Map((d[s]||[]).map(c=>[c.t,c]))}
 const tl=[...new Set(sides.flatMap(s=>(d[s]||[]).map(c=>c.t)))].sort((a,b)=>a-b),n=tl.length;if(!n){$("ogMsg").textContent="No candles for this side.";return}
 const U=d.und&&d.und.length?d.und:null,uh=U?170:0,W=Math.max($("ogSvg").clientWidth||700,Math.min(2400,n*(og.view==="both"?5:7)+70)),H0=sides.length===1?330:290,H=H0+uh,pl=64,pr=10,pt=10,pb=26,vh=sides.length===1?60:0,ph=H0-pt-pb-vh;
 const ipk=og.view==="both"?null:(d.ind||{})[og.view],isg=ipk&&ipk.sig,vals=sides.flatMap(s=>(d[s]||[]).flatMap(c=>og.view==="both"?[c.c]:[c.h,c.l])).concat(isg?[isg.entry,isg.sl,isg.tps[0]]:[]),mx=Math.max(...vals),mn=Math.min(...vals),pad=(mx-mn)*.06||1,hi=mx+pad,lo=Math.max(0,mn-pad);
 const y=v=>pt+(hi-v)/(hi-lo)*ph,step=(W-pl-pr)/n,x=i=>pl+(i+.5)*step;
 const tk=(()=>{const raw=(hi-lo)/5,p=Math.pow(10,Math.floor(Math.log10(raw||1))),m=raw/p;return (m<=1?1:m<=2?2:m<=5?5:10)*p})();
 let g="";for(let t=Math.ceil(lo/tk)*tk;t<=hi;t+=tk)g+=`<line class="ax" x1="${pl}" x2="${W-pr}" y1="${y(t)}" y2="${y(t)}"/><text x="${pl-5}" y="${y(t)+3}" text-anchor="end">${f2(t)}</text>`;
 const any=(d.ce&&d.ce.length?d.ce:d.pe)||[],byT=new Map(any.map(c=>[c.t,c]));let xl="",lastD=null,every=Math.max(1,Math.ceil(n/(W>900?12:7)));
 tl.forEach((t,i)=>{const c=maps.ce.get(t)||maps.pe.get(t),nd=c.d!==lastD;if(nd&&lastD!==null)g+=`<line class="ax" x1="${x(i)-step/2}" x2="${x(i)-step/2}" y1="${pt}" y2="${H-pb}" style="stroke-dasharray:2 3"/>`;
  if(nd||i%every===0)xl+=`<text x="${x(i)}" y="${H-pb+13}" text-anchor="middle">${og.days>1&&nd?c.d+" ":""}${c.lbl}</text>`;lastD=c.d});
 let body="";
 if(og.view==="both"){for(const s of sides){const pts=[];tl.forEach((t,i)=>{const c=maps[s].get(t);if(c)pts.push(`${x(i)},${y(c.c)}`)});body+=`<polyline fill="none" stroke="${col[s]}" stroke-width="2" stroke-linejoin="round" points="${pts.join(" ")}"/>`}}
 else{const s=sides[0],vmax=Math.max(1,...(d[s]||[]).map(c=>c.v)),bw=Math.max(1,Math.min(14,step*.7));
  tl.forEach((t,i)=>{const c=maps[s].get(t);if(!c)return;const up=c.c>=c.o,cl=up?"var(--buy)":"var(--sell)",top=y(Math.max(c.o,c.c)),bh=Math.max(1,Math.abs(y(c.o)-y(c.c)));
   body+=`<line x1="${x(i)}" x2="${x(i)}" y1="${y(c.h)}" y2="${y(c.l)}" stroke="${cl}"/><rect x="${x(i)-bw/2}" y="${top}" width="${bw}" height="${bh}" fill="${cl}"/>`
   +`<rect x="${x(i)-bw/2}" y="${H0-pb-c.v/vmax*(vh-8)}" width="${bw}" height="${c.v/vmax*(vh-8)}" fill="${col[s]}" opacity=".45"/>`})}
 const idc=s=>new Map((d[s]||[]).map(c=>[c.t,{h:c.h,l:c.l,c:c.c}]));
 let ind="";
 if(og.view==="both"){for(const s of ["ce","pe"]){const pk=(d.ind||{})[s];if(pk)ind+=indSvg({stop:[],ema:[],sig:pk.sig},{tl,x,y,lo,hi,W,pr,lines:false,levels:false,cand:new Map([...idc(s)].map(([t,c])=>[t,{h:c.c,l:c.c}])),clip:"ogcA"})}}
 else if(ipk)ind=indSvg(ipk,{tl,x,y,lo,hi,W,pr,lines:true,levels:true,cand:idc(og.view),clip:"ogcA"});
 let und="",defs=`<clipPath id="ogcA"><rect x="${pl}" y="${pt}" width="${W-pl-pr}" height="${ph}"/></clipPath>`;
 if(U){const um=new Map(U.map(c=>[c.t,c])),y0=H0-pb+22,ub=H-pb-4,uhh=ub-y0,upk=(d.ind||{}).und,usg=upk&&upk.sig,vals2=tl.map(t=>um.get(t)).filter(Boolean).flatMap(c=>[c.h,c.l]).concat(usg?[usg.entry,usg.sl,usg.tps[0]]:[]);
  if(vals2.length){const mx2=Math.max(...vals2),mn2=Math.min(...vals2),pd2=(mx2-mn2)*.08||1,h2=mx2+pd2,l2=mn2-pd2,uy=v=>y0+(h2-v)/(h2-l2)*uhh,raw=(h2-l2)/4,pw=Math.pow(10,Math.floor(Math.log10(raw||1))),mm=raw/pw,tk2=(mm<=1?1:mm<=2?2:mm<=5?5:10)*pw,bw2=Math.max(1,Math.min(14,step*.7));
   und+=`<text x="${pl}" y="${y0-8}" class="ugl" style="font-weight:700">${esc(d.symbol)} (parent script, same time axis)</text><line class="ax" x1="${pl}" x2="${W-pr}" y1="${y0}" y2="${y0}"/>`;
   for(let t=Math.ceil(l2/tk2)*tk2;t<=h2;t+=tk2)und+=`<line class="ax" x1="${pl}" x2="${W-pr}" y1="${uy(t)}" y2="${uy(t)}"/><text x="${pl-5}" y="${uy(t)+3}" text-anchor="end">${f2(t)}</text>`;
   tl.forEach((t,i)=>{const c=um.get(t);if(!c)return;const cl=c.c>=c.o?"var(--buy)":"var(--sell)";und+=`<line x1="${x(i)}" x2="${x(i)}" y1="${uy(c.h)}" y2="${uy(c.l)}" stroke="${cl}"/><rect x="${x(i)-bw2/2}" y="${uy(Math.max(c.o,c.c))}" width="${bw2}" height="${Math.max(1,Math.abs(uy(c.o)-uy(c.c)))}" fill="${cl}"/>`});
   und+=indSvg(upk,{tl,x,y:uy,lo:l2,hi:h2,W,pr,lines:true,levels:true,cand:new Map(U.map(c=>[c.t,{h:c.h,l:c.l,c:c.c}])),clip:"ogcB"});
   defs+=`<clipPath id="ogcB"><rect x="${pl}" y="${y0}" width="${W-pl-pr}" height="${uhh}"/></clipPath>`;og.und=um}}
 $("ogSvg").innerHTML=`<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(d.symbol)} ${d.strike} price chart with the parent script"><defs>${defs}</defs>${g}${body}${ind}${und}${xl}<line class="xh" id="ogx" x1="0" x2="0" y1="${pt}" y2="${H-pb}" visibility="hidden"/></svg>`;
 og.geom={pl,step,n,tl,maps,W};
 const sm=s=>{const a=d[s]||[];if(!a.length)return `<div><b style="color:${col[s]}">${s==="ce"?"Call":"Put"}</b> no candles</div>`;const o=a[0].o,l=a[a.length-1].c,ch=(l-o)/o*100;
  return `<div><b style="color:${col[s]}">${s==="ce"?"Call (CE)":"Put (PE)"}</b> last <b>${f2(l)}</b> <span class="${ch>=0?"up":"dn"}">${ch>=0?"+":""}${ch.toFixed(1)}%</span><br>open ${f2(o)} · high ${f2(Math.max(...a.map(c=>c.h)))} · low ${f2(Math.min(...a.map(c=>c.l)))} · volume ${fk(a.reduce((q,c)=>q+c.v,0))}</div>`};
 const lastC=s=>{const a=d[s]||[];return a.length?a[a.length-1].c:null},tb=(s,side,right)=>lastC(s)==null?"":`<button class="${side==="BUY"?"b":"s"}" data-ot="${side}-${right}">${side==="BUY"?"Buy":"Sell"} ${right==="CE"?"Call (CE)":"Put (PE)"} ${f2(lastC(s))}</button>`;
 $("ogTrade").innerHTML=(og.view==="pe"?"":tb("ce","BUY","CE")+tb("ce","SELL","CE"))+(og.view==="ce"?"":tb("pe","BUY","PE")+tb("pe","SELL","PE"))+'<span class="sub">Opens the order ticket at this price to review. Nothing is sent yet.</span>';
 const um=U?(()=>{const o=U[0].o,l=U[U.length-1].c,ch=(l-o)/o*100;return `<div><b>${esc(d.symbol)}</b> last <b>${f2(l)}</b> <span class="${ch>=0?"up":"dn"}">${ch>=0?"+":""}${ch.toFixed(2)}%</span><br>open ${f2(o)} · high ${f2(Math.max(...U.map(c=>c.h)))} · low ${f2(Math.min(...U.map(c=>c.l)))}</div>`})():`<div class="sub">${esc(d.und_error||"No chart for the parent script.")}</div>`;
 $("ogSum").innerHTML=(og.view==="pe"?"":sm("ce"))+(og.view==="ce"?"":sm("pe"))+um}
function ogHover(e){const gm=og.geom,sv=$("ogSvg").querySelector("svg");if(!gm||!sv)return;const r=sv.getBoundingClientRect(),px=(e.clientX-r.left)*(gm.W/r.width),i=Math.min(gm.n-1,Math.max(0,Math.floor((px-gm.pl)/gm.step)));
 const t=gm.tl[i],ln=$("ogx");ln.setAttribute("x1",gm.pl+(i+.5)*gm.step);ln.setAttribute("x2",gm.pl+(i+.5)*gm.step);ln.setAttribute("visibility","visible");
 const c=gm.maps.ce.get(t),p=gm.maps.pe.get(t),f=(n,c)=>c?`<b style="color:${n==="CE"?"#ff9f43":"#35c9ff"}">${n}</b> O ${f2(c.o)} H ${f2(c.h)} L ${f2(c.l)} C ${f2(c.c)}`:`<b>${n}</b> –`;
 const any=c||p,uc=og.und&&og.und.get(t);$("ogTip").innerHTML=`${esc(any.d)} ${any.lbl} · `+(og.view==="pe"?"":f("CE",c))+(og.view==="both"?" · ":"")+(og.view==="ce"?"":f("PE",p))+(uc?` · <b>${esc(og.data.symbol)}</b> O ${f2(uc.o)} H ${f2(uc.h)} L ${f2(uc.l)} C ${f2(uc.c)}`:"")}
function initChainGraph(){
 $("ogClose").onclick=()=>$("optDlg").close();
 for(const [id,key] of [["ogView","view"],["ogTf","tf"],["ogDays","days"]])$(id).onclick=e=>{const b=e.target.closest("button[data-v]");if(!b)return;og[key]=key==="days"?+b.dataset.v:b.dataset.v;try{localStorage.setItem("og",JSON.stringify({tf:og.tf,days:og.days,view:og.view}))}catch(x){}ogSeg();if(key==="view"&&og.data)ogDraw();else ogLoad()};
 $("ogSvg").addEventListener("pointermove",ogHover);
 $("ogTrade").onclick=e=>{const b=e.target.closest("button[data-ot]");if(!b||!og.data)return;const [side,right]=b.dataset.ot.split("-"),a=og.data[right.toLowerCase()]||[];if(!a.length)return;
  openTicket({kind:"option",symbol:og.data.symbol,expiry:og.data.expiry,strike:og.k,right,side,price:a[a.length-1].c,label:`${og.data.symbol} ${og.k} ${right} ${og.data.expiry}`})};
$("ocSeg").onclick=e=>{const b=e.target.closest("button[data-m]");if(!b||b.disabled)return;ocm=b.dataset.m;try{localStorage.setItem("ocm",ocm)}catch(x){}drawChain(lastChain)};
 $("ocOn").checked=(()=>{try{return localStorage.getItem("ocOn")!=="0"}catch(e){return true}})();
 $("ocOn").onchange=()=>{try{localStorage.setItem("ocOn",$("ocOn").checked?"1":"0")}catch(x){}drawChain(lastChain)}}
function renderChain(d){
 const box=document.getElementById("onote"),sum=document.getElementById("osum"),tb=document.getElementById("otable");
 if(d.error){sum.hidden=true;tb.innerHTML="";box.textContent=d.error;drawChain(null);return}
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
  if(lv)h+=`<div class="osl"><span>${["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","NIFTYNXT50","SENSEX","BANKEX"].includes(d.symbol)?"Index":"Stock"} levels:</span>${lv}</div>`}
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
  return `<tr class="${atm?"atm":""}">${ch}<td class="k"><button class="kb" data-k="${r.strike}" title="Price charts of the ${r.strike} call and put">${r.strike.toLocaleString("en-IN")} <span>📈</span></button></td>${ph}</tr>`}).join("");
 const cH=more?["OI","Chg OI","Volume","IV","LTP"]:["OI","Chg OI","LTP"],pH=more?["LTP","IV","Volume","Chg OI","OI"]:["LTP","Chg OI","OI"];
 drawChain(d);
 tb.innerHTML=`<thead><tr><th class="g" colspan="${cH.length}">Calls</th><th class="k" rowspan="2">Strike</th><th class="g" colspan="${pH.length}">Puts</th></tr><tr>${cH.map(h=>`<th>${h}</th>`).join("")}${pH.map(h=>`<th>${h}</th>`).join("")}</tr></thead><tbody>${rows}</tbody>`;
 box.textContent=(d.note?d.note+" ":"")+(d.timestamp?`NSE data as of ${d.timestamp}. `:"")+(d.stale?"NSE did not respond just now, so this is the last copy received. ":"")+"Calls are tinted when the stock has a BUY signal and puts when it has a SELL signal. The signal is calculated on the underlying's own candles, not on option prices, because NSE does not provide option candle history. The at-the-money strike is shaded. Click B or S beside a price to buy or sell that option.";
}
setInterval(()=>{if(mode==="options"&&osym)loadChain(true)},60000);
// ── Orders and order ticket ──
let tk=null,tkToken=null,tradeInfo={mode:"paper",caps:{}},lastChain=null;
const $=id=>document.getElementById(id);
const post=async(u,b)=>(await api(u,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(b)})).json();
const ltpBtn=(r,s)=>{const l=r[s.toLowerCase()].ltp;if(l==null)return "–";const d=`data-strike="${r.strike}" data-right="${s}" data-ltp="${l}"`;
 return `<span class="px">${f2(l)}</span><button class="ob b" ${d} data-side="BUY" title="Buy ${s}" aria-label="Buy ${r.strike} ${s}">B</button><button class="ob s" ${d} data-side="SELL" title="Sell ${s}" aria-label="Sell ${r.strike} ${s}">S</button>`};
async function loadTrading(){try{tradeInfo=await (await api("/api/trading")).json();const b=$("banner");b.className="banner "+tradeInfo.mode;paintMode();
 b.textContent=tradeInfo.mode==="live"?`LIVE trading is on. Orders go to your ${(tradeInfo.brokers||[]).map(x=>tradeInfo.broker_names[x]).join(" or ")} account. Limits: ₹${tradeInfo.caps.max_value.toLocaleString("en-IN")} per order and ${tradeInfo.caps.max_orders} orders a day. Futures: up to ${tradeInfo.caps.max_fut_lots} lot(s) and ₹${tradeInfo.caps.max_fut_value.toLocaleString("en-IN")} notional.`:tradeInfo.note}catch(e){}}
let fundsInfo=null,fundsTimer=null;
function fundsBody(){return {kind:tk.kind,symbol:tk.symbol,side:$("tkSide").value,order_type:$("tkType").value,price:$("tkPrice").value===""?null:+$("tkPrice").value,product:$("tkProd").value,broker:$("tkBrokerL").hidden?"":$("tkBroker").value,tf:(tk.tf||(tk.kind==="option"?optTf():tf))||"",expiry:tk.expiry,strike:tk.strike,right:tk.right}}
async function fundsLoad(){fundsInfo=null;const b=$("tkFunds");if(!tradeInfo||tradeInfo.mode!=="live"){b.hidden=true;return}
 b.hidden=false;b.textContent="Reading your broker balance…";
 try{const d=await post("/api/orders/funds",fundsBody());fundsInfo=d;fundsPaint()}catch(e){b.className="tkfunds bad";b.textContent="Could not read your broker balance. Try closing and opening the order again."}}
function fundsPaint(){const b=$("tkFunds"),d=fundsInfo;if(!d||!d.live){b.hidden=true;return}
 b.hidden=false;
 if(d.error){b.className="tkfunds bad";b.textContent=d.error;return}
 if(d.sizing_error){b.className="tkfunds";b.innerHTML=`<div><span>${esc(d.broker)} balance</span><b>${inr0(d.balance)}</b></div><p class="sub">How much you can buy could not be worked out: ${esc(d.sizing_error)}</p>`;return}
 const q=Math.max(0,+$("tkQty").value||0),need=q*d.per_qty,left=d.balance-need,u=d.unit+(d.max_qty===1?"":"s");
 b.className="tkfunds"+(need>d.balance||q>d.max_qty?" bad":"");
 b.innerHTML=`<div><span>${esc(d.broker)} balance</span><b>${inr0(d.balance)}</b></div><div><span>This order needs about</span><b>${inr0(need)}</b></div>`+
  `<div><span>${need>d.balance?"Short by":"Left after"}</span><b>${inr0(Math.abs(left))}</b></div><div><span>You can buy up to</span><b>${d.max_qty} ${u} <button class="mini" id="tkMax" type="button">Use max</button></b></div>`+
  `${q>d.max_qty&&need<=d.balance?`<p class="sub" style="color:var(--sell)">This is above your per-order limit.</p>`:""}<p class="sub">Estimated from ${esc(d.basis)}, ${inr0(d.per_qty)} per ${d.unit}. Your broker makes the final call.</p>`}
const inr0=v=>"₹"+Number(v).toLocaleString("en-IN",{maximumFractionDigits:0});
function resetReview(){tkToken=null;$("tkReview").hidden=true;$("tkConfirm").hidden=true;$("tkPlace").hidden=true;$("tkReviewBtn").hidden=false;$("tkMsg").textContent=""}
function openTicket(s){tk=s;const opt=s.kind==="option",fut=s.kind==="future";
 $("tkTitle").textContent=opt?"Option order":fut?"Futures order":"Order ticket";$("tkInst").value=s.label;
 $("tkSide").innerHTML="<option>BUY</option><option>SELL</option>";$("tkSide").value=s.side;
 $("tkQtyT").textContent=opt||fut?"Lots":"Quantity (shares)";$("tkQty").value=1;
 $("tkType").innerHTML=opt?'<option value="LIMIT">Limit</option><option value="MARKET">Market</option>':'<option value="MARKET">Market</option><option value="LIMIT">Limit</option>';
 $("tkPrice").value=s.price??"";$("tkPrice").disabled=$("tkType").value!=="LIMIT";
 $("tkProd").innerHTML=opt||fut?'<option value="INTRADAY">Intraday</option><option value="CARRYFORWARD">Carry forward</option>':'<option value="INTRADAY">Intraday</option><option value="DELIVERY">Delivery</option>';
 $("tkMode").textContent=tradeInfo.mode==="live"?"LIVE: this sends a real order to your broker.":"Paper mode: this order is only simulated.";
 const bl=tradeInfo.brokers||[];$("tkBroker").innerHTML=bl.map(x=>`<option value="${x}">${tradeInfo.broker_names[x]}</option>`).join("");
 $("tkBrokerL").hidden=bl.length<2;if(tradeInfo.default_broker&&bl.length)$("tkBroker").value=tradeInfo.default_broker;
 $("tkAutoBox").hidden=opt;$("tkAuto").checked=!opt;$("tkTrail").checked=false;$("tkTrailL").hidden=false;
 $("tkPin").value="";resetReview();$("ticket").showModal();fundsLoad()}
["tkSide","tkQty","tkType","tkPrice","tkProd","tkBroker","tkAuto","tkTrail"].forEach(id=>{$(id).addEventListener("input",resetReview);$(id).addEventListener("change",resetReview)});
$("tkQty").addEventListener("input",fundsPaint);
["tkSide","tkType","tkPrice","tkProd","tkBroker"].forEach(id=>$(id).addEventListener("change",()=>{clearTimeout(fundsTimer);fundsTimer=setTimeout(fundsLoad,300)}));
$("tkFunds").onclick=e=>{if(e.target.id==="tkMax"&&fundsInfo&&fundsInfo.max_qty!=null){$("tkQty").value=Math.max(1,fundsInfo.max_qty);resetReview();fundsPaint()}};
$("tkType").addEventListener("change",()=>{$("tkPrice").disabled=$("tkType").value!=="LIMIT"});
$("tkClose").onclick=()=>$("ticket").close();
initChart();initFlash();initModes();initScanner();initBot();initBacktest();initConf();initInstall();initChainGraph();
$("tkReviewBtn").onclick=async()=>{
 const body={kind:tk.kind,symbol:tk.symbol,side:$("tkSide").value,qty:+$("tkQty").value,order_type:$("tkType").value,price:$("tkPrice").value===""?null:+$("tkPrice").value,product:$("tkProd").value,broker:$("tkBrokerL").hidden?"":$("tkBroker").value,tf:(tk.tf||(tk.kind==="option"?optTf():tf))||"",expiry:tk.expiry,strike:tk.strike,right:tk.right,auto_sl:!$("tkAutoBox").hidden&&$("tkAuto").checked,trail:!$("tkAutoBox").hidden&&$("tkAuto").checked&&$("tkTrail").checked};
 $("tkMsg").textContent="Checking the order…";
 const d=await post("/api/orders/preview",body);
 if(d.error){$("tkMsg").textContent=d.error;return}
 tkToken=d.token;$("tkReview").hidden=false;
 $("tkReview").innerHTML=d.lines.map(l=>`<div><span>${esc(l[0])}</span><b>${esc(l[1])}</b></div>`).join("")+d.warnings.map(w=>`<p class="warn">${esc(w)}</p>`).join("");
 $("tkConfirm").hidden=d.mode!=="live";$("tkPinL").hidden=d.mode!=="live";$("tkPlace").hidden=false;$("tkReviewBtn").hidden=true;
 $("tkMsg").textContent=(d.mode==="live"?"Check the details, enter your PIN and place the order. This review is valid for 90 seconds.":"Check the details and place the order. This review is valid for 90 seconds.")};
$("tkPlace").onclick=async()=>{const b=$("tkPlace");b.disabled=true;
 try{const d=await post("/api/orders/confirm",{token:tkToken,pin:$("tkPin").value});
  if(d.error){$("tkMsg").textContent=d.error;return}
  $("tkMsg").textContent=`Order sent. ID ${d.order.id}, status ${d.order.status}.`+(d.order.sl?" Place the stop-loss from the Portfolio tab once it is filled.":"");
  tkToken=null;$("tkPlace").hidden=true;$("tkConfirm").hidden=true;$("tkPin").value="";loadOrders()}
 finally{b.disabled=false}};
$("body").onclick=e=>{const cb=e.target.closest("[data-chart]");if(cb){openChart(cb.dataset.chart);return}
 const b=e.target.closest("[data-trade]");if(!b||!data)return;const r=data.rows.find(x=>x.symbol===b.dataset.trade);if(!r)return;
 const side=r.signal.endsWith("SELL")?"SELL":"BUY";
 openTicket(mode==="futures"?{kind:"future",symbol:r.symbol,label:r.symbol+" futures, nearest month",side,price:r.close}:{kind:"equity",symbol:r.symbol,label:r.symbol,side,price:r.close})};
$("otable").onclick=e=>{const kb=e.target.closest(".kb");if(kb&&lastChain){openOptChart(+kb.dataset.k);return}
 const b=e.target.closest(".ob");if(!b||!lastChain)return;
 openTicket({kind:"option",symbol:lastChain.symbol,expiry:lastChain.expiry,strike:+b.dataset.strike,right:b.dataset.right,side:b.dataset.side,price:+b.dataset.ltp,label:`${lastChain.symbol} ${+b.dataset.strike} ${b.dataset.right} ${lastChain.expiry}`})};
$("chk").onclick=async()=>{$("chkMsg").textContent="Checking…";const bs=(tradeInfo.brokers||[]).length?tradeInfo.brokers:[""];const out=[];
 try{for(const b of bs){out.push((await (await api("/api/trading/check?which="+b)).json()).message)}$("chkMsg").textContent=out.join(" ")}catch(e){$("chkMsg").textContent="Cannot reach the server."}};
const cancelable=new Set(["open","trigger pending","open pending","validation pending","put order req received","modified","after market order req received"]);
async function loadOrders(){try{renderOrders(await (await api("/api/orders")).json())}catch(e){}loadPortfolio()}
const pnlCls=v=>v>0?"pnlup":v<0?"pnldn":"";const money=v=>(v<0?"-":v>0?"+":"")+"₹"+Math.abs(v).toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
async function loadPortfolio(){try{const d=await (await api("/api/portfolio")).json(),t=d.totals;
 const bal=(d.funds||[]).map(f=>`<div><span class="l">${esc(f.broker)} balance</span><span class="v">${f.balance==null?"–":"₹"+Number(f.balance).toLocaleString("en-IN",{maximumFractionDigits:0})}</span></div>`).join("");
 const open=d.positions.filter(r=>r.qty).length;
 $("pfKpi").innerHTML=`<div><span class="l">Today's P&amp;L</span><span class="v ${pnlCls(t.pnl)}">${money(t.pnl)}</span><span class="s">${d.mode==="live"?"From your broker":"Paper, simulated"}</span></div>`+
  `<div><span class="l">Unrealised (open)</span><span class="v ${pnlCls(t.unrealized)}">${money(t.unrealized)}</span><span class="s">${open} open position${open===1?"":"s"}</span></div>`+
  `<div><span class="l">Realised (closed)</span><span class="v ${pnlCls(t.realized)}">${money(t.realized)}</span></div>`+bal;
 $("pfNote").textContent=(d.errors||[]).join(" ")||(d.mode==="live"?"Live positions and profit from your broker. Refreshes every few seconds.":"Paper positions are worked out from your simulated orders, priced at the latest price on this dashboard. Option positions are priced from the option chain, a few seconds behind.");
 $("pfTable").innerHTML=d.positions.length?`<thead><tr><th>Instrument</th><th>Side</th><th>Net qty</th><th>Avg price</th><th>Last price</th><th>Unrealised</th><th>Realised</th><th>Total P&amp;L</th></tr></thead><tbody>`+d.positions.map(r=>`<tr><td>${esc(r.label)}${r.broker&&r.broker!=="Paper"?`<span class="tag">${esc(r.broker)}</span>`:""}</td><td>${r.qty>0?"LONG":r.qty<0?"SHORT":"Closed"}</td><td>${r.qty}</td><td>${r.avg?f2(r.avg):"–"}</td><td>${r.ltp?f2(r.ltp):"–"}</td><td class="${pnlCls(r.unrealized)}">${money(r.unrealized)}</td><td class="${pnlCls(r.realized)}">${money(r.realized)}</td><td class="${pnlCls(r.pnl)}"><b>${money(r.pnl)}</b></td></tr>`).join("")+"</tbody>":`<tbody><tr><td class="empty">No positions today.</td></tr></tbody>`}catch(e){}}
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
setInterval(()=>{if(mode==="orders")loadOrders()},8000);
loadTrading();
// ── Account and admin ──
const send=async(m,u,b)=>(await api(u,{method:m,headers:{"Content-Type":"application/json"},body:b===undefined?undefined:JSON.stringify(b)})).json();
async function loadMe(){const d=await (await api("/api/me")).json();me=d;csrf=d.csrf;
 $("dpBanner").hidden=!d.default_password;$("who").textContent=d.user.username+(d.user.role==="admin"?" (admin)":"")+(d.build?" · build "+d.build:"");$("navAdmin").hidden=d.user.role!=="admin";renderAccount(d)}
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
async function loadSession(){try{const d=await (await api("/api/admin/session")).json();if(d.error)return;$("ssNote").textContent=(d.override?`Special session open today ${d.override.start} to ${d.override.end}. `:"")+(d.env.length?`From settings: ${d.env.join(", ")}. `:"")+(d.open_now?"The market counts as open right now.":"The market counts as closed right now.")}catch(e){}}
async function loadSource(){try{const d=await (await api("/api/admin/source")).json();if(d.error){$("srcNote").textContent=d.error;return}
 const sm={connected:"connected",idle:"waiting for the market",closed:"reconnecting",error:"error",off:"off"};
 $("srcNote").innerHTML=`Now using <b>${d.source==="dhan"?"Dhan":"Yahoo"}</b>`+(d.dhan_available?(`. Live stream: ${sm[d.stream]||esc(d.stream)}`+(d.subscribed?`, ${d.subscribed} instruments`:"")+(d.age!=null&&d.healthy?`, last price ${d.age}s ago`:"")+(d.stream_error?`. <span style="color:var(--sell)">${esc(d.stream_error)}</span>`:"")+(d.hist_error?`. <span style="color:var(--sell)">Dhan history: ${esc(d.hist_error)}</span>`:"")+(d.clock?`. Candle clock: ${esc(d.clock)}`:"")+(d.fallback?". Yahoo fills any gap.":"")+tokTxt(d.token)):". Dhan is not set up on this server.")}catch(e){}}
function tokTxt(t){if(!t||!t.enabled)return "";const h=s=>s==null?"":s>=3600?Math.floor(s/3600)+"h "+Math.floor(s%3600/60)+"m":Math.max(0,Math.floor(s/60))+"m";
 return `. Dhan token: ${t.expires_in==null?"expiry unknown":t.expires_in>0?"valid for "+h(t.expires_in):'<span style="color:var(--sell)">expired</span>'}, ${t.auto?"signs in by itself every day at "+esc(t.at)+" IST":"extended automatically (add DHAN_PIN and DHAN_TOTP_SECRET to sign in by itself)"}${t.how&&t.last_ok_age!=null?`, last ${esc(t.how)} ${h(t.last_ok_age)} ago`:""}${t.error?`. <span style="color:var(--sell)">${esc(t.error)}</span>`:""} <button class="btn" onclick="tokNow(this)">Renew now</button>`}
async function tokNow(b){b.disabled=true;b.textContent="Renewing…";const d=await post("/api/admin/dhan-token",{});if(d.error)$("srcNote").textContent=d.error;else loadSource()}
async function srcSet(w){const d=await post("/api/admin/source",{source:w});if(d.error)$("srcNote").textContent=d.error;else loadSource()}
async function ssSet(b){const d=await post("/api/admin/session",b);if(d.error)$("ssNote").textContent=d.error;else loadSession()}
async function loadUsers(){loadSession();loadSource();loadOverview();const d=await (await api("/api/admin/users")).json();if(d.error){$("admNote").textContent=d.error;return}users=d.users;renderUsers()}
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
$("ssOpen").onclick=()=>ssSet({start:$("ssFrom").value,end:$("ssTo").value});$("ssClear").onclick=()=>ssSet({clear:true});$("srcDhan").onclick=()=>srcSet("dhan");$("srcYahoo").onclick=()=>srcSet("yahoo");
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
 $("hNote").textContent=!d.scanned?(d.progress?`Scanning ${d.progress[0]} of ${d.progress[1]} symbols…`+(d.step?` (now ${d.step})`:""):"The first scan is running. This page updates on its own."):
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
