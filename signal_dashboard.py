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
from fastapi import FastAPI, Request, Response
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

    base = {"symbol": symbol, "close": round(float(c[-1]), 2), "signal": "–", "entry": None, "sl": None,
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

    tickers = [s + ".NS" for s in SYMBOLS]
    data = yf.download(tickers, period=PERIODS[interval], interval=interval, group_by="ticker",
                       auto_adjust=False, threads=True, progress=False)
    rows, errors = [], []
    for s in SYMBOLS:
        try:
            row = analyse(s, data[s + ".NS"].dropna(how="all"), interval)
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
        raw = nse.get_json("/api/option-chain-v3", {"type": "Equity", "symbol": symbol, "expiry": expiry})
        parsed = parse_chain(raw, expiry) if raw else None
    if not parsed:  # older endpoint as a fallback
        raw = nse.get_json("/api/option-chain-equities", {"symbol": symbol})
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
@media(max-width:640px){.tally{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main>
<header>
 <div><h1>Signal board</h1><div class="sub" id="meta">Loading first scan…</div></div>
 <div class="ctl"><div class="seg" id="tfs" role="group" aria-label="Timeframe"></div><button class="btn" id="refresh">Refresh now</button></div>
</header>
<nav class="mode" id="mode" aria-label="View"><button data-m="stocks">Stocks</button><button data-m="options">Options</button></nav>
<section id="view-stocks">
<div class="tally" id="tally"></div>
<div class="bar"><input id="q" placeholder="Search symbol" aria-label="Search symbol"></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table></div>
<div class="note" id="note"></div>
</section>
<section id="view-options" hidden>
<div class="obar">
 <input id="osym" list="fno" placeholder="F&amp;O stock symbol, e.g. RELIANCE" aria-label="Stock symbol" autocomplete="off">
 <datalist id="fno"></datalist>
 <button class="btn" id="oload">Load chain</button>
</div>
<div class="chips" id="chips"></div>
<div class="osum" id="osum" hidden></div>
<div class="wrap"><table class="otab" id="otable"></table></div>
<div class="note" id="onote">Pick a stock with a signal above, or type any F&amp;O stock symbol.</div>
</section>
</main>
<script>
const cols=[["symbol","Symbol"],["signal","Signal"],["close","Close"],["entry","Entry"],["sl","SL"],["tp1","T1"],["tp2","T2"],["tp3","T3"],["tp4","T4"],["status","Status"],["signal_time","Signal candle"]];
let data=null,filter="ALL",sortKey="signal",sortDir=1,q="",timer=null,tf=null;
try{tf=localStorage.getItem("tf")}catch(e){}
const tfName={"5m":"5 min","15m":"15 min","1d":"Daily"};
const rank={BUY:0,SELL:1,"Open BUY":2,"Open SELL":2,"–":3};
const fmt=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
document.getElementById("head").innerHTML=cols.map(c=>`<th data-k="${c[0]}">${c[1]}</th>`).join("");
document.getElementById("head").onclick=e=>{const k=e.target.dataset.k;if(!k)return;sortDir=sortKey===k?-sortDir:1;sortKey=k;render()};
document.getElementById("q").oninput=e=>{q=e.target.value.trim().toUpperCase();render()};
document.getElementById("refresh").onclick=async()=>{const b=document.getElementById("refresh");b.disabled=true;await fetch("/api/refresh?tf="+(tf||""),{method:"POST"});setTimeout(()=>{load();b.disabled=false},4000)};
document.getElementById("tfs").onclick=e=>{const b=e.target.closest("button");if(!b||b.dataset.tf===tf)return;
 tf=b.dataset.tf;try{localStorage.setItem("tf",tf)}catch(x){}
 data=null;document.getElementById("body").innerHTML=`<tr><td colspan="11" class="empty">Scanning ${tfName[tf]} candles…</td></tr>`;
 document.querySelectorAll("#tfs button").forEach(x=>x.setAttribute("aria-pressed",x.dataset.tf===tf));load();if(osym)loadChain()};
async function load(){clearTimeout(timer);const asked=tf;
 try{const d=await (await fetch("/api/signals"+(asked?"?tf="+asked:""))).json();if(asked!==tf)return;data=d;tf=d.interval;render();
  timer=setTimeout(load,(d.refreshing||!d.updated)?3000:20000)}
 catch(e){document.getElementById("meta").textContent="Cannot reach the server.";timer=setTimeout(load,10000)}}
const isOpen=r=>r.signal.startsWith("Open");
function render(){
 if(!data)return;
 document.getElementById("tfs").innerHTML=data.intervals.map(x=>`<button data-tf="${x}" aria-pressed="${x===tf}">${tfName[x]||x}</button>`).join("");
 renderChips();
 const rows=data.rows,cnt=s=>rows.filter(r=>r.signal===s).length;
 const tabs=[["ALL","All symbols",rows.length,""],["BUY","Buy",cnt("BUY"),"buy"],["SELL","Sell",cnt("SELL"),"sell"],["OPEN","Open trades",rows.filter(isOpen).length,""]];
 const t=document.getElementById("tally");
 t.innerHTML=tabs.map(x=>`<button class="${x[3]}" data-f="${x[0]}" aria-pressed="${filter===x[0]}"><b>${x[2]}</b><span>${x[1]}</span></button>`).join("");
 t.onclick=e=>{const b=e.target.closest("button");if(b){filter=b.dataset.f;render()}};
 document.getElementById("meta").textContent=(data.updated?`Updated ${data.updated} · `:"")+`${tfName[data.interval]} candles · ${data.settings}`+(data.refreshing?" · scanning…":"");
 let v=rows.filter(r=>(filter==="ALL"||(filter==="OPEN"?isOpen(r):r.signal===filter))&&(!q||r.symbol.includes(q)));
 v.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];if(sortKey==="signal"){x=rank[x];y=rank[y];if(x===y){x=a.bars_ago??999;y=b.bars_ago??999}}
  if(x==null)x=1e9;if(y==null)y=1e9;return (x>y?1:x<y?-1:0)*sortDir});
 const tp=(r,k)=>`<td class="${r.hits>=k?"up":""}">${fmt(r["tp"+k])}${r.hits>=k?" ✓":""}</td>`;
 document.getElementById("body").innerHTML=v.length?v.map(r=>`<tr><td><b>${r.symbol}</b></td><td><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span></td><td>${fmt(r.close)}</td><td>${fmt(r.entry)}</td><td class="${r.sl_hit?"dn":""}">${fmt(r.sl)}${r.sl_hit?" ✖":""}</td>${tp(r,1)}${tp(r,2)}${tp(r,3)}${tp(r,4)}<td>${r.status}</td><td>${r.signal_time}</td></tr>`).join(""):`<tr><td colspan="11" class="empty">${data.updated?"No symbols match this filter.":"First scan is running. This page updates on its own."}</td></tr>`;
 document.getElementById("note").textContent=(data.errors.length?`${data.errors.length} symbol(s) skipped: ${data.errors.slice(0,3).join("; ")}${data.errors.length>3?"…":""}. `:"")+`BUY and SELL show for ${data.fresh_bars} candles after a signal. Open trades are older signals whose stop and T4 have not been reached. Entry, SL and targets come from the latest signal for each symbol.`;
}
// ── Options tab ──
let mode="stocks",osym="",oexp="",oreq=0;
try{mode=localStorage.getItem("mode")||"stocks"}catch(e){}
const esc=s=>s.replace(/&/g,"&amp;");
const fk=n=>{if(n==null)return "–";const a=Math.abs(n),g=n<0?"-":"";return g+(a>=1e7?(a/1e7).toFixed(2)+" Cr":a>=1e5?(a/1e5).toFixed(2)+" L":a>=1e3?(a/1e3).toFixed(1)+" K":String(Math.round(a)))};
const f2=n=>n==null?"–":n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
const sgn=n=>n==null?"–":`<span class="${n>=0?"up":"dn"}">${n>0?"+":""}${fk(n)}</span>`;
function setMode(m){mode=m;try{localStorage.setItem("mode",m)}catch(e){}
 document.getElementById("view-stocks").hidden=m!=="stocks";document.getElementById("view-options").hidden=m!=="options";
 document.querySelectorAll("#mode button").forEach(b=>b.setAttribute("aria-pressed",b.dataset.m===m));if(m==="options")renderChips()}
document.getElementById("mode").onclick=e=>{const b=e.target.closest("button");if(b)setMode(b.dataset.m)};
function renderChips(){
 if(!data)return;
 const dl=document.getElementById("fno");if(!dl.children.length&&data.symbols)dl.innerHTML=data.symbols.map(s=>`<option value="${esc(s)}">`).join("");
 const sig=data.rows.filter(r=>r.signal!=="–").sort((a,b)=>rank[a.signal]-rank[b.signal]||(a.bars_ago??999)-(b.bars_ago??999));
 document.getElementById("chips").innerHTML=sig.length?sig.map(r=>`<button class="chip" data-s="${esc(r.symbol)}" aria-pressed="${r.symbol===osym}"><b>${esc(r.symbol)}</b><span class="pill ${r.signal.replace(" ","_")}">${r.signal}</span></button>`).join(""):`<span class="sub">No stocks have a signal on this timeframe. Type any F&amp;O symbol to see its chain.</span>`;
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
 oexp=d.expiry;const sg=d.signal||"–";
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
  const ce=[fk(r.ce.oi),sgn(r.ce.doi),fk(r.ce.vol),iv(r.ce),f2(r.ce.ltp)].map((c,i)=>`<td class="${cls(d.bias==="CE"&&"bias-ce",pickCe(atm,i))}">${c}</td>`).join("");
  const pe=[f2(r.pe.ltp),iv(r.pe),fk(r.pe.vol),sgn(r.pe.doi),fk(r.pe.oi)].map((c,i)=>`<td class="${cls(d.bias==="PE"&&"bias-pe",pickPe(atm,i))}">${c}</td>`).join("");
  return `<tr class="${atm?"atm":""}">${ce}<td class="k">${r.strike.toLocaleString("en-IN")}</td>${pe}</tr>`}).join("");
 tb.innerHTML=`<thead><tr><th class="g" colspan="5">Calls</th><th class="k" rowspan="2">Strike</th><th class="g" colspan="5">Puts</th></tr><tr><th>OI</th><th>Change in OI</th><th>Volume</th><th>IV</th><th>LTP</th><th>LTP</th><th>IV</th><th>Volume</th><th>Change in OI</th><th>OI</th></tr></thead><tbody>${rows}</tbody>`;
 box.textContent=(d.timestamp?`NSE data as of ${d.timestamp}. `:"")+(d.stale?"NSE did not respond just now, so this is the last copy received. ":"")+"Calls are tinted when the stock has a BUY signal and puts when it has a SELL signal. The indicator runs on the stock's own candles, not on option prices, because NSE does not provide option candle history. The at-the-money strike is shaded.";
}
setInterval(()=>{if(mode==="options"&&osym)loadChain(true)},60000);
setMode(mode);
load();
</script></body></html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
