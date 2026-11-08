#!/usr/bin/env python3
"""Backtest of the NIFTY gap-continuation rule used by SignalDesk's autopilot.

Run next to signal_dashboard.py, on a machine with internet:
    pip install yfinance pandas numpy
    python nifty_backtest.py                 # last ~58 days of ^NSEI 5 min candles from Yahoo
    python nifty_backtest.py --csv my.csv    # your own 5 min data: Datetime,Open,High,Low,Close (IST)
Options: --lot 75 --delta 0.5 --cost 120 --theta 0   (cost = rupees per round trip, theta = option points lost per hour)

It replays the SAME functions the live autopilot uses (analyse, nifty_inputs, nifty_plan), so the numbers
describe the real rule. Option P&L is an ESTIMATE (underlying move x delta x lot, minus costs): real option
prices, spreads and slippage will differ. Yahoo only keeps ~60 days of 5 min data, so the sample is small.
"""
import os, re, sys, argparse, warnings
warnings.filterwarnings("ignore")
os.environ.setdefault("ADMIN_PASSWORD", "backtest-only-12345")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import signal_dashboard as sd
from datetime import time as T


def load(csv=None):
    if csv:
        df = pd.read_csv(csv, parse_dates=[0], index_col=0)
    else:
        import yfinance as yf
        df = yf.download("^NSEI", period="58d", interval="5m", auto_adjust=False, progress=False)
        if df is None or df.empty:
            sys.exit("No data from Yahoo. Try --csv.")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close"]].dropna()
    # keep the index timezone-aware (India time), exactly as the live autopilot sees it
    df.index = df.index.tz_localize("Asia/Kolkata") if df.index.tz is None else df.index.tz_convert("Asia/Kolkata")
    return df.sort_index()


def _k(w):
    return re.sub(r'[-+]?\d+(\.\d+)?', '#', w)


def run(df, lot=75, delta=0.5, cost=120.0, theta=0.0, last_exit=T(15, 15)):
    trades, skips = [], {}
    days = sorted(set(df.index.date))
    for d in days[1:]:
        part = df[df.index.date <= d]
        side, prev, first, why = sd.nifty_inputs(part, d)
        if why:
            skips[_k(why)] = skips.get(_k(why), 0) + 1
            continue
        plan, why = sd.nifty_plan(side, prev, first)
        if not plan:
            skips[_k(why)] = skips.get(_k(why), 0) + 1
            continue
        buy = plan["side"] == "BUY"
        day = df[df.index.date == d]
        bars = day.iloc[1:]          # entry at the close of the 9:15 candle, managed from the next candle
        exit_px, reason, exit_t = None, "time", None
        for t, r in bars.iterrows():
            if t.time() >= last_exit:
                exit_px, exit_t = float(r["Open"]), t
                break
            hit_sl = r["Low"] <= plan["sl"] if buy else r["High"] >= plan["sl"]
            hit_tp = r["High"] >= plan["tp2"] if buy else r["Low"] <= plan["tp2"]
            if hit_sl:                               # stop first when both are inside one candle
                exit_px, reason, exit_t = plan["sl"], "stop", t
                break
            if hit_tp:
                exit_px, reason, exit_t = plan["tp2"], "target", t
                break
        if exit_px is None:
            exit_px, exit_t = float(bars["Close"].iloc[-1]), bars.index[-1]
        move = (exit_px - plan["entry"]) * (1 if buy else -1)
        risk = abs(plan["entry"] - plan["sl"])
        hours = (exit_t - day.index[0]).total_seconds() / 3600
        opt_pts = move * delta - theta * hours
        pnl = opt_pts * lot - cost
        trades.append(dict(date=d, side=plan["side"], entry=round(plan["entry"], 1), sl=round(plan["sl"], 1),
                           target=round(plan["tp2"], 1), exit=round(exit_px, 1), reason=reason,
                           index_pts=round(move, 1), R=round(move / risk, 2) if risk else 0, pnl=round(pnl)))
    return pd.DataFrame(trades), skips


def report(t, skips, days):
    print(f"\nDays tested: {days}   Trades: {len(t)}   Skipped: {sum(skips.values())}")
    for k, v in sorted(skips.items(), key=lambda x: -x[1]):
        print(f"   skipped {v:3d}  {k}")
    if t.empty:
        print("No trades."); return
    w, l = t[t.pnl > 0], t[t.pnl <= 0]
    eq = t.pnl.cumsum(); dd = (eq - eq.cummax()).min()
    streak = best = 0
    for p in t.pnl:
        streak = streak + 1 if p <= 0 else 0; best = max(best, streak)
    print(f"\nWin rate        {len(w)/len(t)*100:5.1f}%   ({len(w)} wins / {len(l)} losses)")
    print(f"Avg win / loss  {w.pnl.mean() if len(w) else 0:8.0f} / {l.pnl.mean() if len(l) else 0:8.0f}  rupees")
    print(f"Avg R multiple  {t.R.mean():5.2f}   (index points / planned risk)")
    print(f"Total P&L       {t.pnl.sum():8.0f}  rupees (1 lot, estimated)")
    print(f"Expectancy      {t.pnl.mean():8.0f}  rupees per trade")
    print(f"Max drawdown    {dd:8.0f}  rupees   Longest losing streak: {best}")
    print("Exits           " + ", ".join(f"{k} {v}" for k, v in t.reason.value_counts().items()))
    print("\nLast trades:\n" + t.tail(15).to_string(index=False))
    print("\nCAUTION: small sample, option prices estimated, past results do not predict future ones.")
    if len(t) < 100:
        print(f"With only {len(t)} trades the win rate could easily be off by +/-10 points or more. "
              "Paper-trade the autopilot for 4-6 weeks before risking money.")


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--csv"); a.add_argument("--lot", type=int, default=75); a.add_argument("--delta", type=float, default=0.5)
    a.add_argument("--cost", type=float, default=120.0); a.add_argument("--theta", type=float, default=0.0)
    a = a.parse_args()
    df = load(a.csv)
    t, s = run(df, a.lot, a.delta, a.cost, a.theta)
    report(t, s, len(set(df.index.date)) - 1)
    if not t.empty:
        t.to_csv("nifty_backtest_trades.csv", index=False); print("\nSaved nifty_backtest_trades.csv")
