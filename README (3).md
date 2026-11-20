# SignalDesk (TradeRadar)

A multi-user web dashboard that scans about 225 NSE stocks and indices on 5-minute, 15-minute and daily candles, shows buy and sell signals with a stop-loss and four targets, and can place paper or live orders through Dhan or Angel One.

One Python file (FastAPI), deployed on Render, with Dhan as the single source of candles and live prices.
Documentation for build **2026-11-11o**.

> The indicator's inner workings are deliberately **not** described in this file. Keep them in a private owner-only document, and never name the indicator's parts in any text that users can see (say "trend line" and "stop").
> Never paste keys, tokens, PINs or passwords into chat, issues or this repository. Enter them only in Render, Environment, or on the Account page.

## What it does

- Scans every stock on 5 min, 15 min and daily candles; lists fresh and open signals with grade A, B or C.
- Futures and option chains with option prices, open interest and an option chart.
- Options Autopilot (one for F&O stocks, one for NIFTY) in paper or live mode. The stock Autopilot trades only early movers, by the NIFTY trend: before 10:00, while NIFTY is down it buys a PE on stocks in the 2 worst sectors and a CE on stocks in the 1 best sector; while NIFTY is up it buys a CE on stocks in the 2 best sectors and a PE on stocks in the 1 worst sector. The stock must also have moved between 1.8% and 3.6% (`AUTO_MOM_PCT`, `AUTO_MOM_MAX`) the same way as its sector. The trades that go with NIFTY come first; if NIFTY is flat nothing is bought. Settings: `AUTO_MOM` (on/off), `AUTO_MOM_END` (10:00), `AUTO_MOM_PCT` (2), `AUTO_MOM_TOP` (2, with the trend), `AUTO_MOM_COUNTER` (1, against it), `AUTO_SENT_MIN` (0.1), `AUTO_OLD_RULES`. The lean filter is off by default (`AUTO_SENT=1` turns it on): with NIFTY flat (inside ±`AUTO_SENT_MIN`, 0.1%) the best sector gets a CE and the worst sector a PE instead of nothing being bought.
- Replay of the Autopilot over past days with a capital simulation (the stock Autopilot replays its early-mover rule), and a backtest with three strategies: Nifty confluence (the Nifty Autopilot rules), signals and early movers.
- Paper and live positions with live profit and loss.
- Admin tools: users, limits, data source, Dhan token status.

## What it does not do

- It does not guarantee profit. Every historical result is a simulation.
- Replay option profit is a Black-Scholes **estimate**, not real option premiums.
- It is not investment advice. Start in paper mode.

## Files

| File | Purpose |
| --- | --- |
| `signal_dashboard.py` | Server, indicator, scanner, Autopilot, orders, Replay and the web page (HTML/CSS/JS in one raw string) |
| `Dockerfile` | Python 3.12 image, data under `/data`; live-trading packages are installed by default; build with `--build-arg WITH_LIVE=0` to leave them out |
| `requirements.txt` | Dashboard packages |
| `requirements-live.txt` | Extra packages for live broker orders |
| `nifty_backtest.py` | Stand-alone NIFTY backtest script |

### Main parts (all in one process)

- **Scanner:** `refresh(tf)` downloads candles and computes signals. Runs at startup for the default timeframe, 5m and 15m, and again when someone views a stale timeframe (`ensure_fresh`).
- **Live feed:** a Dhan WebSocket thread streams prices and builds the running candle; a feed thread keeps last prices and previous close.
- **Autopilot loop:** a thread every 10 s (`AUTOPILOT_SECONDS`) managing entries and exits per user.
- **Orders:** `make_order` checks all limits, then paper or broker placement; every order is saved to the database and to a JSON-lines log.
- **Storage:** SQLite (`DB_PATH`) by default, Postgres when `DATABASE_URL` is set.
- **Alerts:** Telegram and ntfy.

## Run locally

```bash
pip install -r requirements.txt            # live trading also: pip install -r requirements-live.txt
ADMIN_USER=admin ADMIN_PASSWORD='choose-a-long-one' python signal_dashboard.py
# open http://127.0.0.1:8000
```

First sign-in: set `ADMIN_USER` and `ADMIN_PASSWORD`, or read the one-time password printed in the console.
Locked out: set `ADMIN_RESET=1` with `ADMIN_PASSWORD`, restart once, sign in, then remove `ADMIN_RESET`.

## Deploy on Render

The production setup is a Render Web Service (Starter plan) built from the `Dockerfile`.

1. Push these files to a Git repository and create a Render Web Service from it (Docker).
2. In Render, Environment, set at least: `ADMIN_USER`, `ADMIN_PASSWORD`, `DATA_BROKER=dhan`, `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `COOKIE_SECURE=1`.
3. For storage that survives deploys, add a Postgres database and set `DATABASE_URL` and `SECRETS_KEY`, or attach a disk mounted at `/data`.
4. Open the site and sign in as admin.

### Updating

1. Replace `signal_dashboard.py` (and other files if changed) and push. Render rebuilds and restarts.
2. Check the **build 13ftamp** next to your user name at the top of the page; it must match the build you shipped.

### After every restart

- The Autopilot is **off** and its settings are back to defaults. Open the Autopilot tab, set the options, press Play.
- Scans for the default timeframe, 5m and 15m start on their own; the first full scan takes a few minutes.
- Without Postgres or a disk, users and saved broker details are lost on redeploy.

## Data source: Dhan

With `DATA_BROKER=dhan`, live prices stream over Dhan's WebSocket and candles come from Dhan's historical data. Yahoo is only an optional fallback (`YAHOO_FALLBACK=1`). Admins can switch under Admin, Data source.

- **Candles:** requested one symbol at a time, at most 5 requests a second shared by the whole app, so a full scan of 225 symbols takes a few minutes. Intraday history is about 30 days.
- **Live candle:** built from streamed prices, replaced by Dhan's candle when it closes.
- **Daily % change:** uses the exchange's previous close sent by the stream.
- **Instrument list:** a large Dhan file downloaded once a day by one thread. A list that arrives without option contracts is rejected and fetched again (at most once a minute).

### Access token

- The server uses `DHAN_ACCESS_TOKEN` (valid about 24 hours). Replace it daily in Render, unless auto-renew is on.
- Auto-renew is **off** by default. To use it set `DHAN_AUTO_RENEW=1`, `DHAN_PIN`, `DHAN_TOTP_SECRET`; the server then signs in at `DHAN_REFRESH_AT` (default 06:00 IST).
- A person's own token under Account is separate from the server token. It is used for that person's orders and positions; prices fall back to the server token when the personal one is refused.
- Live orders at Dhan need the server's public IP registered with Dhan (static IP).
- Fixed IP for orders: Dhan takes two addresses. Sign up for a fixed-IP proxy (QuotaGuard Static gives a pair), save both addresses in Dhan's Static IP Setting, and set `QUOTAGUARDSTATIC_URL` (or `ORDER_PROXY`) on the server to the proxy's HTTP/HTTPS URL. Orders that place, change or cancel go through the proxy; prices, candles and reads go straight out, so they do not use the proxy's request allowance. Admin tab, Order IP, shows the address Dhan will see.

## Features by tab

| Tab | What it shows |
| --- | --- |
| SWING | All scanned F&O stocks plus about 400 cash stocks that are not in F&O (a Cash tag marks them; pick All, F&O or Cash stocks next to the search box; the cash stocks are scanned on Daily and Weekly only when SWING is opened, with live prices from the broker feed, and are not used by the Autopilot, Trade Desk, futures or options; they can be bought and sold as shares). Each name is checked against the broker's instrument list at run time and dropped if it has futures or is not listed. To use your own list run make_cash_list.py on a PC that can reach NSE (it writes cash_symbols.txt: Nifty 500 minus the F&O list), place the file next to signal_dashboard.py and redeploy, or set CASH_EXTRA=ABC,XYZ to add names. The line under the title shows whether prices are live: a green "Live prices · Ns ago", "Market closed", or an amber "Prices NOT live" with the reason (when it is amber, rows show the last candle close, which is a day old). The chart popup now moves the forming candle with the live price every 3 seconds during market hours (a "● live" label shows the time and price; Weekly is not ticked). The SWING, Trade Desk and Autopilot tabs have their own colours (blue, orange, green). Click a stock to open its chart, then move through the list like TradingView: ↑ ↓ (or K and J, or the ▲ ▼ buttons) go to the previous or next stock in the list you are viewing (its filters and sort are kept), ← → change the timeframe, Esc closes; the next three charts are loaded ahead so each step is nearly instant. Sectors are filter buttons with a stock count and signal counts and no % change. For swing trading: all scanned stocks and indices: live price, % change, signal, stop, T1 to T4, grade; timeframe switch Daily / Weekly (Weekly candles are built from the daily ones and scanned when first opened; a weekly signal is confirmed at Friday's close) |
| Trade Desk (the former Charts and Options tabs; the Intraday Scanner tab was removed) | Tall watchlist with filters (grade badges, sector list ranked by its average move today, green or red by sign, a signals or flashing filter, sort buttons for A–Z, grade and % change) (rows flash green when the 5 min and 15 min signals are both buy, red when both are sell) plus one big chart for a stock, its nearest futures contract or an option (expiry, strike, CE or PE), with the underlying stock chart under the futures and option charts; the open chart updates by itself (the live price moves the forming candle every 3 seconds, the full chart reloads every 20 seconds, zoom kept); the Daily charts include today's still-forming candle during the session (option Daily candles are built from hourly prices); every buy and sell signal on the shown candles is marked with an arrow; on an option chart the stock's signals read as option trades (BUY CE or BUY PE, then EXIT), zoom-synced. A market mood strip under the page header (on every tab) starts with NIFTY, SENSEX and BANKNIFTY (price, day change, green or red, live when the feed is up) and a one-word market sentiment (Bullish when at least two of the three are up and none is down, Bearish the other way, Mixed otherwise; moves under 0.1% count as flat), then shows advances and declines across the stocks tracked, a breadth label (Bullish at 65% or more advancing, Bearish at 35% or less), India VIX (from Yahoo, refreshed every minute; rising VIX is shown red) and the strongest and weakest sector today by average move; it refreshes every 5 seconds. Star any stock (on its row or beside the chart title) to keep a personal watchlist, saved on the server for your login, and pick My watchlist in the Show list. A ⚠ on a row, a bar above the chart and a line in the order ticket warn when the stock has results, a board meeting or an ex-date (dividend, split, bonus, rights) today or on the next trading day; the calendar comes from NSE's public site and is checked every 6 hours (if NSE blocks the server, set EVENTS_EXTRA, for example RELIANCE,2026-11-20,Results;TCS,2026-11-21,Ex-dividend). ↑ ↓ move through the list, ← → change the timeframe. Under the chart: a recommendation for the stock, futures and an option on every timeframe, then the full option chain (OI, change in OI, volume, IV, PCR, OI graph, buy and sell buttons). Futures and option charts need Dhan as the data source |
| BTST (a tab inside Scanner) | F&O stocks that meet 3 or 4 rules for buy-today, sell-tomorrow: close in the top 20% of the day's range, day up 1% to 6%, volume at least 1.5x its 20 day average, price at a 20 day high. Best checked 3:00 to 3:25 pm. A stock with an event today or the next trading day is greyed out with the event named. Buy places a delivery order ticket; sell in your broker app the next morning (this dashboard cannot sell delivery shares). Below the list, a backtest of the same rules on about two years of daily candles shows win rate, average result and worst loss for three exits, with charges taken off |
| Scanner | Four lists from the daily candles: near the 52 week high, near the 52 week low (within 1, 2, 3 or 5%, your choice), monthly breakout (price above the previous calendar month's high) and monthly breakdown (below its low). Closest to the level first, so the freshest breakouts lead. One more list: Signal + level (a daily BUY or SELL up to 5 candles old and still running, with a 52 week or monthly level behind it). A BTST (buy today, sell tomorrow) list sits in the same tab, with its backtest underneath. They cover the F&O stocks plus the cash stocks once SWING has been opened |
| Performance › Journal | Kill switch and trade journal. **Kill switch:** a daily loss limit you set (₹, 0 = off) stops new orders for the rest of the day once today's result falls that far (checked every 30 seconds, so a fast loss can pass it slightly); the stop cannot be undone the same day. "Stop trading for today" does the same by hand and can be lifted with your order PIN. A stop pauses your autopilots, can also sell their open positions, and still lets you close a position you opened today. A red bar shows on every tab while it is on. The admin has "Stop all trading" in the Admin tab. It ends by itself tomorrow. **Journal:** every closed trade (autopilot trades, and buys matched against sells from the order ticket), kept without the 14 day limit of the order log, with result, win rate, profit factor, average per trade, drawdown, a running-result chart, breakdowns by source, tag and weekday, and a note and tag on each trade. "Add a trade made elsewhere" records trades this dashboard cannot see, such as shares sold in the broker app. Charges are not included |
| Sector charts | On the SWING tab, click a sector then "Chart" for an equal-weight index of its stocks, with direction, breadth, leaders and laggards |
| Commodities | Nearest MCX futures (gold, silver, crude oil, natural gas, copper, zinc, aluminium, lead, nickel and the mini contracts) with the same signals, stop and targets T1 to T4, live price and a chart. View-only: no orders. Needs Dhan as the data source |
| Autopilot | Two autopilots: settings, Play/Pause/Stop, today's trades, and a table of today's signals for manual trades (open, taken, passed, earlier) |
| Performance › History | Past signals and results per stock, side, sector, hour; plus your Autopilot's own trades by entry type, by day and one by one |
| Performance › Backtest | Autopilot Replay with capital simulation, plus backtests (Nifty confluence, signals, or early movers using the whole F&O list on 5 minute candles) |
| Portfolio | Open positions with live P&L, today's orders, broker funds |
| Account | Personal broker details, order PIN, alerts |
| Admin | Users and limits, data source, special sessions, Dhan token status |

**Order ticket:** side, quantity, order type (Limit or Market, for stocks, futures and options), price, product (Intraday or Carry forward), optional automatic stop-loss and trailing. A review screen with warnings comes first; nothing is sent until you confirm, and live orders also need the order PIN.

## Autopilot rules

Two autopilots per user with separate settings, state and limits: **F&O stocks** and **NIFTY**. Each buys one at-the-money call (CE) on a BUY signal or put (PE) on a SELL signal.

**Entries**

- **Fresh:** a new signal on the **5 minute chart** only (`AUTO_ENTRY_TFS`). The 15 minute chart must be in the same-direction zone (a BUY only while it is also in a BUY zone, a SELL only while it is in a SELL zone).
- **Continuation:** only 09:15 to 09:45 (`AUTO_CONT_START`, `AUTO_CONT_END`): a signal from an earlier day is still open and the price is back near its trend line (within `OPTBOT_NEAR_ATR`).
- **Universe:** every F&O stock whose move today is between 1% and 3%, up or down (`AUTO_MOVE_MIN`, `AUTO_MOVE_MAX`).
- **Window:** entries 09:20 to 14:30 (`OPTBOT_LAST_ENTRY`).
- **User settings:** trade grade, entries (fresh / continuation / both), trades a day, lots each, stop for the day at a rupee loss.

**Exits**

- Stop-loss and T1 to T4 are the indicator's own levels on the stock.
- The stop moves to entry at T1, to T1 at T2, to T2 at T3; everything is sold at T4 or at the stop.
- The option stop is never more than 50% below the price paid (`OPTBOT_MAX_LOSS_PCT`).
- Everything is sold by 15:15 (`OPTBOT_SQUAREOFF`).

**Operation:** Play / Pause / Stop; Off after a restart; live mode must be armed with the order PIN each day; orders are never retried automatically; the Autopilot places **limit** orders for entries and exits; when nothing is bought, the log says why.

## Orders, brokers and safety limits

- **Modes:** orders are paper (simulated) unless `LIVE_TRADING=1` on the server **and** an admin enabled live for that person.
- **Brokers:** Dhan and Angel One; each person stores their own details under Account, encrypted with `SECRETS_KEY`.

| Limit (per person) | Default | Setting |
| --- | --- | --- |
| Order value | ₹50,000 | `MAX_ORDER_VALUE` |
| Quantity per order | 500 | `MAX_QTY` |
| Orders per day (entries and exits) | 10 | `MAX_ORDERS_PER_DAY` |
| Futures lots | 1 | `MAX_FUT_LOTS` |
| Futures value | ₹25,00,000 | `MAX_FUT_VALUE` |
| Selling options | allowed | `ALLOW_OPTION_SELLING` |

Admins can change them per user under Admin, Users, Limits. The Autopilot's "Trades a day" box is separate; each autopilot trade uses at least two orders, so keep the order limit well above trades a day × 2.

- **Checks before accepting an order:** limits, contract found in the broker list (exact expiry day only), price within a band of the last price (limit orders), signal warnings (against it, old, stop hit), duplicate guard (identical order within a minute is refused).
- **Market orders on options:** allowed since build 2026-11-11f; the review screen warns they can fill far from the last price on thin contracts.
- **Paper positions** are priced from the broker (own account, then server token, then Dhan's option chain call, then NSE's chain). The Portfolio answers at once and fills prices in within seconds.
- **Order log:** every order is saved to the database and to `ORDER_LOG`, so a restart keeps managing open positions.

## Replay and backtest

The Replay (Backtest tab) runs the Autopilot's rules over past days and answers "how much would a starting capital have become?".
- **Autopilot keeps running:** each autopilot's state (mode, paused, limits, armed today) is saved to the database whenever it changes and restored after a server restart, the same day only. A live autopilot comes back only if it was armed with your PIN today and your account may still trade live; signing in again no longer switches an armed live autopilot off. It is NOT restarted after you press Stop, after the daily loss limit, the kill switch, or an order whose result was unsure, and a new day always needs a fresh Play (and PIN for Live). A watchdog restarts any background loop (autopilot, feeds, quotes) that has died, and the emergency memory drop never touches the autopilot's state or the option chains it buys from.
- **Memory and speed:** the caches (backtest candles, option history, charts, replay sets, option chains) expire by themselves, and when memory passes 95% of the instance limit (`MEM_EMERGENCY`, default 0.95) every cache is dropped, Python's freed memory is handed back to the system (`malloc_trim`) and a note is printed in the log. `MALLOC_ARENA_MAX=2` is set in the Dockerfile (many threads otherwise leave memory fragmented). Backtests and replays run `HEAVY_JOBS` at a time (default 2) and the rest wait. Big JSON answers are compressed (the live stream is not). Admin > Server health shows memory, the limit, the peak and what each cache holds, with a 24-hour chart (one point a minute since the server started) of memory against the limit and the emergency line, and the number of cached items.
- **Live option charts and market depth:** the Trade Desk option, futures and share charts show the best 5 bids and offers (orders, quantity, price), buyers vs sellers, spread, volume, open interest, day high/low and circuit limits, pushed live from Dhan's exchange feed (a server-sent stream, /api/stream; the page falls back to polling once a second and a half if the stream is silent); the same table is in the order ticket. The option, futures and share candles tick with that live price, and the option history reloads every 12 seconds. The latest signal's entry, stop and T1 to T4 are drawn on the stock chart, and on the option chart as estimated premiums (the option price plus delta times the stock's move; marked "est."; only when the signal matches the option side, a BUY for a CE, a SELL for a PE). Depth needs Dhan as the data source. The server subscribes the contracts people are viewing on its existing Dhan connection (full packets, 5 levels) and drops them 2 minutes after the last viewer; the fallback uses one batched /marketfeed/quote call a second. It is only live during market hours. Dhan's 20-level depth feed is not used.
- **NIFTY Autopilot (merged strategy):** trades NIFTY only, with an at-the-money option. One direction filter (trend line, VWAP and RSI: a CE needs RSI 55 to 75, a PE 25 to 45, 45 to 55 is chop) feeds two entries: a strong candle closing through the central pivot or R1/S1 (entry on a break of its high or low, stop at the other end), and a strong 5 minute close beyond the first 15 minute range between 9:30 and 10:30 (stop at the middle of the range, or the far end when it is narrow). T1 is 1x the risk, T2 the next pivot (at least 1.5x, else 2x), T3 and T4 trail; a trade that has not moved in 20 minutes is closed; 3 trades a day, none on expiry day. VWAP comes from the Nifty futures candles (Dhan); without them it is approximated from the index and the Replay says so. The Replay and the Backtest (strategy "Nifty confluence", 5 minute candles) use the same rules; they cannot see option prices, so the Replay prices the option with Black-Scholes. The earlier Nifty rules are gone. Settings: `NIFTY_TSTOP_MIN` (20), `NIFTY_BODY` (0.5), `NIFTY_MIN_RISK_PCT` (0.08), `NIFTY_MAX_RISK_PCT` (0.45), `NIFTY_NARROW_PCT` (0.25).
- **Stock Autopilot:** the Replay and the early-movers backtest use the same rule as the live Autopilot (NIFTY trend, best and worst sectors so far, a move above the set percentage, entries before the cut-off time, the stocks that go with NIFTY first, one trade per stock a day). NIFTY is loaded with the stocks for this.

- **Capital:** a starting amount; trades open at the same time share the account (the whole account when only one is open); no risk-per-trade input; stop and targets are the system's own.
- **Charges:** one percentage per trade.
- **Entries:** 5 minute chart only, 15 minute chart in the same-direction zone (as live).
- **Option profit:** each at-the-money option is priced with Black-Scholes from the stock price, a volatility estimate and days to the monthly expiry — an estimate, not real premiums.

Limits: expiry-day behaviour and holiday-shifted expiries are not modelled; the grade filter is not applied; history is about 30 days; fills and slippage are not modelled. Forward-test in paper mode for several weeks before trusting any number.

## Users, admin and security

- **Adding people:** an admin adds users, or people sign up when `ALLOW_SIGNUP=1` (optionally `SIGNUP_CODE`). New users cannot trade live until an admin enables it.
- **Roles:** admin and user.
- **Sessions:** `SESSION_HOURS` (default 12); CSRF token on every change request; repeated failed sign-ins are slowed down.
- **HTTPS:** `COOKIE_SECURE=1` behind https (always on Render).
- **Secrets:** broker details are encrypted with `SECRETS_KEY`. Back up the database and the key together; without the key saved broker details cannot be read.
- **Order PIN:** a personal PIN confirms live orders.
- **Special sessions:** `SPECIAL_SESSIONS` or the Admin page opens a trading window outside normal hours.
- **Alerts:** Telegram (`TELEGRAM_BOT_TOKEN`) and ntfy (`NTFY_SERVER`).

## Environment variables

Defaults are the values in the code. Optional file `symbols.txt` (one NSE symbol per line) replaces the default stock list.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADMIN_USER`, `ADMIN_PASSWORD` | admin, none | First admin account |
| `ALLOW_SIGNUP`, `SIGNUP_CODE` | 1, none | Self sign-up and its code |
| `SESSION_HOURS` | 12 | Sign-in length |
| `COOKIE_SECURE` | 0 | Set 1 behind https |
| `DATABASE_URL` | none | Postgres address; keeps users across deploys |
| `DB_PATH`, `ORDER_LOG` | dashboard.db, orders_log.jsonl | SQLite file and order log (`/data/...` in Docker) |
| `SECRETS_KEY` | none | Encrypts broker details; keep a backup |
| `DATA_BROKER` | none | `dhan` for one Dhan source |
| `CANDLE_SOURCE`, `YAHOO_FALLBACK` | none, 0 | Candle source; optional Yahoo fallback |
| `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN` | none | Server's Dhan credentials |
| `BTST_POS`, `BTST_CHG_MIN`, `BTST_CHG_MAX`, `BTST_VOL`, `BTST_COST` | 0.8, 1.0, 6.0, 1.5, 0.25 | BTST rules: close position in the day's range, day change limits in per cent, volume multiple, round trip charges in per cent used by the backtest |
| `KILL_SECONDS` | 30 | How often the daily loss limit is checked |
| `SCAN_SIG_BARS` | 5 | Scanner: how many daily candles old a signal may be for Signal + level |
| `QUOTAGUARDSTATIC_URL` or `ORDER_PROXY` | none | Fixed-IP proxy URL; order calls only go through it (keep it secret, it holds a password) |
| `DHAN_AUTO_RENEW`, `DHAN_PIN`, `DHAN_TOTP_SECRET` | 0, none, none | Optional daily token renewal |
| `DHAN_REFRESH_AT`, `DHAN_RENEW_EVERY_H` | 06:00, 12 | When and how often renewal runs |
| `INTERVAL` | 15m | Default timeframe (5m, 15m, 1d) |
| `REFRESH_SECONDS`, `LIVE_RESYNC_SECONDS` | 300, 1800 | Rescan frequency |
| `FEED_SECONDS`, `LIVE_CANDLES` | 5, 1 | Live price refresh; build candles from the feed |
| `YF_BATCH` | 30 | Symbols per scan batch |
| `LIVE_TRADING` | 0 | 1 allows live orders on the server |
| `MAX_ORDER_VALUE`, `MAX_QTY`, `MAX_ORDERS_PER_DAY`, `MAX_FUT_LOTS`, `MAX_FUT_VALUE`, `ALLOW_OPTION_SELLING` | see limits table | Default per-person limits |
| `AUTOPILOT_SECONDS` | 10 | Autopilot check interval |
| `AUTO_ENTRY_TFS` | 5m | Charts the Autopilot may enter on |
| `AUTO_MOVE_MIN`, `AUTO_MOVE_MAX` | 1, 3 | Day-move band (percent) |
| `AUTO_CONT_START`, `AUTO_CONT_END` | 09:15, 09:45 | Continuation window |
| `OPTBOT_LAST_ENTRY`, `OPTBOT_SQUAREOFF` | 14:30, 15:15 | Last entry, forced exit |
| `OPTBOT_FRESH_BARS`, `OPTBOT_NEAR_ATR`, `OPTBOT_MAX_CHG`, `OPTBOT_MAX_LOSS_PCT` | 1, 0.6, 2, 50 | Autopilot tuning |
| `CONF_A`, `CONF_B` | 75, 55 | Grade thresholds |
| `SCANNER_TOP`, `SCANNER_MIN_PRICE`, `SCANNER_RANK`, `SCANNER_INCLUDE`, `SCANNER_EXCLUDE`, `SCANNER_INDICES` | 15, 150, value, none, none, NIFTY,SENSEX | Intraday Scanner list |
| `STRIKES_EACH_SIDE`, `OPTION_TTL` | 25, 120 | Option chain size and cache seconds |
| `PAST_BOT_DAYS` | 30 | Days of past Autopilot trades shown |
| `TELEGRAM_BOT_TOKEN`, `NTFY_SERVER` | none, https://ntfy.sh | Alerts |
| `SPECIAL_SESSIONS` | none | Extra trading windows |
| `HOST`, `PORT` | 127.0.0.1, 8000 | Bind address (the Dockerfile sets `HOST=0.0.0.0`) |

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Data source says "Live stream: error" | A bug in an earlier build 13fverwrote the live-candle store | Fixed in 2026-11-11n; deploy it |
| Scan stuck at "0 of 225" | Several threads each downloaded Dhan's large instrument list | One download now; the History tab shows what the scan is doing |
| Nothing scanned after a restart | Only the default timeframe was scanned at startup | Startup now scans default, 5m and 15m |
| Portfolio shows "No live price" | Saved orders did not store expiry, strike and type | Fixed in 2026-11-11m |
| Portfolio blank or very slow | Price lookups queued behind the candle scan's Dhan requests | Prices now load in the background |
| "Access token expired" or a refusal | Dhan token is valid about 24 hours | Paste a new token in Render (server) or under Account (personal) |
| Wrong % change | Missing previous close or stale daily candle | Uses the exchange previous close from the stream |
| "Daily limit of 10 orders reached" | Account limit on entries and exits | Raise it under Admin, Users, Limits or set `MAX_ORDERS_PER_DAY` |
| Autopilot stopped after 3 trades | "Trades a day" box on the Autopilot tab | Change it, press Save settings |
| Autopilot off after a deploy | Settings are kept in memory | Set again and press Play |
| Live order rejected at Dhan | Server IP not registered with Dhan | Register the static IP in Dhan's API settings |
| Option not found for an order | Contract not in the broker's list for that exact expiry | Reload the option chain, choose the contract again |
| Users or brokers missing after a deploy | No Postgres or disk | Set `DATABASE_URL` and `SECRETS_KEY`, or mount `/data` |

General approach: check the build 13ftamp, then the Admin page's Data source and token lines, then the red line on the Portfolio page, which names the failing step.

## Known limits and ideas

**Limits**

- Replay option profit is modelled; expiry-day trades and holiday-shifted expiries are not modelled.
- The grade filter is not applied in the Replay; intraday history is about 30 days.
- Autopilot settings are in memory and reset on restart.
- Dhan data requests are limited to 5 a second.
- Live Dhan orders need a registered static IP.
- Results so far come from simulated data and paper trading, not a long live record.

**Not yet built**

- Cap on capital per trade in the Replay; real option premiums in the Replay; expiry-day handling.
- Replay option for 5 min only / 15 min only.
- Stacked call and put candle panels in the option chart.
- Sort control on the main SWING table.
- Login attempt lockout.
- Saving Autopilot settings to the database.

## Maintenance notes

- **Build stamp:** change the constant `BUILD_ID` in `signal_dashboard.py` on every release. Do not name it `BUILD`: that name is the live-candle store (reusing it caused the live-stream error fixed in 2026-11-11n).
- **Before shipping:** `python3 -m pyflakes signal_dashboard.py`, run your regression scripts, then rebuild 13fhe zip:
  `zip signal_dashboard_bundle.zip signal_dashboard.py Dockerfile requirements.txt requirements-live.txt nifty_backtest.py`
- **Tests** use fake data (the build environment cannot reach Yahoo, NSE or Dhan). The real Dhan connection is verified only on the live server.
- **Page text:** the page is a raw Python string; keep quoting consistent when editing it.
- **Time:** use `ist_now()` for India time; `datetime.now()` on the server is UTC.
- **First checks on a new deploy:** build 13ftamp, scan progress on the History tab, Data source panel (stream connected, instruments count), Portfolio price line.
- **Backups:** back up Postgres (or `/data`) together with `SECRETS_KEY`.
