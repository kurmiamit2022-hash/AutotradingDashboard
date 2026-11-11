# SignalDesk (TradeRadar)

A multi-user web dashboard that scans about 225 NSE stocks and indices on 5-minute, 15-minute and daily candles, shows buy and sell signals with a stop-loss and four targets, and can place paper or live orders through Dhan or Angel One.

One Python file (FastAPI), deployed on Render, with Dhan as the single source of candles and live prices.
Documentation for build **2026-11-11o**.

> The indicator's inner workings are deliberately **not** described in this file. Keep them in a private owner-only document, and never name the indicator's parts in any text that users can see (say "trend line" and "stop").
> Never paste keys, tokens, PINs or passwords into chat, issues or this repository. Enter them only in Render, Environment, or on the Account page.

## What it does

- Scans every stock on 5 min, 15 min and daily candles; lists fresh and open signals with grade A, B or C.
- Futures and option chains with option prices, open interest and an option chart.
- Options Autopilot (one for F&O stocks, one for NIFTY) in paper or live mode.
- Replay of the Autopilot over past days with a capital simulation, and a signal backtest.
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
| `Dockerfile` | Python 3.12 image, data under `/data`; build arg `WITH_LIVE=1` installs live-trading packages |
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
2. Check the **build stamp** next to your user name at the top of the page; it must match the build you shipped.

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

## Features by tab

| Tab | What it shows |
| --- | --- |
| Stocks | All scanned stocks and indices: live price, % change, signal, stop, T1 to T4, grade; timeframe switch 5 min / 15 min / Daily |
| Futures | Near-month futures with lot size, price, signal |
| Options | Option chain per stock/index: price, OI, IV; chips for stocks with a signal; sort by signal, name, % up, % down, biggest move; option chart |
| Intraday Scanner | The day's most active liquid stocks (top 15 by traded value by default) |
| Commodities | Nearest MCX futures (gold, silver, crude oil, natural gas, copper, zinc, aluminium, lead, nickel and the mini contracts) with the same signals, stop and targets T1 to T4, live price and a chart. View-only: no orders. Needs Dhan as the data source |
| Autopilot | Two autopilots: settings, Play/Pause/Stop, today's trades, and a table of today's signals for manual trades (open, taken, passed, earlier) |
| History | Past signals and results per stock, side, sector, hour |
| Backtest | Autopilot Replay with capital simulation, plus signal backtest |
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

- **Capital:** a starting amount; the whole account is used for each trade; no risk-per-trade input; stop and targets are the system's own.
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
| Data source says "Live stream: error" | A bug in an earlier build overwrote the live-candle store | Fixed in 2026-11-11n; deploy it |
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

General approach: check the build stamp, then the Admin page's Data source and token lines, then the red line on the Portfolio page, which names the failing step.

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
- Sort control on the main Stocks table.
- Login attempt lockout.
- Saving Autopilot settings to the database.

## Maintenance notes

- **Build stamp:** change the constant `BUILD_ID` in `signal_dashboard.py` on every release. Do not name it `BUILD`: that name is the live-candle store (reusing it caused the live-stream error fixed in 2026-11-11n).
- **Before shipping:** `python3 -m pyflakes signal_dashboard.py`, run your regression scripts, then rebuild the zip:
  `zip signal_dashboard_bundle.zip signal_dashboard.py Dockerfile requirements.txt requirements-live.txt nifty_backtest.py`
- **Tests** use fake data (the build environment cannot reach Yahoo, NSE or Dhan). The real Dhan connection is verified only on the live server.
- **Page text:** the page is a raw Python string; keep quoting consistent when editing it.
- **Time:** use `ist_now()` for India time; `datetime.now()` on the server is UTC.
- **First checks on a new deploy:** build stamp, scan progress on the History tab, Data source panel (stream connected, instruments count), Portfolio price line.
- **Backups:** back up Postgres (or `/data`) together with `SECRETS_KEY`.
