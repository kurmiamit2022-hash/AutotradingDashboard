# ── Trading (off by default) ──
# TRADING_MODE=off    the dashboard only reads data (default)
# TRADING_MODE=paper  the whole order flow runs, but fills are simulated and no broker is contacted
# TRADING_MODE=live   real orders through BROKER=angelone or BROKER=dhan (needs DASH_PASSWORD)
TRADING_MODE = os.getenv("TRADING_MODE", "off").lower()
BROKER_NAME = os.getenv("BROKER", "").lower()
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "500"))          # rupees lost if the stop is hit; this sets the quantity
MAX_ORDER_VALUE = float(os.getenv("MAX_ORDER_VALUE", "50000"))      # rupees per entry order
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "5"))
MAX_SIGNAL_AGE_BARS = int(os.getenv("MAX_SIGNAL_AGE_BARS", "1"))    # refuse signals older than this many candles
MAX_CHASE_R = float(os.getenv("MAX_CHASE_R", "0.5"))                # refuse if price already ran this many R past the entry
TRADES_FILE = os.getenv("TRADES_FILE", "trades.jsonl")
TRADE_TF = ("5m", "15m")                                            # intraday timeframes only (positions are MIS)
MARKET_OPEN, ENTRY_CUTOFF = dtime(9, 15), dtime(15, 0)              # new entries only inside this window (IST)
FILL_TIMEOUT = 20                                                   # seconds to wait for an order to fill
TICK = 0.05                                                         # NSE cash-equity tick; every multiple of it is valid for 0.01-tick stocks too


class BrokerError(Exception):
    def __init__(self, msg, uncertain=False):
        super().__init__(msg)
        self.uncertain = uncertain  # True when we cannot tell whether the broker received the order


def _need(name):
    v = os.getenv(name)
    if not v:
        raise BrokerError(f"Set the {name} environment variable.")
    return v


def now_ist():
    return pd.Timestamp.now(tz=IST)


def tick_round(x):
    return round(round(x / TICK) * TICK, 2)


def _cached_download(url, path, max_age=20 * 3600):
    """Instrument lists are big, so keep one copy on disk per day."""
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < max_age:
        return path
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    with open(path + ".tmp", "wb") as f:
        f.write(r.content)
    os.replace(path + ".tmp", path)
    return path


class Broker:
    """Every broker offers the same five calls. kind is "MARKET" (entry/exit) or "SL_M" (protective stop)."""
    name = ""
    instant = False  # True only for the paper broker, so tests do not wait between polls

    def resolve(self, symbol):
        raise NotImplementedError

    def place(self, inst, side, qty, kind, trigger=None, ref=None):
        raise NotImplementedError

    def statuses(self, ids):
        """{order id: {"state": FILLED|OPEN|REJECTED|CANCELLED, "filled": int, "avg": float|None, "msg": str}}"""
        raise NotImplementedError

    def cancel(self, oid, kind):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError


class PaperBroker(Broker):
    name = "Paper"
    instant = True

    def __init__(self):
        self.orders, self.lock = {}, threading.Lock()

    def resolve(self, symbol):
        return {"symbol": symbol}

    def place(self, inst, side, qty, kind, trigger=None, ref=None):
        oid = "PAPER-" + secrets.token_hex(3)
        with self.lock:
            if kind == "MARKET":
                self.orders[oid] = {"state": "FILLED", "filled": qty, "avg": ref, "msg": ""}
            else:  # a paper stop just sits there: price movement is not simulated
                self.orders[oid] = {"state": "OPEN", "filled": 0, "avg": None, "msg": "paper stop"}
        return oid

    def statuses(self, ids):
        with self.lock:
            return {i: dict(self.orders[i]) for i in ids if i in self.orders}

    def cancel(self, oid, kind):
        with self.lock:
            o = self.orders.get(oid)
            if o and o["state"] == "OPEN":
                o["state"] = "CANCELLED"

    def check(self):
        return {"funds": None, "resolved": 0, "missing": []}


class AngelOne(Broker):
    name = "Angel One"
    MASTER = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

    def __init__(self):
        self.key, self.client = _need("ANGEL_API_KEY"), _need("ANGEL_CLIENT_ID")
        self.pin, self.totp = _need("ANGEL_PIN"), _need("ANGEL_TOTP_SECRET")
        self.api, self.login_day, self.index = None, None, None
        self.lock = threading.RLock()

    def _login(self):
        try:
            from SmartApi import SmartConnect
            import pyotp
        except ImportError:
            raise BrokerError("Install the Angel One packages first: pip install smartapi-python pyotp")
        api = SmartConnect(api_key=self.key)
        r = api.generateSession(self.client, self.pin, pyotp.TOTP(self.totp).now())
        if not isinstance(r, dict) or not r.get("status"):
            raise BrokerError("Angel One login failed: " + str((r or {}).get("message", "no response")))
        self.api, self.login_day = api, now_ist().date()

    def _session(self):
        with self.lock:  # Angel logs every session out at midnight, so log in again on a new day
            if self.api is None or self.login_day != now_ist().date():
                self._login()
            return self.api

    def _call(self, fn):
        """Read-only calls: one automatic re-login and retry. Order placement never goes through here."""
        for attempt in (0, 1):
            try:
                r = fn(self._session())
            except BrokerError:
                raise
            except Exception as e:
                if attempt:
                    raise BrokerError(f"Angel One request failed: {e}")
                self.api = None
                continue
            if attempt == 0 and isinstance(r, dict) and r.get("status") is False and str(r.get("errorcode")) in ("AG8001", "AG8002"):
                self.api = None
                continue
            return r

    def _load(self):
        if self.index is None:
            with open(_cached_download(self.MASTER, "angel_master.json")) as f:
                rows = json.load(f)
            idx = {}
            for e in rows:
                if e.get("exch_seg") == "NSE" and str(e.get("symbol", "")).endswith("-EQ"):
                    n = e.get("name")
                    idx[n] = None if n in idx else {"tradingsymbol": e["symbol"], "token": str(e["token"])}  # None marks an ambiguous name
            self.index = idx
        return self.index

    def resolve(self, symbol):
        inst = self._load().get(symbol)
        if not inst:
            raise BrokerError(f"{symbol} was not found (or is ambiguous) in the Angel One instrument list.")
        return inst

    def place(self, inst, side, qty, kind, trigger=None, ref=None):
        p = {"variety": "NORMAL", "tradingsymbol": inst["tradingsymbol"], "symboltoken": inst["token"],
             "transactiontype": side, "exchange": "NSE", "ordertype": "MARKET", "producttype": "INTRADAY",
             "duration": "DAY", "price": "0", "squareoff": "0", "stoploss": "0", "quantity": str(int(qty))}
        if kind == "SL_M":
            p.update(variety="STOPLOSS", ordertype="STOPLOSS_MARKET", triggerprice=f"{trigger:.2f}")
        api = self._session()  # any login happens here; the order call below is made exactly once and never retried
        try:
            fn = getattr(api, "placeOrderFullResponse", None)
            r = fn(p) if fn else {"status": True, "data": {"orderid": api.placeOrder(p)}}
        except Exception as e:
            raise BrokerError(f"Angel One order request failed ({e}). It may or may not have been placed: check the Angel One app.", uncertain=True)
        oid = ((r or {}).get("data") or {}).get("orderid") if isinstance(r, dict) else None
        if not oid:
            raise BrokerError("Angel One rejected the order: " + str((r or {}).get("message", "no response")))
        return str(oid)

    def statuses(self, ids):
        r = self._call(lambda api: api.orderBook())
        out = {}
        for o in ((r or {}).get("data") or []) if isinstance(r, dict) else []:
            oid = str(o.get("orderid"))
            if oid in ids:
                s = str(o.get("orderstatus") or o.get("status") or "").lower()
                out[oid] = {"state": {"complete": "FILLED", "rejected": "REJECTED", "cancelled": "CANCELLED"}.get(s, "OPEN"),
                            "filled": int(float(o.get("filledshares") or 0)), "avg": _num(o.get("averageprice")) or None,
                            "msg": str(o.get("text") or "")}
        return out

    def cancel(self, oid, kind):
        variety = "STOPLOSS" if kind == "SL_M" else "NORMAL"
        r = self._call(lambda api: api.cancelOrder(oid, variety))
        if isinstance(r, dict) and r.get("status") is False:
            raise BrokerError("Angel One cancel failed: " + str(r.get("message")))

    def check(self):
        idx = self._load()
        missing = [s for s in SYMBOLS if not idx.get(s)]
        r = self._call(lambda api: api.rmsLimit())
        cash = ((r or {}).get("data") or {}).get("availablecash") if isinstance(r, dict) else None
        return {"funds": cash, "resolved": len(SYMBOLS) - len(missing), "missing": missing}


class Dhan(Broker):
    name = "Dhan"
    BASE = "https://api.dhan.co/v2"
    MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"

    def __init__(self):
        self.client, self.token, self.index = _need("DHAN_CLIENT_ID"), _need("DHAN_ACCESS_TOKEN"), None

    def _http(self, method, path, body=None, placing=False):
        try:
            r = requests.request(method, self.BASE + path, json=body, timeout=10,
                                 headers={"access-token": self.token, "Accept": "application/json", "Content-Type": "application/json"})
        except Exception as e:
            raise BrokerError(f"Dhan request failed ({e})." + (" The order may or may not have been placed: check the Dhan app." if placing else ""),
                              uncertain=placing)
        try:
            data = r.json()
        except ValueError:
            data = None
        if r.status_code >= 400:
            msg = (data.get("errorMessage") or data.get("message")) if isinstance(data, dict) else None
            raise BrokerError(f"Dhan answered {r.status_code}: {msg or r.text[:200]}")
        return data

    def _load(self):
        if self.index is None:
            df = pd.read_csv(_cached_download(self.MASTER, "dhan_master.csv"), low_memory=False)
            df = df[(df["SEM_EXM_EXCH_ID"] == "NSE") & (df["SEM_SEGMENT"] == "E") & (df["SEM_INSTRUMENT_NAME"] == "EQUITY")]
            if "SEM_SERIES" in df.columns:
                df = df[df["SEM_SERIES"] == "EQ"]  # the same symbol can also exist as a bond or other series
            idx = {}
            for sym, g in df.groupby("SEM_TRADING_SYMBOL"):
                if len(g) > 1 and "SEM_EXCH_INSTRUMENT_TYPE" in g.columns:
                    g = g[g["SEM_EXCH_INSTRUMENT_TYPE"] == "ES"]
                if len(g) == 1:  # anything still ambiguous is left out, so it can never be traded by mistake
                    idx[str(sym)] = {"security_id": str(int(g.iloc[0]["SEM_SMST_SECURITY_ID"]))}
            self.index = idx
        return self.index

    def resolve(self, symbol):
        inst = self._load().get(symbol)
        if not inst:
            raise BrokerError(f"{symbol} was not found (or is ambiguous) in the Dhan instrument list.")
        return inst

    def place(self, inst, side, qty, kind, trigger=None, ref=None):
        body = {"dhanClientId": self.client, "correlationId": "sd" + secrets.token_hex(8), "transactionType": side,
                "exchangeSegment": "NSE_EQ", "productType": "INTRADAY", "orderType": "MARKET", "validity": "DAY",
                "securityId": inst["security_id"], "quantity": int(qty), "price": 0}
        if kind == "SL_M":
            body.update(orderType="STOP_LOSS_MARKET", triggerPrice=round(float(trigger), 2))
        data = self._http("POST", "/orders", body, placing=True)
        if not isinstance(data, dict) or not data.get("orderId"):
            raise BrokerError("Dhan did not accept the order: " + str(data)[:200])
        return str(data["orderId"])

    def statuses(self, ids):
        data = self._http("GET", "/orders")
        out = {}
        for o in data if isinstance(data, list) else []:
            oid = str(o.get("orderId"))
            if oid in ids:
                s = str(o.get("orderStatus", "")).upper()
                out[oid] = {"state": {"TRADED": "FILLED", "REJECTED": "REJECTED", "CANCELLED": "CANCELLED", "EXPIRED": "CANCELLED"}.get(s, "OPEN"),
                            "filled": int(o.get("filledQty") or 0), "avg": _num(o.get("averageTradedPrice")) or None,
                            "msg": str(o.get("omsErrorDescription") or "")}
        return out

    def cancel(self, oid, kind):
        self._http("DELETE", f"/orders/{oid}")

    def check(self):
        idx = self._load()
        missing = [s for s in SYMBOLS if s not in idx]
        d = self._http("GET", "/fundlimit")
        funds = (d.get("availabelBalance", d.get("availableBalance")) if isinstance(d, dict) else None)  # Dhan spells it "availabel"
        return {"funds": funds, "resolved": len(SYMBOLS) - len(missing), "missing": missing}


def make_broker():
    if TRADING_MODE == "off":
        return None
    if TRADING_MODE == "paper":
        return PaperBroker()
    return {"angelone": AngelOne, "dhan": Dhan}[BROKER_NAME]()


if TRADING_MODE not in ("off", "paper", "live"):
    raise SystemExit("TRADING_MODE must be off, paper or live.")
if TRADING_MODE == "live":
    if BROKER_NAME not in ("angelone", "dhan"):
        raise SystemExit("Live trading needs BROKER=angelone or BROKER=dhan.")
    if not PASSWORD:
        raise SystemExit("Live trading needs DASH_PASSWORD, so nobody else can place orders from this page.")
try:
    BROKER = make_broker()
except BrokerError as e:
    raise SystemExit(str(e))

# ── Ledger, confirmation tokens, locks ──
TRADES = {}                                   # trade id -> record
PENDING = {}                                  # confirmation token -> {"plan", "exp"}
PENDING_LOCK, EXEC_LOCK = threading.Lock(), threading.Lock()
HALTED = False
LAST_REFRESH = [0.0]
NOT_TRADED = ("REJECTED", "NOT_FILLED")       # these never opened a position, so they do not count and may be retried
STATUS_TEXT = {"ENTRY_SENT": "Entering", "PROTECTED": "Open, stop live", "STOPPED_OUT": "Stopped out", "CLOSED": "Closed",
               "FLATTENED": "Exited (stop failed)", "UNPROTECTED": "NO STOP: act now", "REJECTED": "Rejected",
               "NOT_FILLED": "Not filled", "UNKNOWN": "Unknown: check broker", "ENDED": "Ended: check broker"}


def save_trade(rec):
    TRADES[rec["id"]] = rec
    try:
        with open(TRADES_FILE, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError:
        pass


def load_trades():
    if TRADING_MODE == "off" or not os.path.exists(TRADES_FILE):
        return
    with open(TRADES_FILE) as f:
        for ln in f:
            try:
                r = json.loads(ln)
                TRADES[r["id"]] = r  # later lines replace earlier ones
            except (ValueError, KeyError):
                continue


load_trades()


def note(rec, text):
    rec["notes"].append(text)


def today_str():
    return now_ist().date().isoformat()


def todays(mode_only=True):
    d = today_str()
    return [t for t in TRADES.values() if t["day"] == d and (not mode_only or t["mode"] == TRADING_MODE)]


def trade_key(row, tf):
    return f"{row['symbol']}|{tf}|{row['side']}|{row['signal_time']}"


def gate(symbol, tf, signal_time=None):
    """Every rule an entry must pass. Runs when the window opens and again on confirm. Returns (row, error)."""
    if TRADING_MODE == "off":
        return None, "Trading is switched off."
    if tf not in INTERVALS:
        return None, "Unknown timeframe."
    if tf not in TRADE_TF:
        return None, "Orders are intraday only. Switch to the 5 min or 15 min view."
    if HALTED:
        return None, "New entries are halted."
    with LOCK:
        st = STATES[tf]
        row = next((dict(r) for r in st["rows"] if r["symbol"] == symbol), None)
        ts = st.get("ts", 0.0)
    if row is None or row["signal"] not in ("BUY", "SELL"):
        return None, "This symbol has no fresh BUY or SELL signal."
    if signal_time and row["signal_time"] != signal_time:
        return None, "The signal changed while this window was open. Open it again."
    if time.time() - ts > 2 * REFRESH_SECONDS + 60:
        return None, "Signals are stale. Press Refresh now, then try again."
    if row["bars_ago"] > MAX_SIGNAL_AGE_BARS:
        return None, f"The signal is {row['bars_ago']} candles old (limit {MAX_SIGNAL_AGE_BARS})."
    if row["sl_hit"] or row["hits"] > 0:
        return None, "This signal has already reached a target or its stop."
    now = now_ist()
    if now.weekday() >= 5 or not (MARKET_OPEN <= now.time() < ENTRY_CUTOFF):
        return None, f"New entries are allowed {MARKET_OPEN:%H:%M} to {ENTRY_CUTOFF:%H:%M} IST on weekdays."
    entry, sl, close = row["entry"], row["sl"], row["close"]
    risk, d = abs(entry - sl), (1 if row["side"] == "BUY" else -1)
    if risk <= 0:
        return None, "The stop equals the entry, so no quantity can be sized."
    if (close - sl) * d <= 0:
        return None, "Price is already through the stop."
    if (close - entry) * d > MAX_CHASE_R * risk:
        return None, f"Price has already moved more than {MAX_CHASE_R:g}R past the entry. Skipping this one."
    if sum(1 for t in todays() if t["status"] not in NOT_TRADED) >= MAX_TRADES_PER_DAY:
        return None, f"Daily limit of {MAX_TRADES_PER_DAY} trades reached."
    key = trade_key(row, tf)
    if any(t.get("key") == key and t["status"] not in NOT_TRADED for t in todays()):
        return None, "This signal was already traded. See Today's trades."
    return row, None


def plan_trade(symbol, tf):
    row, err = gate(symbol, tf)
    if err:
        return {"error": err}
    risk = abs(row["entry"] - row["sl"])
    qty = min(int(RISK_PER_TRADE // risk), int(MAX_ORDER_VALUE // row["close"]))
    if qty < 1:
        return {"error": f"The quantity rounds to zero (risk per share ₹{risk:.2f}, price ₹{row['close']:.2f}). Raise RISK_PER_TRADE or MAX_ORDER_VALUE."}
    plan = {"symbol": symbol, "tf": tf, "side": row["side"], "qty": qty, "entry": row["entry"], "close": row["close"],
            "sl": tick_round(row["sl"]), "tps": [row[f"tp{i}"] for i in (1, 2, 3, 4)], "signal_time": row["signal_time"],
            "key": trade_key(row, tf), "risk_amount": round(qty * risk, 2), "value": round(qty * row["close"], 2),
            "mode": TRADING_MODE, "broker": BROKER.name}
    token = secrets.token_urlsafe(16)
    with PENDING_LOCK:
        PENDING[token] = {"plan": plan, "exp": time.time() + 90}
    return {**plan, "token": token}


def wait_state(br, oid, timeout, want=("FILLED", "REJECTED", "CANCELLED")):
    end, last = time.time() + timeout, None
    while True:
        time.sleep(0 if br.instant else 1.5)
        try:
            last = br.statuses([oid]).get(oid) or last
        except BrokerError:
            pass
        if (last and last["state"] in want) or time.time() >= end:
            return last


def flatten(br, rec, inst, opp, qty, ref):
    """Exit at market because the position could not be protected."""
    try:
        eid = br.place(inst, opp, qty, "MARKET", ref=ref)
        rec["exit_order"] = eid
        st = wait_state(br, eid, FILL_TIMEOUT)
        if st and st["state"] == "FILLED":
            rec["status"] = "FLATTENED"
            note(rec, "Position exited because the stop could not be placed. Check the broker app for stray orders.")
            return
        note(rec, "The exit order did not confirm.")
    except BrokerError as e:
        note(rec, f"Exit failed: {e}")
    rec["status"] = "UNPROTECTED"
    note(rec, "ACT NOW: the position may be open with no stop. Exit it in the broker app.")


def run_entry(br, rec, plan):
    side, symbol = plan["side"], plan["symbol"]
    opp = "SELL" if side == "BUY" else "BUY"
    try:
        inst = br.resolve(symbol)
        oid = br.place(inst, side, plan["qty"], "MARKET", ref=plan["close"])
    except BrokerError as e:
        rec["status"] = "UNKNOWN" if e.uncertain else "REJECTED"
        note(rec, str(e))
        return
    rec["entry_order"] = oid
    st = wait_state(br, oid, FILL_TIMEOUT)
    if not st or st["state"] != "FILLED":
        if st and st["state"] == "REJECTED":
            rec["status"] = "REJECTED"
            note(rec, "Entry rejected: " + (st["msg"] or "no reason given"))
            return
        try:
            br.cancel(oid, "MARKET")
        except BrokerError as e:
            note(rec, f"Cancel failed: {e}")
        st = wait_state(br, oid, 8)
        if st and st["filled"] > 0:
            note(rec, f"Partly filled: {st['filled']} of {plan['qty']}.")
        elif st and st["state"] in ("CANCELLED", "REJECTED"):
            rec["status"] = "NOT_FILLED"
            note(rec, "The entry did not fill in time and was cancelled.")
            return
        else:
            rec["status"] = "UNKNOWN"
            note(rec, "Could not confirm what happened to the entry order. Check the broker app now.")
            return
    filled, avg = st["filled"] or plan["qty"], st["avg"]
    rec.update(qty=filled, entry_avg=avg)
    stop = plan["sl"]
    if avg is not None and ((side == "BUY" and avg <= stop) or (side == "SELL" and avg >= stop)):
        note(rec, f"Filled at {avg:.2f}, already through the stop {stop:.2f}. Exiting.")
        return flatten(br, rec, inst, opp, filled, plan["close"])
    try:
        sid = br.place(inst, opp, filled, "SL_M", trigger=stop)
    except BrokerError as e:
        note(rec, f"Stop order failed: {e}" + (" A stop may exist: check for stray orders." if e.uncertain else ""))
        return flatten(br, rec, inst, opp, filled, plan["close"])
    rec.update(sl_order=sid, status="PROTECTED")
    s2 = wait_state(br, sid, 3, want=("REJECTED", "CANCELLED", "FILLED"))
    if s2 and s2["state"] in ("REJECTED", "CANCELLED"):
        note(rec, "Stop rejected: " + (s2["msg"] or "no reason given"))
        return flatten(br, rec, inst, opp, filled, plan["close"])
    if s2 and s2["state"] == "FILLED":
        rec["status"] = "STOPPED_OUT"
    verb = "Bought" if side == "BUY" else "Sold short"
    note(rec, f"{verb} {filled} {symbol}" + (f" at {avg:.2f}" if avg else "") + f". Stop at {stop:.2f}.")


def place_trade(token):
    with PENDING_LOCK:
        now = time.time()
        for k in [k for k, v in PENDING.items() if v["exp"] < now]:
            del PENDING[k]
        item = PENDING.pop(token, None)  # single use
    if not item:
        return {"error": "This confirmation expired. Open the trade window again."}
    plan = item["plan"]
    with EXEC_LOCK:  # one order flow at a time; a second click waits, then fails the duplicate check
        row, err = gate(plan["symbol"], plan["tf"], plan["signal_time"])
        if err:
            return {"error": err}
        rec = {"id": secrets.token_hex(4), "day": today_str(), "ts": now_ist().strftime("%H:%M:%S"), "t": time.time(),
               "mode": TRADING_MODE, "broker": BROKER.name, "symbol": plan["symbol"], "tf": plan["tf"], "side": plan["side"],
               "qty": plan["qty"], "entry_ref": plan["close"], "sl": plan["sl"], "signal_time": plan["signal_time"],
               "key": plan["key"], "status": "ENTRY_SENT", "entry_order": None, "entry_avg": None, "sl_order": None, "notes": []}
        save_trade(rec)
        try:
            run_entry(BROKER, rec, plan)
        except Exception as e:  # a bug must never leave a trade without a clear status
            rec["status"] = "UNKNOWN"
            note(rec, f"Unexpected error ({e}). Check the broker app now.")
        save_trade(rec)
    text = ("Paper trade: " if TRADING_MODE == "paper" else "") + " ".join(rec["notes"][-2:])
    return {"message": text} if rec["status"] in ("PROTECTED", "STOPPED_OUT") else {"error": text or STATUS_TEXT.get(rec["status"], rec["status"])}


def close_trade(tid):
    """Cancel the stop first, then exit at market. Never exits while the stop is still live, so the two cannot both fire."""
    with EXEC_LOCK:
        rec = TRADES.get(tid)
        if not rec or rec["status"] != "PROTECTED" or rec["mode"] != TRADING_MODE:
            return {"error": "This trade cannot be closed from here."}
        br, sid = BROKER, rec["sl_order"]
        opp = "SELL" if rec["side"] == "BUY" else "BUY"
        try:
            inst = br.resolve(rec["symbol"])
            cur = (br.statuses([sid]).get(sid) or {})
            if cur.get("state") != "FILLED":
                try:
                    br.cancel(sid, "SL_M")
                except BrokerError as e:
                    note(rec, f"Stop cancel failed: {e}")
                cur = wait_state(br, sid, 6) or {}
            if cur.get("state") == "FILLED":
                rec["status"] = "STOPPED_OUT"
                note(rec, "The stop had already been hit.")
                save_trade(rec)
                return {"message": "The stop had already been hit, so the position is closed."}
            if cur.get("state") != "CANCELLED":
                save_trade(rec)
                return {"error": "Could not confirm that the stop was cancelled, so nothing was sent. Check the broker app."}
            eid = br.place(inst, opp, rec["qty"], "MARKET", ref=rec["entry_ref"])
            rec["exit_order"] = eid
            st = wait_state(br, eid, FILL_TIMEOUT)
            if st and st["state"] == "FILLED":
                rec["status"] = "CLOSED"
                note(rec, "Closed at market" + (f" at {st['avg']:.2f}." if st.get("avg") else "."))
                save_trade(rec)
                return {"message": rec["notes"][-1]}
            try:  # exit not confirmed: stop it from lingering, then put the stop back
                br.cancel(eid, "MARKET")
            except BrokerError:
                pass
            st = wait_state(br, eid, 6)
            if st and st["state"] == "FILLED":
                rec["status"] = "CLOSED"
                note(rec, "Closed at market.")
                save_trade(rec)
                return {"message": "Closed at market."}
            rec["sl_order"] = br.place(inst, opp, rec["qty"], "SL_M", trigger=rec["sl"])
            note(rec, "The exit did not fill, so the stop was placed again.")
            save_trade(rec)
            return {"error": "The exit order did not fill. The stop is back in place; try again or use the broker app."}
        except BrokerError as e:
            rec["status"] = "UNPROTECTED"
            note(rec, f"Close failed: {e}. ACT NOW: check the broker app.")
            save_trade(rec)
            return {"error": f"Close failed: {e}. Check the broker app now, the position may have no stop."}


def refresh_trades():
    """Mark open trades as stopped out (or ended) when their stop order is no longer live. At most once per 15 seconds."""
    if TRADING_MODE == "off" or time.time() - LAST_REFRESH[0] < 15 or not EXEC_LOCK.acquire(blocking=False):
        return
    try:
        LAST_REFRESH[0] = time.time()
        live = [t for t in todays() if t["status"] == "PROTECTED" and t.get("sl_order")]
        if not live:
            return
        sts = BROKER.statuses([t["sl_order"] for t in live])
        for t in live:
            s = sts.get(t["sl_order"])
            if s and s["state"] == "FILLED":
                t["status"] = "STOPPED_OUT"
                note(t, "The stop was hit.")
                save_trade(t)
            elif s and s["state"] in ("CANCELLED", "REJECTED"):
                t["status"] = "ENDED"
                note(t, "The stop order is no longer live (broker end-of-day square-off, or cancelled elsewhere). Check the broker app.")
                save_trade(t)
    except BrokerError:
        pass
    finally:
        EXEC_LOCK.release()


def trading_state():
    rows = sorted(todays(), key=lambda t: t["t"], reverse=True)
    return {"enabled": True, "mode": TRADING_MODE, "broker": BROKER.name, "halted": HALTED, "risk": RISK_PER_TRADE,
            "max_value": MAX_ORDER_VALUE, "max_trades": MAX_TRADES_PER_DAY,
            "trades_today": sum(1 for t in rows if t["status"] not in NOT_TRADED), "trades": rows}


