# ============================================================
#  ib_core.py
#  OptionLab — Core Foundation File
#
#  Every other script just does:
#  from ib_core import get_app, fetch_history, upload_to_cloud
# ============================================================

import time
import threading
import io
import json
import pandas as pd
import numpy as np
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from google.cloud import storage

# ============================================================
#  CONFIG
# ============================================================

LOCAL_HOST      = "127.0.0.1"
LOCAL_PORT      = 7496
LOCAL_CLIENT_ID = 1

CLOUD_HOST      = "YOUR_VM_IP"
CLOUD_PORT      = 4002
CLOUD_CLIENT_ID = 1

KEY_FILE        = r"D:\optionlab\scripts\optionlab-sa.json"
BUCKET_NAME     = "option_lab_data"
FOLDER_OHLCV    = "data/daily"
FOLDER_SCREENER = "screener_output"
FOLDER_BACKTEST = "backtest_results"

NIFTY_50 = {
    "RELIANCE":   "RELIANCE",
    "TCS":        "TCS",
    "HDFCBANK":   "HDFCBANK",
    "INFY":       "INFY",
    "ICICIBANK":  "ICICIBANK",
    "HINDUNILVR": "HINDUNILVR",
    "ITC":        "ITC",
    "SBIN":       "SBIN",
    "BHARTIARTL": "BHARTIARTL",
    "KOTAKBANK":  "KOTAKBANK",
    "LT":         "LT",
    "AXISBANK":   "AXISBANK",
    "ASIANPAINT": "ASIANPAINT",
    "MARUTI":     "MARUTI",
    "TITAN":      "TITAN",
    "SUNPHARMA":  "SUNPHARMA",
    "ULTRACEMCO": "ULTRACEMCO",
    "WIPRO":      "WIPRO",
    "NESTLEIND":  "NESTLEIND",
    "BAJFINANCE": "BAJFINANCE",
    "POWERGRID":  "POWERGRID",
    "NTPC":       "NTPC",
    "TATAMOTORS": "TATAMOTORS",
    "HCLTECH":    "HCLTECH",
    "JSWSTEEL":   "JSWSTEEL",
    "TATASTEEL":  "TATASTEEL",
    "TECHM":      "TECHM",
    "INDUSINDBK": "INDUSINDBK",
    "ADANIENT":   "ADANIENT",
    "ADANIPORTS": "ADANIPORTS",
    "COALINDIA":  "COALINDIA",
    "DRREDDY":    "DRREDDY",
    "BAJAJFINSV": "BAJAJFINSV",
    "GRASIM":     "GRASIM",
    "CIPLA":      "CIPLA",
    "HINDALCO":   "HINDALCO",
    "EICHERMOT":  "EICHERMOT",
    "BPCL":       "BPCL",
    "TATACONSUM": "TATACONSUM",
    "BRITANNIA":  "BRITANNIA",
    "APOLLOHOSP": "APOLLOHOSP",
    "ONGC":       "ONGC",
    "SBILIFE":    "SBILIFE",
    "HDFCLIFE":   "HDFCLIFE",
    "DIVISLAB":   "DIVISLAB",
    "BAJAJ-AUTO": "BAJAJ-AUTO",
    "HEROMOTOCO": "HEROMOTOCO",
    "UPL":        "UPL",
    "SHREECEM":   "SHREECEM",
    "M&M":        "M&M",
}

IGNORE_CODES = {2104, 2106, 2107, 2108, 2158, 2119, 2100}


# ============================================================
#  REQUEST ID MANAGER
# ============================================================

class ReqIdManager:

    def __init__(self):
        self._id   = 1
        self._map  = {}
        self._lock = threading.Lock()

    def next(self, label=""):
        with self._lock:
            rid = self._id
            self._map[rid] = label
            self._id += 1
            return rid

    def label(self, rid):
        return self._map.get(rid, "unknown")

    def release(self, rid):
        with self._lock:
            if rid in self._map:
                del self._map[rid]

    def release_all(self):
        with self._lock:
            self._map.clear()
            self._id = 1

    def active(self):
        return dict(self._map)


req_manager = ReqIdManager()


# ============================================================
#  IB APP CLASS
# ============================================================

class IBApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)
        self.hist_data    = {}
        self.live_data    = {}
        self.option_data  = {}
        self.contract_map = {}
        self.req_done     = {}
        self.next_req_id  = 1
        self.connected    = False
        self.conn_mode    = None
        self._lock        = threading.Lock()

    def nextValidId(self, orderId):
        self.next_req_id = orderId
        self.connected   = True
        req_manager._id  = orderId
        print(f"  [TWS] Connected — Next Order ID: {orderId}")

    def connectionClosed(self):
        self.connected = False
        print("  [TWS] Connection closed.")

    def error(self, reqId, errorCode, errorString,
              advancedOrderRejectJson="", errorTime=""):
        if errorCode in IGNORE_CODES:
            return
        print(f"  [TWS Error {errorCode}] reqId={reqId} — {errorString}")

    def historicalData(self, reqId, bar):
        with self._lock:
            if reqId not in self.hist_data:
                self.hist_data[reqId] = []
            self.hist_data[reqId].append({
                "date":   bar.date,
                "open":   round(bar.open,  2),
                "high":   round(bar.high,  2),
                "low":    round(bar.low,   2),
                "close":  round(bar.close, 2),
                "volume": int(bar.volume),
            })

    def historicalDataEnd(self, reqId, start, end):
        self.req_done[reqId] = True
        bars  = len(self.hist_data.get(reqId, []))
        label = req_manager.label(reqId)
        print(f"  [TWS] Historical data done — {label} ({bars} bars)")

    def historicalDataUpdate(self, reqId, bar):
        with self._lock:
            if reqId not in self.hist_data:
                self.hist_data[reqId] = []
            self.hist_data[reqId].append({
                "date":   bar.date,
                "open":   round(bar.open,  2),
                "high":   round(bar.high,  2),
                "low":    round(bar.low,   2),
                "close":  round(bar.close, 2),
                "volume": int(bar.volume),
            })

    def tickPrice(self, reqId, tickType, price, attrib):
        if price <= 0:
            return
        with self._lock:
            if reqId not in self.live_data:
                self.live_data[reqId] = {}
            tick_map = {
                1:  "bid",
                2:  "ask",
                4:  "last",
                6:  "high",
                7:  "low",
                9:  "close",
                14: "open",
            }
            if tickType in tick_map:
                self.live_data[reqId][tick_map[tickType]] = round(price, 2)

    def tickSize(self, reqId, tickType, size):
        with self._lock:
            if reqId not in self.live_data:
                self.live_data[reqId] = {}
            if tickType == 8:
                self.live_data[reqId]["volume"] = int(size)
            if tickType == 74:
                self.live_data[reqId]["last_size"] = int(size)

    def tickGeneric(self, reqId, tickType, value):
        with self._lock:
            if reqId not in self.live_data:
                self.live_data[reqId] = {}
            self.live_data[reqId][f"tick_{tickType}"] = round(value, 4)

    def tickSnapshotEnd(self, reqId):
        self.req_done[reqId] = True

    def tickOptionComputation(self, reqId, tickType, tickAttrib,
                               impliedVol, delta, optPrice,
                               pvDividend, gamma, vega, theta, undPrice):
        with self._lock:
            if reqId not in self.option_data:
                self.option_data[reqId] = {}
            d = self.option_data[reqId]
            if impliedVol and impliedVol > 0:
                d["iv"]    = round(impliedVol * 100, 2)
            if delta is not None:
                d["delta"] = round(delta,  4)
            if gamma is not None:
                d["gamma"] = round(gamma,  6)
            if theta is not None:
                d["theta"] = round(theta,  2)
            if vega is not None:
                d["vega"]  = round(vega,   2)
            if optPrice and optPrice > 0:
                d["ltp"]   = round(optPrice, 2)
            if undPrice and undPrice > 0:
                d["spot"]  = round(undPrice, 2)

    def contractDetails(self, reqId, contractDetails):
        with self._lock:
            self.contract_map[reqId] = {
                "symbol":   contractDetails.contract.symbol,
                "conid":    contractDetails.contract.conId,
                "expiry":   contractDetails.contract.lastTradeDateOrContractMonth,
                "strike":   contractDetails.contract.strike,
                "right":    contractDetails.contract.right,
                "exchange": contractDetails.contract.exchange,
                "lot_size": contractDetails.minSize,
            }

    def contractDetailsEnd(self, reqId):
        self.req_done[reqId] = True


# ============================================================
#  CONNECTION MANAGER
# ============================================================

_app_instance = None
_app_lock     = threading.Lock()


def get_app():
    return _app_instance


def is_connected():
    return _app_instance is not None and _app_instance.connected


def connect_local():
    global _app_instance
    with _app_lock:
        if is_connected():
            print("  [TWS] Already connected.")
            return _app_instance

        print(f"\n  Connecting to local TWS on port {LOCAL_PORT}...")
        app           = IBApp()
        app.conn_mode = "local"
        app.connect(LOCAL_HOST, LOCAL_PORT, LOCAL_CLIENT_ID)

        thread = threading.Thread(target=app.run, daemon=True)
        thread.start()
        time.sleep(2)

        if app.connected:
            _app_instance = app
            print("  [TWS] Local connection established!")
        else:
            print("  [TWS] Connection failed. Is TWS running?")

        return _app_instance


def connect_cloud():
    global _app_instance
    with _app_lock:
        if is_connected():
            print("  [Gateway] Already connected.")
            return _app_instance

        print(f"\n  Connecting to IB Gateway on {CLOUD_HOST}:{CLOUD_PORT}...")
        app           = IBApp()
        app.conn_mode = "cloud"
        app.connect(CLOUD_HOST, CLOUD_PORT, CLOUD_CLIENT_ID)

        thread = threading.Thread(target=app.run, daemon=True)
        thread.start()
        time.sleep(2)

        if app.connected:
            _app_instance = app
            print("  [Gateway] Cloud connection established!")
        else:
            print("  [Gateway] Connection failed. Is IB Gateway running?")

        return _app_instance


def reconnect():
    global _app_instance
    if _app_instance is None:
        return
    mode = _app_instance.conn_mode
    disconnect()
    time.sleep(3)
    if mode == "local":
        connect_local()
    elif mode == "cloud":
        connect_cloud()


def disconnect():
    global _app_instance
    if _app_instance is not None:
        try:
            _app_instance.disconnect()
        except:
            pass
        _app_instance = None
        req_manager.release_all()
        print("  [TWS] Disconnected.")


# ============================================================
#  CONTRACT BUILDERS
# ============================================================

def make_stock(symbol):
    c          = Contract()
    c.symbol   = symbol
    c.secType  = "STK"
    c.exchange = "NSE"
    c.currency = "INR"
    return c


def make_index(symbol):
    c          = Contract()
    c.symbol   = symbol
    c.secType  = "IND"
    c.exchange = "NSE"
    c.currency = "INR"
    return c


def make_futures(symbol, expiry):
    c          = Contract()
    c.symbol   = symbol
    c.secType  = "FUT"
    c.exchange = "NSE"
    c.currency = "INR"
    c.lastTradeDateOrContractMonth = expiry
    return c


def make_option(symbol, strike, right, expiry, multiplier="50"):
    c            = Contract()
    c.symbol     = symbol
    c.secType    = "OPT"
    c.exchange   = "NSE"
    c.currency   = "INR"
    c.strike     = float(strike)
    c.right      = right
    c.lastTradeDateOrContractMonth = expiry
    c.multiplier = multiplier
    return c


# ============================================================
#  WAIT / SYNC HELPERS
# ============================================================

def wait_for(app, req_id, timeout=20):
    start = time.time()
    while req_id not in app.req_done:
        if time.time() - start > timeout:
            print(f"  [Timeout] reqId={req_id} ({req_manager.label(req_id)})")
            return False
        time.sleep(0.1)
    req_manager.release(req_id)
    return True


def wait_for_all(app, req_ids, timeout=30):
    start   = time.time()
    pending = set(req_ids)
    while pending:
        if time.time() - start > timeout:
            print(f"  [Timeout] Still pending: {pending}")
            return False
        done    = {r for r in pending if r in app.req_done}
        pending -= done
        time.sleep(0.1)
    for r in req_ids:
        req_manager.release(r)
    return True


def cancel_market_data(app, req_id):
    try:
        app.cancelMktData(req_id)
        req_manager.release(req_id)
    except:
        pass


# ============================================================
#  DATA FETCH FUNCTIONS
# ============================================================

def fetch_history(app, symbol, duration="1 Y", bar_size="1 day",
                  sec_type="STK", expiry=None):
    if not is_connected():
        print("  [Error] Not connected to TWS.")
        return None

    rid = req_manager.next(f"{symbol}_HIST")

    if sec_type == "IND":
        contract = make_index(symbol)
    elif sec_type == "FUT":
        contract = make_futures(symbol, expiry)
    else:
        contract = make_stock(symbol)

    app.reqHistoricalData(
        rid,
        contract,
        "",
        duration,
        bar_size,
        "TRADES",
        1,
        1,
        False,
        []
    )

    success = wait_for(app, rid, timeout=20)
    bars    = app.hist_data.get(rid, [])

    if not bars:
        print(f"  [Warning] No data returned for {symbol}")
        return None

    df         = pd.DataFrame(bars)
    df["date"] = pd.to_datetime(df["date"])
    df         = df.sort_values("date").reset_index(drop=True)

    print(f"  Fetched {symbol} — {len(df)} bars")
    return df


def fetch_live_price(app, symbol, sec_type="STK", timeout=5):
    if not is_connected():
        print("  [Error] Not connected to TWS.")
        return None

    rid = req_manager.next(f"{symbol}_LIVE")

    if sec_type == "IND":
        contract = make_index(symbol)
    else:
        contract = make_stock(symbol)

    app.reqMktData(rid, contract, "", True, False, [])
    time.sleep(timeout)
    cancel_market_data(app, rid)

    data  = app.live_data.get(rid, {})
    price = data.get("last") or data.get("close") or 0

    print(f"  Live price {symbol}: {price}")
    return data


def fetch_option_chain(app, symbol, expiry, strikes=None,
                       spot=None, width=500):
    if not is_connected():
        print("  [Error] Not connected to TWS.")
        return []

    if strikes is None and spot is not None:
        atm     = round(spot / 50) * 50
        strikes = [atm + i * 50 for i in range(-5, 6)]

    req_map = {}

    for strike in strikes:
        for right in ["C", "P"]:
            rid      = req_manager.next(f"{symbol}_{strike}{right}")
            contract = make_option(symbol, strike, right, expiry)
            app.reqMktData(rid, contract, "100,101", False, False, [])
            req_map[rid] = (strike, right)
            time.sleep(0.05)

    print(f"  Waiting for {len(req_map)} option quotes...")
    time.sleep(5)

    for rid in req_map:
        cancel_market_data(app, rid)

    chain = {}
    for rid, (strike, right) in req_map.items():
        opt  = app.option_data.get(rid, {})
        tick = app.live_data.get(rid, {})
        ltp  = opt.get("ltp") or tick.get("last") or tick.get("close") or 0

        if strike not in chain:
            chain[strike] = {
                "strike": strike,
                "atm":    spot and abs(strike - spot) < 75,
            }

        side = "call" if right == "C" else "put"
        chain[strike][side] = {
            "ltp":   round(ltp, 2),
            "iv":    opt.get("iv",    0),
            "delta": opt.get("delta", 0),
            "gamma": opt.get("gamma", 0),
            "theta": opt.get("theta", 0),
            "vega":  opt.get("vega",  0),
            "oi":    tick.get("volume", 0),
        }

    result = sorted(chain.values(), key=lambda x: x["strike"])
    print(f"  Option chain: {len(result)} strikes fetched")
    return result


# ============================================================
#  GOOGLE CLOUD STORAGE
# ============================================================

def get_bucket():
    client = storage.Client.from_service_account_json(KEY_FILE)
    return client.bucket(BUCKET_NAME)


def upload_to_cloud(symbol, df, folder=None):
    folder = folder or FOLDER_OHLCV
    path   = f"{folder}/{symbol}.parquet"
    bucket = get_bucket()
    blob   = bucket.blob(path)
    blob.upload_from_string(
        df.to_parquet(index=False),
        content_type="application/octet-stream"
    )
    print(f"  Uploaded -> gs://{BUCKET_NAME}/{path}")


def download_from_cloud(symbol, folder=None):
    folder = folder or FOLDER_OHLCV
    path   = f"{folder}/{symbol}.parquet"
    bucket = get_bucket()
    blob   = bucket.blob(path)

    if not blob.exists():
        print(f"  [GCS] File not found: {path}")
        return None

    data = blob.download_as_bytes()
    df   = pd.read_parquet(io.BytesIO(data))
    print(f"  Downloaded {symbol} — {len(df)} rows")
    return df


def file_exists_on_cloud(symbol, folder=None):
    folder = folder or FOLDER_OHLCV
    path   = f"{folder}/{symbol}.parquet"
    bucket = get_bucket()
    return bucket.blob(path).exists()


def list_cloud_files(folder=None):
    folder = folder or FOLDER_OHLCV
    bucket = get_bucket()
    blobs  = bucket.list_blobs(prefix=folder)
    files  = [b.name for b in blobs]
    print(f"  GCS files in {folder}: {len(files)}")
    return files


def upload_json(filename, data, folder=None):
    folder = folder or FOLDER_SCREENER
    path   = f"{folder}/{filename}"
    bucket = get_bucket()
    blob   = bucket.blob(path)
    blob.upload_from_string(
        json.dumps(data, indent=2, default=str),
        content_type="application/json"
    )
    print(f"  Uploaded JSON -> gs://{BUCKET_NAME}/{path}")


def download_json(filename, folder=None):
    folder = folder or FOLDER_SCREENER
    path   = f"{folder}/{filename}"
    bucket = get_bucket()
    blob   = bucket.blob(path)

    if not blob.exists():
        print(f"  [GCS] JSON not found: {path}")
        return None

    data = json.loads(blob.download_as_text())
    return data


# ============================================================
#  STATUS CHECK
# ============================================================

def status():
    print("\n" + "="*50)
    print("  OPTIONLAB — System Status")
    print("="*50)
    print(f"  TWS Connected : {is_connected()}")
    if _app_instance:
        print(f"  Mode          : {_app_instance.conn_mode}")
        print(f"  Next Req ID   : {_app_instance.next_req_id}")
    print(f"  GCS Bucket    : {BUCKET_NAME}")
    print(f"  Universe      : {len(NIFTY_50)} stocks")
    print(f"  Active Reqs   : {len(req_manager.active())}")
    print("="*50 + "\n")
