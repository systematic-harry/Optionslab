# ============================================================
#  fetch_data.py
#  Fetches 1 year OHLCV data for all Nifty 50 stocks from IB
#  Reads universe (symbols + conIds) from GCS Excel file
#  Stores as Parquet files in Google Cloud Storage
# ============================================================

import io
import sys
import time
import threading
import datetime
import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from google.cloud import storage

sys.path.insert(0, r"D:\optionlab\scripts")

# ── CONFIG ───────────────────────────────────────────────────
KEY_FILE        = r"D:\optionlab\scripts\optionlab-sa.json"
BUCKET_NAME     = "option_lab_data"
FOLDER_DATA     = "data/daily"
FOLDER_UNIVERSE = "universe"
UNIVERSE_FILE   = "nifty50_universe.xlsx"
HOST            = "127.0.0.1"
PORT            = 7496
CLIENT_ID       = 1


# ── IB APP ───────────────────────────────────────────────────
class IBApp(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)
        self.hist_data        = {}
        self.req_done         = {}
        self.next_req_id      = 1
        self.pacing_violation = False

    def nextValidId(self, orderId):
        print(f"  Connected to TWS. OrderId: {orderId}")
        self.next_req_id = orderId

    def error(self, reqId, errorCode, errorString,
              advancedOrderRejectJson="", errorTime=""):
        if errorCode in [2104, 2106, 2107, 2108, 2158, 2119, 2100]:
            return
        if errorCode == 504:
            print("  [IB] Pacing violation — will wait 10 minutes...")
            self.pacing_violation = True
            return
        print(f"  [Error {errorCode}] {errorString}")

    def historicalData(self, reqId, bar):
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


# ── HELPERS ──────────────────────────────────────────────────
def get_bucket():
    client = storage.Client.from_service_account_json(KEY_FILE)
    return client.bucket(BUCKET_NAME)


def load_universe(bucket):
    """Read nifty50_universe.xlsx from GCS — returns DataFrame."""
    print("  Loading universe from GCS...")
    path = f"{FOLDER_UNIVERSE}/{UNIVERSE_FILE}"
    blob = bucket.blob(path)
    if not blob.exists():
        print(f"  [Error] Universe file not found: {path}")
        return None
    df = pd.read_excel(io.BytesIO(blob.download_as_bytes()))
    df = df[df["ConId"].notna()].reset_index(drop=True)
    print(f"  Loaded {len(df)} stocks with conIds")
    return df


def file_exists(bucket, symbol):
    return bucket.blob(f"{FOLDER_DATA}/{symbol}.parquet").exists()


def upload_parquet(bucket, symbol, df):
    path = f"{FOLDER_DATA}/{symbol}.parquet"
    bucket.blob(path).upload_from_string(
        df.to_parquet(index=False),
        content_type="application/octet-stream"
    )
    print(f"  Uploaded → gs://{BUCKET_NAME}/{path}")


def download_parquet(bucket, symbol):
    blob = bucket.blob(f"{FOLDER_DATA}/{symbol}.parquet")
    if not blob.exists():
        return None
    return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))


def wait_for(app, req_id, timeout=20):
    start = time.time()
    while req_id not in app.req_done:
        if time.time() - start > timeout:
            print(f"  Timeout reqId={req_id}")
            return False
        time.sleep(0.1)
    return True


def fetch_history(app, conid, symbol, duration="1 Y"):
    req_id = app.next_req_id
    app.next_req_id += 1

    contract          = Contract()
    contract.conId    = int(conid)
    contract.exchange = "NSE"

    app.reqHistoricalData(
        req_id, contract, "",
        duration, "1 day", "TRADES",
        1, 1, False, []
    )
    wait_for(app, req_id, timeout=20)
    return app.hist_data.get(req_id, [])


# ── MAIN ─────────────────────────────────────────────────────
def run(mode="full"):
    print("\n" + "="*50)
    print(f"  OPTIONLAB — Fetch Data ({mode.upper()} mode)")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    # Connect to GCS
    bucket   = get_bucket()

    # Load universe from GCS
    universe = load_universe(bucket)
    if universe is None:
        return

    # Connect to TWS
    print("\n  Connecting to TWS...")
    app    = IBApp()
    app.connect(HOST, PORT, CLIENT_ID)
    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()
    time.sleep(2)

    duration = "1 Y" if mode == "full" else "1 D"
    success  = 0
    failed   = []

    for _, row in universe.iterrows():
        name   = str(row["Name"]).strip()
        symbol = str(row["IB_Symbol"]).strip()
        conid  = int(row["ConId"])

       
        # Auto wait if pacing violation
        if app.pacing_violation:
            print("\n  Waiting 10 minutes for IB pacing reset...")
            time.sleep(600)
            app.pacing_violation = False

        bars = fetch_history(app, conid, symbol, duration=duration)

        if not bars:
            print("NO DATA")
            failed.append((name, symbol))
            time.sleep(12)
            continue

        df         = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df["date"])
        df         = df.sort_values("date").reset_index(drop=True)

        if mode == "daily":
            existing = download_parquet(bucket, symbol)
            if existing is not None:
                existing["date"] = pd.to_datetime(existing["date"])
                df = pd.concat([existing, df]).drop_duplicates(
                    subset="date").sort_values("date").reset_index(drop=True)

        upload_parquet(bucket, symbol, df)
        print(f"{len(df)} bars")
        success += 1
        time.sleep(12)

    app.disconnect()

    print(f"\n{'='*50}")
    print(f"  Done! {success} uploaded.")
    if failed:
        print(f"  Failed: {[s for _, s in failed]}")
    print(f"{'='*50}\n")


# ── RUN ──────────────────────────────────────────────────────
run(mode="full")
# run(mode="daily")
