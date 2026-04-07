# ============================================================
#  server.py
#  OptionLab — FastAPI Bridge
#
#  Flow:
#  1. Reads conIds from GCS universe Excel
#  2. Fetches OHLCV from TWS using conIds (in memory)
#  3. Runs screener script on memory data
#  4. Returns results to dashboard
#  5. Saves to GCS only if "Save for backtesting" checked
#
#  Run in Anaconda Prompt:
#  cd D:\optionlab\scripts
#  python server.py
#
#  Runs on: http://localhost:8000
# ============================================================

import sys
import io
import json
import time
import importlib
import importlib.util
import threading
import pandas as pd
import pandas_ta as ta
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uvicorn

sys.path.insert(0, r"D:\optionlab\scripts")

from ib_core import (
    connect_local, disconnect, is_connected,
    get_app, get_bucket,
    req_manager, wait_for,
    FOLDER_OHLCV, FOLDER_SCREENER
)
from ibapi.contract import Contract
from backtester import run_backtest
from options_backtester import run_options_backtest
import upstox_core as ux
import upstox_client
from datetime import datetime

STRATEGIES_DIR = Path(r"D:\optionlab\scripts\options_strategies")

# ── App setup ─────────────────────────────────────────────────
app = FastAPI(title="OptionLab API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Screeners folder ──────────────────────────────────────────
SCREENERS_DIR = Path(r"D:\optionlab\scripts\screeners")
SCREENERS_DIR.mkdir(exist_ok=True)

# ── In-memory data cache ──────────────────────────────────────
_cache = {
    "frequency": None,   # last fetched frequency
    "periods":   None,   # last fetched periods
    "data":      {},     # {symbol: DataFrame}
    "universe":  {},     # {symbol: conId}
}

# ── Request models ────────────────────────────────────────────
class ScanRequest(BaseModel):
    screener:  str
    frequency: str
    periods:   int
    save:      Optional[bool] = False


# ── Frequency → IB bar size + duration mapping ────────────────
FREQ_MAP = {
    "1 min":   ("1 min",   "7 D"),
    "5 mins":  ("5 mins",  "10 D"),
    "15 mins": ("15 mins", "30 D"),
    "30 mins": ("30 mins", "30 D"),
    "1 hour":  ("1 hour",  "60 D"),
    "4 hours": ("4 hours", "60 D"),
    "1 day":   ("1 day",   "1 Y"),
    "1 week":  ("1 week",  "1 Y"),
    "1 month": ("1 month", "1 Y"),
}


# ============================================================
#  ROUTES
# ============================================================

# ── Status ────────────────────────────────────────────────────
@app.get("/status")
def get_status():
    return {
        "tws_connected":  is_connected(),
        "cache_frequency": _cache["frequency"],
        "cached_stocks":  len(_cache["data"]),
        "universe_loaded": len(_cache["universe"]),
    }


# ── TWS Connection ────────────────────────────────────────────
@app.post("/connect")
def connect_tws():
    """Connect to TWS — called by dashboard Connect button."""
    if is_connected():
        return {"status": "already_connected"}
    instance = connect_local()
    if instance and is_connected():
        return {"status": "connected"}
    raise HTTPException(status_code=500,
                        detail="Could not connect to TWS. Is TWS running?")


@app.post("/disconnect")
def disconnect_tws():
    disconnect()
    return {"status": "disconnected"}


# ── Screeners list ────────────────────────────────────────────
@app.get("/screeners")
def list_screeners():
    """List all screener scripts — populates dashboard dropdown."""
    scripts = []
    for f in SCREENERS_DIR.glob("*.py"):
        scripts.append({
            "name":     f.stem.replace("_", " ").title(),
            "filename": f.name,
        })
    return {"screeners": scripts}


# ── Main scan endpoint ────────────────────────────────────────
@app.post("/scan")
def run_scan(req: ScanRequest):
    """
    Run a screener.
    1. Load universe conIds from GCS
    2. Fetch OHLCV from TWS (cached per frequency)
    3. Run screener script
    4. Return results
    """
    if not is_connected():
        raise HTTPException(status_code=400,
                            detail="Not connected to TWS. Click Connect first.")

    # ── Load universe from GCS (once per session) ─────────────
    if not _cache["universe"]:
        _cache["universe"] = load_universe_from_gcs()
        if not _cache["universe"]:
            raise HTTPException(status_code=500,
                                detail="Could not load universe from GCS.")

    # ── Fetch data from TWS (cached per frequency) ────────────
    if _cache["frequency"] != req.frequency or not _cache["data"]:
        print(f"\n  Fetching data from TWS — frequency: {req.frequency}")
        _cache["data"]      = fetch_all_from_tws(req.frequency)
        _cache["frequency"] = req.frequency
        print(f"  Fetched {len(_cache['data'])} stocks into memory\n")
    else:
        print(f"  Using cached data — {len(_cache['data'])} stocks")

    # ── Load screener script ──────────────────────────────────
    script_path = SCREENERS_DIR / req.screener
    if not script_path.exists():
        raise HTTPException(status_code=404,
                            detail=f"Screener not found: {req.screener}")

    spec   = importlib.util.spec_from_file_location("screener", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # ── Check market conditions ───────────────────────────────
    if hasattr(module, "check_conditions"):
        ok, reason = module.check_conditions()
        if not ok:
            return {
                "status":  "skipped",
                "reason":  reason,
                "results": []
            }

    # ── Run screener on each stock ────────────────────────────
    results = []
    for symbol, df in _cache["data"].items():
        if df is None or df.empty:
            continue
        try:
            signal, reasons = module.screen(df, req.periods)
            results.append({
                "symbol":  symbol,
                "signal":  signal,
                "reasons": reasons,
                "price":   round(float(df["close"].iloc[-1]), 2),
                "change":  round(float(
                    (df["close"].iloc[-1] - df["close"].iloc[-2])
                    / df["close"].iloc[-2] * 100), 2),
                "rsi":     calc_indicator(df, "rsi",      req.periods),
                "adx":     calc_indicator(df, "adx",      req.periods),
                "macd":    calc_indicator(df, "macd",     req.periods),
                "bb_width":calc_indicator(df, "bb_width", req.periods),
                "chart":   df.tail(60)[
                    ["date","open","high","low","close","volume"]
                ].assign(date=lambda x: x["date"].astype(str)).to_dict("records"),
            })
        except Exception as e:
            print(f"  [Error] {symbol}: {e}")
            continue

    # Sort STRONG → WATCH → SKIP
    order = {"STRONG": 0, "WATCH": 1, "SKIP": 2}
    results.sort(key=lambda x: order.get(x["signal"], 3))

    output = {
        "status":    "success",
        "screener":  req.screener,
        "frequency": req.frequency,
        "periods":   req.periods,
        "total":     len(results),
        "strong":    sum(1 for r in results if r["signal"] == "STRONG"),
        "watch":     sum(1 for r in results if r["signal"] == "WATCH"),
        "skip":      sum(1 for r in results if r["signal"] == "SKIP"),
        "results":   results,
    }

    # Save to GCS if requested
    if req.save:
        save_to_gcs(req.screener, output)

    return output


# ── Refresh data ──────────────────────────────────────────────
@app.post("/refresh")
def refresh_data():
    """Force refetch data from TWS — clears cache."""
    _cache["data"]      = {}
    _cache["frequency"] = None
    return {"status": "cache_cleared"}


# ── VIX ───────────────────────────────────────────────────────
@app.get("/vix")
def get_vix():
    try:
        import yfinance as yf
        df  = yf.download("^INDIAVIX", period="5d", progress=False)
        vix = round(float(df["Close"].iloc[-1]), 2)
        return {"vix": vix}
    except:
        return {"vix": None}


# ============================================================
#  HELPERS
# ============================================================

def load_universe_from_gcs():
    """Read conIds from GCS universe Excel → {symbol: conId}."""
    try:
        bucket = get_bucket()
        blob   = bucket.blob("universe/nifty50_universe.xlsx")
        if not blob.exists():
            return {}
        df = pd.read_excel(io.BytesIO(blob.download_as_bytes()))
        df = df[df["ConId"].notna()].reset_index(drop=True)
        universe = {
            str(row["IB_Symbol"]).strip(): int(row["ConId"])
            for _, row in df.iterrows()
        }
        print(f"  Universe loaded from GCS — {len(universe)} stocks")
        return universe
    except Exception as e:
        print(f"  [Error] load_universe: {e}")
        return {}


def fetch_all_from_tws(frequency):
    """
    Fetch OHLCV for all stocks from TWS using conIds.
    Stores in memory — returns {symbol: DataFrame}
    """
    ib_app   = get_app()
    bar_size, duration = FREQ_MAP.get(frequency, ("1 day", "1 Y"))
    data     = {}

    for symbol, conid in _cache["universe"].items():
        try:
            rid      = req_manager.next(f"{symbol}_HIST")
            contract = Contract()
            contract.conId    = int(conid)
            contract.exchange = "NSE"

            ib_app.reqHistoricalData(
                rid, contract, "",
                duration, bar_size,
                "TRADES", 1, 1, False, []
            )

            wait_for(ib_app, rid, timeout=20)
            bars = ib_app.hist_data.get(rid, [])

            if bars:
                df         = pd.DataFrame(bars)
                df["date"] = pd.to_datetime(df["date"])
                df         = df.sort_values("date").reset_index(drop=True)
                data[symbol] = df
                print(f"  Fetched {symbol} — {len(df)} bars")
            else:
                print(f"  No data for {symbol}")

            time.sleep(12)

        except Exception as e:
            print(f"  [Error] {symbol}: {e}")

    return data


def calc_indicator(df, name, periods):
    """Calculate indicator and return latest value."""
    try:
        if name == "rsi":
            val = ta.rsi(df["close"], length=periods)
            return round(float(val.iloc[-1]), 2) if val is not None else None
        elif name == "adx":
            val = ta.adx(df["high"], df["low"], df["close"], length=periods)
            col = [c for c in val.columns if c.startswith("ADX")]
            return round(float(val[col[0]].iloc[-1]), 2) if col else None
        elif name == "macd":
            val = ta.macd(df["close"])
            col = [c for c in val.columns if "MACDh" in c]
            return round(float(val[col[0]].iloc[-1]), 2) if col else None
        elif name == "bb_width":
            val = ta.bbands(df["close"], length=periods)
            col = [c for c in val.columns if "BBB" in c]
            return round(float(val[col[0]].iloc[-1]), 2) if col else None
    except:
        return None


def save_to_gcs(screener_name, data):
    """Save scan results to GCS."""
    try:
        bucket   = get_bucket()
        filename = screener_name.replace(".py", "") + "_results.json"
        path     = f"{FOLDER_SCREENER}/{filename}"
        blob     = bucket.blob(path)
        blob.upload_from_string(
            json.dumps(data, indent=2, default=str),
            content_type="application/json"
        )
        print(f"  Results saved → gs://option_lab_data/{path}")
    except Exception as e:
        print(f"  [Error] save_to_gcs: {e}")

# ── Backtest request model ────────────────────────────────────
class BacktestRequest(BaseModel):
    screener:   str
    symbols:    list
    frequency:  str
    start_date: str
    end_date:   str
    capital:    float

# ── Backtest endpoint ─────────────────────────────────────────
@app.post("/backtest")
def run_backtest_endpoint(req: BacktestRequest):
    if not is_connected():
        raise HTTPException(status_code=400,
                            detail="Not connected to TWS.")
    if not _cache["universe"]:
        _cache["universe"] = load_universe_from_gcs()
        if not _cache["universe"]:
            raise HTTPException(status_code=500,
                                detail="Could not load universe.")

    symbols = req.symbols if req.symbols else list(_cache["universe"].keys())
    conids  = _cache["universe"]
    start   = datetime.strptime(req.start_date, "%Y-%m-%d")
    end     = datetime.strptime(req.end_date,   "%Y-%m-%d")

    result  = run_backtest(
        screener_name = req.screener,
        symbols       = symbols,
        conids        = conids,
        frequency     = req.frequency,
        start_date    = start,
        end_date      = end,
        capital       = req.capital,
    )
    return result
# ============================================================
#  UPSTOX
# ============================================================

class TokenRequest(BaseModel):
    token: str

@app.post("/upstox/token")
def set_upstox_token(req: TokenRequest):
    """Set Upstox access token — called from dashboard daily."""
    try:
        ux.set_token(req.token)
        configuration = upstox_client.Configuration()
        configuration.access_token = req.token
        api = upstox_client.UserApi(upstox_client.ApiClient(configuration))
        profile = api.get_profile("2.0")
        return {
            "status":    "connected",
            "user_name": profile.data.user_name,
            "user_id":   profile.data.user_id,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/upstox/status")
def get_upstox_status():
    return {"connected": ux.is_token_set()}


# ── Options strategies list ───────────────────────────────────
@app.get("/options_strategies")
def list_options_strategies():
    scripts = []
    for f in STRATEGIES_DIR.glob("*.py"):
        try:
            spec   = importlib.util.spec_from_file_location("s", f)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            info = module.get_strategy_info()
            scripts.append({
                "filename":    f.name,
                "name":        info.get("name", f.stem),
                "description": info.get("description", ""),
                "direction":   info.get("direction", "neutral"),
                "legs":        info.get("legs", 2),
            })
        except:
            scripts.append({
                "filename":    f.name,
                "name":        f.stem.replace("_", " ").title(),
                "description": "",
            })
    return {"strategies": scripts}


# ── Options backtest ──────────────────────────────────────────
class OptionsBacktestRequest(BaseModel):
    screener:   str
    strategy:   str
    symbols:    list
    start_date: str
    end_date:   str
    capital:    float


@app.post("/options_backtest")
def run_options_backtest_endpoint(req: OptionsBacktestRequest):
    if not ux.is_token_set():
        raise HTTPException(
            status_code=400,
            detail="Upstox token not set. Please set token in dashboard."
        )
    start  = datetime.strptime(req.start_date, "%Y-%m-%d")
    end    = datetime.strptime(req.end_date,   "%Y-%m-%d")
    result = run_options_backtest(
        screener_name = req.screener,
        strategy_name = req.strategy,
        symbols       = req.symbols,
        start_date    = start,
        end_date      = end,
        capital       = req.capital,
    )
    return result


# ============================================================
#  RUN
# ============================================================
if __name__ == "__main__":
    print("\n" + "="*50)
    print("  OPTIONLAB — Server starting...")
    print("  Dashboard: http://localhost:5173")
    print("  API:       http://localhost:8000")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
