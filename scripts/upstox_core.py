# ============================================================
#  upstox_core.py
#  OptionLab — Upstox Data Core (REST API)
#
#  All Upstox API calls via requests (no SDK dependency)
#  If Upstox updates API → only change URLs/params here
#
#  Usage:
#  from upstox_core import set_token, get_underlying, ...
# ============================================================

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

# ── Base URL ──────────────────────────────────────────────────
BASE_URL = "https://api.upstox.com/v2"

# ── Global token store ────────────────────────────────────────
_token = None


# ============================================================
#  TOKEN MANAGEMENT
# ============================================================

def set_token(token: str):
    """Store Upstox access token."""
    global _token
    _token = token.strip()
    print(f"  Upstox token set ✓")


def get_token() -> Optional[str]:
    """Retrieve stored token."""
    return _token


def is_token_set() -> bool:
    return _token is not None and len(_token) > 0


def _headers() -> dict:
    """Return auth headers for all requests."""
    if not is_token_set():
        raise ValueError("Upstox token not set. Call set_token() first.")
    return {
        "Content-Type": "application/json",
        "Accept":        "application/json",
        "Authorization": f"Bearer {_token}"
    }


def _get(url: str, params: dict = None) -> dict:
    """Generic GET request with error handling."""
    try:
        r = requests.get(url, headers=_headers(), params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"  [Error] {r.status_code}: {r.text}")
            return {}
    except Exception as e:
        print(f"  [Error] Request failed: {e}")
        return {}


# ============================================================
#  UNDERLYING DATA
# ============================================================

def get_underlying(symbol: str, interval: str = "day",
                   from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """
    Fetch OHLCV for underlying stock/index from Upstox.

    symbol   → Upstox instrument key e.g. "NSE_EQ|INE002A01018" (RELIANCE)
                                        or "NSE_INDEX|Nifty 50"
    interval → day, 1minute, 5minute, 15minute, 30minute
    """
    if to_date is None:
        to_date = datetime.now().strftime("%Y-%m-%d")
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    url  = f"{BASE_URL}/historical-candle/{symbol}/{interval}/{to_date}/{from_date}"
    data = _get(url)

    if not data or "data" not in data:
        return pd.DataFrame()

    candles = data["data"].get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles,
                      columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# ============================================================
#  EXPIRY DATES
# ============================================================

def get_expiries(instrument_key: str) -> list:
    """
    Get list of expired expiry dates for an underlying.

    instrument_key → "NSE_INDEX|Nifty 50" or "NSE_EQ|INE002A01018"
    Returns list of date strings e.g. ["2024-10-03", "2024-10-10", ...]
    """
    return get_expired_expiries(instrument_key)


def get_expired_expiries(instrument_key: str) -> list:
    """Get expired expiry dates via ExpiredInstrument API."""
    url    = f"{BASE_URL}/expired-instruments/expiries"
    params = {"instrument_key": instrument_key}
    data   = _get(url, params)

    if not data or "data" not in data:
        return []

    return sorted(data.get("data", []))


# ============================================================
#  OPTION CONTRACTS
# ============================================================

def get_contracts(instrument_key: str, expiry_date: str) -> pd.DataFrame:
    """
    Get all option contracts for a given underlying and expiry.

    Returns DataFrame with columns:
    instrument_key, trading_symbol, strike_price, instrument_type (CE/PE), lot_size
    """
    url    = f"{BASE_URL}/option/chain"
    params = {
        "instrument_key": instrument_key,
        "expiry_date":    expiry_date
    }
    data = _get(url, params)

    if not data or "data" not in data:
        return pd.DataFrame()

    records = []
    for item in data.get("data", []):
        for opt_type in ["call_options", "put_options"]:
            opt = item.get(opt_type, {})
            if not opt:
                continue
            md = opt.get("market_data", {})
            records.append({
                "instrument_key":  opt.get("instrument_key", ""),
                "trading_symbol":  opt.get("instrument_key", ""),
                "strike_price":    item.get("strike_price", 0),
                "instrument_type": "CE" if opt_type == "call_options" else "PE",
                "lot_size":        opt.get("lot_size", 1),
                "ltp":             md.get("ltp", 0),
                "oi":              md.get("oi", 0),
                "volume":          md.get("volume", 0),
            })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("strike_price").reset_index(drop=True)
    return df


def get_expired_contracts(instrument_key: str, expiry_date: str) -> pd.DataFrame:
    """
    Get expired option contracts for a given underlying and expiry.
    Uses ExpiredInstrumentApi endpoint.
    """
    url    = f"{BASE_URL}/expired-instruments/contracts"
    params = {
        "instrument_key": instrument_key,
        "expiry_date":    expiry_date
    }
    data = _get(url, params)

    if not data or "data" not in data:
        return pd.DataFrame()

    records = []
    for c in data.get("data", []):
        records.append({
            "instrument_key":  c.get("instrument_key", ""),
            "trading_symbol":  c.get("trading_symbol", ""),
            "strike_price":    c.get("strike_price", 0),
            "instrument_type": c.get("instrument_type", ""),
            "lot_size":        c.get("lot_size", 1),
            "expiry":          c.get("expiry", ""),
        })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("strike_price").reset_index(drop=True)
    return df


# ============================================================
#  OPTION OHLCV
# ============================================================

def get_option_ohlcv(instrument_key: str, interval: str = "day",
                     from_date: str = None, to_date: str = None,
                     expired: bool = True) -> pd.DataFrame:
    """
    Fetch OHLCV for a specific option contract.

    instrument_key → from get_contracts() e.g. "NSE_FO|58718|03-10-2024"
    interval       → day, 1minute, 5minute, 15minute, 30minute
    expired        → True for expired contracts, False for active
    """
    if to_date is None:
        to_date = datetime.now().strftime("%Y-%m-%d")
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    if expired:
        url = f"{BASE_URL}/expired-instruments/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
    else:
        url = f"{BASE_URL}/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"

    data = _get(url)

    if not data or "data" not in data:
        return pd.DataFrame()

    candles = data["data"].get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles,
                      columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


def get_option_live(instrument_key: str) -> dict:
    """
    Get live LTP + OI for a specific option contract.
    Used for live monitoring during market hours.
    """
    url    = f"{BASE_URL}/market-quote/ltp"
    params = {"instrument_key": instrument_key}
    data   = _get(url, params)

    if not data or "data" not in data:
        return {}

    quote = data["data"].get(instrument_key.replace("|", "_"), {})
    return {
        "ltp":    quote.get("last_price", 0),
        "close":  quote.get("close_price", 0),
        "change": quote.get("net_change", 0),
    }


def get_live_ohlcv(instrument_key: str, interval: str = "30minute") -> pd.DataFrame:
    """
    Get current day intraday OHLCV.
    interval → 1minute or 30minute
    """
    url  = f"{BASE_URL}/historical-candle/intraday/{instrument_key}/{interval}"
    data = _get(url)

    if not data or "data" not in data:
        return pd.DataFrame()

    candles = data["data"].get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles,
                      columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# ============================================================
#  ANALYTICS
# ============================================================

def get_hv(symbol: str, days: int = 30,
           from_date: str = None, to_date: str = None) -> float:
    """
    Calculate Historical Volatility (annualised) for a symbol.

    Uses daily close returns standard deviation × sqrt(252)
    """
    df = get_underlying(symbol, "day", from_date, to_date)

    if df.empty or len(df) < days:
        return 0.0

    df = df.tail(days + 1)
    returns = df["close"].pct_change().dropna()
    hv = float(returns.std() * np.sqrt(252))
    return round(hv, 4)


def get_1sd_range(price: float, hv: float, days: int = 30) -> dict:
    """
    Calculate 1 Standard Deviation range for options strategy selection.

    price → current underlying price
    hv    → historical volatility (annualised, e.g. 0.15 = 15%)
    days  → days to expiry

    Returns:
    {
        "upper": price + 1SD move,
        "lower": price - 1SD move,
        "sd":    1SD move in points
    }
    """
    sd_move = price * hv * np.sqrt(days / 252)
    return {
        "upper": round(price + sd_move, 2),
        "lower": round(price - sd_move, 2),
        "sd":    round(sd_move, 2),
        "hv":    round(hv * 100, 2),
    }


def find_strike(contracts_df: pd.DataFrame, target_price: float,
                option_type: str = "CE", offset: int = 0) -> dict:
    """
    Find nearest strike to target price.

    target_price → ATM = current price, OTM = price ± offset
    option_type  → "CE" or "PE"
    offset       → number of strikes away from ATM (0 = ATM)

    Returns contract dict with instrument_key, strike_price etc.
    """
    if contracts_df.empty:
        return {}

    df = contracts_df[contracts_df["instrument_type"] == option_type].copy()
    if df.empty:
        return {}

    # Find ATM strike
    df["diff"] = abs(df["strike_price"] - target_price)
    df = df.sort_values("diff").reset_index(drop=True)

    # Apply offset
    atm_idx = 0
    target_idx = atm_idx + offset
    target_idx = max(0, min(target_idx, len(df) - 1))

    return df.iloc[target_idx].to_dict()


def get_atm_strike(contracts_df: pd.DataFrame, price: float) -> float:
    """Get ATM strike price closest to current price."""
    if contracts_df.empty:
        return 0.0
    strikes = contracts_df["strike_price"].unique()
    return float(min(strikes, key=lambda x: abs(x - price)))


# ============================================================
#  INSTRUMENT KEY HELPERS
# ============================================================

# Common instrument keys
INSTRUMENT_KEYS = {
    "NIFTY":     "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY":  "NSE_INDEX|Nifty Fin Service",
    "SENSEX":    "BSE_INDEX|SENSEX",
}

# NSE stock instrument keys (add more as needed)
STOCK_KEYS = {
    "RELIANCE":  "NSE_EQ|INE002A01018",
    "TCS":       "NSE_EQ|INE467B01029",
    "INFY":      "NSE_EQ|INE009A01021",
    "HDFCBANK":  "NSE_EQ|INE040A01034",
    "ICICIBANK": "NSE_EQ|INE090A01021",
    "WIPRO":     "NSE_EQ|INE075A01022",
    "BAJFINANCE":"NSE_EQ|INE296A01024",
    "TATASTEEL": "NSE_EQ|INE081A01020",
    "AXISBANK":  "NSE_EQ|INE238A01034",
    "SBIN":      "NSE_EQ|INE062A01020",
}


def get_instrument_key(symbol: str) -> str:
    """
    Get Upstox instrument key for a symbol.
    Checks indices first, then stocks.
    """
    sym = symbol.upper()
    if sym in INSTRUMENT_KEYS:
        return INSTRUMENT_KEYS[sym]
    if sym in STOCK_KEYS:
        return STOCK_KEYS[sym]
    return ""


def search_instrument(query: str) -> list:
    """
    Search for instrument key by name.
    Returns list of matching instruments.
    """
    url    = f"{BASE_URL}/instruments/search"
    params = {"query": query}
    data   = _get(url, params)

    if not data or "data" not in data:
        return []

    return data.get("data", [])
