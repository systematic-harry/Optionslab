# ============================================================
#  options_backtester.py
#  OptionLab — Options Backtest Engine (Infrastructure)
#
#  Pure infrastructure — no strategy logic here.
#  Strategy logic lives in options_strategies/ scripts.
#  Signal logic lives in screeners/ scripts.
#
#  Flow:
#  1. Load screener script → get signals on stocks
#  2. Load strategy script → get legs definition
#  3. Fetch contracts from Upstox
#  4. Fetch OHLCV for each leg
#  5. Calculate P&L + metrics
# ============================================================

import sys
import importlib
import importlib.util
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime

sys.path.insert(0, r"D:\optionlab\scripts")
import upstox_core as ux
import upstox_client

SCREENERS_DIR   = Path(r"D:\optionlab\scripts\screeners")
STRATEGIES_DIR  = Path(r"D:\optionlab\scripts\options_strategies")


# ============================================================
#  HELPERS
# ============================================================

def get_next_expiry(expiries: list, signal_date: str) -> str:
    """Find nearest expiry on or after signal date."""
    for exp in sorted(expiries):
        if exp >= signal_date:
            return exp
    return expiries[-1] if expiries else None


def find_contract(contracts_df: pd.DataFrame,
                  option_type: str, target_price: float) -> dict:
    """Find nearest contract to target price."""
    df = contracts_df[contracts_df["instrument_type"] == option_type].copy()
    if df.empty:
        return {}
    df["diff"] = abs(df["strike_price"] - target_price)
    return df.nsmallest(1, "diff").iloc[0].to_dict()


def fetch_leg_prices(instrument_key: str,
                     entry_date: str, exit_date: str) -> dict:
    """Fetch entry and exit prices for a leg."""
    df = ux.get_option_ohlcv(
        instrument_key,
        interval  = "day",
        from_date = entry_date,
        to_date   = exit_date,
        expired   = True
    )

    if df.empty:
        return {"entry": 0, "exit": 0, "found": False}

    entry_rows = df[df["datetime"].dt.strftime("%Y-%m-%d") == entry_date]
    exit_rows  = df[df["datetime"].dt.strftime("%Y-%m-%d") == exit_date]

    if entry_rows.empty:
        entry_rows = df.head(1)
    if exit_rows.empty:
        exit_rows = df.tail(1)

    return {
        "entry": round(float(entry_rows.iloc[0]["close"]), 2),
        "exit":  round(float(exit_rows.iloc[-1]["close"]), 2),
        "found": True,
    }


# ============================================================
#  LOAD MODULES
# ============================================================

def load_module(path: Path):
    spec   = importlib.util.spec_from_file_location("mod", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_upstox_api():
    configuration = upstox_client.Configuration()
    configuration.access_token = ux.get_token()
    return upstox_client.ExpiredInstrumentApi(
        upstox_client.ApiClient(configuration))


# ============================================================
#  EXECUTE TRADE
# ============================================================

def execute_trade(legs_def: list, contracts_df: pd.DataFrame,
                  underlying_price: float, sd_move: float,
                  entry_date: str, exit_date: str) -> dict:
    """
    Execute strategy legs and calculate P&L.

    legs_def → from strategy script get_legs()
    """
    executed_legs = []

    for leg in legs_def:
        opt_type     = leg["type"]          # CE or PE
        action       = leg["action"]        # BUY or SELL
        target_price = leg.get("target_price") or underlying_price

        # Find nearest contract
        contract = find_contract(contracts_df, opt_type, target_price)
        if not contract:
            continue

        key    = contract.get("instrument_key", "")
        strike = contract.get("strike_price", 0)
        lot    = contract.get("lot_size", 1)

        # Fetch prices
        prices = fetch_leg_prices(key, entry_date, exit_date)
        if not prices["found"]:
            continue

        ep = prices["entry"]
        xp = prices["exit"]

        # P&L calculation
        if action == "BUY":
            pnl = round((xp - ep) * lot, 2)
        else:  # SELL
            pnl = round((ep - xp) * lot, 2)

        executed_legs.append({
            "action":        action,
            "type":          opt_type,
            "strike":        strike,
            "instrument_key":key,
            "lot_size":      lot,
            "entry_premium": ep,
            "exit_premium":  xp,
            "pnl":           pnl,
        })

    if not executed_legs:
        return {}

    total_pnl = round(sum(l["pnl"] for l in executed_legs), 2)
    return {
        "legs":      executed_legs,
        "total_pnl": total_pnl,
        "win":       total_pnl > 0,
    }


# ============================================================
#  MAIN ENGINE
# ============================================================

def run_options_backtest(screener_name: str, strategy_name: str,
                         symbols: list, start_date: datetime,
                         end_date: datetime, capital: float):
    """
    Main options backtest function.

    screener_name  → e.g. "ema_crossover.py"
    strategy_name  → e.g. "short_straddle.py"
    symbols        → list of stock symbols e.g. ["RELIANCE", "TCS"]
    """
    if not ux.is_token_set():
        return {"error": "Upstox token not set."}

    print(f"\n  Options Backtest")
    print(f"  Screener: {screener_name} | Strategy: {strategy_name}")
    print(f"  Period: {start_date.date()} → {end_date.date()}")
    print(f"  Stocks: {symbols}")

    # Load screener
    screener_path = SCREENERS_DIR / screener_name
    if not screener_path.exists():
        return {"error": f"Screener not found: {screener_name}"}
    screener = load_module(screener_path)

    # Load strategy
    strategy_path = STRATEGIES_DIR / strategy_name
    if not strategy_path.exists():
        return {"error": f"Strategy not found: {strategy_name}"}
    strategy = load_module(strategy_path)

    upstox_api  = get_upstox_api()
    all_trades  = []

    for symbol in symbols:
        print(f"\n  Processing {symbol}...")

        # Upstox instrument key
        instrument_key = ux.get_instrument_key(symbol)
        if not instrument_key:
            print(f"  SKIP {symbol} — no instrument key")
            continue

        # Get expiries
        expiries = ux.get_expiries(instrument_key)
        if not expiries:
            print(f"  SKIP {symbol} — no expiries")
            continue

        # Fetch daily data from Yahoo Finance
        raw = yf.download(symbol + ".NS",
                          start=start_date.strftime("%Y-%m-%d"),
                          end=end_date.strftime("%Y-%m-%d"),
                          auto_adjust=True, progress=False)
        if raw.empty or len(raw) < 25:
            print(f"  SKIP {symbol} — insufficient data")
            continue

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Open","High","Low","Close","Volume"]].copy()
        df.columns = ["open","high","low","close","volume"]
        df.index   = pd.to_datetime(df.index)
        df         = df.reset_index()
        df.columns = ["date","open","high","low","close","volume"]
        print(f"  {symbol}: {len(df)} bars")

        prev_signal = None

        for i in range(25, len(df)):
            df_slice = df.iloc[:i+1].copy()
            today    = df_slice.iloc[-1]
            date_str = str(today["date"].date())
            price    = float(today["close"])

            if today["date"] < start_date or today["date"] > end_date:
                continue
            if date_str < expiries[0]:
                continue

            # Get signal
            try:
                signal, reasons = screener.screen(df_slice)
            except:
                continue

            if signal not in ["STRONG", "SKIP"]:
                prev_signal = signal
                continue

            # Only on crossover
            if signal == prev_signal:
                continue
            prev_signal = signal

            # Find expiry
            expiry = get_next_expiry(expiries, date_str)
            if not expiry:
                continue

            direction = "BULLISH" if signal == "STRONG" else "BEARISH"
            print(f"  {symbol} {signal} on {date_str} → expiry {expiry}")

            # Get contracts
            try:
                raw_contracts = upstox_api.get_expired_option_contracts(
                    instrument_key = instrument_key,
                    expiry_date    = expiry
                )
                contracts_list = raw_contracts.data or []
            except Exception as e:
                print(f"  [Error] contracts: {e}")
                continue

            if not contracts_list:
                continue

            # Build contracts DataFrame
            contracts_df = pd.DataFrame([{
                "instrument_key":  c.instrument_key,
                "strike_price":    c.strike_price,
                "instrument_type": c.instrument_type,
                "lot_size":        c.lot_size,
            } for c in contracts_list])

            # Get 1SD move
            hv      = ux.get_hv(instrument_key, days=30)
            sd_info = ux.get_1sd_range(price, hv, days=30)
            sd_move = sd_info["sd"]

            # Get legs from strategy script
            legs_def = strategy.get_legs(price, contracts_df,
                                          sd_move, direction)

            # Execute trade
            result = execute_trade(
                legs_def, contracts_df, price,
                sd_move, date_str, expiry
            )

            if not result:
                print(f"  No result for {symbol} {date_str}")
                continue

            hold_days = (datetime.strptime(expiry, "%Y-%m-%d") -
                         datetime.strptime(date_str, "%Y-%m-%d")).days

            all_trades.append({
                "symbol":      symbol,
                "signal":      signal,
                "direction":   direction,
                "strategy":    strategy_name.replace(".py",""),
                "entry_date":  date_str,
                "exit_date":   expiry,
                "underlying":  round(price, 2),
                "sd_move":     round(sd_move, 2),
                "hold_days":   hold_days,
                "total_pnl":   result["total_pnl"],
                "win":         result["win"],
                "legs":        result["legs"],
            })
            print(f"  P&L: {result['total_pnl']} | Win: {result['win']}")

    # Calculate metrics
    metrics = calculate_metrics(all_trades, capital, start_date, end_date)
    metrics.update({
        "screener":    screener_name,
        "strategy":    strategy_name,
        "capital":     capital,
        "start_date":  str(start_date.date()),
        "end_date":    str(end_date.date()),
        "stocks":      symbols,
        "trade_log":   all_trades,
    })

    print(f"\n  Done — {len(all_trades)} trades | P&L: {metrics['net_profit']}")
    return metrics


# ============================================================
#  METRICS
# ============================================================

def calculate_metrics(trades, capital, start_date, end_date):
    if not trades:
        return empty_metrics()

    df     = pd.DataFrame(trades)
    wins   = df[df["win"] == True]
    losses = df[df["win"] == False]

    gross_profit  = round(wins["total_pnl"].sum(),   2) if len(wins)   > 0 else 0
    gross_loss    = round(losses["total_pnl"].sum(), 2) if len(losses) > 0 else 0
    net_profit    = round(gross_profit + gross_loss, 2)
    profit_factor = round(abs(gross_profit/gross_loss), 2) \
                    if gross_loss != 0 else float("inf")

    max_consec = curr = 0
    for _, t in df.iterrows():
        if not t["win"]:
            curr += 1
            max_consec = max(max_consec, curr)
        else:
            curr = 0

    equity_curve = build_equity_curve(trades, capital, start_date, end_date)
    eq_values    = [e["value"] for e in equity_curve]
    max_dd       = calc_max_drawdown(eq_values)
    max_dd_pct   = round(max_dd / capital * 100, 2)
    monthly      = calc_monthly_returns(equity_curve)
    monthly_avg  = round(np.mean(monthly), 2) if monthly else 0
    monthly_std  = round(np.std(monthly),  2) if monthly else 0
    sharpe       = round(monthly_avg / monthly_std * np.sqrt(12), 2) \
                   if monthly_std != 0 else 0
    total_ret    = round(net_profit / capital * 100, 2)
    mar          = round(total_ret / max_dd_pct, 2) if max_dd_pct != 0 else 0

    stock_summary = []
    for sym in df["symbol"].unique():
        sym_df   = df[df["symbol"] == sym]
        sym_wins = sym_df[sym_df["win"] == True]
        stock_summary.append({
            "symbol":       sym,
            "total_pnl":    round(sym_df["total_pnl"].sum(), 2),
            "win_rate":     round(len(sym_wins)/len(sym_df)*100, 1),
            "total_trades": len(sym_df),
            "best_trade":   round(sym_df["total_pnl"].max(), 2),
            "worst_trade":  round(sym_df["total_pnl"].min(), 2),
        })
    stock_summary.sort(key=lambda x: x["total_pnl"], reverse=True)

    return {
        "net_profit":         net_profit,
        "gross_profit":       gross_profit,
        "gross_loss":         gross_loss,
        "profit_factor":      profit_factor,
        "total_return_pct":   total_ret,
        "monthly_avg_return": monthly_avg,
        "monthly_std":        monthly_std,
        "total_trades":       len(df),
        "winning_trades":     len(wins),
        "losing_trades":      len(losses),
        "win_rate":           round(len(wins)/len(df)*100, 1),
        "avg_pnl_per_trade":  round(df["total_pnl"].mean(), 2),
        "max_profit":         round(df["total_pnl"].max(), 2),
        "max_loss":           round(df["total_pnl"].min(), 2),
        "max_consec_losses":  max_consec,
        "avg_win_days":       round(wins["hold_days"].mean(), 1) if len(wins) > 0 else 0,
        "avg_loss_days":      round(losses["hold_days"].mean(), 1) if len(losses) > 0 else 0,
        "max_drawdown":       round(max_dd, 2),
        "max_drawdown_pct":   max_dd_pct,
        "sharpe_ratio":       sharpe,
        "mar_ratio":          mar,
        "equity_curve":       equity_curve,
        "underwater_curve":   build_underwater_curve(equity_curve, capital),
        "stock_summary":      stock_summary,
        "best_stock":         stock_summary[0]["symbol"]  if stock_summary else "—",
        "worst_stock":        stock_summary[-1]["symbol"] if stock_summary else "—",
    }


def build_equity_curve(trades, capital, start_date, end_date):
    dates     = pd.bdate_range(start=start_date, end=end_date)
    equity    = capital
    exit_pnls = {}
    for t in trades:
        d = t["exit_date"]
        exit_pnls[d] = exit_pnls.get(d, 0) + t["total_pnl"]
    curve = []
    for d in dates:
        ds = str(d.date())
        if ds in exit_pnls:
            equity += exit_pnls[ds]
        curve.append({"date": ds, "value": round(equity, 2)})
    return curve


def build_underwater_curve(equity_curve, capital):
    peak = capital
    result = []
    for p in equity_curve:
        if p["value"] > peak:
            peak = p["value"]
        result.append({"date": p["date"],
                        "drawdown": round((p["value"]-peak)/peak*100, 2)})
    return result


def calc_max_drawdown(values):
    peak = values[0] if values else 0
    max_dd = 0
    for v in values:
        if v > peak: peak = v
        dd = peak - v
        if dd > max_dd: max_dd = dd
    return round(max_dd, 2)


def calc_monthly_returns(equity_curve):
    if not equity_curve: return []
    df          = pd.DataFrame(equity_curve)
    df["date"]  = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    monthly     = df.groupby("month")["value"].last().pct_change().dropna()
    return list(monthly * 100)


def empty_metrics():
    return {k: 0 for k in [
        "net_profit","gross_profit","gross_loss","profit_factor",
        "total_return_pct","monthly_avg_return","monthly_std",
        "total_trades","winning_trades","losing_trades","win_rate",
        "avg_pnl_per_trade","max_profit","max_loss","max_consec_losses",
        "avg_win_days","avg_loss_days","max_drawdown","max_drawdown_pct",
        "sharpe_ratio","mar_ratio",
    ]} | {"equity_curve":[],"underwater_curve":[],"stock_summary":[],
          "best_stock":"—","worst_stock":"—"}
