# ============================================================
#  backtester.py
#  OptionLab — Backtest Engine
#
#  Data source: Yahoo Finance (adjusted prices)
#  NaN handling: NSE calendar aware
#  Position sizing: Fixed Amount / Fixed Qty / % Capital / Full
#  Long + Short support
# ============================================================

import sys
import io
import importlib
import importlib.util
import numpy as np
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, r"D:\optionlab\scripts")

from ib_core import get_bucket, FOLDER_OHLCV

SCREENERS_DIR = Path(r"D:\optionlab\scripts\screeners")


# ============================================================
#  DATA LOADING — Yahoo Finance
# ============================================================

def get_nse_trading_days(start_date, end_date):
    """Get all NSE trading days between start and end."""
    try:
        nse      = mcal.get_calendar('XNSE')
        schedule = nse.schedule(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        trading_days = mcal.date_range(schedule, frequency='1D')
        return set(d.date() for d in trading_days)
    except Exception as e:
        print(f"  [Warning] Could not load NSE calendar: {e}")
        return None


def get_ticker(symbol):
    """
    Auto detect correct Yahoo Finance ticker format.
    Tries NSE → BSE → US (as-is)
    Returns correct ticker string or None if not found.
    """
    # User already added suffix
    if "." in symbol:
        df = yf.download(symbol, period="5d", progress=False)
        return symbol if not df.empty else None

    # Try NSE
    nse = symbol + ".NS"
    df  = yf.download(nse, period="5d", progress=False)
    if not df.empty:
        return nse

    # Try BSE
    bse = symbol + ".BO"
    df  = yf.download(bse, period="5d", progress=False)
    if not df.empty:
        return bse

    # Try as-is (US stocks)
    df = yf.download(symbol, period="5d", progress=False)
    if not df.empty:
        return symbol

    return None


def fetch_from_yahoo(symbol, start_date, end_date):
    """
    Fetch OHLCV from Yahoo Finance.
    Auto detects correct ticker suffix (NSE/BSE/US).
    Returns (DataFrame, quality_report_dict)
    """
    ticker = get_ticker(symbol)
    quality = {
        "symbol":   symbol,
        "fetched":  0,
        "dropped":  0,
        "filled":   0,
        "final":    0,
        "warnings": []
    }

    if ticker is None:
        quality["warnings"].append(f"Symbol '{symbol}' not found on Yahoo Finance — tried .NS, .BO and as-is")
        return None, quality

    quality["ticker"] = ticker
    print(f"  {symbol} → {ticker}")

    try:
        df = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False
        )

        if df.empty:
            quality["warnings"].append("No data returned from Yahoo Finance")
            return None, quality

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Keep only OHLCV
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.columns = ["open", "high", "low", "close", "volume"]
        df.index   = pd.to_datetime(df.index).date

        quality["fetched"] = len(df)

        # Get NSE trading days
        trading_days = get_nse_trading_days(start_date, end_date)

        # Handle NaN rows
        nan_mask = df.isnull().any(axis=1)
        nan_dates = df[nan_mask].index.tolist()

        dropped = 0
        filled  = 0

        for d in nan_dates:
            if trading_days and d not in trading_days:
                # Holiday — drop
                df = df.drop(d)
                dropped += 1
            else:
                # Valid trading day with missing data — forward fill
                filled += 1

        # Forward fill remaining NaN (valid trading days)
        if filled > 0:
            df = df.ffill()
            quality["warnings"].append(
                f"{filled} valid trading day(s) forward filled"
            )

        quality["dropped"] = dropped
        quality["filled"]  = filled
        quality["final"]   = len(df)

        # Warn if too much data missing
        if filled > quality["fetched"] * 0.05:
            quality["warnings"].append(
                f"Warning: {filled} days filled — data quality may be poor"
            )

        if len(df) < 30:
            quality["warnings"].append(
                f"Only {len(df)} bars — insufficient for backtesting"
            )
            return None, quality

        # Reset index
        df = df.reset_index()
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df["date"] = pd.to_datetime(df["date"])

        print(f"  {symbol}: {quality['fetched']} fetched, "
              f"{quality['dropped']} dropped, "
              f"{quality['filled']} filled, "
              f"{quality['final']} final bars")

        return df, quality

    except Exception as e:
        quality["warnings"].append(f"Error: {str(e)}")
        return None, quality


def load_symbols_from_gcs():
    """Load all symbols from GCS universe Excel."""
    try:
        bucket = get_bucket()
        blob   = bucket.blob("universe/nifty50_universe.xlsx")
        if not blob.exists():
            return []
        df      = pd.read_excel(io.BytesIO(blob.download_as_bytes()))
        symbols = df["IB_Symbol"].dropna().tolist()
        print(f"  Loaded {len(symbols)} symbols from GCS")
        return [str(s).strip() for s in symbols]
    except Exception as e:
        print(f"  [Error] load_symbols_from_gcs: {e}")
        return []


# ============================================================
#  POSITION SIZING
# ============================================================

def calculate_shares(capital, available_capital, entry_price,
                     sizing_type, sizing_value):
    """
    Calculate number of shares based on position sizing.

    sizing_type  : "fixed_amount" | "fixed_qty" | "pct_capital" | "full_capital"
    sizing_value : amount / qty / percentage
    """
    if entry_price <= 0:
        return 0

    if sizing_type == "fixed_amount":
        return max(1, int(sizing_value / entry_price))

    elif sizing_type == "fixed_qty":
        return int(sizing_value)

    elif sizing_type == "pct_capital":
        amount = capital * (sizing_value / 100)
        return max(1, int(amount / entry_price))

    elif sizing_type == "full_capital":
        return max(1, int(available_capital / entry_price))

    return 1


# ============================================================
#  BACKTEST ENGINE — Long + Short
# ============================================================

def run_backtest_on_stock(symbol, df, screener_module,
                          capital, sizing_type, sizing_value):
    """
    Run screener day by day on historical data.
    Handles long and short positions with position sizing.
    """
    trades            = []
    position          = None   # None / "LONG" / "SHORT"
    entry             = None
    available_capital = capital

    for i in range(25, len(df)):
        df_slice = df.iloc[:i+1].copy()
        today    = df_slice.iloc[-1]
        price    = float(today["close"])
        date     = today["date"]

        try:
            signal, reasons = screener_module.screen(df_slice)
        except Exception as e:
            continue

        # ── STRONG = BUY signal ───────────────────────────
        if signal == "STRONG":
            # Close SHORT position
            if position == "SHORT":
                pnl       = round((entry["price"] - price) * entry["shares"], 2)
                pnl_pct   = round((entry["price"] - price) / entry["price"] * 100, 2)
                hold_days = (date - entry["date"]).days
                available_capital += (entry["price"] * entry["shares"]) + pnl
                trades.append({
                    "symbol":      symbol,
                    "direction":   "SHORT",
                    "entry_date":  str(entry["date"].date()),
                    "entry_price": round(entry["price"], 2),
                    "exit_date":   str(date.date()),
                    "exit_price":  round(price, 2),
                    "shares":      entry["shares"],
                    "pnl":         pnl,
                    "pnl_pct":     pnl_pct,
                    "hold_days":   hold_days,
                    "win":         pnl > 0,
                    "reasons":     entry["reasons"],
                })
                position = None
                entry    = None

            # Open LONG position
            if position is None:
                shares = calculate_shares(
                    capital, available_capital, price,
                    sizing_type, sizing_value
                )
                cost              = shares * price
                available_capital -= cost
                position          = "LONG"
                entry             = {
                    "date": date, "price": price,
                    "shares": shares, "reasons": reasons
                }

        # ── SKIP = SELL/SHORT signal ──────────────────────
        elif signal == "SKIP":
            # Close LONG position
            if position == "LONG":
                pnl       = round((price - entry["price"]) * entry["shares"], 2)
                pnl_pct   = round((price - entry["price"]) / entry["price"] * 100, 2)
                hold_days = (date - entry["date"]).days
                available_capital += (entry["price"] * entry["shares"]) + pnl
                trades.append({
                    "symbol":      symbol,
                    "direction":   "LONG",
                    "entry_date":  str(entry["date"].date()),
                    "entry_price": round(entry["price"], 2),
                    "exit_date":   str(date.date()),
                    "exit_price":  round(price, 2),
                    "shares":      entry["shares"],
                    "pnl":         pnl,
                    "pnl_pct":     pnl_pct,
                    "hold_days":   hold_days,
                    "win":         pnl > 0,
                    "reasons":     entry["reasons"],
                })
                position = None
                entry    = None

            # Open SHORT position
            if position is None:
                shares = calculate_shares(
                    capital, available_capital, price,
                    sizing_type, sizing_value
                )
                position = "SHORT"
                entry    = {
                    "date": date, "price": price,
                    "shares": shares, "reasons": reasons
                }

    # Close open position at end
    if position is not None and entry is not None:
        last  = df.iloc[-1]
        price = float(last["close"])
        date  = last["date"]

        if position == "LONG":
            pnl = round((price - entry["price"]) * entry["shares"], 2)
        else:
            pnl = round((entry["price"] - price) * entry["shares"], 2)

        pnl_pct   = round(pnl / (entry["price"] * entry["shares"]) * 100, 2)
        hold_days = (date - entry["date"]).days

        trades.append({
            "symbol":      symbol,
            "direction":   position,
            "entry_date":  str(entry["date"].date()),
            "entry_price": round(entry["price"], 2),
            "exit_date":   str(date.date()),
            "exit_price":  round(price, 2),
            "shares":      entry["shares"],
            "pnl":         pnl,
            "pnl_pct":     pnl_pct,
            "hold_days":   hold_days,
            "win":         pnl > 0,
            "reasons":     entry["reasons"],
            "open":        True,
        })

    return trades


# ============================================================
#  METRICS
# ============================================================

def calculate_metrics(all_trades, capital, df_dict,
                      start_date, end_date, quality_reports):
    if not all_trades:
        return empty_metrics()

    trades_df = pd.DataFrame(all_trades)
    wins      = trades_df[trades_df["win"] == True]
    losses    = trades_df[trades_df["win"] == False]

    gross_profit = round(wins["pnl"].sum(),   2) if len(wins)   > 0 else 0
    gross_loss   = round(losses["pnl"].sum(), 2) if len(losses) > 0 else 0
    net_profit   = round(gross_profit + gross_loss, 2)

    profit_factor      = round(abs(gross_profit / gross_loss), 2) if gross_loss != 0 else float("inf")
    largest_winner     = round(trades_df["pnl"].max(), 2)
    largest_loser      = round(trades_df["pnl"].min(), 2)
    largest_winner_pct = round(largest_winner / gross_profit * 100, 2) if gross_profit != 0 else 0
    largest_loser_pct  = round(abs(largest_loser / gross_loss) * 100, 2) if gross_loss != 0 else 0

    # Max consecutive losses
    max_consec = curr = 0
    for _, t in trades_df.iterrows():
        if not t["win"]:
            curr += 1
            max_consec = max(max_consec, curr)
        else:
            curr = 0

    avg_win_days  = round(wins["hold_days"].mean(),   1) if len(wins)   > 0 else 0
    avg_loss_days = round(losses["hold_days"].mean(), 1) if len(losses) > 0 else 0

    equity_curve     = build_equity_curve(all_trades, capital, start_date, end_date)
    eq_values        = [e["value"] for e in equity_curve]
    max_dd           = calculate_max_drawdown(eq_values)
    max_dd_pct       = round(max_dd / capital * 100, 2)
    monthly          = calculate_monthly_returns(equity_curve)
    monthly_avg      = round(np.mean(monthly), 2) if monthly else 0
    monthly_std      = round(np.std(monthly),  2) if monthly else 0
    sharpe           = round(monthly_avg / monthly_std * np.sqrt(12), 2) if monthly_std != 0 else 0
    total_return_pct = round(net_profit / capital * 100, 2)
    mar              = round(total_return_pct / max_dd_pct, 2) if max_dd_pct != 0 else 0
    bh_return        = calculate_buy_hold(df_dict, start_date, end_date)

    # Per stock summary
    stock_summary = []
    for sym in trades_df["symbol"].unique():
        sym_trades = trades_df[trades_df["symbol"] == sym]
        sym_wins   = sym_trades[sym_trades["win"] == True]
        stock_summary.append({
            "symbol":       sym,
            "total_pnl":    round(sym_trades["pnl"].sum(), 2),
            "win_rate":     round(len(sym_wins) / len(sym_trades) * 100, 1),
            "total_trades": len(sym_trades),
            "best_trade":   round(sym_trades["pnl"].max(), 2),
            "worst_trade":  round(sym_trades["pnl"].min(), 2),
            "long_trades":  len(sym_trades[sym_trades["direction"] == "LONG"]),
            "short_trades": len(sym_trades[sym_trades["direction"] == "SHORT"]),
        })
    stock_summary.sort(key=lambda x: x["total_pnl"], reverse=True)

    return {
        "net_profit":           net_profit,
        "gross_profit":         gross_profit,
        "gross_loss":           gross_loss,
        "profit_factor":        profit_factor,
        "total_return_pct":     total_return_pct,
        "buy_hold_return_pct":  bh_return,
        "monthly_avg_return":   monthly_avg,
        "monthly_std":          monthly_std,
        "total_trades":         len(trades_df),
        "winning_trades":       len(wins),
        "losing_trades":        len(losses),
        "win_rate":             round(len(wins) / len(trades_df) * 100, 1),
        "avg_pnl_per_trade":    round(trades_df["pnl"].mean(), 2),
        "max_profit":           largest_winner,
        "max_loss":             largest_loser,
        "largest_winner_pct":   largest_winner_pct,
        "largest_loser_pct":    largest_loser_pct,
        "max_consec_losses":    max_consec,
        "avg_win_days":         avg_win_days,
        "avg_loss_days":        avg_loss_days,
        "max_drawdown":         round(max_dd, 2),
        "max_drawdown_pct":     max_dd_pct,
        "sharpe_ratio":         sharpe,
        "mar_ratio":            mar,
        "long_trades":          len(trades_df[trades_df["direction"] == "LONG"]),
        "short_trades":         len(trades_df[trades_df["direction"] == "SHORT"]),
        "best_stock":           stock_summary[0]["symbol"]  if stock_summary else "—",
        "worst_stock":          stock_summary[-1]["symbol"] if stock_summary else "—",
        "equity_curve":         equity_curve,
        "underwater_curve":     build_underwater_curve(equity_curve, capital),
        "stock_summary":        stock_summary,
        "trade_log":            all_trades,
        "data_quality":         quality_reports,
    }


def build_equity_curve(trades, capital, start_date, end_date):
    dates       = pd.bdate_range(start=start_date, end=end_date)
    equity      = capital
    curve       = []
    trade_exits = {}
    for t in trades:
        d = t["exit_date"]
        trade_exits[d] = trade_exits.get(d, 0) + t["pnl"]

    for d in dates:
        date_str = str(d.date())
        if date_str in trade_exits:
            equity += trade_exits[date_str]
        curve.append({"date": date_str, "value": round(equity, 2)})
    return curve


def build_underwater_curve(equity_curve, capital):
    peak   = capital
    result = []
    for point in equity_curve:
        if point["value"] > peak:
            peak = point["value"]
        dd_pct = round((point["value"] - peak) / peak * 100, 2)
        result.append({"date": point["date"], "drawdown": dd_pct})
    return result


def calculate_max_drawdown(values):
    peak = values[0] if values else 0
    max_dd = 0
    for v in values:
        if v > peak:
            peak = v
        dd = peak - v
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 2)


def calculate_monthly_returns(equity_curve):
    if not equity_curve:
        return []
    df          = pd.DataFrame(equity_curve)
    df["date"]  = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    monthly     = df.groupby("month")["value"].last().pct_change().dropna()
    return list(monthly * 100)


def calculate_buy_hold(df_dict, start_date, end_date):
    returns = []
    for symbol, df in df_dict.items():
        if df is None or df.empty:
            continue
        try:
            start_price = float(df.iloc[0]["close"])
            end_price   = float(df.iloc[-1]["close"])
            ret         = (end_price - start_price) / start_price * 100
            returns.append(ret)
        except:
            continue
    return round(np.mean(returns), 2) if returns else 0


def empty_metrics():
    return {k: 0 for k in [
        "net_profit", "gross_profit", "gross_loss", "profit_factor",
        "total_return_pct", "buy_hold_return_pct", "monthly_avg_return",
        "monthly_std", "total_trades", "winning_trades", "losing_trades",
        "win_rate", "avg_pnl_per_trade", "max_profit", "max_loss",
        "largest_winner_pct", "largest_loser_pct", "max_consec_losses",
        "avg_win_days", "avg_loss_days", "max_drawdown", "max_drawdown_pct",
        "sharpe_ratio", "mar_ratio", "long_trades", "short_trades",
    ]} | {"best_stock": "—", "worst_stock": "—", "equity_curve": [],
          "underwater_curve": [], "stock_summary": [], "trade_log": [],
          "data_quality": []}


# ============================================================
#  MAIN
# ============================================================

def run_backtest(screener_name, symbols, use_gcs,
                 frequency, start_date, end_date,
                 capital, sizing_type, sizing_value):
    """
    Main backtest function — called from server.py

    symbols      → single symbol string or list
    use_gcs      → True = load all from GCS, False = use symbols list
    sizing_type  → "fixed_amount" | "fixed_qty" | "pct_capital" | "full_capital"
    sizing_value → amount / qty / percentage value
    """
    print(f"\n  Running backtest: {screener_name}")
    print(f"  Period: {start_date.date()} → {end_date.date()}")
    print(f"  Sizing: {sizing_type} = {sizing_value}")

    # Load screener script
    script_path = SCREENERS_DIR / screener_name
    if not script_path.exists():
        return {"error": f"Screener not found: {screener_name}"}

    spec   = importlib.util.spec_from_file_location("screener", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get symbols list
    if use_gcs:
        symbol_list = load_symbols_from_gcs()
    else:
        symbol_list = [symbols] if isinstance(symbols, str) else symbols

    if not symbol_list:
        return {"error": "No symbols to backtest"}

    print(f"  Stocks: {len(symbol_list)}")

    all_trades      = []
    df_dict         = {}
    quality_reports = []

    for symbol in symbol_list:
        df, quality = fetch_from_yahoo(symbol, start_date, end_date)
        quality_reports.append(quality)

        if df is None or len(df) < 30:
            print(f"  SKIP {symbol} — insufficient data")
            continue

        df_dict[symbol] = df
        trades = run_backtest_on_stock(
            symbol, df, module,
            capital, sizing_type, sizing_value
        )
        all_trades.extend(trades)
        print(f"  {symbol}: {len(trades)} trades")

    metrics = calculate_metrics(
        all_trades, capital, df_dict,
        start_date, end_date, quality_reports
    )
    metrics["screener"]      = screener_name
    metrics["frequency"]     = frequency
    metrics["capital"]       = capital
    metrics["sizing_type"]   = sizing_type
    metrics["sizing_value"]  = sizing_value
    metrics["start_date"]    = str(start_date.date())
    metrics["end_date"]      = str(end_date.date())
    metrics["stocks"]        = symbol_list

    print(f"\n  Done — {metrics['total_trades']} trades | "
          f"Net P&L: ₹{metrics['net_profit']}")
    return metrics
