# ============================================================
#  backtester.py
#  OptionLab — Universal Backtest Engine
#
#  Supports any frequency: 1 min, 5 mins, 15 mins, 30 mins,
#                          1 hour, 1 day, 1 week, 1 month
#
#  Screener script declares:
#    FREQUENCY     → bar size
#    LOOKBACK      → warmup bars needed
#    ENTRY_RULE    → "next_bar_open" (only supported rule)
#    EXIT_RULE     → "next_bar_open" | "next_day_open" |
#                    "same_bar_close" | "next_day_close"
#    CAPITAL_SPLIT → "equal" (only supported rule)
#    DIRECTION     → {signal_name: "LONG" | "SHORT"}
#    SIZING_TYPE   → "fixed_amount" | "fixed_qty" |
#                    "pct_capital"  | "full_capital"
#    SIZING_VALUE  → numeric value matching SIZING_TYPE
#
#  Dashboard values override script values unless dashboard
#  dropdown is set to "script" for that parameter.
#
#  PnL formula (universal):
#    qty   > 0 for LONG  → pnl = (exit - entry) * qty
#    qty   < 0 for SHORT → pnl = (exit - entry) * qty
#    This means short profits when exit < entry. Clean.
#
#  Metrics (17 exactly):
#    Net Profit, Gross Profit, Gross Loss, Profit Factor,
#    Total Return %, Buy & Hold Return %, Monthly Avg Return,
#    Win Rate, Avg P&L/Trade, Max Profit, Max Loss,
#    Max Consec Losses, Avg Win Days, Avg Loss Days,
#    Max Drawdown, Sharpe Ratio, MAR Ratio
#    + Equity Curve, Underwater Curve (charts)
# ============================================================

import sys
import importlib
import importlib.util
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, r"D:\optionlab\scripts")

from ib_core import (
    get_app, is_connected,
    fetch_history, load_universe
)

SCREENERS_DIR = Path(r"D:\optionlab\scripts\screeners")

# ── Bar size → annualisation factor ──────────────────────────
# Used for Sharpe ratio: sqrt(bars_per_year)
BARS_PER_YEAR = {
    "1 min":   252 * 375,   # NSE: 6h15m = 375 mins/day
    "5 mins":  252 * 75,
    "15 mins": 252 * 25,
    "30 mins": 252 * 13,
    "1 hour":  252 * 6,     # ~6 hourly bars/day on NSE
    "1 day":   252,
    "1 week":  52,
    "1 month": 12,
}


# ============================================================
#  DATA FETCH — IBKR Gateway via ib_core
# ============================================================

def _fetch_bars(symbol, bar_size, duration, start_date, end_date):
    """
    Fetch OHLCV from Gateway for any bar size.
    Returns DataFrame with lowercase columns:
      date, open, high, low, close, volume
    date column is tz-naive for daily/weekly/monthly,
    tz-aware IST for intraday.
    Filters to [start_date, end_date] after fetch.
    """
    app = get_app()
    if app is None:
        return None

    df = fetch_history(app, symbol, duration=duration, bar_size=bar_size)
    if df is None or df.empty:
        return None

    # Normalise column names to lowercase
    df.columns = [c.lower() for c in df.columns]
    if "date" not in df.columns:
        df = df.rename(columns={df.columns[0]: "date"})

    df = df.sort_values("date").reset_index(drop=True)

    # Filter to end_date — keep warmup bars before start_date intentionally
    is_intraday = bar_size not in ("1 day", "1 week", "1 month")
    if is_intraday:
        end_ts = pd.Timestamp(end_date).tz_localize("Asia/Kolkata")
        df = df[df["date"] <= end_ts].copy()
    else:
        df = df[df["date"] <= pd.Timestamp(end_date)].copy()

    return df.reset_index(drop=True) if not df.empty else None


def fetch_data_for_backtest(symbol, bar_size, duration, start_date, end_date):
    """
    Fetch bars for backtest. For intraday screeners that need daily
    context (e.g. OEH/OEL pivot), also fetch daily bars.
    Returns (df, daily_df, quality)
      daily_df is None for daily/weekly/monthly bar sizes.
    """
    quality = {"symbol": symbol, "fetched": 0, "final": 0, "warnings": []}

    df = _fetch_bars(symbol, bar_size, duration, start_date, end_date)
    if df is None or len(df) < 5:
        quality["warnings"].append(f"No data for {symbol} at {bar_size}")
        return None, None, quality

    quality["fetched"] = len(df)
    quality["final"]   = len(df)

    # For intraday — also fetch daily for context
    daily_df = None
    is_intraday = bar_size not in ("1 day", "1 week", "1 month")
    if is_intraday:
        daily_df = _fetch_bars(symbol, "1 day", duration, start_date, end_date)
        if daily_df is not None:
            # Add Date column (date only, no time)
            daily_df["Date"] = pd.to_datetime(daily_df["date"]).dt.normalize()

        # Add Date column to intraday df
        df["Date"] = pd.to_datetime(df["date"]).dt.date

    in_range = (df["date"] >= pd.Timestamp(start_date)).sum() if not is_intraday else \
               (df["date"] >= pd.Timestamp(start_date).tz_localize("Asia/Kolkata")).sum()
    print(f"  {symbol}: {len(df)} bars ({in_range} in range + {len(df)-in_range} warmup)")

    return df, daily_df, quality


# ============================================================
#  POSITION SIZING
# ============================================================

def calc_qty(entry_price, alloc_capital, sizing_type, sizing_value, direction):
    """
    Calculate signed qty. Positive = LONG, Negative = SHORT.
    alloc_capital : capital allocated to this trade
    """
    if entry_price <= 0:
        return 0

    if sizing_type == "fixed_qty":
        qty = int(sizing_value)
    elif sizing_type == "fixed_amount":
        qty = max(1, int(sizing_value / entry_price))
    elif sizing_type == "pct_capital":
        qty = max(1, int((alloc_capital * sizing_value / 100) / entry_price))
    else:  # full_capital / equal split
        qty = max(1, int(alloc_capital / entry_price))

    return qty if direction == "LONG" else -qty


# ============================================================
#  EXIT PRICE RESOLVER
# ============================================================

def resolve_exit(exit_rule, df, signal_idx, bar_size):
    """
    Resolve exit price and date from exit_rule.

    exit_rule options:
      "next_bar_open"   → open of bar immediately after signal bar
      "same_bar_close"  → close of signal bar itself
      "next_day_open"   → open of first bar of next calendar trading day
      "next_day_close"  → close of last bar of next calendar trading day

    df        : full DataFrame for this symbol (all bars)
    signal_idx: integer index of the signal bar in df
    bar_size  : bar size string

    Returns (exit_price, exit_date, exit_idx) or (None, None, None) if not resolvable.
    """
    if signal_idx >= len(df) - 1:
        return None, None, None

    is_intraday = bar_size not in ("1 day", "1 week", "1 month")

    if exit_rule == "same_bar_close":
        row = df.iloc[signal_idx]
        return float(row["close"]), row["date"], signal_idx

    elif exit_rule == "next_bar_open":
        next_idx = signal_idx + 1
        if next_idx >= len(df):
            return None, None, None
        row = df.iloc[next_idx]
        return float(row["open"]), row["date"], next_idx

    elif exit_rule in ("next_day_open", "next_day_close"):
        if not is_intraday:
            # For daily bars, next_day = next bar
            next_idx = signal_idx + 1
            if next_idx >= len(df):
                return None, None, None
            row = df.iloc[next_idx]
            price = float(row["open"]) if exit_rule == "next_day_open" else float(row["close"])
            return price, row["date"], next_idx
        else:
            # For intraday — find first/last bar of next calendar day
            signal_date = df.iloc[signal_idx]["Date"]
            future = df[df["Date"] > signal_date]
            if future.empty:
                return None, None, None
            next_date  = future["Date"].iloc[0]
            next_day_bars = df[df["Date"] == next_date]
            if next_day_bars.empty:
                return None, None, None
            if exit_rule == "next_day_open":
                row = next_day_bars.iloc[0]
            else:
                row = next_day_bars.iloc[-1]
            exit_idx = next_day_bars.index[0] if exit_rule == "next_day_open" \
                       else next_day_bars.index[-1]
            return float(row["open"] if exit_rule == "next_day_open" else row["close"]), \
                   row["date"], exit_idx

    return None, None, None


# ============================================================
#  UNIVERSAL BACKTEST ENGINE
# ============================================================

def run_engine(df_dict, daily_dict, screener_module, bar_size,
               capital, sizing_type, sizing_value,
               start_date, end_date, cost_type=None, cost_value=0.0):
    """
    Universal vectorized backtest engine — works for any bar size.

    df_dict     : {symbol: DataFrame} — full bars including warmup
    daily_dict  : {symbol: daily_df} — None for daily/weekly/monthly
    bar_size    : IBKR bar size string
    cost_type   : None | "pct" | "abs"
    cost_value  : % per side (if pct) or abs amount per trade (if abs)

    Mechanics:
    - For each bar in chronological order across all symbols:
      1. Run screen() on data up to and including this bar
      2. If signal fires and no open position → queue entry
      3. Entry executed at next bar open (ENTRY_RULE)
      4. Exit resolved per EXIT_RULE from script
      5. PnL = (exit - entry) * qty
         qty > 0 LONG, qty < 0 SHORT — universal formula
    - Capital split equally across all signals firing same bar
    - Warmup: no trades before start_date, but screen() runs for warmup
    """
    # ── Read script attributes — function first, then attribute ─
    direction_map = getattr(screener_module, "DIRECTION",     {})
    capital_split = getattr(screener_module, "CAPITAL_SPLIT", "equal")
    trade_mode    = getattr(screener_module, "TRADE_MODE",    "signal_change")

    # Exit rule — script function overrides attribute
    _exit_fn   = getattr(screener_module, "get_exit",   None)
    exit_rule  = getattr(screener_module, "EXIT_RULE",  "next_bar_open")

    # Trade mode — script function overrides attribute
    _should_trade_fn = getattr(screener_module, "should_trade", None)

    # Capital split — script function overrides attribute
    _capital_fn = getattr(screener_module, "get_capital", None)

    is_intraday = bar_size not in ("1 day", "1 week", "1 month")

    # Build unified sorted date/bar index across all symbols
    all_bar_dates = sorted(set(
        row for df in df_dict.values()
        for row in df["date"].tolist()
    ))

    trades            = []
    available_capital = capital

    # Per symbol state
    open_positions  = {}  # symbol → None | position dict
    current_signal  = {}  # symbol → last signal string (tracks direction changes)
    for sym in df_dict:
        open_positions[sym] = None
        current_signal[sym] = None

    # Convert start_date for warmup check
    if is_intraday:
        _start_ts = pd.Timestamp(start_date).tz_localize("Asia/Kolkata")
    else:
        _start_ts = pd.Timestamp(start_date)

    # Build per-symbol date→index map for O(1) lookup
    date_idx_map = {}
    for sym, df in df_dict.items():
        date_idx_map[sym] = {d: i for i, d in enumerate(df["date"].tolist())}

    # ── Main loop — one bar at a time ────────────────────────
    # Group bars by date to allow same-bar capital splitting
    from itertools import groupby
    bar_groups = {}
    for d in all_bar_dates:
        bar_groups[d] = []
        for sym, df in df_dict.items():
            if d in date_idx_map[sym]:
                bar_groups[d].append(sym)

    for bar_date in all_bar_dates:
        in_warmup = bar_date < _start_ts
        syms_this_bar = bar_groups[bar_date]

        # ── Step 1: Check exits for open positions ────────────
        for sym in syms_this_bar:
            pos = open_positions[sym]
            if pos is None:
                continue
            df  = df_dict[sym]
            idx = date_idx_map[sym][bar_date]

            # Check if current bar is the exit bar
            if idx == pos.get("exit_idx"):
                exit_price = pos["exit_price"]
                exit_date  = bar_date

                qty = pos["qty"]
                pnl = round((exit_price - pos["entry_price"]) * qty, 2)

                # Apply cost
                if cost_type == "pct" and cost_value > 0:
                    cost = abs(qty) * pos["entry_price"] * cost_value / 100 + \
                           abs(qty) * exit_price * cost_value / 100
                    pnl -= round(cost, 2)
                elif cost_type == "abs" and cost_value > 0:
                    pnl -= cost_value * 2  # entry + exit

                pnl_pct   = round(pnl / (abs(qty) * pos["entry_price"]) * 100, 3)
                hold_bars = idx - pos["entry_idx"]

                available_capital += abs(qty) * pos["entry_price"] + pnl

                trades.append({
                    "symbol":      sym,
                    "direction":   "LONG" if qty > 0 else "SHORT",
                    "entry_date":  str(pos["entry_date"].date()) if hasattr(pos["entry_date"], "date") else str(pos["entry_date"]),
                    "entry_price": round(pos["entry_price"], 2),
                    "exit_date":   str(exit_date.date()) if hasattr(exit_date, "date") else str(exit_date),
                    "exit_price":  round(exit_price, 2),
                    "shares":      abs(qty),
                    "pnl":         pnl,
                    "pnl_pct":     pnl_pct,
                    "hold_bars":   hold_bars,
                    "win":         pnl > 0,
                    "reasons":     pos["reasons"],
                })
                open_positions[sym] = None

        if in_warmup:
            continue

        # ── Step 2: Generate signals this bar ─────────────────
        new_signals = []
        for sym in syms_this_bar:
            df  = df_dict[sym]
            idx = date_idx_map[sym][bar_date]
            df_slice = df.iloc[:idx + 1]

            if len(df_slice) < 5:
                continue

            d_df = daily_dict.get(sym) if daily_dict else None

            try:
                result = screener_module.screen(df_slice) if not is_intraday else \
                         screener_module.screen(df_slice, d_df)
                signal, reasons = result[0], result[1]
            except Exception as e:
                continue

            # ── Trade mode resolution ─────────────────────────
            # Function-first: script.should_trade() overrides TRADE_MODE
            prev_signal = current_signal[sym]
            current_signal[sym] = signal

            if signal not in direction_map:
                # Not a tradeable signal — update state but no action
                continue

            new_direction = direction_map[signal]

            # Decide whether to act based on trade mode
            if _should_trade_fn is not None:
                # Script provides full custom logic
                act = _should_trade_fn(
                    signal       = signal,
                    prev_signal  = prev_signal,
                    has_position = open_positions[sym] is not None,
                )
            elif trade_mode == "signal_change":
                # Only act when signal direction changes
                act = signal != prev_signal
            elif trade_mode == "independent":
                # Act on every qualifying bar when flat
                act = open_positions[sym] is None
            else:
                act = signal != prev_signal  # safe default

            if not act:
                continue

            # ── Close existing position on signal change ──────
            # Capital is returned immediately so reversal trade
            # in the same bar gets the full compounded capital
            pos = open_positions[sym]
            if pos is not None:
                close_idx = idx + 1
                if close_idx < len(df):
                    close_price = float(df.iloc[close_idx]["open"])
                    close_date  = df.iloc[close_idx]["date"]
                    qty  = pos["qty"]
                    pnl  = round((close_price - pos["entry_price"]) * qty, 2)
                    if cost_type == "pct" and cost_value > 0:
                        cost = abs(qty) * pos["entry_price"] * cost_value / 100 +                                abs(qty) * close_price * cost_value / 100
                        pnl -= round(cost, 2)
                    elif cost_type == "abs" and cost_value > 0:
                        pnl -= cost_value * 2
                    pnl_pct   = round(pnl / (abs(qty) * pos["entry_price"]) * 100, 3)
                    hold_bars = close_idx - pos["entry_idx"]
                    # ↓ Return capital immediately — reversal uses this
                    available_capital += abs(qty) * pos["entry_price"] + pnl
                    trades.append({
                        "symbol":      sym,
                        "direction":   "LONG" if qty > 0 else "SHORT",
                        "entry_date":  str(pos["entry_date"].date()) if hasattr(pos["entry_date"], "date") else str(pos["entry_date"]),
                        "entry_price": round(pos["entry_price"], 2),
                        "exit_date":   str(close_date.date()) if hasattr(close_date, "date") else str(close_date),
                        "exit_price":  round(close_price, 2),
                        "shares":      abs(qty),
                        "pnl":         pnl,
                        "pnl_pct":     pnl_pct,
                        "hold_bars":   hold_bars,
                        "win":         pnl > 0,
                        "reasons":     pos["reasons"],
                    })
                open_positions[sym] = None

            # ── Open new position ─────────────────────────────
            entry_idx = idx + 1
            if entry_idx >= len(df):
                continue
            entry_price = float(df.iloc[entry_idx]["open"])
            entry_date  = df.iloc[entry_idx]["date"]

            # Resolve exit — script function first, then EXIT_RULE attribute
            if _exit_fn is not None:
                exit_price, exit_date, exit_idx = _exit_fn(df, entry_idx, bar_size)
            else:
                exit_price, exit_date, exit_idx = resolve_exit(
                    exit_rule, df, entry_idx, bar_size
                )
            if exit_price is None:
                continue

            new_signals.append({
                "sym":         sym,
                "direction":   new_direction,
                "entry_price": entry_price,
                "entry_date":  entry_date,
                "entry_idx":   entry_idx,
                "exit_price":  exit_price,
                "exit_date":   exit_date,
                "exit_idx":    exit_idx,
                "reasons":     reasons,
            })

        if not new_signals:
            continue

        # ── Step 3: Split capital + open positions ────────────
        # available_capital already includes capital returned from
        # any positions closed this bar (signal change exits above)
        n = len(new_signals)
        if _capital_fn is not None:
            # Script provides full custom capital allocation
            per_trade_capital = _capital_fn(available_capital, n, new_signals)
        elif capital_split == "equal":
            per_trade_capital = available_capital / n
        else:
            per_trade_capital = available_capital

        for s in new_signals:
            qty = calc_qty(
                s["entry_price"], per_trade_capital,
                sizing_type, sizing_value, s["direction"]
            )
            if qty == 0:
                continue

            cost_entry = abs(qty) * s["entry_price"]
            available_capital -= cost_entry

            open_positions[s["sym"]] = {
                "entry_price": s["entry_price"],
                "entry_date":  s["entry_date"],
                "entry_idx":   s["entry_idx"],
                "exit_price":  s["exit_price"],
                "exit_date":   s["exit_date"],
                "exit_idx":    s["exit_idx"],
                "qty":         qty,
                "reasons":     s["reasons"],
                "capital_at_entry": available_capital + cost_entry,  # for compounding reference
            }

    # ── Close any remaining open positions at last bar ────────
    for sym, pos in open_positions.items():
        if pos is None:
            continue
        df   = df_dict[sym]
        last = df.iloc[-1]
        qty  = pos["qty"]
        exit_price = float(last["close"])
        exit_date  = last["date"]
        pnl = round((exit_price - pos["entry_price"]) * qty, 2)
        pnl_pct = round(pnl / (abs(qty) * pos["entry_price"]) * 100, 3)

        trades.append({
            "symbol":      sym,
            "direction":   "LONG" if qty > 0 else "SHORT",
            "entry_date":  str(pos["entry_date"].date()) if hasattr(pos["entry_date"], "date") else str(pos["entry_date"]),
            "entry_price": round(pos["entry_price"], 2),
            "exit_date":   str(exit_date.date()) if hasattr(exit_date, "date") else str(exit_date),
            "exit_price":  round(exit_price, 2),
            "shares":      abs(qty),
            "pnl":         pnl,
            "pnl_pct":     pnl_pct,
            "hold_bars":   len(df) - 1 - pos["entry_idx"],
            "win":         pnl > 0,
            "reasons":     pos["reasons"],
            "open":        True,
        })

    return trades


# ============================================================
#  METRICS — exactly 17 + equity/underwater curves
# ============================================================

def calculate_metrics(trades, capital, df_dict, start_date, end_date,
                      bar_size, quality_reports):
    if not trades:
        return empty_metrics()

    tdf = pd.DataFrame(trades)
    wins   = tdf[tdf["win"] == True]
    losses = tdf[tdf["win"] == False]

    gross_profit  = round(wins["pnl"].sum(),   2) if len(wins)   > 0 else 0
    gross_loss    = round(losses["pnl"].sum(), 2) if len(losses) > 0 else 0
    net_profit    = round(gross_profit + gross_loss, 2)
    profit_factor = round(abs(gross_profit / gross_loss), 2) if gross_loss != 0 else float("inf")
    total_return  = round(net_profit / capital * 100, 2)
    bh_return     = _buy_hold(df_dict, start_date, end_date)
    win_rate      = round(len(wins) / len(tdf) * 100, 1)
    avg_pnl       = round(tdf["pnl"].mean(), 2)
    max_profit    = round(tdf["pnl"].max(), 2)
    max_loss      = round(tdf["pnl"].min(), 2)

    # Max consecutive losses
    max_consec = curr = 0
    for w in tdf["win"]:
        if not w:
            curr += 1
            max_consec = max(max_consec, curr)
        else:
            curr = 0

    avg_win_bars  = round(wins["hold_bars"].mean(),   1) if len(wins)   > 0 else 0
    avg_loss_bars = round(losses["hold_bars"].mean(), 1) if len(losses) > 0 else 0

    # Equity curve + drawdown
    eq_curve  = _equity_curve(trades, capital, start_date, end_date)
    eq_values = [p["value"] for p in eq_curve]
    max_dd    = _max_drawdown(eq_values)
    max_dd_pct = round(max_dd / capital * 100, 2) if capital > 0 else 0

    # Sharpe — annualised from trade returns
    ann_factor = BARS_PER_YEAR.get(bar_size, 252)
    returns    = tdf["pnl_pct"].values / 100
    ret_mean   = np.mean(returns)
    ret_std    = np.std(returns)
    sharpe     = round(ret_mean / ret_std * np.sqrt(ann_factor), 2) if ret_std != 0 else 0

    # MAR
    mar = round(total_return / max_dd_pct, 2) if max_dd_pct != 0 else 0

    # Monthly avg return from equity curve
    monthly = _monthly_returns(eq_curve)
    monthly_avg = round(np.mean(monthly), 2) if monthly else 0

    # Per stock summary
    stock_summary = []
    for sym in tdf["symbol"].unique():
        st = tdf[tdf["symbol"] == sym]
        sw = st[st["win"] == True]
        stock_summary.append({
            "symbol":       sym,
            "total_pnl":    round(st["pnl"].sum(), 2),
            "win_rate":     round(len(sw) / len(st) * 100, 1),
            "total_trades": len(st),
            "best_trade":   round(st["pnl"].max(), 2),
            "worst_trade":  round(st["pnl"].min(), 2),
            "long_trades":  len(st[st["direction"] == "LONG"]),
            "short_trades": len(st[st["direction"] == "SHORT"]),
        })
    stock_summary.sort(key=lambda x: x["total_pnl"], reverse=True)

    return {
        # Profitability
        "net_profit":          net_profit,
        "gross_profit":        gross_profit,
        "gross_loss":          gross_loss,
        "profit_factor":       profit_factor,
        "total_return_pct":    total_return,
        "buy_hold_return_pct": bh_return,
        "monthly_avg_return":  monthly_avg,
        # Trades
        "win_rate":            win_rate,
        "avg_pnl_per_trade":   avg_pnl,
        "max_profit":          max_profit,
        "max_loss":            max_loss,
        "max_consec_losses":   max_consec,
        "avg_win_bars":        avg_win_bars,
        "avg_loss_bars":       avg_loss_bars,
        # Risk
        "max_drawdown":        round(max_dd, 2),
        "max_drawdown_pct":    max_dd_pct,
        "sharpe_ratio":        sharpe,
        "mar_ratio":           mar,
        # Meta
        "total_trades":        len(tdf),
        "winning_trades":      len(wins),
        "losing_trades":       len(losses),
        "long_trades":         len(tdf[tdf["direction"] == "LONG"]),
        "short_trades":        len(tdf[tdf["direction"] == "SHORT"]),
        # Charts
        "equity_curve":        eq_curve,
        "underwater_curve":    _underwater_curve(eq_curve, capital),
        # Detail
        "stock_summary":       stock_summary,
        "trade_log":           trades,
        "data_quality":        quality_reports,
    }


def _equity_curve(trades, capital, start_date, end_date):
    dates  = pd.bdate_range(start=start_date, end=end_date)
    equity = capital
    exits  = {}
    for t in trades:
        d = t["exit_date"]
        exits[d] = exits.get(d, 0) + t["pnl"]
    curve = []
    for d in dates:
        ds = str(d.date())
        if ds in exits:
            equity += exits[ds]
        curve.append({"date": ds, "value": round(equity, 2)})
    return curve


def _underwater_curve(eq_curve, capital):
    peak   = capital
    result = []
    for p in eq_curve:
        if p["value"] > peak:
            peak = p["value"]
        dd = round((p["value"] - peak) / peak * 100, 2)
        result.append({"date": p["date"], "drawdown": dd})
    return result


def _max_drawdown(values):
    peak = values[0] if values else 0
    max_dd = 0
    for v in values:
        if v > peak:
            peak = v
        dd = peak - v
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 2)


def _monthly_returns(eq_curve):
    if not eq_curve:
        return []
    df = pd.DataFrame(eq_curve)
    df["date"]  = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    monthly = df.groupby("month")["value"].last().pct_change().dropna()
    return list(monthly * 100)


def _buy_hold(df_dict, start_date, end_date):
    returns = []
    for sym, df in df_dict.items():
        if df is None or df.empty:
            continue
        try:
            in_range = df[df["date"] >= pd.Timestamp(start_date)]
            if len(in_range) < 2:
                continue
            start_p = float(in_range.iloc[0]["close"])
            end_p   = float(in_range.iloc[-1]["close"])
            returns.append((end_p - start_p) / start_p * 100)
        except:
            continue
    return round(np.mean(returns), 2) if returns else 0


def empty_metrics():
    return {
        "net_profit": 0, "gross_profit": 0, "gross_loss": 0,
        "profit_factor": 0, "total_return_pct": 0, "buy_hold_return_pct": 0,
        "monthly_avg_return": 0, "win_rate": 0, "avg_pnl_per_trade": 0,
        "max_profit": 0, "max_loss": 0, "max_consec_losses": 0,
        "avg_win_bars": 0, "avg_loss_bars": 0, "max_drawdown": 0,
        "max_drawdown_pct": 0, "sharpe_ratio": 0, "mar_ratio": 0,
        "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
        "long_trades": 0, "short_trades": 0,
        "equity_curve": [], "underwater_curve": [],
        "stock_summary": [], "trade_log": [], "data_quality": [],
    }


# ============================================================
#  IBKR BAR SIZE MAPPING
# ============================================================

# Frequency string from dashboard/script → IBKR bar_size string
FREQ_TO_BARSIZE = {
    "1 min":   "1 min",
    "5 mins":  "5 mins",
    "15 mins": "15 mins",
    "30 mins": "30 mins",
    "1 hour":  "1 hour",
    "hourly":  "1 hour",    # legacy alias
    "daily":   "1 day",     # legacy alias
    "1 day":   "1 day",
    "1 week":  "1 week",
    "1 month": "1 month",
}

# Bar size → max duration per single Gateway request
BARSIZE_MAX_DURATION = {
    "1 min":   "7 D",
    "5 mins":  "1 M",
    "15 mins": "2 M",
    "30 mins": "2 M",
    "1 hour":  "1 Y",
    "1 day":   "20 Y",
    "1 week":  "20 Y",
    "1 month": "20 Y",
}


# ============================================================
#  MAIN ENTRY POINT — called from server.py
# ============================================================

def run_backtest(screener_name, symbols, frequency, start_date, end_date,
                 capital, sizing_type, sizing_value, duration,
                 cost_type=None, cost_value=0.0,
                 script_path_override=None):
    """
    Main backtest entry point — called from server.py.

    screener_name : filename e.g. "ema_crossover.py"
    symbols       : list of NSE symbol strings
    frequency     : IBKR bar size string OR "script" to read from screener
    start_date    : datetime object
    end_date      : datetime object
    capital       : float
    sizing_type   : "fixed_amount"|"fixed_qty"|"pct_capital"|"full_capital"|"script"
    sizing_value  : float — ignored if sizing_type == "script"
    duration      : IBKR duration string e.g. "3 Y"
    cost_type     : None | "pct" | "abs"
    cost_value    : float
    script_path_override : Path — if screener lives in a category subfolder
    """
    print(f"\n  Running backtest: {screener_name}")
    print(f"  Period: {start_date.date()} → {end_date.date()}")
    print(f"  Capital: ₹{capital:,.0f} | Sizing: {sizing_type}={sizing_value}")

    # ── Load screener module ──────────────────────────────────
    if script_path_override:
        script_path = Path(script_path_override)
    else:
        script_path = SCREENERS_DIR / screener_name
        if not script_path.exists():
            # Search category subfolders
            cat_dir = SCREENERS_DIR / "Categories"
            if cat_dir.exists():
                for cat in cat_dir.iterdir():
                    candidate = cat / screener_name
                    if candidate.exists():
                        script_path = candidate
                        break

    if not script_path.exists():
        return {"error": f"Screener not found: {screener_name}"}

    spec   = importlib.util.spec_from_file_location("screener", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # ── Resolve effective frequency ───────────────────────────
    if frequency == "script":
        raw_freq = getattr(module, "FREQUENCY", "1 day")
    else:
        raw_freq = frequency
    bar_size = FREQ_TO_BARSIZE.get(raw_freq, raw_freq)
    print(f"  Frequency: {bar_size}")

    # ── Resolve sizing from script if requested ───────────────
    if sizing_type == "script":
        sizing_type  = getattr(module, "SIZING_TYPE",  "full_capital")
        sizing_value = getattr(module, "SIZING_VALUE", 100)

    # ── Symbol list ───────────────────────────────────────────
    symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
    if not symbol_list:
        return {"error": "No symbols to backtest"}
    print(f"  Stocks: {len(symbol_list)}")

    # ── Fetch data ────────────────────────────────────────────
    df_dict      = {}
    daily_dict   = {}
    quality_reports = []

    for sym in symbol_list:
        df, daily_df, quality = fetch_data_for_backtest(
            sym, bar_size, duration, start_date, end_date
        )
        quality_reports.append(quality)
        if df is None:
            print(f"  SKIP {sym} — no data")
            continue
        df_dict[sym]    = df
        daily_dict[sym] = daily_df

    if not df_dict:
        return {"error": "No data fetched for any symbol"}

    print(f"  Loaded {len(df_dict)} symbols — running engine...")

    # ── Run engine ────────────────────────────────────────────
    all_trades = run_engine(
        df_dict      = df_dict,
        daily_dict   = daily_dict,
        screener_module = module,
        bar_size     = bar_size,
        capital      = capital,
        sizing_type  = sizing_type,
        sizing_value = sizing_value,
        start_date   = start_date,
        end_date     = end_date,
        cost_type    = cost_type,
        cost_value   = cost_value,
    )

    print(f"  Total trades: {len(all_trades)}")

    # ── Debug: data range + first 5 trades ───────────────────
    for sym, df in df_dict.items():
        print(f"  [Debug] {sym}: {len(df)} bars | "
              f"{df['date'].iloc[0]} -> {df['date'].iloc[-1]}")
    if all_trades:
        print("  [Debug] First 5 trades:")
        for t in all_trades[:5]:
            print(f"    {t['symbol']} | {t['direction']} | "
                  f"Entry: {t['entry_date']} @ {t['entry_price']} | "
                  f"Exit: {t['exit_date']} @ {t['exit_price']} | "
                  f"PnL: {t['pnl']}")
    else:
        print("  [Debug] No trades — check DIRECTION map and signal names")


    # ── Calculate metrics ─────────────────────────────────────
    metrics = calculate_metrics(
        trades        = all_trades,
        capital       = capital,
        df_dict       = df_dict,
        start_date    = start_date,
        end_date      = end_date,
        bar_size      = bar_size,
        quality_reports = quality_reports,
    )

    metrics["screener"]     = screener_name
    metrics["frequency"]    = bar_size
    metrics["capital"]      = capital
    metrics["sizing_type"]  = sizing_type
    metrics["sizing_value"] = sizing_value
    metrics["start_date"]   = str(start_date.date())
    metrics["end_date"]     = str(end_date.date())
    metrics["stocks"]       = symbol_list

    print(f"\n  Done — {metrics['total_trades']} trades | Net P&L: ₹{metrics['net_profit']:,.2f}")
    return metrics
