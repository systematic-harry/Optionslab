# ============================================================
#  oel.py
#  OptionLab Screener — OEL Strategy (LONG only, Hourly Logic)
#
#  OEL (Opening Edge Low) — LONG:
#    Pattern: during the 1st hourly bar of the day, Low does not drop
#             more than 0.05% below the day's official Open (from daily_df).
#             i.e. bar1.Low >= Day_Open * (1 - 0.0005)
#
#    NOTE: Day_Open is taken from daily_df (NSE official open, set in the
#          pre-open auction at 9:15:00) — NOT from the 1st hourly bar's Open,
#          which can lag by a few seconds and quote a slightly different price.
#
#    Entry: Open of the 2nd hourly bar of the day (no lookahead)
#    Exit:  Next trading day's 1st bar Open
#
#    Risk levels (informational only):
#      ATR  = 14-period ATR from daily data
#      Stop = Day Open
#      Target = Next Day Open (= actual exit)
#
#    Skip conditions:
#      - Fewer than 2 hourly bars on the day (incl. holidays)
#      - daily_df not provided
#      - Daily row missing or Open NaN for the date (FLAGGED in reasons)
#      - Pattern not satisfied
#      - Next-day open not available (last day in dataset)
#
#  Data: Yahoo Finance hourly
#
#  Save to: D:\optionlab\scripts\screeners\oel.py
# ============================================================

import numpy as np
import pandas as pd

FREQUENCY      = "hourly"      # backtester routes to hourly path when frequency="script"
SIZING_TYPE    = "pct_capital" # 10% of capital per signal
SIZING_VALUE   = 10
ATR_PERIOD     = 14
OEL_TOLERANCE  = 0.0005   # 0.05% — Low may dip this much below Open and still qualify
                          # (e.g. on Open ₹1000, threshold = ₹999.50, so dips up to 50 paise are OK)


def check_conditions():
    return True, "OEL — no pre-conditions"


def screen(df_hourly, date, daily_df=None, periods=ATR_PERIOD):
    """
    df_hourly : full hourly dataframe with columns:
                Open, High, Low, Close, Volume, Date
    date      : the date to check (datetime.date)
    daily_df  : daily OHLC (optional, used for ATR)
    Returns   : (signal, reasons, entry_price)
                signal in {"OEL", "SKIP"}
    """
    reasons = []

    # ── Get today's bars ──────────────────────────────────
    day_bars = df_hourly[df_hourly["Date"] == date]
    if len(day_bars) < 2:
        return "SKIP", ["Need at least 2 hourly bars on the day"], None

    bar1 = day_bars.iloc[0]   # check window (Low source)
    bar2 = day_bars.iloc[1]   # entry bar

    # ── Day Open from daily_df (NSE official open, not 9:15 bar) ──
    if daily_df is None:
        return "SKIP", ["daily_df not provided — cannot fetch official Open"], None
    today_daily = daily_df[daily_df["Date"] == pd.Timestamp(date)]
    if today_daily.empty:
        return "SKIP", [f"Daily row missing for {date} (FLAG: data gap)"], None
    day_open = float(today_daily.iloc[0]["Open"])
    if pd.isna(day_open):
        return "SKIP", [f"Daily Open NaN for {date} (FLAG: data gap)"], None

    bar1_low = float(bar1["Low"])

    # ── OEL check: Low held within 0.05% of Open ─────────
    low_threshold = day_open * (1 - OEL_TOLERANCE)
    if bar1_low < low_threshold:
        return "SKIP", ["Not OEL — Low breached threshold"], None

    # ── Entry & Exit ─────────────────────────────────────
    entry_price = float(bar2["Open"])

    future_bars = df_hourly[df_hourly["Date"] > date]
    if future_bars.empty:
        return "SKIP", ["No next-day data for exit"], None
    next_day = future_bars["Date"].iloc[0]
    exit_price = float(future_bars[future_bars["Date"] == next_day].iloc[0]["Open"])

    # ── ATR from daily ────────────────────────────────────
    if daily_df is not None and len(daily_df) >= ATR_PERIOD:
        prev_days = daily_df[daily_df["Date"] < pd.Timestamp(date)]
        if len(prev_days) >= ATR_PERIOD:
            window = prev_days.iloc[-ATR_PERIOD:]
            highs  = window["High"].values
            lows   = window["Low"].values
            closes = window["Close"].values
            prev_c = np.roll(closes, 1)
            prev_c[0] = closes[0]
            tr  = np.maximum(
                highs - lows,
                np.maximum(np.abs(highs - prev_c), np.abs(lows - prev_c))
            )
            atr = round(float(np.mean(tr)), 2)
        else:
            atr = round(float(day_bars["High"].max() - day_bars["Low"].min()), 2)
    else:
        atr = round(float(day_bars["High"].max() - day_bars["Low"].min()), 2)

    # ── Reasons / context ────────────────────────────────
    reasons.append(f"Open:{day_open:.2f} | Bar1 Low:{bar1_low:.2f} | Threshold:{low_threshold:.2f} (0.05%) — held")
    reasons.append(f"Entry@Bar2 Open:{entry_price:.2f} | Exit@NextOpen:{exit_price:.2f}")
    reasons.append(f"Stop (Day Open):{day_open:.2f}")
    reasons.append(f"ATR({ATR_PERIOD}):{atr}")

    return "OEL", reasons, entry_price
