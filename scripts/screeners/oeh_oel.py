# ============================================================
#  oeh_oel.py
#  OptionLab Screener — OEH/OEL Strategy (Hourly Logic)
#
#  OEL (Opening Edge Low) — LONG:
#    1. From open price to 11:00, Low never drops below Open
#    2. Price only went UP from Open
#    3. TC/BC filter: Open > TC = STRONG, else WATCH
#    4. Entry: 11:15 bar Open
#    5. Exit: Day Close
#
#  OEH (Opening Edge High) — SHORT:
#    1. From open price to 11:00, High never goes above Open
#    2. Price only went DOWN from Open
#    3. TC/BC filter: Open < BC = SHORT, else WATCH
#    4. Entry: 11:15 bar Open
#    5. Exit: Day Close
#
#  Check window: 9:15 bar + 10:15 bar (covers 9:15 to 11:14)
#  Entry bar: 11:15 bar Open
#
#  Data: Yahoo Finance hourly
#  Capital: evenly distributed across all signals per day
#
#  Save to: D:\optionlab\scripts\screeners\oeh_oel.py
# ============================================================

import numpy as np
import pandas as pd

ATR_PERIOD = 14
ATR_MULT   = 1.5


def check_conditions():
    return True, "OEH/OEL — no pre-conditions"


def screen(df_hourly, date, daily_df=None, periods=ATR_PERIOD):
    """
    df_hourly : full hourly dataframe with columns:
                Open, High, Low, Close, Volume, Hour, Minute, Date
    date      : the date to check (datetime.date)
    daily_df  : daily OHLC (optional, built internally if None)
    Returns   : (signal, reasons, entry_price)
    """
    reasons = []

    # ── Get today's bars ──────────────────────────────────
    day_bars = df_hourly[df_hourly["Date"] == date]
    if day_bars.empty or len(day_bars) < 3:
        return "SKIP", ["No data for this day"], None

    day_open  = float(day_bars.iloc[0]["Open"])    # 9:15 bar Open
    day_close = float(day_bars.iloc[-1]["Close"])   # last bar Close

    # ── Check window: 9:15 bar + 10:15 bar ───────────────
    # 9:15 bar covers 9:15–10:14
    # 10:15 bar covers 10:15–11:14 (includes 11:00)
    window = day_bars[
        (day_bars["Hour"] == 9) | (day_bars["Hour"] == 10)
    ]
    if window.empty:
        return "SKIP", ["No bars in check window"], None

    # ── OEL: Low never dropped below Open ─────────────────
    low_breached  = (window["Low"] < day_open).any()

    # ── OEH: High never went above Open ───────────────────
    high_breached = (window["High"] > day_open).any()

    oel = (not low_breached) and high_breached
    oeh = (not high_breached) and low_breached

    if not oel and not oeh:
        return "SKIP", ["Not OEL or OEH"], None

    # ── Entry price = 11:15 bar Open ─────────────────────
    entry_bar = day_bars[
        (day_bars["Hour"] == 11) & (day_bars["Minute"] == 15)
    ]
    if entry_bar.empty:
        after_11 = day_bars[day_bars["Hour"] >= 11]
        if after_11.empty:
            return "SKIP", ["No bar at/after 11:15"], None
        entry_price = float(after_11.iloc[0]["Open"])
    else:
        entry_price = float(entry_bar.iloc[0]["Open"])

    # ── Pivot levels from previous day ────────────────────
    if daily_df is not None and len(daily_df) >= 2:
        prev_days = daily_df[daily_df["Date"] < pd.Timestamp(date)]
        if len(prev_days) >= 1:
            prev = prev_days.iloc[-1]
            prev_high  = float(prev["High"])
            prev_low   = float(prev["Low"])
            prev_close = float(prev["Close"])
        else:
            prev_high = prev_low = prev_close = day_open
    else:
        prev_high = prev_low = prev_close = day_open

    bc = round((prev_high + prev_low) / 2, 2)
    pp = round((prev_high + prev_low + prev_close) / 3, 2)
    tc = round((pp - bc) + pp, 2)

    reasons.append(f"BC:{bc} | PP:{pp} | TC:{tc}")
    reasons.append(f"Open:{day_open} | Entry@11:15:{entry_price}")

    # ── ATR from daily ────────────────────────────────────
    if daily_df is not None and len(daily_df) >= ATR_PERIOD:
        highs  = daily_df["High"].values[-ATR_PERIOD:]
        lows   = daily_df["Low"].values[-ATR_PERIOD:]
        closes = daily_df["Close"].values[-ATR_PERIOD:]
        prev_c = np.roll(closes, 1)
        prev_c[0] = closes[0]
        tr  = np.maximum(highs - lows,
              np.maximum(np.abs(highs - prev_c), np.abs(lows - prev_c)))
        atr = round(float(np.mean(tr)), 2)
    else:
        atr = round(float(day_bars["High"].max() - day_bars["Low"].min()), 2)

    reasons.append(f"ATR({ATR_PERIOD}):{atr}")

    # ── OEL — LONG ───────────────────────────────────────
    if oel:
        window_low  = float(window["Low"].min())
        window_high = float(window["High"].max())
        reasons.append(f"OEL — Low held at {window_low}, High moved to {window_high}")

        stop   = round(day_open - ATR_MULT * atr, 2)
        target = round(entry_price + ATR_MULT * atr, 2)
        reasons.append(f"Stop:{stop} | Target:{target}")

        if day_open > tc:
            reasons.append("Open > TC — strong OEL")
            return "STRONG", reasons, entry_price
        else:
            reasons.append("OEL but Open below TC")
            return "WATCH", reasons, entry_price

    # ── OEH — SHORT ──────────────────────────────────────
    if oeh:
        window_low  = float(window["Low"].min())
        window_high = float(window["High"].max())
        reasons.append(f"OEH — High held at {window_high}, Low moved to {window_low}")

        stop   = round(day_open + ATR_MULT * atr, 2)
        target = round(entry_price - ATR_MULT * atr, 2)
        reasons.append(f"Stop:{stop} | Target:{target}")

        if day_open < bc:
            reasons.append("Open < BC — strong OEH")
            return "SHORT", reasons, entry_price
        else:
            reasons.append("OEH but Open above BC")
            return "WATCH", reasons, entry_price

    return "SKIP", reasons, None
