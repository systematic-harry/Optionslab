# ============================================================
#  ema_crossover.py
#  Strategy: 10 EMA / 21 EMA Crossover
#
#  Rules:
#  Signal Day  → close existing position at CLOSE
#  Next Day    → open new position at OPEN
# ============================================================

import pandas_ta as ta


def check_conditions():
    return True, "No conditions — runs always"


def screen(df):
    """
    10 EMA / 21 EMA crossover.
    Returns signal based on PREVIOUS bar close.
    Entry will be taken on NEXT bar open by backtester.
    """
    if len(df) < 25:
        return "SKIP", ["Not enough data"]

    ema10 = ta.ema(df["close"], length=10)
    ema21 = ta.ema(df["close"], length=21)

    if ema10 is None or ema21 is None:
        return "SKIP", ["EMA calculation failed"]

    # Current and previous values
    curr10 = round(float(ema10.iloc[-1]), 2)
    curr21 = round(float(ema21.iloc[-1]), 2)
    prev10 = round(float(ema10.iloc[-2]), 2)
    prev21 = round(float(ema21.iloc[-2]), 2)

    bullish_cross = prev10 <= prev21 and curr10 > curr21
    bearish_cross = prev10 >= prev21 and curr10 < curr21

    if bullish_cross:
        return "STRONG", [f"10 EMA {curr10} crossed above 21 EMA {curr21} ✓"]
    elif bearish_cross:
        return "SKIP",   [f"10 EMA {curr10} crossed below 21 EMA {curr21} ✗"]
    elif curr10 > curr21:
        return "WATCH",  [f"10 EMA {curr10} above 21 EMA {curr21} — holding"]
    else:
        return "SKIP",   [f"10 EMA {curr10} below 21 EMA {curr21} ✗"]
