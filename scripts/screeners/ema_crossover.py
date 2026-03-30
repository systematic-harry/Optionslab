# ============================================================
#  ema_crossover.py
#  Strategy: 10 EMA / 21 EMA Crossover
#
#  Entry → 10 EMA crosses above 21 EMA
#  Exit  → 10 EMA crosses below 21 EMA
#
#  Place in: D:\optionlab\scripts\screeners\
# ============================================================

import pandas_ta as ta

def check_conditions():
    return True, "No conditions — runs always"

def screen(df):
    """
    10 EMA / 21 EMA crossover strategy.
    df → OHLCV DataFrame
    """
    reasons = []

    if len(df) < 25:
        return "SKIP", ["Not enough data"]

    ema10 = ta.ema(df["close"], length=10)
    ema21 = ta.ema(df["close"], length=21)

    if ema10 is None or ema21 is None:
        return "SKIP", ["EMA calculation failed"]

    current_10  = round(float(ema10.iloc[-1]), 2)
    current_21  = round(float(ema21.iloc[-1]), 2)
    previous_10 = round(float(ema10.iloc[-2]), 2)
    previous_21 = round(float(ema21.iloc[-2]), 2)

    # Crossover detection
    bullish_cross = previous_10 <= previous_21 and current_10 > current_21
    bearish_cross = previous_10 >= previous_21 and current_10 < current_21

    if bullish_cross:
        signal = "STRONG"
        reasons.append(f"10 EMA {current_10} crossed above 21 EMA {current_21} ✓")
    elif current_10 > current_21:
        signal = "WATCH"
        reasons.append(f"10 EMA {current_10} above 21 EMA {current_21} — holding")
    elif bearish_cross:
        signal = "SKIP"
        reasons.append(f"10 EMA {current_10} crossed below 21 EMA {current_21} ✗")
    else:
        signal = "SKIP"
        reasons.append(f"10 EMA {current_10} below 21 EMA {current_21} ✗")

    return signal, reasons
