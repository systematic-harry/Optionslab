import pandas_ta as ta

def check_conditions():
    return True, "No conditions"

def screen(df, periods):
    score   = 0
    reasons = []

    macd_df    = ta.macd(df["close"], fast=12, slow=26, signal=9)
    macd_col   = [c for c in macd_df.columns if "MACD_"  in c and "h" not in c and "s" not in c]
    signal_col = [c for c in macd_df.columns if "MACDs_" in c]
    hist_col   = [c for c in macd_df.columns if "MACDh_" in c]

    macd_val   = round(float(macd_df[macd_col[0]].iloc[-1]),   2)
    signal_val = round(float(macd_df[signal_col[0]].iloc[-1]), 2)
    hist_val   = round(float(macd_df[hist_col[0]].iloc[-1]),   2)
    hist_prev  = round(float(macd_df[hist_col[0]].iloc[-2]),   2)

    if macd_val > signal_val:
        score += 2
        reasons.append(f"MACD {macd_val} > Signal {signal_val} ✓")
    else:
        score -= 1
        reasons.append(f"MACD {macd_val} < Signal {signal_val} ✗")

    if hist_val > 0:
        score += 1
        reasons.append(f"Histogram {hist_val} positive ✓")
    else:
        reasons.append(f"Histogram {hist_val} negative ✗")

    if hist_val > hist_prev:
        score += 1
        reasons.append(f"Histogram growing ✓")
    else:
        reasons.append(f"Histogram shrinking")

    if macd_val > 0:
        score += 1
        reasons.append(f"MACD above zero ✓")
    else:
        reasons.append(f"MACD below zero")

    signal = "STRONG" if score >= 4 else "WATCH" if score >= 2 else "SKIP"
    return signal, reasons
