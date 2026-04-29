import pandas as pd
import numpy as np
import yfinance as yf
import time
import warnings
warnings.filterwarnings("ignore")

NIFTY50 = {
    "RELIANCE":   "RELIANCE.NS",   "TCS":        "TCS.NS",
    "HDFCBANK":   "HDFCBANK.NS",   "INFY":       "INFY.NS",
    "ICICIBANK":  "ICICIBANK.NS",  "HINDUNILVR": "HINDUNILVR.NS",
    "ITC":        "ITC.NS",        "SBIN":       "SBIN.NS",
    "BHARTIARTL": "BHARTIARTL.NS", "KOTAKBANK":  "KOTAKBANK.NS",
    "LT":         "LT.NS",        "AXISBANK":   "AXISBANK.NS",
    "ASIANPAINT": "ASIANPAINT.NS", "MARUTI":     "MARUTI.NS",
    "TITAN":      "TITAN.NS",     "SUNPHARMA":  "SUNPHARMA.NS",
    "BAJFINANCE": "BAJFINANCE.NS", "WIPRO":      "WIPRO.NS",
    "HCLTECH":    "HCLTECH.NS",   "NTPC":       "NTPC.NS",
    "POWERGRID":  "POWERGRID.NS", "ULTRACEMCO": "ULTRACEMCO.NS",
    "TATAMOTORS": "TATAMOTORS.NS", "TATASTEEL":  "TATASTEEL.NS",
    "ONGC":       "ONGC.NS",      "COALINDIA":  "COALINDIA.NS",
    "M&M":        "M&M.NS",       "ADANIENT":   "ADANIENT.NS",
    "ADANIPORTS": "ADANIPORTS.NS", "BAJAJFINSV": "BAJAJFINSV.NS",
    "BAJAJ-AUTO": "BAJAJ-AUTO.NS", "DRREDDY":    "DRREDDY.NS",
    "NESTLEIND":  "NESTLEIND.NS", "JSWSTEEL":   "JSWSTEEL.NS",
    "GRASIM":     "GRASIM.NS",    "CIPLA":      "CIPLA.NS",
    "TECHM":      "TECHM.NS",     "DIVISLAB":   "DIVISLAB.NS",
    "HEROMOTOCO": "HEROMOTOCO.NS", "BPCL":       "BPCL.NS",
    "BRITANNIA":  "BRITANNIA.NS", "EICHERMOT":  "EICHERMOT.NS",
    "INDUSINDBK": "INDUSINDBK.NS", "HINDALCO":   "HINDALCO.NS",
    "SBILIFE":    "SBILIFE.NS",   "HDFCLIFE":   "HDFCLIFE.NS",
    "APOLLOHOSP": "APOLLOHOSP.NS", "TATACONSUM": "TATACONSUM.NS",
    "LTIM":       "LTIM.NS",      "SHRIRAMFIN": "SHRIRAMFIN.NS",
}

print("\n" + "=" * 70)
print("  OEL/OEH OBSERVATION — NIFTY 50 — WITH TC/BC FILTER")
print("  Cutoffs: 10:15 (1 bar) and 11:15 (2 bars)")
print("  Filter: OEL only if Open > TC | OEH only if Open < BC")
print("  Returns: Same Day Close, Next Day Open, Next Day Close, 1 Week Close")
print("  Pivot: BC=(H+L)/2  PP=(H+L+C)/3  TC=(PP-BC)+PP")
print("=" * 70)

all_oel_1bar = []
all_oeh_1bar = []
all_oel_2bar = []
all_oeh_2bar = []

for i, (name, ticker) in enumerate(NIFTY50.items(), 1):
    print(f"  [{i:02d}/50] {name}...", end="", flush=True)

    try:
        raw = yf.download(ticker, period="2y", interval="1h",
                          auto_adjust=True, progress=False)
        if raw is None or len(raw) < 100:
            print(" SKIP")
            continue
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        df = raw[["Open","High","Low","Close"]].copy()
        df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("Asia/Kolkata")
        df = df.sort_index()
        df["Date"] = df.index.date

        # Build daily
        daily = df.groupby("Date").agg(
            Open=("Open","first"), High=("High","max"),
            Low=("Low","min"), Close=("Close","last"),
        ).reset_index()
        daily["Date"] = pd.to_datetime(daily["Date"]).dt.date
        daily = daily.sort_values("Date").reset_index(drop=True)

        dates = sorted(df["Date"].unique())
        counts = {"oel1": 0, "oeh1": 0, "oel2": 0, "oeh2": 0}

        for date in dates:
            day_bars = df[df["Date"] == date].reset_index(drop=True)
            if len(day_bars) < 3:
                continue

            day_open      = float(day_bars.iloc[0]["Open"])
            day_close     = float(day_bars.iloc[-1]["Close"])
            full_day_low  = float(day_bars["Low"].min())
            full_day_high = float(day_bars["High"].max())
            price_1115    = float(day_bars.iloc[2]["Open"])

            # Pivot from previous day
            d_idx = daily[daily["Date"] == date].index
            if len(d_idx) == 0 or d_idx[0] == 0:
                continue
            d_idx = d_idx[0]

            prev = daily.iloc[d_idx - 1]
            prev_high  = float(prev["High"])
            prev_low   = float(prev["Low"])
            prev_close = float(prev["Close"])

            bc = round((prev_high + prev_low) / 2, 2)
            pp = round((prev_high + prev_low + prev_close) / 3, 2)
            tc = round((pp - bc) + pp, 2)

            # Next day / 1 week
            next_day_open  = float(daily.iloc[d_idx+1]["Open"])  if d_idx+1 < len(daily) else day_close
            next_day_close = float(daily.iloc[d_idx+1]["Close"]) if d_idx+1 < len(daily) else day_close
            week_close     = float(daily.iloc[min(d_idx+5, len(daily)-1)]["Close"])

            # ── CUTOFF 1: After bar 0 only (10:15) ───────
            bar0 = day_bars.iloc[0:1]
            low_breach_1  = (bar0["Low"] < day_open).any()
            high_breach_1 = (bar0["High"] > day_open).any()
            entry_1 = float(day_bars.iloc[1]["Open"])

            # OEL at 10:15: Low held + Open > TC
            if not low_breach_1 and day_open > tc:
                row = {
                    "Stock": name, "Date": str(date),
                    "Open": round(day_open, 2),
                    "BC": bc, "PP": pp, "TC": tc,
                    "Entry_1015": round(entry_1, 2),
                    "Price_1115": round(price_1115, 2),
                    "Day_Low": round(full_day_low, 2),
                    "Day_High": round(full_day_high, 2),
                    "Day_Close": round(day_close, 2),
                    "Next_Day_Open": round(next_day_open, 2),
                    "Next_Day_Close": round(next_day_close, 2),
                    "Week_Close": round(week_close, 2),
                    "Low_held_full_day": full_day_low >= day_open,
                    "Ret_DayClose%": round((day_close - entry_1) / entry_1 * 100, 4),
                    "Ret_NextOpen%": round((next_day_open - entry_1) / entry_1 * 100, 4),
                    "Ret_NextClose%": round((next_day_close - entry_1) / entry_1 * 100, 4),
                    "Ret_WeekClose%": round((week_close - entry_1) / entry_1 * 100, 4),
                }
                all_oel_1bar.append(row)
                counts["oel1"] += 1

            # OEH at 10:15: High held + Open < BC
            if not high_breach_1 and day_open < bc:
                row = {
                    "Stock": name, "Date": str(date),
                    "Open": round(day_open, 2),
                    "BC": bc, "PP": pp, "TC": tc,
                    "Entry_1015": round(entry_1, 2),
                    "Price_1115": round(price_1115, 2),
                    "Day_Low": round(full_day_low, 2),
                    "Day_High": round(full_day_high, 2),
                    "Day_Close": round(day_close, 2),
                    "Next_Day_Open": round(next_day_open, 2),
                    "Next_Day_Close": round(next_day_close, 2),
                    "Week_Close": round(week_close, 2),
                    "High_held_full_day": full_day_high <= day_open,
                    "Ret_DayClose%": round((entry_1 - day_close) / entry_1 * 100, 4),
                    "Ret_NextOpen%": round((entry_1 - next_day_open) / entry_1 * 100, 4),
                    "Ret_NextClose%": round((entry_1 - next_day_close) / entry_1 * 100, 4),
                    "Ret_WeekClose%": round((entry_1 - week_close) / entry_1 * 100, 4),
                }
                all_oeh_1bar.append(row)
                counts["oeh1"] += 1

            # ── CUTOFF 2: After bar 0+1 (11:15) ──────────
            window2 = day_bars.iloc[0:2]
            low_breach_2  = (window2["Low"] < day_open).any()
            high_breach_2 = (window2["High"] > day_open).any()
            entry_2 = price_1115

            # OEL at 11:15: Low held + Open > TC
            if not low_breach_2 and day_open > tc:
                row = {
                    "Stock": name, "Date": str(date),
                    "Open": round(day_open, 2),
                    "BC": bc, "PP": pp, "TC": tc,
                    "Entry_1115": round(entry_2, 2),
                    "Day_Low": round(full_day_low, 2),
                    "Day_High": round(full_day_high, 2),
                    "Day_Close": round(day_close, 2),
                    "Next_Day_Open": round(next_day_open, 2),
                    "Next_Day_Close": round(next_day_close, 2),
                    "Week_Close": round(week_close, 2),
                    "Low_held_full_day": full_day_low >= day_open,
                    "Ret_DayClose%": round((day_close - entry_2) / entry_2 * 100, 4),
                    "Ret_NextOpen%": round((next_day_open - entry_2) / entry_2 * 100, 4),
                    "Ret_NextClose%": round((next_day_close - entry_2) / entry_2 * 100, 4),
                    "Ret_WeekClose%": round((week_close - entry_2) / entry_2 * 100, 4),
                }
                all_oel_2bar.append(row)
                counts["oel2"] += 1

            # OEH at 11:15: High held + Open < BC
            if not high_breach_2 and day_open < bc:
                row = {
                    "Stock": name, "Date": str(date),
                    "Open": round(day_open, 2),
                    "BC": bc, "PP": pp, "TC": tc,
                    "Entry_1115": round(entry_2, 2),
                    "Day_Low": round(full_day_low, 2),
                    "Day_High": round(full_day_high, 2),
                    "Day_Close": round(day_close, 2),
                    "Next_Day_Open": round(next_day_open, 2),
                    "Next_Day_Close": round(next_day_close, 2),
                    "Week_Close": round(week_close, 2),
                    "High_held_full_day": full_day_high <= day_open,
                    "Ret_DayClose%": round((entry_2 - day_close) / entry_2 * 100, 4),
                    "Ret_NextOpen%": round((entry_2 - next_day_open) / entry_2 * 100, 4),
                    "Ret_NextClose%": round((entry_2 - next_day_close) / entry_2 * 100, 4),
                    "Ret_WeekClose%": round((entry_2 - week_close) / entry_2 * 100, 4),
                }
                all_oeh_2bar.append(row)
                counts["oeh2"] += 1

        print(f" 10:15->OEL:{counts['oel1']} OEH:{counts['oeh1']} | 11:15->OEL:{counts['oel2']} OEH:{counts['oeh2']}")

    except Exception as e:
        print(f" ERROR: {e}")

    time.sleep(0.3)

# ══════════════════════════════════════════════════════════════
def show_stats(label, df_list, held_col):
    d = pd.DataFrame(df_list)
    if d.empty:
        print(f"\n  {label}: No data")
        return
    total = len(d)
    held  = d[held_col].sum()
    broke = total - held

    print(f"\n  {label}")
    print(f"  {'─'*65}")
    print(f"  Days: {total} | Held all day: {held} ({round(held/total*100,1)}%) | Broke: {broke} ({round(broke/total*100,1)}%)")

    for ret_col, exit_name in [
        ("Ret_DayClose%", "Same Day Close"),
        ("Ret_NextOpen%", "Next Day Open"),
        ("Ret_NextClose%", "Next Day Close"),
        ("Ret_WeekClose%", "1 Week Close"),
    ]:
        wins = (d[ret_col] > 0).sum()
        wr = round(wins / total * 100, 1)
        avg = round(d[ret_col].mean(), 4)
        tot = round(d[ret_col].sum(), 2)
        print(f"    {exit_name:<18} Win: {wins}/{total} = {wr:>5.1f}%  Avg: {avg:>+8.4f}%  Total: {tot:>+10.2f}%")

print(f"\n\n{'='*70}")
print(f"  RESULTS — CUTOFF 10:15 (1 bar) + TC/BC FILTER")
print(f"{'='*70}")
show_stats("OEL — Low held + Open > TC", all_oel_1bar, "Low_held_full_day")
show_stats("OEH — High held + Open < BC", all_oeh_1bar, "High_held_full_day")

print(f"\n\n{'='*70}")
print(f"  RESULTS — CUTOFF 11:15 (2 bars) + TC/BC FILTER")
print(f"{'='*70}")
show_stats("OEL — Low held + Open > TC", all_oel_2bar, "Low_held_full_day")
show_stats("OEH — High held + Open < BC", all_oeh_2bar, "High_held_full_day")

# ── Save Excel ────────────────────────────────────────────────
OUTPUT = r"D:\optionlab\reports\oeh_oel_observation.xlsx"
try:
    with pd.ExcelWriter(OUTPUT, engine="openpyxl") as w:
        if all_oel_1bar:
            pd.DataFrame(all_oel_1bar).to_excel(w, sheet_name="OEL 10.15", index=False)
        if all_oeh_1bar:
            pd.DataFrame(all_oeh_1bar).to_excel(w, sheet_name="OEH 10.15", index=False)
        if all_oel_2bar:
            pd.DataFrame(all_oel_2bar).to_excel(w, sheet_name="OEL 11.15", index=False)
        if all_oeh_2bar:
            pd.DataFrame(all_oeh_2bar).to_excel(w, sheet_name="OEH 11.15", index=False)
    print(f"\n  Saved: {OUTPUT}")
except:
    OUTPUT = "oeh_oel_observation.xlsx"
    with pd.ExcelWriter(OUTPUT, engine="openpyxl") as w:
        if all_oel_1bar:
            pd.DataFrame(all_oel_1bar).to_excel(w, sheet_name="OEL 10.15", index=False)
        if all_oeh_1bar:
            pd.DataFrame(all_oeh_1bar).to_excel(w, sheet_name="OEH 10.15", index=False)
        if all_oel_2bar:
            pd.DataFrame(all_oel_2bar).to_excel(w, sheet_name="OEL 11.15", index=False)
        if all_oeh_2bar:
            pd.DataFrame(all_oeh_2bar).to_excel(w, sheet_name="OEH 11.15", index=False)
    print(f"\n  Saved: {OUTPUT}")

print(f"\n  Done!")
