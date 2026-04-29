import pandas as pd
import numpy as np
import yfinance as yf
import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.insert(0, r"D:\optionlab\scripts")
from screeners.oeh_oel import screen

print("\n" + "=" * 60)
print("  OEH/OEL TEST — TCS — MARCH 2026")
print("=" * 60)

print("\n  Downloading TCS.NS hourly...")
raw = yf.download("TCS.NS", period="2y", interval="1h",
                  auto_adjust=True, progress=False)

if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = raw.columns.get_level_values(0)

df = raw[["Open","High","Low","Close","Volume"]].copy()
df.index = pd.to_datetime(df.index)
if df.index.tz is not None:
    df.index = df.index.tz_convert("Asia/Kolkata")
df = df.sort_index()
df["Hour"]   = df.index.hour
df["Minute"] = df.index.minute
df["Date"]   = df.index.date

# Build daily
daily = df.groupby("Date").agg(
    Open=("Open","first"), High=("High","max"),
    Low=("Low","min"), Close=("Close","last"),
).reset_index()
daily["Date"] = pd.to_datetime(daily["Date"])

# Test March 15-31
print(f"\n  {'Date':<14} {'Signal':<10} {'Entry':>10} {'Reasons'}")
print(f"  {'─'*70}")

for d in pd.date_range("2026-03-15", "2026-03-31"):
    date = d.date()
    day_bars = df[df["Date"] == date]
    if day_bars.empty:
        continue

    signal, reasons, entry = screen(df, date, daily)
    marker = " <<<" if signal in ("STRONG", "SHORT") else ""
    entry_str = f"{entry:.2f}" if entry else "—"
    print(f"  {date}   {signal:<10} {entry_str:>10}  {reasons[-1]}{marker}")

print(f"\n  Done!")
