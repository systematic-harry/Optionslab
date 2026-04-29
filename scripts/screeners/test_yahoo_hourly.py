import pandas as pd
import numpy as np
import yfinance as yf
import warnings
warnings.filterwarnings("ignore")

print("\n" + "=" * 60)
print("  YAHOO FINANCE HOURLY DATA — QUALITY CHECK")
print("=" * 60)

print("\n  Downloading TCS.NS hourly (2y)...")
raw = yf.download("TCS.NS", period="2y", interval="1h",
                  auto_adjust=True, progress=False)

if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = raw.columns.get_level_values(0)

df = raw[["Open","High","Low","Close","Volume"]].copy()
df.index = pd.to_datetime(df.index)

# Convert to IST if timezone aware, otherwise assume IST
if df.index.tz is not None:
    df.index = df.index.tz_convert("Asia/Kolkata")

df = df.sort_index()
df["Hour"]   = df.index.hour
df["Minute"] = df.index.minute
df["Date"]   = df.index.date

print(f"  Total bars: {len(df)}")
print(f"  Date range: {df.index[0]} to {df.index[-1]}")
print(f"  Days covered: {(df.index[-1] - df.index[0]).days}")

print(f"\n\n{'='*60}")
print(f"  MARCH 20, 2026 — ALL BARS")
print(f"{'='*60}")

mar20 = df[df["Date"] == pd.Timestamp("2026-03-20").date()]
if mar20.empty:
    print("  NO DATA for March 20, 2026!")
else:
    for idx, row in mar20.iterrows():
        print(f"  {idx.strftime('%H:%M')}  O:{row['Open']:.2f}  H:{row['High']:.2f}  "
              f"L:{row['Low']:.2f}  C:{row['Close']:.2f}  V:{int(row['Volume'])}")

    print(f"\n  9:15-11:00 window:")
    window = mar20[
        ((mar20["Hour"] == 9) & (mar20["Minute"] >= 15)) |
        (mar20["Hour"] == 10)
    ]
    if window.empty:
        print("  NO BARS in 9:15-11:00 window!")
    else:
        day_open = float(mar20.iloc[0]["Open"])
        print(f"  Day Open: {day_open:.2f}")
        for idx, row in window.iterrows():
            low_ok = "OK" if row["Low"] >= day_open else "BREACHED"
            high_ok = "OK" if row["High"] <= day_open else "BREACHED"
            print(f"  {idx.strftime('%H:%M')}  L:{row['Low']:.2f} ({low_ok})  "
                  f"H:{row['High']:.2f} ({high_ok})")

        low_breached = (window["Low"] < day_open).any()
        high_breached = (window["High"] > day_open).any()
        print(f"\n  OEL check (Low never < Open): {'FAIL' if low_breached else 'PASS'}")
        print(f"  OEH check (High never > Open): {'FAIL' if high_breached else 'PASS'}")

print(f"\n\n{'='*60}")
print(f"  DATA QUALITY — ALL DAYS")
print(f"{'='*60}")

days = df.groupby("Date").size()

print(f"  Total trading days: {len(days)}")
print(f"  Avg bars per day: {days.mean():.1f}")
print(f"  Min bars in a day: {days.min()}")
print(f"  Max bars in a day: {days.max()}")

thin = days[days < 5]
if len(thin) > 0:
    print(f"\n  Days with < 5 bars (gaps): {len(thin)}")
    for d, count in thin.head(10).items():
        print(f"    {d} -- {count} bars")
else:
    print(f"\n  No thin days -- data looks clean!")

print(f"\n  Checking 9:15-11:00 bar availability...")
morning_count = 0
missing_morning = 0
for date in days.index:
    day_df = df[df["Date"] == date]
    window = day_df[
        ((day_df["Hour"] == 9) & (day_df["Minute"] >= 15)) |
        (day_df["Hour"] == 10)
    ]
    if len(window) > 0:
        morning_count += 1
    else:
        missing_morning += 1

print(f"  Days WITH 9:15-11:00 bars: {morning_count}")
print(f"  Days WITHOUT: {missing_morning}")
print(f"  Coverage: {round(morning_count/len(days)*100,1)}%")

print(f"\n  Done!")
