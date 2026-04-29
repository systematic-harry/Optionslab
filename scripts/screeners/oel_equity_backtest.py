# ============================================================
#  oel_equity_backtest.py
#  OptionLab — OEL Equity Backtest Driver
#
#  Thin driver: delegates entirely to backtester.run_backtest().
#  No custom engine here — this is intentional so OEL runs
#  through the same infrastructure as every other strategy.
#
#  How it works:
#    frequency = "script" → backtester reads FREQUENCY from oel.py
#    oel.py has FREQUENCY = "hourly"
#    backtester routes to run_backtest_hourly_multi() automatically
#
#  To run:
#    python oel_equity_backtest.py
#    (adjust CONFIG block below as needed)
#
#  Save to: D:\optionlab\scripts\oel_equity_backtest.py
# ============================================================

import sys
from pathlib import Path
from datetime import datetime, timedelta

# ── Make backtester importable ────────────────────────────────
SCRIPT_DIR = Path(__file__).parent if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(SCRIPT_DIR))

from backtester import run_backtest   # noqa: E402

# ── CONFIG ────────────────────────────────────────────────────
# Adjust these before running

SCREENER_NAME = "oel.py"           # must exist in D:\optionlab\scripts\screeners\

USE_GCS       = False              # True = load Nifty 50 from GCS universe file
                                   # False = use SYMBOLS list below

SYMBOLS = [                        # ignored if USE_GCS = True
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "BAJFINANCE.NS",
    "HCLTECH.NS", "SUNPHARMA.NS", "TITAN.NS", "ULTRACEMCO.NS", "WIPRO.NS",
    "NESTLEIND.NS", "M&M.NS", "ADANIENT.NS", "POWERGRID.NS", "NTPC.NS",
    "TATAMOTORS.NS", "ONGC.NS", "JSWSTEEL.NS", "BAJAJFINSV.NS", "TECHM.NS",
    "TATASTEEL.NS", "COALINDIA.NS", "HINDALCO.NS", "GRASIM.NS", "BAJAJ-AUTO.NS",
    "INDUSINDBK.NS", "DRREDDY.NS", "CIPLA.NS", "EICHERMOT.NS", "BRITANNIA.NS",
    "DIVISLAB.NS", "BPCL.NS", "HEROMOTOCO.NS", "APOLLOHOSP.NS", "ADANIPORTS.NS",
    "TATACONSUM.NS", "SBILIFE.NS", "HDFCLIFE.NS", "LTIM.NS", "SHRIRAMFIN.NS",
]

END_DATE      = datetime.today()
START_DATE    = END_DATE - timedelta(days=730)   # ~2 years

CAPITAL       = 10_00_000          # ₹10 lakh starting capital
SIZING_TYPE   = "full_capital"     # even split across signals each day
SIZING_VALUE  = 0                  # not used for full_capital

# "script" tells backtester to read FREQUENCY from oel.py (= "hourly")
FREQUENCY     = "script"

# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("OEL EQUITY BACKTEST")
    print(f"Period : {START_DATE.date()} → {END_DATE.date()}")
    print(f"Capital: ₹{CAPITAL:,.0f}")
    print(f"Symbols: {'GCS universe' if USE_GCS else f'{len(SYMBOLS)} stocks'}")
    print("=" * 60)

    results = run_backtest(
        screener_name = SCREENER_NAME,
        symbols       = SYMBOLS,
        use_gcs       = USE_GCS,
        frequency     = FREQUENCY,
        start_date    = START_DATE,
        end_date      = END_DATE,
        capital       = CAPITAL,
        sizing_type   = SIZING_TYPE,
        sizing_value  = SIZING_VALUE,
    )

    if "error" in results:
        print(f"\nBacktest error: {results['error']}")
        sys.exit(1)

    # ── Print summary ─────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    keys = [
        "total_trades", "winning_trades", "losing_trades", "win_rate",
        "net_profit", "total_return_pct", "buy_hold_return_pct",
        "max_drawdown_pct", "sharpe_ratio", "profit_factor",
        "avg_pnl_per_trade", "monthly_avg_return",
        "best_stock", "worst_stock",
    ]
    for k in keys:
        v = results.get(k, "—")
        if isinstance(v, float):
            print(f"  {k:<26}: {v:.2f}")
        else:
            print(f"  {k:<26}: {v}")

    print("\nPer-stock breakdown:")
    for s in results.get("stock_summary", []):
        print(f"  {s['symbol']:<18} trades={s['total_trades']:>3}  "
              f"win%={s['win_rate']:>5.1f}  pnl=₹{s['total_pnl']:>10,.0f}")
