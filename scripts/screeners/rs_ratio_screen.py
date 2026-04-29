# ============================================================
#  rs_ratio_screener.py  (server-compatible version)
#  Goes in: D:\optionlab\scripts\screeners\rs_ratio_screener.py
#
#  DATA_SOURCE = "yahoo" tells server.py to use Yahoo Finance path
#  Stock list read from STOCK_LIST_FILE
#  Benchmark downloaded in check_conditions(), passed to screen()
# ============================================================

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ── Server.py reads these module-level attributes ─────────────
DATA_SOURCE     = "yahoo"
DATA_PERIOD     = "3y"
STOCK_LIST_FILE = r"D:\optionlab\data\rs_ratio_stocks.csv"
BENCHMARK       = "^CRSLDX"        # Nifty 500

# ── Parameters ────────────────────────────────────────────────
EMA_SHORT        = 10
EMA_LONG         = 21
ZSCORE_RATIO_WIN = 252
ZSCORE_MOM_WIN   = 126

# ── Default stock list (fallback if CSV not found) ────────────
DEFAULT_STOCKS = {
    "MARUTI.NS": "Maruti Suzuki",
    "M&M.NS": "Mahindra & Mahindra",
    "BAJAJ-AUTO.NS": "Bajaj Auto",
    "EICHERMOT.NS": "Eicher Motors",
    "TVSMOTOR.NS": "TVS Motor Company",
    "HYUNDAI.NS": "Hyundai Motor",
    "MOTHERSON.NS": "Samvardhana Motherson",
    "TATAMOTORS.NS": "Tata Motors",
    "BOSCHLTD.NS": "Bosch",
    "HEROMOTOCO.NS": "Hero MotoCorp",
    "ASHOKLEY.NS": "Ashok Leyland",
    "BHARATFORG.NS": "Bharat Forge",
    "UNOMINDA.NS": "UNO Minda",
    "MRF.NS": "MRF",
    "TIINDIA.NS": "Tube Investments",
    "BALKRISIND.NS": "Balkrishna Industries",
    "SONACOMS.NS": "Sona BLW Precision",
    "APOLLOTYRE.NS": "Apollo Tyres",
    "EXIDEIND.NS": "Exide Industries",
    "AMARARAJA.NS": "Amara Raja Energy",
    "SBIN.NS": "SBI",
    "BEL.NS": "Bharat Electronics",
    "VEDL.NS": "Vedanta",
    "SHRIRAMFIN.NS": "Shriram Finance",
    "UNIONBANK.NS": "Union Bank",
    "MUTHOOTFIN.NS": "Muthoot Finance",
    "CUMMINSIND.NS": "Cummins",
    "BSE.NS": "BSE Ltd",
    "INDIANB.NS": "Indian Bank",
    "SOLARINDS.NS": "Solar Industries",
    "CANBK.NS": "Canara Bank",
    "POWERINDIA.NS": "Hitachi Energy",
    "POLYCAB.NS": "Polycab",
    "ABCAPITAL.NS": "Aditya Birla Capital",
    "NALCO.NS": "National Aluminium",
    "NYKAA.NS": "Nykaa",
    "AUBANK.NS": "AU SF Bank",
    "PAYTM.NS": "Paytm",
    "FEDERALBNK.NS": "Federal Bank",
    "LTF.NS": "L&T Finance",
    "MCX.NS": "MCX",
    "BANKINDIA.NS": "Bank of India",
    "FORTIS.NS": "Fortis Healthcare",
    "GLENMARK.NS": "Glenmark Pharma",
    "LAURUSLABS.NS": "Laurus Labs",
    "MFSL.NS": "Max Financial",
    "ONGC.NS": "ONGC",
    "ADANIPOWER.NS": "Adani Power",
    "ADANIPORTS.NS": "Adani Ports",
    "HAL.NS": "HAL",
    "ADANIENT.NS": "Adani Enterprises",
    "TATASTEEL.NS": "Tata Steel",
    "ADANIGREEN.NS": "Adani Green Energy",
    "PFC.NS": "Power Finance",
    "BANKBARODA.NS": "Bank of Baroda",
    "DLF.NS": "DLF",
    "ADANIENSOL.NS": "Adani Energy",
    "IRFC.NS": "IRFC",
    "PNB.NS": "Punjab National Bank",
    "TATAPOWER.NS": "Tata Power",
    "INDUSTOWER.NS": "Indus Towers",
    "AMBUJACEM.NS": "Ambuja Cements",
    "GAIL.NS": "GAIL",
    "GMRAIRPORT.NS": "GMR Airports",
    "IDEA.NS": "Vodafone Idea",
    "MAZDOCK.NS": "Mazagon Dock",
    "BHEL.NS": "BHEL",
    "RECLTD.NS": "REC",
    "JSWENERGY.NS": "JSW Energy",
    "SAIL.NS": "SAIL",
    "INDUSINDBK.NS": "IndusInd Bank",
    "ATGL.NS": "Adani Total Gas",
    "GODREJPROP.NS": "Godrej Properties",
    "MOTILALOFS.NS": "Motilal Oswal",
    "CONCOR.NS": "Container Corp",
    "HUDCO.NS": "HUDCO",
    "LICHSGFIN.NS": "LIC Housing Finance",
    "BANDHANBNK.NS": "Bandhan Bank",
    "IRB.NS": "IRB Infra",
    "NBCC.NS": "NBCC",
    "MANAPPURAM.NS": "Manappuram Finance",
    "RBLBANK.NS": "RBL Bank",
    "AARTIIND.NS": "Aarti Industries",
    "HFCL.NS": "HFCL",
    "NCC.NS": "NCC",
    "ZEEL.NS": "Zee Entertainment",
    "BHARTIARTL.NS": "Bharti Airtel",
    "BAJFINANCE.NS": "Bajaj Finance",
    "ASIANPAINT.NS": "Asian Paints",
    "HINDALCO.NS": "Hindalco",
    "SBILIFE.NS": "SBI Life Insurance",
    "INDIGO.NS": "InterGlobe Aviation",
    "CHOLAFIN.NS": "Cholamandalam",
    "UPL.NS": "UPL",
    "HDFCAMC.NS": "HDFC AMC",
    "NAM-INDIA.NS": "Nippon Life AMC",
    "360ONE.NS": "360 One WAM",
    "ANANDRATHI.NS": "Anand Rathi Wealth",
    "ABSLAMC.NS": "ABSL AMC",
    "CDSL.NS": "CDSL",
    "ANGELONE.NS": "Angel One",
    "NUVAMA.NS": "Nuvama Wealth",
    "CAMS.NS": "CAMS",
    "KFINTECH.NS": "KFin Technologies",
    "UTIAMC.NS": "UTI AMC",
    "IEX.NS": "IEX",
    "GET&D.NS": "GE T&D",
    "M&MFIN.NS": "M&M Financial",
    "RADICO.NS": "Radico Khaitan",
    "NH.NS": "Narayana Hrudayalaya",
    "ASTERDM.NS": "Aster DM Healthcare",
    "NAVINFLUOR.NS": "Navin Fluorine",
    "FORCEMOT.NS": "Force Motors",
    "KARURVYSYA.NS": "Karur Vysya Bank",
    "PTCIL.NS": "PTC Industries",
    "ASAHIINDIA.NS": "Asahi India Glass",
    "HBLPOWER.NS": "HBL Power Systems",
    "IIFL.NS": "IIFL Finance",
    "GMDC.NS": "Gujarat Mineral",
    "CUB.NS": "City Union Bank",
    "SYRMA.NS": "Syrma SGS",
    "CHOICEIN.NS": "Choice International",
    "ICICIPRULI.NS": "ICICI Pru Life",
    "KPIL.NS": "Kalpataru Projects",
    "RASHIPERI.NS": "Rashi Peripherals",
}


# ── Calculation helpers ───────────────────────────────────────

def _ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def _zscore_norm(series, window):
    min_obs   = max(int(window * 0.7), 30)
    roll_mean = series.rolling(window=window, min_periods=min_obs).mean()
    roll_std  = series.rolling(window=window, min_periods=min_obs).std()
    roll_std  = roll_std.replace(0, np.nan)
    z = (series - roll_mean) / roll_std
    return 100 + z * 10

def _calc_rs(stock_close, bench_close, ema_period):
    combined = pd.DataFrame({"stock": stock_close, "bench": bench_close}).dropna()
    if len(combined) < (ZSCORE_RATIO_WIN + ZSCORE_MOM_WIN):
        return None, None
    rs_raw      = (combined["stock"] / combined["bench"]) * 100
    rs_ema      = _ema(rs_raw, ema_period)
    rs_ratio    = _zscore_norm(rs_ema, ZSCORE_RATIO_WIN)
    rs_roc      = rs_ratio - rs_ratio.shift(ema_period)
    rs_momentum = _zscore_norm(rs_roc, ZSCORE_MOM_WIN)
    r = rs_ratio.dropna()
    m = rs_momentum.dropna()
    return (float(r.iloc[-1]) if len(r) > 0 else None,
            float(m.iloc[-1]) if len(m) > 0 else None)

def _quadrant(r, m):
    if r is None or m is None:
        return "N/A"
    if r >= 100 and m >= 100: return "Leading"
    if r >= 100 and m <  100: return "Weakening"
    if r <  100 and m <  100: return "Lagging"
    return "Improving"


# ── Server interface functions ────────────────────────────────

def check_conditions():
    """
    Download benchmark (Nifty 500) once.
    Returns: (ok, reason, extra_data_dict)
    extra_data is passed to screen() as **kwargs by server.py
    """
    import yfinance as yf
    try:
        bench = yf.download(BENCHMARK, period=DATA_PERIOD, progress=False, auto_adjust=True)
        if isinstance(bench.columns, pd.MultiIndex):
            bench.columns = bench.columns.get_level_values(0)
        bench_close = bench["Close"].dropna()
        if len(bench_close) < ZSCORE_RATIO_WIN + ZSCORE_MOM_WIN:
            return False, f"Insufficient benchmark data ({len(bench_close)} days)", {}
        print(f"  Benchmark loaded: {len(bench_close)} days")
        return True, f"Benchmark OK ({len(bench_close)} days)", {"bench_close": bench_close}
    except Exception as e:
        return False, f"Benchmark download failed: {e}", {}


def screen(df, periods=10, bench_close=None, **kwargs):
    """
    Screen one stock against Nifty 500 benchmark.

    Args:
        df          : stock OHLCV DataFrame (from Yahoo Finance)
        periods     : not used (EMA periods are fixed at 10/21)
        bench_close : Nifty 500 close series (from check_conditions)

    Returns:
        (signal, reasons, extras_dict)
        extras_dict contains RS values for dynamic table columns
    """
    if bench_close is None:
        return "SKIP", ["No benchmark data"], {}

    # Get stock close
    if "Close" in df.columns:
        stock_close = df["Close"].dropna()
    elif "close" in df.columns:
        stock_close = df["close"].dropna()
    else:
        return "SKIP", ["No close data"], {}

    # Calculate RS for both EMA periods
    r10, m10 = _calc_rs(stock_close, bench_close, EMA_SHORT)
    q10 = _quadrant(r10, m10)

    r21, m21 = _calc_rs(stock_close, bench_close, EMA_LONG)
    q21 = _quadrant(r21, m21)

    # Signal
    if q10 == "Improving" or q21 == "Improving":
        signal = "STRONG"
    elif q10 == "Leading" or q21 == "Leading":
        signal = "WATCH"
    else:
        signal = "SKIP"

    # Reasons
    reasons = []
    if r10 is not None:
        reasons.append(f"10 EMA: Ratio {r10:.1f}, Mom {m10:.1f} → {q10} {'✓' if q10 in ('Leading','Improving') else '✗'}")
    if r21 is not None:
        reasons.append(f"21 EMA: Ratio {r21:.1f}, Mom {m21:.1f} → {q21} {'✓' if q21 in ('Leading','Improving') else '✗'}")

    # Extras — these become dynamic table columns in the dashboard
    extras = {
        "RS_Ratio_10":  round(r10, 2) if r10 is not None else None,
        "RS_Mom_10":    round(m10, 2) if m10 is not None else None,
        "Quadrant_10":  q10,
        "RS_Ratio_21":  round(r21, 2) if r21 is not None else None,
        "RS_Mom_21":    round(m21, 2) if m21 is not None else None,
        "Quadrant_21":  q21,
    }

    return signal, reasons, extras
