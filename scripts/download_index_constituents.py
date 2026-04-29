# ============================================================
#  download_index_constituents.py
#  Downloads constituents for 20 indexes from niftyindices.com
#  and BSE India, deduplicates, and saves master stock list.
#
#  Run in Spyder:  %run download_index_constituents.py
#  Output:         master_stock_list.csv  (in same folder)
# ============================================================

import requests
import pandas as pd
import io
import time
import os

# ── Configuration ─────────────────────────────────────────────
OUTPUT_DIR = r"D:\optionlab\scripts"   # Change if needed
MASTER_FILE = os.path.join(OUTPUT_DIR, "master_stock_list.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.niftyindices.com/indices/equity/sectoral-indices",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Nifty Index CSV URLs (niftyindices.com) ───────────────────
# Pattern: https://www.niftyindices.com/IndexConstituent/ind_{name}list.csv
NIFTY_INDEXES = {
    "NIFTYAUTO":            "ind_niftyautolist",
    "NI200MOM30":           "ind_nifty200Momentum30list",
    "NIFTYALPHA50":         "ind_niftyalpha50list",
    "NIFTY500MOMENTM50":    "ind_nifty500Momentum50list",
    "NFMC150M50":           "ind_niftymidcap150Momentum50list",
    "NFINSERV25":           "ind_niftyfinancialservices2550list",
    "NIFTYREALTY":          "ind_niftyrealtylist",
    "FINNIFTY":             "ind_niftyfinancialserviceslist",
    "NIFTYCAPITALMKT":      "ind_niftycapitalmarketslist",
    "NIFTYHIGHBETA50":      "ind_niftyhighbeta50list",
    "NIFTYINDDEFENCE":      "ind_niftyindiadefencelist",
    "NIFTYENERGY":          "ind_niftyenergylist",
    "NIFTYMIDCAPLIQUID15":  "ind_niftymidcapliq15list",
}

BASE_URL = "https://www.niftyindices.com/IndexConstituent/"


# ── BSE Index constituent URLs ────────────────────────────────
# BSE uses a different API. We'll try the BSE website CSV download.
# If BSE fails, we hardcode the known constituents.
BSE_INDEXES = {
    "BSEAUTO":         "S&P BSE Auto",
    "BSEIPO":          "S&P BSE IPO",
    "BSECG":           "S&P BSE Capital Goods",
    "BSECD":           "S&P BSE Consumer Durables",
    "BSEINDUSTRIALS":  "S&P BSE Industrials",
    "BSEREALTY":       "S&P BSE Realty",
    "BSESMEIPO":       "S&P BSE SME IPO",
}

BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/GetIndexConsti/w?code="
BSE_INDEX_CODES = {
    "BSEAUTO":         "SNXT50",   # BSE Auto
    "BSEIPO":          "BSEIPO",
    "BSECG":           "BSECG",
    "BSECD":           "BSECD",
    "BSEINDUSTRIALS":  "BSEIND",
    "BSEREALTY":       "BSERE",
    "BSESMEIPO":       "BSESMEIPO",
}


def download_nifty_csv(index_code, csv_name):
    """Download constituent CSV from niftyindices.com"""
    url = f"{BASE_URL}{csv_name}.csv"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and len(r.text) > 50:
            # Try to parse as CSV
            df = pd.read_csv(io.StringIO(r.text))
            # Find the symbol column (could be 'Symbol', 'symbol', etc.)
            sym_col = None
            for col in df.columns:
                if 'symbol' in col.lower():
                    sym_col = col
                    break
            if sym_col is None:
                # Sometimes first column is company name, try others
                for col in df.columns:
                    if 'industry' not in col.lower() and 'company' not in col.lower():
                        # Check if values look like symbols (uppercase, no spaces)
                        sample = str(df[col].iloc[0]).strip()
                        if sample.isupper() and ' ' not in sample:
                            sym_col = col
                            break

            if sym_col:
                symbols = df[sym_col].dropna().str.strip().tolist()
                symbols = [s for s in symbols if s and isinstance(s, str)]
                print(f"  ✅ {index_code}: {len(symbols)} stocks from niftyindices.com")
                return symbols
            else:
                print(f"  ⚠️  {index_code}: CSV downloaded but no symbol column found")
                print(f"      Columns: {list(df.columns)}")
                print(f"      First row: {df.iloc[0].tolist()}")
                return []
        else:
            print(f"  ❌ {index_code}: HTTP {r.status_code}")
            return []
    except Exception as e:
        print(f"  ❌ {index_code}: {e}")
        return []


def download_bse_constituents(index_code, index_name):
    """Try to download BSE index constituents via BSE API"""
    # BSE API endpoint for index constituents
    try:
        bse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.bseindia.com/",
            "Accept": "application/json",
        }
        # Try scraping from BSE website
        url = f"https://api.bseindia.com/BseIndiaAPI/api/GetSensexData/w?code={index_code}"
        r = requests.get(url, headers=bse_headers, timeout=15)
        if r.status_code == 200 and r.text:
            import json
            data = json.loads(r.text)
            if isinstance(data, list):
                symbols = []
                for item in data:
                    if 'scripname' in item or 'SCRIPNAME' in item:
                        sym = item.get('scripid', item.get('SCRIPID', ''))
                        if sym:
                            symbols.append(sym.strip())
                if symbols:
                    print(f"  ✅ {index_code}: {len(symbols)} stocks from BSE API")
                    return symbols
        print(f"  ⚠️  {index_code}: BSE API didn't return data — will need manual entry")
        return []
    except Exception as e:
        print(f"  ⚠️  {index_code}: BSE API failed ({e}) — will need manual entry")
        return []


def main():
    print("=" * 60)
    print("  INDEX CONSTITUENT DOWNLOADER")
    print("  20 Indexes → Deduplicated Master List")
    print("=" * 60)

    all_stocks = {}  # symbol → set of indexes it belongs to

    # ── Download Nifty indexes ────────────────────────────────
    print("\n📥 Downloading Nifty indexes from niftyindices.com...\n")
    for code, csv_name in NIFTY_INDEXES.items():
        symbols = download_nifty_csv(code, csv_name)
        for s in symbols:
            if s not in all_stocks:
                all_stocks[s] = set()
            all_stocks[s].add(code)
        time.sleep(0.5)  # Be polite

    # ── Download BSE indexes ──────────────────────────────────
    print("\n📥 Attempting BSE indexes...\n")
    for code, name in BSE_INDEXES.items():
        symbols = download_bse_constituents(code, name)
        for s in symbols:
            if s not in all_stocks:
                all_stocks[s] = set()
            all_stocks[s].add(code)
        time.sleep(0.5)

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  TOTAL UNIQUE STOCKS: {len(all_stocks)}")
    print("=" * 60)

    # Build DataFrame
    rows = []
    for symbol in sorted(all_stocks.keys()):
        indexes = sorted(all_stocks[symbol])
        rows.append({
            "Symbol": symbol,
            "Yahoo_Ticker": f"{symbol}.NS",
            "Index_Count": len(indexes),
            "Indexes": ", ".join(indexes),
        })

    df = pd.DataFrame(rows)
    df.to_csv(MASTER_FILE, index=False)
    print(f"\n  Saved to: {MASTER_FILE}")
    print(f"  Total unique symbols: {len(df)}")
    print(f"\n  Top stocks (in most indexes):")
    top = df.nlargest(15, "Index_Count")
    for _, row in top.iterrows():
        print(f"    {row['Symbol']:20s} → {row['Index_Count']} indexes")

    # ── Show which indexes failed ─────────────────────────────
    all_codes = set(NIFTY_INDEXES.keys()) | set(BSE_INDEXES.keys())
    found_codes = set()
    for indexes in all_stocks.values():
        found_codes.update(indexes)
    missing = all_codes - found_codes
    if missing:
        print(f"\n  ⚠️  Indexes with NO stocks downloaded:")
        for m in sorted(missing):
            print(f"      {m}")
        print(f"\n  For BSE indexes, you may need to manually add stocks.")
        print(f"  Go to bseindia.com → Markets → Indices → select index → download CSV")
        print(f"  Then add symbols to {MASTER_FILE}")

    print("\n✅ Done!")
    return df


if __name__ == "__main__":
    df = main()
