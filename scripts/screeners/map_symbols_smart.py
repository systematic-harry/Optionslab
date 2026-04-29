"""
Smart ticker mapper - searches Yahoo Finance for each company name
Automatically finds NSE/BSE tickers without manual mapping
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
import time

# ============================================================
# Core Logic
# ============================================================

def get_yfinance_tickers():
    """
    Returns common NSE/BSE mappings built into yfinance
    This is used as a fallback lookup
    """
    # This is a minimal set - yfinance has built-in access to NSE/BSE
    return {}  # yfinance doesn't expose a public ticker list, we'll use search


def search_ticker_on_yahoo(company_name):
    """
    Try to find ticker on Yahoo Finance for a company name
    Returns (ticker, found) tuple
    """
    
    # Strategy 1: Try company name directly + .NS
    attempts = [
        f"{company_name}.NS",  # NSE default
        f"{company_name}.BO",  # BSE alternative
    ]
    
    # Strategy 2: Try common abbreviations
    words = company_name.split()
    if len(words) > 1:
        # Try first word + last word (e.g., "Adani Enterprises" → "ADANIENT.NS")
        abbrev1 = (words[0][:3] + words[-1][:3]).upper()
        attempts.extend([
            f"{abbrev1}.NS",
            f"{abbrev1}.BO",
        ])
        
        # Try first 3 chars of company name
        abbrev2 = company_name[:3].upper()
        attempts.extend([
            f"{abbrev2}.NS",
            f"{abbrev2}.BO",
        ])
    
    # Clean attempts - remove spaces, special chars
    attempts = [a.replace(" ", "") for a in attempts]
    attempts = list(dict.fromkeys(attempts))  # Remove duplicates
    
    # Try each attempt
    for ticker in attempts:
        try:
            # Try downloading 1 day of data - fast check
            data = yf.download(ticker, period="1d", progress=False, quiet=True)
            if data is not None and not data.empty:
                return ticker, True
        except:
            pass
        time.sleep(0.1)  # Rate limit
    
    return None, False


def clean_company_name(name):
    """Clean company name for better matching"""
    # Remove common suffixes
    name = name.strip()
    name = name.replace(" Limited", "").replace(" Ltd", "").replace(" Ltd.", "")
    name = name.replace(" Private", "").replace(" Pvt", "")
    name = name.replace(" Industries", "").replace(" Ind.", "")
    name = name.replace(" & ", " ")
    return name.strip()


# ============================================================
# Main Script
# ============================================================

if __name__ == "__main__":
    input_csv = r"D:\optionlab\data\rs_ratio_stocks.csv"
    output_excel = r"D:\optionlab\data\rs_ratio_stocks.xlsx"
    
    print("=" * 70)
    print("  RS RATIO STOCKS - TICKER MAPPER")
    print("=" * 70)
    
    # Read CSV
    print(f"\n✓ Reading {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"  Found {len(df)} companies")
    
    # Get company name column
    company_col = "Stock Name"
    if company_col not in df.columns:
        print(f"\n✗ ERROR: Column '{company_col}' not found!")
        print(f"  Available columns: {df.columns.tolist()}")
        exit(1)
    
    # Map tickers
    print(f"\n→ Searching Yahoo Finance for tickers...")
    print("  (This may take a minute...)\n")
    
    results = []
    for idx, row in df.iterrows():
        company_name = str(row[company_col]).strip()
        
        # Clean name for better matching
        clean_name = clean_company_name(company_name)
        
        # Search
        ticker, found = search_ticker_on_yahoo(clean_name)
        
        status = "✓" if found else "✗"
        print(f"  [{idx+1:3d}/{len(df)}] {status} {company_name[:40]:40s} → {ticker if ticker else 'NOT FOUND':15s}")
        
        results.append({
            "Stock Name": company_name,
            "Ticker": ticker,
            "Found": found
        })
        
        # Don't hammer Yahoo Finance
        time.sleep(0.2)
    
    # Create result dataframe
    result_df = pd.DataFrame(results)
    
    # Summary
    found_count = result_df["Found"].sum()
    not_found = result_df[~result_df["Found"]]
    
    print("\n" + "=" * 70)
    print(f"  SUMMARY")
    print("=" * 70)
    print(f"  Total companies: {len(result_df)}")
    print(f"  ✓ Found:        {found_count}")
    print(f"  ✗ Not found:    {len(not_found)}")
    
    if len(not_found) > 0:
        print(f"\n  Companies not found (may need manual mapping):")
        for idx, row in not_found.iterrows():
            print(f"    - {row['Stock Name']}")
    
    # Save to Excel
    print(f"\n→ Saving to {output_excel}...")
    
    # Only keep found ones for now (or save all with None for not found)
    # For screener to work, we only need the found ones
    output_df = result_df[result_df["Found"]][["Stock Name", "Ticker"]].reset_index(drop=True)
    
    output_df.to_excel(output_excel, index=False)
    
    print(f"  ✓ Saved {len(output_df)} tickers to Excel")
    print(f"\n  Ready to use in screener: {output_excel}")
    print("\n" + "=" * 70)

