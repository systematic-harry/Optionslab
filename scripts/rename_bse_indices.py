"""
BSE Index Renamer
Reads all downloaded BSE index CSVs, identifies the index name from the 
sector column, and rebuilds Excel with proper sheet names.
"""

import pandas as pd
import json
from pathlib import Path

INPUT_DIR  = Path("D:/optionlab/data/bse_indices")
EXCEL_PATH = INPUT_DIR / "BSE_Index_Constituents.xlsx"

# Manual code → index name mapping based on known BSE indices
# Add/correct as needed after reviewing
CODE_NAMES = {
    1:   "BSE Sensex",
    2:   "BSE Sensex 50",
    3:   "BSE Sensex Next 50",
    4:   "BSE 100",
    5:   "BSE 200",
    6:   "BSE 500",
    7:   "BSE Midcap",
    8:   "BSE Smallcap",
    9:   "BSE Large Cap",
    10:  "BSE Mid Cap Select",
    11:  "BSE Allcap",
    12:  "BSE Bluechip",
    13:  "BSE Sensex 100",
    14:  "BSE Midcap Select",
    15:  "BSE SME IPO",
    16:  "BSE MidSmallcap",
    17:  "BSE Largecap",
    18:  "BSE IPO",
    19:  "BSE Microcap",
    20:  "BSE Greenex",
    21:  "BSE Carbonex",
    22:  "BSE PSU",
    23:  "BSE India Infrastructure",
    24:  "BSE CPSE",
    25:  "BSE Bharat 22",
    # Sectoral
    50:  "BSE Auto",
    51:  "BSE Bankex",
    52:  "BSE Capital Goods",
    53:  "BSE Consumer Discretionary",
    54:  "BSE Consumer Durables",
    55:  "BSE Consumer Staples",
    56:  "BSE Fast Moving Consumer Goods",
    57:  "BSE Finance",
    58:  "BSE Healthcare",
    59:  "BSE Industrials",
    60:  "BSE Information Technology",
    61:  "BSE Metal",
    62:  "BSE Oil & Gas",
    63:  "BSE Power",
    64:  "BSE Realty",
    65:  "BSE Telecom",
    66:  "BSE Utilities",
    67:  "BSE Materials",
    68:  "BSE Energy",
    69:  "BSE Communication Services",
    # Thematic
    88:  "BSE India Manufacturing",
    89:  "BSE Consumer Discretionary",
    90:  "BSE Energy",
    91:  "BSE Finance",
    92:  "BSE Healthcare",
    93:  "BSE Industrials",
    94:  "BSE Information Technology",
    95:  "BSE Materials",
    96:  "BSE Utilities",
    97:  "BSE FMCG",
    98:  "BSE Auto",
    99:  "BSE Metal",
    100: "BSE Realty",
    101: "BSE Bankex",
    102: "BSE Capital Goods",
    103: "BSE Consumer Durables",
}


def get_name_from_csv(filepath, code):
    """Try to determine index name from CSV content."""
    try:
        df = pd.read_csv(filepath)
        # Get sector from 3rd column if it exists
        if df.shape[1] >= 3:
            sector = df.iloc[:, 2].dropna().mode()
            if len(sector) > 0:
                return f"BSE {sector.iloc[0].strip()}"
    except:
        pass
    return CODE_NAMES.get(code, f"BSE Index {code}")


def main():
    # Find all downloaded CSVs
    csv_files = sorted(INPUT_DIR.glob("bse_index_*.csv"))
    print(f"Found {len(csv_files)} CSV files\n")

    index_data = []
    for f in csv_files:
        code = int(f.stem.replace("bse_index_", ""))
        name = CODE_NAMES.get(code) or get_name_from_csv(f, code)
        try:
            df = pd.read_csv(f)
            rows = len(df)
        except:
            df = pd.DataFrame()
            rows = 0
        index_data.append({"code": code, "name": name, "rows": rows, "df": df, "file": f})
        print(f"  code={code:3d}: {rows:4d} stocks → {name}")

    # Build Excel with proper names
    print(f"\nBuilding Excel: {EXCEL_PATH}")
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as writer:
        # Summary sheet
        summary_df = pd.DataFrame([
            {"Code": d["code"], "Index Name": d["name"], "Stocks": d["rows"]}
            for d in index_data
        ])
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # One sheet per index — use name (truncated to 31 chars for Excel)
        used_names = {}
        for d in index_data:
            sheet_name = d["name"][:31]
            # Handle duplicate sheet names
            if sheet_name in used_names:
                used_names[sheet_name] += 1
                sheet_name = f"{sheet_name[:28]}_{used_names[sheet_name]}"
            else:
                used_names[sheet_name] = 1

            if not d["df"].empty:
                d["df"].to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✓ Excel saved: {EXCEL_PATH}")
    print("\nCheck the Summary sheet — update CODE_NAMES dict for any wrong names!")


if __name__ == "__main__":
    main()
