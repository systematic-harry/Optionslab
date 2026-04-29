"""
BSE Index Constituent Downloader
- Brute forces codes 1-300 to find all valid indices
- Downloads CSVs and builds Excel
"""

import requests
import pandas as pd
import json
import time
from pathlib import Path

OUTPUT_DIR = Path("D:/optionlab/data/bse_indices")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.bseindices.com/AsiaIndexAPI/api/Codewise_IndicesDownload/w?code="
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def try_code(code):
    try:
        r = requests.get(BASE_URL + str(code), headers=HEADERS, timeout=10)
        content = r.text.strip()

        # Validate it's a real CSV
        if not content or content.startswith("<") or content.startswith("{"):
            return None

        lines = content.split("\n")
        if len(lines) < 2:
            return None

        # Must have a proper header row
        header = lines[0].strip()
        if "Constituents" not in header and "Symbol" not in header:
            return None

        return {"code": code, "rows": len(lines) - 1, "header": header, "content": content}

    except Exception as e:
        print(f"  code={code}: error - {str(e)[:60]}")
        return None


def main():
    print("Scanning BSE index codes 1-300...\n")
    valid = []

    for code in range(1, 301):
        result = try_code(code)
        if result:
            # Try to extract index name from first data row sector or header
            print(f"  ✓ code={code}: {result['rows']} rows")
            valid.append(result)
        else:
            print(f"  - code={code}: invalid")
        time.sleep(0.2)  # polite delay

    print(f"\nFound {len(valid)} valid indices!\n")

    # Save individual CSVs
    saved = []
    for r in valid:
        filepath = OUTPUT_DIR / f"bse_index_{r['code']}.csv"
        filepath.write_text(r["content"], encoding="utf-8")

        # Try to get index name from content
        lines = r["content"].split("\n")
        # Get sector from first data row (3rd column if exists)
        try:
            first_data = lines[1].split(",")
            sector = first_data[2].strip() if len(first_data) > 2 else f"code_{r['code']}"
        except:
            sector = f"code_{r['code']}"

        saved.append({
            "code": r["code"],
            "rows": r["rows"],
            "sector_hint": sector,
            "file": str(filepath)
        })

    # Save summary
    summary_path = OUTPUT_DIR / "bse_download_summary.json"
    summary_path.write_text(json.dumps(saved, indent=2))
    print(f"Summary saved: {summary_path}")

    # Build Excel
    print("Building Excel...")
    excel_path = OUTPUT_DIR / "BSE_Index_Constituents.xlsx"

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        # Summary sheet
        summary_df = pd.DataFrame([
            {"Code": r["code"], "Stocks": r["rows"], "Sector Hint": r["sector_hint"]}
            for r in saved
        ])
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # One sheet per index
        for r in saved:
            try:
                df = pd.read_csv(r["file"])
                sheet_name = f"code_{r['code']}"
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            except Exception as e:
                print(f"  Excel error for code {r['code']}: {e}")

    print(f"✓ Excel saved: {excel_path}")
    print("\nDone! Check the Summary sheet to identify each index by its code.")


if __name__ == "__main__":
    main()
