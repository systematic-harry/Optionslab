"""
NSE Index Constituent Downloader v2
- Directly hits CSV URLs using Playwright session (no subpage navigation)
- Exports all successful downloads to a single Excel file
"""

import asyncio
import os
import json
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright

OUTPUT_DIR = Path("D:/optionlab/data/nse_indices")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# (index_name, csv_filename) — filename goes into:
# https://www.niftyindices.com/IndexConstituent/<filename>
INDICES = [
    # Broad Based
    ("Nifty 50",                         "ind_nifty50list.csv"),
    ("Nifty Next 50",                    "ind_niftynext50list.csv"),
    ("Nifty 100",                        "ind_nifty100list.csv"),
    ("Nifty 200",                        "ind_nifty200list.csv"),
    ("Nifty Total Market",               "ind_niftytotalmarket_list.csv"),
    ("Nifty 500",                        "ind_nifty500list.csv"),
    ("Nifty500 Multicap 50:25:25",       "ind_nifty500Multicap502525_list.csv"),
    ("Nifty Midcap 150",                 "ind_niftymidcap150list.csv"),
    ("Nifty Midcap 50",                  "ind_niftymidcap50list.csv"),
    ("Nifty Midcap Select",              "ind_niftymidcapselect_list.csv"),
    ("Nifty Midcap 100",                 "ind_niftymidcap100list.csv"),
    ("Nifty Smallcap 500",               "ind_NiftySmallcap500_list.csv"),
    ("Nifty Smallcap 250",               "ind_niftysmallcap250list.csv"),
    ("Nifty Smallcap 50",                "ind_niftysmallcap50list.csv"),
    ("Nifty Smallcap 100",               "ind_niftysmallcap100list.csv"),
    ("Nifty Microcap 250",               "ind_niftymicrocap250_list.csv"),
    ("Nifty LargeMidcap 250",            "ind_niftylargemidcap250list.csv"),
    ("Nifty MidSmallcap 400",            "ind_niftymidsmallcap400list.csv"),
    # Sectoral
    ("Nifty Auto",                       "ind_niftyautolist.csv"),
    ("Nifty Bank",                       "ind_niftybanklist.csv"),
    ("Nifty Financial Services",         "ind_niftyfinancelist.csv"),
    ("Nifty FMCG",                       "ind_niftyfmcglist.csv"),
    ("Nifty Healthcare",                 "ind_niftyhealthcarelist.csv"),
    ("Nifty IT",                         "ind_niftyitlist.csv"),
    ("Nifty Media",                      "ind_niftymedialist.csv"),
    ("Nifty Metal",                      "ind_niftymetallist.csv"),
    ("Nifty Pharma",                     "ind_niftypharmalist.csv"),
    ("Nifty Private Bank",               "ind_nifty_privatebanklist.csv"),
    ("Nifty PSU Bank",                   "ind_niftypsubanklist.csv"),
    ("Nifty Realty",                     "ind_niftyrealtylist.csv"),
    ("Nifty Consumer Durables",          "ind_niftyconsumerdurableslist.csv"),
    ("Nifty Oil and Gas",                "ind_niftyoilgaslist.csv"),
    # Thematic
    ("Nifty Commodities",                "ind_niftycommoditieslist.csv"),
    ("Nifty CPSE",                       "ind_niftycpselist.csv"),
    ("Nifty Energy",                     "ind_niftyenergylist.csv"),
    ("Nifty India Consumption",          "ind_niftyconsumptionlist.csv"),
    ("Nifty India Defence",              "ind_niftyindiadefence_list.csv"),
    ("Nifty Infrastructure",             "ind_niftyinfralist.csv"),
    ("Nifty MNC",                        "ind_niftymnclist.csv"),
    ("Nifty PSE",                        "ind_niftypselist.csv"),
    ("Nifty Services Sector",            "ind_niftyservicelist.csv"),
    ("Nifty Capital Markets",            "ind_niftyCapitalMarkets_list.csv"),
    ("Nifty Housing",                    "ind_niftyhousing_list.csv"),
    ("Nifty India Manufacturing",        "ind_niftyindiamanufacturing_list.csv"),
    ("Nifty India Digital",              "ind_niftyindiadigital_list.csv"),
    # Strategy
    ("Nifty High Beta 50",               "nifty_High_Beta50_Index.csv"),
    ("Nifty Low Volatility 50",          "nifty_Low_Volatility50_Index.csv"),
    ("Nifty Alpha 50",                   "ind_nifty_Alpha_Index.csv"),
    ("Nifty100 Quality 30",              "ind_nifty100quality30list.csv"),
    ("Nifty50 Equal Weight",             "ind_Nifty50EqualWeight.csv"),
    ("Nifty200 Momentum 30",             "ind_nifty200Momentum30_list.csv"),
    ("Nifty Dividend Opportunities 50",  "ind_niftydivopp50list.csv"),
    ("Nifty500 Momentum 50",             "ind_nifty500Momentum50_list.csv"),
    ("Nifty Midcap150 Momentum 50",      "ind_niftymidcap150momentum50_list.csv"),
    ("Nifty Alpha Low Volatility 30",    "ind_nifty_alpha_lowvol30list.csv"),
    ("Nifty200 Alpha 30",                "ind_nifty200alpha30_list.csv"),
    ("Nifty500 Quality 50",              "ind_nifty500Quality50_list.csv"),
    ("Nifty500 Low Volatility 50",       "ind_nifty500LowVolatility50_list.csv"),
    ("Nifty500 Value 50",                "ind_nifty500Value50_list.csv"),
    ("Nifty500 Equal Weight",            "ind_nifty500EqualWeight_list.csv"),
]

BASE_URL = "https://www.niftyindices.com/IndexConstituent/"


async def main():
    print(f"Downloading {len(INDICES)} indices...\n")
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--ignore-certificate-errors"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            ignore_https_errors=True,
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        # Warm up session
        print("Warming up session...")
        await page.goto("https://www.niftyindices.com", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(4000)
        print("Session ready. Starting downloads...\n")

        for index_name, csv_file in INDICES:
            csv_url = BASE_URL + csv_file
            result = {"index": index_name, "csv_url": csv_url, "status": "failed", "rows": 0}
            try:
                response = await page.request.get(csv_url, timeout=20000)
                if response.ok:
                    content = await response.text()
                    # Validate it's actually a CSV and not an HTML error page
                    stripped = content.strip()
                    if stripped.startswith("<") or "<!DOCTYPE" in stripped[:100]:
                        result["status"] = "html_404"
                        print(f"  ✗ {index_name}: got HTML instead of CSV (wrong filename)")
                    elif len(stripped.split("\n")) > 1:
                        safe_name = index_name.replace(" ", "_").replace(":", "-").replace("/", "-").replace("&", "and")
                        filepath = OUTPUT_DIR / f"{safe_name}.csv"
                        filepath.write_text(content, encoding="utf-8")
                        result["status"] = "success"
                        result["rows"] = len(stripped.split("\n")) - 1
                        result["file"] = str(filepath)
                        print(f"  ✓ {index_name}: {result['rows']} stocks")
                    else:
                        result["status"] = "empty"
                        print(f"  ✗ {index_name}: empty response")
                else:
                    result["status"] = f"http_{response.status}"
                    print(f"  ✗ {index_name}: HTTP {response.status}")
            except Exception as e:
                result["status"] = f"error: {str(e)[:80]}"
                print(f"  ✗ {index_name}: {e}")

            results.append(result)
            await asyncio.sleep(0.5)  # polite delay

        await browser.close()

    # Summary
    success = [r for r in results if r["status"] == "success"]
    failed  = [r for r in results if r["status"] != "success"]
    print(f"\n{'='*50}")
    print(f"✓ Downloaded: {len(success)} indices")
    print(f"✗ Failed:     {len(failed)} indices")

    if failed:
        print("\nFailed:")
        for r in failed:
            print(f"  - {r['index']}: {r['status']}")

    # Save summary
    (OUTPUT_DIR / "download_summary_v2.json").write_text(json.dumps(results, indent=2))

    # Build Excel — one sheet per index
    if success:
        print(f"\nBuilding Excel file...")
        excel_path = OUTPUT_DIR / "NSE_Index_Constituents.xlsx"

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Summary sheet
            summary_df = pd.DataFrame([
                {"Index": r["index"], "Stocks": r["rows"], "Status": r["status"], "CSV URL": r["csv_url"]}
                for r in results
            ])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # One sheet per index
            for r in success:
                try:
                    df = pd.read_csv(r["file"])
                    sheet_name = r["index"][:31]  # Excel sheet name limit
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    print(f"  Excel error for {r['index']}: {e}")

        print(f"✓ Excel saved: {excel_path}")

if __name__ == "__main__":
    asyncio.run(main())
