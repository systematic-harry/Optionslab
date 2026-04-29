"""
NSE Index CSV URL Finder
Visits each failed index page and extracts the actual Index Constituent CSV URL
"""

import asyncio
import json
from playwright.async_api import async_playwright

# Failed indices with their page URLs
FAILED_INDICES = [
    ("Nifty Total Market",               "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-total-market"),
    ("Nifty500 Multicap 50:25:25",       "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty500-multicap-50-25-25-index"),
    ("Nifty Midcap Select",              "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-midcap-select-index"),
    ("Nifty Smallcap 500",               "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-smallcap-500"),
    ("Nifty Cement",                     "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-cement-and-construction-index"),
    ("Nifty Chemicals",                  "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-chemicals-index"),
    ("Nifty Private Bank",               "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-private-bank"),
    ("Nifty Consumer Durables",          "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-consumer-durables-index"),
    ("Nifty India Consumption",          "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-consumption"),
    ("Nifty India Defence",              "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-defence"),
    ("Nifty Services Sector",            "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-services-sector"),
    ("Nifty Capital Markets",            "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-capital-markets"),
    ("Nifty Housing",                    "https://www.niftyindices.com/indices/equity/thematic-indices/nifty--housing"),
    ("Nifty India Manufacturing",        "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-manufacturing"),
    ("Nifty EV & New Age Auto",          "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-ev-and-new-age-automotive-index"),
    ("Nifty India Digital",              "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-digital"),
    ("Nifty Alpha 50",                   "https://www.niftyindices.com/indices/equity/strategy-indices/nifty-alpha-50"),
    ("Nifty50 Equal Weight",             "https://www.niftyindices.com/indices/equity/strategy-indices/nifty50-equal-weight"),
    ("Nifty100 Equal Weight",            "https://www.niftyindices.com/indices/equity/strategy-indices/nifty-100-equal-weight"),
    ("Nifty200 Momentum 30",             "https://www.niftyindices.com/indices/equity/strategy-indices/nifty200-momentum-30-index"),
    ("Nifty Dividend Opportunities 50",  "https://www.niftyindices.com/indices/equity/strategy-indices/nifty-dividend-opportunities-50"),
    ("Nifty500 Momentum 50",             "https://www.niftyindices.com/indices/equity/strategy-indices/nifty500--momentum--50"),
    ("Nifty Midcap150 Momentum 50",      "https://www.niftyindices.com/indices/equity/strategy-indices/nifty-midcap150-momentum-50"),
    ("Nifty Alpha Low Volatility 30",    "https://www.niftyindices.com/indices/equity/strategy-indices/nifty-alpha-low-volatility-30"),
    ("Nifty200 Alpha 30",                "https://www.niftyindices.com/indices/equity/strategy-indices/nifty200-alpha-30"),
    ("Nifty500 Quality 50",              "https://www.niftyindices.com/indices/equity/strategy-indices/nifty500-quality-50"),
    ("Nifty500 Low Volatility 50",       "https://www.niftyindices.com/indices/equity/strategy-indices/nifty500-low-volatility-50"),
    ("Nifty500 Value 50",                "https://www.niftyindices.com/indices/equity/strategy-indices/nifty500-value-50"),
    ("Nifty500 Equal Weight",            "https://www.niftyindices.com/indices/equity/strategy-indices/nifty500-equal-weight"),
]


async def find_csv_url(page, index_name, page_url):
    try:
        await page.goto(page_url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        # Find any link containing IndexConstituent or .csv
        links = await page.evaluate("""
            () => Array.from(document.querySelectorAll('a[href]'))
                       .map(a => a.href)
                       .filter(h => h.includes('IndexConstituent') || 
                                    (h.toLowerCase().includes('.csv')))
        """)

        if links:
            return {"index": index_name, "csv_url": links[0], "status": "found"}
        else:
            # Try clicking the "Index Constituent" text link
            el = await page.query_selector("text=Index Constituent")
            if el:
                href = await el.get_attribute("href")
                return {"index": index_name, "csv_url": href, "status": "found"}
            return {"index": index_name, "csv_url": None, "status": "not_found"}

    except Exception as e:
        return {"index": index_name, "csv_url": None, "status": f"error: {str(e)[:80]}"}


async def main():
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--ignore-certificate-errors"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        # Warm up
        print("Warming up session...")
        await page.goto("https://www.niftyindices.com", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        print("Done. Scanning pages...\n")

        for index_name, page_url in FAILED_INDICES:
            result = await find_csv_url(page, index_name, page_url)
            results.append(result)

            if result["csv_url"]:
                filename = result["csv_url"].split("/")[-1]
                print(f'  ✓ {index_name}: "{filename}"')
            else:
                print(f'  ✗ {index_name}: {result["status"]}')

            await asyncio.sleep(1)

        await browser.close()

    # Save results
    output_path = "D:/optionlab/data/nse_indices/correct_csv_urls.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {output_path}")
    print("\nCopy these into download_nse_indices_v2.py:")
    print("-" * 60)
    for r in results:
        if r["csv_url"]:
            filename = r["csv_url"].split("/")[-1]
            print(f'    ("{r["index"]}", "{filename}"),')
        else:
            print(f'    # ✗ {r["index"]}: NOT FOUND')


if __name__ == "__main__":
    asyncio.run(main())
