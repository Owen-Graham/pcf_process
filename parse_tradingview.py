# parse_tradingview.py

import asyncio
import csv
import os
import re
from datetime import datetime, timedelta, timezone

from playwright.async_api import async_playwright

# List of the first two VIX futures in the cycle: M5 (June 2025) and N5 (July 2025)
CONTRACTS = ["VXM2025", "VXN2025"]


async def scrape_contract(page, contract_code):
    """
    Given a Playwright Page and a contract code (e.g. "VXM2025"),
    navigate to the TradingView URL, wait for the live quote, and return a dict:
      {
        "future_descriptor": "CBOE:VXM2025",
        "price":             "19.150",
        "date":              "2025-06-05",
        "time":              "18:36",
        "timestamp":         "As of today at 18:36 GMT+8"
      }
    """
    url = f"https://www.tradingview.com/symbols/CBOE-VX1!/?contract={contract_code}"
    await page.goto(url, wait_until="networkidle")

    # 1. Locate the DIV that actually contains the "As of …" span
    header_selector = "div.js-symbol-header-ticker:has(span.js-symbol-lp-time)"
    await page.wait_for_selector(header_selector, timeout=15_000)
    header = page.locator(header_selector)

    # 2. future_descriptor = data-symbol attribute
    descriptor = await header.get_attribute("data-symbol")
    #    e.g. "CBOE:VXM2025"

    # 3. Scrape price
    price_selector = "span.last-zoF9r75I.js-symbol-last"
    await page.wait_for_selector(price_selector, timeout=10_000)
    price_el = page.locator(price_selector)
    price_text = (await price_el.inner_text()).strip()  # e.g. "19.150"

    # 4. Scrape the "As of today at HH:MM GMT±X" line
    ts_selector = "span.js-symbol-lp-time"
    await page.wait_for_selector(ts_selector, timeout=10_000)
    ts_el = page.locator(ts_selector)
    tv_timestamp = (await ts_el.inner_text()).strip()
    #    e.g. "As of today at 18:36 GMT+8"

    # 5. Parse out HH:MM and GMT offset from that string
    match = re.search(r"As of today at (\d{1,2}:\d{2}) GMT([+-]\d{1,2})", tv_timestamp)
    if match:
        time_str = match.group(1)               # e.g. "18:36"
        offset_hours = int(match.group(2))      # e.g. "+8" → 8
    else:
        # If TradingView format changes, fallback to empty fields
        time_str = ""
        offset_hours = 0

    # 6. Convert current UTC to that offset to get the correct "date"
    page_tz = timezone(timedelta(hours=offset_hours))
    now_utc = datetime.now(timezone.utc)
    now_in_page_tz = now_utc.astimezone(page_tz)
    date_str = now_in_page_tz.strftime("%Y-%m-%d")  # e.g. "2025-06-05"

    return {
        "future_descriptor": descriptor,
        "price":             price_text,
        "date":              date_str,
        "time":              time_str,
        "timestamp":         tv_timestamp,
    }


async def main():
    # 1. Launch browser once, reuse the same page for all contracts
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        results = []
        for contract in CONTRACTS:
            data = await scrape_contract(page, contract)
            results.append(data)

        await browser.close()

    # 2. Build a UTC‐based filename
    now_utc = datetime.now(timezone.utc)
    suffix = now_utc.strftime("%Y%m%d-%H%M%S")  # e.g. "20250605-102530"
    base_filename = f"vix_tradingview_snapshot_{suffix}.csv"

    # 3. Ensure data/ directory exists
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", base_filename)

    # 4. Write all rows into one CSV
    with open(filepath, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(["future_descriptor", "price", "date", "time", "timestamp"])
        # One row per contract
        for row in results:
            writer.writerow([
                row["future_descriptor"],
                row["price"],
                row["date"],
                row["time"],
                row["timestamp"],
            ])

    print(f"▶ Saved → {filepath}")


if __name__ == "__main__":
    asyncio.run(main())
