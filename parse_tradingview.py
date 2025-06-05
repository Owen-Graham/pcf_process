# parse_tradingview.py

import asyncio
import csv
import argparse
import os
import re
from datetime import datetime, timedelta, timezone

from playwright.async_api import async_playwright

# List of the first two VIX futures in the cycle: M5 (June 2025) and N5 (July 2025)
DEFAULT_CONTRACTS = ["VXM2025", "VXN2025"]


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


async def scrape_usdjpy(page):
    """
    Given a Playwright Page, navigate to the TradingView URL for USDJPY,
    wait for the live quote, and return a dict:
      {
        "pair": "USDJPY",
        "price": "157.842",
        "timestamp": "As of today at 18:36 GMT+8"
      }
    """
    url = "https://www.tradingview.com/symbols/USDJPY/"
    await page.goto(url, wait_until="networkidle")

    # 1. Locate the DIV that actually contains the "As of …" span
    header_selector = "div.js-symbol-header-ticker:has(span.js-symbol-lp-time)"
    await page.wait_for_selector(header_selector, timeout=15_000)
    header = page.locator(header_selector)

    # 2. Scrape price
    price_selector = "span.last-zoF9r75I.js-symbol-last"
    await page.wait_for_selector(price_selector, timeout=10_000)
    price_el = page.locator(price_selector)
    price_text = (await price_el.inner_text()).strip()  # e.g. "157.842"

    # 3. Scrape the "As of today at HH:MM GMT±X" line
    ts_selector = "span.js-symbol-lp-time"
    await page.wait_for_selector(ts_selector, timeout=10_000)
    ts_el = page.locator(ts_selector)
    tv_timestamp = (await ts_el.inner_text()).strip()
    #    e.g. "As of today at 18:36 GMT+8"

    # 4. Parse out HH:MM and GMT offset from that string
    match = re.search(r"As of today at (\d{1,2}:\d{2}) GMT([+-]\d{1,2})", tv_timestamp)
    if match:
        time_str = match.group(1)               # e.g. "18:36"
        offset_hours = int(match.group(2))      # e.g. "+8" → 8
    else:
        # If TradingView format changes, fallback to empty fields
        time_str = ""
        offset_hours = 0

    # 5. Convert current UTC to that offset to get the correct "date"
    page_tz = timezone(timedelta(hours=offset_hours))
    now_utc = datetime.now(timezone.utc)
    now_in_page_tz = now_utc.astimezone(page_tz)
    date_str = now_in_page_tz.strftime("%Y-%m-%d")  # e.g. "2025-06-05"

    return {
        "pair": "USDJPY",
        "price": price_text,
        "date": date_str,
        "time": time_str,
        "timestamp": tv_timestamp,
    }


async def main(contracts_to_scrape):
    # 1. Launch browser once, reuse the same page for all contracts
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        vix_results = []
        for contract in contracts_to_scrape:
            data = await scrape_contract(page, contract)
            vix_results.append(data)

        usdjpy_data = await scrape_usdjpy(page)

        await browser.close()

    # 2. Build a UTC‐based filename
    now_utc = datetime.now(timezone.utc)
    suffix = now_utc.strftime("%Y%m%d%H%M")
    vix_filename = f"cboe_vix_futures_{suffix}.csv"
    fx_filename = f"tradingview_fx_data_{suffix}.csv"

    # 3. Ensure data/ directory exists
    os.makedirs("data", exist_ok=True)
    vix_filepath = os.path.join("data", vix_filename)
    fx_filepath = os.path.join("data", fx_filename)

    # 4. Write VIX data to CSV
    with open(vix_filepath, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(["timestamp", "price_date", "symbol", "price"])
        # One row per contract
        for row in vix_results:
            writer.writerow([
                now_utc.strftime("%Y-%m-%d %H:%M:%S"), # timestamp
                row["date"], # price_date
                row["future_descriptor"], # symbol
                row["price"], # price
            ])
    print(f"▶ Saved VIX data → {vix_filepath}")

    # 5. Write USDJPY data to CSV
    with open(fx_filepath, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Header
        writer.writerow(["timestamp", "date", "source_url", "pair", "label", "rate"])
        # Data
        writer.writerow([
            now_utc.strftime("%Y-%m-%d %H:%M:%S"), # timestamp
            usdjpy_data["date"], # date
            "https://www.tradingview.com/symbols/USDJPY/", # source_url
            usdjpy_data["pair"], # pair
            "TradingView Realtime", # label
            usdjpy_data["price"], # rate
        ])
    print(f"▶ Saved USDJPY data → {fx_filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape VIX futures contract data from TradingView.")
    parser.add_argument(
        "contracts",
        nargs="*",
        default=DEFAULT_CONTRACTS,
        help="List of VIX futures contract codes (e.g., VXM2025 VXN2025). Defaults to VXM2025 and VXN2025 if not provided.",
    )
    args = parser.parse_args()

    asyncio.run(main(args.contracts))
