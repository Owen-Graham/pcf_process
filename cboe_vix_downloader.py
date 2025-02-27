import os
import requests
from bs4 import BeautifulSoup
import re
import traceback
import time
from datetime import datetime
import pandas as pd
from common import setup_logging, SAVE_DIR, format_vix_data

# For Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up logging
logger = setup_logging('cboe_vix_downloader')

def download_vix_futures_from_cboe():
    """
    Download VIX futures prices from CBOE website using Selenium
    
    Returns:
        dict: Dictionary with VIX futures prices
    """
    start_time = time.time()
    browser = None
    
    try:
        logger.info("Downloading VIX futures data from CBOE using Selenium...")
        
        # Set up Chrome options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Initialize the browser
        logger.info("Initializing Chrome browser...")
        browser = webdriver.Chrome(options=options)
        
        # CBOE VIX futures page
        url = "https://www.cboe.com/tradable_products/vix/vix_futures/"
        
        # Navigate to the page
        logger.info(f"Navigating to {url}")
        browser.get(url)
        
        # Wait for the page to load completely
        logger.info("Waiting for page to load...")
        WebDriverWait(browser, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Get the page source after JavaScript execution
        page_source = browser.page_source
        
        # Save HTML for debugging
        debug_html_path = os.path.join(SAVE_DIR, "cboe_debug.html")
        with open(debug_html_path, "w", encoding="utf-8") as f:
            f.write(page_source)
        logger.debug(f"Saved CBOE HTML to {debug_html_path} for reference")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Get current date
        futures_data = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'timestamp': datetime.now().strftime("%Y%m%d%H%M")
        }
        
        # Find tables that might contain futures data
        tables = soup.find_all('table')
        logger.debug(f"Found {len(tables)} tables on CBOE page")
        
        # Look for VIX futures table - typically has columns like Symbol, Expiration, Last, etc.
        for i, table in enumerate(tables):
            try:
                logger.debug(f"Analyzing table {i+1}")
                
                # Get headers to identify the futures table
                headers = [th.get_text().strip() for th in table.find_all(['th', 'td']) if th.get_text().strip()]
                logger.debug(f"Table {i+1} headers: {headers}")
                
                # Check if this looks like a futures table
                required_headers = ['SYMBOL', 'EXPIRATION', 'LAST', 'SETTLEMENT']
                if any(header.upper() in [h.upper() for h in headers] for header in required_headers):
                    logger.info(f"Found potential VIX futures table (table {i+1})")
                    
                    # Map column positions to their meaning
                    col_map = {}
                    for j, header in enumerate(headers):
                        header_upper = header.upper()
                        if 'SYMBOL' in header_upper:
                            col_map['symbol'] = j
                        elif 'EXPIRATION' in header_upper or 'DATE' in header_upper:
                            col_map['expiration'] = j
                        elif 'SETTLEMENT' in header_upper:
                            col_map['settlement'] = j
                        elif 'LAST' in header_upper:
                            col_map['last'] = j
                    
                    # Process the rows
                    rows = table.find_all('tr')
                    logger.debug(f"Processing {len(rows)-1} data rows")
                    
                    for row in rows[1:]:  # Skip header row
                        cells = row.find_all(['td'])
                        if len(cells) < len(headers):
                            continue
                        
                        # Extract data based on column mapping
                        symbol_text = cells[col_map.get('symbol', 0)].get_text().strip()
                        
                        # Get settlement price or last price
                        price = None
                        if 'settlement' in col_map and cells[col_map['settlement']].get_text().strip():
                            price_text = cells[col_map['settlement']].get_text().strip()
                            try:
                                price = float(price_text.replace(',', '').replace('$', ''))
                            except ValueError:
                                price = None
                        
                        # Use last price if settlement not available
                        if price is None and 'last' in col_map and cells[col_map['last']].get_text().strip():
                            price_text = cells[col_map['last']].get_text().strip()
                            try:
                                price = float(price_text.replace(',', '').replace('$', ''))
                            except ValueError:
                                continue
                        
                        if price is None:
                            continue
                        
                        # Extract the contract code from the symbol (various formats possible)
                        vx_pattern = re.compile(r'VX([A-Z])(\d{1,2})|VX\/([A-Z])(\d{1,2})|VIX\s+([A-Za-z]{3})[^0-9]*(\d{2})', re.IGNORECASE)
                        match = vx_pattern.search(symbol_text)
                        
                        if match:
                            # Determine which pattern matched and extract info
                            if match.group(1) and match.group(2):  # VXH5 format
                                month_code = match.group(1)
                                year_digit = match.group(2)[-1]  # Last digit of year
                            elif match.group(3) and match.group(4):  # VX/H5 format
                                month_code = match.group(3)
                                year_digit = match.group(4)[-1]
                            else:  # VIX MAR 25 format
                                month_str = match.group(5).upper()
                                year_digit = match.group(6)[-1]
                                
                                # Convert month name to code
                                month_map = {
                                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                }
                                month_code = month_map.get(month_str, '?')
                            
                            # Format the VIX future contract codes
                            cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                            std_ticker = f"/VX{month_code}{year_digit}"
                            
                            # Store both formats
                            futures_data[cboe_ticker] = price
                            futures_data[std_ticker] = price
                            logger.info(f"Extracted: {cboe_ticker} = {price}")
                        elif symbol_text.upper() == 'VIX' and price is not None:
                            # This is the VIX index itself
                            futures_data['CBOE:VIX'] = price
                            logger.info(f"Extracted VIX index: {price}")
            except Exception as e:
                logger.warning(f"Error processing table {i+1}: {str(e)}")
        
        # Check if we found any futures data
        if len(futures_data) > 2:  # More than just date and timestamp
            logger.info(f"Successfully extracted {len(futures_data)-2} VIX futures from CBOE (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            logger.warning("Could not extract VIX futures data from CBOE website")
            return None
    
    except Exception as e:
        logger.error(f"Error downloading from CBOE: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    
    finally:
        # Always close the browser
        if browser:
            try:
                browser.quit()
                logger.info("Browser session closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {str(e)}")

def save_cboe_data(futures_data, save_dir=SAVE_DIR):
    """Save CBOE futures data as CSV"""
    if not futures_data or len(futures_data) <= 2:
        logger.warning("No CBOE futures data to save")
        return None
    
    try:
        # Format data into standardized records
        records = format_vix_data(futures_data, "CBOE")
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        csv_filename = f"vix_futures_cboe_{timestamp}.csv"
        csv_path = os.path.join(save_dir, csv_filename)
        
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved CBOE futures data to {csv_path}")
        
        return csv_path
    
    except Exception as e:
        logger.error(f"Error saving CBOE data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    # Download VIX futures from CBOE
    cboe_data = download_vix_futures_from_cboe()
    
    # Save to CSV if data was found
    if cboe_data:
        csv_path = save_cboe_data(cboe_data)
        if csv_path:
            print(f"✅ CBOE VIX futures data saved to: {csv_path}")
        else:
            print("❌ Failed to save CBOE data")
            exit(1)
    else:
        print("❌ No VIX futures data found from CBOE")
        exit(1)
