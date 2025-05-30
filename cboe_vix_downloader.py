import os
import sys # Added
import pandas as pd # Added
import re
import time as time_module  # Rename to avoid conflict
import traceback
from datetime import datetime, timedelta, time
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pytz
from common import setup_logging, SAVE_DIR, InvalidDataError, MissingCriticalDataError

# Set up logging
logger = setup_logging('cboe_vix_downloader')

def validate_cboe_date():
    """
    Determine when the CBOE website was last updated based on current time
    
    Returns:
        str: Trading date in YYYY-MM-DD format that the CBOE website is showing
    """
    # Get current time in Central Time
    central = pytz.timezone('US/Central')
    now = datetime.now(central)
    
    # If it's a weekend (Saturday or Sunday), data is from Friday
    if now.weekday() >= 5:  # 5=Saturday, 6=Sunday
        # Get previous Friday
        days_to_subtract = now.weekday() - 4  # 4=Friday
        friday_date = now.date() - timedelta(days=days_to_subtract)
        return friday_date.strftime("%Y-%m-%d")
    
    # On business days
    if now.time() >= time(17, 0):
        # After 5 PM CT, the website should be updated with today's data
        return now.date().strftime("%Y-%m-%d")
    else:
        # Before 5 PM CT, the website likely shows previous business day's data
        if now.weekday() == 0:  # Monday
            # If it's Monday before 5 PM, data is from Friday
            friday_date = now.date() - timedelta(days=3)
            return friday_date.strftime("%Y-%m-%d")
        else:
            # Otherwise, previous business day
            prev_date = now.date() - timedelta(days=1)
            return prev_date.strftime("%Y-%m-%d")

def download_vix_futures_from_cboe():
    """
    Download VIX futures prices from CBOE website using Selenium
    
    Returns:
        dict: Dictionary with VIX futures prices
    """
    start_time = time_module.time()  # Use the renamed module here
    browser = None
    
    try:
        logger.info("Downloading VIX futures data from CBOE using Selenium...")
        
        # Set up Chrome options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        # Initialize the browser
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
        
        # Determine the date for the price data using our heuristic
        price_date = validate_cboe_date()
        logger.info(f"Using estimated date for CBOE data: {price_date}")
        
        # Get current date
        futures_data = {
            'date': price_date,
            'timestamp': datetime.now().strftime("%Y%m%d%H%M")
        }
        
        # Try to find VIX futures data table
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables on the page")
        
        for i, table in enumerate(tables):
            headers = []
            header_row = table.find('tr')
            if header_row:
                # Get table headers
                headers = [th.get_text().strip() for th in header_row.find_all(['th'])]
                if not headers and header_row.find_all(['td']):
                    # Sometimes headers are in td tags
                    headers = [td.get_text().strip() for td in header_row.find_all(['td'])]
                
                logger.info(f"Table {i+1} headers: {headers}")
                
                # Check if this looks like a VIX futures table
                if any(header.upper() in ['SYMBOL', 'EXPIRATION', 'SETTLEMENT', 'LAST'] for header in headers):
                    logger.info(f"Found potential VIX futures table (table {i+1})")
                    
                    # Map column indices
                    col_map = {}
                    for j, header in enumerate(headers):
                        header_upper = header.upper()
                        if 'SYMBOL' in header_upper:
                            col_map['symbol'] = j
                        elif 'EXPIRATION' in header_upper:
                            col_map['expiration'] = j
                        elif 'SETTLEMENT' in header_upper:
                            col_map['settlement'] = j
                        elif 'LAST' in header_upper:
                            col_map['last'] = j
                        elif 'VOLUME' in header_upper:
                            col_map['volume'] = j
                    
                    # Default mappings if not found
                    if 'symbol' not in col_map:
                        col_map['symbol'] = 0  # Assume first column is symbol
                    if 'settlement' not in col_map and 'last' not in col_map:
                        # Try to find price columns by position
                        if len(headers) >= 7:
                            col_map['settlement'] = 6  # Often 7th column
                        if len(headers) >= 3:
                            col_map['last'] = 2  # Often 3rd column
                    
                    # Process all rows
                    data_rows = table.find_all('tr')[1:]  # Skip header
                    logger.info(f"Processing {len(data_rows)} data rows")
                    
                    contracts_found = 0
                    
                    for row in data_rows:
                        cells = row.find_all(['td'])
                        if len(cells) < max(col_map.values()) + 1:
                            continue
                        
                        try:
                            symbol_cell = cells[col_map['symbol']]
                            symbol = symbol_cell.get_text().strip()
                            
                            # Try to get settlement price
                            settlement_price = None
                            if 'settlement' in col_map:
                                settlement_text = cells[col_map['settlement']].get_text().strip()
                                if settlement_text and settlement_text != '-':
                                    try:
                                        settlement_price = float(settlement_text.replace(',', ''))
                                    except ValueError:
                                        raise InvalidDataError(f"Could not convert settlement text '{settlement_text}' to float for symbol '{symbol}'.")
                            
                            # If no settlement price, try last price
                            if (settlement_price is None or settlement_price == 0) and 'last' in col_map:
                                last_text = cells[col_map['last']].get_text().strip()
                                if last_text and last_text != '-' and last_text != '0':
                                    try:
                                        settlement_price = float(last_text.replace(',', ''))
                                    except ValueError:
                                        raise InvalidDataError(f"Could not convert last price text '{last_text}' to float for symbol '{symbol}'.")
                            
                            if not symbol or settlement_price is None or settlement_price == 0:
                                continue
                                
                            # Special case for VIX index
                            if symbol.upper() == 'VIX':
                                futures_data['CBOE:VIX'] = settlement_price
                                logger.info(f"Extracted VIX index: {settlement_price}")
                                contracts_found += 1
                                continue
                            
                            # Process VIX futures symbols
                            vx_patterns = [
                                re.compile(r'VX(\d+)?\/([A-Z])(\d+)'),  # VX/H5, VX09/H5
                                re.compile(r'VX([A-Z])(\d+)'),          # VXH5, VXH25
                                re.compile(r'VIX\s+([A-Z]{3})[^0-9]*(\d{2})') # VIX MAR 25
                            ]
                            
                            for pattern in vx_patterns:
                                match = pattern.match(symbol)
                                if match:
                                    # Extract month code and year
                                    if len(match.groups()) == 3 and match.group(2):  # VX/H5 format
                                        month_code = match.group(2)
                                        year_digit = match.group(3)[-1]  # Last digit of year
                                    elif len(match.groups()) == 2 and match.group(1):
                                        if len(match.group(1)) == 1:  # VXH5 format
                                            month_code = match.group(1)
                                            year_digit = match.group(2)[-1]
                                        else:  # VIX MAR 25 format
                                            month_str = match.group(1).upper()
                                            year_digit = match.group(2)[-1]
                                            
                                            # Convert month name to code
                                            month_map = {
                                                'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                                'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                                'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                            }
                                            month_code = month_map.get(month_str, '?')
                                    else:
                                        continue
                                    
                                    # Format standard tickers
                                    cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                                    std_ticker = f"/VX{month_code}{year_digit}"
                                    
                                    futures_data[cboe_ticker] = settlement_price
                                    futures_data[std_ticker] = settlement_price
                                    
                                    logger.info(f"Extracted future: {cboe_ticker} = {settlement_price}")
                                    contracts_found += 1
                                    break
                            
                        except Exception as e:
                            logger.warning(f"Error processing row: {str(e)}")
                    
                    if contracts_found > 0:
                        logger.info(f"Found {contracts_found} contracts in table {i+1}")
                        break  # Stop after finding a valid table
        
        # Check if we found any futures data
                        if len(futures_data) <= 2: # Only contains 'date' and 'timestamp'
                            logger.error("No VIX futures contracts were successfully extracted from CBOE.")
                            raise MissingCriticalDataError("No VIX futures contracts found on CBOE website after parsing.")
                        logger.info(f"Successfully extracted {len(futures_data)-2} VIX futures from CBOE (processing took {time_module.time() - start_time:.2f}s)")
                        return futures_data
            # Try direct extraction from page using driver
            # This block is reached if BeautifulSoup parsing didn't find enough contracts
            logger.info("Attempting direct extraction from browser elements as primary parsing failed or found insufficient data.")
            try:
                # Find the VIX futures table
                tables = browser.find_elements(By.TAG_NAME, "table")
                if tables:
                    # Get rows directly
                    rows = tables[0].find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 1:  # Skip header
                        contracts_found = 0
                        
                        for row in rows[1:]:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) < 7:  # Need minimum cells
                                continue
                                
                            try:
                                symbol = cells[0].text.strip()
                                
                                # Try settlement price first
                                settlement_text = cells[6].text.strip() if len(cells) > 6 else ""
                                settlement_price = None
                                
                                if settlement_text and settlement_text != '-':
                                    try:
                                        settlement_price = float(settlement_text.replace(',', ''))
                                    except ValueError:
                                        raise InvalidDataError(f"Could not convert settlement text '{settlement_text}' to float for symbol '{symbol}' (direct extraction).")
                                
                                # If no settlement price, try last price
                                if settlement_price is None or settlement_price == 0:
                                    last_text = cells[2].text.strip() if len(cells) > 2 else ""
                                    if last_text and last_text != '-' and last_text != '0':
                                        try:
                                            settlement_price = float(last_text.replace(',', ''))
                                        except ValueError:
                                            raise InvalidDataError(f"Could not convert last price text '{last_text}' to float for symbol '{symbol}' (direct extraction).")
                                
                                if not symbol or settlement_price is None or settlement_price == 0:
                                    continue
                                
                                # Process symbol (similar to above)
                                if symbol.upper() == 'VIX':
                                    futures_data['CBOE:VIX'] = settlement_price
                                    logger.info(f"Extracted VIX index: {settlement_price}")
                                    contracts_found += 1
                                    continue
                                    
                                # Use same regex patterns as above
                                vx_patterns = [
                                    re.compile(r'VX(\d+)?\/([A-Z])(\d+)'),
                                    re.compile(r'VX([A-Z])(\d+)'),
                                    re.compile(r'VIX\s+([A-Z]{3})[^0-9]*(\d{2})')
                                ]
                                
                                for pattern in vx_patterns:
                                    match = pattern.match(symbol)
                                    if match:
                                        # Extract month and year (similar to above)
                                        if len(match.groups()) == 3 and match.group(2):
                                            month_code = match.group(2)
                                            year_digit = match.group(3)[-1]
                                        elif len(match.groups()) == 2 and match.group(1):
                                            if len(match.group(1)) == 1:
                                                month_code = match.group(1)
                                                year_digit = match.group(2)[-1]
                                            else:
                                                month_str = match.group(1).upper()
                                                year_digit = match.group(2)[-1]
                                                
                                                month_map = {
                                                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                                }
                                                month_code = month_map.get(month_str, '?')
                                        else:
                                            continue
                                        
                                        cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                                        std_ticker = f"/VX{month_code}{year_digit}"
                                        
                                        futures_data[cboe_ticker] = settlement_price
                                        futures_data[std_ticker] = settlement_price
                                        
                                        logger.info(f"Extracted future (direct): {cboe_ticker} = {settlement_price}")
                                        contracts_found += 1
                                        break
                                
                            except Exception as e:
                                logger.warning(f"Error in direct extraction: {str(e)}")
                        
                        if contracts_found > 0: # Check if direct extraction found anything
                            logger.info(f"Successfully extracted {contracts_found} contracts via direct browser access")
                            # Final check on futures_data length after direct extraction
                            if len(futures_data) <= 2:
                                logger.error("Direct extraction populated contracts, but futures_data is still too short.")
                                raise MissingCriticalDataError("Direct extraction failed to populate futures_data correctly.")
                            logger.info(f"Successfully extracted {len(futures_data)-2} VIX futures from CBOE (processing took {time_module.time() - start_time:.2f}s)")
                            return futures_data
            except Exception as e: # Catch errors during direct extraction
                logger.warning(f"Direct extraction failed: {str(e)}")
                # Do not re-raise here, let it fall through to the final check
            
            # Final check if any data was extracted by either method
            if len(futures_data) <= 2: # Only contains 'date' and 'timestamp'
                logger.error("No VIX futures contracts were successfully extracted from CBOE by any method.")
                raise MissingCriticalDataError("No VIX futures contracts found on CBOE website after all parsing attempts.")
            # This part should ideally not be reached if the logic above is correct,
            # but as a fallback:
            logger.info(f"Successfully extracted {len(futures_data)-2} VIX futures from CBOE (processing took {time_module.time() - start_time:.2f}s)")
            return futures_data

    except Exception as e:
        logger.error(f"Error downloading from CBOE: {str(e)}")
        logger.error(traceback.format_exc())
        raise MissingCriticalDataError(f"Failed to download or parse CBOE VIX data: {str(e)}")
    
    finally:
        # Always close the browser
        if browser:
            try:
                browser.quit()
                logger.info("Browser session closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {str(e)}")

def save_cboe_data(data_dict, save_dir=SAVE_DIR):
    if not data_dict or len(data_dict) <= 2: # Basic check for non-empty data beyond timestamp/date
        logger.warning("No CBOE data provided to save or data is empty.")
        return None

    timestamp = data_dict.get('timestamp', datetime.now().strftime("%Y%m%d%H%M"))
    price_date = data_dict.get('date', 'unknown_date')
    
    records = []
    for key, value in data_dict.items():
        if key in ['date', 'timestamp']:
            continue
        records.append({
            'timestamp': timestamp,
            'price_date': price_date,
            'symbol': key,
            'price': value
        })
    
    if not records:
        logger.warning("No actual contract records found in CBOE data to save.")
        return None

    df = pd.DataFrame(records)
    
    # Sort by symbol for consistent output
    df = df.sort_values(by='symbol').reset_index(drop=True)

    file_name = f"cboe_vix_futures_{timestamp}.csv"
    file_path = os.path.join(save_dir, file_name)
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"CBOE VIX futures data saved to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed to save CBOE data to CSV: {e}")
        return None

if __name__ == "__main__":
    logger.info("Running CBOE VIX Downloader as a standalone script...")
    data = None
    try:
        data = download_vix_futures_from_cboe()
        if data:
            logger.info(f"Successfully downloaded CBOE VIX futures data: {len(data)-2} contracts found.")
            file_saved_path = save_cboe_data(data)
            if file_saved_path:
                print(f"✅ CBOE VIX futures data downloaded and saved to {file_saved_path}")
            else:
                print("⚠️ CBOE VIX futures data downloaded but failed to save.")
        else:
            # This case should ideally be less frequent if download_vix_futures_from_cboe raises exceptions for no data
            print("ℹ️ No data returned from CBOE VIX download function, though no direct error was raised.")
    except (MissingCriticalDataError, InvalidDataError) as e:
        print(f"❌ ERROR: {e}")
        logger.error(f"CBOE VIX download failed: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        logger.error(f"An unexpected error occurred in CBOE VIX downloader: {e}", exc_info=True)
        sys.exit(1)
