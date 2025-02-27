import os
import requests
from bs4 import BeautifulSoup
import re
import traceback
import time
from datetime import datetime
from common import setup_logging, SAVE_DIR, format_vix_data

# Set up logging
logger = setup_logging('cboe_vix_downloader')

def log_response_details(response):
    """Log detailed information about HTTP responses"""
    logger.debug(f"CBOE response: status={response.status_code}, content-type={response.headers.get('content-type')}, length={len(response.content)}")

def download_vix_futures_from_cboe():
    """
    Download VIX futures prices from CBOE website
    
    Returns:
        dict: Dictionary with VIX futures prices
    """
    start_time = time.time()
    try:
        logger.info("Downloading VIX futures data from CBOE...")
        
        # CBOE VIX futures page
        url = "https://www.cboe.com/tradable_products/vix/vix_futures/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            log_response_details(response)
            
            # Save HTML for debugging
            debug_html_path = os.path.join(SAVE_DIR, "cboe_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.debug(f"Saved CBOE HTML to {debug_html_path} for reference")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve CBOE page: {str(e)}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get current date
        futures_data = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'timestamp': datetime.now().strftime("%Y%m%d%H%M")
        }
        
        # Approach 1: Find tables that might contain futures data
        tables = soup.find_all('table')
        logger.debug(f"Found {len(tables)} tables on CBOE page")
        
        # Analyze each table to identify potential futures data
        for i, table in enumerate(tables):
            try:
                logger.debug(f"Analyzing table {i+1}")
                rows = table.find_all('tr')
                if not rows:
                    continue
                    
                # Check headers for identifying words
                headers = [th.get_text().strip() for th in rows[0].find_all(['th', 'td'])]
                logger.debug(f"Table {i+1} headers: {headers}")
                
                # Look for keywords in headers
                if any(keyword in ' '.join(headers).lower() for keyword in 
                      ['vix', 'future', 'expiration', 'settlement', 'price']):
                    logger.info(f"Table {i+1} appears to be a VIX futures table")
                    
                    # Process data rows
                    for j, row in enumerate(rows[1:], 1):  # Skip header
                        cells = row.find_all('td')
                        if len(cells) < 2:
                            continue
                            
                        # Try to extract contract and price info
                        contract_text = cells[0].get_text().strip()
                        
                        # Look for price in other cells
                        prices = []
                        for cell in cells[1:]:
                            text = cell.get_text().strip()
                            # Look for numeric values (potential prices)
                            if re.match(r'\d+\.\d+', text):
                                prices.append(text)
                        
                        if prices and (('VX' in contract_text) or ('VIX' in contract_text)):
                            # Extract contract info using regex
                            vx_pattern = re.compile(r'VX([A-Z])(\d{1,2})|VIX\s+([A-Za-z]{3})[^0-9]*(\d{2})', re.IGNORECASE)
                            match = vx_pattern.search(contract_text)
                            
                            if match:
                                # Determine which pattern matched and extract info
                                if match.group(1) and match.group(2):  # VXH5 format
                                    month_code = match.group(1)
                                    year_digit = match.group(2)[-1]  # Last digit of year
                                else:  # VIX MAR 25 format
                                    month_str = match.group(3).upper()
                                    year_digit = match.group(4)[-1]  # Last digit of year
                                    
                                    # Convert month name to code
                                    month_map = {
                                        'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                        'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                        'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                    }
                                    month_code = month_map.get(month_str, '?')
                                
                                # Price will be the first numeric value found
                                price_str = prices[0]
                                price_str = price_str.replace('$', '').replace(',', '')
                                price = float(price_str)
                                
                                # Store with both formats
                                cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                                std_ticker = f"/VX{month_code}{year_digit}"
                                
                                futures_data[cboe_ticker] = price
                                futures_data[std_ticker] = price
                                logger.info(f"CBOE table: {cboe_ticker} = {price}")
            except Exception as e:
                logger.warning(f"Error processing table {i+1}: {str(e)}")
        
        # Approach 2: Look for table-like data in divs
        div_tables = soup.find_all('div', class_=lambda c: c and ('table' in c.lower() or 'grid' in c.lower()))
        logger.debug(f"Found {len(div_tables)} div elements that might contain table data")
        
        for i, div in enumerate(div_tables):
            try:
                logger.debug(f"Analyzing div table {i+1}")
                
                # Look for rows in the div
                rows = div.find_all('div', class_=lambda c: c and ('row' in c.lower() or 'tr' in c.lower()))
                if not rows:
                    # Try to find direct child divs that might be rows
                    rows = div.find_all('div', recursive=False)
                
                if not rows:
                    continue
                
                logger.debug(f"Found {len(rows)} potential rows in div table {i+1}")
                
                # Check for VIX futures content
                for j, row in enumerate(rows):
                    text = row.get_text().strip()
                    
                    # Look for VIX futures pattern and price pattern in the same row
                    vx_pattern = re.compile(r'VX([A-Z])(\d{1,2})|VIX\s+([A-Za-z]{3})[^0-9]*(\d{2})', re.IGNORECASE)
                    price_pattern = re.compile(r'(\d+\.\d+)')
                    
                    vx_match = vx_pattern.search(text)
                    price_matches = price_pattern.findall(text)
                    
                    if vx_match and price_matches:
                        # Extract month and year info
                        if vx_match.group(1) and vx_match.group(2):  # VXH5 format
                            month_code = vx_match.group(1)
                            year_digit = vx_match.group(2)[-1]
                        else:  # VIX MAR 25 format
                            month_str = vx_match.group(3).upper()
                            year_digit = vx_match.group(4)[-1]
                            
                            # Convert month name to code
                            month_map = {
                                'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                            }
                            month_code = month_map.get(month_str, '?')
                        
                        # Use the first price found
                        price = float(price_matches[0])
                        
                        # Store with both formats
                        cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                        std_ticker = f"/VX{month_code}{year_digit}"
                        
                        futures_data[cboe_ticker] = price
                        futures_data[std_ticker] = price
                        logger.info(f"CBOE div table: {cboe_ticker} = {price}")
            except Exception as e:
                logger.warning(f"Error processing div table {i+1}: {str(e)}")
        
        # Approach 3: Fallback to page text parsing if no data found
        if len(futures_data) <= 2:  # Only has date and timestamp
            logger.debug("Falling back to page text parsing")
            
            # Get all text from the page
            page_text = soup.get_text()
            
            # Look for patterns like "VXH5" or "VIX MAR 25" followed by prices
            patterns = [
                # Pattern: VXH5 followed by price
                re.compile(r'VX([A-Z])(\d{1,2})[^\d]*(\d+\.\d+)', re.IGNORECASE),
                
                # Pattern: VIX [month] [year] followed by price
                re.compile(r'VIX\s+([A-Za-z]{3})[^0-9]*(\d{2})[^\d]*(\d+\.\d+)', re.IGNORECASE)
            ]
            
            for pattern in patterns:
                matches = pattern.findall(page_text)
                
                for match in matches:
                    # Check which pattern matched
                    if len(match[0]) == 1:  # Month code (H, J, etc.)
                        month_code = match[0].upper()
                        year = match[1]
                        price_str = match[2]
                    else:  # Month name (MAR, APR, etc.)
                        month_str = match[0].upper()
                        year = match[1]
                        price_str = match[2]
                        
                        # Convert month name to code
                        month_map = {
                            'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                            'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                            'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                        }
                        month_code = month_map.get(month_str, '?')
                    
                    year_digit = year[-1]
                    
                    # Try to parse price
                    try:
                        price = float(price_str)
                        
                        # Store with both formats
                        cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                        std_ticker = f"/VX{month_code}{year_digit}"
                        
                        futures_data[cboe_ticker] = price
                        futures_data[std_ticker] = price
                        logger.info(f"CBOE text: {cboe_ticker} = {price}")
                    except ValueError:
                        logger.debug(f"Could not convert {price_str} to float in text parsing")
        
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

def save_cboe_data(futures_data, save_dir=SAVE_DIR):
    """Save CBOE futures data as CSV"""
    if not futures_data or len(futures_data) <= 2:
        logger.warning("No CBOE futures data to save")
        return None
    
    try:
        # Format data into standardized records
        records = format_vix_data(futures_data, "CBOE")
        
        # Create DataFrame
        import pandas as pd
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
