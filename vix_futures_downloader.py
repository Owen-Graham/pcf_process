import os
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import traceback
import sys
import logging
from datetime import datetime, timedelta
import glob
import re
import time

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join("data", "vix_downloader.log"))
    ]
)
logger = logging.getLogger('vix_futures_downloader')

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)

def log_response_details(response, source_name):
    """Log detailed information about HTTP responses"""
    logger.debug(f"{source_name} response: status={response.status_code}, content-type={response.headers.get('content-type')}, length={len(response.content)}")

def format_vix_data_for_output(cboe_data, yfinance_data, simplex_data):
    """
    Formats all VIX futures data into a standardized format for output
    
    Returns:
    pandas.DataFrame: DataFrame with standardized columns:
        - timestamp: Unix timestamp of data collection
        - price_date: Date prices are for (YYYY-MM-DD)
        - vix_future: Standardized VIX future code (e.g., VXH5, VXK5)
        - source: Data source (Yahoo, CBOE, PCF)
        - symbol: Original symbol/identifier from the source
        - price: Price value
    """
    # Current timestamp in desired format
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    
    # Current date used for pricing date (could be modified if needed)
    price_date = datetime.now().strftime("%Y-%m-%d")
    
    # List to store all rows
    all_rows = []
    
    # Process CBOE data
    if cboe_data:
        for key, value in cboe_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
                
            # Skip null values
            if value is None:
                continue
                
            # Process standardized CBOE tickers (CBOE:VXH5)
            if key.startswith('CBOE:VX'):
                vix_future = key.split(':')[1]  # Extract VXH5 from CBOE:VXH5
                all_rows.append({
                    'timestamp': timestamp,
                    'price_date': price_date,
                    'vix_future': vix_future,
                    'source': 'CBOE',
                    'symbol': key,
                    'price': float(value)
                })
            # Process legacy CBOE tickers (/VXH5)
            elif key.startswith('/VX') and not key.endswith('_cboe'):
                vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
                all_rows.append({
                    'timestamp': timestamp,
                    'price_date': price_date,
                    'vix_future': vix_future,
                    'source': 'CBOE',
                    'symbol': key,
                    'price': float(value)
                })
    
    # Process Yahoo Finance data
    if yfinance_data:
        # Define mapping of Yahoo tickers to standardized VIX futures
        yahoo_mapping = {
            # VIX index
            'VX=F': 'VIX',
            'YAHOO:VIX': 'VIX',
            
            # Direct contracts
            '/VXH5': 'VXH5',
            '/VXJ5': 'VXJ5',
            '/VXK5': 'VXK5',
            '/VXM5': 'VXM5',
            '/VXN5': 'VXN5',
            
            # Standard Yahoo patterns
            'YAHOO:VXH5': 'VXH5',
            'YAHOO:VXJ5': 'VXJ5',
            'YAHOO:VXK5': 'VXK5',
            'YAHOO:VXM5': 'VXM5',
            'YAHOO:VXN5': 'VXN5',
            
            # VFTW series (1st, 2nd, 3rd month)
            '^VFTW1': 'VXK5',  # Adjusted based on current contract months
            '^VFTW2': 'VXM5', 
            '^VFTW3': 'VXN5',
            'YAHOO:^VFTW1': 'VXK5',
            'YAHOO:^VFTW2': 'VXM5',
            'YAHOO:^VFTW3': 'VXN5',
            
            # VXIND series (1st, 2nd, 3rd month)
            '^VXIND1': 'VXK5',  # Adjusted based on current contract months
            '^VXIND2': 'VXM5',
            '^VXIND3': 'VXN5',
            'YAHOO:^VXIND1': 'VXK5',
            'YAHOO:^VXIND2': 'VXM5', 
            'YAHOO:^VXIND3': 'VXN5'
        }
        
        # Update mapping based on current month/year if needed
        # This should be done dynamically based on the current date
        # Note: In a full implementation, these mappings would be derived from 
        # the position_to_contract logic in the download_vix_futures_from_yfinance function
        
        for key, value in yfinance_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
                
            # Skip null values
            if value is None:
                continue
            
            # Get standardized future name if available
            if key in yahoo_mapping:
                vix_future = yahoo_mapping[key]
            elif key.startswith('/VX'):
                # For direct /VX tickers, convert to VX format
                vix_future = 'VX' + key[3:]
            elif key.startswith('YAHOO:VX'):
                # For YAHOO:VX format
                vix_future = key.split(':')[1]
            else:
                # Skip unknown tickers
                continue
                
            all_rows.append({
                'timestamp': timestamp,
                'price_date': price_date,
                'vix_future': vix_future,
                'source': 'Yahoo',
                'symbol': key,
                'price': float(value)
            })
    
    # Process Simplex PCF data
    if simplex_data:
        # Regular Simplex format
        for key, value in simplex_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
                
            # Skip null values
            if value is None:
                continue
                
            # Process standardized Simplex tickers (SIMPLEX:VXH5)
            if key.startswith('SIMPLEX:VX'):
                vix_future = key.split(':')[1]  # Extract VXH5 from SIMPLEX:VXH5
                all_rows.append({
                    'timestamp': timestamp,
                    'price_date': price_date,
                    'vix_future': vix_future,
                    'source': 'PCF',
                    'symbol': key,
                    'price': float(value)
                })
            # Process legacy Simplex tickers (/VXH5)
            elif key.startswith('/VX') and not key.endswith(('_simplex', '_yahoo', '_cboe')):
                vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
                all_rows.append({
                    'timestamp': timestamp,
                    'price_date': price_date,
                    'vix_future': vix_future,
                    'source': 'PCF',
                    'symbol': key,
                    'price': float(value)
                })
    
    # Create DataFrame from collected rows
    df = pd.DataFrame(all_rows)
    
    # Sort by VIX future and source
    if not df.empty:
        df = df.sort_values(['vix_future', 'source'])
    
    return df

def save_vix_data(df, save_dir="data"):
    """
    Save the VIX futures data to CSV files
    """
    if df.empty:
        return False
        
    timestamp = df['timestamp'].iloc[0]
    
    # Daily snapshot filename
    csv_filename = f"vix_futures_{timestamp}.csv"
    csv_path = os.path.join(save_dir, csv_filename)
    
    # Save daily snapshot
    df.to_csv(csv_path, index=False)
    
    # Master CSV path - always append to this file
    master_csv_path = os.path.join(save_dir, "vix_futures_master.csv")
    
    # Append to master CSV if it exists, otherwise create it
    if os.path.exists(master_csv_path):
        try:
            # Read existing master CSV
            master_df = pd.read_csv(master_csv_path)
            
            # Check if current date's data already exists
            price_date = df['price_date'].iloc[0]
            timestamp_val = df['timestamp'].iloc[0]
            
            # Remove data with the same timestamp (exact same run)
            master_df = master_df[master_df['timestamp'] != timestamp_val]
            
            # Append new data
            combined_df = pd.concat([master_df, df], ignore_index=True)
            combined_df.to_csv(master_csv_path, index=False)
        except Exception as e:
            # If error reading/updating master, just overwrite with new file
            df.to_csv(master_csv_path, index=False)
    else:
        # Create new master file
        df.to_csv(master_csv_path, index=False)
    
    return True

def read_latest_simplex_etf():
    """Read the latest Simplex ETF 318A data and extract VIX futures prices."""
    try:
        start_time = time.time()
        logger.info("Reading latest Simplex ETF 318A data")
        
        # Look for both original format and new PCF format files
        file_patterns = [
            os.path.join(SAVE_DIR, "318A-*.csv"),  # Original format from download_etf_csv.py
            os.path.join(SAVE_DIR, "318A*.csv")    # PCF format (more general pattern)
        ]
        
        all_files = []
        for pattern in file_patterns:
            all_files.extend(glob.glob(pattern))
        
        # Remove duplicates
        files = list(set(all_files))
        
        # If we don't find the files, try running the ETF downloader directly
        if not files:
            logger.warning("No Simplex ETF 318A files found. Attempting to run download_etf_csv.py")
            try:
                import subprocess
                logger.debug("Executing download_etf_csv.py")
                result = subprocess.run(["python", "download_etf_csv.py"], 
                                       capture_output=True, text=True, timeout=60)
                logger.debug(f"download_etf_csv.py stdout: {result.stdout}")
                logger.debug(f"download_etf_csv.py stderr: {result.stderr}")
                logger.debug(f"download_etf_csv.py return code: {result.returncode}")
                
                # Check if the script ran successfully
                if result.returncode == 0:
                    # Try to find the files again
                    for pattern in file_patterns:
                        all_files.extend(glob.glob(pattern))
                    files = list(set(all_files))  # Remove duplicates
                    
                    if not files:
                        logger.warning("Script ran successfully but no 318A files found")
                        return None
                else:
                    logger.error(f"Failed to run download_etf_csv.py: {result.stderr}")
                    return None
            except Exception as e:
                logger.error(f"Error running download_etf_csv.py: {str(e)}")
                logger.error(traceback.format_exc())
                return None
        
        # If we still don't have files, give up
        if not files:
            logger.warning("No Simplex ETF 318A files found even after trying to run download_etf_csv.py")
            return None
            
        # Get the most recent file based on the timestamp in the filename
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest Simplex ETF file: {latest_file} (modified: {datetime.fromtimestamp(os.path.getmtime(latest_file))})")
        
        # First, try to read the file structure to determine if it's a PCF or regular format
        with open(latest_file, 'r', encoding='utf-8') as f:
            preview_lines = [line.strip() for line in f.readlines()[:20]]
            
        logger.debug(f"File preview (first 20 lines or less):")
        for i, line in enumerate(preview_lines):
            logger.debug(f"Line {i+1}: {line[:100]}..." if len(line) > 100 else f"Line {i+1}: {line}")
        
        # Check if this is a PCF format file (header followed by holdings sections)
        is_pcf_format = False
        for line in preview_lines:
            if "ETF Code" in line and "ETF Name" in line:
                is_pcf_format = True
                logger.info("Detected PCF format file with header information")
                break
                
        # Specific PCF parsing for the Simplex ETF 318A format
        if is_pcf_format:
            # Try to find the code/name/stock price line
            header_row = None
            data_start_row = None
            
            for i, line in enumerate(preview_lines):
                if "Code,Name" in line or "Code,Name  " in line:
                    header_row = i
                    data_start_row = i + 1
                    logger.info(f"Found holdings header at line {header_row+1}")
                    break
            
            if header_row is not None:
                try:
                    # Read the PCF file starting at the header row
                    holdings_df = pd.read_csv(latest_file, skiprows=header_row)
                    logger.debug(f"Holdings dataframe: {holdings_df.shape}, columns: {holdings_df.columns.tolist()}")
                    
                    # Look for VIX futures in the data
                    futures_data = {
                        'date': datetime.now().strftime("%Y-%m-%d"),
                        'timestamp': datetime.now().strftime("%Y%m%d%H%M")
                    }
                    
                    # Find the name and price columns
                    desc_col = None
                    price_col = None
                    
                    # Expected column names in PCF format
                    for col in holdings_df.columns:
                        if col.lower() in ['name', 'name  ']:
                            desc_col = col
                        if col.lower() in ['stock price', 'price']:
                            price_col = col
                    
                    # Check if we still need to find columns
                    if desc_col is None:
                        # Try the second column as name
                        if len(holdings_df.columns) > 1:
                            desc_col = holdings_df.columns[1]
                    
                    if price_col is None:
                        # Try the last column as price
                        if len(holdings_df.columns) > 2:
                            price_col = holdings_df.columns[-1]
                    
                    logger.info(f"Using columns: Description='{desc_col}', Price='{price_col}'")
                    
                    if desc_col is not None and price_col is not None:
                        # Process each row
                        futures_found = 0
                        vix_pattern = re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE)
                        
                        for _, row in holdings_df.iterrows():
                            if pd.isna(row[desc_col]):
                                continue
                                
                            desc = str(row[desc_col])
                            match = vix_pattern.search(desc)
                            
                            if match:
                                # Extract month/year from the code (e.g., 2503 -> 03/25)
                                code = match.group(1)
                                
                                # If it's a 4-digit code, first two digits are year, second two are month
                                if len(code) == 4:
                                    year = int(code[:2])
                                    month = int(code[2:])
                                    
                                    # Map month number to VIX futures month code
                                    month_map = {
                                        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                                        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                                    }
                                    
                                    if month in month_map:
                                        month_letter = month_map[month]
                                        vix_future = f"VX{month_letter}{year}"
                                        
                                        # Also store in SIMPLEX:VX format
                                        simplex_ticker = f"SIMPLEX:{vix_future}"
                                        
                                        try:
                                            price = float(row[price_col])
                                            futures_data[simplex_ticker] = price
                                            futures_data[f"/{vix_future}"] = price  # Also in /VX format for compatibility
                                            futures_found += 1
                                            logger.info(f"Extracted from PCF: {vix_future} = {price} (from {desc})")
                                        except (ValueError, TypeError) as e:
                                            logger.warning(f"Could not convert price '{row[price_col]}' to float: {str(e)}")
                        
                        if futures_found > 0:
                            logger.info(f"Successfully extracted {futures_found} VIX futures from PCF file")
                            return futures_data
                    else:
                        logger.warning("Could not identify name or price columns in PCF file")
                except Exception as e:
                    logger.error(f"Error processing PCF file: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.warning("Could not find holdings header in PCF file")
        
        # If PCF format failed or wasn't detected, try standard CSV parsing
        try:
            df = pd.read_csv(latest_file)
            logger.debug(f"Standard CSV parse: {df.shape}, columns: {df.columns.tolist()}")
            
            # If the file is empty, try alternative approaches
            if df.empty or len(df.columns) < 2:
                logger.warning(f"CSV file appears to be empty or inadequate: {latest_file}")
                return None
            
            # Try to identify possible description and price columns
            desc_columns = ['Description', 'Security Description', 'Name', 'Security Name', 
                          'Asset', 'Asset Description', 'Holding', 'Holding Name', 'Security']
            
            price_columns = ['Price', 'Market Price', 'Close', 'Market Value', 'Value',
                           'Last Price', 'Settlement Price', 'Settlement', 'Closing Price']
            
            # Find the description column
            desc_col = None
            for col in desc_columns:
                if col in df.columns:
                    desc_col = col
                    break
            
            # Find the price column
            price_col = None
            for col in price_columns:
                if col in df.columns:
                    price_col = col
                    break
            
            if desc_col and price_col:
                # Process the standard CSV
                futures_data = {
                    'date': datetime.now().strftime("%Y-%m-%d"),
                    'timestamp': datetime.now().strftime("%Y%m%d%H%M")
                }
                
                # Look for VIX futures in the holdings
                futures_found = 0
                vix_pattern = re.compile(r'VX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE)
                
                for _, row in df.iterrows():
                    if pd.isna(row[desc_col]):
                        continue
                        
                    desc = str(row[desc_col])
                    match = vix_pattern.search(desc)
                    
                    if match:
                        month = match.group(1).upper()
                        year = match.group(2)
                        
                        # Map month codes to letters
                        month_map = {
                            'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                            'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                            'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                        }
                        
                        if month in month_map:
                            # Format ticker like SIMPLEX:VXH5
                            ticker = f"SIMPLEX:VX{month_map[month]}{year[-1]}"
                            standard_ticker = f"/VX{month_map[month]}{year[-1]}"
                            
                            try:
                                price_val = row[price_col]
                                if isinstance(price_val, str):
                                    price_val = price_val.strip()
                                    price_val = price_val.replace("$", "")
                                    price_val = price_val.replace(",", "")
                                
                                price = float(price_val)
                                futures_data[ticker] = price
                                futures_data[standard_ticker] = price
                                futures_found += 1
                                logger.info(f"Extracted from standard CSV: {ticker} = {price} (from {desc})")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Could not convert price '{row[price_col]}' to float: {str(e)}")
                
                if futures_found > 0:
                    logger.info(f"Successfully extracted {futures_found} VIX futures from standard CSV")
                    return futures_data
            
        except Exception as e:
            logger.error(f"Error processing the file: {str(e)}")
            logger.error(traceback.format_exc())
        
        # If all methods failed, try direct text parsing as last resort
        logger.info("Attempting manual text parsing of file content")
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                file_content = f.readlines()
            
            futures_data = {
                'date': datetime.now().strftime("%Y-%m-%d"),
                'timestamp': datetime.now().strftime("%Y%m%d%H%M")
            }
            
            # Look for patterns
            futures_found = 0
            
            # Pattern for PCF format (CBOEVIX 2503)
            cboevix_pattern = re.compile(r'(\d+),CBOEVIX\s*(\d+)[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,(\d+\.\d+)')
            
            for line in file_content:
                match = cboevix_pattern.search(line)
                if match:
                    code = match.group(2)
                    price_str = match.group(3)
                    
                    # Parse year/month from code
                    if len(code) == 4:
                        year = int(code[:2])
                        month = int(code[2:])
                        
                        # Map month number to VIX futures month code
                        month_map = {
                            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                        }
                        
                        if month in month_map:
                            month_letter = month_map[month]
                            vix_future = f"VX{month_letter}{year}"
                            
                            # Clean and convert price
                            price_str = price_str.strip().replace("$", "").replace(",", "")
                            price = float(price_str)
                            
                            # Store with both formats
                            simplex_ticker = f"SIMPLEX:{vix_future}"
                            std_ticker = f"/{vix_future}"
                            
                            futures_data[simplex_ticker] = price
                            futures_data[std_ticker] = price
                            futures_found += 1
                            logger.info(f"Extracted via text parsing: {vix_future} = {price}")
            
            if futures_found > 0:
                logger.info(f"Successfully extracted {futures_found} VIX futures via text parsing")
                return futures_data
            
        except Exception as e:
            logger.error(f"Error in manual text parsing: {str(e)}")
            logger.error(traceback.format_exc())
        
        # If all methods failed
        logger.warning("All parsing methods failed. No VIX futures extracted from Simplex file.")
        return None
            
    except Exception as e:
        logger.error(f"Error reading Simplex ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def download_vix_futures_from_cboe():
    """Download VIX futures data directly from CBOE website using the new URL"""
    start_time = time.time()
    try:
        logger.info("Downloading VIX futures data from CBOE...")
        
        # CBOE VIX futures page (new URL)
        url = "https://www.cboe.com/tradable_products/vix/vix_futures/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            log_response_details(response, "CBOE")
            
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
        
        # Logging the document structure to help with debugging
        logger.debug(f"CBOE HTML title: {soup.title.string if soup.title else 'No title'}")
        
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
        
        # Approach 3: Look for JSON data in script tags
        scripts = soup.find_all('script')
        logger.debug(f"Found {len(scripts)} script tags to analyze")
        
        for i, script in enumerate(scripts):
            try:
                script_text = script.string if script.string else ""
                
                # Skip empty scripts
                if not script_text:
                    continue
                
                # Look for potential JSON data
                json_pattern = re.compile(r'({[^{]*?"(?:vx|future)"[^}]*?})', re.IGNORECASE)
                
                json_matches = json_pattern.findall(script_text)
                if json_matches:
                    logger.debug(f"Found potential JSON data in script {i+1}")
                    
                    for json_str in json_matches:
                        try:
                            # Try to parse as JSON
                            from json import loads
                            data = loads(json_str)
                            
                            # If successful, look for contract and price info
                            if isinstance(data, dict):
                                logger.debug(f"Successfully parsed JSON: {data}")
                        except:
                            # If not valid JSON, continue with regex
                            pass
                
                # Fallback to regex for script content
                vx_pattern = re.compile(r'VX([A-Z])(\d{1,2}).*?(\d+\.\d+)', re.IGNORECASE)
                matches = vx_pattern.findall(script_text)
                
                for match in matches:
                    month_code, year, price_str = match
                    year_digit = year[-1]
                    
                    # Try to parse price
                    try:
                        price = float(price_str)
                        
                        # Store with both formats
                        cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                        std_ticker = f"/VX{month_code}{year_digit}"
                        
                        futures_data[cboe_ticker] = price
                        futures_data[std_ticker] = price
                        logger.info(f"CBOE script: {cboe_ticker} = {price}")
                    except ValueError:
                        logger.debug(f"Could not convert {price_str} to float in script {i+1}")
            except Exception as e:
                logger.warning(f"Error processing script {i+1}: {str(e)}")
        
        # Approach 4: Fall back to whole page text parsing
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

def download_vix_futures_from_yfinance():
    """Download VIX futures data from Yahoo Finance using updated ticker patterns"""
    start_time = time.time()
    try:
        logger.info("Downloading VIX futures data from Yahoo Finance...")
        
        # Get current date
        current_date = datetime.now()
        
        futures_data = {
            'date': current_date.strftime("%Y-%m-%d"),
            'timestamp': current_date.strftime("%Y%m%d%H%M")
        }
        
        # Get the VIX index price as a fallback for the front month
        try:
            vix_data = yf.download("^VIX", period="1d", progress=False)
            if not vix_data.empty and 'Close' in vix_data.columns:
                # Extract the numeric value properly
                vix_value = float(vix_data['Close'].iloc[-1])
                futures_data['VX=F'] = vix_value
                futures_data['YAHOO:VIX'] = vix_value  # Standardized format
                logger.info(f"Yahoo: ^VIX = {vix_value}")
            else:
                logger.warning("Could not download ^VIX data from Yahoo Finance")
        except Exception as e:
            logger.error(f"Error downloading ^VIX: {str(e)}")
        
        # Try different ticker patterns for VIX futures
        # VIX futures patterns to try:
        # 1. ^VFTW1, ^VFTW2, etc.
        # 2. ^VXIND1, ^VXIND2, etc.
        vix_patterns = [
            ['^VFTW1', '^VFTW2', '^VFTW3'],
            ['^VXIND1', '^VXIND2', '^VXIND3']
        ]
        
        # Map the positions to VIX ticker format
        month_map = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
        current_month = current_date.month
        year_suffix = str(current_date.year)[-1]
        
        # Try to map the positions to actual contract months
        position_to_contract = {}
        for i in range(1, 4):
            month_idx = (current_month + i - 1) % 12 + 1  # 1-indexed month
            if month_idx <= current_month:
                next_year_suffix = str(current_date.year + 1)[-1]
                position_to_contract[i] = f"/VX{month_map[month_idx]}{next_year_suffix}"
            else:
                position_to_contract[i] = f"/VX{month_map[month_idx]}{year_suffix}"
        
        logger.debug(f"Position to contract mapping: {position_to_contract}")
        
        futures_found = False
        
        # Try each of the ticker patterns
        for pattern_group in vix_patterns:
            logger.info(f"Trying ticker pattern group: {pattern_group}")
            
            for i, ticker in enumerate(pattern_group, 1):
                try:
                    logger.debug(f"Downloading {ticker} from Yahoo Finance")
                    data = yf.download(ticker, period="1d", progress=False)
                    
                    if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                        # Extract numeric value properly
                        settlement_price = float(data['Close'].iloc[-1])
                        
                        # Map to the corresponding VIX contract ticker format
                        if i in position_to_contract:
                            # Standard format
                            contract_ticker = position_to_contract[i]
                            # Get the month code and year digit from the standard format
                            month_code = contract_ticker[3:4]  # Extract 'H' from '/VXH5'
                            year_digit = contract_ticker[4:5]  # Extract '5' from '/VXH5'
                            
                            # Store with standard format
                            futures_data[contract_ticker] = settlement_price
                            
                            # Store with the standardized source prefix
                            yahoo_ticker = f"YAHOO:VX{month_code}{year_digit}"
                            futures_data[yahoo_ticker] = settlement_price
                            
                            logger.info(f"Yahoo: {ticker} → {yahoo_ticker} = {settlement_price}")
                            futures_found = True
                        
                        # Also store with the original Yahoo ticker without a series structure
                        futures_data[f"YAHOO:{ticker}"] = settlement_price
                    else:
                        logger.warning(f"No data found for ticker {ticker}")
                except Exception as e:
                    logger.warning(f"Error downloading {ticker} from Yahoo: {str(e)}")
                    # Try downloading with a different period or interval
                    try:
                        logger.debug(f"Retrying {ticker} with different parameters")
                        data = yf.download(ticker, period="5d", interval="1d", progress=False)
                        
                        if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                            # Extract numeric value properly
                            settlement_price = float(data['Close'].iloc[-1])
                            
                            # Map to the corresponding VIX contract ticker format
                            if i in position_to_contract:
                                # Standard format
                                contract_ticker = position_to_contract[i]
                                # Get the month code and year digit from the standard format
                                month_code = contract_ticker[3:4]  # Extract 'H' from '/VXH5'
                                year_digit = contract_ticker[4:5]  # Extract '5' from '/VXH5'
                                
                                # Store with standard format
                                futures_data[contract_ticker] = settlement_price
                                
                                # Store with the standardized source prefix
                                yahoo_ticker = f"YAHOO:VX{month_code}{year_digit}"
                                futures_data[yahoo_ticker] = settlement_price
                                
                                logger.info(f"Yahoo (retry): {ticker} → {yahoo_ticker} = {settlement_price}")
                                futures_found = True
                            
                            # Also store with the original Yahoo ticker
                            futures_data[f"YAHOO:{ticker}"] = settlement_price
                    except Exception as retry_e:
                        logger.warning(f"Retry for {ticker} also failed: {str(retry_e)}")
        
        if futures_found:
            logger.info(f"Successfully retrieved VIX futures data from Yahoo Finance (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            # As a fallback, try the original contract notation
            logger.debug("Trying direct contract notation as a fallback")
            
            futures_tickers = [position_to_contract[i] for i in range(1, 4) if i in position_to_contract]
            
            for ticker in futures_tickers:
                try:
                    data = yf.download(ticker, period="1d", progress=False)
                    
                    if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                        # Extract numeric value properly
                        settlement_price = float(data['Close'].iloc[-1])
                        
                        # Standard format (already in the correct format)
                        futures_data[ticker] = settlement_price
                        
                        # Get the month code and year digit from the standard format
                        month_code = ticker[3:4]  # Extract 'H' from '/VXH5'
                        year_digit = ticker[4:5]  # Extract '5' from '/VXH5'
                        
                        # Store with the standardized source prefix
                        yahoo_ticker = f"YAHOO:VX{month_code}{year_digit}"
                        futures_data[yahoo_ticker] = settlement_price
                        
                        logger.info(f"Yahoo (direct): {ticker} → {yahoo_ticker} = {settlement_price}")
                        futures_found = True
                except Exception as e:
                    logger.warning(f"Error downloading {ticker} directly: {str(e)}")
            
            if futures_found:
                logger.info(f"Successfully retrieved some VIX futures using direct contract notation (processing took {time.time() - start_time:.2f}s)")
                return futures_data
            else:
                logger.warning("Could not retrieve any VIX futures data from Yahoo Finance")
                return futures_data  # Return with just the VIX index if available
    
    except Exception as e:
        logger.error(f"Error in Yahoo Finance download: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def download_vix_futures():
    """Download VIX futures data from all sources and combine them"""
    overall_start_time = time.time()
    
    # Download from all sources
    cboe_data = download_vix_futures_from_cboe()
    yfinance_data = download_vix_futures_from_yfinance()
    
    # Try to get Simplex ETF data
    logger.info("Attempting to get Simplex ETF data")
    simplex_data = read_latest_simplex_etf()
    
    # Log summary of results from each source
    logger.info("=== SOURCE SUMMARY ===")
    if cboe_data:
        cboe_contracts = [k for k in cboe_data.keys() if k not in ['date', 'timestamp']]
        logger.info(f"CBOE: Found {len(cboe_contracts)} contracts: {', '.join(cboe_contracts)}")
    else:
        logger.info("CBOE: No data retrieved")
    
    if yfinance_data:
        yf_contracts = [k for k in yfinance_data.keys() if k not in ['date', 'timestamp']]
        logger.info(f"Yahoo Finance: Found {len(yf_contracts)} contracts/tickers: {', '.join(yf_contracts)}")
    else:
        logger.info("Yahoo Finance: No data retrieved")
    
    if simplex_data:
        simplex_contracts = [k for k in simplex_data.keys() if k not in ['date', 'timestamp']]
        logger.info(f"Simplex ETF: Found {len(simplex_contracts)} contracts: {', '.join(simplex_contracts)}")
    else:
        logger.info("Simplex ETF: No data retrieved")
    
    logger.info("=====================")
    
    if cboe_data or yfinance_data or simplex_data:
        # Format data into new structure
        df = format_vix_data_for_output(cboe_data, yfinance_data, simplex_data)
        
        if not df.empty:
            # Save the data
            save_success = save_vix_data(df, SAVE_DIR)
            
            if save_success:
                # Log results
                logger.info(f"Saved VIX futures data with {len(df)} price records")
                logger.info("Data sample:")
                logger.info(df.head(10).to_string())
                
                # Log any duplicate prices for the same future to highlight discrepancies
                futures = df['vix_future'].unique()
                for future in futures:
                    future_df = df[df['vix_future'] == future]
                    if len(future_df) > 1:
                        prices = future_df['price'].tolist()
                        max_diff = max(prices) - min(prices)
                        if max_diff > 0.1:  # Threshold for significant difference
                            logger.warning(f"Price discrepancy for {future}: max diff = {max_diff:.4f}")
                            logger.warning(future_df[['source', 'symbol', 'price']].to_string())
                
                logger.info(f"Total processing time: {time.time() - overall_start_time:.2f}s")
                return True
            else:
                logger.warning("Failed to save VIX futures data")
                return False
        else:
            logger.warning("No valid price data collected. Not saving empty files.")
            return False
    else:
        logger.error("Failed to get data from all sources.")
        return False

if __name__ == "__main__":
    logger.info("Starting VIX futures download process")
    success = download_vix_futures()
    if not success:
        logger.error("Script completed with errors.")
        sys.exit(1)  # Exit with error code
    else:
        logger.info("Script completed successfully.")
