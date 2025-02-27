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
    level=logging.INFO,
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

def read_latest_simplex_etf():
    """Read the latest Simplex ETF 318A data and extract VIX futures prices."""
    try:
        start_time = time.time()
        logger.info("Reading latest Simplex ETF 318A data")
        
        # Find the most recent 318A file from the download_etf_csv.py output
        file_pattern = os.path.join(SAVE_DIR, "318A-*.csv")
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No Simplex ETF 318A files found. Make sure download_etf_csv.py has been run.")
            return None
        
        # Get the most recent file based on the timestamp in the filename
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest Simplex ETF file: {latest_file} (modified: {datetime.fromtimestamp(os.path.getmtime(latest_file))})")
        
        # Read the CSV file
        try:
            df = pd.read_csv(latest_file)
            logger.debug(f"Simplex ETF file shape: {df.shape}, columns: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Error reading Simplex ETF CSV: {str(e)}")
            logger.debug(f"File content preview: {open(latest_file, 'r').read(500)}")
            return None
        
        # Log the columns to help with debugging
        logger.debug(f"Columns in Simplex ETF file: {', '.join(df.columns)}")
        
        # Look for VIX futures in the holdings
        vix_data = {}
        vix_pattern = re.compile(r'VX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE)
        
        # Columns we might find the descriptions in
        desc_columns = ['Description', 'Security Description', 'Name', 'Security Name']
        
        # Find the description column
        desc_col = None
        for col in desc_columns:
            if col in df.columns:
                desc_col = col
                logger.debug(f"Using '{col}' as description column in Simplex ETF file")
                break
        
        if not desc_col:
            logger.warning(f"Could not find description column in Simplex ETF file. Available columns: {', '.join(df.columns)}")
            return None
        
        # Map month codes to letters
        month_map = {
            'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
            'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
            'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
        }
        
        # Columns we might find the price in
        price_columns = ['Price', 'Market Price', 'Close', 'Market Value']
        price_col = None
        for col in price_columns:
            if col in df.columns:
                price_col = col
                logger.debug(f"Using '{col}' as price column in Simplex ETF file")
                break
        
        if not price_col:
            logger.warning(f"Could not find price column in Simplex ETF file. Available columns: {', '.join(df.columns)}")
            return None
        
        # Get current date
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M")
        
        futures_data = {
            'date': date_str,
            'timestamp': timestamp_str
        }
        
        # Extract VIX futures data
        futures_found = 0
        for _, row in df.iterrows():
            if pd.isna(row[desc_col]):
                continue
                
            desc = str(row[desc_col])
            match = vix_pattern.search(desc)
            
            if match:
                month = match.group(1).upper()
                year = match.group(2)
                
                if month in month_map:
                    # Format ticker like /VXH5
                    ticker = f"/VX{month_map[month]}{year[-1]}"
                    try:
                        price = float(row[price_col])
                        futures_data[ticker] = price
                        futures_found += 1
                        logger.info(f"Extracted from Simplex ETF: {ticker} = {price} (from {desc})")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not convert price '{row[price_col]}' to float for {ticker}: {str(e)}")
        
        if futures_found > 0:
            logger.info(f"Successfully extracted {futures_found} VIX futures from Simplex ETF (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            logger.warning(f"No VIX futures found in Simplex ETF file. Check if the pattern matching is correct.")
            # Log a few sample descriptions to help with debugging
            sample_descriptions = df[desc_col].dropna().head(5).tolist()
            logger.debug(f"Sample descriptions from file: {sample_descriptions}")
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
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve CBOE page: {str(e)}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Logging the document structure to help with debugging
        logger.debug(f"CBOE HTML title: {soup.title.string if soup.title else 'No title'}")
        
        # Looking for the futures table on the new page
        # First try the standard table element
        futures_data = {}
        # Get current date
        futures_data['date'] = datetime.now().strftime("%Y-%m-%d")
        futures_data['timestamp'] = datetime.now().strftime("%Y%m%d%H%M")
        
        # Try to find the futures table
        # The table might be in various elements, so we'll try multiple approaches
        tables = soup.find_all('table')
        logger.debug(f"Found {len(tables)} tables on CBOE page")
        
        # Also look for div elements that might contain the data
        data_divs = soup.find_all('div', class_=lambda c: c and ('table' in c.lower() or 'grid' in c.lower() or 'data' in c.lower()))
        logger.debug(f"Found {len(data_divs)} potential data divs on CBOE page")
        
        # Check for iframe elements that might contain the data
        iframes = soup.find_all('iframe')
        if iframes:
            logger.debug(f"Found {len(iframes)} iframes on CBOE page. The data might be in an iframe.")
            for i, iframe in enumerate(iframes):
                logger.debug(f"Iframe {i+1} src: {iframe.get('src', 'No src attribute')}")
        
        # Look for script tags that might contain the futures data in JSON
        scripts = soup.find_all('script', type='application/json') + soup.find_all('script', type='text/javascript')
        data_scripts = []
        for script in scripts:
            script_text = script.string if script.string else ""
            if script_text and ('VIX' in script_text or 'futures' in script_text.lower() or 'vx' in script_text.lower()):
                data_scripts.append(script)
        
        logger.debug(f"Found {len(data_scripts)} script tags that might contain futures data")
        
        # If we found a script with potential data, try to extract it
        # This is a simplified example - actual extraction would depend on the structure
        futures_found = False
        for script in data_scripts:
            script_text = script.string if script.string else ""
            # Log a snippet to help with debugging
            logger.debug(f"Script content snippet (first 200 chars): {script_text[:200]}")
            
            # Very simplified example - look for patterns like "VX" followed by month code and price
            vix_pattern = re.compile(r'VX([A-Z])([\d]{1}).*?[":]*([\d\.]+)', re.IGNORECASE)
            matches = vix_pattern.findall(script_text)
            
            for match in matches:
                try:
                    month_code, year, price = match
                    ticker = f"/VX{month_code}{year}"
                    price = float(price)
                    futures_data[ticker] = price
                    futures_found = True
                    logger.info(f"CBOE: {ticker} = {price}")
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing potential futures data: {str(e)}")
        
        # If the above methods didn't work, try to find settlements in the page text
        if not futures_found:
            logger.debug("Didn't find structured futures data. Looking for text patterns...")
            
            # Look for text patterns like "VXH25: 19.325" or similar
            page_text = soup.get_text()
            vix_text_pattern = re.compile(r'VX([A-Z])(\d{2})[:\s]+(\d+\.\d+)', re.IGNORECASE)
            matches = vix_text_pattern.findall(page_text)
            
            for match in matches:
                try:
                    month_code, year, price = match
                    ticker = f"/VX{month_code}{year[-1]}"
                    price = float(price)
                    futures_data[ticker] = price
                    futures_found = True
                    logger.info(f"CBOE (text): {ticker} = {price}")
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing potential text futures data: {str(e)}")
        
        if len(futures_data) > 2:  # More than just date and timestamp
            logger.info(f"Successfully extracted futures data from CBOE (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            logger.warning("Could not extract VIX futures data from CBOE website")
            # Save HTML for debugging
            debug_html_path = os.path.join(SAVE_DIR, "cboe_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.debug(f"Saved CBOE HTML to {debug_html_path} for debugging")
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
                futures_data['VX=F'] = vix_data['Close'].iloc[-1]
                logger.info(f"Yahoo: ^VIX = {futures_data['VX=F']}")
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
        month_map = {1: 'H', 2: 'J', 3: 'K', 4: 'M', 5: 'N', 6: 'Q', 7: 'U', 8: 'V', 9: 'X', 10: 'Z', 11: 'F', 12: 'G'}
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
                        settlement_price = data['Close'].iloc[-1]
                        
                        # Map to the corresponding VIX contract ticker format
                        if i in position_to_contract:
                            contract_ticker = position_to_contract[i]
                            futures_data[contract_ticker] = settlement_price
                            logger.info(f"Yahoo: {ticker} → {contract_ticker} = {settlement_price}")
                            futures_found = True
                        
                        # Also store with the original Yahoo ticker
                        futures_data[ticker] = settlement_price
                    else:
                        logger.warning(f"No data found for ticker {ticker}")
                except Exception as e:
                    logger.warning(f"Error downloading {ticker} from Yahoo: {str(e)}")
                    # Try downloading with a different period or interval
                    try:
                        logger.debug(f"Retrying {ticker} with different parameters")
                        data = yf.download(ticker, period="5d", interval="1d", progress=False)
                        
                        if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                            settlement_price = data['Close'].iloc[-1]
                            
                            # Map to the corresponding VIX contract ticker format
                            if i in position_to_contract:
                                contract_ticker = position_to_contract[i]
                                futures_data[contract_ticker] = settlement_price
                                logger.info(f"Yahoo (retry): {ticker} → {contract_ticker} = {settlement_price}")
                                futures_found = True
                            
                            # Also store with the original Yahoo ticker
                            futures_data[ticker] = settlement_price
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
                        settlement_price = data['Close'].iloc[-1]
                        futures_data[ticker] = settlement_price
                        logger.info(f"Yahoo (direct): {ticker} = {settlement_price}")
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
    
    # Create combined data structure
    if cboe_data or yfinance_data or simplex_data:
        # Use first available source for date/timestamp
        result = {}
        for source_data in [cboe_data, yfinance_data, simplex_data]:
            if source_data:
                result['date'] = source_data['date']
                result['timestamp'] = source_data['timestamp']
                break
        
        # Combine all contract data with source information
        all_contracts = set()
        
        # Add data from CBOE
        if cboe_data:
            for key in cboe_data:
                if key not in ['date', 'timestamp'] and cboe_data[key] is not None:
                    all_contracts.add(key)
                    result[f"{key}_cboe"] = cboe_data[key]
        
        # Add data from Yahoo Finance
        if yfinance_data:
            for key in yfinance_data:
                if key not in ['date', 'timestamp'] and yfinance_data[key] is not None:
                    # Only add VIX futures contracts, not the Yahoo-specific tickers
                    if key.startswith('/VX') or key == 'VX=F':
                        all_contracts.add(key)
                    result[f"{key}_yahoo"] = yfinance_data[key]
        
        # Add data from Simplex ETF
        if simplex_data:
            for key in simplex_data:
                if key not in ['date', 'timestamp'] and simplex_data[key] is not None:
                    all_contracts.add(key)
                    result[f"{key}_simplex"] = simplex_data[key]
        
        # Create preferred value columns (prefer CBOE, then Simplex, then Yahoo)
        for contract in all_contracts:
            if f"{contract}_cboe" in result:
                result[contract] = result[f"{contract}_cboe"]
            elif f"{contract}_simplex" in result:
                result[contract] = result[f"{contract}_simplex"]
            elif f"{contract}_yahoo" in result:
                result[contract] = result[f"{contract}_yahoo"]
        
        # Create DataFrame
        df = pd.DataFrame([result])
        
        # Save daily snapshot
        current_datetime = datetime.now().strftime("%Y%m%d%H%M")
        csv_filename = f"vix_futures_{current_datetime}.csv"
        csv_path = os.path.join(SAVE_DIR, csv_filename)
        
        # Check if we have any actual price data
        price_columns = [col for col in df.columns if col not in ['date', 'timestamp'] 
                         and not (col.endswith('_cboe') or col.endswith('_yahoo') or col.endswith('_simplex'))]
        
        if df[price_columns].notna().any(axis=1).iloc[0]:
            # Update or create master CSV
            master_csv_path = os.path.join(SAVE_DIR, "vix_futures_master.csv")
            
            if os.path.exists(master_csv_path):
                try:
                    master_df = pd.read_csv(master_csv_path)
                    logger.debug(f"Read master CSV: {master_df.shape}")
                    
                    # Check if we already have data for this date
                    if not master_df.empty and 'date' in master_df.columns:
                        if df['date'].iloc[0] in master_df['date'].values:
                            logger.info(f"Data for {df['date'].iloc[0]} already exists in master file. Updating...")
                            # Remove existing entry for this date
                            master_df = master_df[master_df['date'] != df['date'].iloc[0]]
                    
                    combined_df = pd.concat([master_df, df], ignore_index=True)
                    combined_df.to_csv(master_csv_path, index=False)
                    logger.info(f"Updated master CSV file: {master_csv_path}")
                except Exception as e:
                    logger.error(f"Error updating master CSV: {str(e)}")
                    logger.error(traceback.format_exc())
                    # Fall back to creating a new file
                    df.to_csv(master_csv_path, index=False)
                    logger.info(f"Created new master CSV file: {master_csv_path}")
            else:
                df.to_csv(master_csv_path, index=False)
                logger.info(f"Created master CSV file: {master_csv_path}")
            
            # Save daily snapshot
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved daily snapshot to: {csv_path}")
            
            # Log the data we collected
            logger.info("Data collected:")
            logger.info(df[['date', 'timestamp'] + price_columns].to_string())
            
            # Calculate and log differences between sources
            if len(all_contracts) > 0:
                logger.info("Source comparison:")
                for contract in all_contracts:
                    sources = []
                    values = []
                    
                    if f"{contract}_cboe" in result:
                        sources.append("CBOE")
                        values.append(result[f"{contract}_cboe"])
                    
                    if f"{contract}_simplex" in result:
                        sources.append("Simplex")
                        values.append(result[f"{contract}_simplex"])
                    
                    if f"{contract}_yahoo" in result:
                        sources.append("Yahoo")
                        values.append(result[f"{contract}_yahoo"])
                    
                    if len(values) > 1:
                        max_diff = max(values) - min(values)
                        logger.info(f"{contract}: Max difference between sources: {max_diff:.4f}")
                        if max_diff > 1.0:
                            logger.warning(f"Large discrepancy (>{max_diff:.4f}) for {contract} across sources")
            
            logger.info(f"Total processing time: {time.time() - overall_start_time:.2f}s")
            return True
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
