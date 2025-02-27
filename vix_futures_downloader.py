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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join("data", "vix_downloader.log"))
    ]
)
logger = logging.getLogger('vix_futures_downloader')

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)

def read_latest_simplex_etf():
    """Read the latest Simplex ETF 318A data and extract VIX futures prices."""
    try:
        logger.info("Attempting to read latest Simplex ETF 318A data")
        
        # Find the most recent 318A file
        file_pattern = os.path.join(SAVE_DIR, "318A-*.csv")
        files = glob.glob(file_pattern)
        
        if not files:
            logger.warning("No Simplex ETF 318A files found")
            return None
        
        # Get the most recent file based on the timestamp in the filename
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest Simplex ETF file: {latest_file}")
        
        # Read the CSV file
        df = pd.read_csv(latest_file)
        
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
                break
        
        if not desc_col:
            logger.warning("Could not find description column in Simplex ETF file")
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
                break
        
        if not price_col:
            logger.warning("Could not find price column in Simplex ETF file")
            return None
        
        # Get current date
        date_str = datetime.now().strftime("%Y-%m-%d")
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M")
        
        futures_data = {
            'date': date_str,
            'timestamp': timestamp_str
        }
        
        # Extract VIX futures data
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
                    price = float(row[price_col])
                    futures_data[ticker] = price
                    logger.info(f"Extracted from Simplex ETF: {ticker} = {price}")
        
        if len(futures_data) > 2:  # More than just date and timestamp
            return futures_data
        else:
            logger.warning("No VIX futures found in Simplex ETF file")
            return None
            
    except Exception as e:
        logger.error(f"Error reading Simplex ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def download_vix_futures_from_cboe():
    """Download VIX futures data directly from CBOE website"""
    try:
        logger.info("Downloading VIX futures data from CBOE...")
        
        # CBOE VIX futures page
        url = "https://www.cboe.com/delayed_quotes/vx/quote_table"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the futures table
        table = soup.find('table', class_='table-quotes')
        
        if not table:
            logger.warning("Could not find VIX futures table on CBOE website")
            return None
        
        futures_data = {}
        # Get current date
        futures_data['date'] = datetime.now().strftime("%Y-%m-%d")
        futures_data['timestamp'] = datetime.now().strftime("%Y%m%d%H%M")
        
        # Get rows from table
        rows = table.find_all('tr')
        
        for row in rows[1:5]:  # Get first 4 contracts (skip header row)
            cells = row.find_all('td')
            if len(cells) >= 7:
                # Extract contract name and settlement price
                contract_name = cells[0].text.strip()
                settlement_price = cells[3].text.strip().replace('$', '').replace(',', '')
                
                # Format to match yfinance tickers
                if "VX" in contract_name and "/" in contract_name:
                    ticker = contract_name.replace(" ", "")
                    futures_data[ticker] = float(settlement_price)
                    logger.info(f"CBOE: {ticker} = {settlement_price}")
        
        return futures_data
    
    except Exception as e:
        logger.error(f"Error downloading from CBOE: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def download_vix_futures_from_yfinance():
    """Download VIX futures data from Yahoo Finance"""
    try:
        logger.info("Downloading VIX futures data from Yahoo Finance...")
        
        # Get current date
        current_date = datetime.now()
        
        futures_data = {
            'date': current_date.strftime("%Y-%m-%d"),
            'timestamp': current_date.strftime("%Y%m%d%H%M")
        }
        
        # Get the VIX index price as a fallback for the front month
        vix_data = yf.download("^VIX", period="1d", progress=False)
        if not vix_data.empty:
            futures_data['VX=F'] = vix_data['Close'].iloc[-1]
            logger.info(f"Yahoo: VX=F = {futures_data['VX=F']}")
        else:
            futures_data['VX=F'] = None
        
        # Fixed tickers for upcoming VIX futures contracts
        futures_tickers = ['/VXH5', '/VXJ5', '/VXK5']  # H=Mar, J=Apr, K=May 2025
        
        for ticker in futures_tickers:
            try:
                data = yf.download(ticker, period="1d", progress=False)
                
                if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                    settlement_price = data['Close'].iloc[-1]
                    futures_data[ticker] = settlement_price
                    logger.info(f"Yahoo: {ticker} = {settlement_price}")
                else:
                    futures_data[ticker] = None
            except Exception as e:
                logger.warning(f"Error downloading {ticker} from Yahoo: {str(e)}")
                futures_data[ticker] = None
        
        return futures_data
    
    except Exception as e:
        logger.error(f"Error in Yahoo Finance download: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def download_vix_futures():
    """Download VIX futures data from all sources and combine them"""
    
    # Download from all sources
    cboe_data = download_vix_futures_from_cboe()
    yfinance_data = download_vix_futures_from_yfinance()
    simplex_data = read_latest_simplex_etf()
    
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
                master_df = pd.read_csv(master_csv_path)
                # Check if we already have data for this date
                if not master_df.empty and 'date' in master_df.columns:
                    if df['date'].iloc[0] in master_df['date'].values:
                        logger.info(f"Data for {df['date'].iloc[0]} already exists in master file. Updating...")
                        # Remove existing entry for this date
                        master_df = master_df[master_df['date'] != df['date'].iloc[0]]
                
                combined_df = pd.concat([master_df, df], ignore_index=True)
                combined_df.to_csv(master_csv_path, index=False)
                logger.info(f"Updated master CSV file: {master_csv_path}")
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
