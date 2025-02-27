import os
import pandas as pd
import traceback
import sys
import logging
from datetime import datetime
import time

# Import the individual downloaders
from cboe_vix_downloader import download_vix_futures_from_cboe
from yahoo_vix_downloader import download_vix_futures_from_yfinance
from pcf_vix_extractor import extract_vix_futures_from_pcf, find_latest_etf_file

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
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
            if key.startswith('SIMPLEX:VX') or key.startswith('PCF:VX'):
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

def save_vix_data(df, save_dir=SAVE_DIR):
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

def download_vix_futures():
    """Download VIX futures data from all sources and combine them"""
    overall_start_time = time.time()
    
    # Download from all sources
    cboe_data = download_vix_futures_from_cboe()
    yfinance_data = download_vix_futures_from_yfinance()
    
    # Try to get PCF data from the ETF files
    logger.info("Attempting to get VIX futures from PCF data")
    simplex_data = extract_vix_futures_from_pcf(find_latest_etf_file())
    
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
