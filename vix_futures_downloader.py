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
from common import MissingCriticalDataError, InvalidDataError # Ensure these are imported

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
    
    # Initialize price_date - we require this from one of the data sources
    price_date = None
    
    # Use the date from the data source
    if yfinance_data and 'date' in yfinance_data:
        price_date = yfinance_data['date']
        logger.info(f"Using Yahoo data date: {price_date}")
    elif cboe_data and 'date' in cboe_data:
        price_date = cboe_data['date']
        logger.info(f"Using CBOE data date: {price_date}")
    elif simplex_data and 'date' in simplex_data:
        price_date = simplex_data['date']
        logger.info(f"Using PCF data date: {price_date}")
    
    # If no valid date is found, we cannot proceed
    if not price_date:
        raise MissingCriticalDataError("No valid price_date could be determined from any data source (CBOE, Yahoo, PCF) for formatting VIX data.")
    
    # List to store all rows
    all_rows = []
    
    # Process CBOE data
    if cboe_data:
        # Get unique CBOE contracts (no duplicates)
        cboe_contracts = {}
        for key, value in cboe_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
            if value is None: # Check for None price value
                raise MissingCriticalDataError(f"Missing price for {key} in CBOE data during final formatting.")
                
            # Process CBOE tickers
            vix_future = None
            if key.startswith('CBOE:VX'):
                vix_future = key.split(':')[1]  # Extract VXH5 from CBOE:VXH5
                # For CBOE, we prefer the /VX format if available
                if f"/VX{vix_future[2:]}" in cboe_data:
                    continue  # Skip CBOE:VX format if /VX format exists
            elif key.startswith('/VX'):
                vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
            else:
                continue
                
            # Store the contract (prefer /VX format over CBOE:VX format)
            if vix_future not in cboe_contracts or key.startswith('/VX'):
                cboe_contracts[vix_future] = (key, float(value))
        
        # Add all unique CBOE contracts to rows
        for vix_future, (symbol, price) in cboe_contracts.items():
            all_rows.append({
                'timestamp': timestamp,
                'price_date': price_date,
                'vix_future': vix_future,
                'source': 'CBOE',
                'symbol': symbol,  # Original symbol from source
                'price': price
            })
    
    # Process Yahoo Finance data
    if yfinance_data:
        yahoo_contracts = {}
        for key, value in yfinance_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
            if value is None: # Check for None price value
                raise MissingCriticalDataError(f"Missing price for {key} in Yahoo data during final formatting.")
                
            # Determine vix_future
            vix_future = None
            if key.startswith('YAHOO:VX'):
                vix_future = key.split(':')[1]  # Extract VXH5 from YAHOO:VXH5
            elif key.startswith('/VX'):
                vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
            elif key in ['VX=F', 'YAHOO:VIX', '^VIX']:
                vix_future = 'VIX'
            # Skip index variants
            elif any(key.startswith(p) for p in ['^VFTW', 'YAHOO:^VFTW', '^VXIND', 'YAHOO:^VXIND']):
                continue
            else:
                continue
                
            # Store the contract (we prefer YAHOO: format over /VX format for Yahoo)
            if vix_future not in yahoo_contracts or key.startswith('YAHOO:'):
                yahoo_contracts[vix_future] = (key, float(value))
        
        # Add all unique Yahoo contracts to rows
        for vix_future, (symbol, price) in yahoo_contracts.items():
            all_rows.append({
                'timestamp': timestamp,
                'price_date': price_date,
                'vix_future': vix_future,
                'source': 'Yahoo',
                'symbol': symbol,  # Original symbol from source
                'price': price
            })
    
    # Process Simplex PCF data
    if simplex_data:
        pcf_contracts = {}
        for key, value in simplex_data.items():
            # Skip non-price fields
            if key in ['date', 'timestamp']:
                continue
            if value is None: # Check for None price value
                raise MissingCriticalDataError(f"Missing price for {key} in Simplex PCF data during final formatting.")
                
            # Determine vix_future
            vix_future = None
            if key.startswith('PCF:VX') or key.startswith('SIMPLEX:VX'):
                vix_future = key.split(':')[1]  # Extract VXH5 from PCF:VXH5
            elif key.startswith('/VX'):
                vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
            else:
                continue
                
            # Store the contract (prefer PCF: format over /VX format for PCF)
            if vix_future not in pcf_contracts or key.startswith('PCF:') or key.startswith('SIMPLEX:'):
                pcf_contracts[vix_future] = (key, float(value))
        
        # Add all unique PCF contracts to rows
        for vix_future, (symbol, price) in pcf_contracts.items():
            all_rows.append({
                'timestamp': timestamp,
                'price_date': price_date,
                'vix_future': vix_future,
                'source': 'PCF',
                'symbol': symbol,  # Original symbol from source
                'price': price
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
        raise InvalidDataError("Cannot save an empty VIX futures DataFrame.")
        
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
            
            # Get current timestamp
            current_timestamp = df['timestamp'].iloc[0]
            
            # Remove data with the same timestamp (to avoid duplicates from the same run)
            # This is important because re-runs might happen in development/testing
            master_df = master_df[master_df['timestamp'] != current_timestamp]
            
            # Append new data
            combined_df = pd.concat([master_df, df], ignore_index=True)
            combined_df.to_csv(master_csv_path, index=False)
        except Exception as e:
            logger.error(f"Error updating master CSV: {str(e)}") # Keep log for context
            raise InvalidDataError(f"Failed to write to master CSV '{master_csv_path}': {str(e)}") from e
    else:
        # Create new master file
        try:
        df.to_csv(master_csv_path, index=False)
        except Exception as e:
            logger.error(f"Error creating new master CSV: {str(e)}") # Keep log for context
            raise InvalidDataError(f"Failed to create new master CSV '{master_csv_path}': {str(e)}") from e
    
    return True

def download_vix_futures():
    """Download VIX futures data from all sources and combine them"""
    overall_start_time = time.time()
    cboe_data, yfinance_data, simplex_data = None, None, None # Initialize
    
    # Download from CBOE
    try:
        cboe_data = download_vix_futures_from_cboe()
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.warning(f"Failed to retrieve data from CBOE: {e}")
        cboe_data = None
    
    # Download from Yahoo Finance
    try:
        yfinance_data = download_vix_futures_from_yfinance()
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.warning(f"Failed to retrieve data from Yahoo Finance: {e}")
        yfinance_data = None
    
    # Try to get PCF data from the ETF files
    logger.info("Attempting to get VIX futures from PCF data")
    try:
        latest_etf_file = find_latest_etf_file() # find_latest_etf_file itself logs errors if no file
        if latest_etf_file:
            simplex_data = extract_vix_futures_from_pcf(latest_etf_file)
        else:
            # This case might be redundant if find_latest_etf_file raises an error or returns None and that's handled
            logger.warning("No ETF file found for PCF extraction by find_latest_etf_file.")
            simplex_data = None
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.warning(f"Failed to retrieve data from Simplex PCF: {e}")
        simplex_data = None
    
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
    
    # Check if we got data from any source
    if not cboe_data and not yfinance_data and not simplex_data:
        raise MissingCriticalDataError("Failed to retrieve VIX futures data from all available sources (CBOE, Yahoo, PCF).")
    
    # Format data into new structure
    df = format_vix_data_for_output(cboe_data, yfinance_data, simplex_data)
    
    if df.empty: # Should ideally be caught by format_vix_data_for_output raising error
        raise MissingCriticalDataError("Formatting VIX data resulted in an empty dataset, even though some sources might have provided data.")
    
    # Save the data
    save_vix_data(df, SAVE_DIR) # This will now raise an error on failure
    
    # Log results (if save_vix_data was successful)
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
    # Removed else block as save_vix_data now raises error

if __name__ == "__main__":
    logger.info("Starting VIX futures download process")
    try:
        success = download_vix_futures()
        if success: # Should be true if no exception from download_vix_futures
            logger.info("Script completed successfully.")
        else:
            # This case might be unreachable if download_vix_futures always raises or returns True
            logger.error("Script completed with an unspecified issue (returned False but no exception).")
            sys.exit(1)
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.error(f"VIX Futures Download Script failed: {e}")
        sys.exit(1)
    except Exception as e: # Catch any other unexpected errors at the top level
        logger.error(f"An unexpected error occurred in the main script: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
