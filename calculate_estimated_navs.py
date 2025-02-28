import os
import glob
import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging
import sys
import traceback

# Set up paths and logging
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "estimated_navs_calculator.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("estimated_navs_calculator")

def normalize_vix_ticker(ticker):
    """
    Normalize VIX futures ticker to standard format (VXH5 instead of VXH25)
    
    Args:
        ticker: VIX futures ticker (e.g., VXH25, VXH5)
    
    Returns:
        Normalized ticker (e.g., VXH5)
    """
    if not ticker or not isinstance(ticker, str):
        return ticker
        
    # If ticker is in format VX<letter><2-digit-year>
    match = re.match(r'(VX[A-Z])(\d{2})$', ticker)
    if match:
        prefix = match.group(1)
        year = match.group(2)
        # Take only the last digit of the year
        return f"{prefix}{year[-1]}"
    return ticker

def find_latest_file(pattern):
    """
    Find the latest file matching a pattern
    
    Args:
        pattern: File pattern to match (e.g., "vix_futures_*.csv")
    
    Returns:
        str: Path to the latest file or None if no files found
    """
    try:
        # Get full pattern path
        full_pattern = os.path.join(DATA_DIR, pattern)
        logger.info(f"Looking for files matching: {full_pattern}")
        
        # Use glob to find all matching files
        matching_files = glob.glob(full_pattern)
        
        # Log what was found
        if matching_files:
            logger.info(f"Found {len(matching_files)} matching files: {matching_files}")
        else:
            logger.warning(f"No files found matching pattern: {pattern}")
            return None
        
        # Sort by modification time (newest first)
        matching_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Return the latest file
        return matching_files[0]
    
    except Exception as e:
        logger.error(f"Error finding files with pattern {pattern}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def read_latest_file(pattern, default_cols=None):
    """
    Read the latest file matching the pattern
    
    Args:
        pattern: File pattern to match (e.g., "vix_futures_*.csv")
        default_cols: Default column list if file doesn't exist
    
    Returns:
        pandas.DataFrame: DataFrame with the file contents or empty DataFrame
    """
    latest_file = find_latest_file(pattern)
    
    if latest_file:
        try:
            logger.info(f"Reading file: {latest_file}")
            df = pd.read_csv(latest_file)
            return df
        except Exception as e:
            logger.error(f"Error reading file {latest_file}: {str(e)}")
            logger.error(traceback.format_exc())
    
    return pd.DataFrame(columns=default_cols if default_cols else [])

def list_all_data_files():
    """List all files in the data directory to help with debugging"""
    try:
        logger.info("Listing all files in data directory:")
        all_files = os.listdir(DATA_DIR)
        for file in all_files:
            file_path = os.path.join(DATA_DIR, file)
            file_size = os.path.getsize(file_path)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            logger.info(f"  {file} - Size: {file_size} bytes, Modified: {file_time}")
        
        logger.info(f"Total files found: {len(all_files)}")
        return len(all_files)
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return 0

def calculate_estimated_nav():
    """
    Calculate estimated NAVs based on VIX futures, ETF characteristics, and FX data
    
    Returns:
        dict: Dictionary with calculated NAV estimates
    """
    try:
        logger.info("Starting NAV calculations")
        
        # First, list all available files for debugging
        file_count = list_all_data_files()
        if file_count == 0:
            logger.error("No files found in the data directory")
            return None
        
        # Read the latest data files
        vix_futures_df = read_latest_file("vix_futures_*.csv", 
                                         ["timestamp", "price_date", "vix_future", "source", "symbol", "price"])
        
        etf_char_df = read_latest_file("etf_characteristics_*.csv",
                                      ["timestamp", "fund_date", "shares_outstanding", 
                                       "fund_cash_component", "shares_amount_near_future", 
                                       "shares_amount_far_future", "near_future_code", "far_future_code"])
        
        nav_data_df = read_latest_file("nav_data_*.csv",
                                      ["timestamp", "source", "fund_date", "nav"])
        
        fx_data_df = read_latest_file("fx_data_*.csv",
                                     ["timestamp", "date", "source", "pair", "label", "rate"])
        
        # Check if we have REQUIRED data (VIX futures and ETF characteristics)
        missing_data = []
        
        if vix_futures_df.empty:
            missing_data.append("VIX futures data")
            
        if etf_char_df.empty:
            missing_data.append("ETF characteristics data")
        
        if missing_data:
            logger.error(f"Missing required data: {', '.join(missing_data)}")
            return None
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        
        # Initialize results dictionary
        nav_results = {
            'timestamp': timestamp,
            'calculation_date': datetime.now().strftime("%Y-%m-%d")
        }
        
        # Extract ETF characteristics
        try:
            shares_outstanding = etf_char_df["shares_outstanding"].iloc[0]
            shares_near = etf_char_df["shares_amount_near_future"].iloc[0]
            shares_far = etf_char_df["shares_amount_far_future"].iloc[0]
            fund_cash = etf_char_df["fund_cash_component"].iloc[0] if "fund_cash_component" in etf_char_df.columns else 0
            
            # Extract future codes if available
            near_future_code = None
            far_future_code = None
            
            if "near_future_code" in etf_char_df.columns and not pd.isna(etf_char_df["near_future_code"].iloc[0]):
                near_future_code = etf_char_df["near_future_code"].iloc[0]
            
            if "far_future_code" in etf_char_df.columns and not pd.isna(etf_char_df["far_future_code"].iloc[0]):
                far_future_code = etf_char_df["far_future_code"].iloc[0]
            
            nav_results['shares_outstanding'] = shares_outstanding
            nav_results['shares_near_future'] = shares_near
            nav_results['shares_far_future'] = shares_far
            nav_results['fund_cash_component'] = fund_cash
            nav_results['near_future_code'] = near_future_code
            nav_results['far_future_code'] = far_future_code
            
            logger.info(f"ETF Characteristics: shares_outstanding={shares_outstanding}, "
                       f"near_shares={shares_near} ({near_future_code}), "
                       f"far_shares={shares_far} ({far_future_code}), "
                       f"cash={fund_cash}")
        except Exception as e:
            logger.error(f"Error extracting ETF characteristics: {str(e)}")
            return None
        
        # Extract latest published NAV if available (this is optional)
        if not nav_data_df.empty:
            try:
                latest_published_nav = nav_data_df["nav"].iloc[0]
                nav_results['published_nav'] = latest_published_nav
                logger.info(f"Latest published NAV: {latest_published_nav}")
            except Exception as e:
                logger.warning(f"Error extracting published NAV: {str(e)}")
        else:
            logger.warning("No published NAV data available (this is optional)")
        
        # Extract ALL USD/JPY exchange rates
        fx_rates = {}
        if not fx_data_df.empty:
            try:
                # Filter to only USDJPY rates if pair column exists
                if 'pair' in fx_data_df.columns:
                    usdjpy_df = fx_data_df[fx_data_df['pair'] == 'USDJPY']
                else:
                    usdjpy_df = fx_data_df  # Assume all rows are USDJPY
                
                # Get all distinct rates with their labels
                if 'label' in usdjpy_df.columns and 'rate' in usdjpy_df.columns:
                    for _, row in usdjpy_df.iterrows():
                        label = row['label'] if not pd.isna(row['label']) else "unknown"
                        rate = row['rate']
                        fx_rates[label] = rate
                        logger.info(f"FX Rate: {label} = {rate}")
                
                # If we have any FX rates, store the first one in the results
                if fx_rates:
                    # Use TTS rate if available (most conservative for the fund)
                    if "T.T.S." in fx_rates:
                        nav_results['usdjpy_rate'] = fx_rates["T.T.S."]
                        nav_results['usdjpy_rate_type'] = "T.T.S."
                    else:
                        # Otherwise use the first rate we found
                        first_key = next(iter(fx_rates.keys()))
                        nav_results['usdjpy_rate'] = fx_rates[first_key]
                        nav_results['usdjpy_rate_type'] = first_key
                    
                    logger.info(f"Using USD/JPY rate: {nav_results['usdjpy_rate']} ({nav_results['usdjpy_rate_type']})")
            except Exception as e:
                logger.warning(f"Error extracting FX rates: {str(e)}")
        else:
            logger.warning("No FX rate data available (this is optional)")
        
        # Find the futures prices matching our near and far future codes
        near_future_price = None
        far_future_price = None
        
        # Store all available contracts for reference
        all_contracts = {}
        for _, row in vix_futures_df.iterrows():
            future_code = row['vix_future']
            price = row['price']
            source = row['source']
            
            # Normalize the code to handle different formats (e.g., VXH25 -> VXH5)
            normalized_code = normalize_vix_ticker(future_code)
            
            # Store in all contracts dictionary
            if normalized_code not in all_contracts:
                all_contracts[normalized_code] = []
            
            all_contracts[normalized_code].append({
                'price': price,
                'source': source
            })
        
        logger.info(f"All available futures contracts: {list(all_contracts.keys())}")
        
        # Look for near future price
        if near_future_code:
            normalized_near = normalize_vix_ticker(near_future_code)
            if normalized_near in all_contracts:
                # Prefer CBOE source if available, otherwise take the first one
                cboe_prices = [c for c in all_contracts[normalized_near] if c['source'] == 'CBOE']
                if cboe_prices:
                    near_future_price = cboe_prices[0]['price']
                    nav_results['near_future_price_source'] = 'CBOE'
                else:
                    near_future_price = all_contracts[normalized_near][0]['price']
                    nav_results['near_future_price_source'] = all_contracts[normalized_near][0]['source']
                
                nav_results['near_future_price'] = near_future_price
                logger.info(f"Found price for near future {normalized_near}: {near_future_price} ({nav_results['near_future_price_source']})")
            else:
                logger.warning(f"Could not find price for near future {near_future_code}/{normalized_near}")
        else:
            logger.warning("No near future code specified")
        
        # Look for far future price
        if far_future_code:
            normalized_far = normalize_vix_ticker(far_future_code)
            if normalized_far in all_contracts:
                # Prefer CBOE source if available, otherwise take the first one
                cboe_prices = [c for c in all_contracts[normalized_far] if c['source'] == 'CBOE']
                if cboe_prices:
                    far_future_price = cboe_prices[0]['price']
                    nav_results['far_future_price_source'] = 'CBOE'
                else:
                    far_future_price = all_contracts[normalized_far][0]['price']
                    nav_results['far_future_price_source'] = all_contracts[normalized_far][0]['source']
                
                nav_results['far_future_price'] = far_future_price
                logger.info(f"Found price for far future {normalized_far}: {far_future_price} ({nav_results['far_future_price_source']})")
            else:
                logger.warning(f"Could not find price for far future {far_future_code}/{normalized_far}")
        else:
            logger.warning("No far future code specified")
        
        # Calculate estimated NAV
        if near_future_price is not None or far_future_price is not None:
            try:
                # Calculate the total value in USD
                nav_usd = 0
                
                # Add near future value if available
                if near_future_price is not None and shares_near > 0:
                    near_value = near_future_price * shares_near * 1000  # Each VIX future is 1000 times the index
                    nav_usd += near_value
                    logger.info(f"Near future value: {near_value:,.2f} USD")
                
                # Add far future value if available
                if far_future_price is not None and shares_far > 0:
                    far_value = far_future_price * shares_far * 1000  # Each VIX future is 1000 times the index
                    nav_usd += far_value
                    logger.info(f"Far future value: {far_value:,.2f} USD")
                
                # Add cash component
                nav_usd += fund_cash
                logger.info(f"Cash component: {fund_cash:,.2f} USD")
                
                # Store USD value
                nav_results['nav_usd'] = nav_usd
                
                # Convert to JPY if we have exchange rate
                if 'usdjpy_rate' in nav_results:
                    nav_jpy = nav_usd * nav_results['usdjpy_rate']
                    
                    # Calculate per share value
                    if shares_outstanding > 0:
                        nav_per_share = nav_jpy / shares_outstanding
                        nav_results['nav_jpy'] = nav_jpy
                        nav_results['estimated_nav_per_share'] = nav_per_share
                        logger.info(f"Estimated NAV: {nav_per_share:,.2f} JPY per share")
                    else:
                        logger.warning("Cannot calculate per-share NAV: shares outstanding is zero")
                else:
                    logger.warning("Cannot convert to JPY: no USD/JPY exchange rate available")
            except Exception as e:
                logger.error(f"Error calculating estimated NAV: {str(e)}")
                logger.error(traceback.format_exc())
        else:
            logger.warning("Cannot calculate estimated NAV: missing future prices")
        
        return nav_results
    
    except Exception as e:
        logger.error(f"Error in calculate_estimated_nav: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def save_nav_results(nav_results):
    """
    Save NAV calculation results to CSV
    
    Args:
        nav_results: Dictionary with NAV calculation results
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not nav_results:
        logger.warning("No NAV results to save")
        return False
    
    try:
        # Create DataFrame
        df = pd.DataFrame([nav_results])
        
        # Save to CSV
        csv_path = os.path.join(DATA_DIR, "estimated_navs.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved estimated NAVs to {csv_path}")
        
        return True
    
    except Exception as e:
        logger.error(f"Error saving NAV results: {str(e)}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    try:
        logger.info("Starting estimated NAV calculations")
        
        # Calculate NAV
        nav_results = calculate_estimated_nav()
        
        # Save results
        if nav_results:
            save_success = save_nav_results(nav_results)
            if save_success:
                logger.info("NAV calculations completed successfully")
            else:
                logger.error("Failed to save NAV results")
                sys.exit(1)
        else:
            logger.error("NAV calculation failed")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Unexpected error in NAV calculations: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)
