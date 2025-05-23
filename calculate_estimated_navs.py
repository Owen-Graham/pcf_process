import os
import glob
import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging
import sys
import traceback

from common import normalize_vix_ticker, find_latest_file

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

def read_latest_file(pattern, default_cols=None):
    """
    Read the latest file matching the pattern
    
    Args:
        pattern: File pattern to match (e.g., "vix_futures_*.csv")
        default_cols: Default column list if file doesn't exist
    
    Returns:
        pandas.DataFrame: DataFrame with the file contents or None if file not found or error
    """
    latest_file = find_latest_file(pattern, directory=DATA_DIR)
    
    if not latest_file:
        logger.error(f"No file found matching pattern: {pattern} in directory {DATA_DIR}")
        return None
    
    try:
        logger.info(f"Reading file: {latest_file}")
        df = pd.read_csv(latest_file)
        
        if df.empty:
            logger.error(f"File {latest_file} is empty")
            return None
            
        return df
    except Exception as e:
        logger.error(f"Error reading file {latest_file}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

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

def fix_etf_characteristic_files():
    """
    Rename columns in ETF characteristics files from near_future_code to near_future
    and far_future_code to far_future
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Find all ETF characteristics files
        etf_files = glob.glob(os.path.join(DATA_DIR, "etf_characteristics_*.csv"))
        
        if not etf_files:
            logger.error("No ETF characteristic files found")
            return False
        
        fixed_count = 0
        for file_path in etf_files:
            try:
                # Read the file
                df = pd.read_csv(file_path)
                
                if df.empty:
                    logger.warning(f"Empty ETF characteristics file: {file_path}")
                    continue
                
                file_changed = False
                
                # Check if the file has the old column names
                if 'near_future_code' in df.columns and 'near_future' not in df.columns:
                    df = df.rename(columns={'near_future_code': 'near_future'})
                    logger.info(f"Renamed near_future_code to near_future in {file_path}")
                    file_changed = True
                
                if 'far_future_code' in df.columns and 'far_future' not in df.columns:
                    df = df.rename(columns={'far_future_code': 'far_future'})
                    logger.info(f"Renamed far_future_code to far_future in {file_path}")
                    file_changed = True
                
                # Only save if changes were made
                if file_changed:
                    # Save the file with the new column names
                    df.to_csv(file_path, index=False)
                    logger.info(f"Updated ETF characteristics file: {file_path}")
                    fixed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing ETF file {file_path}: {str(e)}")
                logger.error(traceback.format_exc())
        
        if fixed_count > 0:
            logger.info(f"Fixed {fixed_count} ETF characteristic files")
            return True
        else:
            logger.info("No ETF characteristic files needed fixing")
            return True
    
    except Exception as e:
        logger.error(f"Error fixing ETF characteristic files: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def calculate_estimated_nav():
    """
    Calculate estimated NAVs based on VIX futures, ETF characteristics, and FX data
    
    This function strictly validates all required data and will return None if any
    required data is missing or cannot be properly parsed.
    
    Returns:
        list: List of dictionaries with calculated NAV estimates for different FX rate types
              or None if any required data is missing or invalid
    """
    try:
        logger.info("Starting NAV calculations")
        
        # First, update ETF characteristics files to use the new column names
        if not fix_etf_characteristic_files():
            logger.error("Failed to fix ETF characteristic files")
            return None
        
        # List all available files for debugging
        file_count = list_all_data_files()
        if file_count == 0:
            logger.error("No files found in the data directory")
            return None
        
        # Read the latest data files
        vix_futures_df = read_latest_file("vix_futures_*.csv")
        etf_char_df = read_latest_file("etf_characteristics_*.csv")
        nav_data_df = read_latest_file("nav_data_*.csv")
        fx_data_df = read_latest_file("fx_data_*.csv")
        
        # Check if we have REQUIRED data (VIX futures and ETF characteristics)
        missing_data = []
        
        if vix_futures_df is None:
            missing_data.append("VIX futures data")
            
        if etf_char_df is None:
            missing_data.append("ETF characteristics data")
        
        if fx_data_df is None:
            missing_data.append("FX rate data")
        
        if missing_data:
            logger.error(f"Missing required data: {', '.join(missing_data)}")
            return None
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        
        # Get all available FX rates
        fx_rates = []
        try:
            # Filter to only USDJPY rates if pair column exists
            if 'pair' in fx_data_df.columns:
                usdjpy_df = fx_data_df[fx_data_df['pair'] == 'USDJPY']
                if usdjpy_df.empty:
                    logger.error("No USDJPY rates found in FX data")
                    return None
            else:
                # Assume all rows are USDJPY
                usdjpy_df = fx_data_df
            
            # Get all distinct rates with their labels
            if 'label' in usdjpy_df.columns and 'rate' in usdjpy_df.columns:
                for _, row in usdjpy_df.iterrows():
                    label = row['label'] if not pd.isna(row['label']) else "unknown"
                    rate = row['rate']
                    
                    if pd.isna(rate) or rate <= 0:
                        logger.warning(f"Skipping invalid FX rate: {label} = {rate}")
                        continue
                        
                    fx_rates.append({
                        'label': label,
                        'rate': rate
                    })
                    logger.info(f"Found FX Rate: {label} = {rate}")
            else:
                logger.error("FX data missing required columns: 'label' and/or 'rate'")
                return None
        except Exception as e:
            logger.error(f"Error extracting FX rates: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        
        # If no valid FX rates found, fail
        if not fx_rates:
            logger.error("No valid FX rates found. Cannot proceed with NAV calculation.")
            return None
        
        # Extract ETF characteristics - strictly check all required fields
        required_fields = ["shares_outstanding", "shares_amount_near_future", "shares_amount_far_future"]
        for field in required_fields:
            if field not in etf_char_df.columns:
                logger.error(f"Missing required field '{field}' in ETF characteristics data")
                return None
            if pd.isna(etf_char_df[field].iloc[0]):
                logger.error(f"Required field '{field}' has null/NA value in ETF characteristics data")
                return None
            
        try:
            shares_outstanding = etf_char_df["shares_outstanding"].iloc[0]
            shares_near = etf_char_df["shares_amount_near_future"].iloc[0]
            shares_far = etf_char_df["shares_amount_far_future"].iloc[0]
            
            # Validate numeric values
            if shares_outstanding <= 0:
                logger.error(f"Invalid shares_outstanding value: {shares_outstanding} (must be positive)")
                return None
            if shares_near <= 0:
                logger.error(f"Invalid shares_near value: {shares_near} (must be positive)")
                return None
            if shares_far <= 0:
                logger.error(f"Invalid shares_far value: {shares_far} (must be positive)")
                return None
            
            # Check for fund_cash_component - it's required
            if "fund_cash_component" not in etf_char_df.columns:
                logger.error("Missing required field 'fund_cash_component' in ETF characteristics data")
                return None
            if pd.isna(etf_char_df["fund_cash_component"].iloc[0]):
                logger.error("Required field 'fund_cash_component' has null/NA value")
                return None
                
            fund_cash = etf_char_df["fund_cash_component"].iloc[0]
            
            # Extract future codes - use new column names
            near_future = None
            far_future = None
            
            # Check for near_future
            if "near_future" in etf_char_df.columns and not pd.isna(etf_char_df["near_future"].iloc[0]):
                near_future = etf_char_df["near_future"].iloc[0]
            else:
                logger.error("Missing near_future in ETF characteristics data")
                return None
            
            # Check for far_future
            if "far_future" in etf_char_df.columns and not pd.isna(etf_char_df["far_future"].iloc[0]):
                far_future = etf_char_df["far_future"].iloc[0]
            else:
                logger.error("Missing far_future in ETF characteristics data")
                return None
            
            # Validate future codes are not empty strings
            if not near_future or len(str(near_future).strip()) == 0:
                logger.error("Invalid empty near_future code")
                return None
                
            if not far_future or len(str(far_future).strip()) == 0:
                logger.error("Invalid empty far_future code")
                return None
            
            logger.info(f"ETF Characteristics: shares_outstanding={shares_outstanding}, "
                       f"near_shares={shares_near} ({near_future}), "
                       f"far_shares={shares_far} ({far_future}), "
                       f"cash={fund_cash}")
                       
        except Exception as e:
            logger.error(f"Error extracting ETF characteristics: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        
        # Extract latest published NAV if available (optional)
        published_nav = None
        if nav_data_df is not None and not nav_data_df.empty:
            try:
                if "nav" in nav_data_df.columns and not pd.isna(nav_data_df["nav"].iloc[0]):
                    published_nav = nav_data_df["nav"].iloc[0]
                    logger.info(f"Latest published NAV: {published_nav}")
            except Exception as e:
                logger.warning(f"Error extracting published NAV (optional): {str(e)}")
        
        # Store all available contracts from VIX futures for reference
        all_contracts = {}
        for _, row in vix_futures_df.iterrows():
            if "vix_future" not in row or "price" not in row or "source" not in row:
                logger.warning("VIX futures data missing required columns")
                continue
                
            future_code = row['vix_future']
            price = row['price']
            source = row['source']
            
            if pd.isna(future_code) or pd.isna(price) or pd.isna(source):
                logger.warning(f"Skipping VIX future row with null values: {row}")
                continue
            
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
        
        # Look for near future price - fail if missing
        if not near_future:
            logger.error("Missing near future code in ETF characteristics data")
            return None
            
        normalized_near = normalize_vix_ticker(near_future)
        if normalized_near not in all_contracts:
            logger.error(f"Could not find price for near future {near_future}/{normalized_near}")
            return None
            
        # Prefer CBOE source if available, otherwise take the first one
        near_future_price = None
        near_future_price_source = None
        
        cboe_prices = [c for c in all_contracts[normalized_near] if c['source'] == 'CBOE']
        if cboe_prices:
            near_future_price = cboe_prices[0]['price']
            near_future_price_source = 'CBOE'
        else:
            # No CBOE source, use first available
            near_future_price = all_contracts[normalized_near][0]['price']
            near_future_price_source = all_contracts[normalized_near][0]['source']
        
        logger.info(f"Found price for near future {normalized_near}: {near_future_price} ({near_future_price_source})")
        
        # Look for far future price - fail if missing
        if not far_future:
            logger.error("Missing far future code in ETF characteristics data")
            return None
            
        normalized_far = normalize_vix_ticker(far_future)
        if normalized_far not in all_contracts:
            logger.error(f"Could not find price for far future {far_future}/{normalized_far}")
            return None
            
        # Prefer CBOE source if available, otherwise take the first one
        far_future_price = None
        far_future_price_source = None
        
        cboe_prices = [c for c in all_contracts[normalized_far] if c['source'] == 'CBOE']
        if cboe_prices:
            far_future_price = cboe_prices[0]['price']
            far_future_price_source = 'CBOE'
        else:
            # No CBOE source, use first available
            far_future_price = all_contracts[normalized_far][0]['price']
            far_future_price_source = all_contracts[normalized_far][0]['source']
        
        logger.info(f"Found price for far future {normalized_far}: {far_future_price} ({far_future_price_source})")
        
        # Create results for each FX rate type
        nav_results_list = []
        
        for fx_rate_info in fx_rates:
            fx_rate_label = fx_rate_info['label']
            fx_rate = fx_rate_info['rate']
            
            # Initialize a new results dictionary for this FX rate
            nav_results = {
                'timestamp': timestamp,
                'nav_date': datetime.now().strftime("%d/%m/%Y"),
                'shares_outstanding': shares_outstanding,
                'shares_near_future': shares_near,
                'shares_far_future': shares_far,
                'fund_cash_component': fund_cash,
                'near_future': near_future,
                'far_future': far_future,
                'usdjpy_rate': fx_rate,
                'usdjpy_rate_type': fx_rate_label
            }
            
            # Add published NAV if available
            if published_nav is not None:
                nav_results['published_nav'] = published_nav
            
            # Add futures prices and sources
            nav_results['near_future_price'] = near_future_price
            nav_results['near_future_price_source'] = near_future_price_source
            nav_results['far_future_price'] = far_future_price
            nav_results['far_future_price_source'] = far_future_price_source
            
            # Calculate estimated NAV for this FX rate
            try:
                # Calculate the total value in USD
                
                # Near future value
                near_value = near_future_price * shares_near * 1000  # Each VIX future is 1000 times the index
                logger.info(f"Near future value: {near_value:,.2f} USD")
                
                # Far future value
                far_value = far_future_price * shares_far * 1000  # Each VIX future is 1000 times the index
                logger.info(f"Far future value: {far_value:,.2f} USD")
                
                # Cash component
                logger.info(f"Cash component: {fund_cash:,.2f} USD")
                
                # Total USD value
                nav_usd = near_value + far_value + fund_cash
                
                # Store USD value
                nav_results['nav_usd'] = nav_usd
                
                # Convert to JPY
                nav_jpy = nav_usd * fx_rate
                
                # Calculate per share value
                nav_per_share = nav_jpy / shares_outstanding
                nav_results['estimated_nav'] = nav_jpy
                nav_results['estimated_nav_per_share'] = nav_per_share
                logger.info(f"Estimated NAV with {fx_rate_label}: {nav_per_share:,.2f} JPY per share")
                    
            except Exception as e:
                logger.error(f"Error calculating estimated NAV with {fx_rate_label}: {str(e)}")
                logger.error(traceback.format_exc())
                return None  # Fail if calculation errors occur
            
            # Add this result to the list
            nav_results_list.append(nav_results)
        
        return nav_results_list
    
    except Exception as e:
        logger.error(f"Error in calculate_estimated_nav: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def save_nav_results(nav_results_list):
    """
    Save NAV calculation results to CSV
    
    Args:
        nav_results_list: List of dictionaries with NAV calculation results
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not nav_results_list:
        logger.error("No NAV results to save")
        return False
    
    try:
        # Create DataFrame
        df = pd.DataFrame(nav_results_list)
        
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
        
        # Calculate NAV for different FX rate types
        nav_results_list = calculate_estimated_nav()
        
        # Save results
        if nav_results_list:
            save_success = save_nav_results(nav_results_list)
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
