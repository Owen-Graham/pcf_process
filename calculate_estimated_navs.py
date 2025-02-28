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
                        label = row['label'] if not p
