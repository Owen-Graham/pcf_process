import os
import pandas as pd
import numpy as np
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

def read_latest_file(pattern, default_cols=None):
    """
    Read the latest file matching the pattern
    
    Args:
        pattern: File pattern to match (e.g., "vix_futures_*.csv")
        default_cols: Default column list if file doesn't exist
    
    Returns:
        pandas.DataFrame: DataFrame with the file contents or empty DataFrame
    """
    try:
        # Get all files matching the pattern
        files = [f for f in os.listdir(DATA_DIR) if f.startswith(pattern.replace("*", ""))]
        
        if not files:
            logger.warning(f"No files found matching pattern: {pattern}")
            return pd.DataFrame(columns=default_cols if default_cols else [])
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: os.path.getmtime(os.path.join(DATA_DIR, x)), reverse=True)
        
        # Get the latest file
        latest_file = os.path.join(DATA_DIR, files[0])
        logger.info(f"Reading latest file: {latest_file}")
        
        # Read the file
        df = pd.read_csv(latest_file)
        return df
    
    except Exception as e:
        logger.error(f"Error reading file with pattern {pattern}: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=default_cols if default_cols else [])

def calculate_estimated_nav():
    """
    Calculate estimated NAVs based on VIX futures, ETF characteristics, and FX data
    
    Returns:
        dict: Dictionary with calculated NAV estimates
    """
    try:
        logger.info("Starting NAV calculations")
        
        # Read the latest data files
        vix_futures_df = read_latest_file("vix_futures_*.csv", 
                                         ["timestamp", "price_date", "vix_future", "source", "symbol", "price"])
        
        etf_char_df = read_latest_file("etf_characteristics_*.csv",
                                      ["timestamp", "fund_date", "shares_outstanding", 
                                       "fund_cash_component", "shares_amount_near_future", 
                                       "shares_amount_far_future"])
        
        nav_data_df = read_latest_file("nav_data_*.csv",
                                      ["timestamp", "source", "fund_date", "nav"])
        
        fx_data_df = read_latest_file("fx_data_*.csv",
                                     ["timestamp", "date", "source", "pair", "label", "rate"])
        
        # Check if we have all required data
        if vix_futures_df.empty:
            logger.error("No VIX futures data available")
            return None
            
        if etf_char_df.empty:
            logger.error("No ETF characteristics data available")
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
            
            nav_results['shares_outstanding'] = shares_outstanding
            nav_results['shares_near_future'] = shares_near
            nav_results['shares_far_future'] = shares_far
            nav_results['fund_cash_component'] = fund_cash
            
            logger.info(f"ETF Characteristics: shares_outstanding={shares_outstanding}, "
                       f"near_shares={shares_near}, far_shares={shares_far}, cash={fund_cash}")
        except Exception as e:
            logger.error(f"Error extracting ETF characteristics: {str(e)}")
            return None
        
        # Extract latest published NAV if available
        if not nav_data_df.empty:
            try:
                latest_published_nav = nav_data_df["nav"].iloc[0]
                nav_results['published_nav'] = latest_published_nav
                logger.info(f"Latest published NAV: {latest_published_nav}")
            except Exception as e:
                logger.warning(f"Error extracting published NAV: {str(e)}")
        
        # Extract USD/JPY exchange rate if available
        usd_jpy_rate = None
        if not fx_data_df.empty:
            try:
                # Look for TTM (middle rate) or any available rate
                if 'label' in fx_data_df.columns and 'rate' in fx_data_df.columns:
                    if 'pair' in fx_data_df.columns:
                        usdjpy_df = fx_data_df[fx_data_df['pair'] == 'USDJPY']
                    else:
                        usdjpy_df = fx_data_df  # Assume all rows are USDJPY
                    
                    # Try to find TTM rate first
                    ttm_rows = usdjpy_df[usdjpy_df['label'].str.contains('TTM|ACC.|A/S', case=False, na=False)]
                    
                    if not ttm_rows.empty:
                        usd_jpy_rate = ttm_rows['rate'].iloc[0]
                    else:
                        # Take any rate as fallback
                        usd_jpy_rate = usdjpy_df['rate'].iloc[0]
                    
                    nav_results['usd_jpy_rate'] = usd_jpy_rate
                    logger.info(f"USD/JPY rate: {usd_jpy_rate}")
            except Exception as e:
                logger.warning(f"Error extracting USD/JPY rate: {str(e)}")
        
        # If no FX rate found, use a default
        if usd_jpy_rate is None:
            usd_jpy_rate = 150.0  # Default fallback rate
            nav_results['usd_jpy_rate'] = usd_jpy_rate
            logger.warning(f"Using default USD/JPY rate: {usd_jpy_rate}")
        
        # Calculate NAV estimates based on VIX futures data
        # Group by futures and average prices across sources
        vix_futures_grouped = vix_futures_df.groupby('vix_future')['price'].mean().reset_index()
        
        # Get near and far month futures
        # Typically, these would be the first two VIX futures contracts
        vix_futures_sorted = vix_futures_grouped.sort_values('vix_future').reset_index(drop=True)
        
        # Calculate the total futures value
        try:
            near_future = vix_futures_sorted['vix_future'].iloc[0] if len(vix_futures_sorted) > 0 else None
            near_price = vix_futures_sorted['price'].iloc[0] if len(vix_futures_sorted) > 0 else 0
            
            far_future = vix_futures_sorted['vix_future'].iloc[1] if len(vix_futures_sorted) > 1 else None
            far_price = vix_futures_sorted['price'].iloc[1] if len(vix_futures_sorted) > 1 else 0
            
            nav_results['near_future'] = near_future
            nav_results['near_future_price'] = near_price
            nav_results['far_future'] = far_future
            nav_results['far_future_price'] = far_price
            
            logger.info(f"Futures: {near_future}={near_price}, {far_future}={far_price}")
            
            # Calculate futures value in USD
            futures_value_usd = (shares_near * near_price * 1000) + (shares_far * far_price * 1000)
            nav_results['futures_value_usd'] = futures_value_usd
            
            # Calculate NAV in JPY
            # NAV = (Futures Value in USD * FX Rate + Cash) / Shares Outstanding
            estimated_nav_jpy = (futures_value_usd * usd_jpy_rate + fund_cash) / shares_outstanding
            nav_results['estimated_nav_jpy'] = estimated_nav_jpy
            
            logger.info(f"Estimated NAV (JPY): {estimated_nav_jpy}")
            
            # If we have published NAV, calculate the difference
            if 'published_nav' in nav_results:
                nav_diff = estimated_nav_jpy - nav_results['published_nav']
                nav_diff_pct = (nav_diff / nav_results['published_nav']) * 100
                
                nav_results['nav_difference'] = nav_diff
                nav_results['nav_difference_pct'] = nav_diff_pct
                
                logger.info(f"NAV difference: {nav_diff} JPY ({nav_diff_pct:.2f}%)")
        
        except Exception as e:
            logger.error(f"Error calculating NAV: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        
        return nav_results
    
    except Exception as e:
        logger.error(f"Error in NAV calculations: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def save_nav_results(nav_results, save_dir=DATA_DIR):
    """
    Save NAV calculation results to CSV file
    
    Args:
        nav_results: Dictionary with NAV calculation results
        save_dir: Directory to save the file
    
    Returns:
        str: Path to saved file
    """
    if not nav_results:
        logger.warning("No NAV results to save")
        return None
    
    try:
        # Create DataFrame
        df = pd.DataFrame([nav_results])
        
        # Save to CSV
        csv_path = os.path.join(save_dir, "estimated_navs.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Saved NAV results to {csv_path}")
        return csv_path
    
    except Exception as e:
        logger.error(f"Error saving NAV results: {str(e)}")
        logger.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    logger.info("Starting estimated NAV calculator")
    
    # Calculate NAV estimates
    nav_results = calculate_estimated_nav()
    
    if nav_results:
        # Save results
        csv_path = save_nav_results(nav_results)
        
        if csv_path:
            print(f"✅ NAV calculations completed and saved to: {csv_path}")
        else:
            print("❌ Failed to save NAV calculations")
            sys.exit(1)
    else:
        print("❌ NAV calculations failed")
        sys.exit(1)
    
    logger.info("NAV calculator completed successfully")
