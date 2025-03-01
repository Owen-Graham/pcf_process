import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import time
import logging
import argparse
import traceback
import sys
from common import setup_logging, SAVE_DIR, get_yfinance_ticker_for_vix_future, normalize_vix_ticker

# Set up logging
logger = setup_logging('limits_alerter')

def get_daily_price_limits(base_price):
    """Determine daily price limits based on the base price from TSE rules in Q7."""
    limits_table = [
        (100, 30),
        (200, 50),
        (500, 80),
        (700, 100),
        (1000, 150),
        (1500, 300),
        (2000, 400),
        (3000, 500),
        (5000, 700),
        (7000, 1000),
        (10000, 1500),
        (15000, 3000),
        (20000, 4000),
        (30000, 5000),
        (50000, 7000),
        (70000, 10000),
        (100000, 15000),
        (150000, 30000),
        (200000, 40000),
        (300000, 50000),
        (500000, 70000),
        (700000, 100000),
        (1000000, 150000),
        (1500000, 300000),
        (2000000, 400000),
        (3000000, 500000),
        (5000000, 700000),
        (7000000, 1000000),
        (10000000, 1500000),
        (15000000, 3000000),
        (20000000, 4000000),
        (30000000, 5000000),
        (50000000, 7000000),
        (float('inf'), 10000000)
    ]
    
    for threshold, limit in limits_table:
        if base_price < threshold:
            lower_limit = max(1, base_price - limit)
            upper_limit = base_price + limit
            return lower_limit, upper_limit
    
    limit = limits_table[-1][1]
    return max(1, base_price - limit), base_price + limit

def get_etf_composition(date):
    """Get the ETF composition from the etf_characteristics_master.csv for the given date."""
    try:
        # Read the ETF characteristics master CSV
        etf_file = os.path.join(SAVE_DIR, "etf_characteristics_master.csv")
        if not os.path.exists(etf_file):
            logger.error(f"ETF characteristics file not found: {etf_file}")
            return None
            
        df = pd.read_csv(etf_file)
        
        # Check if we have data
        if df.empty:
            logger.error("ETF characteristics file is empty")
            return None
            
        # Check if we have the required columns
        required_cols = ['timestamp', 'fund_date', 'near_future', 'far_future', 
                         'shares_amount_near_future', 'shares_amount_far_future']
        
        # Check for old column names as well
        if 'near_future_code' in df.columns and 'near_future' not in df.columns:
            df = df.rename(columns={'near_future_code': 'near_future'})
            logger.info("Renamed near_future_code to near_future")
            
        if 'far_future_code' in df.columns and 'far_future' not in df.columns:
            df = df.rename(columns={'far_future_code': 'far_future'})
            logger.info("Renamed far_future_code to far_future")
            
        # Check if all required columns are present
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in ETF characteristics file: {missing_cols}")
            return None
        
        # Convert timestamp to datetime for comparison if needed
        if 'fund_date' in df.columns:
            # Try to parse fund_date to datetime
            try:
                df['date'] = pd.to_datetime(df['fund_date'], format='%Y%m%d')
            except:
                # If that format fails, try different formats
                try:
                    df['date'] = pd.to_datetime(df['fund_date'])
                except:
                    logger.warning("Could not convert fund_date to datetime, using timestamp")
                    # Use timestamp as a fallback
                    df['date'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M')
        else:
            # Use timestamp as fallback
            df['date'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M')
            
        # Find the row for the given date (closest date that's not in the future)
        # Convert both to naive datetimes to avoid timezone comparison issues
        target_date = pd.to_datetime(date).tz_localize(None)
        
        # Make sure all dates in df are also timezone naive
        df['date'] = df['date'].dt.tz_localize(None)
        df = df[df['date'] <= target_date]
        
        if df.empty:
            logger.error(f"No data found for date {date} or earlier")
            return None
            
        # Get the latest data
        latest_data = df.sort_values('date', ascending=False).iloc[0]
        
        # Extract the composition
        composition = {}
        
        # Get near and far futures with their weights
        near_future = None
        far_future = None
        
        # Try different column names that might contain the futures data
        future_columns = ['near_future', 'far_future', 'near_future_code', 'far_future_code']
        
        # Find near future
        for col in future_columns:
            if col.startswith('near') and col in latest_data and not pd.isna(latest_data.get(col)):
                near_future = latest_data.get(col)
                logger.info(f"Found near future in column '{col}': {near_future}")
                break
        
        # Find far future
        for col in future_columns:
            if col.startswith('far') and col in latest_data and not pd.isna(latest_data.get(col)):
                far_future = latest_data.get(col)
                logger.info(f"Found far future in column '{col}': {far_future}")
                break
        
        # Check if we found both futures
        if pd.isna(near_future) or pd.isna(far_future) or near_future is None or far_future is None:
            logger.error("Missing future codes in ETF data")
            logger.error(f"Available columns: {latest_data.index.tolist()}")
            
            # Try to use a fallback from vix_futures_master.csv if available
            vix_futures_file = os.path.join(SAVE_DIR, "vix_futures_master.csv")
            if os.path.exists(vix_futures_file):
                logger.info("Attempting to use VIX futures master file as fallback")
                try:
                    vix_df = pd.read_csv(vix_futures_file)
                    if not vix_df.empty and 'vix_future' in vix_df.columns:
                        # Get the two most recent futures
                        unique_futures = vix_df['vix_future'].unique()
                        # Filter out any non-VX futures (like VIX index)
                        vx_futures = [f for f in unique_futures if f.startswith('VX')]
                        
                        if len(vx_futures) >= 2:
                            # Sort them to get the nearest and next futures
                            sorted_futures = sorted(vx_futures)
                            near_future = sorted_futures[0]
                            far_future = sorted_futures[1]
                            logger.info(f"Using fallback futures: near={near_future}, far={far_future}")
                        else:
                            logger.error(f"Not enough VX futures in vix_futures_master.csv: {vx_futures}")
                            return None
                    else:
                        logger.error("VIX futures master file doesn't have required data")
                        return None
                except Exception as e:
                    logger.error(f"Error reading VIX futures master file: {str(e)}")
                    return None
            else:
                # Last resort fallback - use hardcoded current month futures 
                # This is better than failing completely
                current_month = datetime.now().month
                current_year = datetime.now().year % 10  # Last digit of year
                
                # Month codes used in VIX futures
                month_codes = {
                    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                }
                
                # Get current and next month codes
                current_code = month_codes.get(current_month)
                next_month = (current_month % 12) + 1
                next_code = month_codes.get(next_month)
                next_year = current_year if next_month > current_month else (current_year + 1) % 10
                
                if current_code and next_code:
                    near_future = f"VX{current_code}{current_year}"
                    far_future = f"VX{next_code}{next_year}"
                    logger.warning(f"Using fallback futures based on current date: near={near_future}, far={far_future}")
                else:
                    logger.error("Could not determine fallback futures")
                    return None
            
        # Normalize futures tickers
        near_future = normalize_vix_ticker(near_future)
        far_future = normalize_vix_ticker(far_future)
        
        # Get shares amounts
        near_shares = 0
        far_shares = 0
        
        # Try different column names for shares
        share_columns = ['shares_amount_near_future', 'shares_near_future', 'shares_near', 
                         'shares_amount_far_future', 'shares_far_future', 'shares_far']
        
        # Find near shares
        for col in share_columns:
            if col.startswith('shares') and 'near' in col and col in latest_data and not pd.isna(latest_data.get(col)):
                near_shares = float(latest_data.get(col))
                logger.info(f"Found near shares in column '{col}': {near_shares}")
                break
        
        # Find far shares
        for col in share_columns:
            if col.startswith('shares') and 'far' in col and col in latest_data and not pd.isna(latest_data.get(col)):
                far_shares = float(latest_data.get(col))
                logger.info(f"Found far shares in column '{col}': {far_shares}")
                break
        
        # Check if we have valid shares amounts
        if near_shares <= 0 or far_shares <= 0:
            logger.warning(f"Invalid or missing shares amounts: near={near_shares}, far={far_shares}")
            
            # Use fallback shares values - better than failing
            # These are placeholder values that will at least let the script run
            near_shares = 1.0
            far_shares = 1.0
            logger.warning(f"Using fallback shares values: near={near_shares}, far={far_shares}")
            
        # Calculate weights (this is simplified - in reality would depend on the share value)
        # but for our purposes, we'll just use the share amounts as weights
        composition[near_future] = float(near_shares)
        composition[far_future] = float(far_shares)
        
        logger.info(f"ETF composition: {composition}")
        return composition
    
    except Exception as e:
        logger.error(f"Error getting ETF composition: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def get_etf_closing_data():
    """Get closing price of 318A ETF on Tokyo Stock Exchange."""
    try:
        import yfinance as yf
        
        ticker = "318A.T"  # Correct ticker for 318A
        
        # Use a 7-day lookback to ensure we get the most recent data even on weekends
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        if len(hist) == 0:
            logger.error(f"No data found for {ticker} in the last 7 days")
            return None, None, None
        
        # Extract closing price and time from the most recent data
        closing_price = hist['Close'].iloc[-1]
        closing_time = hist.index[-1]
        
        # Convert to JST timezone
        jst = pytz.timezone('Asia/Tokyo')
        closing_time_jst = closing_time.astimezone(jst)
        
        # Calculate price limits for next trading day
        lower_limit, upper_limit = get_daily_price_limits(closing_price)
        
        logger.info(f"Most recent closing price of {ticker}: {closing_price:.2f} JPY at {closing_time_jst}")
        logger.info(f"Daily price limits: {lower_limit:.2f} to {upper_limit:.2f} JPY")
        
        return closing_price, closing_time_jst, (lower_limit, upper_limit)
    
    except Exception as e:
        logger.error(f"Error getting ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None, None

def get_vix_futures_prices(composition, reference_time):
    """Get VIX futures prices from Yahoo Finance using proper ticker mapping."""
    try:
        import yfinance as yf
        
        futures_prices = {}
        
        # Format date for yfinance - request a longer period to ensure we get the most recent data
        # even on weekends/holidays
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        # Get prices for each futures contract in the composition
        for futures_ticker, weight in composition.items():
            # Convert to Yahoo Finance ticker format
            yahoo_ticker = get_yfinance_ticker_for_vix_future(futures_ticker)
            
            # Get data - use a 7-day lookback to ensure we get the most recent data
            future = yf.Ticker(yahoo_ticker)
            future_data = future.history(start=start_date, end=end_date)
            
            if len(future_data) > 0:
                # Get the most recent closing price
                futures_prices[futures_ticker] = future_data['Close'].iloc[-1]
                logger.info(f"Retrieved most recent price for {futures_ticker} ({yahoo_ticker}): {futures_prices[futures_ticker]:.2f}")
            else:
                logger.error(f"No data found for {yahoo_ticker} ({futures_ticker})")
                return None
        
        if not futures_prices:
            logger.error("Could not retrieve any futures prices")
            return None
            
        return futures_prices
    
    except Exception as e:
        logger.error(f"Error getting VIX futures prices: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def get_exchange_rate(reference_time):
    """Get USD/JPY exchange rate at the specified time."""
    try:
        import yfinance as yf
        
        usdjpy = yf.Ticker("USDJPY=X")
        
        # Format date for yfinance - use a 7-day lookback to ensure we get the most recent data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        # Get exchange rate data
        fx_data = usdjpy.history(start=start_date, end=end_date)
        
        if len(fx_data) == 0:
            logger.error(f"No USD/JPY data found in the last 7 days")
            return None
        
        # Get the most recent exchange rate
        exchange_rate = fx_data['Close'].iloc[-1]
        logger.info(f"Most recent USD/JPY exchange rate: {exchange_rate:.2f}")
        
        return exchange_rate
    
    except Exception as e:
        logger.error(f"Error getting exchange rate: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def calculate_basket_value(composition, futures_prices, exchange_rate):
    """Calculate the value of the VIX futures basket in JPY."""
    if not futures_prices or not exchange_rate:
        return None
    
    basket_value_usd = 0
    total_weight = 0
    
    for futures_ticker, weight in composition.items():
        if futures_ticker in futures_prices:
            # VIX futures are 1000 times the index
            contract_multiplier = 1000
            basket_value_usd += futures_prices[futures_ticker] * weight * contract_multiplier
            total_weight += weight
    
    # Check if we have valid weights
    if total_weight == 0:
        logger.error("Total weight of futures is zero. Cannot calculate basket value.")
        return None
    
    # Convert to JPY
    basket_value_jpy = basket_value_usd * exchange_rate
    
    logger.info(f"Calculated basket value: {basket_value_jpy:.2f} JPY (USD: {basket_value_usd:.2f})")
    return basket_value_jpy

def check_for_alerts(current_value, initial_value, price_limits, closing_price):
    """Check if the basket value has changed beyond the allowed price limits."""
    lower_limit, upper_limit = price_limits
    
    # Calculate percentage changes
    value_pct_change = (current_value - initial_value) / initial_value
    allowed_lower_pct = (lower_limit - closing_price) / closing_price
    allowed_upper_pct = (upper_limit - closing_price) / closing_price
    
    # Check if value change exceeds allowed ETF price change
    if value_pct_change < allowed_lower_pct or value_pct_change > allowed_upper_pct:
        message = (
            f"ALERT: Basket value changed by {value_pct_change:.2%}, exceeding daily price limits!\n"
            f"Allowed range: {allowed_lower_pct:.2%} to {allowed_upper_pct:.2%}\n"
            f"Current basket value: {current_value:.2f} JPY\n"
            f"Initial basket value: {initial_value:.2f} JPY"
        )
        logger.warning(message)
        print(f"\n*** ALERT ***\n{message}\n**************")
        return True
    
    return False

def monitor_basket_value(composition, initial_basket_value, price_limits, closing_price, check_interval=60):
    """Start continuous monitoring of the basket value."""
    logger.info(f"Starting monitoring. Will check every {check_interval} seconds.")
    
    try:
        alert_counter = 0
        max_alerts = 5
        
        while alert_counter < max_alerts:
            current_time = datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
            
            # Get current futures prices and exchange rate
            current_futures_prices = get_vix_futures_prices(composition, current_time)
            current_exchange_rate = get_exchange_rate(current_time)
            
            if not current_futures_prices or not current_exchange_rate:
                logger.warning("Could not get current prices. Skipping this check.")
                time.sleep(check_interval)
                continue
            
            # Calculate current basket value
            current_basket_value = calculate_basket_value(composition, current_futures_prices, current_exchange_rate)
            
            if current_basket_value:
                # Calculate percentage change
                pct_change = (current_basket_value - initial_basket_value) / initial_basket_value
                logger.info(f"Current basket value: {current_basket_value:.2f} JPY ({pct_change:.2%} change)")
                
                # Check for alerts
                if check_for_alerts(current_basket_value, initial_basket_value, price_limits, closing_price):
                    alert_counter += 1
                    logger.warning(f"Alert {alert_counter} of {max_alerts}")
                    
                    # Save alert to file
                    alert_file = os.path.join(SAVE_DIR, f"price_alert_{datetime.now().strftime('%Y%m%d%H%M')}.log")
                    with open(alert_file, "w") as f:
                        f.write(f"PRICE LIMIT ALERT\n")
                        f.write(f"Time: {current_time}\n")
                        f.write(f"Basket value: {current_basket_value:.2f} JPY\n")
                        f.write(f"Initial value: {initial_basket_value:.2f} JPY\n")
                        f.write(f"Change: {pct_change:.2%}\n")
                        f.write(f"Price limits: {price_limits[0]:.2f} to {price_limits[1]:.2f} JPY\n")
                        f.write(f"Allowed range: {(price_limits[0]-closing_price)/closing_price:.2%} to {(price_limits[1]-closing_price)/closing_price:.2%}\n")
            
            # Wait for next check
            time.sleep(check_interval)
    
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user.")
    except Exception as e:
        logger.error(f"Error in monitoring loop: {str(e)}")
        logger.error(traceback.format_exc())

def main():
    """Main function to analyze ETF price limits and underlying basket value."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Check 318A ETF basket value against price limits')
    parser.add_argument('--check-only', action='store_true', help='Perform one-time check only (no monitoring)', default=True)
    parser.add_argument('--monitor', action='store_true', help='Enable continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in seconds (default: 60)')
    parser.add_argument('--duration', type=int, default=60, help='Monitoring duration in minutes (default: 60)')
    args = parser.parse_args()
    
    # Get ETF closing data - the most recent data available
    closing_price, closing_time, price_limits = get_etf_closing_data()
    
    if not all([closing_price, closing_time, price_limits]):
        logger.error("Could not get ETF data. Exiting.")
        return 1
    
    # Get the ETF composition from the master CSV - the most recent available
    composition = get_etf_composition(closing_time)
    
    if not composition:
        logger.error("Could not get ETF composition. Exiting.")
        return 1
            
    logger.info(f"ETF composition for {closing_time.strftime('%Y-%m-%d')}: {composition}")
    
    # Get most recent futures prices and exchange rate
    futures_prices = get_vix_futures_prices(composition, closing_time)
    exchange_rate = get_exchange_rate(closing_time)
    
    if not futures_prices:
        logger.error("Could not get VIX futures prices. Exiting.")
        return 1
    
    if not exchange_rate:
        logger.error("Could not get USD/JPY exchange rate. Exiting.")
        return 1
    
    # Calculate current basket value based on most recent data
    current_basket_value = calculate_basket_value(composition, futures_prices, exchange_rate)
    
    if not current_basket_value:
        logger.error("Could not calculate basket value. Exiting.")
        return 1
    
    logger.info(f"Current basket value: {current_basket_value:.2f} JPY")
    logger.info(f"Price limits: {price_limits[0]:.2f} to {price_limits[1]:.2f} JPY")
    
    # Calculate share value if shares outstanding is available
    shares_outstanding = 0
    try:
        # Try to read the ETF characteristics to get shares outstanding
        etf_file = os.path.join(SAVE_DIR, "etf_characteristics_master.csv")
        if os.path.exists(etf_file):
            df = pd.read_csv(etf_file)
            if 'shares_outstanding' in df.columns and not df.empty:
                shares_outstanding = df.iloc[-1]['shares_outstanding']
                if shares_outstanding > 0:
                    nav_per_share = current_basket_value / shares_outstanding
                    logger.info(f"Estimated NAV per share: {nav_per_share:.2f} JPY")
    except Exception as e:
        logger.warning(f"Could not calculate NAV per share: {str(e)}")
    
    # Calculate initial basket value based on the ETF's price
    # This assumes the ETF tracks the basket perfectly
    initial_basket_value = closing_price * shares_outstanding if shares_outstanding > 0 else closing_price
    
    # Check for alerts (compare current basket value to price limits)
    is_alert = check_for_alerts(current_basket_value, initial_basket_value, price_limits, closing_price)
    
    # If any alert was detected, save it to a file
    if is_alert:
        pct_change = (current_basket_value - initial_basket_value) / initial_basket_value
        current_time = datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
            
        # Save alert to file
        alert_file = os.path.join(SAVE_DIR, f"price_alert_{datetime.now().strftime('%Y%m%d%H%M')}.log")
        with open(alert_file, "w") as f:
            f.write(f"PRICE LIMIT ALERT\n")
            f.write(f"Time: {current_time}\n")
            f.write(f"Basket value: {current_basket_value:.2f} JPY\n")
            f.write(f"Initial value: {initial_basket_value:.2f} JPY\n")
            f.write(f"Change: {pct_change:.2%}\n")
            f.write(f"Price limits: {price_limits[0]:.2f} to {price_limits[1]:.2f} JPY\n")
            f.write(f"Allowed range: {(price_limits[0]-closing_price)/closing_price:.2%} to {(price_limits[1]-closing_price)/closing_price:.2%}\n")
            
        logger.info("Price limit alert detected and saved to file")
    else:
        logger.info("No price limit alerts detected")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
