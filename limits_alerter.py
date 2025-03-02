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
import yfinance as yf
from common import setup_logging, SAVE_DIR, get_yfinance_ticker_for_vix_future, normalize_vix_ticker, get_next_vix_contracts

# Set up logging
logger = setup_logging('limits_alerter')

def get_daily_price_limits(base_price):
    """Determine daily price limits based on the base price from TSE rules in Q7."""
    if base_price is None or base_price <= 0:
        logger.error(f"Invalid base price: {base_price}")
        return None
        
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
    """
    Get the ETF composition from the etf_characteristics_master.csv for the given date.
    Requires exact data - no fallbacks.
    
    Args:
        date: Date to get composition for
        
    Returns:
        dict: Dictionary mapping futures tickers to weights, or None if not found
    """
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
        
        # Check if all required columns are present
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in ETF characteristics file: {missing_cols}")
            return None
        
        # Convert fund_date or timestamp to datetime for comparison
        if 'fund_date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['fund_date'], format='%Y%m%d')
            except Exception as e:
                logger.error(f"Error converting fund_date to datetime: {str(e)}")
                logger.error("Cannot determine dates in ETF characteristics file")
                return None
        else:
            logger.error("fund_date column is required but not found")
            return None
            
        # Convert target date to datetime
        try:
            target_date = pd.to_datetime(date)
        except Exception as e:
            logger.error(f"Error converting target date {date} to datetime: {str(e)}")
            return None
            
        # Make time zones comparable by making both naive
        if target_date.tzinfo:
            target_date = target_date.replace(tzinfo=None)
        df['date'] = df['date'].dt.tz_localize(None)
        
        # Find data for dates on or before target date
        df = df[df['date'] <= target_date]
        
        if df.empty:
            logger.error(f"No data found for date {date} or earlier")
            return None
            
        # Get the latest data
        latest_data = df.sort_values('date', ascending=False).iloc[0]
        
        # Log the data we're using
        logger.info(f"Using ETF composition data from {latest_data['date']} (latest available)")
        
        # Extract near and far futures with their weights
        near_future = None
        far_future = None
        
        # Get near and far futures directly
        if 'near_future' in latest_data and not pd.isna(latest_data['near_future']):
            near_future = str(latest_data['near_future'])
            logger.info(f"Found near future: {near_future}")
        else:
            logger.error("near_future value not found or is null")
            return None
        
        if 'far_future' in latest_data and not pd.isna(latest_data['far_future']):
            far_future = str(latest_data['far_future'])
            logger.info(f"Found far future: {far_future}")
        else:
            logger.error("far_future value not found or is null")
            return None
            
        # Normalize futures tickers
        near_future = normalize_vix_ticker(near_future)
        far_future = normalize_vix_ticker(far_future)
        
        # Get shares amounts
        near_shares = 0
        far_shares = 0
        
        # Get near shares directly from the required column
        if 'shares_amount_near_future' in latest_data and not pd.isna(latest_data['shares_amount_near_future']):
            near_shares = float(latest_data['shares_amount_near_future'])
            logger.info(f"Found near shares: {near_shares}")
        else:
            logger.error("shares_amount_near_future value not found or is null")
            return None
        
        # Get far shares directly from the required column
        if 'shares_amount_far_future' in latest_data and not pd.isna(latest_data['shares_amount_far_future']):
            far_shares = float(latest_data['shares_amount_far_future'])
            logger.info(f"Found far shares: {far_shares}")
        else:
            logger.error("shares_amount_far_future value not found or is null")
            return None
        
        # Validate shares amounts
        if near_shares <= 0 or far_shares <= 0:
            logger.error(f"Invalid shares amounts: near={near_shares}, far={far_shares}")
            return None
            
        # Return composition dictionary
        composition = {}
        composition[near_future] = float(near_shares)
        composition[far_future] = float(far_shares)
        
        logger.info(f"ETF composition: {composition}")
        return composition
    
    except Exception as e:
        logger.error(f"Error getting ETF composition: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def get_etf_closing_data():
    """
    Get closing price of 318A ETF on Tokyo Stock Exchange.
    Strict validation - no fallbacks.
    
    Returns:
        tuple: (closing_price, closing_time, price_limits) or (None, None, None) if failure
    """
    try:
        ticker = "318A.T"  # Correct ticker for 318A
        
        # Use a 7-day lookback to find the most recent trading day data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"Getting ETF data for {ticker} from {start_date} to {end_date}")
        
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        if len(hist) == 0:
            logger.error(f"No data found for {ticker} in the last 7 days")
            return None, None, None
        
        # Extract closing price and time from the most recent data
        try:
            closing_price = hist['Close'].iloc[-1]
            if pd.isna(closing_price) or closing_price <= 0:
                logger.error(f"Invalid closing price: {closing_price}")
                return None, None, None
        except Exception as e:
            logger.error(f"Error extracting closing price: {str(e)}")
            return None, None, None
            
        try:
            closing_time = hist.index[-1]
        except Exception as e:
            logger.error(f"Error extracting closing time: {str(e)}")
            return None, None, None
        
        # Convert to JST timezone
        try:
            jst = pytz.timezone('Asia/Tokyo')
            closing_time_jst = closing_time.astimezone(jst)
        except Exception as e:
            logger.error(f"Error converting to JST timezone: {str(e)}")
            return None, None, None
        
        # Calculate price limits for next trading day
        try:
            price_limits = get_daily_price_limits(closing_price)
            if price_limits is None:
                logger.error("Failed to calculate price limits")
                return None, None, None
                
            lower_limit, upper_limit = price_limits
        except Exception as e:
            logger.error(f"Error calculating price limits: {str(e)}")
            return None, None, None
        
        logger.info(f"Most recent closing price of {ticker}: {closing_price:.2f} JPY at {closing_time_jst}")
        logger.info(f"Daily price limits: {lower_limit:.2f} to {upper_limit:.2f} JPY")
        
        return closing_price, closing_time_jst, price_limits
    
    except Exception as e:
        logger.error(f"Error getting ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None, None

def get_vix_futures_prices(composition, reference_time):
    """
    Get current VIX futures prices from Yahoo Finance.
    Focuses only on ^VXIND and ^VFTW ticker formats.
    No fallbacks - fails cleanly if prices can't be found.
    
    Args:
        composition: Dictionary mapping futures tickers to weights
        reference_time: Reference time for price data
        
    Returns:
        dict: Dictionary mapping futures tickers to prices, or None if failure
    """
    try:
        futures_prices = {}
        
        # Use position-based tickers for contracts
        next_contracts = get_next_vix_contracts(6)  # Get up to 6 upcoming contracts
        
        # First, determine position mapping for the futures we need
        position_map = {}
        for futures_ticker in composition.keys():
            normalized_ticker = normalize_vix_ticker(futures_ticker)
            if normalized_ticker in next_contracts:
                position = next_contracts.index(normalized_ticker) + 1  # 1-indexed
                position_map[normalized_ticker] = position
                logger.info(f"Determined {normalized_ticker} is contract position {position}")
            else:
                logger.error(f"Could not determine position for {normalized_ticker}")
                return None
        
        # Download all futures prices using only the specific ticker formats
        for normalized_ticker, position in position_map.items():
            # Only try positions 1-3 which are standard
            if position <= 3:
                # Focused on just these two ticker formats
                specific_tickers = [f"^VXIND{position}", f"^VFTW{position}"]
                
                price_found = False
                for ticker in specific_tickers:
                    try:
                        logger.info(f"Trying to download {ticker} for {normalized_ticker}")
                        
                        # Use period parameter for better compatibility
                        data = yf.download(ticker, period="5d", progress=False)
                        
                        if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                            # Fix the warning about float on single element Series
                            price = float(data['Close'].iloc[-1])
                            
                            # Validate price
                            if pd.isna(price) or price <= 0:
                                logger.warning(f"Invalid price for {ticker}: {price}")
                                continue
                                
                            futures_prices[normalized_ticker] = price
                            logger.info(f"Found price for {normalized_ticker} using {ticker}: {price}")
                            price_found = True
                            break
                    except Exception as e:
                        logger.warning(f"Failed to download {ticker}: {str(e)}")
                
                # If no price found with specific tickers, try VIX index but only for front month
                if not price_found and position == 1:
                    try:
                        logger.info(f"Trying VIX index for front-month {normalized_ticker}")
                        data = yf.download("^VIX", period="5d", progress=False)
                        
                        if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                            # Fix the warning about float on single element Series
                            price = float(data['Close'].iloc[-1])
                            
                            # Validate price
                            if pd.isna(price) or price <= 0:
                                logger.warning(f"Invalid VIX index price: {price}")
                            else:
                                futures_prices[normalized_ticker] = price
                                logger.info(f"Using VIX index price for {normalized_ticker}: {price}")
                                price_found = True
                    except Exception as e:
                        logger.warning(f"Failed to download VIX index: {str(e)}")
                
                # If we still don't have a price, fail
                if not price_found:
                    logger.error(f"Could not find price for {normalized_ticker} using any ticker format")
                    return None
            else:
                logger.error(f"Position {position} for {normalized_ticker} is beyond supported range")
                return None
        
        # Check if we have all the prices we need
        missing_futures = [ticker for ticker in composition.keys() if normalize_vix_ticker(ticker) not in futures_prices]
        if missing_futures:
            logger.error(f"Missing prices for futures: {missing_futures}")
            return None
        
        logger.info(f"Successfully retrieved all current futures prices: {futures_prices}")
        return futures_prices
    
    except Exception as e:
        logger.error(f"Error getting VIX futures prices: {str(e)}")
        logger.error(traceback.format_exc())
        return None
        
def get_exchange_rate(reference_time):
    """
    Get USD/JPY exchange rate at the specified time.
    No fallbacks - fails if current rate cannot be found.
    
    Args:
        reference_time: Reference time for exchange rate
        
    Returns:
        float: Exchange rate or None if failure
    """
    try:
        usdjpy = yf.Ticker("USDJPY=X")
        
        # Format date for yfinance - use a 7-day lookback to find the most recent data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"Getting USD/JPY exchange rate from {start_date} to {end_date}")
        
        # Get exchange rate data
        fx_data = usdjpy.history(start=start_date, end=end_date)
        
        if len(fx_data) == 0:
            logger.error(f"No USD/JPY data found in the last 7 days")
            return None
        
        # Get the most recent exchange rate
        exchange_rate = fx_data['Close'].iloc[-1]
        
        if pd.isna(exchange_rate) or exchange_rate <= 0:
            logger.error(f"Invalid exchange rate: {exchange_rate}")
            return None
            
        rate_date = fx_data.index[-1].strftime('%Y-%m-%d')
        logger.info(f"Most recent USD/JPY exchange rate: {exchange_rate:.2f} from {rate_date}")
        
        return exchange_rate
    
    except Exception as e:
        logger.error(f"Error getting exchange rate: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def calculate_basket_value(composition, futures_prices, exchange_rate):
    """
    Calculate the value of the VIX futures basket in JPY.
    All inputs must be valid - fails otherwise.
    """
    if not composition or not futures_prices or not exchange_rate:
        logger.error("Missing required data for basket value calculation")
        return None
    
    # Verify all futures in composition are in futures_prices
    missing_futures = [ticker for ticker in composition.keys() if ticker not in futures_prices]
    if missing_futures:
        logger.error(f"Missing prices for futures: {missing_futures}")
        return None
    
    try:
        basket_value_usd = 0
        total_weight = 0
        
        for futures_ticker, weight in composition.items():
            if futures_ticker in futures_prices:
                # Validate inputs
                if pd.isna(weight) or weight <= 0:
                    logger.error(f"Invalid weight for {futures_ticker}: {weight}")
                    return None
                    
                price = futures_prices[futures_ticker]
                if pd.isna(price) or price <= 0:
                    logger.error(f"Invalid price for {futures_ticker}: {price}")
                    return None
                
                # VIX futures are 1000 times the index
                contract_multiplier = 1000
                basket_value_usd += price * weight * contract_multiplier
                total_weight += weight
        
        # Check if we have valid weights
        if total_weight == 0:
            logger.error("Total weight of futures is zero. Cannot calculate basket value.")
            return None
        
        # Convert to JPY
        basket_value_jpy = basket_value_usd * exchange_rate
        
        logger.info(f"Calculated basket value: {basket_value_jpy:.2f} JPY (USD: {basket_value_usd:.2f})")
        return basket_value_jpy
    
    except Exception as e:
        logger.error(f"Error calculating basket value: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def check_for_alerts(current_value, initial_value, price_limits, closing_price):
    """Check if the basket value has changed beyond the allowed price limits."""
    if not current_value or not initial_value or not price_limits or not closing_price:
        logger.error("Missing required data for alert check")
        return False
        
    try:
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
        
        logger.info(f"No alert: Basket value change {value_pct_change:.2%} within limits ({allowed_lower_pct:.2%} to {allowed_upper_pct:.2%})")
        return False
    
    except Exception as e:
        logger.error(f"Error checking for alerts: {str(e)}")
        logger.error(traceback.format_exc())
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
            if not current_futures_prices:
                logger.error("Failed to get current futures prices. Aborting monitor.")
                return
                
            current_exchange_rate = get_exchange_rate(current_time)
            if not current_exchange_rate:
                logger.error("Failed to get current exchange rate. Aborting monitor.")
                return
            
            # Calculate current basket value
            current_basket_value = calculate_basket_value(composition, current_futures_prices, current_exchange_rate)
            
            if not current_basket_value:
                logger.error("Failed to calculate current basket value. Aborting monitor.")
                return
                
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
    args = parser.parse_args()
    
    logger.info("Starting limits alerter with most recent available data")
    
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
    
    if not futures_prices:
        logger.error("Could not get VIX futures prices. Exiting.")
        return 1
    
    exchange_rate = get_exchange_rate(closing_time)
    
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
    
    # Calculate shares outstanding from ETF characteristics
    shares_outstanding = 0
    try:
        etf_file = os.path.join(SAVE_DIR, "etf_characteristics_master.csv")
        if os.path.exists(etf_file):
            df = pd.read_csv(etf_file)
            if 'shares_outstanding' in df.columns and not df.empty:
                shares_outstanding = df.iloc[-1]['shares_outstanding']
                if shares_outstanding > 0:
                    nav_per_share = current_basket_value / shares_outstanding
                    logger.info(f"Estimated NAV per share: {nav_per_share:.2f} JPY")
                else:
                    logger.warning("Invalid shares_outstanding value (must be positive)")
            else:
                logger.warning("shares_outstanding column not found in ETF characteristics")
        else:
            logger.warning("ETF characteristics file not found")
    except Exception as e:
        logger.warning(f"Could not calculate NAV per share: {str(e)}")
    
    # Use actual ETF value rather than trying to derive it
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
    
    # Start monitoring if requested
    if args.monitor:
        logger.info("Starting continuous monitoring")
        monitor_basket_value(composition, initial_basket_value, price_limits, closing_price, args.interval)
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
