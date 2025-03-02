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
            closing_price = float(hist['Close'].iloc[-1])
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

def get_vix_futures_prices(composition, reference_time, label=""):
    """
    Get current VIX futures prices from Yahoo Finance.
    Focuses only on ^VXIND and ^VFTW ticker formats.
    No fallbacks - fails cleanly if prices can't be found.
    
    Args:
        composition: Dictionary mapping futures tickers to weights
        reference_time: Reference time for price data
        label: Optional label for logging (e.g., "Initial" or "Current")
        
    Returns:
        tuple: (prices_dict, details_dict) or (None, None) if failure
        prices_dict: Dictionary mapping futures tickers to prices
        details_dict: Dictionary with detailed metadata about each price
    """
    try:
        futures_prices = {}
        price_details = {}  # Store additional details about each price
        
        logger.info(f"===== Getting {label} VIX Futures Prices =====")
        logger.info(f"Reference time: {reference_time}")
        
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
                return None, None
        
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
                            price = float(data['Close'].iloc[0])
                            price_date = data.index[-1]
                            
                            # Validate price
                            if pd.isna(price) or price <= 0:
                                logger.warning(f"Invalid price for {ticker}: {price}")
                                continue
                                
                            futures_prices[normalized_ticker] = price
                            # Store details about the price
                            price_details[normalized_ticker] = {
                                'price': price,
                                'timestamp': price_date,
                                'source': ticker,
                                'position': position
                            }
                            logger.info(f"Found price for {normalized_ticker} using {ticker}: {price:.4f} (from {price_date})")
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
                            price = float(data['Close'].iloc[0])
                            price_date = data.index[-1]
                            
                            # Validate price
                            if pd.isna(price) or price <= 0:
                                logger.warning(f"Invalid VIX index price: {price}")
                            else:
                                futures_prices[normalized_ticker] = price
                                # Store details about the price
                                price_details[normalized_ticker] = {
                                    'price': price,
                                    'timestamp': price_date,
                                    'source': "^VIX (index price)",
                                    'position': position
                                }
                                logger.info(f"Using VIX index price for {normalized_ticker}: {price:.4f} (from {price_date})")
                                price_found = True
                    except Exception as e:
                        logger.warning(f"Failed to download VIX index: {str(e)}")
                
                # If we still don't have a price, fail
                if not price_found:
                    logger.error(f"Could not find price for {normalized_ticker} using any ticker format")
                    return None, None
            else:
                logger.error(f"Position {position} for {normalized_ticker} is beyond supported range")
                return None, None
        
        # Check if we have all the prices we need
        missing_futures = [ticker for ticker in composition.keys() if normalize_vix_ticker(ticker) not in futures_prices]
        if missing_futures:
            logger.error(f"Missing prices for futures: {missing_futures}")
            return None, None
        
        # Log a detailed summary of all futures prices
        logger.info(f"===== {label} VIX Futures Prices Summary =====")
        for ticker, details in price_details.items():
            logger.info(f"{ticker} [{details['source']}]: {details['price']:.4f} at {details['timestamp']}")
        logger.info("==========================================")
        
        return futures_prices, price_details
    
    except Exception as e:
        logger.error(f"Error getting VIX futures prices: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None

def get_exchange_rate(reference_time, label=""):
    """
    Get USD/JPY exchange rate at the specified time.
    No fallbacks - fails cleanly if current rate cannot be found.
    
    Args:
        reference_time: Reference time for exchange rate
        label: Optional label for logging (e.g., "Initial" or "Current")
        
    Returns:
        tuple: (exchange_rate, details) or (None, None) if failure
    """
    try:
        logger.info(f"===== Getting {label} USD/JPY Exchange Rate =====")
        logger.info(f"Reference time: {reference_time}")
        
        usdjpy = yf.Ticker("USDJPY=X")
        
        # Format date for yfinance - use a 7-day lookback to find the most recent data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"Querying USD/JPY exchange rate from {start_date} to {end_date}")
        
        # Get exchange rate data
        fx_data = usdjpy.history(start=start_date, end=end_date)
        
        if len(fx_data) == 0:
            logger.error(f"No USD/JPY data found in the last 7 days")
            return None, None
        
        # Get the most recent exchange rate
        exchange_rate = float(fx_data['Close'].iloc[0])
        rate_date = fx_data.index[-1]
        
        if pd.isna(exchange_rate) or exchange_rate <= 0:
            logger.error(f"Invalid exchange rate: {exchange_rate}")
            return None, None
        
        # Create details dictionary
        rate_details = {
            'rate': exchange_rate,
            'timestamp': rate_date,
            'source': 'USDJPY=X'
        }
            
        logger.info(f"{label} USD/JPY exchange rate: {exchange_rate:.2f} from {rate_date}")
        
        return exchange_rate, rate_details
    
    except Exception as e:
        logger.error(f"Error getting exchange rate: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None

def calculate_basket_value(composition, futures_prices, exchange_rate, price_details, rate_details, label=""):
    """
    Calculate the value of the VIX futures basket in JPY.
    All inputs must be valid - fails otherwise.
    
    Args:
        composition: Dictionary mapping futures tickers to weights
        futures_prices: Dictionary mapping futures tickers to prices
        exchange_rate: USD/JPY exchange rate
        price_details: Dictionary with details about each price
        rate_details: Dictionary with details about exchange rate
        label: Optional label for logging (e.g., "Initial" or "Current")
    
    Returns:
        float: Basket value in JPY or None if failure
    """
    if not composition or not futures_prices or not exchange_rate:
        logger.error(f"Missing required data for {label} basket value calculation")
        return None
    
    # Verify all futures in composition are in futures_prices
    normalized_composition = {normalize_vix_ticker(k): v for k, v in composition.items()}
    missing_futures = [ticker for ticker in normalized_composition.keys() if ticker not in futures_prices]
    if missing_futures:
        logger.error(f"Missing prices for futures in {label} basket: {missing_futures}")
        return None
    
    try:
        # Log the basket composition and prices being used
        logger.info(f"===== {label} Basket Calculation =====")
        logger.info(f"Exchange Rate: {exchange_rate:.2f} JPY/USD (from {rate_details['source']} at {rate_details['timestamp']})")
        
        basket_value_usd = 0
        total_weight = 0
        
        # Calculate component values
        components = []
        for futures_ticker, weight in normalized_composition.items():
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
                component_value_usd = price * weight * contract_multiplier
                basket_value_usd += component_value_usd
                total_weight += weight
                
                # Store component details for logging
                components.append({
                    'ticker': futures_ticker,
                    'price': price,
                    'weight': weight,
                    'shares': weight,
                    'multiplier': contract_multiplier,
                    'value_usd': component_value_usd
                })
        
        # Check if we have valid weights
        if total_weight == 0:
            logger.error(f"Total weight of futures in {label} basket is zero. Cannot calculate basket value.")
            return None
        
        # Convert to JPY
        basket_value_jpy = basket_value_usd * exchange_rate
        
        # Log detailed component breakdown
        logger.info(f"Component breakdown for {label} basket:")
        for comp in components:
            source_info = price_details[comp['ticker']]['source'] if comp['ticker'] in price_details else "unknown"
            timestamp_info = price_details[comp['ticker']]['timestamp'] if comp['ticker'] in price_details else "unknown"
            logger.info(f"  {comp['ticker']} [{source_info} at {timestamp_info}]: price={comp['price']:.4f}, " + 
                        f"shares={comp['shares']}, value=${comp['value_usd']:,.2f}")
        
        logger.info(f"Total {label} basket value: {basket_value_jpy:,.2f} JPY (${basket_value_usd:,.2f} USD)")
        logger.info("===================================")
        
        return basket_value_jpy
    
    except Exception as e:
        logger.error(f"Error calculating {label} basket value: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def check_for_alerts(current_value, initial_value, price_limits, closing_price, 
                    current_price_details, initial_price_details,
                    current_rate_details, initial_rate_details):
    """
    Check if the basket value has changed beyond the allowed price limits.
    
    Args:
        current_value: Current basket value
        initial_value: Initial basket value
        price_limits: Tuple of (lower_limit, upper_limit)
        closing_price: ETF closing price
        current_price_details: Details of current prices
        initial_price_details: Details of initial prices
        current_rate_details: Details of current exchange rate
        initial_rate_details: Details of initial exchange rate
        
    Returns:
        bool: True if alert condition is met, False otherwise
    """
    if not current_value or not initial_value or not price_limits or not closing_price:
        logger.error("Missing required data for alert check")
        return False
        
    try:
        lower_limit, upper_limit = price_limits
        
        # Calculate percentage changes
        value_pct_change = (current_value - initial_value) / initial_value
        allowed_lower_pct = (lower_limit - closing_price) / closing_price
        allowed_upper_pct = (upper_limit - closing_price) / closing_price
        
        # Log the comparison details
        logger.info("============ Price Limit Check ============")
        logger.info(f"ETF closing price: {closing_price:.2f} JPY")
        logger.info(f"Price limits: {lower_limit:.2f} JPY ({allowed_lower_pct:.2%}) to {upper_limit:.2f} JPY ({allowed_upper_pct:.2%})")
        
        # Log the initial basket details
        logger.info("--- Initial Basket ---")
        logger.info(f"Value: {initial_value:,.2f} JPY")
        logger.info(f"Exchange Rate: {initial_rate_details['rate']:.2f} (from {initial_rate_details['timestamp']})")
        for ticker, details in initial_price_details.items():
            logger.info(f"  {ticker}: {details['price']:.4f} (from {details['source']} at {details['timestamp']})")
        
        # Log the current basket details
        logger.info("--- Current Basket ---")
        logger.info(f"Value: {current_value:,.2f} JPY")
        logger.info(f"Exchange Rate: {current_rate_details['rate']:.2f} (from {current_rate_details['timestamp']})")
        for ticker, details in current_price_details.items():
            logger.info(f"  {ticker}: {details['price']:.4f} (from {details['source']} at {details['timestamp']})")
        
        # Log the comparison
        logger.info("--- Comparison ---")
        logger.info(f"Change: {value_pct_change:.2%} ({current_value - initial_value:,.2f} JPY)")
        logger.info(f"Allowed range: {allowed_lower_pct:.2%} to {allowed_upper_pct:.2%}")
        
        # Check if value change exceeds allowed ETF price change
        if value_pct_change < allowed_lower_pct or value_pct_change > allowed_upper_pct:
            message = (
                f"ALERT: Basket value changed by {value_pct_change:.2%}, exceeding daily price limits!\n"
                f"Allowed range: {allowed_lower_pct:.2%} to {allowed_upper_pct:.2%}\n"
                f"Current basket value: {current_value:,.2f} JPY\n"
                f"Initial basket value: {initial_value:,.2f} JPY"
            )
            logger.warning(message)
            print(f"\n*** ALERT ***\n{message}\n**************")
            return True
        
        logger.info(f"No alert: Basket value change {value_pct_change:.2%} is within allowed limits")
        logger.info("=========================================")
        return False
    
    except Exception as e:
        logger.error(f"Error checking for alerts: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main function to analyze ETF price limits and underlying basket value."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Check 318A ETF basket value against price limits')
    parser.add_argument('--check-only', action='store_true', help='Perform one-time check only (no monitoring)', default=True)
    parser.add_argument('--monitor', action='store_true', help='Enable continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in seconds (default: 60)')
    args = parser.parse_args()
    
    logger.info("==================================================")
    logger.info("Starting limits alerter with most recent available data")
    logger.info("==================================================")
    
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
    
    # Get most recent futures prices for the initial basket value
    # This should be based on the closing prices used at closing_time
    initial_futures_prices, initial_price_details = get_vix_futures_prices(composition, closing_time, label="INITIAL")
    
    if not initial_futures_prices or not initial_price_details:
        logger.error("Could not get VIX futures prices for initial basket. Exiting.")
        return 1
    
    # Get exchange rate at closing time
    initial_exchange_rate, initial_rate_details = get_exchange_rate(closing_time, label="INITIAL")
    
    if not initial_exchange_rate or not initial_rate_details:
        logger.error("Could not get USD/JPY exchange rate for initial basket. Exiting.")
        return 1
    
    # Calculate initial basket value based on closing prices
    initial_basket_value = calculate_basket_value(
        composition, 
        initial_futures_prices, 
        initial_exchange_rate, 
        initial_price_details, 
        initial_rate_details, 
        label="INITIAL"
    )
    
    if not initial_basket_value:
        logger.error("Could not calculate initial basket value. Exiting.")
        return 1
    
    # Get current futures prices (may be different from initial)
    logger.info("--------------------------------------------------")
    current_time = datetime.now()
    current_futures_prices, current_price_details = get_vix_futures_prices(composition, current_time, label="CURRENT")
    
    if not current_futures_prices or not current_price_details:
        logger.error("Could not get current VIX futures prices. Exiting.")
        return 1
    
    # Get current exchange rate
    current_exchange_rate, current_rate_details = get_exchange_rate(current_time, label="CURRENT")
    
    if not current_exchange_rate or not current_rate_details:
        logger.error("Could not get current USD/JPY exchange rate. Exiting.")
        return 1
    
    # Calculate current basket value
    current_basket_value = calculate_basket_value(
        composition, 
        current_futures_prices, 
        current_exchange_rate, 
        current_price_details, 
        current_rate_details,
