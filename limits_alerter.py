import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import time
import logging
import argparse
from common import convert_futures_ticker_to_yahoo

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    """Get the ETF composition from the master CSV for the given date."""
    try:
        # Read the ETF characteristics master CSV
        df = pd.read_csv("etf_characteristics_master.csv")
        
        # Convert date column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Find the row for the given date
        date_str = date.strftime('%Y-%m-%d')
        closest_date = df.loc[df['date'] <= date_str, 'date'].max()
        
        if pd.isna(closest_date):
            logger.error(f"No composition data found for date {date_str} or earlier")
            return None
        
        # Get the row for the closest date
        composition_row = df[df['date'] == closest_date].iloc[0]
        
        # Extract futures tickers and weights
        composition = {}
        for col in df.columns:
            if '_weight' in col:
                # Extract the futures ticker
                futures_key = col.replace('_weight', '')
                if futures_key in composition_row and not pd.isna(composition_row[futures_key]):
                    futures_ticker = composition_row[futures_key]
                    weight = composition_row[col]
                    if not pd.isna(weight) and weight > 0:
                        composition[futures_ticker] = weight
        
        return composition
    
    except Exception as e:
        logger.error(f"Error reading ETF composition: {e}")
        return None

def get_etf_closing_data():
    """Get closing price of 318A ETF on Tokyo Stock Exchange."""
    try:
        ticker = "318A.T"  # Correct ticker for 318A
        
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        
        if len(hist) == 0:
            logger.error(f"No data found for {ticker}")
            return None, None, None
        
        # Extract closing price and time
        closing_price = hist['Close'][-1]
        closing_time = hist.index[-1]
        
        # Convert to JST timezone
        jst = pytz.timezone('Asia/Tokyo')
        closing_time_jst = closing_time.astimezone(jst)
        
        # Get time around 15:00 JST (market close)
        market_close_hour = 15
        if closing_time_jst.hour != market_close_hour:
            logger.warning(f"Closing time is not at expected 15:00 JST: {closing_time_jst}")
        
        # Calculate price limits for next trading day
        lower_limit, upper_limit = get_daily_price_limits(closing_price)
        
        logger.info(f"Closing price of {ticker}: {closing_price:.2f} JPY at {closing_time_jst}")
        logger.info(f"Daily price limits for next trading day: {lower_limit:.2f} to {upper_limit:.2f} JPY")
        
        return closing_price, closing_time_jst, (lower_limit, upper_limit)
    
    except Exception as e:
        logger.error(f"Error getting ETF data: {e}")
        return None, None, None

def get_vix_futures_prices(composition, reference_time):
    """Get VIX futures prices from Yahoo Finance using proper ticker mapping."""
    try:
        futures_prices = {}
        
        # Format date for yfinance
        date_str = reference_time.strftime('%Y-%m-%d')
        next_day = (reference_time + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Get prices for each futures contract in the composition
        for futures_ticker, weight in composition.items():
            # Convert to Yahoo Finance ticker format
            yahoo_ticker = convert_futures_ticker_to_yahoo(futures_ticker)
            
            # Get data
            future = yf.Ticker(yahoo_ticker)
            future_data = future.history(period="1d", start=date_str, end=next_day)
            
            if len(future_data) > 0:
                futures_prices[futures_ticker] = future_data['Close'][-1]
                logger.info(f"Retrieved price for {futures_ticker} ({yahoo_ticker}): {futures_prices[futures_ticker]:.2f}")
            else:
                logger.warning(f"No data found for {yahoo_ticker} ({futures_ticker})")
        
        if not futures_prices:
            logger.error("Could not retrieve any futures prices")
            return None
            
        return futures_prices
    
    except Exception as e:
        logger.error(f"Error getting VIX futures prices: {e}")
        return None

def get_exchange_rate(reference_time):
    """Get USD/JPY exchange rate at the specified time."""
    try:
        usdjpy = yf.Ticker("USDJPY=X")
        
        # Convert time to date string
        date_str = reference_time.strftime('%Y-%m-%d')
        next_day = (reference_time + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Get exchange rate data
        fx_data = usdjpy.history(period="1d", start=date_str, end=next_day)
        
        if len(fx_data) == 0:
            logger.warning(f"No USD/JPY data found for {date_str}")
            return None
        
        # Get the exchange rate
        exchange_rate = fx_data['Close'][-1]
        logger.info(f"USD/JPY exchange rate: {exchange_rate:.2f}")
        
        return exchange_rate
    
    except Exception as e:
        logger.error(f"Error getting exchange rate: {e}")
        return None

def calculate_basket_value(composition, futures_prices, exchange_rate):
    """Calculate the value of the VIX futures basket in JPY."""
    if not futures_prices or not exchange_rate:
        return None
    
    basket_value_usd = 0
    total_weight = 0
    
    for futures_ticker, weight in composition.items():
        if futures_ticker in futures_prices:
            basket_value_usd += futures_prices[futures_ticker] * weight
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
        while True:
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
                check_for_alerts(current_basket_value, initial_basket_value, price_limits, closing_price)
            
            # Wait for next check
            time.sleep(check_interval)
    
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user.")
    except Exception as e:
        logger.error(f"Error in monitoring loop: {e}")

def main():
    """Main function to analyze ETF price limits and underlying basket value."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Monitor 318A ETF and underlying basket value')
    parser.add_argument('--monitor', action='store_true', help='Enable continuous monitoring')
    parser.add_argument('--interval', type=int, default=60, help='Monitoring interval in seconds (default: 60)')
    args = parser.parse_args()
    
    # Get ETF closing data
    closing_price, closing_time, price_limits = get_etf_closing_data()
    
    if not all([closing_price, closing_time, price_limits]):
        logger.error("Could not get ETF data. Exiting.")
        return
    
    # Get the ETF composition from the master CSV
    composition = get_etf_composition(closing_time)
    
    if not composition:
        logger.error("Could not get ETF composition. Exiting.")
        return
    
    logger.info(f"ETF composition for {closing_time.strftime('%Y-%m-%d')}: {composition}")
    
    # Get initial futures prices and exchange rate
    futures_prices = get_vix_futures_prices(composition, closing_time)
    exchange_rate = get_exchange_rate(closing_time)
    
    if not futures_prices:
        logger.error("Could not get VIX futures prices. Exiting.")
        return
    
    if not exchange_rate:
        logger.error("Could not get USD/JPY exchange rate. Exiting.")
        return
    
    # Calculate initial basket value
    initial_basket_value = calculate_basket_value(composition, futures_prices, exchange_rate)
    
    if not initial_basket_value:
        logger.error("Could not calculate initial basket value. Exiting.")
        return
    
    logger.info(f"Initial basket value: {initial_basket_value:.2f} JPY")
    logger.info(f"Price limits for next trading day: {price_limits[0]:.2f} to {price_limits[1]:.2f} JPY")
    
    # Current value check (single run)
    current_time = datetime.now().astimezone(pytz.timezone('Asia/Tokyo'))
    current_futures_prices = get_vix_futures_prices(composition, current_time)
    current_exchange_rate = get_exchange_rate(current_time)
    
    if current_futures_prices and current_exchange_rate:
        current_basket_value = calculate_basket_value(composition, current_futures_prices, current_exchange_rate)
        if current_basket_value:
            pct_change = (current_basket_value - initial_basket_value) / initial_basket_value
            logger.info(f"Current basket value: {current_basket_value:.2f} JPY ({pct_change:.2%} change)")
            check_for_alerts(current_basket_value, initial_basket_value, price_limits, closing_price)
    
    # Start monitoring if requested
    if args.monitor:
        monitor_basket_value(composition, initial_basket_value, price_limits, closing_price, args.interval)

if __name__ == "__main__":
    main()
