"""
Utility functions for price limits and stock data handling.
Shared code used by price_limits_tracker.py and limits_alerter.py
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import logging
import traceback
import yfinance as yf

def get_daily_price_limits(base_price):
    """
    Determine daily price limits based on the base price from TSE rules in Q7.
    
    Args:
        base_price (float): Base price to calculate limits from
        
    Returns:
        tuple: (lower_limit, upper_limit) or None if base_price is invalid
    """
    if base_price is None or base_price <= 0:
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

def get_closing_price(symbol, lookback_days=7, logger=None):
    """
    Get closing price of a symbol on its exchange.
    
    Args:
        symbol (str): Stock symbol (e.g., "318A.T")
        lookback_days (int): Number of days to look back if no recent data
        logger: Optional logger instance for logging
        
    Returns:
        tuple: (closing_price, closing_date, price_limits) or (None, None, None) if failure
    """
    try:
        # Use a lookback period to find the most recent trading day data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        if logger:
            logger.info(f"Getting closing data for {symbol} from {start_date} to {end_date}")
        
        stock = yf.Ticker(symbol)
        hist = stock.history(start=start_date, end=end_date)
        
        if len(hist) == 0:
            if logger:
                logger.error(f"No data found for {symbol} in the last {lookback_days} days")
            return None, None, None
        
        # Extract closing price and date from the most recent data
        try:
            closing_price = float(hist['Close'].iloc[-1])
            if pd.isna(closing_price) or closing_price <= 0:
                if logger:
                    logger.error(f"Invalid closing price: {closing_price}")
                return None, None, None
        except Exception as e:
            if logger:
                logger.error(f"Error extracting closing price: {str(e)}")
            return None, None, None
            
        try:
            closing_date = hist.index[-1]
            # Convert to string format for consistency
            closing_date_str = closing_date.strftime('%Y-%m-%d')
            
            # Also keep the datetime object for timezone conversions if needed
            if closing_date.tzinfo is None:
                # If timezone is not set, assume it's UTC
                closing_datetime = pytz.utc.localize(closing_date)
            else:
                closing_datetime = closing_date
        except Exception as e:
            if logger:
                logger.error(f"Error extracting closing date: {str(e)}")
            return None, None, None
        
        # Calculate price limits for next trading day
        try:
            price_limits = get_daily_price_limits(closing_price)
            if price_limits is None:
                if logger:
                    logger.error("Failed to calculate price limits")
                return None, None, None
                
            lower_limit, upper_limit = price_limits
        except Exception as e:
            if logger:
                logger.error(f"Error calculating price limits: {str(e)}")
            return None, None, None
        
        if logger:
            logger.info(f"Most recent closing price of {symbol}: {closing_price:.2f} at {closing_date_str}")
            logger.info(f"Daily price limits: {lower_limit:.2f} to {upper_limit:.2f}")
        
        return closing_price, closing_datetime, price_limits
    
    except Exception as e:
        if logger:
            logger.error(f"Error getting stock data: {str(e)}")
            logger.error(traceback.format_exc())
        return None, None, None

def get_tse_closing_time(reference_date):
    """
    Get the Tokyo Stock Exchange closing time (15:00 JST) for the given date.
    
    Args:
        reference_date: The reference date to use
        
    Returns:
        datetime: The TSE closing time as a timezone-aware datetime
    """
    # Create a datetime at 15:00 JST on the reference date
    jst = pytz.timezone('Asia/Tokyo')
    
    # If reference_date doesn't have a timezone, assume it's already in JST
    if reference_date.tzinfo is None:
        reference_date = jst.localize(reference_date)
    else:
        # Convert to JST if needed
        reference_date = reference_date.astimezone(jst)
    
    # Set the time to 15:00 JST (TSE closing time)
    closing_time = datetime.combine(
        reference_date.date(),
        datetime.min.time().replace(hour=15, minute=0, second=0)
    )
    closing_time = jst.localize(closing_time)
    
    return closing_time
