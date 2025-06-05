import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import pytz
import time as time_module
import logging
import argparse
import traceback
import sys

# Import shared utility functions
from price_utils import get_daily_price_limits, get_closing_price, get_tse_closing_time
from common import setup_logging, SAVE_DIR, normalize_vix_ticker, find_latest_file, MissingCriticalDataError, get_tradingview_vix_contract_code

# Set up logging
logger = setup_logging('limits_alerter')


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

        # Use the shared utility function
        closing_price, closing_time, price_limits = get_closing_price(ticker, 7, logger)

        if not all([closing_price, closing_time, price_limits]):
            return None, None, None

        # Convert to JST timezone for consistency
        try:
            jst = pytz.timezone('Asia/Tokyo')
            closing_time_jst = closing_time.astimezone(jst)
        except Exception as e:
            logger.error(f"Error converting to JST timezone: {str(e)}")
            return None, None, None

        logger.info(f"Most recent closing price of {ticker}: {closing_price:.2f} JPY at {closing_time_jst}")
        lower_limit, upper_limit = price_limits
        logger.info(f"Daily price limits: {lower_limit:.2f} to {upper_limit:.2f} JPY")

        return closing_price, closing_time_jst, price_limits

    except Exception as e:
        logger.error(f"Error getting ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None, None


def get_latest_us_market_time(reference_time):
    """Get the latest US market time based on the reference time."""
    # This function is specific to limits_alerter.py and not shared
    # It returns a time relevant to US market hours for VIX futures
    eastern = pytz.timezone('US/Eastern')

    # If reference_time doesn't have a timezone, assume it's UTC
    if reference_time.tzinfo is None:
        reference_time = pytz.utc.localize(reference_time)

    # Convert to Eastern Time for US market
    et_time = reference_time.astimezone(eastern)

    # US market opens at 9:30 AM ET and closes at 4:00 PM ET
    # Return the most appropriate time depending on current time
    market_open = et_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = et_time.replace(hour=16, minute=0, second=0, microsecond=0)

    weekday = et_time.weekday()  # 0=Monday, 6=Sunday

    # If it's a weekend, or before market open, or after market close
    if weekday >= 5 or et_time < market_open or et_time > market_close:
        # Return the most recent market close time
        if weekday == 5:  # Saturday
            # Return Friday's close
            days_back = 1
        elif weekday == 6:  # Sunday
            # Return Friday's close
            days_back = 2
        elif et_time < market_open:
            # Before market open, return previous day's close
            if weekday == 0:  # Monday before open
                days_back = 3  # Go back to Friday
            else:
                days_back = 1
        else:  # After market close
            days_back = 0  # Today's close

        # Calculate the market close time
        market_time = et_time - timedelta(days=days_back)
        market_time = market_time.replace(hour=16, minute=0, second=0, microsecond=0)
    else:
        # During market hours, return current time
        market_time = et_time

    logger.info(f"Using US market time: {market_time} ET")
    return market_time


def get_vix_futures_prices(composition, reference_time, label=""): # reference_time is currently unused with CSVs
    """
    Get VIX futures prices from the latest cboe_vix_futures_YYYYMMDDHHMM.csv file.

    Args:
        composition: Dictionary mapping futures tickers to weights (used to know which tickers to look for)
        reference_time: Not currently used with CSV file loading. Kept for interface consistency.
        label: Optional label for logging (e.g., "Initial" or "Current")

    Returns:
        tuple: (prices_dict, details_dict) or (None, None) if failure
    """
    try:
        futures_prices = {}
        price_details = {}

        logger.info(f"===== Getting {label} VIX Futures Prices from CSV =====")

        # Find the latest VIX futures CSV file
        # Files are named like cboe_vix_futures_YYYYMMDDHHMM.csv
        vix_file_pattern = "cboe_vix_futures_*.csv"
        latest_vix_file = find_latest_file(vix_file_pattern, directory=SAVE_DIR)

        if not latest_vix_file:
            logger.error(f"No VIX futures CSV file found with pattern {vix_file_pattern} in {SAVE_DIR}")
            raise MissingCriticalDataError(f"No VIX futures CSV file found for {label} prices.")

        logger.info(f"Reading VIX futures data from: {latest_vix_file}")
        df_vix = pd.read_csv(latest_vix_file)

        if df_vix.empty:
            logger.error(f"VIX futures file {latest_vix_file} is empty.")
            raise MissingCriticalDataError(f"VIX futures file {latest_vix_file} is empty for {label} prices.")

        # Ensure required columns are present
        required_cols = ['timestamp', 'price_date', 'symbol', 'price']
        if not all(col in df_vix.columns for col in required_cols):
            logger.error(f"Missing required columns in {latest_vix_file}. Required: {required_cols}")
            raise MissingCriticalDataError(f"Malformed VIX futures CSV {latest_vix_file}.")

        # The 'timestamp' in cboe_vix_futures_*.csv is the script execution time.
        # The 'price_date' is the date extracted from TradingView for the contract.
        # We should use 'price_date' for the 'timestamp' in price_details,
        # and the file's 'timestamp' (script run time) can be part of the source if needed.

        for _index, row in df_vix.iterrows():
            raw_ticker = row['symbol']  # e.g., CBOE:VXM2025

            # Attempt to normalize the ticker from "CBOE:VXM2025" to "VXM5"
            # The normalize_vix_ticker expects "VXM2025" or "VXM25"
            contract_part = raw_ticker.split(':')[-1] # Should give "VXM2025"
            if not contract_part.startswith("VX"):
                logger.warning(f"Skipping unexpected symbol format in VIX CSV: {raw_ticker}")
                continue

            try:
                normalized_ticker = normalize_vix_ticker(contract_part) # e.g. VXM2025 -> VXM5
            except Exception as e:
                logger.warning(f"Could not normalize ticker {contract_part} from {raw_ticker}: {e}")
                continue

            price = row['price']
            price_timestamp_str = row['price_date'] # This is the date of the price on TradingView

            # Convert price_date to datetime object
            try:
                price_dt = pd.to_datetime(price_timestamp_str).to_pydatetime()
            except ValueError:
                logger.warning(f"Could not parse price_date '{price_timestamp_str}' for {normalized_ticker}. Skipping.")
                continue

            if pd.isna(price) or price <= 0:
                logger.warning(f"Invalid price for {normalized_ticker} in {latest_vix_file}: {price}")
                continue

            # Store if this ticker is in our composition
            # Composition keys might be like 'VXM5' or 'VXN2025'
            # We need to check against normalized_ticker which is 'VXM5'
            if normalized_ticker in composition or contract_part in composition: # Check both forms
                 # Prefer normalized_ticker for consistency if available
                key_to_use = normalized_ticker

                futures_prices[key_to_use] = float(price)
                price_details[key_to_use] = {
                    'price': float(price),
                    'timestamp': price_dt, # This is the market data's date
                    'source': latest_vix_file,
                    # 'position' was from yfinance logic, not directly available here unless derived
                }
                logger.info(f"Loaded price for {key_to_use} ({raw_ticker}): {price:.4f} from {price_dt} (source: {latest_vix_file})")
            else:
                # Log if a price is in the CSV but not in the current composition (for info)
                logger.debug(f"Price for {normalized_ticker} ({raw_ticker}) found in CSV but not in current composition: {composition.keys()}")


        # Check if we have all the prices we need for the composition
        # Composition keys are already normalized by get_etf_composition
        missing_futures = [ticker for ticker in composition.keys() if ticker not in futures_prices]
        if missing_futures:
            logger.error(f"Missing prices for futures (after reading {latest_vix_file}): {missing_futures}")
            logger.error(f"Available prices in CSV were for: {list(df_vix['symbol'])}")
            logger.error(f"Normalized and loaded tickers: {list(futures_prices.keys())}")
            return None, None # Strict failure if any required future is missing

        logger.info(f"===== {label} VIX Futures Prices Summary (from {latest_vix_file}) =====")
        for ticker, details in price_details.items():
            logger.info(f"{ticker} [{details['source']}]: {details['price']:.4f} at {details['timestamp']}")
        logger.info("===================================================================")

        return futures_prices, price_details

    except MissingCriticalDataError: # Already logged
        return None, None
    except FileNotFoundError:
        logger.error(f"VIX futures CSV file not found for {label} prices.")
        return None, None
    except pd.errors.EmptyDataError:
        logger.error(f"VIX futures CSV file is empty for {label} prices.")
        return None, None
    except Exception as e:
        logger.error(f"Error getting {label} VIX futures prices from CSV: {str(e)}")
        logger.error(traceback.format_exc())
        return None, None

def get_exchange_rate(reference_time, label=""): # reference_time is currently unused with CSVs
    """
    Get the USD/JPY exchange rate from the latest tradingview_fx_data_YYYYMMDDHHMM.csv file.

    Args:
        reference_time: Not currently used with CSV file loading. Kept for interface consistency.
        label: Optional label for logging (e.g., "Initial" or "Current")

    Returns:
        tuple: (exchange_rate, details) or (None, None) if failure
    """
    try:
        logger.info(f"===== Getting {label} USD/JPY Exchange Rate from CSV =====")

        # Find the latest FX CSV file
        # Files are named like tradingview_fx_data_YYYYMMDDHHMM.csv
        fx_file_pattern = "tradingview_fx_data_*.csv"
        latest_fx_file = find_latest_file(fx_file_pattern, directory=SAVE_DIR)

        if not latest_fx_file:
            logger.error(f"No FX data CSV file found with pattern {fx_file_pattern} in {SAVE_DIR}")
            raise MissingCriticalDataError(f"No FX data CSV file found for {label} rate.")

        logger.info(f"Reading FX data from: {latest_fx_file}")
        df_fx = pd.read_csv(latest_fx_file)

        if df_fx.empty:
            logger.error(f"FX data file {latest_fx_file} is empty.")
            raise MissingCriticalDataError(f"FX data file {latest_fx_file} is empty for {label} rate.")

        # Ensure required columns are present
        # columns: timestamp, date, source_url, pair, label, rate
        required_cols = ['timestamp', 'date', 'pair', 'rate', 'source_url']
        if not all(col in df_fx.columns for col in required_cols):
            logger.error(f"Missing required columns in {latest_fx_file}. Required: {required_cols}")
            raise MissingCriticalDataError(f"Malformed FX data CSV {latest_fx_file}.")

        # Find the USDJPY rate
        usdjpy_data = df_fx[df_fx['pair'] == 'USDJPY']
        if usdjpy_data.empty:
            logger.error(f"No USDJPY data found in {latest_fx_file}")
            raise MissingCriticalDataError(f"No USDJPY data in {latest_fx_file} for {label} rate.")

        # Get the latest USDJPY entry if multiple (should usually be one)
        latest_usdjpy_row = usdjpy_data.iloc[-1]
        exchange_rate = latest_usdjpy_row['rate']

        # 'date' in the CSV is the market date of the rate
        # 'timestamp' in the CSV is the script execution time
        rate_timestamp_str = latest_usdjpy_row['date']
        source_url = latest_usdjpy_row['source_url']

        try:
            rate_dt = pd.to_datetime(rate_timestamp_str).to_pydatetime()
        except ValueError:
            logger.warning(f"Could not parse rate date '{rate_timestamp_str}' for USDJPY. Skipping.")
            return None, None

        if pd.isna(exchange_rate) or exchange_rate <= 0:
            logger.error(f"Invalid exchange rate for USDJPY in {latest_fx_file}: {exchange_rate}")
            return None, None

        rate_details = {
            'rate': float(exchange_rate),
            'timestamp': rate_dt, # This is the market data's date
            'source': latest_fx_file, # File source
            'original_source_url': source_url # e.g. https://www.tradingview.com/symbols/USDJPY/
        }

        logger.info(f"{label} USD/JPY exchange rate: {exchange_rate:.3f} from {rate_dt} (source: {latest_fx_file})")
        return float(exchange_rate), rate_details

    except MissingCriticalDataError: # Already logged
        return None, None
    except FileNotFoundError:
        logger.error(f"FX data CSV file not found for {label} rate.")
        return None, None
    except pd.errors.EmptyDataError:
        logger.error(f"FX data CSV file is empty for {label} rate.")
        return None, None
    except Exception as e:
        logger.error(f"Error getting {label} USD/JPY exchange rate from CSV: {str(e)}")
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
        logger.info(
            f"Exchange Rate: {exchange_rate:.2f} JPY/USD (from {rate_details['source']} at {rate_details['timestamp']})")

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
            timestamp_info = price_details[comp['ticker']]['timestamp'] if comp[
                                                                               'ticker'] in price_details else "unknown"
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
        logger.info(
            f"Price limits: {lower_limit:.2f} JPY ({allowed_lower_pct:.2%}) to {upper_limit:.2f} JPY ({allowed_upper_pct:.2%})")

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


def monitor_basket_value(composition, initial_basket_value, price_limits, closing_price,
                         initial_price_details, initial_rate_details, check_interval=60):
    """Start continuous monitoring of the basket value."""
    logger.info(f"Starting monitoring. Will check every {check_interval} seconds.")

    try:
        alert_counter = 0
        max_alerts = 5

        while alert_counter < max_alerts:
            current_time = datetime.now()
            us_market_time = get_latest_us_market_time(current_time)

            # Get current futures prices and exchange rate
            current_futures_prices, current_price_details = get_vix_futures_prices(
                composition,
                us_market_time,
                label="MONITOR"
            )

            if not current_futures_prices or not current_price_details:
                logger.error("Failed to get current futures prices. Aborting monitor.")
                return

            current_exchange_rate, current_rate_details = get_exchange_rate(
                us_market_time,
                label="MONITOR"
            )

            if not current_exchange_rate or not current_rate_details:
                logger.error("Failed to get current exchange rate. Aborting monitor.")
                return

            # Calculate current basket value
            current_basket_value = calculate_basket_value(
                composition,
                current_futures_prices,
                current_exchange_rate,
                current_price_details,
                current_rate_details,
                label="MONITOR"
            )

            if not current_basket_value:
                logger.error("Failed to calculate current basket value. Aborting monitor.")
                return

            # Check for alerts
            is_alert = check_for_alerts(
                current_basket_value,
                initial_basket_value,
                price_limits,
                closing_price,
                current_price_details,
                initial_price_details,
                current_rate_details,
                initial_rate_details
            )

            if is_alert:
                alert_counter += 1
                logger.warning(f"Alert {alert_counter} of {max_alerts}")

                # Save alert to file (simplified version for monitoring)
                alert_file = os.path.join(SAVE_DIR, f"price_alert_monitor_{datetime.now().strftime('%Y%m%d%H%M')}.log")
                with open(alert_file, "w") as f:
                    f.write(f"PRICE LIMIT ALERT (Monitoring)\n")
                    f.write(f"Time: {current_time}\n")
                    f.write(f"Basket value: {current_basket_value:.2f} JPY\n")
                    f.write(f"Initial value: {initial_basket_value:.2f} JPY\n")
                    pct_change = (current_basket_value - initial_basket_value) / initial_basket_value
                    f.write(f"Change: {pct_change:.2%}\n")
                    f.write(f"Price limits: {price_limits[0]:.2f} to {price_limits[1]:.2f} JPY\n")
                    allowed_lower_pct = (price_limits[0] - closing_price) / closing_price
                    allowed_upper_pct = (price_limits[1] - closing_price) / closing_price
                    f.write(f"Allowed range: {allowed_lower_pct:.2%} to {allowed_upper_pct:.2%}\n")

            # Wait for next check
            time_module.sleep(check_interval)

    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user.")
    except Exception as e:
        logger.error(f"Error in monitoring loop: {str(e)}")
        logger.error(traceback.format_exc())


def main():
    """Main function to analyze ETF price limits and underlying basket value."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Check 318A ETF basket value against price limits')
    parser.add_argument('--check-only', action='store_true', help='Perform one-time check only (no monitoring)',
                        default=True)
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

    logger.info(f"ETF composition (normalized) for {closing_time.strftime('%Y-%m-%d')}: {composition}")

    # Convert normalized contract codes from composition to TradingView format
    # This list would be used if parse_tradingview.py were invoked directly.
    tradingview_contract_codes = []
    if composition:
        for normalized_code in composition.keys():
            try:
                tv_code = get_tradingview_vix_contract_code(normalized_code)
                tradingview_contract_codes.append(tv_code)
            except ValueError as e:
                logger.error(f"Error converting normalized code {normalized_code} to TradingView code: {e}")
        logger.info(f"TradingView-formatted VIX contract codes for parse_tradingview.py: {tradingview_contract_codes}")
    else:
        logger.warning("ETF composition is empty, cannot generate TradingView contract codes.")


    # Calculate the exact 15:00 JST time for the initial basket valuation
    # This ensures we use the prices at TSE closing time
    tse_closing_time = get_tse_closing_time(closing_time)

    # Get futures prices for the initial basket value at TSE closing time
    logger.info("Getting futures prices for INITIAL basket value at TSE closing time")
    initial_futures_prices, initial_price_details = get_vix_futures_prices(
        composition,
        tse_closing_time,
        label="INITIAL"
    )

    if not initial_futures_prices or not initial_price_details:
        logger.error("Could not get VIX futures prices for initial basket. Exiting.")
        return 1

    # Get exchange rate at TSE closing time
    initial_exchange_rate, initial_rate_details = get_exchange_rate(
        tse_closing_time,
        label="INITIAL"
    )

    if not initial_exchange_rate or not initial_rate_details:
        logger.error("Could not get USD/JPY exchange rate for initial basket. Exiting.")
        return 1

    # Calculate initial basket value based on TSE closing time prices
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

    # Get current futures prices - now using the most current price available
    logger.info("--------------------------------------------------")
    logger.info("Getting CURRENT futures prices (most recent available)")

    # Just use current time as reference, but the function will use fast_info.last_price
    current_time = datetime.now()

    current_futures_prices, current_price_details = get_vix_futures_prices(
        composition,
        current_time,
        label="CURRENT"
    )

    if not current_futures_prices or not current_price_details:
        logger.error("Could not get current VIX futures prices. Exiting.")
        return 1

    # Get current exchange rate - also using fast_info.last_price
    current_exchange_rate, current_rate_details = get_exchange_rate(
        current_time,
        label="CURRENT"
    )

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
        label="CURRENT"
    )

    if not current_basket_value:
        logger.error("Could not calculate current basket value. Exiting.")
        return 1

    # Calculate shares outstanding from ETF characteristics
    shares_outstanding = 0
    nav_per_share = 0 # Initialize nav_per_share
    try:
        etf_file = os.path.join(SAVE_DIR, "etf_characteristics_master.csv")
        if not os.path.exists(etf_file):
            logger.warning(f"ETF characteristics file not found: {etf_file}. Cannot calculate NAV per share.")
        else:
            df_etf_char = pd.read_csv(etf_file)
            if df_etf_char.empty:
                logger.warning(f"ETF characteristics file {etf_file} is empty. Cannot calculate NAV per share.")
            elif 'shares_outstanding' not in df_etf_char.columns:
                logger.warning(f"'shares_outstanding' column not found in {etf_file}. Cannot calculate NAV per share.")
            elif 'fund_date' not in df_etf_char.columns:
                logger.warning(f"'fund_date' column not found in {etf_file}. Cannot reliably get latest shares_outstanding.")
            else:
                try:
                    df_etf_char['date'] = pd.to_datetime(df_etf_char['fund_date'], format='%Y%m%d')
                     # Use closing_time (from get_etf_closing_data) as the reference for selecting the latest record.
                    # Ensure closing_time is timezone naive for comparison if df_etf_char['date'] is naive.
                    ref_date_for_shares = closing_time.replace(tzinfo=None) if closing_time.tzinfo else closing_time

                    df_etf_char_sorted = df_etf_char[df_etf_char['date'] <= ref_date_for_shares].sort_values('date', ascending=False)

                    if df_etf_char_sorted.empty:
                        logger.warning(f"No 'shares_outstanding' data found in {etf_file} on or before {ref_date_for_shares.strftime('%Y-%m-%d')}.")
                    else:
                        latest_record = df_etf_char_sorted.iloc[0]
                        shares_outstanding = latest_record['shares_outstanding']
                        fund_date_of_shares = latest_record['date']
                        if pd.isna(shares_outstanding) or shares_outstanding <= 0:
                            logger.warning(f"Invalid 'shares_outstanding' value ({shares_outstanding}) found for fund_date {fund_date_of_shares.strftime('%Y-%m-%d')}.")
                        else:
                            nav_per_share = current_basket_value / shares_outstanding
                            logger.info(
                                f"New TSE:318A Price (Estimated NAV per share): {nav_per_share:.2f} JPY "
                                f"(Basket: {current_basket_value:,.2f} JPY / Shares: {shares_outstanding:,.0f} "
                                f"as of fund_date {fund_date_of_shares.strftime('%Y-%m-%d')})"
                            )
                except Exception as e_parse:
                    logger.warning(f"Error processing ETF characteristics file for shares_outstanding: {str(e_parse)}")
                    logger.error(traceback.format_exc())

    except Exception as e:
        logger.warning(f"Could not calculate NAV per share due to an unexpected error: {str(e)}")
        logger.error(traceback.format_exc())

    # Check for alerts (compare current basket value to price limits)
    is_alert = check_for_alerts(
        current_basket_value,
        initial_basket_value,
        price_limits,
        closing_price,
        current_price_details,
        initial_price_details,
        current_rate_details,
        initial_rate_details
    )

    # If any alert was detected, save it to a file
    if is_alert:
        pct_change = (current_basket_value - initial_basket_value) / initial_basket_value
        current_time_str = datetime.now().astimezone(pytz.timezone('Asia/Tokyo')).strftime("%Y-%m-%d %H:%M:%S %Z")

        # Define limits for cleaner code below
        lower_limit, upper_limit = price_limits
        allowed_lower_pct = (lower_limit - closing_price) / closing_price
        allowed_upper_pct = (upper_limit - closing_price) / closing_price

        # Save alert to file with detailed information
        alert_file = os.path.join(SAVE_DIR, f"price_alert_{datetime.now().strftime('%Y%m%d%H%M')}.log")
        with open(alert_file, "w") as f:
            f.write(f"PRICE LIMIT ALERT\n")
            f.write(f"Alert Time: {current_time_str}\n\n")

            f.write(f"=== ETF Information ===\n")
            f.write(f"ETF: 318A (Simplex VIX Short-Term Futures ETF)\n")
            f.write(f"Closing Price: {closing_price:.2f} JPY\n")
            f.write(f"Price Limits: {lower_limit:.2f} JPY to {upper_limit:.2f} JPY\n")
            f.write(f"Allowed Range: {allowed_lower_pct:.2%} to {allowed_upper_pct:.2%}\n\n")

            f.write(f"=== Basket Value Change ===\n")
            f.write(f"Initial Basket Value: {initial_basket_value:,.2f} JPY\n")
            f.write(f"Current Basket Value: {current_basket_value:,.2f} JPY\n")
            f.write(f"Change: {pct_change:.2%} ({current_basket_value - initial_basket_value:,.2f} JPY)\n")
            if nav_per_share > 0 : # nav_per_share might be 0 if shares_outstanding was not available
                f.write(f"New TSE:318A Price (Estimated NAV per share): {nav_per_share:.2f} JPY\n\n")
            else:
                f.write(f"New TSE:318A Price (Estimated NAV per share): Not available\n\n")

            f.write(f"=== Initial Futures Prices ===\n")
            f.write(f"Initial Reference Time: {tse_closing_time}\n")
            for ticker, details in initial_price_details.items():
                f.write(f"  {ticker}: {details['price']:.4f} (from {details['source']} at {details['timestamp']})\n")
            f.write(f"\n")

            f.write(f"=== Current Futures Prices ===\n")
            f.write(f"Current Reference Time: {current_time}\n")
            for ticker, details in current_price_details.items():
                f.write(f"  {ticker}: {details['price']:.4f} (from {details['source']} at {details['timestamp']})\n")
            f.write(f"\n")

            f.write(f"=== Exchange Rates ===\n")
            f.write(
                f"Initial Rate: {initial_exchange_rate:.2f} JPY/USD (from {initial_rate_details['source']} at {initial_rate_details['timestamp']})\n")
            f.write(
                f"Current Rate: {current_exchange_rate:.2f} JPY/USD (from {current_rate_details['source']} at {current_rate_details['timestamp']})\n")

        logger.info(f"Price limit alert detected and saved to {alert_file}")
    else:
        logger.info("No price limit alerts detected")

    # Start monitoring if requested
    if args.monitor:
        logger.info("Starting continuous monitoring")
        monitor_basket_value(
            composition,
            initial_basket_value,
            price_limits,
            closing_price,
            initial_price_details,
            initial_rate_details,
            args.interval
        )

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
