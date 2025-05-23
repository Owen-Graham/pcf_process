import yfinance as yf
import traceback
import time
from datetime import datetime, timedelta
import pandas as pd
import os
import pytz
from common import setup_logging, SAVE_DIR, format_vix_data, MissingCriticalDataError, InvalidDataError
import requests # For requests.exceptions.RequestException

# Set up logging
logger = setup_logging('yahoo_vix_downloader')

def determine_yahoo_trading_date(timestamp):
    """
    Map a Yahoo Finance timestamp to the correct CBOE VIX futures trading date
    
    Args:
        timestamp: Timestamp from Yahoo Finance data
    
    Returns:
        str: Trading date in YYYY-MM-DD format
    """
    if timestamp is None:
        raise InvalidDataError("Timestamp cannot be None for determining Yahoo trading date.")
    
    # Convert to Central Time (CBOE's timezone)
    central = pytz.timezone('US/Central')
    if not timestamp.tzinfo:
        # If timestamp has no timezone, assume it's UTC
        timestamp = pytz.utc.localize(timestamp)
    
    ct_time = timestamp.astimezone(central)
    ct_date = ct_time.date()
    ct_hour = ct_time.hour
    
    # Trading session logic:
    # Sunday: After 17:00 CT = Monday's session
    # Monday-Thursday: After 17:00 CT = next day's session
    # Friday: After 17:00 CT = Monday's session (skip weekend)
    
    weekday = ct_date.weekday()  # 0=Monday, 6=Sunday
    
    if weekday == 6:  # Sunday
        if ct_hour >= 17:
            # After 5 PM Sunday -> Monday's session
            next_date = ct_date + timedelta(days=1)
            return next_date.strftime("%Y-%m-%d")
        else:
            # Before 5 PM Sunday -> Friday's session
            prev_date = ct_date - timedelta(days=2)
            return prev_date.strftime("%Y-%m-%d")
    elif weekday == 4:  # Friday
        if ct_hour >= 17:
            # After 5 PM Friday -> Monday's session
            next_date = ct_date + timedelta(days=3)
            return next_date.strftime("%Y-%m-%d")
        else:
            # Before 5 PM Friday -> Friday's session
            return ct_date.strftime("%Y-%m-%d")
    elif weekday == 5:  # Saturday
        # All Saturday data is for Monday
        monday_date = ct_date + timedelta(days=2)
        return monday_date.strftime("%Y-%m-%d")
    else:  # Monday-Thursday
        if ct_hour >= 17:
            # After 5 PM -> next day's session
            next_date = ct_date + timedelta(days=1)
            return next_date.strftime("%Y-%m-%d")
        else:
            # Before 5 PM -> current day's session
            return ct_date.strftime("%Y-%m-%d")

def download_vix_futures_from_yfinance():
    """
    Download VIX futures data from Yahoo Finance
    
    Returns:
        dict: Dictionary with VIX futures prices
    """
    start_time = time.time()
    try:
        logger.info("Downloading VIX futures data from Yahoo Finance...")
        
        # Get current date
        current_date = datetime.now()
        
        futures_data = {
            'timestamp': current_date.strftime("%Y%m%d%H%M")
        }
        
        # Track the timestamp of the data we download to determine trading date
        data_timestamp = None
        
        # Get the VIX index price as a fallback for the front month
        try:
            vix_data = yf.download("^VIX", period="1d", progress=False)
            if not vix_data.empty and 'Close' in vix_data.columns:
                # Extract the numeric value properly
                vix_value = float(vix_data['Close'].iloc[-1])
                futures_data['VX=F'] = vix_value
                futures_data['YAHOO:VIX'] = vix_value  # Standardized format
                logger.info(f"Yahoo: ^VIX = {vix_value}")
                
                # Save the timestamp for date validation
                data_timestamp = vix_data.index[-1]
                logger.info(f"Yahoo data timestamp: {data_timestamp}")
            else:
                logger.warning("Could not download ^VIX data from Yahoo Finance")
                raise MissingCriticalDataError("Could not download ^VIX index data from Yahoo Finance, which is essential (empty or no 'Close' column).")
        except Exception as e:
            logger.error(f"Error downloading ^VIX: {str(e)}")
            raise MissingCriticalDataError(f"Could not download ^VIX index data from Yahoo Finance due to error: {str(e)}") from e
        
        # Try different ticker patterns for VIX futures
        vix_patterns = [
            ['^VFTW1', '^VFTW2', '^VFTW3'],  # Front-month patterns
            ['^VXIND1', '^VXIND2', '^VXIND3']  # Alternative index patterns
        ]
        
        # Map the positions to VIX ticker format
        month_map = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 
                    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
        current_month = current_date.month
        year_suffix = str(current_date.year)[-1]
        
        # Try to map the positions to actual contract months
        position_to_contract = {}
        for i in range(1, 4):
            month_idx = (current_month + i - 1) % 12 + 1  # 1-indexed month
            if month_idx <= current_month:
                next_year_suffix = str(current_date.year + 1)[-1]
                position_to_contract[i] = f"/VX{month_map[month_idx]}{next_year_suffix}"
            else:
                position_to_contract[i] = f"/VX{month_map[month_idx]}{year_suffix}"
        
        logger.debug(f"Position to contract mapping: {position_to_contract}")
        
        futures_found = False
        
        # Try each of the ticker patterns
        for pattern_group in vix_patterns:
            logger.info(f"Trying ticker pattern group: {pattern_group}")
            
            for i, ticker in enumerate(pattern_group, 1):
                try:
                    logger.debug(f"Downloading {ticker} from Yahoo Finance")
                    data = yf.download(ticker, period="1d", progress=False)
                    
                    if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                        # Extract numeric value properly
                        settlement_price = float(data['Close'].iloc[-1])
                        
                        # Update data timestamp if not already set
                        if data_timestamp is None:
                            data_timestamp = data.index[-1]
                            logger.info(f"Yahoo data timestamp from {ticker}: {data_timestamp}")
                        
                        # Map to the corresponding VIX contract ticker format
                        if i in position_to_contract:
                            # Standard format
                            contract_ticker = position_to_contract[i]
                            # Get the month code and year digit from the standard format
                            month_code = contract_ticker[3:4]  # Extract 'H' from '/VXH5'
                            year_digit = contract_ticker[4:5]  # Extract '5' from '/VXH5'
                            
                            # Store with standard format
                            futures_data[contract_ticker] = settlement_price
                            
                            # Store with the standardized source prefix
                            yahoo_ticker = f"YAHOO:VX{month_code}{year_digit}"
                            futures_data[yahoo_ticker] = settlement_price
                            
                            logger.info(f"Yahoo: {ticker} → {yahoo_ticker} = {settlement_price}")
                            futures_found = True
                        
                        # Also store with the original Yahoo ticker without a series structure
                        futures_data[f"YAHOO:{ticker}"] = settlement_price
                    else:
                        logger.warning(f"No data found for ticker {ticker}")
                except Exception as e:
                    logger.warning(f"Error downloading {ticker} from Yahoo: {str(e)}")
                    # No retry attempts - we remove the fallback
        
        # If we have a timestamp, determine the trading date
        if data_timestamp is not None:
            try:
                trading_date = determine_yahoo_trading_date(data_timestamp)
                logger.info(f"Determined trading date for Yahoo data: {trading_date}")
                futures_data['date'] = trading_date
            except InvalidDataError as e: # Catch error from determine_yahoo_trading_date
                logger.error(f"Failed to determine trading date: {str(e)}")
                raise MissingCriticalDataError(f"Failed to determine Yahoo trading date: {str(e)}") from e
        else:
            # We need a valid trading date
            raise MissingCriticalDataError("No valid data timestamp obtained from Yahoo Finance to determine trading date.")
        
        if not futures_found:
            raise MissingCriticalDataError("No VIX futures contracts (e.g., ^VFTW1) were successfully retrieved from Yahoo Finance.")
            
        logger.info(f"Successfully retrieved VIX futures data from Yahoo Finance (processing took {time.time() - start_time:.2f}s)")
        return futures_data
    
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.error(f"Yahoo Finance download failed: {str(e)}")
        raise # Re-raise known critical errors
    except Exception as e: # Catch other yfinance/network issues
        logger.error(f"Unexpected error in Yahoo Finance download: {str(e)}")
        logger.error(traceback.format_exc())
        raise MissingCriticalDataError(f"An unexpected error occurred with Yahoo Finance VIX downloader: {str(e)}") from e

def save_yahoo_data(futures_data, save_dir=SAVE_DIR):
    """Save Yahoo futures data as CSV"""
    if not futures_data or len(futures_data) <= 2: # Check if only contains timestamp/date
        raise MissingCriticalDataError("No actual futures data provided to save_yahoo_data.")
    
    try:
        # Format data into standardized records
        records = format_vix_data(futures_data, "Yahoo")
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        csv_filename = f"vix_futures_yahoo_{timestamp}.csv"
        csv_path = os.path.join(save_dir, csv_filename)
        
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved Yahoo futures data to {csv_path}")
        
        return csv_path
    
    except Exception as e:
        logger.error(f"Error saving Yahoo data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    try:
        # Download VIX futures from Yahoo Finance
        yahoo_data = download_vix_futures_from_yfinance()
        
        # Save to CSV if data was found (should be true if no exception)
        if yahoo_data:
            csv_path = save_yahoo_data(yahoo_data)
            if csv_path:
                print(f"✅ Yahoo VIX futures data saved to: {csv_path}")
            else:
                # This path should ideally not be reached if save_yahoo_data raises MissingCriticalDataError
                print("❌ Failed to save Yahoo data (no path returned, though no direct error caught).")
                exit(1)
        else:
            # This path should also ideally not be reached
            print("❌ No VIX futures data found from Yahoo Finance (no data returned, though no direct error caught).")
            exit(1)
    except (MissingCriticalDataError, InvalidDataError) as e:
        print(f"❌ Yahoo VIX Downloader Error: {e}")
        exit(1)
    except Exception as e: # Catch any other unexpected errors
        print(f"❌ An unexpected error occurred: {e}")
        exit(1)
