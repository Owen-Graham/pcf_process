import yfinance as yf
import traceback
import time
from datetime import datetime
import pandas as pd
import os
from common import setup_logging, SAVE_DIR, format_vix_data

# Set up logging
logger = setup_logging('yahoo_vix_downloader')

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
            'date': current_date.strftime("%Y-%m-%d"),
            'timestamp': current_date.strftime("%Y%m%d%H%M")
        }
        
        # Get the VIX index price as a fallback for the front month
        try:
            vix_data = yf.download("^VIX", period="1d", progress=False)
            if not vix_data.empty and 'Close' in vix_data.columns:
                # Extract the numeric value properly
                vix_value = float(vix_data['Close'].iloc[-1])
                futures_data['VX=F'] = vix_value
                futures_data['YAHOO:VIX'] = vix_value  # Standardized format
                logger.info(f"Yahoo: ^VIX = {vix_value}")
            else:
                logger.warning("Could not download ^VIX data from Yahoo Finance")
        except Exception as e:
            logger.error(f"Error downloading ^VIX: {str(e)}")
        
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
                    # Try downloading with a different period or interval
                    try:
                        logger.debug(f"Retrying {ticker} with different parameters")
                        data = yf.download(ticker, period="5d", interval="1d", progress=False)
                        
                        if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                            # Extract numeric value properly
                            settlement_price = float(data['Close'].iloc[-1])
                            
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
                                
                                logger.info(f"Yahoo (retry): {ticker} → {yahoo_ticker} = {settlement_price}")
                                futures_found = True
                            
                            # Also store with the original Yahoo ticker
                            futures_data[f"YAHOO:{ticker}"] = settlement_price
                    except Exception as retry_e:
                        logger.warning(f"Retry for {ticker} also failed: {str(retry_e)}")
        
        if futures_found:
            logger.info(f"Successfully retrieved VIX futures data from Yahoo Finance (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            # As a fallback, try the original contract notation
            logger.debug("Trying direct contract notation as a fallback")
            
            futures_tickers = [position_to_contract[i] for i in range(1, 4) if i in position_to_contract]
            
            for ticker in futures_tickers:
                try:
                    data = yf.download(ticker, period="1d", progress=False)
                    
                    if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                        # Extract numeric value properly
                        settlement_price = float(data['Close'].iloc[-1])
                        
                        # Standard format (already in the correct format)
                        futures_data[ticker] = settlement_price
                        
                        # Get the month code and year digit from the standard format
                        month_code = ticker[3:4]  # Extract 'H' from '/VXH5'
                        year_digit = ticker[4:5]  # Extract '5' from '/VXH5'
                        
                        # Store with the standardized source prefix
                        yahoo_ticker = f"YAHOO:VX{month_code}{year_digit}"
                        futures_data[yahoo_ticker] = settlement_price
                        
                        logger.info(f"Yahoo (direct): {ticker} → {yahoo_ticker} = {settlement_price}")
                        futures_found = True
                except Exception as e:
                    logger.warning(f"Error downloading {ticker} directly: {str(e)}")
            
            if futures_found:
                logger.info(f"Successfully retrieved some VIX futures using direct contract notation (processing took {time.time() - start_time:.2f}s)")
                return futures_data
            else:
                logger.warning("Could not retrieve any VIX futures data from Yahoo Finance")
                return futures_data  # Return with just
