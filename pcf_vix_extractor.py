import os
import glob
import re
import pandas as pd
import traceback
from datetime import datetime
import logging
from common import setup_logging, SAVE_DIR, format_vix_data, normalize_vix_ticker, MissingCriticalDataError, InvalidDataError
from etf_characteristics_parser import find_latest_etf_file

# Set up logging
logger = setup_logging('pcf_vix_extractor')

# Define expected PCF format based on the provided example
PCF_COLUMNS = [
    'ETF Code', 'ETF Name', 'Fund Cash Component', 
    'Shares Outstanding', 'Fund Date', 'Unnamed: 5', 'Unnamed: 6'
]

def extract_fund_date_from_pcf(file_path):
    """
    Extract the Fund Date from PCF file and convert to YYYY-MM-DD format
    
    Args:
        file_path: Path to PCF file
    
    Returns:
        str: Fund date in YYYY-MM-DD format or None if not found/invalid
    """
    try:
        if not file_path or not os.path.exists(file_path):
            raise MissingCriticalDataError(f"PCF file not found: {file_path}")
        
        # Read just the header rows where Fund Date is typically located
        header_df = pd.read_csv(file_path, nrows=2)
        
        # Check if Fund Date column exists
        if 'Fund Date' in header_df.columns and not header_df['Fund Date'].isna().all():
            fund_date = str(header_df['Fund Date'].iloc[0]).strip()
            logger.info(f"Found raw Fund Date: {fund_date}")
            
            # Convert MM/DD/YYYY format to YYYY-MM-DD
            if '/' in fund_date:
                try:
                    date_parts = fund_date.split('/')
                    if len(date_parts) == 3:
                        month, day, year = date_parts
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception: # Catch specific error if possible, otherwise general
                    raise InvalidDataError(f"Could not parse Fund Date string '{fund_date}' (MM/DD/YYYY format) to YYYY-MM-DD.")
            
            # Try to handle YYYYMMDD format
            if fund_date.isdigit() and len(fund_date) == 8:
                try:
                    year = fund_date[:4]
                    month = fund_date[4:6]
                    day = fund_date[6:]
                    return f"{year}-{month}-{day}"
                except Exception:
                    raise InvalidDataError(f"Could not parse Fund Date string '{fund_date}' (YYYYMMDD format) to YYYY-MM-DD.")
                    
            # Try general date parsing
            try:
                parsed_date = pd.to_datetime(fund_date)
                return parsed_date.strftime("%Y-%m-%d")
            except Exception:
                raise InvalidDataError(f"Could not parse Fund Date string '{fund_date}' (general format) to YYYY-MM-DD.")
        
        raise MissingCriticalDataError("Fund Date column not found or empty in PCF file.")
    
    except Exception as e:
        logger.error(f"Error extracting Fund Date: {str(e)}") # Keep logging for context
        logger.error(traceback.format_exc())
        # Re-raise as InvalidDataError or MissingCriticalDataError if it's one of those, else wrap
        if isinstance(e, (MissingCriticalDataError, InvalidDataError)):
            raise
        raise InvalidDataError(f"Error extracting Fund Date from {file_path}: {str(e)}") from e

def extract_vix_futures_from_pcf(file_path=None):
    """
    Extract VIX futures prices from Simplex ETF PCF file
    
    Args:
        file_path: Path to PCF file (optional, will find latest if not provided)
    
    Returns:
        dict: Dictionary with VIX futures prices or None if extraction fails
    """
    try:
        # Find latest PCF file if not provided
        if file_path is None:
            file_path = find_latest_etf_file()
            if file_path is None:
                raise MissingCriticalDataError("No PCF file found for VIX futures extraction.")
        
        logger.info(f"Extracting VIX futures from PCF file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path): # This check is somewhat redundant if find_latest_etf_file works correctly
            raise MissingCriticalDataError(f"PCF file not found: {file_path}")
        
        # Extract Fund Date first - this is critical for price_date accuracy
        try:
            fund_date = extract_fund_date_from_pcf(file_path)
        except (MissingCriticalDataError, InvalidDataError) as e:
            logger.error(f"Critical error extracting fund date: {str(e)}")
            raise # Re-raise the specific error from extract_fund_date_from_pcf
        
        if not fund_date: # Should be caught by exception now, but as a safeguard
            raise MissingCriticalDataError("Could not extract Fund Date, which is critical for PCF VIX futures extraction.")
        
        logger.info(f"Using Fund Date: {fund_date}")
        
        # Preview file content to determine format
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            preview_lines = [line.strip() for line in f.readlines()[:20]]
        
        logger.debug(f"File preview (first few lines):")
        for i, line in enumerate(preview_lines[:5]):
            logger.debug(f"Line {i+1}: {line[:100]}..." if len(line) > 100 else f"Line {i+1}: {line}")
        
        # Initialize futures data
        futures_data = {
            'date': fund_date,
            'timestamp': datetime.now().strftime("%Y%m%d%H%M")
        }
        
        # Determine if this matches the expected PCF format (based on column example)
        is_standard_pcf = False
        
        # Parse the PCF file
        try:
            df = pd.read_csv(file_path, encoding='utf-8', errors='replace')
            logger.debug(f"CSV columns: {df.columns.tolist()}")
            
            # Check if this is the standard PCF format we expect
            if all(col in df.columns for col in ['ETF Code', 'ETF Name']):
                is_standard_pcf = True
                logger.info("Detected standard PCF format with ETF Code/Name columns")
                
                # For this format, we need to examine specific structure
                # The first few rows contain ETF metadata, followed by holdings
                
                # Extract holdings data - we need to read the file again to skip header rows
                # and get to the actual holdings section
                holdings_found = False
                securities_section_started = False
                
                for i, line in enumerate(preview_lines):
                    if 'Code,Name' in line:
                        securities_section_started = True
                        # The next row should be the start of securities
                        try:
                            # Read the file again starting from this section
                            holdings_df = pd.read_csv(file_path, skiprows=i)
                            logger.debug(f"Holdings section found at line {i+1}")
                            logger.debug(f"Holdings columns: {holdings_df.columns.tolist()}")
                            holdings_found = True
                            break
                        except Exception as e:
                            raise InvalidDataError(f"Failed to parse holdings section from PCF file {file_path}: {str(e)}") from e
                
                if not holdings_found:
                    raise MissingCriticalDataError("Could not locate securities/holdings section in PCF file.")
                
                # Process the holdings section
                logger.info("Processing holdings section")
                
                # In this format, commonly:
                # Column 1: Code
                # Column 2: Name/Description
                # Last column or near last: Price/Value
                
                # Identify columns based on position and content
                desc_col = holdings_df.columns[1] if len(holdings_df.columns) > 1 else None
                price_col = holdings_df.columns[-1] if len(holdings_df.columns) > 2 else None
                
                # Verify these columns have appropriate content
                if desc_col and holdings_df[desc_col].dtype == object:
                    logger.info(f"Using '{desc_col}' as description column")
                else:
                    raise MissingCriticalDataError("Could not determine description column in PCF holdings.")
                
                # For price column, check if it has numeric-like values
                if price_col:
                    try:
                        # Convert a sample to float to check if it's a price column
                        sample = holdings_df[price_col].dropna().head(1)
                        if len(sample) > 0:
                            float(str(sample.iloc[0]).replace(',', '')) # Check if convertible
                            logger.info(f"Using '{price_col}' as price column")
                        else:
                            # This case means the column exists but has no valid (non-NA) data to sample
                            raise MissingCriticalDataError(f"Price column '{price_col}' contains no valid samples to confirm it's numeric.")
                    except Exception as e: # Catch issues from float conversion or other problems
                        raise MissingCriticalDataError(f"Could not validate price column '{price_col}': {str(e)}")
                else:
                    raise MissingCriticalDataError("Could not determine or validate price column in PCF holdings.")
                
                # Define patterns to match VIX futures
                patterns = [
                    # Pattern for CBOEVIX YYMM format (e.g., CBOEVIX 2503 for March 2025)
                    re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE),
                    
                    # Pattern for VIX FUTURE MMM-YY format (e.g., VIX FUTURE MAR-25)
                    re.compile(r'VIX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE),
                    
                    # Pattern for direct VXH5 format or VXH25 format
                    re.compile(r'VX([A-Z])(\d{1,2})', re.IGNORECASE)
                ]
                
                futures_found = 0
                
                # Process holdings to extract VIX futures
                for _, row in holdings_df.iterrows():
                    if pd.isna(row[desc_col]):
                        continue
                        
                    desc = str(row[desc_col])
                    
                    for pattern in patterns:
                        match = pattern.search(desc)
                        if match:
                            # Process based on which pattern matched
                            if len(match.groups()) == 1 and match.group(1).isdigit() and len(match.group(1)) == 4:
                                # CBOEVIX 2503 format
                                code = match.group(1)
                                
                                # If it's a 4-digit code, first two digits are year, second two are month
                                if len(code) == 4:
                                    year = int(code[:2])
                                    month = int(code[2:])
                                    
                                    # Map month number to VIX futures month code
                                    month_map = {
                                        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                                        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                                    }
                                    
                                    if month in month_map:
                                        month_letter = month_map[month]
                                        # Use only last digit of year
                                        vix_future = f"VX{month_letter}{year % 10}"
                                        
                                        # Also store in PCF:VX format
                                        pcf_ticker = f"PCF:VX{month_letter}{year % 10}"
                                        std_ticker = f"/VX{month_letter}{year % 10}"
                            elif len(match.groups()) == 2 and match.group(1).isalpha() and len(match.group(1)) == 3:
                                # VIX FUTURE MAR-25 format
                                month_str = match.group(1).upper()
                                year = match.group(2)
                                
                                # Convert month name to code
                                month_map = {
                                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                }
                                
                                if month_str in month_map:
                                    month_letter = month_map[month_str]
                                    # Use only last digit of year
                                    vix_future = f"VX{month_letter}{year[-1]}"
                                    
                                    # Store in PCF:VX format
                                    pcf_ticker = f"PCF:VX{month_letter}{year[-1]}"
                                    std_ticker = f"/VX{month_letter}{year[-1]}"
                            else:
                                # VXH5 or VXH25 format
                                month_letter = match.group(1)
                                year_digits = match.group(2)
                                
                                # Normalize to use only last digit of year
                                if len(year_digits) > 1:
                                    year_digit = year_digits[-1]
                                else:
                                    year_digit = year_digits
                                    
                                vix_future = f"VX{month_letter}{year_digit}"
                                
                                # Store in PCF:VX format
                                pcf_ticker = f"PCF:VX{month_letter}{year_digit}"
                                std_ticker = f"/VX{month_letter}{year_digit}"
                            
                            # Get the price value
                            try:
                                price_val = row[price_col]
                                if isinstance(price_val, str):
                                    price_val = price_val.strip()
                                    price_val = price_val.replace("$", "")
                                    price_val = price_val.replace(",", "")
                                
                                price = float(price_val)
                                
                                # Store prices with different ticker formats
                                futures_data[pcf_ticker] = price
                                futures_data[std_ticker] = price
                                futures_found += 1
                                logger.info(f"Extracted: {vix_future} = {price} (from {desc})")
                            except (ValueError, TypeError) as e:
                                raise InvalidDataError(f"Could not convert price '{row[price_col]}' to float for VIX future '{desc}': {str(e)}") from e
                            
                            break  # Move to next row after finding a match
                
                if futures_found == 0: # Changed from futures_found > 0
                    raise MissingCriticalDataError(f"No VIX futures contracts found in PCF file {file_path}.")
                
                logger.info(f"Successfully extracted {futures_found} VIX futures from PCF")
                return futures_data
                
            else: # Corresponds to: if all(col in df.columns for col in ['ETF Code', 'ETF Name']):
                raise InvalidDataError(f"File {file_path} does not match expected PCF structure (missing 'ETF Code' or 'ETF Name' columns).")
        
        except Exception as e: # Catches errors from pd.read_csv for the main df
            logger.error(f"Error parsing main PCF CSV file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            if isinstance(e, (MissingCriticalDataError, InvalidDataError)): # Re-raise if already custom
                raise
            raise InvalidDataError(f"Error parsing main PCF CSV file {file_path}: {str(e)}") from e
        
    except Exception as e:
        logger.error(f"Overall error extracting VIX futures from PCF {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        if isinstance(e, (MissingCriticalDataError, InvalidDataError)): # Re-raise if already custom
            raise
        raise InvalidDataError(f"Overall error extracting VIX futures from PCF {file_path}: {str(e)}") from e
