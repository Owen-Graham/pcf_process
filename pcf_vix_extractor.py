import os
import glob
import re
import pandas as pd
import traceback
from datetime import datetime
import logging
from common import setup_logging, SAVE_DIR, format_vix_data

# Set up logging
logger = setup_logging('pcf_vix_extractor')

# Define expected PCF format based on the provided example
PCF_COLUMNS = [
    'ETF Code', 'ETF Name', 'Fund Cash Component', 
    'Shares Outstanding', 'Fund Date', 'Unnamed: 5', 'Unnamed: 6'
]

def find_latest_etf_file():
    """Find the latest Simplex ETF 318A file in the data directory."""
    file_patterns = [
        os.path.join(SAVE_DIR, "318A-*.csv"),
        os.path.join(SAVE_DIR, "318A*.csv")
    ]
    
    all_files = []
    for pattern in file_patterns:
        all_files.extend(glob.glob(pattern))
    
    if not all_files:
        logger.error("No Simplex ETF 318A files found")
        return None
    
    # Get the most recent file based on the timestamp in the filename
    latest_file = max(all_files, key=os.path.getmtime)
    logger.info(f"Found latest Simplex ETF file: {latest_file}")
    return latest_file

def normalize_vix_ticker(ticker):
    """
    Normalize VIX futures ticker to standard format (VXM5 instead of VXM25)
    
    Args:
        ticker: VIX futures ticker (e.g., VXM25, VXM5)
    
    Returns:
        Normalized ticker (e.g., VXM5)
    """
    # If ticker is in format VX<letter><2-digit-year>
    match = re.match(r'(VX[A-Z])(\d{2})$', ticker)
    if match:
        prefix = match.group(1)
        year = match.group(2)
        # Take only the last digit of the year
        return f"{prefix}{year[-1]}"
    return ticker

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
            logger.error(f"PCF file not found: {file_path}")
            return None
        
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
                except:
                    logger.error(f"Failed to parse date with / format: {fund_date}")
                    return None
            
            # Try to handle YYYYMMDD format
            if fund_date.isdigit() and len(fund_date) == 8:
                try:
                    year = fund_date[:4]
                    month = fund_date[4:6]
                    day = fund_date[6:]
                    return f"{year}-{month}-{day}"
                except:
                    logger.error(f"Failed to parse date with YYYYMMDD format: {fund_date}")
                    return None
                    
            # Try general date parsing
            try:
                parsed_date = pd.to_datetime(fund_date)
                return parsed_date.strftime("%Y-%m-%d")
            except:
                logger.error(f"Could not parse Fund Date: {fund_date}")
                return None
        
        logger.error("Fund Date not found in PCF file")
        return None
    
    except Exception as e:
        logger.error(f"Error extracting Fund Date: {str(e)}")
        logger.error(traceback.format_exc())
        return None

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
                logger.error("No PCF file found")
                return None
        
        logger.info(f"Extracting VIX futures from PCF file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"PCF file not found: {file_path}")
            return None
        
        # Extract Fund Date first - this is critical for price_date accuracy
        fund_date = extract_fund_date_from_pcf(file_path)
        if not fund_date:
            logger.error("Could not extract Fund Date. Aborting extraction.")
            return None
        
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
                            logger.error(f"Failed to parse holdings section: {str(e)}")
                            return None
                
                if not holdings_found:
                    logger.error("Could not locate securities section in PCF file")
                    return None
                
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
                    logger.error("Could not determine description column")
                    return None
                
                # For price column, check if it has numeric-like values
                if price_col:
                    try:
                        # Convert a sample to float to check if it's a price column
                        sample = holdings_df[price_col].dropna().head(1)
                        if len(sample) > 0:
                            float(str(sample.iloc[0]).replace(',', ''))
                            logger.info(f"Using '{price_col}' as price column")
                        else:
                            logger.error("Price column contains no valid samples")
                            return None
                    except:
                        logger.error("Could not validate price column")
                        return None
                else:
                    logger.error("Could not determine price column")
                    return None
                
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
                                logger.warning(f"Could not convert price '{row[price_col]}' to float: {str(e)}")
                            
                            break  # Move to next row after finding a match
                
                if futures_found > 0:
                    logger.info(f"Successfully extracted {futures_found} VIX futures from PCF")
                    return futures_data
                else:
                    logger.error("No VIX futures contracts found in PCF file")
                    return None
                
            else:
                logger.error("File format does not match expected PCF structure")
                return None
        
        except Exception as e:
            logger.error(f"Error parsing PCF file: {str(e)}")
            logger.error(traceback.format_exc())
            return None
        
    except Exception as e:
        logger.error(f"Error extracting VIX futures from PCF: {str(e)}")
        logger.error(traceback.format_exc())
        return None
