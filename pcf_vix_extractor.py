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

def extract_vix_futures_from_pcf(file_path=None):
    """
    Extract VIX futures prices from Simplex ETF PCF file
    
    Args:
        file_path: Path to PCF file (optional, will find latest if not provided)
    
    Returns:
        dict: Dictionary with VIX futures prices
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
        
        # Preview file content to determine format
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            preview_lines = [line.strip() for line in f.readlines()[:20]]
        
        logger.debug(f"File preview (first few lines):")
        for i, line in enumerate(preview_lines[:5]):
            logger.debug(f"Line {i+1}: {line[:100]}..." if len(line) > 100 else f"Line {i+1}: {line}")
        
        # Initialize futures data
        futures_data = {
            'date': datetime.now().strftime("%Y-%m-%d"),
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
                
                # Extract the fund date if available
                if 'Fund Date' in df.columns and len(df) > 0:
                    fund_date = df['Fund Date'].iloc[0]
                    logger.info(f"Fund Date from PCF: {fund_date}")
                
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
                            logger.warning(f"Failed to parse holdings section: {str(e)}")
                
                if not holdings_found:
                    logger.warning("Could not locate securities section in PCF file")
                    # Try alternative approach - read entire file and look for VIX future patterns in all cells
                    vix_futures_extracted = extract_vix_patterns_from_all_cells(df)
                    if vix_futures_extracted:
                        futures_data.update(vix_futures_extracted)
                        return futures_data
                else:
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
                        desc_col = None
                    
                    # For price column, check if it has numeric-like values
                    if price_col:
                        try:
                            # Convert a sample to float to check if it's a price column
                            sample = holdings_df[price_col].dropna().head(1)
                            if len(sample) > 0:
                                float(str(sample.iloc[0]).replace(',', ''))
                                logger.info(f"Using '{price_col}' as price column")
                            else:
                                price_col = None
                        except:
                            price_col = None
                    
                    # If columns not identified, try to find them by content
                    if desc_col is None or price_col is None:
                        # Look for VIX futures in any column
                        vix_futures_extracted = extract_vix_patterns_from_all_cells(holdings_df)
                        if vix_futures_extracted:
                            futures_data.update(vix_futures_extracted)
                            return futures_data
                    
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
                    if desc_col is not None and price_col is not None:
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
                # Not the standard PCF format, fall back to general approach
                logger.info("PCF format doesn't match expected structure, attempting general extraction")
                
                # Try to find the description and price columns
                desc_columns = ['Description', 'Security Description', 'Name', 'Security Name', 
                               'Asset', 'Asset Description', 'Holding', 'Holding Name', 'Security']
                
                price_columns = ['Price', 'Market Price', 'Close', 'Settlement Price', 'Value',
                               'Last Price', 'Settlement', 'Closing Price']
                
                # Find the description column
                desc_col = None
                for col in desc_columns:
                    if col in df.columns:
                        desc_col = col
                        break
                
                # If no standard name found, try to identify the most likely column
                if desc_col is None:
                    # Look at the first few columns (often description is first or second)
                    for i in range(min(3, len(df.columns))):
                        col = df.columns[i]
                        # Check if this column contains text that might be descriptions
                        if df[col].dtype == 'object':
                            # Look for VIX future keywords in this column
                            sample = df[col].astype(str)
                            if any(sample.str.contains(keyword, case=False).any() 
                                  for keyword in ['VIX', 'CBOE', 'Future', 'CBOEVIX']):
                                desc_col = col
                                logger.info(f"Using column '{col}' as description column")
                                break
                
                # Find the price column
                price_col = None
                for col in price_columns:
                    if col in df.columns:
                        price_col = col
                        break
                
                # If price column not found, try to infer from numeric columns
                if price_col is None:
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    for col in numeric_cols:
                        # Skip if this is an index-like column (mostly sequential integers)
                        if len(df) > 5 and df[col].dtype.kind in 'iu' and df[col].is_monotonic_increasing:
                            continue
                        
                        # Look for price-like columns (decimal values that seem like prices)
                        sample_values = df[col].dropna().head(5)
                        if len(sample_values) > 0 and any(10 < val < 50 for val in sample_values):
                            price_col = col
                            logger.info(f"Using column '{col}' as price column")
                            break
                
                logger.info(f"Using columns: Description='{desc_col}', Price='{price_col}'")
                
                # If we couldn't find appropriate columns, try to extract via all cells
                if desc_col is None or price_col is None:
                    vix_futures_extracted = extract_vix_patterns_from_all_cells(df)
                    if vix_futures_extracted:
                        futures_data.update(vix_futures_extracted)
                        return futures_data
                
                # Extract VIX futures information
                if desc_col is not None and price_col is not None:
                    futures_found = 0
                    
                    # Define patterns to match VIX futures
                    patterns = [
                        # Pattern for CBOEVIX YYMM format (e.g., CBOEVIX 2503 for March 2025)
                        re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE),
                        
                        # Pattern for VIX FUTURE MMM-YY format (e.g., VIX FUTURE MAR-25)
                        re.compile(r'VIX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE),
                        
                        # Pattern for direct VXH5 format or VXH25 format
                        re.compile(r'VX([A-Z])(\d{1,2})', re.IGNORECASE)
                    ]
                    
                    for _, row in df.iterrows():
                        if pd.isna(row[desc_col]):
                            continue
                            
                        desc = str(row[desc_col])
                        
                        for pattern in patterns:
                            match = pattern.search(desc)
                            if match:
                                # Process pattern match (same as above)
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
                                elif len(match.groups()) == 2 and match.group(1).isalpha():
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
                        logger.info(f"Successfully extracted {futures_found} VIX futures from standard format")
                        return futures_data
            
        except Exception as e:
            logger.error(f"Error parsing PCF file: {str(e)}")
            logger.error(traceback.format_exc())
        
        # Try alternative approach - manually parse the PCF format based on the provided example
        logger.info("Attempting PCF parsing based on expected format")
        try:
            # Read the file as text and look for the securities section
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
            
            # Find the section with Code,Name header
            code_name_line_idx = None
            for i, line in enumerate(lines):
                if 'Code,Name' in line:
                    code_name_line_idx = i
                    break
            
            if code_name_line_idx is not None:
                logger.info(f"Found Code,Name header at line {code_name_line_idx+1}")
                
                # Parse the securities section
                futures_found = 0
                for i in range(code_name_line_idx + 1, len(lines)):
                    line = lines[i].strip()
                    if not line:  # Skip empty lines
                        continue
                    
                    parts = line.split(',')
                    if len(parts) < 2:  # Need at least code and name
                        continue
                    
                    # Typically in this format:
                    # Code, Name, Other fields..., Price
                    code = parts[0]
                    name = parts[1]
                    
                    # Look for VIX futures in the name
                    patterns = [
                        re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE),
                        re.compile(r'VIX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE),
                        re.compile(r'VX([A-Z])(\d{1,2})', re.IGNORECASE)
                    ]
                    
                    for pattern in patterns:
                        match = pattern.search(name)
                        if match:
                            # Process the match (same as above)
                            if len(match.groups()) == 1 and match.group(1).isdigit() and len(match.group(1)) == 4:
                                # CBOEVIX 2503 format
                                code = match.group(1)
                                year = int(code[:2])
                                month = int(code[2:])
                                
                                month_map = {
                                    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                                    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                                }
                                
                                if month in month_map:
                                    month_letter = month_map[month]
                                    # Use only last digit of year
                                    vix_future = f"VX{month_letter}{year % 10}"
                                    
                                    # Try to get price from the last field
                                    if len(parts) > 2:
                                        try:
                                            price_str = parts[-1].strip().replace('$', '').replace(',', '')
                                            price = float(price_str)
                                            
                                            # Store in different formats
                                            pcf_ticker = f"PCF:{vix_future}"
                                            std_ticker = f"/{vix_future}"
                                            
                                            futures_data[pcf_ticker] = price
                                            futures_data[std_ticker] = price
                                            futures_found += 1
                                            logger.info(f"Manually extracted: {vix_future} = {price}")
                                        except (ValueError, TypeError) as e:
                                            logger.warning(f"Could not convert price '{parts[-1]}' to float: {str(e)}")
                            elif len(match.groups()) == 2 and match.group(1).isalpha():
                                # VIX FUTURE MAR-25 format
                                month_str = match.group(1).upper()
                                year = match.group(2)
                                
                                month_map = {
                                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J',
                                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                }
                                
                                if month_str in month_map:
                                    month_letter = month_map[month_str]
                                    # Use only last digit of year
                                    vix_future = f"VX{month_letter}{year[-1]}"
                                    
                                    # Try to get price from the last field
                                    if len(parts) > 2:
                                        try:
                                            price_str = parts[-1].strip().replace('$', '').replace(',', '')
                                            price = float(price_str)
                                            
                                            # Store in different formats
                                            pcf_ticker = f"PCF:{vix_future}"
                                            std_ticker = f"/{vix_future}"
                                            
                                            futures_data[pcf_ticker] = price
                                            futures_data[std_ticker] = price
                                            futures_found += 1
                                            logger.info(f"Manually extracted: {vix_future} = {price}")
                                        except (ValueError, TypeError) as e:
                                            logger.warning(f"Could not convert price '{parts[-1]}' to float: {str(e)}")
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
                                
                                # Try to get price from the last field
                                if len(parts) > 2:
                                    try:
                                        price_str = parts[-1].strip().replace('$', '').replace(',', '')
                                        price = float(price_str)
                                        
                                        # Store in different formats
                                        pcf_ticker = f"PCF:{vix_future}"
                                        std_ticker = f"/{vix_future}"
                                        
                                        futures_data[pcf_ticker] = price
                                        futures_data[std_ticker] = price
                                        futures_found += 1
                                        logger.info(f"Manually extracted: {vix_future} = {price}")
                                    except (ValueError, TypeError) as e:
                                        logger.warning(f"Could not convert price '{parts[-1]}' to float: {str(e)}")
                            
                            break  # Move to next line after finding a match
                
                if futures_found > 0:
                    logger.info(f"Successfully extracted {futures_found} VIX futures from manual parsing")
                    return futures_data
            
        except Exception as e:
            logger.error(f"Error in manual PCF parsing: {str(e)}")
            logger.error(traceback.format_exc())
        
        logger.warning("All parsing methods failed. No VIX futures extracted from PCF file.")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting VIX futures from PCF: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def extract_vix_patterns_from_all_cells(df):
    """Helper function to scan all cells in dataframe for VIX futures patterns"""
    futures_data = {}
    futures_found = 0
    
    # Define patterns to match VIX futures
    patterns = [
        re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE),  # CBOEVIX 2503
        re.compile(r'VIX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE),  # VIX FUTURE MAR-25
        re.compile(r'VX([A-Z])(\d{1,2})', re.IGNORECASE)   # VXH5 or VXH25
    ]
    
    # Look for price pattern (number that could be a VIX futures price)
    price_pattern = re.compile(r'\b(\d{1,2}\.\d+)\b')
    
    # Scan all cells for VIX futures and potential price combinations
    for i, row in df.iterrows():
        vix_future = None
        price = None
        
        # First pass: find VIX futures
        for col in df.columns:
            if pd.isna(row[col]):
                continue
            
            cell_text = str(row[col])
            
            for pattern in patterns:
                match = pattern.search(cell_text)
                if match:
                    # Process based on pattern type (same logic as above)
                    if len(match.groups()) == 1 and match.group(1).isdigit() and len(match.group(1)) == 4:
                        # CBOEVIX 2503 format
                        code = match.group(1)
                        year = int(code[:2])
                        month = int(code[2:])
                        
                        month_map = {
                            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                        }
                        
                        if month in month_map:
                            month_letter = month_map[month]
                            # Use only last digit of year
                            vix_future = f"VX{month_letter}{year % 10}"
                    elif len(match.groups()) == 2 and match.group(1).isalpha():
                        # VIX FUTURE MAR-25 format
                        month_str = match.group(1).upper()
                        year = match.group(2)
                        
                        month_map = {
                            'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                            'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                            'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                        }
                        
                        if month_str in month_map:
                            month_letter = month_map[month_str]
                            # Use only last digit of year
                            vix_future = f"VX{month_letter}{year[-1]}"
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
                    
                    break
            
            if vix_future:
                break
        
        # If we found a VIX future, look for price in the same row
        if vix_future:
            # First check numeric columns
            for col in df.columns:
                if pd.isna(row[col]):
                    continue
                
                try:
                    # Try to convert to float - likely a price if between 10-50
                    val = float(str(row[col]).replace(', '').replace(',', ''))
                    if 10 <= val <= 50:  # Typical VIX futures price range
                        price = val
                        break
                except (ValueError, TypeError):
                    # Not a numeric value, check for embedded price using regex
                    cell_text = str(row[col])
                    price_match = price_pattern.search(cell_text)
                    if price_match:
                        try:
                            val = float(price_match.group(1))
                            if 10 <= val <= 50:
                                price = val
                                break
                        except:
                            pass
            
            # If we found both a future and price, store them
            if price is not None:
                pcf_ticker = f"PCF:{vix_future}"
                std_ticker = f"/{vix_future}"
                
                futures_data[pcf_ticker] = price
                futures_data[std_ticker] = price
                futures_found += 1
                logger.info(f"Extracted from cell scan: {vix_future} = {price}")
    
    if futures_found > 0:
        logger.info(f"Successfully extracted {futures_found} VIX futures from cell scanning")
        return futures_data
    else:
        return None
