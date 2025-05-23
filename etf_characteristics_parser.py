import os
import pandas as pd
import glob
import re
from datetime import datetime
import logging
from common import setup_logging, SAVE_DIR, MONTH_CODES, normalize_vix_ticker, MissingCriticalDataError, InvalidDataError

# Set up logging
logger = setup_logging('etf_characteristics')

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

def extract_vix_future_code(code, name):
    """
    Extract VIX future code from PCF Code and Name fields
    
    Args:
        code: Code field from PCF
        name: Name field from PCF
    
    Returns:
        str: Normalized VIX future code (e.g., VXH5) or None if not found
    """
    if pd.isna(code) and pd.isna(name):
        return None
        
    code_str = str(code) if not pd.isna(code) else ""
    name_str = str(name) if not pd.isna(name) else ""
    
    # Define patterns to match VIX futures
    patterns = [
        # Pattern for direct VX codes like VXH5 or VXH25
        re.compile(r'VX([A-Z])(\d{1,2})', re.IGNORECASE),
        
        # Pattern for CBOEVIX YYMM format (e.g., CBOEVIX 2503 for March 2025)
        re.compile(r'CBOEVIX\s*(\d{4})', re.IGNORECASE),
        
        # Pattern for VIX FUTURE MMM-YY format (e.g., VIX FUTURE MAR-25)
        re.compile(r'VIX\s*(?:FUT|FUTURE)\s*(\w{3})[-\s]*(\d{2})', re.IGNORECASE)
    ]
    
    # Check both code and name for matches
    for text in [code_str, name_str]:
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                # Process based on which pattern matched
                if pattern == patterns[0]:  # VXH5/VXH25 pattern
                    month_letter = match.group(1)
                    year_digits = match.group(2)
                    
                    # Normalize to use only last digit of year
                    year_digit = year_digits[-1] if len(year_digits) > 0 else year_digits
                    return f"VX{month_letter}{year_digit}"
                    
                elif pattern == patterns[1]:  # CBOEVIX 2503 format
                    yymm = match.group(1)
                    if len(yymm) == 4:
                        year = yymm[:2]
                        month = int(yymm[2:])
                        
                        # Map month number to VIX futures month code
                        month_map = {
                            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
                        }
                        
                        if month in month_map:
                            # Use only last digit of year
                            return f"VX{month_map[month]}{year[-1]}"
                            
                elif pattern == patterns[2]:  # VIX FUTURE MAR-25 format
                    month_str = match.group(1).upper()
                    year = match.group(2)
                    
                    # Convert month name to code
                    month_map = {
                        'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                        'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                        'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                    }
                    
                    if month_str in month_map:
                        # Use only last digit of year
                        return f"VX{month_map[month_str]}{year[-1]}"
    
    # If we get here, no valid VIX future code was found
    return None

def parse_etf_characteristics(file_path=None):
    """
    Parse ETF characteristics from PCF file using a simplified approach
    
    Args:
        file_path: Path to PCF file (optional, will find latest if not provided)
    
    Returns:
        dict: Dictionary with ETF characteristics
    """
    try:
        # Find latest PCF file if not provided
        if file_path is None:
            file_path = find_latest_etf_file()
            if file_path is None:
                raise MissingCriticalDataError("No Simplex ETF 318A file found.")
        
        logger.info(f"Parsing ETF characteristics from PCF file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise MissingCriticalDataError(f"PCF file not found: {file_path}")
        
        # Initialize characteristics
        characteristics = {
            'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
            'fund_date': None,
            'shares_outstanding': None,
            'fund_cash_component': None,
            'shares_amount_near_future': 0,
            'shares_amount_far_future': 0,
            'near_future': None,   # Changed from 'near_future_code' to 'near_future'
            'far_future': None     # Changed from 'far_future_code' to 'far_future'
        }
        
        # 1. Read the header section (first 2 rows) to get fund info
        try:
            header_df = pd.read_csv(file_path, nrows=2)
            logger.info(f"Header columns: {header_df.columns.tolist()}")
            
            # Extract fund date
            if 'Fund Date' in header_df.columns and not header_df['Fund Date'].isna().all():
                fund_date = str(header_df['Fund Date'].iloc[0]).strip()
                # Convert format if needed (sometimes it's YYYYMMDD, sometimes MM/DD/YYYY)
                if '/' in fund_date:
                    date_parts = fund_date.split('/')
                    if len(date_parts) == 3:
                        # Convert MM/DD/YYYY to YYYYMMDD
                        fund_date = f"{date_parts[2]}{date_parts[0].zfill(2)}{date_parts[1].zfill(2)}"
                characteristics['fund_date'] = fund_date
                logger.info(f"Found fund date: {fund_date}")
            else:
                raise MissingCriticalDataError("Fund Date not found in PCF header.")
            
            # Extract shares outstanding
            if 'Shares Outstanding' in header_df.columns and not header_df['Shares Outstanding'].isna().all():
                shares_value = header_df['Shares Outstanding'].iloc[0]
                try:
                    # Convert from float to int if it's a number with decimal
                    if isinstance(shares_value, float):
                        characteristics['shares_outstanding'] = int(shares_value)
                    else:
                        shares_str = str(shares_value).strip()
                        characteristics['shares_outstanding'] = int(float(shares_str))
                    logger.info(f"Found shares outstanding: {characteristics['shares_outstanding']}")
                except (ValueError, TypeError):
                    raise InvalidDataError(f"Could not convert Shares Outstanding '{shares_value}' to integer.")
            else:
                raise MissingCriticalDataError("Shares Outstanding not found in PCF header.")
            
            # Extract fund cash component
            if 'Fund Cash Component' in header_df.columns and not header_df['Fund Cash Component'].isna().all():
                cash_str = str(header_df['Fund Cash Component'].iloc[0]).strip()
                try:
                    characteristics['fund_cash_component'] = float(cash_str)
                    logger.info(f"Found fund cash component: {cash_str}")
                except ValueError:
                    raise InvalidDataError(f"Could not convert Fund Cash Component '{cash_str}' to float.")
            else:
                raise MissingCriticalDataError("Fund Cash Component not found in PCF header.")
        except Exception as e:
            raise InvalidDataError(f"Error parsing PCF header: {str(e)}")
        
        # 2. Read the holding section (starting from row 4)
        try:
            # Skip the first 3 rows (header section and blank row)
            holdings_df = pd.read_csv(file_path, skiprows=3)
            logger.info(f"Holdings columns: {holdings_df.columns.tolist()}")
            
            # Look for the CBOEVIX futures in the holdings
            futures_rows = []
            
            # Make sure required columns exist
            required_cols = ["shares_outstanding", "shares_amount_near_future", "shares_amount_far_future"]
            name_cols = ['Name', 'Security Name', 'Description']
            code_cols = ['Code', 'Security Code', 'Ticker']
            
            # Find the actual name column
            name_col = None
            for col in name_cols:
                if col in holdings_df.columns:
                    name_col = col
                    logger.info(f"Using '{name_col}' as name column")
                    break
            if name_col is None:
                raise MissingCriticalDataError("Could not identify the instrument Name/Description column in PCF holdings.")
            
            # Find the actual code column
            code_col = None
            for col in code_cols:
                if col in holdings_df.columns:
                    code_col = col
                    logger.info(f"Using '{code_col}' as code column")
                    break
            if code_col is None:
                raise MissingCriticalDataError("Could not identify the instrument Code/Ticker column in PCF holdings.")
            
            # Check if we have the required columns
            shares_col = 'Shares Amount' if 'Shares Amount' in holdings_df.columns else None
            
            if not shares_col:
                logger.info("Standard 'Shares Amount' column not found, trying alternatives.")
                # Try to find an alternative column
                for col in holdings_df.columns:
                    if 'shares' in col.lower() and 'amount' in col.lower():
                        shares_col = col
                        logger.info(f"Using alternative shares column: {shares_col}")
                        break
            if shares_col is None:
                raise MissingCriticalDataError("Could not identify the Shares Amount column in PCF holdings.")
            
            # This part of the original if condition (name_col and code_col) is now guaranteed by checks above
            # if name_col and code_col and shares_col:
            logger.info(f"Found all required columns: Name='{name_col}', Code='{code_col}', Shares='{shares_col}'")
            
            # Find rows with VIX futures
                for _, row in holdings_df.iterrows():
                    code = row[code_col] if not pd.isna(row[code_col]) else ""
                    name = row[name_col] if not pd.isna(row[name_col]) else ""
                    
                    # Check if this is a VIX future
                    is_vix_future = False
                    name_str = str(name).upper()
                    code_str = str(code).upper()
                    
                    if ('VIX' in name_str or 'CBOEVIX' in name_str or 'VIX' in code_str or 
                        'FUTURE' in name_str or 'FUT' in name_str):
                        is_vix_future = True
                    
                    if is_vix_future:
                        # Extract the VIX future code
                        future_code = extract_vix_future_code(code, name)
                        
                        # Get the shares amount
                        shares_amount = 0
                        if not pd.isna(row[shares_col]):
                            try:
                                shares_amount = int(float(row[shares_col]))
                            except (ValueError, TypeError):
                                raise InvalidDataError(f"Could not convert shares amount '{row[shares_col]}' to integer for instrument '{row[name_col]}'.")
                        
                        if future_code:
                            futures_rows.append((future_code, shares_amount, name, code))
                            logger.info(f"Found future: {future_code}, shares: {shares_amount}, name: {name}, code: {code}")
                        elif is_vix_future: # future_code is None but it was identified as a VIX future
                            raise InvalidDataError(f"Could not extract VIX future code from identified VIX future row: name='{name}', code='{code}'.")
                        # else: # Not a VIX future or not parsable and not identified as VIX, so we just skip.
                            # logger.warning(f"Could not extract future code from: name='{name}', code='{code}'")
            else: # This 'else' block corresponds to the 'if name_col and code_col and shares_col:' which is now handled by raising errors.
                  # So this block relating to scanning all columns should ideally not be reached if columns are missing.
                  # However, to adhere to the plan of modifying the fallback:
                logger.warning("Attempting to scan all columns as primary column identification failed (this path should ideally not be taken).")
                
                # Find a likely shares column
                shares_col = None
                for col in holdings_df.columns:
                    if holdings_df[col].dtype in ['int64', 'float64']:
                        # Check if values are in a reasonable range for shares
                        values = holdings_df[col].dropna()
                        if len(values) > 0 and values.mean() > 0 and values.mean() < 1000:
                            shares_col = col
                            logger.info(f"Using '{shares_col}' as likely shares column")
                            break
                
                if not shares_col:
                    raise MissingCriticalDataError("Could not identify a suitable shares column even after scanning all columns.")
                
                # Scan all text columns for VIX futures
                for _, row in holdings_df.iterrows():
                    vix_future_found = False
                    vix_code = None
                    
                    # Scan each column for VIX future indications
                    for col in holdings_df.columns:
                        if pd.isna(row[col]):
                            continue
                            
                        cell_text = str(row[col]).upper()
                        if 'VIX' in cell_text or 'CBOEVIX' in cell_text or 'FUTURE' in cell_text:
                            # Try to extract a VIX future code
                            for other_col in holdings_df.columns:
                                if other_col != col and not pd.isna(row[other_col]):
                                    extracted_code = extract_vix_future_code(row[col], row[other_col])
                                    if extracted_code:
                                        vix_code = extracted_code
                                        vix_future_found = True
                                        break
                        
                        if vix_future_found:
                            break
                    
                    if vix_future_found and vix_code:
                        # Get the shares amount
                        shares_amount = 0
                        if not pd.isna(row[shares_col]):
                            try:
                                shares_amount = int(float(row[shares_col]))
                            except (ValueError, TypeError):
                                logger.warning(f"Could not convert shares amount to integer: {row[shares_col]}")
                        
                        futures_rows.append((vix_code, shares_amount, "Unknown", "Unknown"))
                        logger.info(f"Found future through scan: {vix_code}, shares: {shares_amount}")
            
            # Sort futures by contract date/code
            def sort_vix_futures(item):
                code, _, _, _ = item
                if not code or len(code) < 4:
                    return "ZZZ999"  # Sort unknown codes last
                
                # Extract month and year
                month_code = code[2]
                year_digit = code[3]
                
                # Convert month code to number (1-12)
                month_num = 0
                for key, value in MONTH_CODES.items():
                    if key == month_code:
                        month_num = value
                        break
                
                # Create sortable string: YYYYMM
                current_year = datetime.now().year
                decade = int(current_year / 10) * 10
                full_year = decade + int(year_digit)
                
                # If the resulting year is in the past, assume next decade
                if full_year < current_year:
                    full_year += 10
                
                return f"{full_year}{month_num:02d}"
            
            # Sort futures by contract date (nearest first)
            sorted_futures = sorted(futures_rows, key=sort_vix_futures)
            logger.info(f"Sorted futures: {sorted_futures}")
            
            # Assign shares to near and far futures with their codes
            if len(sorted_futures) >= 1:
                characteristics['near_future'] = sorted_futures[0][0]  # Changed from 'near_future_code' to 'near_future'
                characteristics['shares_amount_near_future'] = sorted_futures[0][1]
                logger.info(f"Near future: {sorted_futures[0][0]} with {sorted_futures[0][1]} shares")
            
            if len(sorted_futures) >= 2:
                characteristics['far_future'] = sorted_futures[1][0]  # Changed from 'far_future_code' to 'far_future'
                characteristics['shares_amount_far_future'] = sorted_futures[1][1]
                logger.info(f"Far future: {sorted_futures[1][0]} with {sorted_futures[1][1]} shares")
            
        except Exception as e:
            logger.warning(f"Error parsing VIX futures holdings: {str(e)}")
            logger.warning(f"Exception details: {str(e)}", exc_info=True)
        
        # Final Validation
        if characteristics.get('fund_date') is None:
            raise MissingCriticalDataError("Fund date was not successfully parsed.")
        if characteristics.get('shares_outstanding') is None or characteristics.get('shares_outstanding') <= 0:
            raise MissingCriticalDataError(f"Shares outstanding is missing or invalid: {characteristics.get('shares_outstanding')}.")
        if characteristics.get('fund_cash_component') is None: # Assuming 0 is a valid cash component
            raise MissingCriticalDataError("Fund cash component was not successfully parsed.")
        if characteristics.get('near_future') is None:
            raise MissingCriticalDataError("Near future contract code was not determined.")
            
        return characteristics
    
    except Exception as e:
        logger.error(f"Error parsing ETF characteristics: {str(e)}")
        logger.error(f"Exception details:", exc_info=True)
        raise InvalidDataError(f"Overall error parsing ETF characteristics from {file_path}: {str(e)}") from e

def save_etf_characteristics(characteristics, save_dir=SAVE_DIR):
    """
    Save ETF characteristics to CSV file
    
    Args:
        characteristics: Dictionary with ETF characteristics
        save_dir: Directory to save the file
    
    Returns:
        str: Path to saved file
    """
    if not characteristics:
        logger.warning("No ETF characteristics to save")
        return None
    
    # Create DataFrame
    df = pd.DataFrame([characteristics])
    
    # Save new data to daily file
    timestamp = characteristics['timestamp']
    daily_file = os.path.join(save_dir, f"etf_characteristics_{timestamp}.csv")
    df.to_csv(daily_file, index=False)
    
    # Master file path
    master_file = os.path.join(save_dir, "etf_characteristics_master.csv")
    
    # Append to master file if it exists, otherwise create it
    if os.path.exists(master_file):
        try:
            master_df = pd.read_csv(master_file)
            
            # Avoid duplicate timestamp entries
            master_df = master_df[master_df['timestamp'] != timestamp]
            
            # Append new data
            combined_df = pd.concat([master_df, df], ignore_index=True)
            combined_df.to_csv(master_file, index=False)
        except Exception as e:
            logger.error(f"Error updating master CSV: {str(e)}")
            # If error, just write new file
            df.to_csv(master_file, index=False)
    else:
        df.to_csv(master_file, index=False)
    
    logger.info(f"Saved ETF characteristics to {daily_file} and {master_file}")
    return daily_file

def process_etf_characteristics():
    """Main function to process ETF characteristics"""
    logger.info("Starting ETF characteristics processing")
    
    try:
        characteristics = parse_etf_characteristics()
        if characteristics: # Should always be true if no exception
            file_path = save_etf_characteristics(characteristics)
            if file_path:
                logger.info(f"Successfully saved ETF characteristics to {file_path}")
                return True
        # This part might be unreachable if parse_etf_characteristics is guaranteed to raise or return valid data
        logger.warning("ETF characteristics processing resulted in no data, though no direct error was raised.") 
        return False
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.error(f"ETF characteristics processing failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = process_etf_characteristics()
    if not success:
        exit(1)
