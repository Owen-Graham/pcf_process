import os
import pandas as pd
import glob
import re
from datetime import datetime
import logging
from common import setup_logging, SAVE_DIR, MONTH_CODES

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

def parse_etf_characteristics(file_path=None):
    """
    Parse ETF characteristics from PCF file
    
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
                logger.error("No PCF file found")
                return None
        
        logger.info(f"Parsing ETF characteristics from PCF file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"PCF file not found: {file_path}")
            return None
        
        # Initialize characteristics
        characteristics = {
            'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
            'fund_date': None,
            'shares_outstanding': None,
            'fund_cash_component': None,
            'shares_amount_near_future': 0,
            'shares_amount_far_future': 0
        }
        
        # First pass: Read the header to get fund date, shares outstanding, and cash component
        try:
            header_df = pd.read_csv(file_path, nrows=5)  # Read first few rows
            
            # Check for expected columns
            if 'ETF Code' in header_df.columns and 'Fund Date' in header_df.columns:
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
                
                # Extract shares outstanding
                if 'Shares Outstanding' in header_df.columns and not header_df['Shares Outstanding'].isna().all():
                    shares_str = str(header_df['Shares Outstanding'].iloc[0]).strip()
                    try:
                        characteristics['shares_outstanding'] = int(shares_str)
                    except ValueError:
                        logger.warning(f"Could not convert shares outstanding to integer: {shares_str}")
                
                # Extract fund cash component
                if 'Fund Cash Component' in header_df.columns and not header_df['Fund Cash Component'].isna().all():
                    cash_str = str(header_df['Fund Cash Component'].iloc[0]).strip()
                    try:
                        characteristics['fund_cash_component'] = float(cash_str)
                    except ValueError:
                        logger.warning(f"Could not convert fund cash component to float: {cash_str}")
        except Exception as e:
            logger.warning(f"Error parsing PCF header: {str(e)}")
        
        # Second pass: look for the holdings section to find VIX futures and their shares
        try:
            # Read the full file
            full_df = pd.read_csv(file_path)
            
            # Look for 'Code' and 'Name' columns that might indicate the holdings section
            codes_section_start = None
            for i, col_name in enumerate(full_df.columns):
                if col_name.lower() == 'code':
                    # Check if the next column is 'Name' or similar
                    if i+1 < len(full_df.columns) and ('name' in full_df.columns[i+1].lower() or 'description' in full_df.columns[i+1].lower()):
                        codes_section_start = i
                        break
            
            # If we found the holdings section, extract the VIX futures
            if codes_section_start is not None:
                code_col = full_df.columns[codes_section_start]
                name_col = full_df.columns[codes_section_start+1]
                
                # Find the "Shares Amount" column specifically
                shares_col = None
                for col in full_df.columns:
                    if col.lower() == 'shares amount' or col.lower() == 'shares_amount':
                        shares_col = col
                        logger.info(f"Found Shares Amount column: {col}")
                        break
                
                # Fallback approaches if the exact column name isn't found
                if shares_col is None:
                    for col in full_df.columns:
                        if 'shares' in col.lower() and 'amount' in col.lower():
                            shares_col = col
                            logger.info(f"Found column containing 'shares' and 'amount': {col}")
                            break
                
                if shares_col is None:
                    # Try to find column with numeric values that could be shares
                    for col in full_df.columns[codes_section_start+2:]:
                        if full_df[col].dtype.kind in 'fi':  # float or integer column
                            # Check if values look like shares (positive integers)
                            if not full_df[col].isna().all() and (full_df[col] >= 0).all():
                                shares_col = col
                                logger.info(f"Using numeric column as shares: {col}")
                                break
                
                if shares_col:
                    # Find rows with CBOEVIX contracts
                    vix_futures_rows = []
                    for i, row in full_df.iterrows():
                        if not pd.isna(row[name_col]):
                            name = str(row[name_col]).upper()
                            if 'CBOEVIX' in name or ('VIX' in name and 'FUTURE' in name):
                                # Use the Shares Amount column for the share count
                                shares_value = row[shares_col] if not pd.isna(row[shares_col]) else 0
                                vix_futures_rows.append((row[code_col], row[name_col], shares_value))
                    
                    # Sort VIX futures by contract date
                    def extract_contract_date(code_name_tuple):
                        code, name, _ = code_name_tuple
                        
                        # Try to extract YYMM code
                        match = re.search(r'(\d{4})', str(name))
                        if match:
                            return match.group(1)
                        match = re.search(r'(\d{4})', str(code))
                        if match:
                            return match.group(1)
                        return '9999'  # Default sort value
                    
                    sorted_futures = sorted(vix_futures_rows, key=extract_contract_date)
                    
                    # Assign shares to near and far futures
                    if len(sorted_futures) >= 1:
                        try:
                            characteristics['shares_amount_near_future'] = int(sorted_futures[0][2])
                            logger.info(f"Near future: {sorted_futures[0][1]} with {sorted_futures[0][2]} shares")
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert near future shares to integer: {sorted_futures[0][2]}")
                    
                    if len(sorted_futures) >= 2:
                        try:
                            characteristics['shares_amount_far_future'] = int(sorted_futures[1][2])
                            logger.info(f"Far future: {sorted_futures[1][1]} with {sorted_futures[1][2]} shares")
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert far future shares to integer: {sorted_futures[1][2]}")
            else:
                # Alternative approach: look for CBOEVIX in any column
                vix_futures = []
                for i, row in full_df.iterrows():
                    for col in full_df.columns:
                        value = str(row[col]).upper() if not pd.isna(row[col]) else ""
                        if 'CBOEVIX' in value:
                            # Try to find a numeric column in this row that could be shares
                            for num_col in full_df.columns:
                                if num_col != col and not pd.isna(row[num_col]):
                                    try:
                                        shares = int(row[num_col])
                                        if shares > 0:  # Looks like a valid shares value
                                            # Extract YYMM from CBOEVIX
                                            match = re.search(r'CBOEVIX\s*(\d{4})', value)
                                            if match:
                                                contract_code = match.group(1)
                                                vix_futures.append((contract_code, value, shares))
                                            break
                                    except (ValueError, TypeError):
                                        pass
                
                # Sort and assign futures
                sorted_futures = sorted(vix_futures)
                if len(sorted_futures) >= 1:
                    characteristics['shares_amount_near_future'] = sorted_futures[0][2]
                if len(sorted_futures) >= 2:
                    characteristics['shares_amount_far_future'] = sorted_futures[1][2]
        
        except Exception as e:
            logger.warning(f"Error parsing VIX futures holdings: {str(e)}")
        
        return characteristics
    
    except Exception as e:
        logger.error(f"Error parsing ETF characteristics: {str(e)}")
        return None

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
    
    # Parse characteristics
    characteristics = parse_etf_characteristics()
    
    if characteristics:
        # Save to CSV
        file_path = save_etf_characteristics(characteristics)
        if file_path:
            logger.info(f"Successfully saved ETF characteristics to {file_path}")
            return True
    
    logger.warning("ETF characteristics processing failed")
    return False

if __name__ == "__main__":
    success = process_etf_characteristics()
    if not success:
        exit(1)
