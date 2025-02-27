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
                    logger.warning(f"Could not convert shares outstanding to integer: {shares_value}")
            
            # Extract fund cash component
            if 'Fund Cash Component' in header_df.columns and not header_df['Fund Cash Component'].isna().all():
                cash_str = str(header_df['Fund Cash Component'].iloc[0]).strip()
                try:
                    characteristics['fund_cash_component'] = float(cash_str)
                    logger.info(f"Found fund cash component: {cash_str}")
                except ValueError:
                    logger.warning(f"Could not convert fund cash component to float: {cash_str}")
        except Exception as e:
            logger.warning(f"Error parsing PCF header: {str(e)}")
        
        # 2. Read the holding section (starting from row 4)
        try:
            # Skip the first 3 rows (header section and blank row)
            holdings_df = pd.read_csv(file_path, skiprows=3)
            logger.info(f"Holdings columns: {holdings_df.columns.tolist()}")
            
            # Look for the CBOEVIX futures in the holdings
            futures_rows = []
            
            # Make sure 'Shares Amount' and 'Name' columns exist
            if 'Shares Amount' in holdings_df.columns and 'Name' in holdings_df.columns:
                logger.info("Found both 'Shares Amount' and 'Name' columns")
                
                # Find rows with CBOEVIX contracts
                for i, row in holdings_df.iterrows():
                    name = str(row['Name']).upper() if not pd.isna(row['Name']) else ""
                    if 'CBOEVIX' in name or ('VIX' in name and 'FUTURE' in name):
                        # Get the share amount directly
                        shares_amount = 0
                        if not pd.isna(row['Shares Amount']):
                            try:
                                shares_amount = int(row['Shares Amount'])
                            except (ValueError, TypeError):
                                logger.warning(f"Could not convert shares amount to integer: {row['Shares Amount']}")
                        
                        # Get the code/contract month
                        code = str(row['Code']) if 'Code' in holdings_df.columns and not pd.isna(row['Code']) else ""
                        
                        futures_rows.append((code, name, shares_amount))
                        logger.info(f"Found future: {name}, code: {code}, shares: {shares_amount}")
            else:
                logger.warning("Missing 'Shares Amount' or 'Name' columns in holdings section")
                # Try to infer column names based on content
                for col in holdings_df.columns:
                    if 'shares' in col.lower() and 'amount' in col.lower():
                        logger.info(f"Found alternative shares column: {col}")
                        shares_col = col
                        for i, row in holdings_df.iterrows():
                            # Try to find VIX futures in any text column
                            for text_col in holdings_df.columns:
                                if holdings_df[text_col].dtype == 'object':
                                    cell_text = str(row[text_col]).upper() if not pd.isna(row[text_col]) else ""
                                    if 'CBOEVIX' in cell_text or ('VIX' in cell_text and 'FUTURE' in cell_text):
                                        shares_amount = 0
                                        if not pd.isna(row[shares_col]):
                                            try:
                                                shares_amount = int(row[shares_col])
                                            except (ValueError, TypeError):
                                                pass
                                        futures_rows.append(("", cell_text, shares_amount))
                                        break
            
            # Sort futures by contract date/code
            def extract_contract_date(code_name_tuple):
                code, name, _ = code_name_tuple
                
                # Try to extract YYMM code from name or code
                for source in [name, code]:
                    match = re.search(r'(\d{4})', str(source))
                    if match:
                        return match.group(1)
                return '9999'  # Default sort value
            
            sorted_futures = sorted(futures_rows, key=extract_contract_date)
            logger.info(f"Sorted futures: {sorted_futures}")
            
            # Assign shares to near and far futures
            if len(sorted_futures) >= 1:
                characteristics['shares_amount_near_future'] = sorted_futures[0][2]
                logger.info(f"Near future: {sorted_futures[0][1]} with {sorted_futures[0][2]} shares")
            
            if len(sorted_futures) >= 2:
                characteristics['shares_amount_far_future'] = sorted_futures[1][2]
                logger.info(f"Far future: {sorted_futures[1][1]} with {sorted_futures[1][2]} shares")
            
        except Exception as e:
            logger.warning(f"Error parsing VIX futures holdings: {str(e)}")
            logger.warning(f"Exception details: {str(e)}", exc_info=True)
        
        return characteristics
    
    except Exception as e:
        logger.error(f"Error parsing ETF characteristics: {str(e)}")
        logger.error(f"Exception details:", exc_info=True)
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
