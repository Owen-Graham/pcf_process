import os
import requests
import pandas as pd
from datetime import datetime
import logging
import traceback
import re
import io
from common import MissingCriticalDataError, InvalidDataError

# Set up paths and logging
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "mufg_fx_downloader.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("mufg_fx_downloader")

# Define standard rate labels
STANDARD_LABELS = ["T.T.S.", "ACC.", "CASH S.", "T.T.B.", "A/S", "D/PED/A", "CASH B."]

def extract_date_from_csv(content):
    """
    Extract the date from the CSV content
    
    Args:
        content: CSV content as string
    
    Returns:
        str: Date in YYYY-MM-DD format or None if not found
    """
    try:
        # Pattern to match Japanese dates (YYYY/MM/DD)
        date_pattern = r'(\d{4})/(\d{1,2})/(\d{1,2})'
        match = re.search(date_pattern, content)
        
        if match:
            year = match.group(1)
            month = match.group(2).zfill(2)  # Ensure 2 digits
            day = match.group(3).zfill(2)    # Ensure 2 digits
            
            date_str = f"{year}-{month}-{day}"
            logger.info(f"Extracted date from CSV: {date_str}")
            return date_str
            
        # Try to find the date in a pandas dataframe
        try:
            # Load the CSV into a DataFrame to check for date columns
            df = pd.read_csv(io.StringIO(content), encoding='utf-8', error_bad_lines=False)
            
            # Look through all cells for date patterns
            for col in df.columns:
                for val in df[col].astype(str):
                    date_match = re.search(date_pattern, val)
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2).zfill(2)
                        day = date_match.group(3).zfill(2)
                        
                        date_str = f"{year}-{month}-{day}"
                        logger.info(f"Extracted date from DataFrame cell: {date_str}")
                        return date_str
        except Exception as e:
            logger.warning(f"Error searching for date in DataFrame: {str(e)}")
        
        raise InvalidDataError("Could not extract date from CSV content.")
    
    except Exception as e:
        raise InvalidDataError(f"Error extracting date: {str(e)}") from e

def download_mufg_fx_rates():
    """
    Download USDJPY FX rates from MUFG Bank website
    
    Returns:
        list: List of dictionaries with FX rate data or None if download fails
    """
    try:
        url = "https://www.bk.mufg.jp/gdocs/kinri/list_j/kinri/spot_rate.csv"
        logger.info(f"Downloading FX rates from: {url}")
        
        # Request the CSV file
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Try different encodings until we find one that works
        encodings = ['shift_jis', 'iso-8859-1', 'utf-8', 'cp932']
        content = None
        
        for encoding in encodings:
            try:
                content = response.content.decode(encoding, errors='replace')
                logger.info(f"Successfully decoded CSV with {encoding} encoding")
                break
            except Exception as e:
                logger.warning(f"Failed to decode with {encoding}: {str(e)}")
        
        if not content:
            raise InvalidDataError("Could not decode CSV with any available encoding (shift_jis, iso-8859-1, utf-8, cp932).")
        
        # Save raw file for debugging
        raw_file_path = os.path.join(DATA_DIR, "mufg_fx_raw.csv")
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Saved raw CSV to: {raw_file_path}")
        
        # Extract date from CSV content - no fallback
        csv_date = extract_date_from_csv(content) # This will now raise InvalidDataError on failure
        # If extract_date_from_csv could still theoretically return None (it shouldn't after its changes):
        # if csv_date is None:
        #     raise MissingCriticalDataError("Date extraction failed - cannot proceed without a valid date.")
        
        # Parse the CSV data
        lines = content.strip().split('\n')
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        
        # Look for USD data pattern in each line
        usd_line = None
        numeric_values = []
        
        for line in lines:
            # Check if line contains USD or ドル (dollar in Japanese)
            if "USD" in line or "ドル" in line or "dollar" in line.lower():
                logger.info(f"Found USD line: {line}")
                usd_line = line
                
                # Extract numeric values from the line
                # Pattern to match numbers with optional commas and decimal points
                matches = re.findall(r'(\d+(?:[,.]\d+)?)', line)
                
                if matches:
                    numeric_values = []
                    for match in matches:
                        try:
                            # Replace commas and convert to float
                            num_value = float(match.replace(',', ''))
                            numeric_values.append(num_value)
                        except ValueError:
                            continue
                    
                    logger.info(f"Extracted numeric values: {numeric_values}")
                    
                    # If we found enough values that look like exchange rates
                    if len(numeric_values) >= 4 and any(100 <= val <= 200 for val in numeric_values):
                        break
        
        # Process the data if we found USD line and numeric values
        if usd_line and numeric_values:
            # Create a list of dictionaries for each rate
            fx_data_list = []
            
            # Based on the screenshot, we need to align values correctly with labels
            # Extract values that look like exchange rates (typically between 100-200 for USD/JPY)
            exchange_rates = [val for val in numeric_values if 100 <= val <= 200]
            
            logger.info(f"Filtered exchange rates: {exchange_rates}")
            
            if len(exchange_rates) < len(STANDARD_LABELS):
                raise MissingCriticalDataError(f"Not enough valid exchange rates found in MUFG data. Expected at least {len(STANDARD_LABELS)}, got {len(exchange_rates)}. Rates found: {exchange_rates}")
            
            # Match standard labels with the exchange rates
            for i, label in enumerate(STANDARD_LABELS):
                if i < len(exchange_rates):
                    fx_data = {
                        'timestamp': timestamp,
                        'date': csv_date,
                        'source': url,
                        'pair': 'USDJPY',
                        'label': label,
                        'rate': exchange_rates[i]
                    }
                    fx_data_list.append(fx_data)
                    logger.info(f"Mapped {label} to rate {exchange_rates[i]}")
            
            logger.info(f"Created {len(fx_data_list)} FX rate entries with date {csv_date}")
            return fx_data_list
        else:
            if not usd_line:
                raise MissingCriticalDataError("USD line not found in MUFG FX CSV data.")
            # It's possible numeric_values is empty even if usd_line is found, if regex fails.
            if not numeric_values:
                 raise MissingCriticalDataError("No numeric values suitable for FX rates found in MUFG USD line.")
            # If both are true, it implies some other logic failure if this path is reached.
            # However, the primary check for not enough rates (below) is more specific.
            # For now, the above two are sufficient.
    
    except (requests.exceptions.RequestException, InvalidDataError, MissingCriticalDataError) as e:
        logger.error(f"Failed to download or parse MUFG FX rates: {str(e)}")
        raise # Re-raise the caught error (custom or request-related)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error downloading MUFG FX rates: {str(e)}")
        logger.error(traceback.format_exc())
        raise InvalidDataError(f"An unexpected error occurred with MUFG FX downloader: {str(e)}") from e

def save_fx_rates(fx_data_list):
    """
    Save FX rate data to CSV files
    
    Args:
        fx_data_list: List of dictionaries with FX rate data
    
    Returns:
        tuple: Paths to daily and master CSV files
    """
    if not fx_data_list:
        logger.warning("No FX data to save")
        return None, None
    
    # Create DataFrame
    df = pd.DataFrame(fx_data_list)
    
    # Get timestamp for file naming
    timestamp = fx_data_list[0]['timestamp']
    
    # Daily snapshot filename
    daily_file = os.path.join(DATA_DIR, f"fx_data_{timestamp}.csv")
    
    # Save daily snapshot
    df.to_csv(daily_file, index=False)
    logger.info(f"Saved daily FX data to {daily_file}")
    
    # Master CSV path
    master_file = os.path.join(DATA_DIR, "fx_data_master.csv")
    
    # Append to master file if it exists, otherwise create it
    if os.path.exists(master_file):
        try:
            existing_df = pd.read_csv(master_file)
            
            # Check if this timestamp already exists in the master file
            if timestamp in existing_df['timestamp'].values:
                # Remove the existing entries with this timestamp
                existing_df = existing_df[existing_df['timestamp'] != timestamp]
            
            # Append new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(master_file, index=False)
            logger.info(f"Updated master FX data file {master_file}")
        except Exception as e:
            logger.error(f"Error updating master CSV: {str(e)}")
            # If error, just write new file
            df.to_csv(master_file, index=False)
            logger.info(f"Created new master FX data file {master_file}")
    else:
        # Create new master file
        df.to_csv(master_file, index=False)
        logger.info(f"Created new master FX data file {master_file}")
    
    return daily_file, master_file

def process_fx_rates():
    """Main function to process FX rates"""
    logger.info("Starting FX rate data processing")
    
    try:
        fx_data_list = download_mufg_fx_rates()
        if fx_data_list: # Should be true if no exception
            daily_file, master_file = save_fx_rates(fx_data_list)
            if daily_file and master_file:
                logger.info(f"Successfully saved FX rate data to {daily_file} and {master_file}")
                return True
        # This part might be unreachable if download_mufg_fx_rates is guaranteed to raise or return valid data
        logger.warning("MUFG FX rate download resulted in no data, though no direct error was raised.")
        return False
    except (MissingCriticalDataError, InvalidDataError, requests.exceptions.RequestException) as e:
        logger.error(f"MUFG FX rate data processing failed: {str(e)}")
        return False

if __name__ == "__main__":
    # import io # Moved to top
    
    success = process_fx_rates()
    
    if success:
        print(f"✅ Successfully processed FX rate data")
    else:
        print("❌ Failed to process FX rate data")
        exit(1)
