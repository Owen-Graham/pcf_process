import os
import requests
import pandas as pd
from datetime import datetime
import logging
import traceback
import csv
import io

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

def download_mufg_fx_rates():
    """
    Download USDJPY FX rates from MUFG Bank website
    
    Returns:
        DataFrame: DataFrame with FX rate data or None if download fails
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
        
        # The file appears to be in Shift-JIS encoding (common for Japanese files)
        content = response.content.decode('shift_jis', errors='replace')
        
        # Save raw file for debugging
        raw_file_path = os.path.join(DATA_DIR, "mufg_fx_raw.csv")
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Saved raw CSV to: {raw_file_path}")
        
        # Parse the CSV data
        # First, let's see what structure the file has
        lines = content.strip().split('\n')
        
        # Create a dictionary to store the FX data
        fx_data = {
            'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
            'date': datetime.now().strftime("%Y-%m-%d"),
            'source': url
        }
        
        # Check if we can find USDJPY data (USD or ドル)
        usd_row = None
        for line in lines:
            if "USD" in line or "ドル" in line:
                usd_row = line
                break
        
        if usd_row:
            logger.info(f"Found USD row: {usd_row}")
            
            # Split the row by commas if it's proper CSV, or by spaces if not
            parts = usd_row.split(',')
            if len(parts) <= 1:  # Not comma-separated, try splitting by whitespace
                parts = usd_row.split()
            
            # Extract rate data based on the provided example
            # 'Ê‰Ý–¼ T.T.S. ACC. CASH S. T.T.B. A/S D/PED/A CASH B. USD (•Äƒhƒ‹) 150.24 150.67 152.04 148.24 147.81 147.51 146.24
            
            # Determine where the numeric rates start
            numeric_parts = []
            for part in parts:
                # Try to convert to float
                try:
                    float_val = float(part.strip())
                    numeric_parts.append(float_val)
                except ValueError:
                    continue
            
            if numeric_parts:
                logger.info(f"Found {len(numeric_parts)} numeric rates: {numeric_parts}")
                
                # Map the rates based on the expected order in the file
                rate_names = ["TTS", "ACC", "CASH_S", "TTB", "AS", "DPEDA", "CASH_B"]
                
                # Ensure we don't exceed the available data
                rate_names = rate_names[:len(numeric_parts)]
                
                # Add rates to fx_data dictionary
                for i, name in enumerate(rate_names):
                    if i < len(numeric_parts):
                        fx_data[f"USD_JPY_{name}"] = numeric_parts[i]
            else:
                logger.warning("No numeric rates found in USD row")
        else:
            logger.warning("USD row not found in CSV data")
        
        # Return the collected data
        return fx_data
    
    except Exception as e:
        logger.error(f"Error downloading FX rates: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def save_fx_rates(fx_data):
    """
    Save FX rate data to CSV files
    
    Args:
        fx_data: Dictionary with FX rate data
    
    Returns:
        tuple: Paths to daily and master CSV files
    """
    if not fx_data:
        logger.warning("No FX data to save")
        return None, None
    
    # Create DataFrame
    df = pd.DataFrame([fx_data])
    
    # Get timestamp for file naming
    timestamp = fx_data['timestamp']
    
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
                # Remove the existing entry with this timestamp
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
    
    # Download FX rates
    fx_data = download_mufg_fx_rates()
    
    if fx_data:
        # Save to CSV
        daily_file, master_file = save_fx_rates(fx_data)
        if daily_file and master_file:
            logger.info(f"Successfully saved FX rate data to {daily_file} and {master_file}")
            return True
    
    logger.warning("FX rate data processing failed")
    return False

if __name__ == "__main__":
    success = process_fx_rates()
    
    if success:
        print(f"✅ Successfully processed FX rate data")
    else:
        print("❌ Failed to process FX rate data")
        exit(1)
