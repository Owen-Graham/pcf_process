import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import traceback
from common import setup_logging, SAVE_DIR

# Set up logging
logger = setup_logging('simplex_nav_parser')

def parse_simplex_nav_data():
    """
    Parse NAV data for ETF 318A from Simplex Asset Management website
    
    Returns:
        dict: Dictionary with parsed NAV data
    """
    try:
        logger.info("Parsing NAV data for ETF 318A from Simplex website")
        
        # URL of the Simplex ETF page
        url = "https://www.simplexasset.com/etf/eng/etf.html"
        
        # Get the HTML content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the NAV value for 318A
        nav_div = soup.find('div', id='code_318A')
        
        if nav_div is None:
            logger.error("Could not find NAV value for ETF 318A")
            return None
            
        # Extract the NAV value and remove the yen symbol
        nav_text = nav_div.text.strip()
        nav_value = nav_text.replace('円', '')
        
        # Try to convert to float
        try:
            nav_float = float(nav_value.replace(',', ''))
            logger.info(f"Successfully parsed NAV for ETF 318A: {nav_float}")
        except ValueError:
            logger.error(f"Could not convert NAV value '{nav_value}' to float")
            return None
            
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        
        # Find the update date (if available)
        update_elem = soup.find('span', id='bDate')
        fund_date = None
        
        if update_elem is not None:
            # Parse the date in the format "YYYY.MM.DD"
            date_str = update_elem.text.strip()
            try:
                # Convert to YYYYMMDD format
                date_parts = date_str.split('.')
                if len(date_parts) == 3:
                    fund_date = f"{date_parts[0]}{date_parts[1].zfill(2)}{date_parts[2].zfill(2)}"
                    logger.info(f"Found fund date: {fund_date}")
            except Exception as e:
                logger.warning(f"Could not parse fund date: {str(e)}")
        
        # If fund_date wasn't found, use current date
        if fund_date is None:
            fund_date = datetime.now().strftime("%Y%m%d")
            logger.info(f"Using current date as fund date: {fund_date}")
            
        # Create the NAV data dictionary
        nav_data = {
            'timestamp': timestamp,
            'source': url,
            'fund_date': fund_date,
            'nav': nav_float,
            'fund_code': '318A'
        }
        
        return nav_data
        
    except Exception as e:
        logger.error(f"Error parsing NAV data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def save_nav_data(nav_data, save_dir=SAVE_DIR):
    """
    Save NAV data to CSV files
    
    Args:
        nav_data: Dictionary with NAV data
        save_dir: Directory to save the files
    
    Returns:
        tuple: Paths to daily and master CSV files
    """
    if not nav_data:
        logger.warning("No NAV data to save")
        return None, None
        
    # Create DataFrame
    df = pd.DataFrame([nav_data])
    
    # Daily snapshot filename
    timestamp = nav_data['timestamp']
    daily_file = os.path.join(save_dir, f"nav_data_{timestamp}.csv")
    
    # Save daily snapshot with all columns
    df.to_csv(daily_file, index=False)
    logger.info(f"Saved daily NAV data to {daily_file}")
    
    # Master CSV path
    master_file = os.path.join(save_dir, "nav_data_master.csv")
    
    # Prepare data for master file (with specified columns only)
    master_columns = ['timestamp', 'source', 'fund_date', 'nav']
    master_df = df[master_columns]
    
    # Append to master file if it exists, otherwise create it
    if os.path.exists(master_file):
        try:
            existing_df = pd.read_csv(master_file)
            
            # Check if this timestamp already exists in the master file
            if timestamp in existing_df['timestamp'].values:
                # Remove the existing entry with this timestamp
                existing_df = existing_df[existing_df['timestamp'] != timestamp]
                
            # Append new data
            combined_df = pd.concat([existing_df, master_df], ignore_index=True)
            combined_df.to_csv(master_file, index=False)
            logger.info(f"Updated master NAV data file {master_file}")
        except Exception as e:
            logger.error(f"Error updating master CSV: {str(e)}")
            # If error, just write new file
            master_df.to_csv(master_file, index=False)
            logger.info(f"Created new master NAV data file {master_file}")
    else:
        # Create new master file
        master_df.to_csv(master_file, index=False)
        logger.info(f"Created new master NAV data file {master_file}")
    
    return daily_file, master_file

def process_simplex_nav():
    """Main function to process Simplex NAV data"""
    logger.info("Starting Simplex NAV data processing")
    
    # Parse NAV data
    nav_data = parse_simplex_nav_data()
    
    if nav_data:
        # Save to CSV
        daily_file, master_file = save_nav_data(nav_data)
        if daily_file and master_file:
            logger.info(f"Successfully saved NAV data to {daily_file} and {master_file}")
            return True
    
    logger.warning("NAV data processing failed")
    return False

if __name__ == "__main__":
    success = process_simplex_nav()
    
    if success:
        print(f"✅ Successfully processed Simplex NAV data")
    else:
        print("❌ Failed to process Simplex NAV data")
        exit(1)
