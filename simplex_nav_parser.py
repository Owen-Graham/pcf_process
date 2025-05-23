import os
import re
import pandas as pd
from datetime import datetime
import logging
import traceback
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from common import setup_logging, SAVE_DIR, MissingCriticalDataError, InvalidDataError

# Set up logging
logger = setup_logging('simplex_nav_parser')

def parse_simplex_nav_with_browser():
    """
    Parse NAV data for ETF 318A from Simplex Asset Management website using a headless browser
    to properly render JavaScript content.
    
    Returns:
        dict: Dictionary with parsed NAV data or None if extraction fails
    """
    try:
        logger.info("Parsing NAV data using headless browser")
        
        # URL of the Simplex ETF page
        url = "https://www.simplexasset.com/etf/eng/etf.html"
        
        # Configure Chrome options for headless operation
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Initialize the driver
        logger.info("Initializing Chrome WebDriver")
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            # Set page load timeout
            driver.set_page_load_timeout(30)
            
            # Load the page
            logger.info(f"Loading URL: {url}")
            driver.get(url)
            
            # Wait for the page to load and potentially for JavaScript to execute
            logger.info("Waiting for page to load...")
            time.sleep(5)  # Give JavaScript time to execute
            
            try:
                # Wait for a specific element to be present to ensure the page is loaded
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "list-table"))
                )
                logger.info("Page loaded successfully")
            except Exception as e:
                logger.warning(f"Timeout waiting for table element: {str(e)}")
                # Continue anyway as the page might still be usable
            
            # Try to find the fund date
            fund_date = None
            try:
                date_element = driver.find_element(By.ID, "bDate")
                if date_element:
                    date_str = date_element.text.strip()
                    # Convert to YYYYMMDD format
                    date_parts = date_str.split('.')
                    if len(date_parts) == 3:
                        fund_date = f"{date_parts[0]}{date_parts[1].zfill(2)}{date_parts[2].zfill(2)}"
                        logger.info(f"Found fund date: {fund_date}")
            except Exception as e:
                logger.warning(f"Could not extract fund date: {str(e)}")
            
            # If fund_date wasn't found, use current date
            if fund_date is None:
                raise MissingCriticalDataError("Could not extract fund date from Simplex website.")
            
            # Try to find the NAV value
            nav_float = None
            source_note = ""
            
            # Method 1: Direct approach - look for the element with id="code_318A"
            try:
                nav_element = driver.find_element(By.ID, "code_318A")
                if nav_element:
                    nav_text = nav_element.text.strip()
                    logger.info(f"Found NAV element with text: '{nav_text}'")
                    
                    if nav_text:
                        # Remove yen symbol if present
                        nav_value = nav_text.replace('円', '').replace(',', '')
                        try:
                            nav_float = float(nav_value)
                            logger.info(f"Extracted NAV value: {nav_float}")
                            source_note = " (browser rendered)"
                        except ValueError as e:
                            raise InvalidDataError(f"Could not convert NAV text '{nav_text}' to float (direct find): {str(e)}")
                    else:
                        logger.warning("NAV element is empty")
            except Exception as e:
                logger.warning(f"Could not find NAV element directly: {str(e)}")
            
            # Method 2: If direct method failed, try to find the element in the context of the table
            if nav_float is None:
                try:
                    # Find the row containing "318A"
                    rows = driver.find_elements(By.TAG_NAME, "tr")
                    for row in rows:
                        if "318A" in row.text:
                            logger.info(f"Found row with 318A text: '{row.text}'")
                            
                            # Try to find the NAV cell (typically the 5th cell)
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 5:  # We need at least 5 cells
                                nav_cell = cells[4]  # 5th cell (0-indexed)
                                nav_text = nav_cell.text.strip()
                                logger.info(f"5th cell text: '{nav_text}'")
                                
                                if nav_text:
                                    # Remove yen symbol if present
                                    nav_value = nav_text.replace('円', '').replace(',', '')
                                    try:
                                        nav_float = float(nav_value)
                                        logger.info(f"Extracted NAV value from table: {nav_float}")
                                        source_note = " (table cell)"
                                    except ValueError as e:
                                        raise InvalidDataError(f"Could not convert NAV text '{nav_text}' from table cell to float: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error finding NAV in table: {str(e)}")
            
            # Method 3: Execute JavaScript to try to get the value
            if nav_float is None:
                try:
                    # Try to execute loadFundSums function if it exists
                    driver.execute_script("if(typeof loadFundSums === 'function') { loadFundSums(); }")
                    time.sleep(2)  # Wait for function to complete
                    
                    # Check if the element has been populated now
                    nav_element = driver.find_element(By.ID, "code_318A")
                    if nav_element:
                        nav_text = nav_element.text.strip()
                        logger.info(f"After JS execution, NAV element text: '{nav_text}'")
                        
                        if nav_text:
                            # Remove yen symbol if present
                            nav_value = nav_text.replace('円', '').replace(',', '')
                            try:
                                nav_float = float(nav_value)
                                logger.info(f"Extracted NAV value after JS execution: {nav_float}")
                                source_note = " (JS execution)"
                            except ValueError as e:
                                raise InvalidDataError(f"Could not convert NAV text '{nav_text}' after JS execution to float: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error executing JavaScript: {str(e)}")
            
            # Method 4: Extract values from rendered page
            if nav_float is None:
                try:
                    # Get the page source after JavaScript has executed
                    page_source = driver.page_source
                    
                    # Save for debugging
                    debug_file = os.path.join(SAVE_DIR, "rendered_page.html")
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(page_source)
                    logger.info(f"Saved rendered page source to {debug_file}")
                    
                    # Look for patterns in the rendered HTML
                    patterns = [
                        r'id="code_318A"[^>]*>(\d+(?:[,.]\d+)?)',
                        r'318A.*?(\d+(?:[,.]\d+)?)\s*円',
                        r'SIMPLEX VIX Short-Term Futures ETF.*?(\d+(?:[,.]\d+)?)\s*円'
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, page_source)
                        if matches:
                            for match in matches:
                                try:
                                    value = float(match.replace(',', ''))
                                    if 10 <= value <= 10000:  # Reasonable range check
                                        nav_float = value
                                        logger.info(f"Extracted NAV value from rendered HTML: {nav_float}")
                                        source_note = " (rendered HTML)"
                                        break
                                except ValueError:
                                    logger.warning(f"Could not convert potential NAV match '{match}' from rendered HTML to float. Trying next match."); continue
                        if nav_float is not None:
                            break
                except Exception as e:
                    logger.warning(f"Error extracting from rendered HTML: {str(e)}")
            
            # Method 5: Take a screenshot for manual analysis
            if nav_float is None:
                try:
                    screenshot_file = os.path.join(SAVE_DIR, "simplex_screenshot.png")
                    driver.save_screenshot(screenshot_file)
                    logger.info(f"Saved screenshot to {screenshot_file} for manual analysis")
                except Exception as e:
                    logger.warning(f"Error taking screenshot: {str(e)}")
            
            # If we couldn't extract a NAV value, return None
            if nav_float is None:
                # Attempt to save screenshot before raising error, if driver is available
                if driver:
                    try:
                        screenshot_file = os.path.join(SAVE_DIR, "simplex_error_screenshot.png")
                        driver.save_screenshot(screenshot_file)
                        logger.info(f"Saved error screenshot to {screenshot_file}")
                    except Exception as se:
                        logger.warning(f"Error taking screenshot during error handling: {str(se)}")
                raise MissingCriticalDataError("Could not extract NAV value using any method from Simplex website.")
            
            # Create the NAV data dictionary
            nav_data = {
                'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
                'source': f"https://www.simplexasset.com/etf/eng/etf.html{source_note}",
                'fund_date': fund_date,
                'nav': nav_float,
                'fund_code': '318A'
            }
            
            logger.info(f"Final NAV data: {nav_data}")
            return nav_data
            
        finally:
            # Always close the browser
            try:
                driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.warning(f"Error closing WebDriver: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in headless browser NAV parsing: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return None to indicate failure
        raise MissingCriticalDataError(f"Failed to parse Simplex NAV data due to an overarching error: {str(e)}") from e

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
    
    try:
        nav_data = parse_simplex_nav_with_browser()
        if nav_data: # Should be true if no exception
            daily_file, master_file = save_nav_data(nav_data)
            if daily_file and master_file:
                logger.info(f"Successfully saved NAV data to {daily_file} and {master_file}")
                return True
        # This part might be unreachable if parse_simplex_nav_with_browser is guaranteed to raise or return valid data
        logger.warning("Simplex NAV parsing resulted in no data, though no direct error was raised.")
        return False
    except (MissingCriticalDataError, InvalidDataError) as e:
        logger.error(f"Simplex NAV parsing failed: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Parse Simplex Asset Management NAV data using headless browser")
    parser.add_argument("--debug", action="store_true", help="Enable extra debug logging")
    parser.add_argument("--output-dir", help="Directory to save output files (default: data)")
    args = parser.parse_args()
        
    # Configure extra debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        
    # Override save directory if specified
    if args.output_dir:
        SAVE_DIR = args.output_dir
        os.makedirs(SAVE_DIR, exist_ok=True)
        
    # Process NAV data
    success = process_simplex_nav()
    
    if success:
        print(f"✅ Successfully processed Simplex NAV data")
    else:
        print("❌ Failed to process Simplex NAV data")
        exit(1)
