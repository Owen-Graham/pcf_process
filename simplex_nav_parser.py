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

def parse_simplex_nav_from_file(file_path):
    """
    Parse NAV data for ETF 318A from a local HTML file
    
    Args:
        file_path: Path to the HTML file
        
    Returns:
        dict: Dictionary with parsed NAV data
    """
    try:
        logger.info(f"Parsing NAV data from local file: {file_path}")
        
        # Read the HTML file
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
            
        # Parse as if it were a response from the website
        return parse_simplex_nav_from_html(html_content)
    
    except Exception as e:
        logger.error(f"Error parsing NAV data from file: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def parse_simplex_nav_from_html(html_content):
    """
    Parse NAV data for ETF 318A from HTML content
    
    Args:
        html_content: HTML content as string
        
    Returns:
        dict: Dictionary with parsed NAV data
    """
    try:
        # Parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the NAV value for 318A using multiple methods
        nav_float = None
        
        # Method 1: Find by div with id="code_318A"
        nav_div = soup.find('div', id='code_318A')
        
        if nav_div is not None and nav_div.text.strip():
            # Extract the NAV value and remove the yen symbol
            nav_text = nav_div.text.strip()
            nav_value = nav_text.replace('円', '')
            
            # Try to convert to float
            try:
                nav_float = float(nav_value.replace(',', ''))
                logger.info(f"Successfully parsed NAV for ETF 318A (method 1): {nav_float}")
            except ValueError:
                logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 1)")
        else:
            logger.warning("Could not find NAV value for ETF 318A using method 1")
            
        # Method 2: Try to find NAV by looking at the ETF row directly
        if nav_float is None:
            logger.info("Trying alternative method to find NAV...")
            
            # Find the row that contains "318A"
            etf_rows = soup.find_all('tr')
            for row in etf_rows:
                cells = row.find_all('td')
                if len(cells) >= 2 and "318A" in cells[1].text.strip():
                    # ETF code is typically in the second column
                    # NAV is typically in the fifth column (index 4)
                    if len(cells) >= 5:
                        nav_cell = cells[4]
                        
                        # Try to extract the NAV value
                        nav_text = nav_cell.text.strip()
                        if nav_text:
                            # Remove any div tags if present
                            if nav_cell.find('div'):
                                nav_text = nav_cell.find('div').text.strip()
                                
                            # Remove the yen symbol and try to convert to float
                            nav_value = nav_text.replace('円', '')
                            try:
                                nav_float = float(nav_value.replace(',', ''))
                                logger.info(f"Successfully parsed NAV for ETF 318A (method 2): {nav_float}")
                                break
                            except ValueError:
                                logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 2)")
            
            if nav_float is None:
                logger.warning("Could not find NAV value using method 2")
        
        # Method 3: Try using regular expression directly on the HTML
        if nav_float is None:
            logger.info("Trying regex method to find NAV...")
            
            # Pattern to match: id="code_318A">VALUE円</div>
            import re
            patterns = [
                r'id="code_318A"[^>]*>(\d+[,.]?\d*)円?</div>',
                r'id=code_318A[^>]*>(\d+[,.]?\d*)円?</div>',
                r'>318A</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>(\d+[,.]?\d*)円?</td>'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html_content)
                if match:
                    nav_value = match.group(1)
                    try:
                        nav_float = float(nav_value.replace(',', ''))
                        logger.info(f"Successfully parsed NAV for ETF 318A (regex method): {nav_float}")
                        break
                    except ValueError:
                        logger.warning(f"Could not convert regex-extracted NAV value '{nav_value}' to float")
        
        if nav_float is None:
            logger.error("Could not extract NAV for ETF 318A")
            return None
            
        logger.info(f"Final NAV value for ETF 318A: {nav_float}")
        
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
                
        # If fund_date wasn't found, try regex
        if fund_date is None:
            import re
            date_pattern = r'id="bDate"[^>]*>(\d{4}\.\d{1,2}\.\d{1,2})</span>'
            date_match = re.search(date_pattern, html_content)
            if date_match:
                date_str = date_match.group(1)
                try:
                    # Convert to YYYYMMDD format
                    date_parts = date_str.split('.')
                    if len(date_parts) == 3:
                        fund_date = f"{date_parts[0]}{date_parts[1].zfill(2)}{date_parts[2].zfill(2)}"
                        logger.info(f"Found fund date (regex): {fund_date}")
                except Exception as e:
                    logger.warning(f"Could not parse fund date from regex: {str(e)}")
        
        # If fund_date still wasn't found, use current date
        if fund_date is None:
            fund_date = datetime.now().strftime("%Y%m%d")
            logger.info(f"Using current date as fund date: {fund_date}")
            
        # Create the NAV data dictionary
        nav_data = {
            'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
            'source': "https://www.simplexasset.com/etf/eng/etf.html",
            'fund_date': fund_date,
            'nav': nav_float,
            'fund_code': '318A'
        }
        
        return nav_data
        
    except Exception as e:
        logger.error(f"Error parsing NAV data from HTML: {str(e)}")
        logger.error(traceback.format_exc())
        return None

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
        
        # Find the NAV value for 318A using multiple methods
        nav_float = None
        
        # Method 1: Find by div with id="code_318A"
        nav_div = soup.find('div', id='code_318A')
        
        if nav_div is not None and nav_div.text.strip():
            # Extract the NAV value and remove the yen symbol
            nav_text = nav_div.text.strip()
            nav_value = nav_text.replace('円', '')
            
            # Try to convert to float
            try:
                nav_float = float(nav_value.replace(',', ''))
                logger.info(f"Successfully parsed NAV for ETF 318A (method 1): {nav_float}")
            except ValueError:
                logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 1)")
        else:
            logger.warning("Could not find NAV value for ETF 318A using method 1")
            
        # Method 2: Try to find NAV by looking at the ETF row directly
        if nav_float is None:
            logger.info("Trying alternative method to find NAV...")
            
            # Find the row that contains "318A"
            etf_rows = soup.find_all('tr')
            for row in etf_rows:
                cells = row.find_all('td')
                if len(cells) >= 2 and "318A" in cells[1].text.strip():
                    # ETF code is typically in the second column
                    # NAV is typically in the fifth column (index 4)
                    if len(cells) >= 5:
                        nav_cell = cells[4]
                        
                        # Try to extract the NAV value
                        nav_text = nav_cell.text.strip()
                        if nav_text:
                            # Remove any div tags if present
                            if nav_cell.find('div'):
                                nav_text = nav_cell.find('div').text.strip()
                                
                            # Remove the yen symbol and try to convert to float
                            nav_value = nav_text.replace('円', '')
                            try:
                                nav_float = float(nav_value.replace(',', ''))
                                logger.info(f"Successfully parsed NAV for ETF 318A (method 2): {nav_float}")
                                break
                            except ValueError:
                                logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 2)")
            
            if nav_float is None:
                logger.warning("Could not find NAV value using method 2")
        
        # Method 3: Try using regular expression directly on the HTML
        if nav_float is None:
            logger.info("Trying regex method to find NAV...")
            
            # Pattern to match: id="code_318A">VALUE円</div>
            import re
            patterns = [
                r'id="code_318A"[^>]*>(\d+[,.]?\d*)円?</div>',
                r'id=code_318A[^>]*>(\d+[,.]?\d*)円?</div>',
                r'>318A</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>(\d+[,.]?\d*)円?</td>'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, response.text)
                if match:
                    nav_value = match.group(1)
                    try:
                        nav_float = float(nav_value.replace(',', ''))
                        logger.info(f"Successfully parsed NAV for ETF 318A (regex method): {nav_float}")
                        break
                    except ValueError:
                        logger.warning(f"Could not convert regex-extracted NAV value '{nav_value}' to float")
        
        # Save the HTML for debugging if we still couldn't extract the NAV
        if nav_float is None:
            debug_html_path = os.path.join(SAVE_DIR, "simplex_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.error(f"Could not extract NAV for ETF 318A. Saved HTML to {debug_html_path} for debugging")
            return None
            
        logger.info(f"Final NAV value for ETF 318A: {nav_float}")
            
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

def process_simplex_nav(use_local_file=False):
    """Main function to process Simplex NAV data
    
    Args:
        use_local_file: If True, use the local HTML file instead of fetching from the website
    
    Returns:
        bool: True if processing was successful, False otherwise
    """
    logger.info("Starting Simplex NAV data processing")
    
    # Parse NAV data
    nav_data = None
    
    if use_local_file:
        local_html = "Lists of all ETFs _ Simplex Asset management.html"
        if os.path.exists(local_html):
            logger.info(f"Using local HTML file: {local_html}")
            nav_data = parse_simplex_nav_from_file(local_html)
            if nav_data:
                # Update the source to indicate it's from the local file
                nav_data['source'] = "https://www.simplexasset.com/etf/eng/etf.html (local file)"
        else:
            logger.error(f"Local HTML file not found: {local_html}")
    else:
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
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Parse Simplex Asset Management NAV data")
    parser.add_argument("--local", action="store_true", help="Use local HTML file instead of fetching from website")
    args = parser.parse_args()
    
    # Process NAV data
    success = process_simplex_nav(use_local_file=args.local)
    
    if success:
        print(f"✅ Successfully processed Simplex NAV data")
    else:
        print("❌ Failed to process Simplex NAV data")
        exit(1)
