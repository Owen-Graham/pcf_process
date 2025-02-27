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
        
        if nav_div is not None:
            logger.debug(f"Found nav_div element: {nav_div}")
            logger.debug(f"nav_div attributes: {nav_div.attrs}")
            logger.debug(f"nav_div parent: {nav_div.parent}")
            
            # Extract the NAV value and remove the yen symbol
            nav_text = nav_div.text.strip()
            logger.debug(f"nav_div text content: '{nav_text}'")
            
            if nav_text:
                nav_value = nav_text.replace('円', '')
                logger.debug(f"Cleaned nav_value: '{nav_value}'")
                
                # Try to convert to float
                try:
                    nav_float = float(nav_value.replace(',', ''))
                    logger.info(f"Successfully parsed NAV for ETF 318A (method 1): {nav_float}")
                except ValueError as e:
                    logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 1): {str(e)}")
            else:
                logger.warning("nav_div element found but text content is empty")
        else:
            logger.warning("Could not find NAV value for ETF 318A using method 1 (div#code_318A not found)")
            
            # Debug other divs with similar IDs
            similar_divs = soup.find_all('div', id=lambda x: x and 'code_' in x)
            logger.debug(f"Found {len(similar_divs)} other divs with 'code_' in id")
            for div in similar_divs[:5]:  # Show up to 5 examples
                logger.debug(f"Similar div: id={div.get('id')}, text='{div.text.strip()}'")
                
            # Debug if 318A appears anywhere in the HTML
            if '318A' in html_content:
                logger.debug("String '318A' found in HTML content")
                index = html_content.find('318A')
                context = html_content[max(0, index-100):min(len(html_content), index+200)]
                logger.debug(f"Context around '318A': {context}")
            else:
                logger.debug("String '318A' NOT found in HTML content")
            
        # Method 2: Try to find NAV by looking at the ETF row directly
        if nav_float is None:
            logger.info("Trying alternative method to find NAV (method 2)...")
            
            # Find all table rows
            etf_rows = soup.find_all('tr')
            logger.debug(f"Found {len(etf_rows)} table rows in the HTML")
            
            # Record rows with "318A" for debugging
            matching_rows = []
            
            # Try to find the row that contains "318A"
            found_row = False
            for i, row in enumerate(etf_rows):
                cells = row.find_all('td')
                row_text = row.get_text()
                
                if "318A" in row_text:
                    logger.debug(f"Found row {i} containing '318A': {row}")
                    matching_rows.append(row)
                    found_row = True
                    
                    # Check if we have enough cells
                    logger.debug(f"Row has {len(cells)} cells")
                    
                    if len(cells) >= 2:
                        # Get the value of the second cell (typical code cell)
                        logger.debug(f"Second cell content: '{cells[1].text.strip()}'")
                        
                        if len(cells) >= 5:
                            # NAV is typically in the fifth column (index 4)
                            nav_cell = cells[4]
                            logger.debug(f"Fifth cell (potential NAV): {nav_cell}")
                            
                            # Try to extract the NAV value
                            nav_text = nav_cell.text.strip()
                            logger.debug(f"NAV cell text content: '{nav_text}'")
                            
                            if nav_text:
                                # Remove any div tags if present
                                if nav_cell.find('div'):
                                    div_elem = nav_cell.find('div')
                                    logger.debug(f"Found div in NAV cell: {div_elem}")
                                    nav_text = div_elem.text.strip()
                                    logger.debug(f"Div text: '{nav_text}'")
                                
                                # Remove the yen symbol and try to convert to float
                                nav_value = nav_text.replace('円', '')
                                logger.debug(f"Cleaned NAV value: '{nav_value}'")
                                
                                try:
                                    nav_float = float(nav_value.replace(',', ''))
                                    logger.info(f"Successfully parsed NAV for ETF 318A (method 2): {nav_float}")
                                    break
                                except ValueError as e:
                                    logger.warning(f"Could not convert NAV value '{nav_value}' to float (method 2): {str(e)}")
                        else:
                            logger.debug(f"Row doesn't have enough cells to find NAV (has {len(cells)}, need at least 5)")
            
            if not found_row:
                logger.warning("Could not find any rows containing '318A'")
            elif len(matching_rows) > 0:
                logger.debug(f"Found {len(matching_rows)} rows containing '318A' but could not extract NAV")
                
            if nav_float is None:
                logger.warning("Could not find NAV value using method 2")
        
        # Method 3: Try using regular expression directly on the HTML
        if nav_float is None:
            logger.info("Trying regex method to find NAV (method 3)...")
            
            # Log some statistics about the HTML content
            logger.debug(f"HTML content length: {len(html_content)} characters")
            logger.debug(f"HTML content contains '318A': {'318A' in html_content}")
            logger.debug(f"HTML content contains 'code_318A': {'code_318A' in html_content}")
            logger.debug(f"HTML content contains 'SIMPLEX VIX': {'SIMPLEX VIX' in html_content}")
            
            # Pattern to match various possible formats for the NAV
            import re
            patterns = [
                # Standard div format
                r'id="code_318A"[^>]*>(\d+[,.]?\d*)円?</div>',
                r'id=code_318A[^>]*>(\d+[,.]?\d*)円?</div>',
                
                # Table cell formats - try various positions
                r'>318A</td>\s*<td[^>]*>[^<]*</td>\s*<td[^>]*>(\d+[,.]?\d*)円?</td>',
                r'>318A</td>(?:.*?<td[^>]*>){3}(\d+[,.]?\d*)円?</td>',
                r'<td[^>]*>318A</td>(?:.*?<td[^>]*>){3}(\d+[,.]?\d*)円?</td>',
                
                # Look for section with 318A and then find a number with yen
                r'318A.*?(\d+[,.]?\d*)円',
                
                # Most generic - any div with NAV pattern near 318A
                r'318A.*?<div[^>]*>(\d+[,.]?\d*)円?</div>',
                
                # Direct table cell containing the value
                r'>318A<.*?<td[^>]*>(\d+[,.]?\d*)円?</td>',
                
                # Look for SIMPLEX VIX Short-Term Futures ETF
                r'SIMPLEX VIX Short-Term Futures ETF.*?(\d+[,.]?\d*)円',
                
                # Extremely generic - just look for a numeric value with yen near 318A
                r'318A.*?[\s>](\d{2,4}(?:,\d{3})?)円'
            ]
            
            for i, pattern in enumerate(patterns):
                logger.debug(f"Trying regex pattern {i+1}: {pattern}")
                
                try:
                    match = re.search(pattern, html_content)
                    if match:
                        nav_value = match.group(1)
                        logger.debug(f"Pattern {i+1} matched: '{nav_value}'")
                        logger.debug(f"Match groups: {match.groups()}")
                        
                        # Show some context around the match
                        start, end = match.span()
                        context_start = max(0, start - 50)
                        context_end = min(len(html_content), end + 50)
                        context = html_content[context_start:context_end]
                        logger.debug(f"Match context: '{context}'")
                        
                        try:
                            nav_float = float(nav_value.replace(',', ''))
                            logger.info(f"Successfully parsed NAV for ETF 318A (regex method {i+1}): {nav_float}")
                            break
                        except ValueError as e:
                            logger.warning(f"Could not convert regex-extracted NAV value '{nav_value}' to float: {str(e)}")
                    else:
                        logger.debug(f"Pattern {i+1} did not match")
                except Exception as e:
                    logger.warning(f"Error applying regex pattern {i+1}: {str(e)}")
            
            if nav_float is None:
                logger.warning("All regex patterns failed to extract a valid NAV value")
        
        # Save the HTML for debugging if we still couldn't extract the NAV
        if nav_float is None:
            debug_html_path = os.path.join(SAVE_DIR, "simplex_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.error(f"Could not extract NAV for ETF 318A. Saved HTML to {debug_html_path} for debugging")
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
        
        # Get the HTML content with more browser-like headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info(f"Requesting URL: {url}")
        
        # Try multiple times with different options
        for attempt in range(1, 4):
            try:
                logger.info(f"Attempt {attempt} to fetch URL")
                if attempt == 1:
                    # First attempt: standard request
                    response = requests.get(url, headers=headers, verify=False)
                elif attempt == 2:
                    # Second attempt: with session
                    session = requests.Session()
                    response = session.get(url, headers=headers, verify=False)
                else:
                    # Third attempt: with different timeout and no compression
                    headers['Accept-Encoding'] = 'identity'
                    response = requests.get(url, headers=headers, verify=False, timeout=30)
                
                response.raise_for_status()
                
                # Log response details
                logger.debug(f"Response status code: {response.status_code}")
                logger.debug(f"Response content type: {response.headers.get('Content-Type')}")
                logger.debug(f"Response content length: {len(response.text)} characters")
                logger.debug(f"Response encoding: {response.encoding}")
                
                # Log the first and last 100 chars of the response text
                if len(response.text) > 0:
                    logger.debug(f"Response text start: '{response.text[:100]}'")
                    logger.debug(f"Response text end: '{response.text[-100:]}'")
                
                # If response is too small, it might be an error page
                if len(response.text) < 1000:
                    logger.warning(f"Response too small ({len(response.text)} bytes), might be an error page")
                    continue
                    
                # Check if response contains the expected content
                has_318a = '318A' in response.text
                has_simplex_vix = 'SIMPLEX VIX' in response.text
                has_etf_table = '<table' in response.text and '</table>' in response.text
                
                logger.debug(f"Response contains '318A': {has_318a}")
                logger.debug(f"Response contains 'SIMPLEX VIX': {has_simplex_vix}")
                logger.debug(f"Response contains table tags: {has_etf_table}")
                
                if not has_318a:
                    logger.warning("Response does not contain '318A', might be redirected to a different page")
                    continue
                    
                # If we get here, the response looks valid
                logger.info(f"Successfully retrieved page with {len(response.text)} bytes")
                break
                
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {str(e)}")
                if attempt == 3:
                    # Last attempt failed, raise the exception
                    raise
        
        # Parse the HTML content
        nav_data = parse_simplex_nav_from_html(response.text)
        
        # If parsing failed, save HTML for debugging
        if nav_data is None:
            debug_html_path = os.path.join(SAVE_DIR, "simplex_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.error(f"Could not extract NAV data. Saved HTML to {debug_html_path} for debugging")
            
            # Try to find the local HTML file as a fallback
            local_html = "Lists of all ETFs _ Simplex Asset management.html"
            if os.path.exists(local_html):
                logger.info(f"Trying to parse from local HTML file: {local_html}")
                nav_data = parse_simplex_nav_from_file(local_html)
                if nav_data:
                    logger.info("Successfully parsed NAV data from local HTML file")
                    # Update the source to indicate it's from the local file
                    nav_data['source'] = f"{url} (local fallback)"
        
        # Update URL in the data if we have valid results
        if nav_data is not None:
            nav_data['source'] = url
            
        return nav_data
        
    except Exception as e:
        logger.error(f"Error parsing NAV data: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Try to find the local HTML file as a fallback
        local_html = "Lists of all ETFs _ Simplex Asset management.html"
        if os.path.exists(local_html):
            logger.info(f"Trying to parse from local HTML file: {local_html}")
            nav_data = parse_simplex_nav_from_file(local_html)
            if nav_data:
                logger.info("Successfully parsed NAV data from local HTML file")
                # Update the source to indicate it's from the local file
                nav_data['source'] = "https://www.simplexasset.com/etf/eng/etf.html (local fallback)"
                return nav_data
        
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
    success = process_simplex_nav(use_local_file=args.local)
    
    if success:
        print(f"✅ Successfully processed Simplex NAV data")
    else:
        print("❌ Failed to process Simplex NAV data")
        exit(1)
