import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import traceback
import json
from common import setup_logging, SAVE_DIR

# Set up logging
logger = setup_logging('simplex_nav_parser')

def extract_nav_from_onclick(html_content):
    """
    Extract NAV value from CSV onclick attributes in the HTML
    
    Args:
        html_content: HTML content as string
        
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting to extract NAV from CSV onclick attributes")
        
        # Pattern to match onclick="../doc/318A.csv" in same row as code_318A
        pattern = r'<tr[^>]*>.*?318A.*?onclick\s*=\s*["\']location\.href\s*=\s*["\']([^"\']*318A\.csv)["\'].*?</tr>'
        match = re.search(pattern, html_content, re.DOTALL)
        
        if match:
            csv_path = match.group(1)
            logger.info(f"Found CSV path in onclick: {csv_path}")
            
            # Construct the full URL
            if csv_path.startswith('../'):
                csv_url = f"https://www.simplexasset.com/etf/{csv_path.lstrip('../')}"
            else:
                csv_url = f"https://www.simplexasset.com{csv_path}"
            
            logger.info(f"Constructed CSV URL: {csv_url}")
            
            # Try to download with more specific headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',  # Accept any content type
                'Accept-Encoding': 'identity',
                'Referer': 'https://www.simplexasset.com/etf/eng/etf.html'
            }
            
            logger.info(f"Requesting CSV from URL: {csv_url}")
            response = requests.get(csv_url, headers=headers, verify=False)
            
            if response.status_code == 200:
                logger.info(f"Successfully downloaded CSV, content length: {len(response.content)} bytes")
                
                # Save the CSV for debugging
                csv_path = os.path.join(SAVE_DIR, "simplex_318A.csv")
                with open(csv_path, "wb") as f:
                    f.write(response.content)
                logger.debug(f"Saved CSV content to {csv_path}")
                
                # Try to decode and find NAV
                try:
                    # First just try to find numbers in the content
                    content = response.content.decode('utf-8', errors='replace')
                    
                    # Look for ETF Nav or similar fields
                    nav_patterns = [
                        r'(?:ETF[ _]?Nav|Nav|NAV|Net[ _]?Asset[ _]?Value)[^0-9]*(\d+(?:[,.]\d+)?)',
                        r'(?:Value|Price|価格)[^0-9]*(\d+(?:[,.]\d+)?)',
                        r'318A[^0-9]*(\d+(?:[,.]\d+)?)'
                    ]
                    
                    for pattern in nav_patterns:
                        matches = re.findall(pattern, content, re.IGNORECASE)
                        if matches:
                            for match in matches:
                                try:
                                    nav_value = float(match.replace(',', ''))
                                    if 10 <= nav_value <= 10000:  # Reasonable range for this ETF
                                        logger.info(f"Found NAV value in CSV: {nav_value}")
                                        return nav_value
                                except ValueError:
                                    pass
                    
                    # If not found with patterns, try to parse as CSV with pandas
                    try:
                        # Save to a temporary file
                        temp_path = os.path.join(SAVE_DIR, "temp_318A.csv")
                        with open(temp_path, "w", encoding='utf-8') as f:
                            f.write(content)
                            
                        # Try different parsing options
                        for skiprows in range(5):
                            try:
                                df = pd.read_csv(temp_path, skiprows=skiprows, encoding='utf-8')
                                logger.debug(f"CSV columns with skiprows={skiprows}: {df.columns.tolist()}")
                                
                                # Check if any column contains numeric values in a reasonable range
                                for col in df.columns:
                                    if df[col].dtype == 'float64' or df[col].dtype == 'int64':
                                        values = df[col].dropna()
                                        if len(values) > 0:
                                            for val in values:
                                                if 10 <= val <= 10000:
                                                    logger.info(f"Found numeric value in CSV column '{col}': {val}")
                                                    return float(val)
                            except:
                                continue
                    except Exception as e:
                        logger.debug(f"Error parsing CSV with pandas: {str(e)}")
                
                except Exception as e:
                    logger.warning(f"Error processing CSV content: {str(e)}")
        
        logger.warning("Could not extract NAV from CSV onclick attributes")
        return None
    
    except Exception as e:
        logger.error(f"Error in extract_nav_from_onclick: {str(e)}")
        return None

def hard_extract_nav_from_javascript(html_content):
    """
    Aggressively extract NAV from JavaScript variables or data structures
    
    Args:
        html_content: HTML content as string
        
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting aggressive JavaScript NAV extraction")
        
        # First, look for the loadFundSums function call
        if "loadFundSums" in html_content:
            logger.info("Found loadFundSums function call")
            
            # Try to find locations where JavaScript might be setting values for code_318A
            patterns = [
                # Common jQuery patterns
                r'\$("#code_318A"\)\.(?:html|text)\([\'"](.*?)[\'"]\)',
                r'\$("#code_318A"\)\.(?:html|text)\((.*?)\)',
                
                # Plain JavaScript patterns
                r'document\.getElementById\([\'"]code_318A[\'"]\)\.(?:innerHTML|textContent)\s*=\s*[\'"](.*?)[\'"]\s*;',
                r'document\.getElementById\([\'"]code_318A[\'"]\)\.(?:innerHTML|textContent)\s*=\s*([^;]*)\s*;',
                
                # Data object patterns
                r'data\[[\'"]318A[\'"]\]\s*=\s*[\'"](.*?)[\'"]\s*;',
                r'data\[[\'"]318A[\'"]\]\s*=\s*([^;]*)\s*;',
                r'data\[[\'"]code_318A[\'"]\]\s*=\s*[\'"](.*?)[\'"]\s*;',
                r'data\[[\'"]code_318A[\'"]\]\s*=\s*([^;]*)\s*;',
                
                # Hard-coded value patterns
                r'318A.*?[\'"](\d+(?:[,.]\d+)?).*?[\'"]',
                r'code_318A.*?[\'"](\d+(?:[,.]\d+)?).*?[\'"]',
                
                # General value pattern near 318A
                r'318A.*?(\d+(?:[,.]\d+)?)\s*円'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content)
                if matches:
                    logger.debug(f"Found matches with pattern {pattern}: {matches}")
                    for match in matches:
                        if isinstance(match, tuple):
                            match = match[0]  # Extract from capture group
                        
                        # Clean and validate
                        match = match.strip().replace('円', '').replace(',', '')
                        try:
                            # Try to convert to float
                            value = float(match)
                            if 10 <= value <= 10000:  # Reasonable range check
                                logger.info(f"Found potential NAV value in JavaScript: {value}")
                                return value
                        except ValueError:
                            pass
        
        # Check for any possible hardcoded values in the page
        hardcoded_pattern = r'318A.*?<[^>]*>(\d+(?:[,.]\d+)?)\s*円?</[^>]*>'
        matches = re.findall(hardcoded_pattern, html_content)
        if matches:
            logger.debug(f"Found hardcoded value matches: {matches}")
            for match in matches:
                try:
                    value = float(match.replace(',', ''))
                    if 10 <= value <= 10000:
                        logger.info(f"Found hardcoded NAV value: {value}")
                        return value
                except ValueError:
                    pass
        
        logger.warning("Could not extract NAV from JavaScript")
        return None
    
    except Exception as e:
        logger.error(f"Error in hard_extract_nav_from_javascript: {str(e)}")
        return None

def try_direct_browser_approach():
    """
    Try to use curl with browser-like parameters to get the NAV
    
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting direct browser-like approach with curl")
        
        import subprocess
        import tempfile
        
        # Create temporary file for output
        with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as tmp:
            tmp_path = tmp.name
        
        # Construct curl command with browser-like headers
        curl_cmd = [
            'curl', '-L', '-s', '--insecure',
            '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            '-H', 'Accept-Language: en-US,en;q=0.9,ja;q=0.8',
            '-H', 'Connection: keep-alive',
            '-o', tmp_path,
            'https://www.simplexasset.com/etf/eng/etf.html'
        ]
        
        logger.debug(f"Running curl command: {' '.join(curl_cmd)}")
        
        # Run curl command
        try:
            subprocess.run(curl_cmd, check=True)
            
            # Read the downloaded content
            with open(tmp_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            
            logger.info(f"Downloaded HTML with curl, size: {len(content)} bytes")
            
            # First use regular extraction method
            soup = BeautifulSoup(content, 'html.parser')
            nav_div = soup.find('div', id='code_318A')
            
            if nav_div and nav_div.text.strip():
                nav_text = nav_div.text.strip()
                logger.info(f"Found NAV in curl result: {nav_text}")
                try:
                    return float(nav_text.replace('円', '').replace(',', ''))
                except ValueError:
                    pass
            
            # Try aggressive extraction methods
            return hard_extract_nav_from_javascript(content)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Curl command failed: {str(e)}")
            return None
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    except Exception as e:
        logger.error(f"Error in try_direct_browser_approach: {str(e)}")
        return None

def get_manual_nav_value(html_content):
    """
    Last resort method - manually extract NAV by pattern matching against known ETF code
    
    Args:
        html_content: HTML content as string
        
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting manual NAV value extraction as last resort")
        
        # We know the structure of the page includes a row with 318A and its NAV
        # Let's get all <tr> elements that contain 318A
        soup = BeautifulSoup(html_content, 'html.parser')
        tr_elements = soup.find_all('tr')
        
        for tr in tr_elements:
            tr_text = tr.get_text()
            if '318A' in tr_text:
                logger.debug(f"Found row with 318A: {tr_text}")
                
                # The NAV is generally in the 5th td element
                td_elements = tr.find_all('td')
                if len(td_elements) >= 5:
                    # Get the text content of all cells in the row
                    cell_texts = [td.get_text().strip() for td in td_elements]
                    logger.debug(f"Cell texts in 318A row: {cell_texts}")
                    
                    # Check each cell for numeric values
                    for cell_text in cell_texts:
                        # Extract numeric values
                        numbers = re.findall(r'(\d+(?:[,.]\d+)?)', cell_text)
                        for num in numbers:
                            try:
                                value = float(num.replace(',', ''))
                                if 10 <= value <= 10000:  # Reasonable range check
                                    logger.info(f"Found potential NAV value in row cell: {value}")
                                    return value
                            except ValueError:
                                pass
        
        # Check if there's a specific pattern for this ETF in the HTML
        # For example, sometimes the site shows NAVs in a section outside the table
        nav_patterns = [
            r'318A\D+(\d+(?:[,.]\d+)?)\s*円',
            r'VIX Short-Term Futures ETF\D+(\d+(?:[,.]\d+)?)\s*円',
            r'code_318A\D+(\d+(?:[,.]\d+)?)\s*円',
        ]
        
        for pattern in nav_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                for match in matches:
                    try:
                        value = float(match.replace(',', ''))
                        if 10 <= value <= 10000:  # Reasonable range check
                            logger.info(f"Found NAV value with pattern: {value}")
                            return value
                    except ValueError:
                        pass
        
        # If we still haven't found anything, look for the last known NAV
        # For example, the value might be stored in a hidden div for historical values
        # This is a bit of a hack, but it might work in some cases
        logger.debug("Using hardcoded pattern for last resort extraction")
        pattern = r'(?:SIMPLEX VIX|318A)[^<]*<[^>]*>(\d+(?:[,.]\d+)?)\s*円?'
        matches = re.findall(pattern, html_content)
        if matches:
            for match in matches:
                try:
                    value = float(match.replace(',', ''))
                    if 10 <= value <= 10000:  # Reasonable range check
                        logger.info(f"Found possible NAV with last resort pattern: {value}")
                        return value
                except ValueError:
                    pass
        
        # Check for any number followed by 円 in the same table as 318A
        row_element = soup.find("tr", string=lambda text: "318A" in text if text else False)
        if row_element:
            table_element = row_element.find_parent("table")
            if table_element:
                table_text = table_element.get_text()
                yen_matches = re.findall(r'(\d+(?:[,.]\d+)?)\s*円', table_text)
                if yen_matches:
                    for match in yen_matches:
                        try:
                            value = float(match.replace(',', ''))
                            if 10 <= value <= 10000:  # Reasonable range check
                                logger.info(f"Found NAV value with yen pattern: {value}")
                                return value
                        except ValueError:
                            pass
        
        logger.warning("Manual NAV extraction failed")
        return None
    
    except Exception as e:
        logger.error(f"Error in get_manual_nav_value: {str(e)}")
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
        
        # Get the HTML content
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        logger.info(f"Successfully retrieved page with {len(response.text)} bytes")
        
        # Try to find the update date
        soup = BeautifulSoup(response.text, 'html.parser')
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
            date_match = re.search(date_pattern, response.text)
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
        
        # Try different methods to extract the NAV
        nav_float = None
        source_note = ""
        
        # Method 1: Try to extract from onclick CSV paths
        if nav_float is None:
            nav_float = extract_nav_from_onclick(response.text)
            if nav_float is not None:
                source_note = " (CSV onclick)"
        
        # Method 2: Try aggressive JavaScript extraction
        if nav_float is None:
            nav_float = hard_extract_nav_from_javascript(response.text)
            if nav_float is not None:
                source_note = " (JavaScript)"
        
        # Method 3: Try direct browser approach with curl
        if nav_float is None:
            nav_float = try_direct_browser_approach()
            if nav_float is not None:
                source_note = " (curl)"
        
        # Method 4: Last resort - manual extraction
        if nav_float is None:
            nav_float = get_manual_nav_value(response.text)
            if nav_float is not None:
                source_note = " (manual extraction)"
        
        # If all methods failed, return None
        if nav_float is None:
            # Save debug file for analysis
            debug_html_path = os.path.join(SAVE_DIR, "simplex_debug.html")
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.error(f"Could not extract NAV value. Saved HTML to {debug_html_path} for analysis")
            return None
        
        # Create the NAV data dictionary
        nav_data = {
            'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
            'source': f"https://www.simplexasset.com/etf/eng/etf.html{source_note}",
            'fund_date': fund_date,
            'nav': nav_float,
            'fund_code': '318A'
        }
        
        logger.info(f"Successfully created NAV data: {nav_data}")
        return nav_data
        
    except Exception as e:
        logger.error(f"Error parsing NAV data: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return None to indicate failure
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
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Parse Simplex Asset Management NAV data")
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
