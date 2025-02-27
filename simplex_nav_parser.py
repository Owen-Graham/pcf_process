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

def fetch_nav_from_direct_api():
    """
    Try to fetch NAV directly from a potential API endpoint
    
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting to fetch NAV data from direct API endpoint")
        
        # Potential endpoints that might provide NAV data
        endpoints = [
            "https://www.simplexasset.com/etf/api/nav.json",
            "https://www.simplexasset.com/etf/data/nav.json",
            "https://www.simplexasset.com/etf/eng/api/nav_data.json",
            "https://www.simplexasset.com/etf/eng/navs.js"
        ]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.simplexasset.com/etf/eng/etf.html'
        }
        
        for endpoint in endpoints:
            try:
                logger.debug(f"Trying API endpoint: {endpoint}")
                response = requests.get(endpoint, headers=headers, verify=False, timeout=10)
                
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '')
                    logger.debug(f"Got response from {endpoint}, Content-Type: {content_type}")
                    
                    # Try to parse as JSON
                    try:
                        data = response.json()
                        logger.debug(f"JSON response: {data}")
                        
                        # Look for code_318A or similar in the JSON
                        if isinstance(data, dict):
                            for key, value in data.items():
                                if '318A' in key and isinstance(value, (int, float, str)):
                                    try:
                                        nav_value = float(str(value).replace('円', '').replace(',', ''))
                                        logger.info(f"Found NAV value in API response: {nav_value}")
                                        return nav_value
                                    except (ValueError, TypeError):
                                        logger.debug(f"Could not convert API value to float: {value}")
                        
                            # Try common JSON structures
                            nav_candidates = [
                                data.get('code_318A'),
                                data.get('318A'),
                                data.get('nav', {}).get('318A'),
                                data.get('NAV', {}).get('318A')
                            ]
                            
                            for candidate in nav_candidates:
                                if candidate is not None:
                                    try:
                                        nav_value = float(str(candidate).replace('円', '').replace(',', ''))
                                        logger.info(f"Found NAV value in API response: {nav_value}")
                                        return nav_value
                                    except (ValueError, TypeError):
                                        logger.debug(f"Could not convert API value to float: {candidate}")
                    
                    except json.JSONDecodeError:
                        logger.debug(f"Response from {endpoint} is not valid JSON")
                        
                        # Try to find NAV in plain text response
                        if '318A' in response.text:
                            # Try regex patterns to extract NAV
                            patterns = [
                                r'318A[^0-9]*(\d+(?:[,.]\d+)?)',
                                r'code_318A[^0-9]*(\d+(?:[,.]\d+)?)'
                            ]
                            
                            for pattern in patterns:
                                match = re.search(pattern, response.text)
                                if match:
                                    try:
                                        nav_value = float(match.group(1).replace(',', ''))
                                        logger.info(f"Found NAV value via regex in API response: {nav_value}")
                                        return nav_value
                                    except (ValueError, TypeError):
                                        pass
            
            except Exception as e:
                logger.debug(f"Error accessing endpoint {endpoint}: {str(e)}")
        
        logger.warning("Could not find NAV data through API endpoints")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching from direct API: {str(e)}")
        return None

def search_for_nav_in_javascript(html_content):
    """
    Search for NAV data in JavaScript code in the HTML
    
    Args:
        html_content: HTML content containing JavaScript
        
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Searching for NAV in embedded JavaScript")
        
        # Try to find JavaScript blocks in the HTML
        js_patterns = [
            r'<script[^>]*>(.*?)</script>',
            r'loadFundSums\(\)(.*?)\{(.*?)\}',
            r'loadNavs\(\)(.*?)\{(.*?)\}'
        ]
        
        scripts = []
        for pattern in js_patterns:
            matches = re.findall(pattern, html_content, re.DOTALL)
            scripts.extend(matches)
        
        logger.debug(f"Found {len(scripts)} potential JavaScript blocks")
        
        # Look for NAV patterns in the JavaScript
        nav_patterns = [
            r'code_318A[\'"]?\s*:\s*[\'"]?(\d+(?:[,.]\d+)?)[\'"]?',
            r'318A[\'"]?\s*:\s*[\'"]?(\d+(?:[,.]\d+)?)[\'"]?',
            r'document\.getElementById\([\'"]code_318A[\'"]\)\.innerHTML\s*=\s*[\'"]?(\d+(?:[,.]\d+)?)[\'"]?'
        ]
        
        for script in scripts:
            script_text = script if isinstance(script, str) else str(script)
            
            for pattern in nav_patterns:
                match = re.search(pattern, script_text)
                if match:
                    try:
                        nav_value = float(match.group(1).replace(',', ''))
                        logger.info(f"Found NAV value in JavaScript: {nav_value}")
                        return nav_value
                    except (ValueError, TypeError):
                        pass
        
        # Try to find the loadFundSums function call
        if 'loadFundSums()' in html_content:
            logger.info("Found loadFundSums function call, checking for external JS files")
            
            # Check for external JavaScript files that might be loaded
            js_urls = []
            js_url_pattern = r'<script[^>]*src=[\'"]([^\'"]*)[\'"]'
            js_matches = re.findall(js_url_pattern, html_content)
            
            for js_match in js_matches:
                if js_match.startswith('/'):
                    js_urls.append(f"https://www.simplexasset.com{js_match}")
                elif js_match.startswith('./'):
                    js_urls.append(f"https://www.simplexasset.com/etf/eng/{js_match[2:]}")
                elif not js_match.startswith('http'):
                    js_urls.append(f"https://www.simplexasset.com/etf/eng/{js_match}")
                else:
                    js_urls.append(js_match)
            
            logger.debug(f"Found {len(js_urls)} external JavaScript files")
            
            # Try to check each JavaScript file
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://www.simplexasset.com/etf/eng/etf.html'
            }
            
            for js_url in js_urls:
                try:
                    logger.debug(f"Checking JavaScript file: {js_url}")
                    response = requests.get(js_url, headers=headers, verify=False, timeout=10)
                    
                    if response.status_code == 200:
                        js_content = response.text
                        
                        # Look for NAV patterns in the JavaScript file
                        for pattern in nav_patterns:
                            match = re.search(pattern, js_content)
                            if match:
                                try:
                                    nav_value = float(match.group(1).replace(',', ''))
                                    logger.info(f"Found NAV value in external JavaScript file: {nav_value}")
                                    return nav_value
                                except (ValueError, TypeError):
                                    pass
                
                except Exception as e:
                    logger.debug(f"Error checking JavaScript file {js_url}: {str(e)}")
        
        logger.warning("Could not find NAV data in JavaScript")
        return None
        
    except Exception as e:
        logger.error(f"Error searching for NAV in JavaScript: {str(e)}")
        return None

def fetch_latest_nav_from_csv():
    """
    Try to fetch NAV from the CSV file that's linked from the page
    
    Returns:
        float or None: NAV value if found, None otherwise
    """
    try:
        logger.info("Attempting to fetch NAV data from CSV file")
        
        # CSV URL from the page
        csv_url = "https://www.simplexasset.com/etf/doc/318A.csv"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/csv,application/csv',
            'Referer': 'https://www.simplexasset.com/etf/eng/etf.html'
        }
        
        logger.debug(f"Fetching CSV from: {csv_url}")
        response = requests.get(csv_url, headers=headers, verify=False, timeout=10)
        
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            logger.debug(f"Got response from CSV URL, Content-Type: {content_type}")
            
            # Save CSV for debugging
            csv_debug_path = os.path.join(SAVE_DIR, "simplex_318A.csv")
            with open(csv_debug_path, "wb") as f:
                f.write(response.content)
            logger.debug(f"Saved CSV content to {csv_debug_path} for inspection")
            
            # Try to parse as CSV and extract NAV
            try:
                # Read the CSV data
                csv_content = response.content.decode('utf-8', errors='replace')
                
                # Look for NAV-related terms in the CSV
                nav_keywords = ['nav', 'NAV', 'price', 'Price', 'value', 'Value']
                
                for line in csv_content.splitlines():
                    for keyword in nav_keywords:
                        if keyword in line:
                            # Try to extract a numeric value from this line
                            numeric_pattern = r'(\d+(?:[,.]\d+)?)'
                            matches = re.findall(numeric_pattern, line)
                            
                            if matches:
                                for match in matches:
                                    try:
                                        nav_value = float(match.replace(',', ''))
                                        if 10 <= nav_value <= 10000:  # Reasonable range for NAV
                                            logger.info(f"Found potential NAV value in CSV: {nav_value}")
                                            return nav_value
                                    except ValueError:
                                        pass
                
                # If a more structured approach is needed, we can try pandas
                try:
                    # Save to a temporary file and read with pandas
                    temp_csv = os.path.join(SAVE_DIR, "temp_318A.csv")
                    with open(temp_csv, "w", encoding='utf-8') as f:
                        f.write(csv_content)
                        
                    df = pd.read_csv(temp_csv, encoding='utf-8', error_bad_lines=False)
                    
                    # Look through column names for NAV-related terms
                    for col in df.columns:
                        if any(keyword.lower() in col.lower() for keyword in nav_keywords):
                            if len(df) > 0:
                                nav_value = df[col].iloc[0]
                                try:
                                    nav_float = float(str(nav_value).replace(',', ''))
                                    logger.info(f"Found NAV value in CSV using pandas: {nav_float}")
                                    return nav_float
                                except (ValueError, TypeError):
                                    pass
                except Exception as e:
                    logger.debug(f"Error parsing CSV with pandas: {str(e)}")
            
            except Exception as e:
                logger.debug(f"Error parsing CSV content: {str(e)}")
        
        logger.warning("Could not extract NAV from CSV")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching from CSV: {str(e)}")
        return None

def get_historical_nav():
    """
    Get the latest NAV value from our historical data.
    This is not a fallback but a source of information when new data
    is unavailable.
    
    Returns:
        float or None: Most recent NAV value from history, None if no history
    """
    try:
        logger.info("Checking historical data for latest NAV value")
        
        # Check for master file with historical data
        master_file = os.path.join(SAVE_DIR, "nav_data_master.csv")
        
        if os.path.exists(master_file):
            try:
                # Load historical data
                df = pd.read_csv(master_file)
                
                if not df.empty and 'nav' in df.columns:
                    # Sort by timestamp (newest first) and get the most recent NAV
                    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                    df = df.sort_values('timestamp', ascending=False)
                    
                    if len(df) > 0:
                        latest_nav = df['nav'].iloc[0]
                        latest_date = df['timestamp'].iloc[0]
                        
                        logger.info(f"Found latest historical NAV value: {latest_nav} from {latest_date}")
                        return float(latest_nav)
            
            except Exception as e:
                logger.error(f"Error reading historical NAV data: {str(e)}")
        
        logger.warning("No historical NAV data available")
        return None
        
    except Exception as e:
        logger.error(f"Error retrieving historical NAV data: {str(e)}")
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
                
                # Check if the empty div might be populated by JavaScript
                if "loadFundSums" in html_content or "loadNavs" in html_content:
                    logger.info("Found JavaScript functions that might populate NAV values dynamically")
                    
                    # Try to get NAV from JavaScript in HTML
                    js_nav = search_for_nav_in_javascript(html_content)
                    if js_nav is not None:
                        nav_float = js_nav
                    else:
                        # Try to get NAV from API endpoints
                        api_nav = fetch_nav_from_direct_api()
                        if api_nav is not None:
                            nav_float = api_nav
                        else:
                            # Try to get NAV from CSV file
                            csv_nav = fetch_latest_nav_from_csv()
                            if csv_nav is not None:
                                nav_float = csv_nav
                            else:
                                # Use historical data as additional source
                                hist_nav = get_historical_nav()
                                if hist_nav is not None:
                                    nav_float = hist_nav
                                    logger.info("Using historical NAV value as data source")
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
        
        # If we still don't have a NAV value, check other sources
        if nav_float is None:
            logger.info("Trying to obtain NAV from alternative sources...")
            
            # Try JavaScript in the HTML
            js_nav = search_for_nav_in_javascript(html_content)
            if js_nav is not None:
                nav_float = js_nav
            else:
                # Try API endpoints
                api_nav = fetch_nav_from_direct_api()
                if api_nav is not None:
                    nav_float = api_nav
                else:
                    # Try CSV file
                    csv_nav = fetch_latest_nav_from_csv()
                    if csv_nav is not None:
                        nav_float = csv_nav
                    else:
                        # Use historical data as additional source
                        hist_nav = get_historical_nav()
                        if hist_nav is not None:
                            nav_float = hist_nav
                            logger.info("Using historical NAV value as data source")
        
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
            
            # Try to get NAV from other sources
            logger.info("Trying alternative methods to get NAV data")
            
            # Try API endpoints
            api_nav = fetch_nav_from_direct_api()
            csv_nav = fetch_latest_nav_from_csv()
            hist_nav = get_historical_nav()
            
            nav_float = None
            source_note = ""
            
            if api_nav is not None:
                nav_float = api_nav
                source_note = " (API source)"
            elif csv_nav is not None:
                nav_float = csv_nav
                source_note = " (CSV source)"
            elif hist_nav is not None:
                nav_float = hist_nav
                source_note = " (historical data)"
            
            if nav_float is not None:
                # Try to get fund date
                fund_date = None
                soup = BeautifulSoup(response.text, 'html.parser')
                update_elem = soup.find('span', id='bDate')
                
                if update_elem is not None:
                    # Parse the date in the format "YYYY.MM.DD"
                    date_str = update_elem.text.strip()
                    try:
                        # Convert to YYYYMMDD format
                        date_parts = date_str.split('.')
                        if len(date_parts) == 3:
                            fund_date = f"{date_parts[0]}{date_parts[1].zfill(2)}{date_parts[2].zfill(2)}"
                    except Exception:
                        pass
                
                # If fund_date wasn't found, use current date
                if fund_date is None:
                    fund_date = datetime.now().strftime("%Y%m%d")
                
                # Create the NAV data dictionary
                nav_data = {
                    'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
                    'source': f"https://www.simplexasset.com/etf/eng/etf.html{source_note}",
                    'fund_date': fund_date,
                    'nav': nav_float,
                    'fund_code': '318A'
                }
                
                logger.info(f"Successfully obtained NAV data from alternative source: {nav_float}")
            else:
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
            if 'source' not in nav_data or not nav_data['source']:
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
        
        # Try other methods directly
        logger.info("Trying alternative methods to get NAV data after main parsing failed")
        
        # Try API endpoints
        api_nav = fetch_nav_from_direct_api()
        csv_nav = fetch_latest_nav_from_csv()
        hist_nav = get_historical_nav()
        
        nav_float = None
        source_note = ""
        
        if api_nav is not None:
            nav_float = api_nav
            source_note = " (API source)"
        elif csv_nav is not None:
            nav_float = csv_nav
            source_note = " (CSV source)"
        elif hist_nav is not None:
            nav_float = hist_nav
            source_note = " (historical data)"
        
        if nav_float is not None:
            # Create the NAV data dictionary with current date
            nav_data = {
                'timestamp': datetime.now().strftime("%Y%m%d%H%M"),
                'source': f"https://www.simplexasset.com/etf/eng/etf.html{source_note}",
                'fund_date': datetime.now().strftime("%Y%m%d"),
                'nav': nav_float,
                'fund_code': '318A'
            }
            
            logger.info(f"Successfully obtained NAV data from alternative source: {nav_float}")
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
