def download_vix_futures_from_cboe():
    """
    Download VIX futures prices from CBOE website using Selenium
    
    Returns:
        dict: Dictionary with VIX futures prices
    """
    start_time = time.time()
    browser = None
    
    try:
        logger.info("Downloading VIX futures data from CBOE using Selenium...")
        
        # Set up Chrome options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        # Initialize the browser
        browser = webdriver.Chrome(options=options)
        
        # CBOE VIX futures page
        url = "https://www.cboe.com/tradable_products/vix/vix_futures/"
        
        # Navigate to the page
        logger.info(f"Navigating to {url}")
        browser.get(url)
        
        # Wait for the page to load completely
        logger.info("Waiting for page to load...")
        WebDriverWait(browser, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Get the page source after JavaScript execution
        page_source = browser.page_source
        
        # Save HTML for debugging
        debug_html_path = os.path.join(SAVE_DIR, "cboe_debug.html")
        with open(debug_html_path, "w", encoding="utf-8") as f:
            f.write(page_source)
        logger.debug(f"Saved CBOE HTML to {debug_html_path} for reference")
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Get current date
        futures_data = {
            'date': datetime.now().strftime("%Y-%m-%d"),
            'timestamp': datetime.now().strftime("%Y%m%d%H%M")
        }
        
        # Try to find VIX futures data table
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables on the page")
        
        for i, table in enumerate(tables):
            headers = []
            header_row = table.find('tr')
            if header_row:
                # Get table headers
                headers = [th.get_text().strip() for th in header_row.find_all(['th'])]
                if not headers and header_row.find_all(['td']):
                    # Sometimes headers are in td tags
                    headers = [td.get_text().strip() for td in header_row.find_all(['td'])]
                
                logger.info(f"Table {i+1} headers: {headers}")
                
                # Check if this looks like a VIX futures table
                if any(header.upper() in ['SYMBOL', 'EXPIRATION', 'SETTLEMENT', 'LAST'] for header in headers):
                    logger.info(f"Found potential VIX futures table (table {i+1})")
                    
                    # Map column indices
                    col_map = {}
                    for j, header in enumerate(headers):
                        header_upper = header.upper()
                        if 'SYMBOL' in header_upper:
                            col_map['symbol'] = j
                        elif 'EXPIRATION' in header_upper:
                            col_map['expiration'] = j
                        elif 'SETTLEMENT' in header_upper:
                            col_map['settlement'] = j
                        elif 'LAST' in header_upper:
                            col_map['last'] = j
                        elif 'VOLUME' in header_upper:
                            col_map['volume'] = j
                    
                    # Default mappings if not found
                    if 'symbol' not in col_map:
                        col_map['symbol'] = 0  # Assume first column is symbol
                    if 'settlement' not in col_map and 'last' not in col_map:
                        # Try to find price columns by position
                        if len(headers) >= 7:
                            col_map['settlement'] = 6  # Often 7th column
                        if len(headers) >= 3:
                            col_map['last'] = 2  # Often 3rd column
                    
                    # Process all rows
                    data_rows = table.find_all('tr')[1:]  # Skip header
                    logger.info(f"Processing {len(data_rows)} data rows")
                    
                    contracts_found = 0
                    
                    for row in data_rows:
                        cells = row.find_all(['td'])
                        if len(cells) < max(col_map.values()) + 1:
                            continue
                        
                        try:
                            symbol_cell = cells[col_map['symbol']]
                            symbol = symbol_cell.get_text().strip()
                            
                            # Try to get settlement price
                            settlement_price = None
                            if 'settlement' in col_map:
                                settlement_text = cells[col_map['settlement']].get_text().strip()
                                if settlement_text and settlement_text != '-':
                                    try:
                                        settlement_price = float(settlement_text.replace(',', ''))
                                    except ValueError:
                                        pass
                            
                            # If no settlement price, try last price
                            if (settlement_price is None or settlement_price == 0) and 'last' in col_map:
                                last_text = cells[col_map['last']].get_text().strip()
                                if last_text and last_text != '-' and last_text != '0':
                                    try:
                                        settlement_price = float(last_text.replace(',', ''))
                                    except ValueError:
                                        pass
                            
                            if not symbol or settlement_price is None or settlement_price == 0:
                                continue
                                
                            # Special case for VIX index
                            if symbol.upper() == 'VIX':
                                futures_data['CBOE:VIX'] = settlement_price
                                logger.info(f"Extracted VIX index: {settlement_price}")
                                contracts_found += 1
                                continue
                            
                            # Process VIX futures symbols
                            vx_patterns = [
                                re.compile(r'VX(\d+)?\/([A-Z])(\d+)'),  # VX/H5, VX09/H5
                                re.compile(r'VX([A-Z])(\d+)'),          # VXH5, VXH25
                                re.compile(r'VIX\s+([A-Z]{3})[^0-9]*(\d{2})') # VIX MAR 25
                            ]
                            
                            for pattern in vx_patterns:
                                match = pattern.match(symbol)
                                if match:
                                    # Extract month code and year
                                    if len(match.groups()) == 3 and match.group(2):  # VX/H5 format
                                        month_code = match.group(2)
                                        year_digit = match.group(3)[-1]  # Last digit of year
                                    elif len(match.groups()) == 2 and match.group(1):
                                        if len(match.group(1)) == 1:  # VXH5 format
                                            month_code = match.group(1)
                                            year_digit = match.group(2)[-1]
                                        else:  # VIX MAR 25 format
                                            month_str = match.group(1).upper()
                                            year_digit = match.group(2)[-1]
                                            
                                            # Convert month name to code
                                            month_map = {
                                                'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                                'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                                'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                            }
                                            month_code = month_map.get(month_str, '?')
                                    else:
                                        continue
                                    
                                    # Format standard tickers
                                    cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                                    std_ticker = f"/VX{month_code}{year_digit}"
                                    
                                    futures_data[cboe_ticker] = settlement_price
                                    futures_data[std_ticker] = settlement_price
                                    
                                    logger.info(f"Extracted future: {cboe_ticker} = {settlement_price}")
                                    contracts_found += 1
                                    break
                            
                        except Exception as e:
                            logger.warning(f"Error processing row: {str(e)}")
                    
                    if contracts_found > 0:
                        logger.info(f"Found {contracts_found} contracts in table {i+1}")
                        break  # Stop after finding a valid table
        
        # Check if we found any futures data
        if len(futures_data) > 2:  # More than just date and timestamp
            logger.info(f"Successfully extracted {len(futures_data)-2} VIX futures from CBOE (processing took {time.time() - start_time:.2f}s)")
            return futures_data
        else:
            # Try direct extraction from page using driver
            logger.info("Attempting direct extraction from browser elements")
            try:
                # Find the VIX futures table
                tables = browser.find_elements(By.TAG_NAME, "table")
                if tables:
                    # Get rows directly
                    rows = tables[0].find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 1:  # Skip header
                        contracts_found = 0
                        
                        for row in rows[1:]:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) < 7:  # Need minimum cells
                                continue
                                
                            try:
                                symbol = cells[0].text.strip()
                                
                                # Try settlement price first
                                settlement_text = cells[6].text.strip() if len(cells) > 6 else ""
                                settlement_price = None
                                
                                if settlement_text and settlement_text != '-':
                                    try:
                                        settlement_price = float(settlement_text.replace(',', ''))
                                    except ValueError:
                                        pass
                                
                                # If no settlement price, try last price
                                if settlement_price is None or settlement_price == 0:
                                    last_text = cells[2].text.strip() if len(cells) > 2 else ""
                                    if last_text and last_text != '-' and last_text != '0':
                                        try:
                                            settlement_price = float(last_text.replace(',', ''))
                                        except ValueError:
                                            pass
                                
                                if not symbol or settlement_price is None or settlement_price == 0:
                                    continue
                                
                                # Process symbol (similar to above)
                                if symbol.upper() == 'VIX':
                                    futures_data['CBOE:VIX'] = settlement_price
                                    logger.info(f"Extracted VIX index: {settlement_price}")
                                    contracts_found += 1
                                    continue
                                    
                                # Use same regex patterns as above
                                vx_patterns = [
                                    re.compile(r'VX(\d+)?\/([A-Z])(\d+)'),
                                    re.compile(r'VX([A-Z])(\d+)'),
                                    re.compile(r'VIX\s+([A-Z]{3})[^0-9]*(\d{2})')
                                ]
                                
                                for pattern in vx_patterns:
                                    match = pattern.match(symbol)
                                    if match:
                                        # Extract month and year (similar to above)
                                        if len(match.groups()) == 3 and match.group(2):
                                            month_code = match.group(2)
                                            year_digit = match.group(3)[-1]
                                        elif len(match.groups()) == 2 and match.group(1):
                                            if len(match.group(1)) == 1:
                                                month_code = match.group(1)
                                                year_digit = match.group(2)[-1]
                                            else:
                                                month_str = match.group(1).upper()
                                                year_digit = match.group(2)[-1]
                                                
                                                month_map = {
                                                    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
                                                    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
                                                    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
                                                }
                                                month_code = month_map.get(month_str, '?')
                                        else:
                                            continue
                                        
                                        cboe_ticker = f"CBOE:VX{month_code}{year_digit}"
                                        std_ticker = f"/VX{month_code}{year_digit}"
                                        
                                        futures_data[cboe_ticker] = settlement_price
                                        futures_data[std_ticker] = settlement_price
                                        
                                        logger.info(f"Extracted future (direct): {cboe_ticker} = {settlement_price}")
                                        contracts_found += 1
                                        break
                                
                            except Exception as e:
                                logger.warning(f"Error in direct extraction: {str(e)}")
                        
                        if contracts_found > 0:
                            logger.info(f"Successfully extracted {contracts_found} contracts via direct browser access")
                            return futures_data
            except Exception as e:
                logger.warning(f"Direct extraction failed: {str(e)}")
            
            logger.warning("Could not extract VIX futures data from CBOE website")
            return None
    
    except Exception as e:
        logger.error(f"Error downloading from CBOE: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    
    finally:
        # Always close the browser
        if browser:
            try:
                browser.quit()
                logger.info("Browser session closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {str(e)}")
