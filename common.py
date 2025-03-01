import os
import logging
import re
from datetime import datetime
import pandas as pd
import traceback

# Define global constants
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)

# VIX futures month codes mapping
MONTH_CODES = {
    'F': 1,   # January
    'G': 2,   # February
    'H': 3,   # March
    'J': 4,   # April
    'K': 5,   # May
    'M': 6,   # June
    'N': 7,   # July
    'Q': 8,   # August
    'U': 9,   # September
    'V': 10,  # October
    'X': 11,  # November
    'Z': 12   # December
}

def setup_logging(name):
    """Set up logging for a script."""
    logger = logging.getLogger(name)
    
    # Check if logger already has handlers to avoid adding duplicate handlers
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create file handler
        file_handler = logging.FileHandler(os.path.join(SAVE_DIR, f"{name}.log"))
        file_handler.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Add formatter to handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

def normalize_vix_ticker(ticker):
    """
    Normalize VIX futures ticker to standard format (VXH5 instead of VXH25)
    
    Args:
        ticker: VIX futures ticker (e.g., VXH25, VXH5)
    
    Returns:
        Normalized ticker (e.g., VXH5)
    """
    if not ticker or not isinstance(ticker, str):
        return ticker
        
    # If ticker is in format VX<letter><2-digit-year>
    match = re.match(r'(VX[A-Z])(\d{2})$', ticker)
    if match:
        prefix = match.group(1)
        year = match.group(2)
        # Take only the last digit of the year
        return f"{prefix}{year[-1]}"
    return ticker

def get_yfinance_ticker_for_vix_future(contract_code):
    """
    Convert VIX futures contract code (e.g., VXJ4) to yfinance ticker format.
    
    Handles different VIX futures ticker formats and dynamically calculates
    the correct year.
    
    Args:
        contract_code (str): VIX futures contract code (e.g., VXJ4, VXK4, VXJ24)
        
    Returns:
        str: yfinance ticker for the VIX futures contract
    """
    if not contract_code or not isinstance(contract_code, str):
        raise ValueError(f"Invalid contract code: {contract_code}")
    
    # Normalize the contract code first (in case it's in VXJ24 format)
    normalized_code = normalize_vix_ticker(contract_code)
    
    # Check if it's in the standard VXM5 format
    if not re.match(r'VX[A-Z]\d{1}$', normalized_code):
        raise ValueError(f"Invalid contract code format: {contract_code} -> {normalized_code}")
    
    # Extract month code and year digit
    month_code = normalized_code[2]
    year_digit = normalized_code[3]
    
    # Month code mapping (futures standard)
    month_to_number = {
        'F': '01',  # January
        'G': '02',  # February
        'H': '03',  # March
        'J': '04',  # April
        'K': '05',  # May
        'M': '06',  # June
        'N': '07',  # July
        'Q': '08',  # August
        'U': '09',  # September
        'V': '10',  # October
        'X': '11',  # November
        'Z': '12',  # December
    }
    
    if month_code not in month_to_number:
        raise ValueError(f"Invalid month code in contract: {contract_code}")
    
    # Get current date information
    current_year = datetime.now().year
    current_decade = current_year // 10 * 10  # Gets e.g. 2020 for 2023
    
    # Calculate the full year
    full_year = current_decade + int(year_digit)
    
    # If the calculated year is in the past, assume next decade
    if full_year < current_year:
        full_year += 10
    
    # Yahoo Finance format for VIX futures
    yahoo_ticker = f"^VIX{month_to_number[month_code]}.{full_year}"
    
    return yahoo_ticker

def get_alternative_yfinance_tickers(contract_code):
    """
    Generate alternative Yahoo Finance ticker formats for VIX futures.
    
    Sometimes Yahoo Finance changes ticker formats or has multiple formats
    for the same contract. This function returns a list of possible formats
    to try when the primary format fails.
    
    Args:
        contract_code (str): VIX futures contract code (e.g., VXJ4, VXK4)
        
    Returns:
        list: List of alternative Yahoo Finance ticker formats
    """
    if not contract_code or not isinstance(contract_code, str):
        return []
    
    try:
        # Normalize the contract code first
        normalized_code = normalize_vix_ticker(contract_code)
        
        # Extract month code and year digit
        if len(normalized_code) < 4:
            return []
            
        month_code = normalized_code[2]
        year_digit = normalized_code[3]
        
        # Month code mapping
        month_to_number = {
            'F': '01',  # January
            'G': '02',  # February
            'H': '03',  # March
            'J': '04',  # April
            'K': '05',  # May
            'M': '06',  # June
            'N': '07',  # July
            'Q': '08',  # August
            'U': '09',  # September
            'V': '10',  # October
            'X': '11',  # November
            'Z': '12',  # December
        }
        
        if month_code not in month_to_number:
            return []
        
        # Get current date information
        current_year = datetime.now().year
        current_decade = current_year // 10 * 10
        
        # Calculate the full year
        full_year = current_decade + int(year_digit)
        
        # If the calculated year is in the past, assume next decade
        if full_year < current_year:
            full_year += 10
        
        # Generate alternative ticker formats
        alternatives = []
        
        # Format 1: ^VIX<month_num>.<year>
        alternatives.append(f"^VIX{month_to_number[month_code]}.{full_year}")
        
        # Format 2: ^VIX<month_num><year_digit> (used by some data sources)
        alternatives.append(f"^VIX{month_to_number[month_code]}{year_digit}")
        
        # Format 3: VIX<month_num>.<year> (without the ^ prefix)
        alternatives.append(f"VIX{month_to_number[month_code]}.{full_year}")
        
        # Format 4: Current month VIX future (VX=F)
        # Only add this if we're looking for the front month contract
        current_month = datetime.now().month
        next_month_idx = (current_month % 12) + 1
        if month_to_number[month_code] == f"{next_month_idx:02d}":
            alternatives.append("VX=F")
        
        # Format 5: Use position-based tickers for near months
        # Check if this is one of the next 3 contracts
        next_contracts = get_next_vix_contracts(3)
        if normalized_code in next_contracts:
            position = next_contracts.index(normalized_code) + 1
            alternatives.append(f"^VFTW{position}")
            alternatives.append(f"^VXIND{position}")
        
        return alternatives
        
    except Exception:
        return []

def get_next_vix_contracts(num_contracts=3):
    """
    Get the next N VIX futures contracts based on the current date.
    
    Args:
        num_contracts (int): Number of future contracts to return
        
    Returns:
        list: List of VIX futures contract codes (e.g., ['VXH5', 'VXJ5', 'VXK5'])
    """
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year % 10  # Last digit of year
    
    # Month codes used in VIX futures (reverse of MONTH_CODES)
    month_codes = {v: k for k, v in MONTH_CODES.items()}
    
    contracts = []
    
    # Get the current month and the next N-1 months
    for i in range(num_contracts):
        # Calculate month and year (handling year wrap)
        month_idx = (current_month + i) % 12
        if month_idx == 0:  # Handle December (0 after modulo)
            month_idx = 12
            
        # Calculate year digit (handle year rollover)
        year_digit = current_year
        if month_idx < current_month:
            year_digit = (current_year + 1) % 10
        
        # Get the month code
        month_code = month_codes.get(month_idx)
        
        # Create contract code
        contract = f"VX{month_code}{year_digit}"
        contracts.append(contract)
    
    return contracts

def format_vix_data(futures_data, source):
    """
    Format VIX futures data into standardized records.
    
    Args:
        futures_data (dict): Raw futures data dictionary
        source (str): Source of the data (e.g., 'CBOE', 'Yahoo')
        
    Returns:
        list: List of standardized records
    """
    records = []
    timestamp = futures_data.get('timestamp', datetime.now().strftime("%Y%m%d%H%M"))
    price_date = futures_data.get('date', datetime.now().strftime("%Y-%m-%d"))
    
    for key, value in futures_data.items():
        # Skip non-price fields
        if key in ['date', 'timestamp'] or value is None:
            continue
        
        # Determine VIX future code and symbol
        vix_future = None
        symbol = key
        
        if key.startswith(f"{source}:VX"):
            vix_future = key.split(':')[1]
        elif key.startswith('/VX'):
            vix_future = 'VX' + key[3:]
        elif key.upper() in ['VX=F', 'VIX']:
            vix_future = 'VIX'
        else:
            continue
        
        # Create standardized record
        record = {
            'timestamp': timestamp,
            'price_date': price_date,
            'vix_future': vix_future,
            'source': source,
            'symbol': symbol,
            'price': float(value)
        }
        
        records.append(record)
    
    return records

def find_latest_file(pattern, directory=SAVE_DIR):
    """
    Find the latest file matching a pattern in a directory.
    
    Args:
        pattern (str): File pattern to match (e.g., "vix_futures_*.csv")
        directory (str): Directory to search in
        
    Returns:
        str: Path to the latest file or None if no files found
    """
    try:
        import glob
        
        # Get full pattern path
        full_pattern = os.path.join(directory, pattern)
        
        # Use glob to find all matching files
        matching_files = glob.glob(full_pattern)
        
        if not matching_files:
            return None
        
        # Sort by modification time (newest first)
        matching_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Return the latest file
        return matching_files[0]
    
    except Exception as e:
        logging.error(f"Error finding files with pattern {pattern}: {str(e)}")
        logging.error(traceback.format_exc())
        return None

def read_latest_file(pattern, default_cols=None, directory=SAVE_DIR):
    """
    Read the latest file matching the pattern into a pandas DataFrame.
    
    Args:
        pattern (str): File pattern to match (e.g., "vix_futures_*.csv")
        default_cols (list): Default column list if file doesn't exist
        directory (str): Directory to search in
        
    Returns:
        pandas.DataFrame: DataFrame with the file contents or empty DataFrame
    """
    latest_file = find_latest_file(pattern, directory)
    
    if latest_file:
        try:
            df = pd.read_csv(latest_file)
            return df
        except Exception as e:
            logging.error(f"Error reading file {latest_file}: {str(e)}")
            logging.error(traceback.format_exc())
    
    return pd.DataFrame(columns=default_cols if default_cols else [])
