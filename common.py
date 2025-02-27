import os
import logging
import sys
from datetime import datetime
import re

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# VIX futures month codes
MONTH_CODES = {
    'F': 1,  # January
    'G': 2,  # February
    'H': 3,  # March
    'J': 4,  # April
    'K': 5,  # May
    'M': 6,  # June
    'N': 7,  # July
    'Q': 8,  # August
    'U': 9,  # September
    'V': 10, # October
    'X': 11, # November
    'Z': 12  # December
}

# Month names to codes
MONTH_NAMES_TO_CODES = {
    'JAN': 'F', 'FEB': 'G', 'MAR': 'H', 'APR': 'J', 
    'MAY': 'K', 'JUN': 'M', 'JUL': 'N', 'AUG': 'Q',
    'SEP': 'U', 'OCT': 'V', 'NOV': 'X', 'DEC': 'Z'
}

# Month numbers to codes
MONTH_NUMBERS_TO_CODES = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

# Set up logging
def setup_logging(name):
    """Configure logging with consistent format"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(SAVE_DIR, f"{name}.log"))
        ]
    )
    return logging.getLogger(name)

def format_vix_data(prices_data, source):
    """
    Formats VIX futures data into a standardized format
    
    Args:
        prices_data: Dictionary with futures prices
        source: Source name (CBOE, Yahoo, PCF)
    
    Returns:
        List of standardized price records
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    price_date = datetime.now().strftime("%Y-%m-%d")
    records = []
    
    for key, value in prices_data.items():
        # Skip non-price fields
        if key in ['date', 'timestamp']:
            continue
        
        # Skip null values
        if value is None:
            continue
        
        # Extract VIX future code based on key format
        if ':' in key:
            # Format: SOURCE:VXH5
            parts = key.split(':')
            if len(parts) == 2 and parts[1].startswith('VX'):
                vix_future = parts[1]  # Extract VXH5 from SOURCE:VXH5
            else:
                continue  # Skip unrecognized formats
        elif key.startswith('/VX'):
            vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
        elif key.startswith('VX'):
            vix_future = key  # Already in VXH5 format
        elif key == 'VX=F' or key == 'YAHOO:VIX':
            vix_future = 'VIX'  # Special case for VIX index
        else:
            continue  # Skip unrecognized formats
        
        records.append({
            'timestamp': timestamp,
            'price_date': price_date,
            'vix_future': vix_future,
            'source': source,
            'symbol': key,
            'price': float(value)
        })
    
    return records

def normalize_vix_ticker(ticker):
    """
    Normalize VIX futures ticker to standard format (VXM5 instead of VXM25)
    
    Args:
        ticker: VIX futures ticker (e.g., VXM25, VXM5)
    
    Returns:
        Normalized ticker (e.g., VXM5)
    """
    # If ticker is in format VX<letter><2-digit-year>
    match = re.match(r'(VX[A-Z])(\d{2})$', ticker)
    if match:
        prefix = match.group(1)
        year = match.group(2)
        # Take only the last digit of the year
        return f"{prefix}{year[-1]}"
    return ticker

def get_next_vix_contracts(num_contracts=3):
    """
    Get the next N VIX future contract codes based on current date
    
    Args:
        num_contracts: Number of future contracts to get
    
    Returns:
        List of VIX futures contract codes (e.g., ["VXK5", "VXM5", "VXN5"])
    """
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    year_suffix = str(current_year)[-1]
    
    contracts = []
    for i in range(1, num_contracts + 1):
        # Calculate contract month (current month + i)
        month_idx = (current_month + i - 1) % 12 + 1  # 1-indexed month
        
        # If we've wrapped around to next year
        if month_idx <= current_month:
            next_year_suffix = str(current_year + 1)[-1]
            contracts.append(f"VX{MONTH_NUMBERS_TO_CODES[month_idx]}{next_year_suffix}")
        else:
            contracts.append(f"VX{MONTH_NUMBERS_TO_CODES[month_idx]}{year_suffix}")
    
    return contracts

def log_response_details(response, source_name):
    """Log detailed information about HTTP responses"""
    logger = logging.getLogger(source_name)
    logger.debug(f"{source_name} response: status={response.status_code}, content-type={response.headers.get('content-type')}, length={len(response.content)}")
