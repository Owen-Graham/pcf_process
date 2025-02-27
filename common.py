import os
import logging
import sys
from datetime import datetime

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

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
        if key.startswith(f"{source}:VX"):
            vix_future = key.split(':')[1]  # Extract VXH5 from SOURCE:VXH5
        elif key.startswith('/VX'):
            vix_future = 'VX' + key[3:]  # Convert /VXH5 to VXH5
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
