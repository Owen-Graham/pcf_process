import os
import logging
import re
from datetime import datetime
import pandas as pd
import traceback

# Custom Exceptions
class MissingCriticalDataError(Exception):
    """Custom exception for missing critical data."""
    pass

class InvalidDataError(Exception):
    """Custom exception for invalid or malformed data."""
    pass

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
        raise InvalidDataError(f"Invalid ticker: {ticker}. Ticker must be a non-empty string.")

    # Try to match VX<letter><4-digit-year>, e.g., VXF2025
    match_4_digit = re.match(r'^(VX[A-Z])(\d{4})$', ticker, re.IGNORECASE)
    if match_4_digit:
        prefix = match_4_digit.group(1).upper()
        year = match_4_digit.group(2)
        return f"{prefix}{year[-1]}"

    # Try to match VX<letter><2-digit-year>, e.g., VXH25
    match_2_digit = re.match(r'^(VX[A-Z])(\d{2})$', ticker, re.IGNORECASE)
    if match_2_digit:
        prefix = match_2_digit.group(1).upper()
        year = match_2_digit.group(2)
        return f"{prefix}{year[-1]}"

    # Try to match VX<letter><1-digit-year>, e.g., VXH5 (already normalized)
    match_1_digit = re.match(r'^(VX[A-Z])(\d{1})$', ticker, re.IGNORECASE)
    if match_1_digit:
        return ticker.upper() # Already in the desired format, ensure uppercase

    # If none of the above, it might be an invalid format or a different kind of ticker
    # For this function's specific purpose, if it doesn't match VX + Letter + Year(s),
    # it's not a standard VIX future code it can normalize in this way.
    # Depending on strictness, either raise error or return original.
    # Given the usage, returning original for non-matching is safer if other tickers pass through.
    # However, the function name implies it's for VIX tickers. Let's be strict.
    raise InvalidDataError(f"Ticker {ticker} is not a recognized VIX futures format for normalization (e.g., VXM5, VXM25, VXM2025).")

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
        if key in ['date', 'timestamp']:
            continue
        if value is None:
            raise MissingCriticalDataError(f"Missing price value for key '{key}' in source '{source}'.")
        
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
    
    if not latest_file:
        raise MissingCriticalDataError(f"No file found for pattern '{pattern}' in directory '{directory}'.")
    
    try:
        df = pd.read_csv(latest_file)
        if df.empty:
            raise MissingCriticalDataError(f"File {latest_file} is empty.")
        return df
    except Exception as e:
        # Log the original error for debugging purposes if desired, then raise the custom error
        logging.error(f"Original error reading file {latest_file}: {str(e)}")
        logging.error(traceback.format_exc())
        raise MissingCriticalDataError(f"Error reading file {latest_file}: {str(e)}")

def map_position_to_contract(position):
    """
    Map a VIX futures position (1st, 2nd, 3rd month) to the actual contract code.
    
    Args:
        position (int): Position (1=front month, 2=second month, etc.)
        
    Returns:
        str: VIX futures contract code (e.g., VXH5, VXJ5)
    """
    if position < 1:
        raise ValueError(f"Invalid position: {position} (must be >= 1)")
    
    # Get the next N VIX futures contracts
    next_contracts = get_next_vix_contracts(position)
    
    # Return the contract at the specified position (1-indexed)
    if position <= len(next_contracts):
        return next_contracts[position-1]
    else:
        # If position is beyond what we calculated, calculate more
        next_contracts = get_next_vix_contracts(position)
        return next_contracts[position-1]

def map_contract_to_position(contract_code):
    """
    Map a VIX futures contract code to its position (1st, 2nd, 3rd month).
    
    Args:
        contract_code (str): VIX futures contract code (e.g., VXH5, VXJ5)
        
    Returns:
        int: Position (1=front month, 2=second month, etc.) or None if not found
    """
    # Normalize the contract code
    normalized_code = normalize_vix_ticker(contract_code)
    
    # Get the next several contracts to search
    next_contracts = get_next_vix_contracts(6)  # Look ahead 6 months
    
    # Find the position
    if normalized_code in next_contracts:
        return next_contracts.index(normalized_code) + 1  # 1-indexed
    
    # Not found in the next 6 months
    return None

def get_tradingview_vix_contract_code(normalized_contract_code, current_dt=None):
    """
    Convert a normalized VIX contract code (e.g., "VXM5") to the TradingView
    specific contract code (e.g., "VXM2025").

    Args:
        normalized_contract_code (str): Normalized VIX code like "VXM5", "VXH6".
        current_dt (datetime, optional): The current datetime to use as a reference.
                                         Defaults to datetime.now(). Used for testing.

    Returns:
        str: TradingView specific contract code (e.g., "VXM2025").
    """
    if not isinstance(normalized_contract_code, str) or not re.match(r"^VX[A-Z]\d$", normalized_contract_code):
        raise ValueError(
            f"Invalid normalized_contract_code: '{normalized_contract_code}'. Expected format like 'VXM5'."
        )

    if current_dt is None:
        current_dt = datetime.now()

    month_letter = normalized_contract_code[2]
    year_digit = int(normalized_contract_code[3])

    current_year = current_dt.year
    current_century = (current_year // 100) * 100  # e.g., 2000 for 2024
    current_decade_within_century = (current_year // 10) % 10 # e.g. 2 for 2024, 0 for 2001

    # Determine the contract's full year
    # Start by assuming the year_digit is in the current decade
    contract_year = (current_year // 10) * 10 + year_digit

    # If this calculated contract_year is far in the past (e.g. current 2024, year_digit 0 -> 2020, but should be 2030)
    # or if the contract_year is in the past relative to current_year (e.g. current 2025, year_digit 4 -> 2024, should be 2034)
    # then it's likely in the next decade.
    # A simple rule: if contract_year % 10 < current_year % 10 and contract_year < current_year, it's next decade.
    # Or if the contract month is earlier than current month in the same year_digit year.
    # More robust: If contract_year < current_year, and year_digit implies a year earlier in the decade than current_year's last digit,
    # it must be the next decade.
    # Example: current_year = 2024 (ends in 4), contract_year_digit = 3. contract_year becomes 2023. This should be 2033.
    # Example: current_year = 2024 (ends in 4), contract_year_digit = 5. contract_year becomes 2025. This is correct.
    # Example: current_year = 2029 (ends in 9), contract_year_digit = 0. contract_year becomes 2020. This should be 2030.

    # If the calculated year (e.g., 2020 for '0' when current is 2029)
    # is less than the current year, and the single year digit indicates this,
    # then it must be for the next decade.
    if contract_year < current_year and year_digit < (current_year % 10):
         contract_year += 10
    # Also, if contract_year is same as current_year, but month is past, it implies next year of that month.
    # However, VIX futures roll, so a VXM5 in Jan 2025 is still VXM2025.
    # The critical part is getting the decade right.
    # If contract_year was formed (e.g. 2020 from '0' when current is 2019) and current is 2019 -> contract_year should be 2020 (correct)
    # If contract_year was formed (e.g. 2029 from '9' when current is 2020) and current is 2020 -> contract_year should be 2029 (correct)

    # If contract_year ends up being more than 10 years in the past, it's likely previous century's end.
    # This logic primarily handles the decade rollover correctly.
    # e.g. current 2023, code VXM2 -> 2022. This implies next decade for VXM2 -> 2032.
    # Let's test this: current_year = 2023, year_digit = 2. contract_year initially 2022.
    # 2022 < 2023 and 2 < 3. So contract_year becomes 2032. Correct.

    # What if current_year = 2023, year_digit = 3. contract_year initially 2023.
    # 2023 is not < 2023. So it remains 2023. Correct.

    # What if current_year = 2023, year_digit = 4. contract_year initially 2024.
    # 2024 is not < 2023. So it remains 2024. Correct.

    # What if current_year = 2029, year_digit = 0. contract_year initially 2020.
    # 2020 < 2029 and 0 < 9. So contract_year becomes 2030. Correct.

    # What if current_year = 2029, year_digit = 8. contract_year initially 2028.
    # 2028 < 2029 and 8 < 9. So contract_year becomes 2038. Correct.

    # This seems to handle the year calculation correctly by ensuring the contract year is not in the past
    # unless the year digit explicitly means the earlier part of the current decade.
    # A simpler approach for contract year:
    # Start with current_year's decade: (current_year // 10) * 10
    # Add year_digit: potential_year = (current_year // 10) * 10 + year_digit
    # If potential_year < current_year, then this contract must be in the next decade.
    #   contract_full_year = potential_year + 10
    # Else
    #   contract_full_year = potential_year
    # This might be too simple. E.g. current_year = 2025, contract_code = VXM4 (April 2024).
    # potential_year = 2020 + 4 = 2024. 2024 < 2025. So contract_full_year = 2034. Incorrect.

    # Let's refine:
    # The year for the contract is derived from current_year's first three digits + contract's year_digit
    base_year_prefix = current_year // 10  # e.g., 202 for 2024
    contract_full_year = base_year_prefix * 10 + year_digit # e.g., 2020 + 5 = 2025 for VXM5 in 2024

    # If this makes the contract_full_year < current_year, it means the contract is for the next decade.
    # E.g. current_year = 2024, code is 'VXM3' (March 2023).
    # contract_full_year = 2020 + 3 = 2023.
    # Since 2023 < 2024, it must be that this '3' refers to 2033.
    if contract_full_year < current_year:
        # Check if it's a genuinely past contract within the decade or if it should roll to next decade
        # If year_digit is less than current_year's last digit, it implies next decade.
        if year_digit < (current_year % 10):
             contract_full_year += 10
        # Else, it's a past contract in the current decade (e.g. VXM3 in Dec 2023 is still VXM2023)
        # This case should be handled by context (is it a historical lookup or future?)
        # For TradingView codes, they always mean a specific future or current contract.
        # So if 'VXM3' is requested in 2024, it means VXM2033.
        # If 'VXM3' is requested in 2023 (before March), it's VXM2023.
        # If 'VXM3' is requested in 2023 (after March), it means VXM2033 for a *new* contract.
        # The standard is that the year digit implies the nearest future year ending in that digit.

    # Simplified logic based on typical future contract naming:
    # The year for the contract is current_year's decade + contract's year_digit.
    # If this year is < current_year, then it's 10 years later.
    # This ensures we always get a future or current year.

    calculated_year = (current_year // 10) * 10 + year_digit
    if calculated_year < current_year:
        # This implies the contract is for the next decade if we are looking for future contracts
        # e.g. current 2024, contract VXM3 -> (202*10)+3 = 2023. 2023 < 2024, so VXM2033.
        # e.g. current 2024, contract VXM5 -> (202*10)+5 = 2025. 2025 > 2024, so VXM2025.
        # e.g. current 2024, contract VXM4 -> (202*10)+4 = 2024. 2024 == 2024, so VXM2024.
        # This seems more robust for "what is the upcoming contract ending in year_digit"
        contract_full_year = calculated_year + 10
    else:
        contract_full_year = calculated_year

    # One final check: if current_year is 2025, VXM5 should be VXM2025, not VXM2035.
    # If calculated_year == current_year, it should be contract_full_year = calculated_year.
    # If current_year = 2025, year_digit = 5. calculated_year = 2020+5 = 2025. contract_full_year = 2025. (Correct)
    # If current_year = 2025, year_digit = 4. calculated_year = 2020+4 = 2024. 2024 < 2025 -> contract_full_year = 2034. (Correct for future)
    # If current_year = 2025, year_digit = 6. calculated_year = 2020+6 = 2026. 2026 > 2025 -> contract_full_year = 2026. (Correct)

    # The MONTH_CODES are 1-indexed for month numbers
    contract_month_number = MONTH_CODES.get(month_letter)
    if not contract_month_number:
        raise ValueError(f"Invalid month letter '{month_letter}' in contract code.")

    # If the calculated_year is the same as current_year,
    # and the contract_month is earlier than the current_month,
    # then this contract is for the next decade.
    # e.g. current is Nov 2024 (month 11), contract is VXM4 (April, month 6).
    # calculated_year = 2024. contract_month = 4. current_month = 11.
    # 4 < 11, so VXM4 should be VXM2034 if we are in Nov 2024.
    # This is only if contract_full_year was not already pushed to next decade.
    if contract_full_year == current_year and contract_month_number < current_dt.month:
        contract_full_year += 10
    # e.g. current Nov 2024 (month 11), contract VXX4 (Nov, month 11) -> VXX2024
    # e.g. current Nov 2024 (month 11), contract VXZ4 (Dec, month 12) -> VXZ2024

    return f"VX{month_letter}{contract_full_year}"

# Unit tests for get_tradingview_vix_contract_code will be added to test_common.py
