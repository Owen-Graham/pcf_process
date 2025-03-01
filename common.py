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
