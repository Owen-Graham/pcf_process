import os
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import traceback
import sys
from datetime import datetime, timedelta
import json

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)

def download_vix_futures_from_cboe():
    """Download VIX futures data directly from CBOE website"""
    try:
        print("Downloading VIX futures data from CBOE...")
        
        # CBOE VIX futures page
        url = "https://www.cboe.com/delayed_quotes/vx/quote_table"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the futures table
        table = soup.find('table', class_='table-quotes')
        
        if not table:
            print("❌ Could not find VIX futures table on CBOE website")
            return None
        
        futures_data = {}
        # Get current date
        futures_data['date'] = datetime.now().strftime("%Y-%m-%d")
        futures_data['timestamp'] = datetime.now().strftime("%Y%m%d%H%M")
        
        # Get rows from table
        rows = table.find_all('tr')
        
        for row in rows[1:5]:  # Get first 4 contracts (skip header row)
            cells = row.find_all('td')
            if len(cells) >= 7:
                # Extract contract name and settlement price
                contract_name = cells[0].text.strip()
                settlement_price = cells[3].text.strip().replace('$', '').replace(',', '')
                
                # Format to match yfinance tickers
                if "VX" in contract_name and "/" in contract_name:
                    ticker = contract_name.replace(" ", "")
                    futures_data[ticker] = float(settlement_price)
                    print(f"✅ CBOE: {ticker} = {settlement_price}")
        
        return futures_data
    
    except Exception as e:
        print(f"❌ Error downloading from CBOE: {str(e)}")
        print(traceback.format_exc())
        return None

def download_vix_futures_from_yfinance():
    """Download VIX futures data from Yahoo Finance"""
    try:
        print("Downloading VIX futures data from Yahoo Finance...")
        
        # Get current date
        current_date = datetime.now()
        
        futures_data = {
            'date': current_date.strftime("%Y-%m-%d"),
            'timestamp': current_date.strftime("%Y%m%d%H%M")
        }
        
        # Get the VIX index price as a fallback for the front month
        vix_data = yf.download("^VIX", period="1d", progress=False)
        if not vix_data.empty:
            futures_data['VX=F'] = vix_data['Close'].iloc[-1]
            print(f"✅ Yahoo: VX=F = {futures_data['VX=F']}")
        else:
            futures_data['VX=F'] = None
        
        # Fixed tickers for upcoming VIX futures contracts
        futures_tickers = ['/VXH5', '/VXJ5', '/VXK5']  # H=Mar, J=Apr, K=May 2025
        
        for ticker in futures_tickers:
            try:
                data = yf.download(ticker, period="1d", progress=False)
                
                if not data.empty and 'Close' in data.columns and len(data['Close']) > 0:
                    settlement_price = data['Close'].iloc[-1]
                    futures_data[ticker] = settlement_price
                    print(f"✅ Yahoo: {ticker} = {settlement_price}")
                else:
                    futures_data[ticker] = None
            except Exception as e:
                print(f"❌ Error downloading {ticker} from Yahoo: {str(e)}")
                futures_data[ticker] = None
        
        return futures_data
    
    except Exception as e:
        print(f"❌ Error in Yahoo Finance download: {str(e)}")
        print(traceback.format_exc())
        return None

def download_vix_futures():
    """Download VIX futures data from both sources and combine them"""
    
    # Download from both sources
    cboe_data = download_vix_futures_from_cboe()
    yfinance_data = download_vix_futures_from_yfinance()
    
    # Create combined data structure
    if cboe_data or yfinance_data:
        # Use first available source for date/timestamp
        result = {}
        if cboe_data:
            result['date'] = cboe_data['date']
            result['timestamp'] = cboe_data['timestamp']
        else:
            result['date'] = yfinance_data['date']
            result['timestamp'] = yfinance_data['timestamp']
        
        # Combine all contract data with source information
        all_contracts = set()
        if cboe_data:
            for key in cboe_data:
                if key not in ['date', 'timestamp'] and cboe_data[key] is not None:
                    all_contracts.add(key)
                    result[f"{key}_cboe"] = cboe_data[key]
        
        if yfinance_data:
            for key in yfinance_data:
                if key not in ['date', 'timestamp'] and yfinance_data[key] is not None:
                    all_contracts.add(key)
                    result[f"{key}_yahoo"] = yfinance_data[key]
        
        # Create preferred value columns (prefer CBOE over Yahoo)
        for contract in all_contracts:
            if f"{contract}_cboe" in result:
                result[contract] = result[f"{contract}_cboe"]
            elif f"{contract}_yahoo" in result:
                result[contract] = result[f"{contract}_yahoo"]
        
        # Create DataFrame
        df = pd.DataFrame([result])
        
        # Save daily snapshot
        current_datetime = datetime.now().strftime("%Y%m%d%H%M")
        csv_filename = f"vix_futures_{current_datetime}.csv"
        csv_path = os.path.join(SAVE_DIR, csv_filename)
        
        # Check if we have any actual price data
        price_columns = [col for col in df.columns if col not in ['date', 'timestamp'] and not (col.endswith('_cboe') or col.endswith('_yahoo'))]
        if df[price_columns].notna().any(axis=1).iloc[0]:
            # Update or create master CSV
            master_csv_path = os.path.join(SAVE_DIR, "vix_futures_master.csv")
            
            if os.path.exists(master_csv_path):
                master_df = pd.read_csv(master_csv_path)
                combined_df = pd.concat([master_df, df], ignore_index=True)
                combined_df.to_csv(master_csv_path, index=False)
                print(f"✅ Updated master CSV file: {master_csv_path}")
            else:
                df.to_csv(master_csv_path, index=False)
                print(f"✅ Created master CSV file: {master_csv_path}")
            
            # Save daily snapshot
            df.to_csv(csv_path, index=False)
            print(f"✅ Saved daily snapshot to: {csv_path}")
            
            # Print the data we collected
            print("\nData collected:")
            print(df[['date', 'timestamp'] + price_columns].to_string())
            return True
        else:
            print("❌ No valid price data collected. Not saving empty files.")
            return False
    else:
        print("❌ Failed to get data from both sources.")
        return False

if __name__ == "__main__":
    success = download_vix_futures()
    if not success:
        print("Script completed with errors.")
        sys.exit(1)  # Exit with error code
    else:
        print("Script completed successfully.")
