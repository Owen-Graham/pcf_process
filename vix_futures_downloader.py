import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Define local storage directory
SAVE_DIR = "data"
os.makedirs(SAVE_DIR, exist_ok=True)

def download_vix_futures():
    # VIX futures tickers - first 4 expirations
    # Format for VIX futures in yfinance: 'VX=F' for front month, and '/VX' + month code + year for others
    # Month codes: F(Jan), G(Feb), H(Mar), J(Apr), K(May), M(Jun), N(Jul), Q(Aug), U(Sep), V(Oct), X(Nov), Z(Dec)
    
    # Get current date
    current_date = datetime.now()
    
    # CBOE VIX futures typically have monthly expirations
    # We'll need to determine the next 4 expiration months dynamically
    
    # Get current month and year
    current_month = current_date.month
    current_year = current_date.year
    
    # Month codes mapping
    month_codes = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }
    
    # Generate the next 4 month codes
    futures_tickers = []
    month = current_month
    year = current_year
    
    for i in range(4):
        # If we've gone past December, increment year and reset month to January
        if month > 12:
            month = 1
            year += 1
        
        # Get the month code
        month_code = month_codes[month]
        
        # Format the ticker
        if i == 0:
            ticker = "VX=F"  # Front month
        else:
            # Format: /VXM4 for June 2024
            ticker = f"/VX{month_code}{str(year)[-1]}"
        
        futures_tickers.append(ticker)
        month += 1
    
    print(f"Downloading data for VIX futures: {futures_tickers}")
    
    # Download settlement prices for each future
    all_data = {}
    
    for ticker in futures_tickers:
        try:
            # Get today's data
            data = yf.download(ticker, period="1d", progress=False)
            
            if not data.empty:
                # We want the settlement price (Close)
                settlement_price = data['Close'].iloc[-1]
                all_data[ticker] = settlement_price
                print(f"✅ Downloaded {ticker}: Settlement Price = {settlement_price}")
            else:
                print(f"❌ No data available for {ticker}")
                all_data[ticker] = None
        except Exception as e:
            print(f"❌ Error downloading {ticker}: {str(e)}")
            all_data[ticker] = None
    
    # Create DataFrame
    df = pd.DataFrame([all_data])
    
    # Add date columns
    df['date'] = current_date.strftime("%Y-%m-%d")
    df['timestamp'] = current_date.strftime("%Y%m%d%H%M")
    
    # Rearrange columns to have date first
    cols = df.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    df = df[cols]
    
    # Save to CSV
    current_datetime = current_date.strftime("%Y%m%d%H%M")
    csv_filename = f"vix_futures_{current_datetime}.csv"
    csv_path = os.path.join(SAVE_DIR, csv_filename)
    
    # If we already have a CSV, append to it, otherwise create new
    master_csv_path = os.path.join(SAVE_DIR, "vix_futures_master.csv")
    
    if os.path.exists(master_csv_path):
        master_df = pd.read_csv(master_csv_path)
        combined_df = pd.concat([master_df, df], ignore_index=True)
        combined_df.to_csv(master_csv_path, index=False)
        print(f"✅ Updated master CSV file: {master_csv_path}")
    else:
        df.to_csv(master_csv_path, index=False)
        print(f"✅ Created master CSV file: {master_csv_path}")
    
    # Also save daily snapshot
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved daily snapshot to: {csv_path}")

if __name__ == "__main__":
    download_vix_futures()
