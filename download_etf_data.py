import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import traceback
from common import setup_logging, SAVE_DIR

# Set up logging
logger = setup_logging('etf_downloader')

def download_simplex_etf_data():
    """
    Download ETF data file for Simplex ETF 318A (CSV/PCF format)
    
    Returns:
        str: Path to downloaded file
    """
    try:
        logger.info("Downloading Simplex ETF 318A data")
        
        # URL of the Simplex ETF page
        url = "https://www.simplexasset.com/etf/eng/etf.html"
        
        # Get the HTML content while bypassing SSL verification
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.debug(f"Requesting URL: {url}")
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find download links for ETF 318A (either CSV or PCF)
        links = []
        for input_tag in soup.find_all("input", {"type": "image"}):
            onclick = input_tag.get("onclick", "")
            if "318A" in onclick:
                file_link = onclick.split("'")[1]
                links.append((file_link, "PCF" if ".pcf" in file_link.lower() else "CSV"))
        
        if not links:
            logger.warning("No ETF 318A links found")
            return None
            
        # Prioritize PCF format if available, otherwise use CSV
        for link, format_type in links:
            # Correct URL format
            if link.startswith(".."):
                file_url = f"https://www.simplexasset.com/etf/{link.lstrip('..')}"
            else:
                file_url = f"https://www.simplexasset.com/etf/{link}"
                
            logger.info(f"Downloading {format_type} from URL: {file_url}")
            file_response = requests.get(file_url, headers=headers, verify=False)
            file_response.raise_for_status()
            
            # Temporary save path
            temp_path = os.path.join(SAVE_DIR, f"temp_318A.csv")
            
            with open(temp_path, "wb") as file:
                file.write(file_response.content)
            
            # Try to read the file to extract Fund Date
            try:
                df = pd.read_csv(temp_path)
                
                # Extract Fund Date from column if available
                if 'Fund Date' in df.columns and len(df) > 0:
                    fund_date = str(df["Fund Date"].iloc[0]).replace("/", "")
                    if fund_date.lower() == "nan":
                        fund_date = "unknown"
                else:
                    fund_date = "unknown"
            except Exception as e:
                logger.warning(f"Could not extract Fund Date: {str(e)}")
                fund_date = "unknown"
            
            # Get current date-time
            current_datetime = datetime.now().strftime("%Y%m%d%H%M")
            
            # Define final file name
            final_filename = f"318A-{format_type}-{fund_date}-{current_datetime}.csv"
            final_path = os.path.join(SAVE_DIR, final_filename)
            
            # Rename file to final filename
            os.rename(temp_path, final_path)
            
            logger.info(f"ETF {format_type} file saved successfully to: {final_path}")
            return final_path
        
        logger.warning("Failed to download any ETF 318A data files")
        return None
    
    except Exception as e:
        logger.error(f"Error downloading ETF data: {str(e)}")
        logger.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    # Download the ETF data file
    etf_path = download_simplex_etf_data()
    
    if etf_path:
        print(f"✅ ETF data file saved successfully to: {etf_path}")
    else:
        print("❌ Failed to download ETF data file")
        exit(1)
