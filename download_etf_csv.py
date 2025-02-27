import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import traceback
from common import setup_logging, SAVE_DIR

# Set up logging
logger = setup_logging('etf_pcf_downloader')

def download_simplex_etf_pcf():
    """
    Download PCF file for Simplex ETF 318A
    
    Returns:
        str: Path to downloaded PCF file
    """
    try:
        logger.info("Downloading Simplex ETF 318A PCF file")
        
        # URL of the Simplex ETF page
        url = "https://www.simplexasset.com/etf/eng/etf.html"
        
        # Get the HTML content while bypassing SSL verification
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the PCF link for ETF 318A
        pcf_link = None
        for input_tag in soup.find_all("input", {"type": "image"}):
            onclick = input_tag.get("onclick", "")
            if "318A" in onclick and ".pcf" in onclick.lower():
                pcf_link = onclick.split("'")[1]
                break
            elif "318A.csv" in onclick:
                # If PCF not found, fall back to CSV format
                pcf_link = onclick.split("'")[1]
                break
        
        # If the PCF link is found, download it
        if pcf_link:
            # Correct URL format
            if pcf_link.startswith(".."):
                pcf_url = f"https://www.simplexasset.com/etf/{pcf_link.lstrip('..')}"
            else:
                pcf_url = f"https://www.simplexasset.com/etf/{pcf_link}"
                
            logger.info(f"Downloading PCF from URL: {pcf_url}")
            pcf_response = requests.get(pcf_url, headers=headers, verify=False)
            pcf_response.raise_for_status()
            
            # Create filename with timestamp
            current_datetime = datetime.now().strftime("%Y%m%d%H%M")
            if ".pcf" in pcf_link.lower():
                final_filename = f"318A-PCF-{current_datetime}.csv"
            else:
                final_filename = f"318A-{current_datetime}.csv"
            
            final_path = os.path.join(SAVE_DIR, final_filename)
            
            with open(final_path, "wb") as file:
                file.write(pcf_response.content)
            
            logger.info(f"PCF file saved successfully to: {final_path}")
            return final_path
        else:
            logger.warning("PCF link for ETF 318A not found")
            return None
    
    except Exception as e:
        logger.error(f"Error downloading PCF file: {str(e)}")
        logger.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    # Download the ETF PCF file
    pcf_path = download_simplex_etf_pcf()
    
    if pcf_path:
        print(f"✅ ETF PCF file saved successfully to: {pcf_path}")
    else:
        print("❌ Failed to download ETF PCF file")
        exit(1)
