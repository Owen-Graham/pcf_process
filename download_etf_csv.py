import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Define local storage directory
SAVE_DIR = "pcfs"
os.makedirs(SAVE_DIR, exist_ok=True)

# URL of the Simplex ETF page
url = "https://www.simplexasset.com/etf/eng/etf.html"

# Get the HTML content while bypassing SSL verification
response = requests.get(url, verify=False)
response.raise_for_status()

# Parse the HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Find the CSV link for ETF 318A
csv_link = None
for input_tag in soup.find_all("input", {"type": "image"}):
    if "318A.csv" in input_tag["onclick"]:
        csv_link = input_tag["onclick"].split("'")[1]
        break

# If the CSV link is found, download it
if csv_link:
    csv_url = f"https://www.simplexasset.com/etf/{csv_link.lstrip('..')}"  # Corrected URL format
    csv_response = requests.get(csv_url, verify=False)
    csv_response.raise_for_status()

    # Temporary save path
    temp_csv_path = os.path.join(SAVE_DIR, "temp_318A.csv")
    
    with open(temp_csv_path, "wb") as file:
        file.write(csv_response.content)
    
    # Read the CSV to extract Fund Date
    df = pd.read_csv(temp_csv_path)

    try:
        fund_date = str(df["Fund Date"].iloc[0]).replace("/", "")  # Extracting Fund Date from column
        if fund_date.lower() == "nan":  # Handle cases where fund_date is not available
            fund_date = "unknown"
    except Exception as e:
        fund_date = "unknown"
    
    # Get current date-time
    current_datetime = datetime.now().strftime("%Y%m%d%H%M")
    
    # Define final file name
    final_filename = f"318A-{fund_date}-{current_datetime}.csv"
    final_csv_path = os.path.join(SAVE_DIR, final_filename)
    
    # Rename file to final filename
    os.rename(temp_csv_path, final_csv_path)
    
    print(f"✅ CSV file saved successfully to: {final_csv_path}")
    
    # Commit the file to the GitHub repository
    os.system("git config --global user.name 'github-actions'")
    os.system("git config --global user.email 'github-actions@github.com'")
    os.system(f"git add {final_csv_path}")
    os.system("git commit -m 'Updated ETF Data'")
    os.system("git push")
else:
    print("❌ CSV link for ETF 318A not found.")
