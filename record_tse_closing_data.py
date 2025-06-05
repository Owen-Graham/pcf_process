import os
import csv
from datetime import datetime
import logging

# Assuming price_utils.py is in the same directory or accessible in PYTHONPATH
from price_utils import get_closing_price
from common import setup_logging, SAVE_DIR # Using common.py for SAVE_DIR and setup_logging

# Set up logging
logger = setup_logging('record_tse_closing_data')

TICKER_SYMBOL = "318A.T"

def ensure_data_directory():
    """Ensures that the SAVE_DIR directory exists."""
    os.makedirs(SAVE_DIR, exist_ok=True)

def record_closing_price():
    """
    Fetches the closing price for TICKER_SYMBOL and saves it to a CSV file.
    The CSV filename includes the closing date.
    """
    logger.info(f"Attempting to fetch closing data for {TICKER_SYMBOL}...")

    # get_closing_price returns (closing_price, closing_date_dt, price_limits)
    # We pass our logger to get_closing_price for its internal logging
    closing_price, closing_date_dt, _ = get_closing_price(TICKER_SYMBOL, logger=logger)

    if closing_price is None or closing_date_dt is None:
        logger.error(f"Failed to retrieve closing data for {TICKER_SYMBOL}. No data will be saved.")
        return

    logger.info(f"Successfully fetched closing price for {TICKER_SYMBOL}: {closing_price} at {closing_date_dt}")

    # Format the closing_date_dt (which is a datetime object) to 'YYYY-MM-DD' string for general use
    # and 'YYYYMMDD' for the filename.
    try:
        # The closing_date_dt from get_closing_price can be timezone-aware (e.g., JST or UTC).
        # For CSV date column, we just want the date part.
        formatted_date_str = closing_date_dt.strftime('%Y-%m-%d')
        filename_date_str = closing_date_dt.strftime('%YMMDD')
    except Exception as e:
        logger.error(f"Error formatting the closing date {closing_date_dt}: {e}")
        return

    # Define CSV filename and path
    csv_filename = f"tse_318a_closing_price_{filename_date_str}.csv"
    filepath = os.path.join(SAVE_DIR, csv_filename)

    # Data to write
    header = ['date', 'symbol', 'closing_price']
    row = [formatted_date_str, TICKER_SYMBOL, closing_price]

    try:
        ensure_data_directory() # Ensure data/ directory exists

        # Check if file already exists for this date. If so, we might overwrite or append.
        # For this requirement, let's assume overwriting is fine for simplicity,
        # as the job is intended to run once per day for that day's closing price.
        # If there's a need to prevent overwriting valid, already fetched data,
        # more sophisticated checks would be needed.

        logger.info(f"Saving closing price data to {filepath}...")
        with open(filepath, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerow(row)
        logger.info(f"Successfully saved data to {filepath}")

    except IOError as e:
        logger.error(f"Could not write to CSV file {filepath}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while saving data: {e}")

if __name__ == "__main__":
    logger.info("--- Starting TSE:318A Closing Price Recording Script ---")
    record_closing_price()
    logger.info("--- TSE:318A Closing Price Recording Script Finished ---")
