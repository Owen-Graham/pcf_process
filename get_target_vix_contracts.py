import os
import pandas as pd
from datetime import datetime, timezone

# Assuming common.py and price_limits_tracker.py (for get_etf_composition logic)
# are in the same directory or accessible in PYTHONPATH.
# We will simplify by re-implementing parts of get_etf_composition logic here
# to avoid circular dependencies or making this script too complex.
from common import (
    setup_logging,
    SAVE_DIR,
    normalize_vix_ticker,
    get_tradingview_vix_contract_code,
    InvalidDataError,
    MissingCriticalDataError
)

logger = setup_logging('get_target_vix_contracts')

def get_latest_etf_composition_contracts():
    """
    Reads etf_characteristics_master.csv, determines the latest composition's
    near and far futures, normalizes them, and converts them to TradingView format.

    Returns:
        list: A list of TradingView-formatted VIX contract codes (e.g., ["VXM2025", "VXN2025"]),
              or an empty list if an error occurs.
    """
    try:
        etf_file = os.path.join(SAVE_DIR, "etf_characteristics_master.csv")
        if not os.path.exists(etf_file):
            logger.error(f"ETF characteristics file not found: {etf_file}")
            return []

        df = pd.read_csv(etf_file)
        if df.empty:
            logger.error("ETF characteristics file is empty.")
            return []

        required_cols = ['fund_date', 'near_future', 'far_future']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in ETF characteristics file: {missing_cols}")
            return []

        try:
            df['date'] = pd.to_datetime(df['fund_date'], format='%Y%m%d')
        except Exception as e:
            logger.error(f"Error converting fund_date to datetime: {str(e)}")
            return []

        # Determine the reference date for "latest" - use today in UTC for this script's purpose
        # as it's preparing contracts for an immediate scrape.
        # In get_etf_composition, a specific closing_time was used. Here, "today" is fine.
        today_utc = datetime.now(timezone.utc)
        target_date = today_utc.replace(tzinfo=None) # Make naive for comparison

        df_filtered = df[df['date'] <= target_date]
        if df_filtered.empty:
            logger.error(f"No data found in ETF characteristics for date {target_date.strftime('%Y-%m-%d')} or earlier.")
            # Fallback: try to use the absolute latest if no data for today/past.
            if not df.empty:
                df_filtered = df # Use all data if filtering by date yields nothing
            else: # df is truly empty
                 return []


        latest_data = df_filtered.sort_values('date', ascending=False).iloc[0]
        logger.info(f"Using ETF composition data from fund_date: {latest_data['date'].strftime('%Y-%m-%d')}")

        near_future_raw = str(latest_data['near_future'])
        far_future_raw = str(latest_data['far_future'])

        if pd.isna(near_future_raw) or pd.isna(far_future_raw):
            logger.error("Near_future or far_future is null in the latest ETF composition.")
            return []

        normalized_near = normalize_vix_ticker(near_future_raw)
        normalized_far = normalize_vix_ticker(far_future_raw)

        logger.info(f"Normalized contracts: Near={normalized_near}, Far={normalized_far}")

        tv_near = get_tradingview_vix_contract_code(normalized_near)
        tv_far = get_tradingview_vix_contract_code(normalized_far)

        logger.info(f"TradingView contracts: Near={tv_near}, Far={tv_far}")

        # Ensure no duplicates if near and far somehow end up being the same after normalization/conversion
        contracts = list(dict.fromkeys([tv_near, tv_far]))
        return contracts

    except (InvalidDataError, MissingCriticalDataError) as e:
        logger.error(f"Data error in getting ETF composition contracts: {e}")
        return []
    except FileNotFoundError:
        logger.error(f"ETF characteristics file not found at {etf_file}.")
        return []
    except pd.errors.EmptyDataError:
        logger.error(f"ETF characteristics file {etf_file} is empty (pandas error).")
        return []
    except Exception as e:
        logger.error(f"Unexpected error getting ETF composition contracts: {e}", exc_info=True)
        return []

if __name__ == "__main__":
    target_contracts = get_latest_etf_composition_contracts()
    if target_contracts:
        print(" ".join(target_contracts))
    else:
        logger.error("No target VIX contracts determined. Exiting with error.")
        exit(1) # Exit with error code if no contracts found
