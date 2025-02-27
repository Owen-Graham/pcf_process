name: VIX and ETF Data Collection

on:
  schedule:
    # ETF data collection - daily at 5:15 AM UTC
    - cron: "15 5 * * *"
    # VIX futures data collection - Mon-Fri at 23:15 UTC (after market close)
    - cron: "15 23 * * 1-5"
  workflow_dispatch:  # Allow manual triggering

permissions:
  contents: write  # Need write permission to push files back to repo

jobs:
  etf-data:
    runs-on: ubuntu-latest
    # Only run ETF jobs during the morning schedule (5:15 AM)
    if: github.event_name == 'workflow_dispatch' || (github.event_name == 'schedule' && github.event.schedule == '15 5 * * *')
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
      with:
        fetch-depth: 0  # Full history for commits

    - name: Set up Python 3.10
      uses: actions/setup-python@v4
      with:
        python-version: "3.10"

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install requests beautifulsoup4 pandas

    - name: R

    - name: Extract VIX futures from PCF
      run: python pcf_vix_extractor.py

    - name: Commit and Push ETF data
      run: |
        git config --global user.name "GitHub Actions Bot"
        git config --global user.email "actions@github.com"
        git add data/*.csv
        git add data/*.log
        git commit -m "Daily ETF data update $(date +'%Y-%m-%d')" || echo "No changes to commit"
        git push

    - name: Upload ETF data as artifacts
      uses: actions/upload-artifact@v4
      with:
        name: etf-data
        path: |
          data/*.csv
          data/*.log
        retention-days: 399  # Keep for 399 days

    - name: Display log file (for debugging)
      if: always()  # Run even if previous steps fail
      run: |
        echo "=== ETF CSV DOWNLOAD LOG ==="
        cat data/etf_csv_downloader.log || echo "Log file not found"
        echo "==========================="
        echo "=== ETF PCF DOWNLOAD LOG ==="
        cat data/etf_pcf_downloader.log || echo "Log file not found"
        echo "==========================="
        echo "=== PCF VIX EXTRACTION LOG ==="
        cat data/pcf_vix_extractor.log || echo "Log file not found"
        echo "==============================="

  vix-futures:
    runs-on: ubuntu-latest
    # Only run VIX jobs during the evening schedule (23:15 UTC) or manual trigger
    if: github.event_name == 'workflow_dispatch' || (github.event_name == 'schedule' && github.event.schedule == '15 23 * * 1-5')
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
      with:
        fetch-depth: 0  # Full history for commits

    - name: Set up Python 3.10
      uses: actions/setup-python@v4
      with:
        python-version: "3.10"

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install yfinance pandas requests beautifulsoup4

    - name: Run CBOE VIX Downloader
      run: python cboe_vix_downloader.py

    - name: Run Yahoo Finance VIX Downloader  
      run: python yahoo_vix_downloader.py

    - name: Run VIX Futures Master Downloader
      run: python vix_futures_downloader.py

    - name: Commit and Push VIX futures data
      run: |
        git config --global user.name "GitHub Actions Bot"
        git config --global user.email "actions@github.com"
        git add data/vix_futures_*.csv
        git add data/vix_downloader.log
        git add data/cboe_vix_downloader.log
        git add data/yahoo_vix_downloader.log
        git add data/cboe_debug.html
        git commit -m "VIX futures update $(date +'%Y-%m-%d')" || echo "No changes to commit"
        git push

    - name: Upload VIX futures data as artifacts
      uses: actions/upload-artifact@v4
      with:
        name: vix-futures-data
        path: |
          data/vix_futures_*.csv
          data/*.log
          data/cboe_debug.html
        retention-days: 399  # Keep for 399 days

    - name: Display log files (for debugging)
      if: always()  # Run even if previous steps fail
      run: |
        echo "=== CBOE VIX DOWNLOADER LOG ==="
        cat data/cboe_vix_downloader.log || echo "Log file not found"
        echo "================================"
        echo "=== YAHOO VIX DOWNLOADER LOG ==="
        cat data/yahoo_vix_downloader.log || echo "Log file not found"
        echo "================================="
        echo "=== VIX FUTURES DOWNLOADER LOG ==="
        cat data/vix_downloader.log || echo "Log file not found"
        echo "==================================="
