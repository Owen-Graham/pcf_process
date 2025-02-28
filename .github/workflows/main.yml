nav-calculations:
    runs-on: ubuntu-latest
    needs: [etf-data, vix-futures, fx-rates]
    # Only run after the other jobs have completed or manually triggered
    if: github.event_name == 'workflow_dispatch' || success()
    
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
        pip install pandas numpy

    - name: Download artifacts from previous jobs
      uses: actions/download-artifact@v4
      with:
        path: data
        pattern: "*-data"
        merge-multiple: true

    - name: Calculate Estimated NAVs
      run: python calculate_estimated_navs.py

    - name: Commit and Push NAV calculations
      run: |
        git config --global user.name "GitHub Actions Bot"
        git config --global user.email "actions@github.com"
        
        # Stash any changes before pull
        git stash -u || true
        
        # Pull with rebase
        git pull origin main --rebase
        
        # Pop the stash if there was something stashed
        git stash pop || true
        
        # Add and commit changes
        git add data/estimated_navs.csv
        git add data/estimated_navs_calculator.log
        git commit -m "NAV calculations update $(date +'%Y-%m-%d')" || echo "No changes to commit"
        
        # Push with force (be careful with this!)
        git push --force

    - name: Upload NAV calculations as artifacts
      uses: actions/upload-artifact@v4
      with:
        name: nav-calculations-data
        path: |
          data/estimated_navs.csv
          data/estimated_navs_calculator.log
        retention-days: 399  # Keep for 399 days

    - name: Display log file (for debugging)
      if: always()  # Run even if previous steps fail
      run: |
        echo "=== ESTIMATED NAVS CALCULATOR LOG ==="
        cat data/estimated_navs_calculator.log || echo "Log file not found"
        echo "======================================="
