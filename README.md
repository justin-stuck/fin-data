# fin-data
Tools to acquire financial data from various sources
## Getting Started
**Chrome Driver for Selenium**
- Get version of Chrome by entering chrome://version/ into the browser
- download the appropriate chromedriver [here](https://chromedriver.chromium.org/downloads)


## Run Locally
```
python -m --scrape_type <type-of-data-to-scrape>
```

## Supported Data
- Financial Statements (2005-present) (via [MacroTrends](https://www.macrotrends.net/)) (WIP)
    - scrape_type='financial_statements'
    - TODO: Create better workflow for updating info
    - TODO: Add customization
- SEC Open Data data sets (via [sec.gov/data](https://www.sec.gov/data))
    - scrape_type='sec'
    - TODO: Add customization
- Market Data (via Yahoo! Finance via pandas datareader) (TODO)