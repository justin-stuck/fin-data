import os
from fire import Fire
from pathlib import Path
from .scrapers.macro_trends_financial_statements import MacroTrendsScraper
from .scrapers.sec import download_sec_data, extract_files_in_folders


DATA_PATH = 'data'


def main(scrape_type: str):
    Path(DATA_PATH).mkdir(exist_ok=True)
    if scrape_type == 'financial_statements':
        scraper = MacroTrendsScraper()
        scraper.get_available_stocks()
        stocks = scraper.get_recent_available_stocks()
        print(stocks)
    elif scrape_type == 'sec':
        path = os.path.join(DATA_PATH, 'SECData')
        Path(path).mkdir(exist_ok=True)
        download_sec_data(data_path=DATA_PATH, overwrite=False)
        extract_files_in_folders()


if __name__ == "__main__":
    Fire(main)