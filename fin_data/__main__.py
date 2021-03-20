import logging
import os
from pathlib import Path

from fire import Fire

from .scrapers.macro_trends_financial_statements import MacroTrendsScraper
from .scrapers.sec import download_sec_data, extract_files_in_folders
from .scrapers.stocks import StockPrices

DATA_PATH = "data"


def main(scrape_type: str, db: bool = True):
    logging.basicConfig(level="INFO")
    Path(DATA_PATH).mkdir(exist_ok=True)
    print(scrape_type)
    if scrape_type == "available_stocks":
        scraper = MacroTrendsScraper()
        scraper.get_available_stocks()
    elif scrape_type == "financial_statements":
        scraper = MacroTrendsScraper()
        available_stocks = scraper.get_recent_available_stocks()
        pass
    elif scrape_type == "sec":
        path = os.path.join(DATA_PATH, "SECData")
        Path(path).mkdir(exist_ok=True)
        download_sec_data(data_path=DATA_PATH, overwrite=False)
        extract_files_in_folders()
    elif scrape_type == "stock":
        scraper = MacroTrendsScraper()
        available_stocks = scraper.get_recent_available_stocks()
        available_stocks = [x["ticker"] for x in available_stocks]
        price_puller = StockPrices(available_stocks)
        if db:
            price_puller.download_available_stock_data_to_db()
        else:
            path = os.path.join(DATA_PATH, "stocks")
            Path(path).mkdir(exist_ok=True)
            price_puller.download_available_stock_data()


if __name__ == "__main__":
    Fire(main)
