from fire import Fire
from .scrapers.macro_trends_financial_statements import MacroTrendsScraper


def main(scrape_type: str):
    if scrape_type == 'financial_statements':
        scraper = MacroTrendsScraper()
        scraper.get_available_stocks()
        stocks = scraper.get_recent_available_stocks()
        print(stocks)



if __name__ == "__main__":
    Fire(main)