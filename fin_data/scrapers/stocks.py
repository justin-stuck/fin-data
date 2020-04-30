import json
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from pandas_datareader import data
from typing import List

# max is old
# min is new
class StockPrices:
    MAX_DATE = "1970-1-1"
    TODAY = datetime.today().strftime("%Y-%m-%d")
    PATH = "data/stocks"
    PULL_CONFIG = os.path.join(PATH, "pull_config.json")

    def __init__(self, available_stocks: List[str]):
        self.available_stocks = available_stocks
        self.pull_config = self.get_pull_config()
        print(self.pull_config)
        # TODO create a check to not pull if executed on the same day and
        # data exists for today

    def get_pull_config(self):
        pull_config = {
            "max_date": self.MAX_DATE,
            "start": self.MAX_DATE,
            "end": self.TODAY,
        }
        if os.path.exists(self.PULL_CONFIG):
            with open(self.PULL_CONFIG, "r") as f:
                old_pull_config = json.load(f)
            old_end = datetime.strptime(old_pull_config["end"], "%Y-%m-%d")
            if (old_end - datetime.today()).days < 0:
                pull_config["start"] = (old_end + timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                )
        return pull_config

    def get_stock_data_from_source(
        self, ticker: str, start=MAX_DATE, end=TODAY
    ):
        # this returns a dataframe with daily
        # High, Low, Open, Close, Volume, Adj Close
        # for each trading day in that interval
        return data.DataReader(
            ticker, start=start, end=end, data_source="yahoo"
        )

    def download_available_stock_data(self, overwrite=False):
        # TODO create functionality to only get data that isn't there
        for ticker in self.available_stocks[:10]:
            logging.info(f"Downloading data for {ticker} on {self.TODAY}")
            try:
                stock_path = os.path.join(self.PATH, f"{ticker}.csv")
                # TODO append if start date differs from max date else pull all
                # also need to check if day was yesterday and handle pulls for 
                # individual days
                start, end = self.pull_config["start"], self.pull_config["end"]
                df = self.get_stock_data_from_source(
                    ticker=ticker, start=start, end=end
                )
                df.to_csv(stock_path)
            except Exception as e:
                logging.warning(
                    f"Issue pulling data for {ticker}. Simple Exception: {e}"
                )
        logging.info(
            f"Successfully finished pulling data on {self.TODAY}. Writing pull config."
        )
        with open(self.PULL_CONFIG, "w") as f:
            json.dump(self.pull_config, f)
        logging.info(
            f"Successfully wrote pull config to {self.PULL_CONFIG}"
        )
