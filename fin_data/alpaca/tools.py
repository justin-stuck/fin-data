import logging
import os
import sqlite3

import alpaca_trade_api as atp

from ..config import config

api = atp.REST(
    config.alpaca_api_key,
    config.alpaca_secret_key,
    config.alpaca_url,
    raw_data=True,
)
# assets = api.list_assets()


def update_stock_info_table():
    connection = sqlite3.connect(config.db_path)
    cursor = connection.cursor()

    tickers = cursor.execute(
        """
        SELECT ticker FROM stock_info
    """
    )
    tickers = [ticker[0] for ticker in tickers.fetchall()]

    assets = api.list_assets()
    for asset in assets:
        try:
            if (
                asset["symbol"] not in tickers
                and asset["status"] == "active"
                and asset["tradable"]
            ):
                cursor.execute(
                    "INSERT INTO stock_info (ticker, name, class, exchange) VALUES (?, ?, ?, ?)",
                    (
                        asset["symbol"],
                        asset["name"],
                        asset["class"],
                        asset["exchange"],
                    ),
                )
                logging.info(
                    f"Added new stock_info entry for {asset['symbol']}"
                )
        except Exception as e:
            logging.warning(
                f"Issue inserting info for ticker {asset['symbol']}"
            )
            logging.warning(e)
    connection.commit()
