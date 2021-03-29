import concurrent.futures
import logging
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, types

from ...config import config
from .stock_price_fetchers import DataReaderStockPriceFetcher

DATE_FORMAT = "%Y-%m-%d"
MAX_DATE = "1970-1-1"
TODAY = datetime.today().strftime(DATE_FORMAT)


def write_df_to_db(df):
    engine = create_engine(f"sqlite:///{config.db_path}", echo=False)
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]
    df["date"] = df["date"].astype(str)
    df.to_sql(
        name="stock_prices",
        con=engine,
        if_exists="append",
        index=False,
        dtype={
            "stock_id": types.Integer,
            "date": types.Text,
            "high": types.Float,
            "low": types.Float,
            "open": types.Float,
            "close": types.Float,
            "volume": types.Integer,
            "adj_close": types.Float,
        },
    )


def get_last_update_date_by_stock():
    # init db connection
    connection = sqlite3.connect(config.db_path)

    cursor = connection.cursor()

    # TODO optimize this query
    cursor.execute(
        """
        SELECT si.stock_id, MAX(sp.id), si.ticker, sp.date FROM stock_info si
        LEFT JOIN stock_prices sp
        ON si.stock_id = sp.stock_id
        GROUP BY sp.stock_id
    """
    )

    rows = cursor.fetchall()
    df = pd.DataFrame(
        rows, columns=["stock_id", "max_id", "ticker", "latest_date"]
    )
    df["latest_date"] = df["latest_date"].fillna(MAX_DATE)
    return df


def get_list_of_stocks():
    # init db connection
    connection = sqlite3.connect(config.db_path)

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT stock_id, ticker FROM stock_info
    """
    )

    return cursor.fetchall()


def update_price_tables(batch_size=500):
    # rows = get_list_of_stocks()
    last_updates = get_last_update_date_by_stock()

    data_fetcher = DataReaderStockPriceFetcher()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        chunks = [
            last_updates.iloc[i : i + batch_size][
                ["stock_id", "ticker", "latest_date"]
            ]
            for i in range(0, last_updates.shape[0], batch_size)
        ]
        for chunk in chunks:
            results = []
            futures = []
            for _, (stock_id, ticker, start_date) in chunk.iterrows():
                # TODO get last trading day...
                if start_date == TODAY:
                    logging.info(f"{ticker} up to date")
                    continue
                start_date = datetime.strptime(start_date, DATE_FORMAT)
                start_date = start_date + timedelta(days=1)
                start_date = start_date.strftime(DATE_FORMAT)
                # TODO need to use a time delta of one day on the start date
                # so that it does not duplicate records for the last date
                futures.append(
                    executor.submit(
                        data_fetcher.get_asset_prices,
                        name=ticker,
                        id=stock_id,
                        start=start_date,
                        end=TODAY,
                    )
                )
            for future in concurrent.futures.as_completed(futures):
                try:
                    df = future.result()
                    logging.info(
                        f"Pulled and adding {df.shape[0]} rows of price data for "
                        + f" from {MAX_DATE} to {TODAY}"
                    )

                    results.append(df)
                except Exception as e:
                    logging.warning(e)
                    logging.warning(f"Failed to pull data for {ticker}")
            if len(results) > 0:
                df = pd.concat(results)
                write_df_to_db(df)
