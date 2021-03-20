import os
import sqlite3

import pandas as pd
from sqlalchemy import create_engine, types

from ..config import config

CREATE_MT_STOCK_INFO_TABLE = """
    CREATE TABLE IF NOT EXISTS stock_info (
        stock_id INTEGER PRIMARY KEY,
        name TEXT,
        ticker TEXT,
        financial_statements_link TEXT,
        exchange TEXT,
        country TEXT,
        sector TEXT,
        industry TEXT
);"""

CREATE_ALPACA_STOCK_INFO_TABLE = """
    CREATE TABLE IF NOT EXISTS stock_info (
        stock_id INTEGER PRIMARY KEY,
        ticker TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        class TEXT NOT NULL,
        exchange TEXT NOT NULL 
);"""

CREATE_STOCK_PRICES_TABLE = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id INTEGER PRIMARY KEY,
        stock_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        open REAL NOT NULL,
        close REAL NOT NULL,
        volume INT NOT NULL,
        adj_close REAL NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock_info (stock_id)
    );
"""

get_drop_table_query = lambda table: f"DROP TABLE IF EXISTS {table};"

connection = sqlite3.connect(config.db_path)


def create_tables():
    with connection:
        connection.execute(CREATE_ALPACA_STOCK_INFO_TABLE)
        connection.execute(CREATE_STOCK_PRICES_TABLE)


def write_df_to_db(df, ticker):
    engine = create_engine(f"sqlite:///{config.db_path}", echo=False)
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]
    df["date"] = df["date"].astype(str)
    df.to_sql(
        name=ticker.lower(),
        con=engine,
        if_exists="replace",
        index=False,
        dtype={
            "date": types.Text,
            "high": types.Float,
            "low": types.Float,
            "open": types.Float,
            "close": types.Float,
            "volume": types.Integer,
            "adj_close": types.Float,
        },
    )


def initialize_table_from_file(path_to_file, ticker):
    df = pd.read_csv(path_to_file)
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]
    df["date"] = df["date"].astype(str)
    write_df_to_db(df, ticker)


# create_tables()
'''
test_q = """SELECT name FROM sqlite_master 
WHERE type IN ('table','view') 
AND name NOT LIKE 'sqlite_%'
ORDER BY 1;"""
with connection:
    cursor = connection.cursor()
    cursor.execute(test_q)
    print(cursor.fetchall())
    cursor.execute("Select * from stock_info")
    print(cursor.fetchall())
'''
