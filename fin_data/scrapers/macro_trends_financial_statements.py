import json
import os
import numpy as np
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from time import sleep
from typing import List
from ..selenium_drivers.chrome_driver import ChromeDriver


p = re.compile(r" var originalData = (.*?);\r\n\r\n\r", re.DOTALL)


def get_financial_statement(base_url: str, statement: str):
    page = requests.get(f"{base_url}/{statement}")
    data = json.loads(p.findall(page.text)[0])
    headers = list(data[0].keys())
    headers.remove("popup_icon")
    result = []
    for row in data:
        soup = BeautifulSoup(row["field_name"], features="lxml")
        field_name = soup.select_one("a, span").text
        fields = list(row.values())[2:]
        fields.insert(0, field_name)
        result.append(fields)

    return pd.DataFrame(result, columns=headers)


def download_financial_statements(data):
    ticker, base_url, statements, freqs = data[0], data[1], data[2], data[3]
    today = date.today().strftime("%m_%d_%Y")
    for freq, freq_opt in freqs.items():
        for statement in statements:
            try:
                df = get_financial_statement(base_url, statement + freq_opt)
                df.to_csv(
                    os.path.join(
                        "data",
                        "financial_statements",
                        freq,
                        statement,
                        f"{ticker}_{today}.csv",
                    )
                )
            except:
                print(f"Issue pulling {freq} {statement} for {ticker}")


class MacroTrendsScraper:
    SCREENER_URL = "https://www.macrotrends.net/stocks/stock-screener"
    financial_statements = [
        "income-statement",
        "balance-sheet",
        "cash-flow-statement",
        "financial-ratios",
    ]
    freqs = {"annual": "", "quarterly": "?freq=Q"}
    today = date.today().strftime("%m_%d_%Y")

    def __init__(self):
        self.create_data_folders()

    def create_data_folders(self):
        # create data folders if they do not exist
        Path("data/available_stocks").mkdir(parents=True, exist_ok=True)
        for statement in self.financial_statements:
            for freq in self.freqs.keys():
                Path(
                    os.path.join(
                        "data", "financial_statements", freq, statement
                    )
                ).mkdir(parents=True, exist_ok=True)

    def get_available_stocks(self):
        all_names = []
        all_tickers = []
        all_links = []
        last_page = False
        with ChromeDriver("fin_data/selenium_drivers/chromedriver.exe") as d:
            d.driver.get(self.SCREENER_URL)
            while not last_page:
                # get stock names
                name_elems = d.driver.find_elements_by_xpath(
                    "//div[@id='contenttablejqxGrid']/div[@role='row']/div[1]/div[1]/div[1]"
                )
                names = d.get_text_from_elements(name_elems)
                all_names.extend(names)

                # get stock ticker
                ticker_elems = d.driver.find_elements_by_xpath(
                    "//div[@id='contenttablejqxGrid']/div[@role='row']/div[2]/div[1]"
                )
                tickers = d.get_text_from_elements(ticker_elems)
                all_tickers.extend(tickers)

                # get stock base url for financial statements
                link_elems = d.driver.find_elements_by_xpath(
                    "//div[@id='contenttablejqxGrid']/div[@role='row']/div[1]/div[1]/div[1]/a[1]"
                )
                links = d.get_attribute_from_elements(link_elems, "href")
                links = ["/".join(link.split("/")[:-1]) for link in links]
                all_links.extend(links)

                # determine if last page
                result_counter = d.driver.find_element_by_xpath(
                    "//div[@id='pagerjqxGrid']/div[1]/div[last()]"
                )
                _, current_max_record, max_record_num = re.findall(
                    "\\d+", result_counter.text
                )
                if current_max_record == max_record_num:
                    last_page = True
                else:
                    # go to next page if more results
                    d.driver.find_element_by_xpath(
                        "//div[@id='pagerjqxGrid']/div[1]/div[last()-2]"
                    ).click()
                    # check that results of next page have loadead. if not, wait a second
                    if (
                        d.driver.find_elements_by_xpath(
                            "//div[@id='contenttablejqxGrid']/div[@id='row0jqxGrid']/div[1]/div[1]/div[1]"
                        )
                        == names[0]
                    ):
                        sleep(1)

        # zip results and filter stocks with missing info
        results = list(zip(all_names, all_tickers, all_links))
        results = list(filter(lambda x: all(i != "" for i in x), results))

        pd.DataFrame(results, columns=["name", "ticker", "link"]).to_csv(
            f"data/available_stocks/{self.today}.csv", index=False
        )
        return results

    def get_recent_available_stocks(self):
        path = "data/available_stocks"
        available_stocks_files = os.listdir(path)
        dates = [
            datetime.strptime(x.split(".")[0], "%m_%d_%Y")
            for x in available_stocks_files
        ]
        most_recent_file = available_stocks_files[np.argmax(dates)]
        df = pd.read_csv(os.path.join(path, most_recent_file))
        return list(df.to_records(index=False))

    def download_available_financial_statements(self, available_stocks):
        params_list = [
            (ticker, base_url, self.financial_statements, self.freqs)
            for _, ticker, base_url in available_stocks
        ]
        print(params_list)
        with ThreadPoolExecutor() as f:
            list(f.map(download_financial_statements, params_list))
