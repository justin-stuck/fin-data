from dataclasses import dataclass

from pandas_datareader import data

from ..price_fetcher_base import PriceFetcher


# TODO consider reworking the abstraction
class DataReaderStockPriceFetcher(PriceFetcher):
    def get_asset_prices(self, name: str, id: str, start: str, end: str):
        df = data.DataReader(
            name, start=start, end=end, data_source="yahoo"
        ).reset_index()
        df["stock_id"] = id
        return df
