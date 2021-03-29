import abc


class PriceFetcher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_asset_prices(self, name: str, id: str, start: str, end: str):
        pass
