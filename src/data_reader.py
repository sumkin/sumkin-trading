from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from time_frame import *

class DataReader(ABC):

    @abstractmethod
    def get_bars_df(self, ticker: str, tf: TimeFrame, start: datetime, end: datetime = None):
        pass

    @abstractmethod
    def get_price(self, ticker: str, tf: TimeFrame, dt: datetime):
        pass

    @abstractmethod
    def get_order_book(self, ticker, count, market="spot"):
        pass

    @abstractmethod
    def get_best_bid(self, ticker, market="spoo"):
        pass

    @abstractmethod
    def get_best_ask(self, ticker, market="spot"):
        pass