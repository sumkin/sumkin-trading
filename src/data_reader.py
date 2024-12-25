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