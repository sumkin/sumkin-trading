from abc import ABC, abstractmethod

class Universe(ABC):

    @abstractmethod
    def get_tickers(self):
        pass
