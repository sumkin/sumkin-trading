class Position:

    def __init__(self):
        self.tickers = []
        self.amounts = []

    def is_empty(self):
        assert len(self.tickers) == len(self.amounts)
        if len(self.tickers) == 0:
            return True

