class Position:

    def __init__(self):
        self.ticker2amount = {}
        self.ticker2price = {}

    def is_empty(self):
        assert len(self.tickers) == len(self.amounts)
        if len(self.tickers) == 0:
            return True

    def buy(self, ticker, p, amount):
        assert amount > 0
        assert ticker not in self.ticker2amount.keys()
        self.ticker2amount[ticker] = amount
        self.ticker2price[ticker] = p

    def sell(self, ticker, p, amount):
        assert amount > 0
        assert ticker not in self.ticker2amount.keys()
        self.ticker2amount[ticker] = -amount
        self.ticker2price[ticker] = p

    def close(self, p1, p2):
        self.ticker2amount = {}
        raise NotImplementedError


