import pandas as pd

class Universe:

    def __init__(self):
        self.sp500_df = pd.read_csv("../data/sp500_companies.csv")

    def get_tickers(self):
        return self.sp500_df["Symbol"].to_list()

if __name__ == "__main__":
    universe = Universe()
    tickers = universe.get_tickers()
    print(tickers)
