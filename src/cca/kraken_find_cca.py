from kraken_cash_and_carry_arbitrage_finder import KrakenCashAndCarryArbitrageFinder
from telegram_bot import TelegramBot

if __name__ == "__main__":
    kccaf = KrakenCashAndCarryArbitrageFinder()
    for spot_ticker, futures_ticker, bid, ask, vol in kccaf.find():
        value = round(bid * vol, 1)
        profit = round(100 * (bid - ask) * vol / value, 3)

        msg = ("Kraken CCA: {} - {}\n"
               "Prices: {}, {}\n"
               "Value: {}\n"
               "Profit: {}%"
               ).format(spot_ticker,
                        futures_ticker,
                        bid,
                        ask,
                        value,
                        profit)
        tb = TelegramBot()
        tb.send_message(msg)

