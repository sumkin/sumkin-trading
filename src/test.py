import sys 
from alpaca.trading.client import TradingClient 
sys.path.append("..")


from alpaca_keys import * 


trading_client = TradingClient(API_KEY, SECRET_KEY, paper = True)

# Getting account information and printing it.
account = trading_client.get_account()
for property_name, value in account:
    print("\"{}\": {}".format(property_name, value))


