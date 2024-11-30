import sys
import requests
sys.path.append("..")

from sumkintradingbot_token import *

class TelegramBot:

    def __init__(self):
        pass

    def send_message(self, text):
        url = f"https://api.telegram.org/bot{sumkintradingbot_token}/sendMessage?chat_id={chat_id}&text={text}"
        requests.get(url).json()

    def send_document(self, title, fname):
        url = f"https://api.telegram.org/bot{sumkintradingbot_token}/sendDocument?"
        data = {
            "chat_id": chat_id,
            "parse_mode": "HTML",
            "caption": title
        }
        files = {
            "document": open(fname, "rb")
        }
        requests.post(url, data=data, files=files, stream=True)

if __name__ == "__main__":
    tb = TelegramBot()
    tb.send_document("test", "../output/cointegration_pairs_finder/AQUA_LSRG.html")

