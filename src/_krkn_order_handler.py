import sys
import http.client
import urllib.request
import urllib.parse
import hashlib
import hmac
import base64
import json
import time

sys.path.append("..")

from kraken_api_keys import KRAKEN_SPOT_PRIVATE_KEY, KRAKEN_SPOT_PUBLIC_KEY
from kraken_api_keys import KRAKEN_FUTURES_PRIVATE_KEY, KRAKEN_FUTURES_PUBLIC_KEY


class KrknOrderHandler:

    def __init__(self):
        pass

    def add_buy_market_order(self, symb, amnt):
        response = self._request(
            method="POST",
            path="/0/private/AddOrder",
            body={
                "ordertype": "market",
                "type": "buy",
                "volume": amnt,
                "pair": symb
            },
            public_key=KRAKEN_SPOT_PUBLIC_KEY,
            private_key=KRAKEN_SPOT_PRIVATE_KEY,
            environment="https://api.kraken.com",
        )
        return response.read().decode()

    def add_sell_market_order(self, symb, amnt):
        response = self._request(
            method="POST",
            path="/0/private/AddOrder",
            body={
                "ordertype": "market",
                "type": "sell",
                "volume": amnt,
                "pair": symb
            },
            public_key=KRAKEN_SPOT_PUBLIC_KEY,
            private_key=KRAKEN_SPOT_PRIVATE_KEY,
            environment="https://api.kraken.com"
        )
        return response.read().decode()

    def get_balance(self, market="spot"):
        if market == "spot":
            response = self._request(
                method="POST",
                path="/0/private/Balance",
                public_key=KRAKEN_SPOT_PUBLIC_KEY,
                private_key=KRAKEN_SPOT_PRIVATE_KEY,
                environment="https://api.kraken.com",
            )
            print(response.read().decode())
        elif market == "futures":
            response = self._request_futures(
                method="GET",
                path="/derivatives/api/v3/accounts",
                public_key=KRAKEN_FUTURES_PUBLIC_KEY,
                private_key=KRAKEN_FUTURES_PRIVATE_KEY,
                environment="https://futures.kraken.com",
            )
            print(response.read().decode())

    def get_open_orders(self):
        response = self._request(
            method="POST",
            path="/0/private/OpenOrders",
            public_key=KRAKEN_SPOT_PUBLIC_KEY,
            private_key=KRAKEN_SPOT_PRIVATE_KEY,
            environment="https://api.kraken.com",
        )
        return response.read().decode()

    def cancel_all_orders(self):
        response = self._request(
            method="POST",
            path="/0/private/CancelAll",
            public_key=KRAKEN_SPOT_PUBLIC_KEY,
            private_key=KRAKEN_SPOT_PRIVATE_KEY,
            environment="https://api.kraken.com",
        )
        return response.read().decode()

    def _request_futures(self,
                         method: str = "GET",
                         path: str = "",
                         body: dict | None = None,
                         public_key: str = "",
                         private_key: str = "",
                         environment: str = "") -> http.client.HTTPResponse:
        """Separate method for futures API authentication"""

        def get_nonce() -> str:
            return str(int(time.time() * 1000))

        def get_futures_signature(private_key: str, endpoint: str, nonce: str, postdata: str) -> str:
            # Kraken futures signature method
            postdata = endpoint + nonce + postdata
            encoded = postdata.encode('utf-8')
            message = hashlib.sha256(encoded).digest()

            mac = hmac.new(base64.b64decode(private_key), message, hashlib.sha512)
            sigdigest = base64.b64encode(mac.digest())
            return sigdigest.decode()

        url = environment + path
        nonce = get_nonce()

        # For futures, body is JSON string, not form data
        body_str = ""
        if body is not None and len(body) > 0:
            body_str = json.dumps(body)

        headers = {
            "APIKey": public_key,  # Different header name for futures
            "Nonce": nonce,
            "Authent": get_futures_signature(private_key, path, nonce, body_str),  # Different header name
        }

        # Only add Content-Type for POST requests with body
        if method == "POST" and body_str:
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(
            method=method,
            url=url,
            data=body_str.encode() if body_str else None,
            headers=headers,
        )
        return urllib.request.urlopen(req)

    def _request(self,
                 method: str = "GET",
                 path: str = "",
                 query: dict | None = None,
                 body: dict | None = None,
                 public_key: str = "",
                 private_key: str = "",
                 environment: str = "") -> http.client.HTTPResponse:
        """Original method for spot trading"""

        def get_nonce() -> str:
            return str(int(time.time() * 1000))

        def sign(private_key: str, message: bytes) -> str:
            return base64.b64encode(
                hmac.new(
                    key=base64.b64decode(private_key),
                    msg=message,
                    digestmod=hashlib.sha512,
                ).digest()
            ).decode()

        def get_signature(private_key: str, data: str, nonce: str, path: str) -> str:
            return sign(
                private_key=private_key,
                message=path.encode() + hashlib.sha256(
                    (nonce + data)
                    .encode()
                ).digest()
            )

        url = environment + path
        query_str = ""
        if query is not None and len(query) > 0:
            query_str = urllib.parse.urlencode(query)
            url += "?" + query_str
        nonce = ""
        if len(public_key) > 0:
            if body is None:
                body = {}
            nonce = body.get("nonce")
            if nonce is None:
                nonce = get_nonce()
                body["nonce"] = nonce
        headers = {}
        body_str = ""
        if body is not None and len(body) > 0:
            # For spot API, use form encoding
            body_str = urllib.parse.urlencode(body)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if len(public_key) > 0:
            headers["API-Key"] = public_key
            headers["API-Sign"] = get_signature(private_key, query_str + body_str, nonce, path)
        req = urllib.request.Request(
            method=method,
            url=url,
            data=body_str.encode() if body_str else None,
            headers=headers,
        )
        return urllib.request.urlopen(req)


if __name__ == "__main__":
    koh = KrknOrderHandler()
    koh.get_balance(market="spot")
    koh.get_balance(market="futures")

