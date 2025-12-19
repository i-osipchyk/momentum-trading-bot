import time
import requests

BASE = "https://fapi.binance.com"

class BinanceClient:
    def __init__(self, rate_sleep=0.2):
        self.rate_sleep = rate_sleep

    def _get(self, path, params=None):
        time.sleep(self.rate_sleep)
        r = requests.get(BASE + path, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    def futures_symbols(self):
        info = self._get("/fapi/v1/exchangeInfo")
        return [
            s["symbol"]
            for s in info["symbols"]
            if s["contractType"] == "PERPETUAL"
            and s["status"] == "TRADING"
            and s["quoteAsset"] == "USDT"
        ]

    def ticker_24h(self):
        return self._get("/fapi/v1/ticker/24hr")

    def klines(self, symbol, interval="1m", limit=600):
        return self._get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
        )
