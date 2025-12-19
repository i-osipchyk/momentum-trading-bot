import time
import threading
from collections import deque
from datetime import datetime, timezone
from symbol import Symbol
from scanner.binance_client import BinanceClient


BATCH_SIZE = 10
SCAN_INTERVAL = 2
MIN_5M_VOL = 300_000
PCT_UP = 0.10

class SymbolSelector:
    def __init__(self, session_dir, session_ts):
        self.client = BinanceClient()
        self.session_dir = session_dir
        self.session_ts = session_ts
        self.symbols = {}
        self.queue = deque()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        all_syms = self.client.futures_symbols()
        self.queue.extend(all_syms)
        self.thread.start()

    def _run(self):
        while True:
            batch = [self.queue.popleft() for _ in range(min(BATCH_SIZE, len(self.queue)))]
            self.queue.extend(batch)

            tickers = {t["symbol"]: t for t in self.client.ticker_24h()}

            for sym in batch:
                if sym in self.symbols or sym not in tickers:
                    continue

                t = tickers[sym]
                open_p = float(t["openPrice"])
                last_p = float(t["lastPrice"])

                if open_p <= 0:
                    continue

                if (last_p - open_p) / open_p < PCT_UP:
                    continue

                kl = self.client.klines(sym, limit=6)
                vol_5m = sum(float(k[5]) for k in kl[:-1])

                if vol_5m >= MIN_5M_VOL:
                    self._add_symbol(sym)

            time.sleep(SCAN_INTERVAL)

    def _add_symbol(self, symbol):
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        print(f"[ADD] {symbol} at {ts}")
        self.symbols[symbol] = Symbol(symbol, self.session_dir, self.session_ts)
