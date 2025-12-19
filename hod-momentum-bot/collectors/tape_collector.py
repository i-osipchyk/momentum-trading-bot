import time
import json
import logging
import asyncio
import threading
import websockets
from aggregators.tape_aggregator import TapeAggregator


# ----- CONFIG -----
from config.settings import BINANCE_FUTURES_WS


class TapeCollector:
    def __init__(self, symbol: str, data_manager):
        self.symbol = symbol.upper()
        self.data_manager = data_manager
        self.agg = TapeAggregator()

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while True:
            try:
                asyncio.run(self._collect())
            except Exception as e:
                logging.error(
                    f"{self.symbol} | TapeCollector crashed: {e}. Restarting in 5s"
                )
                time.sleep(5)

    async def _collect(self):
        stream = f"{self.symbol.lower()}@trade"
        url = f"{BINANCE_FUTURES_WS}?streams={stream}"

        logging.info(f"{self.symbol} | TapeCollector connected")

        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data")
                    if payload:
                        self._handle_trade(payload)

        except Exception as e:
            logging.warning(f"{self.symbol} | TapeCollector connection lost: {e}")
            raise

    def _handle_trade(self, t: dict):
        price = float(t["p"])
        if price <= 0.0:
            logging.warning(f"{self.symbol} | Dropped trade with price=0.0")
            return

        ts_sec = t["T"] // 1000
        qty = float(t["q"])
        side = "sell" if t["m"] else "buy"

        metrics = self.agg.update_trade(
            price=round(price, 6),
            size=round(qty, 6),
            side=side,
            ts=ts_sec,
        )

        if metrics:
            self.data_manager.add_tape(metrics)
