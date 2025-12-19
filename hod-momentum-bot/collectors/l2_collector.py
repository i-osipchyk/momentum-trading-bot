import time
import json
import logging
import asyncio
import threading
import websockets
from aggregators.l2_aggregator import L2Aggregator


# ----- CONFIG -----
from config.settings import BINANCE_FUTURES_WS


class L2Collector:
    def __init__(self, symbol: str, data_manager, depth_levels: int = 20):
        self.symbol = symbol.upper()
        self.data_manager = data_manager
        self.depth_levels = depth_levels
        self.agg = L2Aggregator(levels_used=self.depth_levels)

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while True:
            try:
                asyncio.run(self._collect())
            except Exception as e:
                logging.error(
                    f"{self.symbol} | L2Collector crashed: {e}. Restarting in 5s"
                )
                time.sleep(5)

    async def _collect(self):
        stream = f"{self.symbol.lower()}@depth@100ms"
        url = f"{BINANCE_FUTURES_WS}?streams={stream}"

        logging.info(f"{self.symbol} | L2Collector connected")
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5
            ) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data")
                    if payload:
                        self._handle_l2(payload)

        except Exception as e:
            logging.warning(f"{self.symbol} | L2Collector connection lost: {e}")
            raise

    def _handle_l2(self, d: dict):
        ts = d.get("E", int(time.time() * 1000)) // 1000

        bids = [(float(p), float(q)) for p, q in d.get("b", []) if float(q) > 0]
        asks = [(float(p), float(q)) for p, q in d.get("a", []) if float(q) > 0]

        if not bids or not asks:
            return

        metrics = self.agg.update_l2(bids, asks, ts)

        if metrics:
            self.data_manager.add_l2(metrics)
