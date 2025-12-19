import csv
from pathlib import Path
from engine.trade_engine import TradeEngine
from storage.data_manager import DataManager
from collectors.l2_collector import L2Collector
from scanner.binance_client import BinanceClient
from collectors.tape_collector import TapeCollector


class Symbol:
    def __init__(self, symbol, session_dir: Path, session_ts):
        self.symbol = symbol
        self.symbol_dir = session_dir / symbol
        self.symbol_dir.mkdir(parents=True, exist_ok=True)

        self.trade_engine = TradeEngine(session_dir, self.symbol, session_ts)
        self.data_manager = DataManager(self.symbol_dir, self.trade_engine)

        self._backfill_klines()

        self.tape = TapeCollector(symbol, self.data_manager)
        self.l2 = L2Collector(symbol, self.data_manager)

    def _backfill_klines(self):
        client = BinanceClient()
        klines = client.klines(self.symbol, limit=600)

        path = self.symbol_dir / f"klines_{self.data_manager.session_ts}.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["symbol","open_time","open","high","low","close","volume","close_time"])
            for k in klines:
                w.writerow([self.symbol, *k[:6], k[6]])
