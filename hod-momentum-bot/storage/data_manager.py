import time
import json
import threading
from pathlib import Path
from collections import deque
from datetime import datetime, timezone
from engine.trade_engine import TradeEngine


class DataManager:
    def __init__(self, symbol_dir: Path, trade_engine):
        self.symbol_dir = symbol_dir
        self.trade_engine = trade_engine

        self.tape_buf = deque()
        self.l2_buf = deque()
        self.cache = {}

        self.lock = threading.Lock()
        self.session_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

        threading.Thread(target=self._flush_loop, daemon=True).start()

    def add_tape(self, m):
        self._ingest("tape", m)

    def add_l2(self, m):
        self._ingest("l2", m)

    def _ingest(self, kind, m):
        ts = m["ts"]
        with self.lock:
            self.cache.setdefault(ts, {})[kind] = m
            getattr(self, f"{kind}_buf").append(m)

            if len(self.cache[ts]) == 2:
                tape = self.cache[ts]["tape"]
                l2 = self.cache[ts]["l2"]
                del self.cache[ts]

        if "tape" in locals():
            self.trade_engine.on_metrics(tape, l2)

    def _flush_loop(self):
        while True:
            now = time.time()
            time.sleep((int(now)//60+1)*60 - now)
            self._flush()

    def _flush(self):
        for kind in ("tape","l2"):
            buf = getattr(self, f"{kind}_buf")
            if not buf:
                continue
            path = self.symbol_dir / f"{kind}_{self.session_ts}.jsonl"
            with open(path,"a") as f:
                while buf:
                    f.write(json.dumps(buf.popleft())+"\n")
