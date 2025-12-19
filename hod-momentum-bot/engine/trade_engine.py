import json
import logging
from pathlib import Path
from collections import deque
from datetime import datetime, timezone
from dataclasses import dataclass, field


# ----- CONFIG -----
from config.settings import *

# ----- LOGGING -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ----- DATA STRUCTURES -----
@dataclass
class Trade:
    symbol: str
    entry_ts: int
    entry_dt: str
    entry_price: float

    max_price: float
    min_price: float

    max_pct: float = 0.0
    min_pct: float = 0.0

    acceptance_count: int = 0
    rejection_count: int = 0

    first_rejection_ts: int | None = None

    price_track: dict = field(default_factory=dict)
    entry_snapshot: dict = field(default_factory=dict)


# ----- TRADE ENGINE -----
class TradeEngine:
    """
    Per-symbol engine.
    Trades from ALL symbols are written into ONE session file.
    """

    def __init__(self, base_dir: Path, symbol: str, session_ts: str):
        self.base_dir = base_dir
        self.symbol = symbol
        self.session_ts = session_ts

        self.state = "FLAT"
        self.trade: Trade | None = None

        self.ready = False
        self.first_ts = None

        self.acceptance_count = 0
        self.rejection_count = 0

        self.price_window = deque(maxlen=PRICE_LOOKBACK_SEC)

    # =========================================================
    # ENTRY POINT
    # =========================================================

    def on_metrics(self, tape: dict, l2: dict):
        ts = tape["ts"]
        price = tape["price"]

        self.price_window.append(price)

        if self.first_ts is None:
            self.first_ts = ts

        if not self._ready(tape):
            self._log("WAITING normalization")
            return

        if not self.ready:
            self.ready = True
            self._log("READY normalization complete")

        # =====================================================
        # FLAT
        # =====================================================
        if self.state == "FLAT":
            self.acceptance_count = 0
            self.rejection_count = 0

            if self._acceptance(tape, l2):
                self.acceptance_count = 1
                self.state = "SETUP"
                self._log("SETUP started")
            return

        # =====================================================
        # SETUP
        # =====================================================
        if self.state == "SETUP":
            if self._acceptance(tape, l2):
                self.acceptance_count += 1
            else:
                self._reset_setup()
                return

            if (
                self.acceptance_count >= ACCEPTANCE_BARS
                and self._price_filter(price)
            ):
                self._enter_trade(ts, price, tape, l2)
                self.state = "IN_TRADE"
            return

        # =====================================================
        # IN TRADE
        # =====================================================
        self._update_extremes(price)
        self._track_exploratory_prices(ts, price)

        # ---- HARD SL ----
        if price <= self.trade.entry_price * (1 - HARD_SL_PCT):
            self._exit_trade(ts, price, "HARD_SL")
            return

        # ---- MODE SPECIFIC ----
        if MODE == "exploratory":
            self._exploratory_exit(ts, price)
        else:
            self._real_exit(ts, price, tape, l2)

    # =========================================================
    # ENTRY CONDITIONS
    # =========================================================

    def _acceptance(self, t, l2):
        return (
            t["z_cvd_slope"] > 0
            and t["z_price_eff"] > 0
            and t["ew_afi"] > 0
            and l2["ew_obi"] > 0
            and l2["ask_liq_delta"] < 0
            and l2["weighted_obi"] > 0
            and t["z_sell_eff"] <= 0
        )

    def _rejection(self, t, l2):
        return (
            (t["z_price_eff"] < 0 and t["z_sell_eff"] > 0)
            or (t["z_sell_eff"] > t["z_buy_eff"])
            or l2["ew_obi"] < 0
        )

    def _price_filter(self, price):
        if len(self.price_window) < PRICE_LOOKBACK_SEC:
            return False
        return price >= max(self.price_window)

    # =========================================================
    # TRADE MGMT
    # =========================================================

    def _enter_trade(self, ts, price, tape, l2):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        snapshot = {}
        for src in (tape, l2):
            for k, v in src.items():
                snapshot[k] = v

        self.trade = Trade(
            symbol=self.symbol,
            entry_ts=ts,
            entry_dt=dt,
            entry_price=price,
            max_price=price,
            min_price=price,
            entry_snapshot=snapshot,
        )

        self._log(f"ENTER price={price:.6f}")

    def _exploratory_exit(self, ts, price):
        t = self.trade

        if price >= t.entry_price * (1 + EXP_TP_PCT):
            self._exit_trade(ts, price, "TP")
        elif price <= t.entry_price * (1 - EXP_SL_PCT):
            self._exit_trade(ts, price, "SL")

    def _real_exit(self, ts, price, tape, l2):
        if self._rejection(tape, l2):
            if self.trade.first_rejection_ts is None:
                self.trade.first_rejection_ts = ts

            self.rejection_count += 1
        else:
            self.rejection_count = 0

        if self.rejection_count >= REJECTION_BARS:
            self._exit_trade(ts, price, "FLOW")

        elif ts - self.trade.entry_ts >= MAX_TRADE_DURATION_SEC:
            self._exit_trade(ts, price, "TIME")

    def _exit_trade(self, ts, price, reason):
        t = self.trade
        exit_dt = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        record = {
            "symbol": t.symbol,
            "entry_time": t.entry_dt,
            "exit_time": exit_dt,
            "entry_price": t.entry_price,
            "exit_price": price,
            "duration_sec": ts - t.entry_ts,
            "max_favorable_pct": t.max_pct,
            "max_adverse_pct": t.min_pct,
            "first_rejection_ts": t.first_rejection_ts,
            "exit_reason": reason,
            "entry_snapshot": t.entry_snapshot,
            "price_path": t.price_track,
            "mode": MODE,
        }

        self._write_trade(record)
        self._log(f"EXIT {reason} price={price:.6f}")

        self.trade = None
        self.state = "FLAT"

    def _update_extremes(self, price):
        t = self.trade
        t.max_price = max(t.max_price, price)
        t.min_price = min(t.min_price, price)
        t.max_pct = (t.max_price - t.entry_price) / t.entry_price
        t.min_pct = (t.min_price - t.entry_price) / t.entry_price

    def _track_exploratory_prices(self, ts, price):
        if MODE != "exploratory":
            return

        elapsed = ts - self.trade.entry_ts
        for sec in EXPLORATORY_TRACK_SEC:
            if elapsed >= sec and sec not in self.trade.price_track:
                self.trade.price_track[sec] = price

    def _reset_setup(self):
        self.state = "FLAT"
        self.acceptance_count = 0

    # =========================================================
    # NORMALIZATION
    # =========================================================

    def _ready(self, t):
        required = [
            "z_cvd_slope",
            "z_vol_accel",
            "z_price_eff",
            "z_buy_eff",
            "z_sell_eff",
        ]
        return all(k in t for k in required)

    # =========================================================
    # FILE IO
    # =========================================================

    def _write_trade(self, record):
        out_dir = self.base_dir / "trades"
        out_dir.mkdir(parents=True, exist_ok=True)

        path = out_dir / f"{self.session_ts}.jsonl"
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")

    # =========================================================
    # LOGGING
    # =========================================================

    def _log(self, msg):
        logging.info(f"{self.symbol} | {msg}")
