import time
import math
from collections import deque
from dataclasses import dataclass


@dataclass
class Bucket:
    """
    Contains aggregated information about volume price in one second.
    """
    ts: int
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    last_price: float = 0.0


class EMA:
    """
    Used to calculate EMA for different varialbles with configurable window.
    """
    def __init__(self, window: int):
        self.alpha = 2 / (window + 1)
        self.value = None

    def update(self, x: float):
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


class TapeAggregator:
    """
    Aggregates data that comes from tape into metrics by seconds.

    Metrics:
        - AFI(Aggressor Flow Impbalance): buy and sell difference divided by total volume. EW is used for decision making.
        - CVD(Cumulative Volume Difference): difference between buy and sell volume, cumulated. Normalized slope of EW delta (t[0]-t[-1]) is used for decision making.
        - Vol Acc(Volume Acceleration): 5 and 20 EW of volume. Difference is used for decision making. # TODO: finish
        - Price Eff(Price Efficiency): how price moved relative to volume in the last n seconds. 
        - Buy Eff(Buy Efficiency): how price moved relative to buy volume in the last n seconds, if it moved up.
        - Sell Eff(Sell Efficiency): how price moved relative to sell volume in the last n seconds, if it moved down.
        - AFI Pos Ratio(AFI Positive Ratio): ratio of positive AFI in the last n seconds.
        - CVD Slope Pos Ratio(CVD Slope Positive Ratio): ratio of positive AFI in the last n seconds.
    """
    def __init__(self):
        self.current_bucket = None

        self.cvd = 0.0
        self.ew_afi = None
        self.ew_cvd = EMA(40)
        self.prev_ew_cvd = None

        self.vol_fast = EMA(5)
        self.vol_slow = EMA(20)

        self.price_hist = deque(maxlen=10)
        self.buy_hist = deque(maxlen=10)
        self.sell_hist = deque(maxlen=10)

        self.afi_hist = deque(maxlen=20)
        self.cvd_slope_hist = deque(maxlen=20)

        # normalization windows (seconds)
        self.z_windows = {
            "afi": 180,
            "cvd_slope": 120,
            "vol_accel": 120,
            "vol_per_sec": 120,
            "total_vol": 120,
            "buy_vol": 120,
            "sell_vol": 120,
            "price_eff": 60,
            "buy_eff": 60,
            "sell_eff": 60,
        }

        self.z_hist = {k: deque(maxlen=v) for k, v in self.z_windows.items()}

    def update_trade(self, price, size, side, ts=None):
        # get time timestamp in seconds from the trade
        ts = ts if ts is not None else int(time.time())
        finalized = None

        if self.current_bucket is None or ts != self.current_bucket.ts:
            if self.current_bucket:
                # finalize bucket if new timestamp detected
                finalized = self._finalize_bucket(self.current_bucket)
            # create new bucket
            self.current_bucket = Bucket(ts=ts, last_price=price)

        # update last price
        self.current_bucket.last_price = price

        # update volume
        if side == "buy":
            self.current_bucket.buy_vol += size
        else:
            self.current_bucket.sell_vol += size

        return finalized

    def _finalize_bucket(self, b: Bucket):
        buy = b.buy_vol
        sell = b.sell_vol
        total = buy + sell

        afi = (buy - sell) / total if total > 0 else 0.0
        self.ew_afi = afi if self.ew_afi is None else 0.2 * afi + 0.8 * self.ew_afi

        self.cvd += buy - sell
        ew_cvd_val = self.ew_cvd.update(self.cvd)
        cvd_slope = 0.0 if self.prev_ew_cvd is None else ew_cvd_val - self.prev_ew_cvd
        self.prev_ew_cvd = ew_cvd_val

        vol_per_sec = total
        fast = self.vol_fast.update(vol_per_sec)
        slow = self.vol_slow.update(vol_per_sec)
        vol_accel = fast - slow

        self.price_hist.append(b.last_price)
        self.buy_hist.append(buy)
        self.sell_hist.append(sell)

        price_eff = buy_eff = sell_eff = 0.0
        if len(self.price_hist) == self.price_hist.maxlen:
            dp = self.price_hist[-1] - self.price_hist[0]
            vol_sum = sum(self.buy_hist) + sum(self.sell_hist)
            if vol_sum > 0:
                price_eff = dp / vol_sum
            if sum(self.buy_hist) > 0:
                buy_eff = max(dp, 0) / sum(self.buy_hist)
            if sum(self.sell_hist) > 0:
                sell_eff = max(-dp, 0) / sum(self.sell_hist)

        self.afi_hist.append(afi > 0)
        self.cvd_slope_hist.append(cvd_slope > 0)

        metrics = {
            "ts": b.ts,
            "price": b.last_price,
            "buy_vol": buy,
            "sell_vol": sell,
            "total_vol": total,
            "vol_per_sec": vol_per_sec,
            "vol_accel": vol_accel,
            "afi": afi,
            "ew_afi": self.ew_afi,
            "cvd": self.cvd,
            "cvd_slope": cvd_slope,
            "price_eff": price_eff,
            "buy_eff": buy_eff,
            "sell_eff": sell_eff,
            "afi_pos_ratio": sum(self.afi_hist) / len(self.afi_hist),
            "cvd_slope_pos_ratio": sum(self.cvd_slope_hist) / len(self.cvd_slope_hist),
        }

        self._normalize(metrics)
        return metrics

    def _normalize(self, metrics):
        for k, window in self.z_windows.items():
            v = metrics.get(k)
            if v is None:
                continue
            hist = self.z_hist[k]
            hist.append(v)
            if len(hist) >= max(5, window // 10):
                mu = sum(hist) / len(hist)
                sigma = math.sqrt(sum((x - mu) ** 2 for x in hist) / len(hist))
                metrics[f"z_{k}"] = (v - mu) / sigma if sigma > 0 else 0.0
            else:
                pass
