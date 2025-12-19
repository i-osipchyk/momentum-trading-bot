import time
import math
from collections import deque
from dataclasses import dataclass


@dataclass
class L2Bucket:
    """
    Contains aggregated information about volume price in one second.
    """
    ts: int
    bid_liq: float = 0.0
    ask_liq: float = 0.0
    weighted_bid_liq: float = 0.0
    weighted_ask_liq: float = 0.0


class EMA:
    """
    Used to calculate EMA for different varialbles with configurable window.
    """
    def __init__(self, alpha: float):
        self.alpha = alpha
        self.value = None

    def update(self, x):
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


class L2Aggregator:
    """
    Aggregates data that comes from tape into metrics by seconds.

    Metrics:
        - OBI(Order Book Imbalance): bid and ask difference divided by total liquidity. EW is used for decision making.
        - Bid/Ask Delta: Difference between bid/ask in t[0] and t[-1].
        - Weighted OBI: weighted bid and ask difference divided by total liquidity. EW is used for decision making.
        - OBI Pos Ratio(OBI Positive Ratio): ratio of positive OBI in the last n seconds.
        - Bid Liq Increasing Ratio(Bid Liquidity Increasing Ratio): ratio of increasing bid liquidity in the last n seconds.
        - Ask Liq Decreasing Ratio(Bid Liquidity Decreasing Ratio): ratio of decreasing ask liquidity in the last n seconds.
    """
    def __init__(self, levels_used=10):
        self.levels_used = levels_used
        self.current_bucket = None

        self.prev_bid_liq = None
        self.prev_ask_liq = None

        self.ew_obi = EMA(0.2)

        self.obi_hist = deque(maxlen=20)
        self.ask_liq_delta_hist = deque(maxlen=20)
        self.bid_liq_delta_hist = deque(maxlen=20)

        # normalization windows (seconds)
        self.z_windows = {
            "bid_liq": 300,
            "ask_liq": 300,
            "weighted_obi": 300,
            "bid_liq_delta": 180,
            "ask_liq_delta": 180,
        }

        self.z_hist = {k: deque(maxlen=v) for k, v in self.z_windows.items()}

    def update_l2(self, bids, asks, ts=None):
        # get time timestamp in seconds from the l2 snapshot
        ts = ts if ts is not None else int(time.time())
        finalized = None

        if self.current_bucket is None or ts != self.current_bucket.ts:
            if self.current_bucket:
                # finalize bucket if new timestamp detected
                finalized = self._finalize_bucket(self.current_bucket)
            # create new bucket
            self.current_bucket = L2Bucket(ts=ts)

        # return in snapshot is empty
        if not bids or not asks:
            return finalized

        # define mid price
        mid = (bids[0][0] + asks[0][0]) / 2

        # aupdate liquidity
        self.current_bucket.bid_liq = sum(q for _, q in bids[:self.levels_used])
        self.current_bucket.ask_liq = sum(q for _, q in asks[:self.levels_used])

        # update weighted liquidity
        self.current_bucket.weighted_bid_liq = sum(
            q / max(abs(mid - p), mid * 1e-4) for p, q in bids[:self.levels_used]
        )
        self.current_bucket.weighted_ask_liq = sum(
            q / max(abs(p - mid), mid * 1e-4) for p, q in asks[:self.levels_used]
        )

        return finalized

    def _finalize_bucket(self, b: L2Bucket):
        bid = b.bid_liq
        ask = b.ask_liq
        total = bid + ask

        obi = (bid - ask) / total if total > 0 else 0.0
        ew_obi_val = self.ew_obi.update(obi)

        bid_delta = 0.0 if self.prev_bid_liq is None else (bid - self.prev_bid_liq) / self.prev_bid_liq
        ask_delta = 0.0 if self.prev_ask_liq is None else (ask - self.prev_ask_liq) / self.prev_ask_liq

        self.prev_bid_liq = bid
        self.prev_ask_liq = ask

        w_total = b.weighted_bid_liq + b.weighted_ask_liq
        weighted_obi = (
            (b.weighted_bid_liq - b.weighted_ask_liq) / w_total if w_total > 0 else 0.0
        )

        self.obi_hist.append(obi > 0)
        self.bid_liq_delta_hist.append(bid_delta > 0)
        self.ask_liq_delta_hist.append(ask_delta < 0)

        metrics = {
            "ts": b.ts,
            "bid_liq": bid,
            "ask_liq": ask,
            "obi": obi,
            "ew_obi": ew_obi_val,
            "bid_liq_delta": bid_delta,
            "ask_liq_delta": ask_delta,
            "weighted_obi": weighted_obi,
            "obi_pos_ratio": sum(self.obi_hist) / len(self.obi_hist),
            "bid_liq_increasing_ratio": sum(self.bid_liq_delta_hist) / len(self.bid_liq_delta_hist),
            "ask_liq_decreasing_ratio": sum(self.ask_liq_delta_hist) / len(self.ask_liq_delta_hist),
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
