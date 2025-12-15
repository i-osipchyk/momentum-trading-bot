import csv
import json
import asyncio
import aiohttp
import logging
import aiofiles
import traceback
import websockets
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta


# ====================== CONFIG =======================
KLINE_INTERVAL = "1m"
UP_PCT_THRESHOLD = 10
EMA_FAST = 8
EMA_SLOW = 20
MAX_KLINES = 21
AVG_VOL_WINDOW = 10
MIN_PULLBACK_DURATION = timedelta(minutes=30)

JSON_ALERTS_PATH = f'hod_breakout_with_pullback/alerts/hod_breakout_trades_{datetime.now(timezone.utc)}.json'.replace(" ", "_").replace(":", "-")
JSON_ALERTS_FILE = Path(JSON_ALERTS_PATH)
JSON_ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
JSON_ALERTS_FILE_LOCK = asyncio.Lock()

JSON_TRADES_PATH = f'hod_breakout_with_pullback/trades/hod_breakout_trades_{datetime.now(timezone.utc)}.json'.replace(" ", "_").replace(":", "-")
JSON_TRADES_FILE = Path(JSON_TRADES_PATH)
JSON_TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
JSON_TRADES_FILE_LOCK = asyncio.Lock()

SYMBOLS_FILE = 'hod_breakout_with_pullback/symbols_full.txt'

with open(SYMBOLS_FILE, "r") as f:
    SYMBOLS = [line.strip().lower() for line in f.readlines() if line.strip()]


# ====================== LOGGING =======================
LOGS_FOLDER_PATH = 'hod_breakout_with_pullback/logs/'
Path(LOGS_FOLDER_PATH).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=f"{LOGS_FOLDER_PATH}logs_{datetime.now(timezone.utc)}.log".replace(" ", "_").replace(":", "-"),
    filemode="a",                      # append
    level=logging.INFO,                # log everything >= INFO
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("TradingBot")
logger.setLevel(logging.INFO)

# show logs in terminal too
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


# ====================== SYMBOL TRACKER =======================
class SymbolTracker:
    def __init__(self, symbol):
        self.symbol = symbol
        self.open_price = None
        self.threshold_price = None
        self.last_open_date = None
        self.hod = float("-inf")
        self.hod_datetime = None
        self.klines_df = pd.DataFrame(
            columns=["t", "open", "high", "low", "close", "volume_usdt", f"EMA_{EMA_FAST}", f"EMA_{EMA_SLOW}"]
        )
        self.in_trade = False
        self.trade = {}
        self.relative_volume = 0 # testing

    async def init_open_price(self, session):
        url = (
            f"https://fapi.binance.com/fapi/v1/klines"
            f"?symbol={self.symbol.upper()}&interval=1d&limit=1"
        )

        async with session.get(url) as resp:
            data = await resp.json()

            if not isinstance(data, list) or len(data) == 0:
                logger.error(f"Failed to fetch open price for {self.symbol}. Response: {data}")
                return

            try:
                kline = data[0]

                open_time_ms = int(kline[0])
                open_price = float(kline[1])
                high_price = float(kline[2])

                open_datetime = pd.to_datetime(open_time_ms, unit="ms")

                self.open_price = open_price
                self.threshold_price = open_price * (1 + UP_PCT_THRESHOLD / 100)
                self.last_open_date = open_datetime.date()

                # ✅ Initialize HOD state
                self.hod = high_price
                self.hod_datetime = pd.Timestamp.utcnow()

                logger.info(
                    f"{self.symbol}: Initialized open={self.open_price}, "
                    f"HOD={self.hod} at {self.hod_datetime}"
                )

            except (IndexError, KeyError, ValueError, TypeError) as e:
                logger.error(
                    f"Error parsing daily kline for {self.symbol}: {e} | Response: {data}"
                )

    # ----- TRADE LOGIC -----
    async def update_from_kline(self, candle):
        """
        Updates rolling 1m klines dataframe (last 21 rows),
        calculates volume in USDT and EMA FAST / EMA SLOW on closed candles.
        """

        # Get values from kline update
        open_time = int(candle["t"])
        is_closed = candle["x"]
        trade_datetime = pd.to_datetime(open_time, unit="ms", utc=True)

        row = {
            "t": open_time,
            "open": float(candle["o"]),
            "high": float(candle["h"]),
            "low": float(candle["l"]),
            "close": float(candle["c"]),
            "volume_usdt": float(candle["q"])  # quote volume (USDT)
        }

        # Reset values in new day is detected
        if trade_datetime.date() > self.last_open_date:
            self.open_price = float(candle["o"])
            self.threshold_price = self.open_price * (1 + UP_PCT_THRESHOLD / 100)
            self.hod = float("-inf")
            self.hod_datetime = None
            self.last_open_date = trade_datetime.date()
            logger.info(f"{self.symbol}: New day detected. Reset open price to {self.open_price} and HOD.")

        # Update klines dataframe
        if self.klines_df.empty:
            self.klines_df = pd.DataFrame([row])
        else:
            last_t = self.klines_df.iloc[-1]["t"]

            if open_time > last_t:
                self.klines_df = pd.concat(
                    [self.klines_df, pd.DataFrame([row])],
                    ignore_index=True
                )
            else:
                for col, val in row.items():
                    self.klines_df.at[self.klines_df.index[-1], col] = val

        self.klines_df = self.klines_df.tail(21).reset_index(drop=True)

        # Calculate indicators
        # if is_closed and len(self.klines_df) >= EMA_SLOW:
        if len(self.klines_df) >= EMA_SLOW:
            self.klines_df[f"EMA_{EMA_FAST}"] = (
                self.klines_df["close"]
                .ewm(span=EMA_FAST, adjust=False)
                .mean()
            )

            self.klines_df[f"EMA_{EMA_SLOW}"] = (
                self.klines_df["close"]
                .ewm(span=EMA_SLOW, adjust=False)
                .mean()
            )

            self.klines_df["AVG_VOL"] = self.klines_df['volume_usdt'].shift(1).rolling(AVG_VOL_WINDOW).mean()

            self.ema_fast = self.klines_df[f"EMA_{EMA_FAST}"].iloc[-1]
            self.ema_slow = self.klines_df[f"EMA_{EMA_SLOW}"].iloc[-1]
            self.average_volume = self.klines_df["AVG_VOL"].iloc[-1]

        # Return if trade values are inconsistent
        if (self.open_price is None or self.threshold_price is None or 
            self.last_open_date is None or self.hod is None):
            return
        
        # Update hod, and potentially open trade
        if row['high'] > self.hod:
            if (row['high'] > self.threshold_price and self.hod_datetime != None and 
                trade_datetime > self.hod_datetime + MIN_PULLBACK_DURATION and not self.in_trade):
                pullback_duration = trade_datetime - self.hod_datetime
                await self.open_trade(trade_datetime, row['high'], row['low'], pullback_duration)

            self.hod = row['high']
            self.hod_datetime = trade_datetime

        # Evaluate trade if opened
        if self.in_trade:
            await self.evaluate_trade(row['high'], row['low'], row['close'], trade_datetime)

    async def open_trade(self, entry_time, entry_price, stop_loss, pullback_duration):
        self.in_trade = True
        self.trade = {
            'symbol': self.symbol,
            'entry_time': entry_time.isoformat(),
            'entry_price': entry_price,
            # 'stop_loss': stop_loss,
            'pullback_duration': int(pullback_duration.total_seconds()),
            'average_volume': self.average_volume if self.average_volume else None,
            'trade_high': entry_price,
            'trade_low': entry_price
        }
        logger.info(f"Opened trade: {self.trade}")

    async def evaluate_trade(self, curr_high, curr_low, curr_close, curr_datetime):
        if curr_high > self.trade.get('trade_high', 0):
            self.trade['trade_high'] = curr_high
        if curr_low < self.trade.get('trage_low', np.inf):
            self.trade['trade_low'] = curr_low
        # if curr_low < self.trade.get('stop_loss'):
        #     self.trade['exit'] = curr_low
        #     self.trade['exit_time'] = curr_datetime.isoformat()
        #     self.trade['exit_reason'] = 'sl'
        #     self.in_trade = False
        #     await self.write_trade()
        if self.ema_fast < self.ema_slow:
            self.trade['exit'] = curr_close
            self.trade['exit_time'] = curr_datetime.isoformat()
            self.trade['exit_reason'] = 'cross'
            self.in_trade = False
            await self.write_trade()

    async def write_trade(self):
        async with JSON_TRADES_FILE_LOCK:
            if JSON_TRADES_FILE.exists():
                async with aiofiles.open(JSON_TRADES_FILE, "r") as f:
                    try:
                        content = await f.read()
                        data = json.loads(content) if content else []
                    except json.JSONDecodeError:
                        data = []
            else:
                data = []

            data.append(self.trade)

            async with aiofiles.open(JSON_TRADES_FILE, "w") as f:
                await f.write(json.dumps(data, indent=4))

        self.trade = {}

    # ----- ALERT LOGIC -----
    async def update_from_kline_alert(self, candle):
        price = float(candle["h"])
        timestamp = int(candle["t"])
        trade_datetime = pd.to_datetime(timestamp, unit='ms')

        if (self.open_price is None or self.threshold_price is None or 
           self.last_open_date is None or self.hod is None):
            return

        if price > self.hod:
            if (price > self.threshold_price and self.hod_datetime != None and 
                trade_datetime > self.hod_datetime + MIN_PULLBACK_DURATION):
                await self.send_alert()

            self.hod = price
            self.hod_datetime = trade_datetime

        if trade_datetime.date() > self.last_open_date:
            self.open_price = float(candle["o"])
            self.threshold_price = self.open_price * (1 + UP_PCT_THRESHOLD / 100)
            self.hod = float("-inf")
            self.hod_datetime = None
            self.last_open_date = trade_datetime.date()
            logger.info(f"{self.symbol}: New day detected. Reset open price to {self.open_price} and HOD.")

    async def send_alert(self):
        alert = {
            "symbol": self.symbol,
            "time": self.hod_datetime.isoformat(),
            "hod": self.hod,
            "open_price": self.open_price,
        }

        async with JSON_ALERTS_FILE_LOCK:
            if JSON_ALERTS_FILE.exists():
                async with aiofiles.open(JSON_ALERTS_FILE, "r") as f:
                    try:
                        content = await f.read()
                        data = json.loads(content) if content else []
                    except json.JSONDecodeError:
                        data = []
            else:
                data = []

            data.append(alert)

            async with aiofiles.open(JSON_ALERTS_FILE, "w") as f:
                await f.write(json.dumps(data, indent=4))

        logger.info(f"Breakout with Pullback Alert: {alert}")

async def collect_symbol_data(symbol, tracker: SymbolTracker):
    """
    Connects to Binance WebSocket for a single symbol and calls tracker update methods
    """
    streams = f"{symbol.lower()}@kline_{KLINE_INTERVAL}"
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    while True:
        try:
            logger.info(f"Connecting → {symbol} streams...")
            async with websockets.connect(url) as ws:
                logger.info(f"{symbol.upper()} connected!")

                async for msg in ws:
                    data = json.loads(msg)
                    payload = data["data"]

                    # ---- KLINE ----
                    await tracker.update_from_kline(payload["k"])

        except (websockets.ConnectionClosed, aiohttp.ClientError) as e:
            logger.warning(f"{symbol.upper()} WebSocket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

        except Exception as e:
            msg = str(e)

            if "float" in msg and "NoneType" in msg and ">" in msg:
                logger.error(
                    f"{symbol.upper()} unexpected error:\n{traceback.format_exc()}"
                )
            else:
                logger.error(
                    f"{symbol.upper()} unexpected error: {e}. Reconnecting in 5s..."
                )

            await asyncio.sleep(5)


async def run_all_symbols(symbols):
    trackers = {}
    async with aiohttp.ClientSession() as session:
        for sym in symbols:
            tracker = SymbolTracker(sym)
            await tracker.init_open_price(session)
            trackers[sym] = tracker

    tasks = [asyncio.create_task(collect_symbol_data(sym, trackers[sym])) for sym in symbols]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run_all_symbols(SYMBOLS))
