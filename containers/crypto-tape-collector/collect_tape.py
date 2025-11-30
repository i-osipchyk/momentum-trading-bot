import os
import json
import boto3
import asyncio
import logging
import websockets
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta


# Default configurations
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
OUT_DIR = os.environ.get("OUT_DIR", "/app/tape")
S3_BUCKET = os.environ.get("S3_BUCKET", "my-crypto-tape")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
LOG_FILE = "/app/logs/collector.log"
SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE", "symbols.txt")
SYMBOLS_SOURCE = os.environ.get("SYMBOLS_SOURCE", "local")
SYMBOLS_BUCKET = os.environ.get("SYMBOLS_BUCKET", "my-symbols-bucket")


# Logging configuration
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)

fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logger.addHandler(sh)


# UTIL
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def load_symbols():
    """
    Load symbols from local file or S3 depending on SYMBOLS_SOURCE.
    """
    if SYMBOLS_SOURCE.lower() == "s3":
        s3 = boto3.client("s3", region_name=AWS_REGION)
        obj = s3.get_object(Bucket=SYMBOLS_BUCKET, Key=SYMBOLS_FILE)
        symbols = obj['Body'].read().decode('utf-8').splitlines()
        return [s.strip().lower() for s in symbols if s.strip()]
    else:
        with open(SYMBOLS_FILE, "r") as f:
            return [line.strip().lower() for line in f.readlines() if line.strip()]


# Single global buffer for all pairs
class HourlyTapeBuffer:
    def __init__(self, out_dir: str, s3_bucket: str, aws_region: str):
        self.current_hour = None
        self.records = []
        self.OUT_DIR = out_dir
        self.S3_BUCKET = s3_bucket
        self.AWS_REGION = aws_region
        self.s3 = boto3.client("s3", region_name=self.AWS_REGION)

    # Ensure directory exists
    def ensure_dir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    # Add a trade
    async def add(self, trade: dict):
        trade_hour = trade["trade_time"].replace(minute=0, second=0, microsecond=0)

        # First trade
        if self.current_hour is None:
            self.current_hour = trade_hour

        # Hour change detected → flush previous hour
        if trade_hour != self.current_hour:
            await self._flush_to_s3(self.current_hour, self.records, 'full')
            self.records = []
            self.current_hour = trade_hour

        # Add new trade to current hour
        self.records.append(trade)

    # Internal method: write parquet + upload S3
    async def _flush_to_s3(self, hour: datetime, records: list, mode: str):
        if not records:
            return

        # Copy records so we don't mutate buffer
        safe_records = []
        for r in records:
            r_copy = r.copy()
            if isinstance(r_copy["event_time"], datetime):
                r_copy["event_time"] = r_copy["event_time"].isoformat()
            if isinstance(r_copy["trade_time"], datetime):
                r_copy["trade_time"] = r_copy["trade_time"].isoformat()
            safe_records.append(r_copy)

        table = pa.Table.from_pylist(safe_records)

        # Local path
        year = hour.year
        month = f"{hour.month:02d}"
        day = f"{hour.day:02d}"
        hh = f"{hour.hour:02d}"
        path_dir = os.path.join(self.OUT_DIR, str(year), month, day)
        self.ensure_dir(path_dir)
        local_file = os.path.join(path_dir, f"{hh}.parquet")

        # Write parquet
        pq.write_table(table, local_file, compression="snappy")
        print(f"[WRITE] {local_file} ({table.num_rows} rows)")

        # Upload to S3
        timestamp = hour.strftime("%Y-%m-%d_%H:00:00")
        key = f"{year}/{month}/{day}/{hh}/{timestamp}_{mode}.parquet"

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self.s3.upload_file, local_file, self.S3_BUCKET, key)
            print(f"[S3] Uploaded {key}")
            os.remove(local_file)
        except Exception as e:
            print(f"[S3 ERROR] {local_file}: {e}")

    # Optional: flush remaining records (e.g., on shutdown)
    async def flush_remaining(self):
        if self.records:
            await self._flush_to_s3(self.current_hour, self.records, 'partial')
            self.records = []
            self.current_hour = None


# WebSocket connection and collection
async def connect(symbol: str):
    url = f"{BINANCE_FUTURES_WS}/{symbol}@aggTrade"
    logger.info(f"[CONNECT] {url}")
    return await websockets.connect(url, ping_interval=20, ping_timeout=20)


async def collect_symbol(symbol: str, buffer: HourlyTapeBuffer):
    while True:
        try:
            ws = await connect(symbol)
            async for msg in ws:
                d = json.loads(msg)
                trade = {
                    "event_time": datetime.fromtimestamp(d["E"]/1000, timezone.utc),
                    "trade_time": datetime.fromtimestamp(d["T"]/1000, timezone.utc),
                    "symbol": d["s"],
                    "price": float(d["p"]),
                    "qty": float(d["q"]),
                    "is_buyer_maker": d["m"],
                    "trade_id": d["a"],
                }
                await buffer.add(trade)

        except Exception as e:
            logger.warning(f"[DISCONNECT] {symbol} → reconnecting: {e}")
            await asyncio.sleep(2)

        finally:
            try:
                await ws.close()
            except:
                pass


async def main():
    symbols = load_symbols()
    buffer = HourlyTapeBuffer(
        out_dir=OUT_DIR,
        s3_bucket=S3_BUCKET,
        aws_region=AWS_REGION
    )

    tasks = []

    # Collector per symbol
    for sym in symbols:
        tasks.append(asyncio.create_task(collect_symbol(sym, buffer)))

    # Run collectors forever
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
