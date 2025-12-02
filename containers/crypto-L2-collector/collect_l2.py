import os
import json
import boto3
import asyncio
import logging
import websockets
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta


BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
OUT_DIR = os.environ.get("OUT_DIR", "/app/l2")
S3_BUCKET = os.environ.get("S3_BUCKET", "crypto-tape-collector-symbols")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
LOG_FILE = "/app/logs/collector.log"
SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE", "symbols.txt")

# Logging
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logger.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logger.addHandler(sh)


def load_symbols():
    with open(SYMBOLS_FILE, "r") as f:
        return [line.strip().lower() for line in f.readlines() if line.strip()]


class HourlyL2Buffer:
    def __init__(self, out_dir: str, s3_bucket: str, aws_region: str):
        self.records = []
        self.OUT_DIR = out_dir
        self.S3_BUCKET = s3_bucket
        self.AWS_REGION = aws_region
        self.s3 = boto3.client("s3", region_name=aws_region)

    async def add(self, entry: dict):
        self.records.append(entry)

    async def quarter_hour_flush_task(self):
        while True:
            now = datetime.now(timezone.utc)
            minute = (now.minute // 15) * 15
            next_flush = (now.replace(minute=minute, second=0, microsecond=0)
                          + timedelta(minutes=15))

            await asyncio.sleep((next_flush - now).total_seconds())
            await self._flush(next_flush - timedelta(minutes=15))

    async def _flush(self, block_start: datetime):
        records = self.records.copy()
        self.records = []
        if not records:
            return

        try:
            table = pa.Table.from_pylist(records)
        except:
            logger.exception("[PARQUET ERROR] Failed creating PyArrow table")
            return

        year = block_start.year
        month = f"{block_start.month:02d}"
        day = f"{block_start.day:02d}"
        hour = f"{block_start.hour:02d}"
        minute = f"{block_start.minute:02d}"

        # local path
        dir_path = os.path.join(self.OUT_DIR, str(year), month, day, hour)
        os.makedirs(dir_path, exist_ok=True)

        timestamp = block_start.strftime("%Y-%m-%d_%H:%M:00")
        local_file = os.path.join(dir_path, f"{timestamp}.parquet")

        try:
            pq.write_table(table, local_file, compression="snappy")
        except:
            logger.exception(f"[WRITE ERROR] Failed writing parquet {local_file}")
            return

        key = f"{year}/{month}/{day}/{hour}/{timestamp}.parquet"
        loop = asyncio.get_running_loop()

        try:
            await loop.run_in_executor(None, self.s3.upload_file, local_file, self.S3_BUCKET, key)
            os.remove(local_file)
            logger.info(f"[S3] Uploaded {key}")
        except:
            logger.exception("[S3 ERROR] Failed uploading")


async def connect(symbol: str):
    url = f"{BINANCE_FUTURES_WS}/{symbol}@depth@100ms"
    logger.info(f"[CONNECT] {url}")
    return await websockets.connect(url, ping_interval=20, ping_timeout=20)


async def collect_depth(symbol: str, buffer: HourlyL2Buffer):
    """
    Collects market depth snapshots every 0.5 seconds.
    """
    while True:
        try:
            ws = await connect(symbol)

            last_update = {}

            async for msg in ws:
                d = json.loads(msg)

                # Skip invalid updates
                if "b" not in d or "a" not in d:
                    continue

                last_update["bids"] = d["b"]
                last_update["asks"] = d["a"]
                last_update["event_time"] = datetime.fromtimestamp(d["E"] / 1000, timezone.utc)

                # Snapshot every 0.5 seconds
                await asyncio.sleep(0.5)

                snapshot = {
                    "symbol": symbol,
                    "event_time": last_update["event_time"],
                    "bids": last_update["bids"],
                    "asks": last_update["asks"]
                }

                await buffer.add(snapshot)

        except:
            logger.exception(f"[ERROR] Depth collector crashed for {symbol}")
            await asyncio.sleep(2)

        finally:
            try:
                await ws.close()
            except:
                pass


async def main():
    symbols = load_symbols()
    buffer = HourlyL2Buffer(OUT_DIR, S3_BUCKET, AWS_REGION)

    tasks = [asyncio.create_task(buffer.quarter_hour_flush_task())]
    for sym in symbols:
        tasks.append(asyncio.create_task(collect_depth(sym, buffer)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
