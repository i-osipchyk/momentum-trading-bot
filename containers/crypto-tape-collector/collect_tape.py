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
        self.records.append(trade)

    # Background task: flush all trades every hour at HH:00:00
    async def quarter_hour_flush_task(self):
        while True:
            now = datetime.now(timezone.utc)

            # Round up to next quarter-hour
            minute = (now.minute // 15) * 15
            next_flush = (now.replace(minute=minute, second=0, microsecond=0)
                            + timedelta(minutes=15))

            sleep_seconds = (next_flush - now).total_seconds()
            await asyncio.sleep(sleep_seconds)

            # Flush for the previous 15-minute block
            flush_start = next_flush - timedelta(minutes=15)
            await self._flush_to_s3(flush_start)

    # Internal flush method (can reuse your existing flush_to_s3)
    async def _flush_to_s3(self, block_start: datetime):
        records_to_flush = self.records.copy()
        self.records = []

        if not records_to_flush:
            return

        # Create PyArrow table
        try:
            table = pa.Table.from_pylist(records_to_flush)
        except Exception:
            logger.exception("[PARQUET ERROR] Failed to create table")
            return

        # Path components
        year = block_start.year
        month = f"{block_start.month:02d}"
        day = f"{block_start.day:02d}"
        hh = f"{block_start.hour:02d}"
        mm = f"{block_start.minute:02d}"

        # Local directory: year/month/day/hour/
        path_dir = os.path.join(self.OUT_DIR, str(year), month, day, hh)
        self.ensure_dir(path_dir)

        # File name: yyyy-mm-dd_HH:MM:00.parquet
        timestamp = block_start.strftime("%Y-%m-%d_%H:%M:00")
        local_file = os.path.join(path_dir, f"{timestamp}.parquet")

        # Write parquet
        try:
            pq.write_table(table, local_file, compression="snappy")
        except Exception:
            logger.exception(f"[WRITE ERROR] Failed writing parquet {local_file}")
            return
        
        logger.info(f"[WRITE] {local_file} ({table.num_rows} rows)")
        key = f"{year}/{month}/{day}/{hh}/{timestamp}.parquet"

        # Upload to S3
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self.s3.upload_file, local_file, self.S3_BUCKET, key)
            logger.info(f"[S3] Uploaded {key}")
            os.remove(local_file)
        except Exception as e:
            logger.exception(f"[S3 ERROR] Upload failed for {local_file}")


# WebSocket connection and collection
async def connect(symbol: str):
    url = f"{BINANCE_FUTURES_WS}/{symbol}@aggTrade"
    logger.info(f"[CONNECT] {url}")
    try:
        return await websockets.connect(url, ping_interval=20, ping_timeout=20)
    except Exception:
        logger.exception(f"[WS ERROR] Failed to connect to {symbol}")
        raise

async def collect_symbol(symbol: str, buffer: HourlyTapeBuffer):
    while True:
        try:
            ws = await connect(symbol)
            async for msg in ws:
                try:
                    d = json.loads(msg)
                except Exception:
                    logger.exception(f"[JSON ERROR] Could not decode message: {msg}")
                    continue

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
            logger.exception(f"[ERROR] Collector crashed for {symbol}")
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

    tasks = [asyncio.create_task(buffer.quarter_hour_flush_task())]

    # Collector per symbol
    for sym in symbols:
        tasks.append(asyncio.create_task(collect_symbol(sym, buffer)))

    # Run collectors forever
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
