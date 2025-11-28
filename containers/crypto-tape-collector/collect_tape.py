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
    def __init__(self):
        self.records = []
        self.current_hour = None

    def add(self, trade: dict):
        trade_hour = trade["trade_time"].replace(minute=0, second=0, microsecond=0)

        if self.current_hour is None:
            self.current_hour = trade_hour

        # Still within same hour
        self.records.append(trade)

    def force_flush(self):
        if not self.records:
            return None, None
        hour = self.current_hour
        records = self.records
        self.records = []
        self.current_hour = None
        return hour, records


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
                buffer.add(trade)

        except Exception as e:
            logger.warning(f"[DISCONNECT] {symbol} → reconnecting: {e}")
            await asyncio.sleep(2)

        finally:
            try:
                await ws.close()
            except:
                pass


# Write to parquet locally
async def write_hour_file(hour: datetime, records: list):
    if not records:
        return

    # Convert datetime to ISO strings for parquet
    for r in records:
        r["event_time"] = r["event_time"].isoformat()
        r["trade_time"] = r["trade_time"].isoformat()

    table = pa.Table.from_pylist(records)

    year = hour.year
    month = f"{hour.month:02d}"
    day = f"{hour.day:02d}"
    hh = f"{hour.hour:02d}"

    path_dir = os.path.join(OUT_DIR, str(year), month, day)
    ensure_dir(path_dir)
    file_path = os.path.join(path_dir, f"{hh}.parquet")

    pq.write_table(table, file_path, compression="snappy")
    logger.info(f"[WRITE] {file_path} ({table.num_rows} rows)")


# Write to S3 and delete locally
async def upload_hour_to_s3(hour: datetime):
    s3 = boto3.client("s3", region_name=AWS_REGION)

    year = hour.year
    month = f"{hour.month:02d}"
    day = f"{hour.day:02d}"
    hh = f"{hour.hour:02d}"
    timestamp = hour.strftime("%Y-%m-%d_%H:00:00")

    local_file = os.path.join(OUT_DIR, str(year), month, day, f"{hh}.parquet")
    if not os.path.exists(local_file):
        return

    key = f"{year}/{month}/{day}/{hh}/{timestamp}.parquet"
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, s3.upload_file, local_file, S3_BUCKET, key)
        logger.info(f"[S3] Uploaded {key}")
        os.remove(local_file)
    except Exception as e:
        logger.error(f"[S3 ERROR] {local_file}: {e}")


# Alignemtn
async def hourly_alignment(buffer: HourlyTapeBuffer):
    while True:
        now = datetime.now(timezone.utc)
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_seconds = (next_hour - now).total_seconds()
        await asyncio.sleep(wait_seconds)

        old_hour, records = buffer.force_flush()
        if old_hour is not None and records:
            await write_hour_file(old_hour, records)
            await upload_hour_to_s3(old_hour)


async def main():
    symbols = load_symbols()
    buffer = HourlyTapeBuffer()

    tasks = []

    # Collector per symbol
    for sym in symbols:
        tasks.append(asyncio.create_task(collect_symbol(sym, buffer)))

    # Hourly alignment flush
    tasks.append(asyncio.create_task(hourly_alignment(buffer)))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
