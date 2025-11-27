import os
import glob
import json
import boto3
import signal
import asyncio
import logging
import websockets
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone


# Configuration
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
OUT_DIR = os.environ.get("OUT_DIR", "/app/tape")
S3_BUCKET = os.environ.get("S3_BUCKET", "my-crypto-tape")
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
UPLOAD_INTERVAL_SEC = 3600  # hourly upload
ACTIVE_STREAM_LOG_INTERVAL = 300  # log active streams every 5 min
SYMBOLS_SOURCE = os.environ.get("SYMBOLS_SOURCE", "local")  # "local" or "s3"
SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE", "symbols.txt")
SYMBOLS_BUCKET = os.environ.get("SYMBOLS_BUCKET", "my-symbols-bucket")
LOGS_BUCKET = os.environ.get("LOGS_BUCKET", "my-logs-bucket")
LOG_FILE = "/app/logs/collector.log"


def ensure_path(path):
    """Ensure a directory exists."""
    if not os.path.exists(path):
        os.makedirs(path)


# Logging
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)
logger.propagate = False  # Prevent double logging if root logger exists

console_handler = logging.StreamHandler()
console_formatter = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_FILE)
file_formatter = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


# Utility Functions
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


# Tape Buffer
class TapeBuffer:
    """Stores trades before flushing to Parquet."""
    def __init__(self):
        self.records = []

    def add(self, trade: dict):
        self.records.append(trade)

    def flush(self):
        if not self.records:
            return None
        table = pa.Table.from_pylist(self.records)
        self.records = []
        return table


# Collector
async def connect_ws(symbol: str):
    url = f"{BINANCE_FUTURES_WS}/{symbol}@aggTrade"
    logger.info(f"[WS] Connecting to {url}")
    return await websockets.connect(url, ping_interval=20, ping_timeout=20)

async def collect_symbol(symbol: str, active_streams: set, buffer: TapeBuffer):
    """Collect trades for a symbol and save parquet files minute-aligned."""
    active_streams.add(symbol)
    last_minute = None
    try:
        while True:
            try:
                ws = await connect_ws(symbol)
                async for msg in ws:
                    data = json.loads(msg)
                    trade = {
                        "event_time": datetime.fromtimestamp(data["E"] / 1000.0, timezone.utc),
                        "trade_time": datetime.fromtimestamp(data["T"] / 1000.0, timezone.utc),
                        "symbol": data["s"],
                        "price": float(data["p"]),
                        "qty": float(data["q"]),
                        "is_buyer_maker": data["m"],
                        "trade_id": data["a"]
                    }
                    buffer.add(trade)

                    # Minute-aligned write
                    now = datetime.now(timezone.utc)
                    current_minute = now.minute
                    if current_minute != last_minute:
                        await save_parquet(buffer, symbol, now)
                        last_minute = current_minute

            except Exception as e:
                logger.warning(f"[DISCONNECT] {symbol} disconnected, reconnecting in 3s → {e}")
                await asyncio.sleep(3)
    finally:
        active_streams.remove(symbol)


# Parquet Writer
async def save_parquet(buffer: TapeBuffer, symbol: str, timestamp: datetime):
    """Flush buffer to Parquet partitioned by year/month/day/symbol."""
    table = buffer.flush()
    if table is None:
        return

    year = timestamp.year
    month = f"{timestamp.month:02d}"
    day = f"{timestamp.day:02d}"

    base_path = os.path.join(OUT_DIR, str(year), month, day, symbol)
    ensure_path(base_path)

    filename = os.path.join(base_path, f"tape_{timestamp.strftime('%H%M%S')}.parquet")
    logger.debug(f"{symbol} → {filename} ({table.num_rows} rows)")

    pq.write_table(table, filename, compression="snappy")


# S3 Upload
async def upload_to_s3():
    """Upload Parquet files to S3 hourly."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    while True:
        logger.info("[S3] Starting hourly upload...")
        uploaded_symbols = set()
        for filepath in glob.glob(f"{OUT_DIR}/**/*.parquet", recursive=True):
            s3_key = os.path.relpath(filepath, OUT_DIR)
            symbol = os.path.basename(os.path.dirname(filepath))
            try:
                s3.upload_file(filepath, S3_BUCKET, s3_key)
                uploaded_symbols.add(symbol)
                os.remove(filepath)
            except Exception as e:
                logger.error(f"[S3 ERROR] Failed to upload {filepath}: {e}")

        if uploaded_symbols:
            logger.info(f"[S3] Uploaded files for {len(uploaded_symbols)} symbols: {', '.join(sorted(uploaded_symbols))}")
        else:
            logger.info("[S3] No new files to upload")

        await asyncio.sleep(UPLOAD_INTERVAL_SEC)


async def upload_logs_to_s3():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    while True:
        if os.path.exists(LOG_FILE):
            try:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                s3_key = f"collector_{timestamp}.log"
                s3.upload_file(LOG_FILE, LOGS_BUCKET, s3_key)
            except Exception as e:
                logger.error(f"[LOGS S3 ERROR] Failed to upload logs: {e}")
        await asyncio.sleep(UPLOAD_INTERVAL_SEC)


# Active Streams Logger
async def log_active_streams(active_streams: set):
    while True:
        logger.info(f"Currently active streams: {len(active_streams)}")
        await asyncio.sleep(ACTIVE_STREAM_LOG_INTERVAL)


# Main
async def main():
    symbols = load_symbols()
    active_streams = set()
    buffers = {}

    # Create collector tasks
    tasks = []
    for symbol in symbols:
        buffer = TapeBuffer()
        buffers[symbol] = buffer
        task = asyncio.create_task(collect_symbol(symbol, active_streams, buffer))
        tasks.append(task)

    # S3 uploader & active stream logger
    tasks.append(asyncio.create_task(upload_to_s3()))
    tasks.append(asyncio.create_task(upload_logs_to_s3()))
    tasks.append(asyncio.create_task(log_active_streams(active_streams)))
    

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def shutdown():
        logger.info("[CTRL+C] Stopping gracefully…")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    await stop_event.wait()

    # Cancel tasks
    for t in tasks:
        t.cancel()

    # Flush remaining buffers
    logger.info("Flushing remaining buffers...")
    for symbol, buffer in buffers.items():
        await save_parquet(buffer, symbol, datetime.now(timezone.utc))

    logger.info("[EXIT] All tasks stopped.")


# Entry Point
if __name__ == "__main__":
    asyncio.run(main())
