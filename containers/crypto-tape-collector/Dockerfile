FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y gcc g++ libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector script (no need to copy local symbols if reading from S3)
COPY collect_tape.py .

# Create directories for Parquet and logs
RUN mkdir -p /app/tape /app/logs

# Environment variables
ENV SYMBOLS_FILE=symbols.txt
ENV SYMBOLS_SOURCE=s3
ENV SYMBOLS_BUCKET=crypto-tape-collector-symbols
ENV OUT_DIR=/app/tape
ENV S3_BUCKET=crypto-tape-collector-tape-data
ENV LOGS_BUCKET=crypto-tape-collector-logs
ENV AWS_REGION=eu-central-1
ENV SAVE_INTERVAL_SEC=60
ENV UPLOAD_INTERVAL_SEC=3600
ENV PYTHONUNBUFFERED=1

# Run collector
CMD ["python", "collect_tape.py"]
