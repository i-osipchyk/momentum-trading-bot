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
COPY collect_l2.py .
COPY symbols.txt .

# Create directories for Parquet and logs
RUN mkdir -p /app/l2 /app/logs

# Environment variables
ENV SYMBOLS_FILE=symbols.txt
ENV SYMBOLS_SOURCE=s3
ENV SYMBOLS_BUCKET=crypto-tape-collector-symbols
ENV OUT_DIR=/app/l2
ENV S3_BUCKET=crypto-l2-collector-l2-data
ENV LOGS_BUCKET=crypto-l2-collector-logs
ENV AWS_REGION=eu-central-1
ENV PYTHONUNBUFFERED=1

# Run collector
CMD ["python", "collect_l2.py"]
