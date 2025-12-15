FROM python:3.10-slim

# System deps for numpy / pandas
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Upgrade pip
RUN pip install --upgrade pip

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY hod_with_pullback_bot.py .
COPY . .

# Ensure required directories exist
RUN mkdir -p \
    /app/crypto-hod-breakout-bot/logs \
    /app/crypto-hod-breakout-bot/alerts \
    /app/crypto-hod-breakout-bot/trades

# Environment
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC

# Run
CMD ["python", "hod_with_pullback_bot.py"]
