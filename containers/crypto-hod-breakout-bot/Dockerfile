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
COPY crypto-hod-breakout-bot ./crypto-hod-breakout-bot

# Ensure required directories exist
RUN mkdir -p \
    /app/rypto-hod-breakout-bot/logs \
    /app/rypto-hod-breakout-bot/alerts \
    /app/rypto-hod-breakout-bot/trades

# Environment
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC

# Run
CMD ["python", "hod_with_pullback_bot.py"]
