# Dockerfile for Medical Telegram Data Warehouse
# Python environment for data scraping, transformation, and API

FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw/images data/raw/telegram_messages logs

# Set permissions
RUN chmod +x scripts/*.sh 2>/dev/null || true

# Default command (can be overridden in docker-compose)
CMD ["python", "src/scraper.py"]
