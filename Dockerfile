FROM python:3.11-slim

# Build-time metadata
LABEL maintainer="music-enricher"
LABEL description="Music metadata harvesting pipeline"

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cache optimization)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Create export directory
RUN mkdir -p /data/exports

# Default entry point — WORKER_TYPE env var selects the worker
ENTRYPOINT ["python", "-m", "app.entrypoint"]
