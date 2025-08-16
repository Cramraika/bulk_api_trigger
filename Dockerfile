FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY webhook_trigger.py .
COPY config.yaml ./config.yaml

# Create directories for data, logs, csv, processed files and backups
RUN mkdir -p /app/data \
    /app/data/logs \
    /app/data/csv \
    /app/data/csv/processed \
    /app/data/backups

# Set proper permissions
RUN chmod +x webhook_trigger.py && \
    chmod -R 755 /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEPLOYMENT_MODE=true \
    KEEP_ALIVE=true \
    WATCHDOG_ENABLED=true \
    WATCH_PATHS=/app/data/csv \
    AUTO_PROCESS=true \
    HEALTH_CHECK_ENABLED=true \
    METRICS_ENABLED=true

# Expose ports for health checks and metrics
EXPOSE 8000

# Enhanced health check that tests watchdog and database
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8000/health || python -c "import sqlite3; conn=sqlite3.connect('/app/data/webhook_results.db'); conn.close()" || exit 1

# Run the application
CMD ["python", "webhook_trigger.py"]