FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create directories for data and logs
RUN mkdir -p /app/data /app/data/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DEPLOYMENT_MODE=true
ENV KEEP_ALIVE=true

# Expose port for health checks (optional)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sqlite3; conn=sqlite3.connect('/data/webhook_results.db'); conn.close()" || exit 1

# Run the application
CMD ["python", "webhook_trigger.py"]