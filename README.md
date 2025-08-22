# 🚀 Enhanced Bulk API Trigger Platform v2.0

Production-ready platform for triggering thousands of webhooks/APIs with advanced features including REST API endpoints, resume support, file watchdog, auto-processing, comprehensive notifications, and real-time monitoring.

## ✨ Key Features

### Core Capabilities
- **🔥 Bulk Processing**: Handle thousands of API calls efficiently with thread pooling
- **🔄 Resume Support**: Automatic checkpoint saving and resume from last position
- **🌐 REST API**: Full-featured webhook endpoints for remote triggering
- **👀 Smart File Watching**: Automatic detection and processing of new CSV files
- **⚡ Dynamic Rate Limiting**: Intelligent rate adjustment based on success/error rates
- **🔁 Smart Retry Logic**: Exponential backoff with Retry-After header support
- **📊 SQLite Database**: Complete job history and request tracking
- **📈 Health Monitoring**: Built-in HTTP server with comprehensive endpoints

### Enterprise Features
- **🔔 Notifications**: Email and Slack integration with rich formatting
- **🔐 API Authentication**: Optional Bearer token authentication
- **⚖️ Rate Limiting**: Per-IP rate limiting for API endpoints
- **🛡️ Deduplication**: File hash-based duplicate detection
- **📁 File Management**: Automatic archiving and rejection handling
- **💾 Database Backups**: Automatic scheduled backups
- **🐳 Cloud Ready**: Docker support with health checks

## 🚀 Quick Start

### Docker Deployment (Recommended)

```bash
# Clone repository
git clone <your-repo-url>
cd bulk-api-trigger

# Create directory structure
mkdir -p data/{csv/{processed,duplicates,rejected},reports,logs,backups}

# Configure environment
cp .env.sample .env
# Edit .env with your settings

# Start with Docker Compose
docker-compose up -d

# Check logs
docker-compose logs -f bulk-api-trigger
```

### Local Development

```bash
pip install -r requirements.txt
python webhook_trigger.py --create-config
python webhook_trigger.py --interactive
```

## 📂 Directory Structure

```
/app/
├── data/
│   ├── csv/                    # 📁 Incoming CSV files (watched)
│   │   ├── processed/          # ✅ Successfully processed files
│   │   ├── duplicates/         # 🔄 Duplicate files (by hash)
│   │   └── rejected/           # ❌ Invalid files
│   ├── reports/                # 📊 JSON job reports & resume markers
│   ├── logs/                   # 📝 Application logs
│   ├── backups/                # 💾 Database backups
│   └── webhook_results.db      # 🗄️ SQLite job database
└── webhook_trigger.py          # 🎯 Main application
```

## 🌐 REST API Endpoints

### Webhook Triggering

```bash
# Trigger single CSV file
curl -X POST http://localhost:8000/trigger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "csv_file": "/app/data/csv/webhooks.csv",
    "job_name": "API Triggered Job",
    "resume": true,
    "force_restart": false
  }'

# Batch trigger multiple files
curl -X POST http://localhost:8000/trigger/batch \
  -H "Content-Type: application/json" \
  -d '{
    "csv_files": [
      "/app/data/csv/file1.csv",
      "/app/data/csv/file2.csv"
    ]
  }'
```

### Resume Management

```bash
# Check resume status
curl -X POST http://localhost:8000/resume/status \
  -d '{"csv_file": "/app/data/csv/webhooks.csv"}'

# Clear resume marker
curl -X POST http://localhost:8000/resume/clear \
  -d '{"csv_file": "/app/data/csv/webhooks.csv"}'

# View all resume markers
curl http://localhost:8000/resume/stats
```

### Monitoring

```bash
# System health
curl http://localhost:8000/health

# Active jobs status
curl http://localhost:8000/status

# Job history
curl http://localhost:8000/jobs

# Specific job details
curl http://localhost:8000/jobs/{job_id}

# Job errors
curl http://localhost:8000/jobs/{job_id}/errors

# System metrics
curl http://localhost:8000/metrics
```

## 📊 CSV File Format

### Required Column
- `webhook_url` - Target URL for the HTTP request

### Optional Columns

| Column | Description | Example |
|--------|-------------|---------|
| `method` | HTTP method | POST, GET, PUT |
| `payload` | Request body (JSON) | `{"key": "value"}` |
| `header` or `headers` | Custom headers (JSON) | `{"Authorization": "Bearer token"}` |
| `name` | Friendly identifier | User Registration Hook |
| `group` | Category/grouping | notifications |

### Example CSV

```csv
webhook_url,method,payload,header,name,group
https://api.example.com/hook1,POST,"{""user"":""john""}","{""X-API-Key"":""secret""}",User Create,users
https://slack.com/hook2,POST,"{""text"":""Alert!""}","{""Content-Type"":""application/json""}",Slack Alert,alerts
```

## ⚙️ Configuration

### Core Settings

```bash
# Performance
MAX_WORKERS=5                    # Concurrent threads
BASE_RATE_LIMIT=2.0             # Seconds between requests
MAX_RATE_LIMIT=10.0             # Max delay on errors

# Resume functionality
RESUME_ENABLED=true
RESUME_CHECKPOINT_INTERVAL=100  # Save every N rows
RESUME_MAX_AGE_DAYS=7           # Ignore old checkpoints

# API Security
WEBHOOK_AUTH_TOKEN=secret123    # Bearer token for API
WEBHOOK_RATE_LIMIT=60           # Requests per minute
```

### Notifications

```bash
# Slack
SLACK_NOTIFICATIONS=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX
SLACK_NOTIFY_PROGRESS=true
SLACK_PROGRESS_EVERY_N=25

# Email
EMAIL_NOTIFICATIONS=true
EMAIL_SMTP_SERVER=smtp.gmail.com
EMAIL_USERNAME=notifications@company.com
EMAIL_PASSWORD=app_password_here
EMAIL_RECIPIENTS=team@company.com
```

## 🎯 Usage Examples

### Automatic Processing

```bash
# Files placed in watch directory are auto-processed
cp webhooks.csv /app/data/csv/
# Monitor processing
curl http://localhost:8000/status
```

### Manual Processing

```bash
# Process specific file
python webhook_trigger.py /path/to/webhooks.csv \
  --job-name "Manual Job" \
  --workers 10 \
  --rate-limit 1.0

# Dry run to validate
python webhook_trigger.py webhooks.csv --dry-run
```

### Resume from Checkpoint

```bash
# Job automatically resumes from last checkpoint
python webhook_trigger.py large_file.csv
# Interrupted at row 5000? Next run starts from 5000
```

## 🔄 Resume Functionality

The platform automatically saves progress checkpoints during processing:

- **Automatic Checkpointing**: Progress saved every 100 rows (configurable)
- **Smart Resume**: Detects previous progress and continues from last checkpoint
- **Hash Validation**: Ensures file hasn't changed since checkpoint
- **Age Checking**: Ignores checkpoints older than 7 days
- **API Control**: Force restart or check status via REST endpoints

## 🐳 Production Deployment

### Docker Compose

```yaml
version: '3.8'
services:
  bulk-api-trigger:
    build: .
    container_name: bulk-api-trigger
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./data:/app/data
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bulk-api-trigger
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: bulk-api-trigger
        image: your-registry/bulk-api-trigger:latest
        ports:
        - containerPort: 8000
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
        envFrom:
        - secretRef:
            name: bulk-api-secrets
```

## 📈 Monitoring & Observability

### Health Check Response

```json
{
  "status": "healthy",
  "system": {
    "watchdog_enabled": true,
    "watchdog_running": true,
    "queue_size": 2,
    "database_accessible": true
  }
}
```

### Job Statistics

```sql
-- Success rate by job
SELECT 
  job_name,
  total_requests,
  successful_requests,
  ROUND(successful_requests * 100.0 / total_requests, 2) as success_rate
FROM job_history 
WHERE start_time >= datetime('now', '-7 days')
ORDER BY start_time DESC;

-- Average response times
SELECT 
  DATE(timestamp) as date,
  AVG(response_time) as avg_response,
  COUNT(*) as total_requests
FROM webhook_results
GROUP BY DATE(timestamp);
```

## 🚨 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Files not processing | Check watchdog: `curl /health`, verify CSV format |
| High memory usage | Reduce `MAX_WORKERS` and `CSV_CHUNK_SIZE` |
| Rate limit errors | Increase `BASE_RATE_LIMIT` value |
| Resume not working | Check `RESUME_ENABLED=true`, verify checkpoint age |
| API authentication failing | Verify `WEBHOOK_AUTH_TOKEN` matches Bearer token |

### Debug Commands

```bash
# Check system status
curl http://localhost:8000/status

# View recent errors
curl http://localhost:8000/jobs/{job_id}/errors

# Check resume markers
curl http://localhost:8000/resume/stats

# Validate CSV file
python webhook_trigger.py file.csv --dry-run -v
```

## 🔒 Security Best Practices

- **API Authentication**: Set `WEBHOOK_AUTH_TOKEN` for production
- **HTTPS Only**: Use HTTPS URLs for all webhooks
- **Network Security**: Implement firewall rules
- **Secrets Management**: Use environment variables or secrets manager
- **Rate Limiting**: Configure appropriate rate limits
- **Monitoring**: Enable logging and alerts

## 📊 Performance Tuning

### High Volume (10k+ webhooks)

```bash
MAX_WORKERS=20
BASE_RATE_LIMIT=0.5
CSV_CHUNK_SIZE=2000
RESUME_CHECKPOINT_INTERVAL=500
```

### Memory Constrained

```bash
MAX_WORKERS=2
CSV_CHUNK_SIZE=500
REPORT_KEEP=50
DATABASE_BACKUP_INTERVAL_HOURS=168
```

### API Gateway Compatible

```bash
WEBHOOK_RATE_LIMIT=100
MAX_WORKERS=10
BASE_RATE_LIMIT=1.0
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.


Built with ❤️ for reliable webhook processing at scale