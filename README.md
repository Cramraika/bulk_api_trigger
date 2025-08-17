# 🚀 Enhanced Bulk API Trigger Platform

A production-ready platform for triggering thousands of webhooks/APIs with advanced features including file watchdog, auto-processing, comprehensive notifications, job reporting, and real-time monitoring.

## ✨ Key Features

- **🔥 Bulk Processing**: Handle thousands of API calls efficiently with thread pooling
- **👀 Smart File Watching**: Automatic detection and processing of new CSV files  
- **⚡ Dynamic Rate Limiting**: Intelligent rate adjustment based on success/error rates
- **🔄 Retry Logic**: Exponential backoff with Retry-After header respect
- **📊 SQLite Database**: Complete job history and request tracking
- **📈 Health Check Server**: Built-in HTTP health endpoint with system status
- **🔔 Smart Notifications**: Email and Slack integration with rich formatting
- **🛡️ Enterprise Features**: Database backups, file archiving, duplicate detection
- **🐳 Cloud Ready**: Docker support with health checks and graceful shutdown

## 🚀 Quick Start

### Docker Deployment (Recommended)

```bash
# Clone and setup
git clone <your-repo-url>
cd bulk-api-trigger
mkdir -p data/{csv/{processed,duplicates,rejected},reports,logs,backups}

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
│   ├── reports/               # 📊 JSON job reports
│   ├── logs/                  # 📝 Application logs
│   ├── backups/              # 💾 Database backups
│   └── webhook_results.db     # 🗄️ SQLite job database
└── webhook_trigger.py         # 🎯 Main application
```

## ⚙️ Configuration

### Key Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DEPLOYMENT_MODE` | Enable enhanced deployment features | `true` |
| `KEEP_ALIVE` | Keep container running with watchdog | `true` |
| `WATCHDOG_ENABLED` | Enable file monitoring | `true` |
| `MAX_WORKERS` | Concurrent request threads | `3` |
| `BASE_RATE_LIMIT` | **Base delay between requests (seconds)** | `3.0` |
| `MAX_RATE_LIMIT` | **Max delay when errors occur (seconds)** | `10.0` |
| `EMAIL_NOTIFICATIONS` | Enable email alerts | `false` |
| `SLACK_NOTIFICATIONS` | Enable Slack alerts | `false` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |

### Example Configuration

```bash
# Basic deployment
DEPLOYMENT_MODE=true
KEEP_ALIVE=true
WATCHDOG_ENABLED=true
MAX_WORKERS=5
BASE_RATE_LIMIT=3.0
MAX_RATE_LIMIT=10.0

# Notifications
EMAIL_NOTIFICATIONS=true
EMAIL_SMTP_SERVER=smtp.gmail.com
EMAIL_USERNAME=notifications@company.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENTS=ops@company.com,dev@company.com

SLACK_NOTIFICATIONS=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX
SLACK_NOTIFY_PROGRESS=true
SLACK_PROGRESS_EVERY_N=50
```

## 📊 CSV File Formats

### Basic Format
```csv
webhook_url
https://httpbin.org/post
https://api.example.com/webhook
```

### Advanced Format
```csv
webhook_url,method,payload,header,name,group
https://httpbin.org/post,POST,"{""message"": ""Hello World""}","{""Authorization"": ""Bearer token123""}",Test Webhook,testing
https://api.slack.com/hooks/xxx,POST,"{""text"": ""Deployment complete!""}","{""Content-Type"": ""application/json""}",Slack Alert,notifications
```

### Column Definitions

| Column | Required | Description | Examples |
|--------|----------|-------------|----------|
| `webhook_url` | ✅ | Target URL for the request | `https://api.example.com/webhook` |
| `method` | ❌ | HTTP method | `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `payload` | ❌ | Request body (JSON string) | `"{""key"": ""value""}"` |
| `header` | ❌ | Custom headers (JSON string) | `"{""Authorization"": ""Bearer token""}"` |
| `name` | ❌ | Friendly identifier | `User Registration Hook` |
| `group` | ❌ | Category/tag | `notifications`, `user_management` |

## 🎯 Usage Examples

### Automatic Processing (Recommended)
```bash
# Place CSV files in the watch directory
cp my_webhooks.csv /app/data/csv/
# Files are automatically detected and processed
```

### Manual Processing
```bash
python webhook_trigger.py /path/to/webhooks.csv --job-name "Manual Processing"
python webhook_trigger.py webhooks.csv --workers 10 --rate-limit 2.0
```

### Health Monitoring
```bash
curl http://localhost:8000/health          # System status
curl http://localhost:8000/jobs            # Job history
curl http://localhost:8000/jobs/{job_id}   # Job details
```

## 🔔 Notification Setup

### Email (Gmail Example)
1. Enable 2-Factor Authentication
2. Generate App Password (Google Account → Security → App passwords)
3. Configure:
   ```bash
   EMAIL_NOTIFICATIONS=true
   EMAIL_SMTP_SERVER=smtp.gmail.com
   EMAIL_USERNAME=your.email@gmail.com
   EMAIL_PASSWORD=your_16_char_app_password
   EMAIL_RECIPIENTS=admin@company.com
   ```

### Slack
1. Create Slack webhook at https://api.slack.com/messaging/webhooks
2. Configure:
   ```bash
   SLACK_NOTIFICATIONS=true
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
   SLACK_NOTIFY_COMPLETION=true
   ```

## 🐳 Production Deployment

### Docker Compose
```yaml
version: '3.8'
services:
  bulk-api-trigger:
    build: .
    container_name: bulk-api-trigger
    restart: unless-stopped
    environment:
      DEPLOYMENT_MODE: "true"
      KEEP_ALIVE: "true"
      WATCHDOG_ENABLED: "true"
      MAX_WORKERS: "5"
      BASE_RATE_LIMIT: "3.0"
      # Add your specific configuration
    volumes:
      - ./data:/app/data
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8000/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## 📈 Monitoring

### Health Endpoints
- `/health` - System health status
- `/jobs` - Recent job history
- `/jobs/{job_id}` - Job details and statistics
- `/metrics` - System performance metrics

### Database Queries
```sql
-- Job success rates
SELECT job_name, total_requests, successful_requests,
       (successful_requests * 100.0 / total_requests) as success_rate
FROM job_history ORDER BY start_time DESC;
```

## 🚨 Troubleshooting

### Common Issues

**Container Exits Immediately**
```bash
# Enable keep-alive mode
export KEEP_ALIVE=true
```

**Files Not Processing**
```bash
# Check watchdog status
curl http://localhost:8000/health

# Verify file format
python webhook_trigger.py /app/data/csv/your_file.csv --dry-run
```

**High Memory Usage**
```bash
export MAX_WORKERS=2
export CSV_CHUNK_SIZE=500
```

**Rate Limiting Issues**
```bash
export BASE_RATE_LIMIT=1.0
export MAX_RATE_LIMIT=3.0
```

## 🔧 Performance Optimization

### High-Volume Processing (10k+ webhooks)
```bash
export MAX_WORKERS=20
export BASE_RATE_LIMIT=1.0
export CSV_CHUNK_SIZE=2000
export LOG_LEVEL=WARNING
```

### Memory-Constrained Environments
```bash
export MAX_WORKERS=2
export CSV_CHUNK_SIZE=500
export REPORT_KEEP=50
```

## 📝 CSV Templates

### User Onboarding Pipeline
```csv
webhook_url,method,payload,header,name,group
https://email-service.com/api/send,POST,"{""to"": ""user@example.com"", ""template"": ""welcome""}","{""Authorization"": ""Bearer token""}",Welcome Email,onboarding
https://analytics.com/api/track,POST,"{""event"": ""user_signup"", ""user_id"": ""123""}","{""X-API-Key"": ""key""}",Track Signup,onboarding
```

### Deployment Notifications
```csv
webhook_url,method,payload,name,group
https://hooks.slack.com/services/xxx,POST,"{""text"": ""🚀 Deployment started"", ""channel"": ""#deployments""}",Slack Deploy,deployment
https://api.pagerduty.com/incidents,POST,"{""incident"": {""type"": ""incident"", ""title"": ""Deployment in progress""}}",PagerDuty Alert,deployment
```

## 🔐 Security & Best Practices

- Store sensitive data in environment variables
- Use HTTPS URLs for all webhook endpoints  
- Ensure appropriate file permissions
- Consider firewall rules for production
- Monitor logs for security events

## 📊 Complete Configuration Reference

<details>
<summary>Click to expand all environment variables</summary>

| Category | Variable | Description | Default |
|----------|----------|-------------|---------|
| **Deployment** | `DEPLOYMENT_MODE` | Enable enhanced deployment features | `true` |
| | `KEEP_ALIVE` | Keep container running with watchdog | `true` |
| | `JOB_NAME` | Default job display name | Auto-generated |
| | `HEALTH_PORT` | Health check server port | `8000` |
| **Rate Limiting** | `MAX_WORKERS` | Concurrent request threads | `3` |
| | `BASE_RATE_LIMIT` | Base delay between requests (seconds) | `3.0` |
| | `STARTING_RATE_LIMIT` | Initial delay between requests (seconds) | `3.0` |
| | `MAX_RATE_LIMIT` | Max delay when errors occur (seconds) | `10.0` |
| | `WINDOW_SIZE` | Rate adjustment window (requests) | `20` |
| | `ERROR_THRESHOLD` | Error rate threshold for slowdown | `0.3` |
| **File Management** | `WATCHDOG_ENABLED` | Enable file monitoring | `true` |
| | `WATCH_PATHS` | Directories to monitor | `/app/data/csv` |
| | `ARCHIVE_PROCESSED` | Archive processed files | `true` |
| | `ARCHIVE_PATH` | Processed files directory | `/app/data/csv/processed` |
| **Database** | `DATABASE_ENABLED` | Enable SQLite tracking | `true` |
| | `DATABASE_PATH` | Database file location | `/app/data/webhook_results.db` |
| | `DATABASE_BACKUP_ENABLED` | Enable automatic backups | `true` |
| **Email** | `EMAIL_NOTIFICATIONS` | Enable email alerts | `false` |
| | `EMAIL_SMTP_SERVER` | SMTP server hostname | `smtp.gmail.com` |
| | `EMAIL_SMTP_PORT` | SMTP server port | `587` |
| | `EMAIL_USERNAME` | SMTP username | — |
| | `EMAIL_PASSWORD` | SMTP password | — |
| | `EMAIL_FROM` | Sender email address | — |
| | `EMAIL_RECIPIENTS` | Comma-separated recipients | — |
| **Slack** | `SLACK_NOTIFICATIONS` | Enable Slack alerts | `false` |
| | `SLACK_WEBHOOK_URL` | Slack webhook URL | — |
| | `SLACK_NOTIFY_COMPLETION` | Slack on job completion | `true` |
| | `SLACK_NOTIFY_PROGRESS` | Slack progress updates | `false` |
| | `SLACK_PROGRESS_EVERY_N` | Progress notification frequency | `25` |
| **Logging** | `LOG_LEVEL` | Logging verbosity | `INFO` |
| | `MAX_LOG_SIZE_MB` | Log file rotation size | `100` |
| | `REPORT_PATH` | JSON reports directory | `/app/data/reports` |

</details>

---

🎯 **Ready to process thousands of webhooks reliably?** Drop your CSV files in the watch directory and let the platform handle the rest!