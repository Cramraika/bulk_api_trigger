# 🚀 Enhanced Bulk API Trigger Platform

A production-ready platform for triggering thousands of webhooks/APIs with advanced features including **file watchdog**, **auto-processing**, **comprehensive notifications**, **job reporting**, and **real-time monitoring**.

## ✨ Key Features

### 🔥 Core Processing
- **Bulk Processing**: Handle thousands of API calls efficiently with thread pooling
- **Smart File Watching**: Automatic detection and processing of new CSV files
- **Multi-File Support**: Process multiple CSV files with pattern matching
- **Auto-Discovery**: Scan and process existing files on startup

### ⚡ Performance & Reliability
- **Dynamic Rate Limiting**: Intelligent rate adjustment based on success/error rates
- **Retry Logic**: Exponential backoff with Retry-After header respect
- **Thread Safety**: Robust concurrent processing with proper resource management
- **Progress Tracking**: Real-time progress bars and detailed metrics

### 📊 Monitoring & Reporting
- **SQLite Database**: Complete job history and request tracking
- **JSON Reports**: Detailed job completion reports with metrics
- **Health Check Server**: Built-in HTTP health endpoint with system status
- **Comprehensive Logging**: Structured logging with rotation and UTF-8 support

### 🔔 Smart Notifications
- **Email Notifications**: SMTP support with HTML formatting and error handling
- **Slack Integration**: Rich notifications with urgency levels and progress updates
- **File Detection Alerts**: Notifications when new files are discovered
- **Completion Reports**: Detailed job summaries with success rates and timing

### 🛡️ Enterprise Features
- **Database Backups**: Automatic scheduled database backups
- **File Archiving**: Smart file management with processed/duplicate/rejected folders
- **Duplicate Detection**: Hash-based duplicate file prevention
- **Error Recovery**: Graceful handling of failures with detailed error tracking

### 🐳 Cloud Ready
- **Container Optimized**: Docker support with health checks and graceful shutdown
- **Cloud Platform Ready**: Coolify, Railway, Heroku, DigitalOcean support
- **Environment Configuration**: Full configuration via environment variables
- **Keep-Alive Mode**: Long-running service mode with watchdog monitoring

## 🚀 Quick Start

### 1. Docker Deployment (Recommended)

```bash
# Clone the repository
git clone <your-repo-url>
cd bulk-api-trigger

# Create directories
mkdir -p data/csv data/csv/processed data/csv/duplicates data/csv/rejected data/reports data/logs data/backups

# Start with Docker Compose
docker-compose up -d

# Check logs
docker-compose logs -f bulk-api-trigger
```

### 2. Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Create sample configuration
python webhook_trigger.py --create-config

# Interactive mode
python webhook_trigger.py --interactive

# Process specific file
python webhook_trigger.py webhooks.csv --job-name "My First Job"
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

### Environment Variables (Primary Configuration)

| Category | Variable | Description | Default |
|----------|----------|-------------|---------|
| **Deployment** | `DEPLOYMENT_MODE` | Enable enhanced deployment features | `true` |
| | `KEEP_ALIVE` | Keep container running with watchdog | `true` |
| | `JOB_NAME` | Default job display name | `Auto-generated` |
| | `HEALTH_PORT` | Health check server port | `8000` |
| **Logging** | `LOG_LEVEL` | Logging verbosity (`DEBUG/INFO/WARN/ERROR`) | `INFO` |
| | `MAX_LOG_SIZE_MB` | Log file rotation size | `100` |
| | `REPORT_PATH` | JSON reports directory | `/app/data/reports` |
| | `REPORT_KEEP` | Number of reports to retain | `200` |
| **Watchdog** | `WATCHDOG_ENABLED` | Enable file monitoring | `true` |
| | `WATCH_PATHS` | Comma-separated directories to monitor | `/app/data/csv` |
| | `DEBOUNCE_DELAY` | File stabilization delay (seconds) | `3.0` |
| | `PROCESSING_WORKERS` | File processing worker threads | `1` |
| | `MAX_QUEUE_SIZE` | Maximum queued files | `100` |
| **CSV Processing** | `CSV_FILE` | Specific file to process (`AUTO` for watchdog) | `AUTO` |
| | `CSV_REQUIRED_COLUMNS` | Required CSV columns | `webhook_url` |
| | `CSV_FILE_PATTERNS` | File discovery patterns | `/app/data/csv/*.csv` |
| | `SKIP_ROWS` | Rows to skip in CSV | `0` |
| | `CSV_CHUNK_SIZE` | Processing chunk size | `1000` |
| **File Management** | `ARCHIVE_PROCESSED` | Archive processed files | `true` |
| | `ARCHIVE_PATH` | Processed files directory | `/app/data/csv/processed` |
| | `DUPLICATES_PATH` | Duplicate files directory | `/app/data/csv/duplicates` |
| | `REJECTED_PATH` | Invalid files directory | `/app/data/csv/rejected` |
| | `ARCHIVE_ON_VALIDATION_FAILURE` | Archive invalid files | `true` |
| **Rate Limiting** | `MAX_WORKERS` | Concurrent request threads | `3` |
| | `BASE_RATE_LIMIT` | Starting requests per second | `3.0` |
| | `MAX_RATE_LIMIT` | Maximum requests per second | `5.0` |
| | `WINDOW_SIZE` | Rate adjustment window | `20` |
| | `ERROR_THRESHOLD` | Error rate threshold for slowdown | `0.3` |
| | `GLOBAL_MAX_REQUESTS` | Global request semaphore limit | `0` (unlimited) |
| **Retry & Timeouts** | `MAX_RETRIES` | Maximum retry attempts | `3` |
| | `RETRY_DELAY` | Base retry delay (seconds) | `1.0` |
| | `REQUEST_TIMEOUT` | HTTP request timeout | `30` |
| **Database** | `DATABASE_ENABLED` | Enable SQLite tracking | `true` |
| | `DATABASE_PATH` | Database file location | `/app/data/webhook_results.db` |
| | `DATABASE_BACKUP_ENABLED` | Enable automatic backups | `true` |
| | `DATABASE_BACKUP_INTERVAL_HOURS` | Backup frequency | `24` |
| **Email Notifications** | `EMAIL_NOTIFICATIONS` | Enable email alerts | `false` |
| | `EMAIL_SMTP_SERVER` | SMTP server hostname | `smtp.gmail.com` |
| | `EMAIL_SMTP_PORT` | SMTP server port | `587` |
| | `EMAIL_USERNAME` | SMTP authentication username | — |
| | `EMAIL_PASSWORD` | SMTP authentication password | — |
| | `EMAIL_FROM` | Sender email address | — |
| | `EMAIL_RECIPIENTS` | Comma-separated recipient emails | — |
| | `EMAIL_NOTIFY_COMPLETION` | Email on job completion | `true` |
| | `EMAIL_NOTIFY_FILE_DETECTED` | Email on new file detection | `false` |
| **Slack Notifications** | `SLACK_NOTIFICATIONS` | Enable Slack alerts | `false` |
| | `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | — |
| | `SLACK_NOTIFY_COMPLETION` | Slack on job completion | `true` |
| | `SLACK_NOTIFY_FILE_DETECTED` | Slack on new file detection | `true` |
| | `SLACK_NOTIFY_PROGRESS` | Slack progress updates | `false` |
| | `SLACK_PROGRESS_EVERY_N` | Progress notification frequency | `25` |
| | `SLACK_PROGRESS_URGENCY` | Progress urgency level (`auto/low/normal/high/critical`) | `auto` |
| **Testing** | `SEND_TEST_NOTIFICATIONS_ON_STARTUP` | Test notifications at startup | `false` |
| | `FORCE_TQDM` | Force progress bar display | `0` |
| | `PROGRESS_EVERY_N` | Log progress frequency | `50` |

### 📋 Example Environment Configuration

```bash
# Basic deployment
DEPLOYMENT_MODE=true
KEEP_ALIVE=true
WATCHDOG_ENABLED=true
MAX_WORKERS=5
BASE_RATE_LIMIT=5.0
MAX_RATE_LIMIT=15.0

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

# Advanced features
DATABASE_BACKUP_ENABLED=true
DATABASE_BACKUP_INTERVAL_HOURS=12
ARCHIVE_PROCESSED=true
LOG_LEVEL=INFO
```

## 📊 CSV File Formats

### Basic Format
```csv
webhook_url
https://httpbin.org/post
https://api.example.com/webhook
https://my-service.com/hook
```

### Advanced Format with All Features
```csv
webhook_url,method,payload,header,name,group
https://httpbin.org/post,POST,"{""message"": ""Hello World"", ""timestamp"": ""2024-01-01T12:00:00Z""}","{""Authorization"": ""Bearer token123"", ""Content-Type"": ""application/json""}",Test Webhook,testing
https://api.slack.com/hooks/xxx,POST,"{""text"": ""Deployment complete!"", ""channel"": ""#alerts""}","{""Content-Type"": ""application/json""}",Slack Alert,notifications
https://api.example.com/users/123,PUT,"{""status"": ""active"", ""last_login"": ""2024-01-01T12:00:00Z""}","{""Authorization"": ""Bearer token456"", ""X-API-Version"": ""v2""}",Update User,user_management
https://webhook.site/unique-url,GET,"","{""X-Custom-Header"": ""monitoring""}",Health Check,monitoring
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

### 1. Automatic File Processing (Recommended)

```bash
# Place CSV files in the watch directory
cp my_webhooks.csv /app/data/csv/

# Files are automatically detected and processed
# Check logs for progress: docker logs bulk-api-trigger
```

### 2. Manual File Processing

```bash
# Process specific file
python webhook_trigger.py /path/to/webhooks.csv --job-name "Manual Processing"

# Process with custom settings
python webhook_trigger.py webhooks.csv \
  --workers 10 \
  --rate-limit 2.0 \
  --skip-rows 1 \
  --job-name "High Volume Processing"
```

### 3. Interactive Mode

```bash
python webhook_trigger.py --interactive
# Follow prompts for file selection, job naming, and configuration
```

### 4. Validation Mode (Dry Run)

```bash
# Test CSV format without sending requests
python webhook_trigger.py webhooks.csv --dry-run
```

### 5. Health Monitoring

```bash
# Check system status
curl http://localhost:8000/health

# Get job history
curl http://localhost:8000/jobs

# Get specific job details
curl http://localhost:8000/jobs/job_20241213_143022_abc123

# Get job errors
curl http://localhost:8000/jobs/job_20241213_143022_abc123/errors

# Get job report
curl http://localhost:8000/jobs/job_20241213_143022_abc123/report
```

## 🔔 Notification Setup

### Email Notifications (Gmail Example)

1. **Enable 2-Factor Authentication** on your Gmail account
2. **Generate App Password**:
   - Go to Google Account Settings → Security
   - 2-Step Verification → App passwords
   - Generate password for "Mail"
3. **Configure Environment Variables**:
   ```bash
   EMAIL_NOTIFICATIONS=true
   EMAIL_SMTP_SERVER=smtp.gmail.com
   EMAIL_SMTP_PORT=587
   EMAIL_USERNAME=your.email@gmail.com
   EMAIL_PASSWORD=your_16_char_app_password
   EMAIL_FROM=your.email@gmail.com
   EMAIL_RECIPIENTS=admin@company.com,team@company.com
   EMAIL_NOTIFY_COMPLETION=true
   ```

### Slack Notifications

1. **Create Slack Webhook**:
   - Go to https://api.slack.com/messaging/webhooks
   - Choose your workspace and channel
   - Copy the webhook URL
2. **Configure Environment Variables**:
   ```bash
   SLACK_NOTIFICATIONS=true
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX
   SLACK_NOTIFY_COMPLETION=true
   SLACK_NOTIFY_FILE_DETECTED=true
   SLACK_NOTIFY_PROGRESS=true
   SLACK_PROGRESS_EVERY_N=25
   ```

## 📈 Monitoring and Analytics

### Built-in Health Check Server

The platform includes a comprehensive health check server accessible at `http://localhost:8000`:

- **`/health`** - System health status and component checks
- **`/jobs`** - Recent job history (last 50 jobs)
- **`/jobs/{job_id}`** - Detailed job statistics
- **`/jobs/{job_id}/errors`** - Failed requests for a job
- **`/jobs/{job_id}/report`** - Complete job report JSON
- **`/metrics`** - System performance metrics
- **`/config`** - Current configuration (secrets redacted)

### Database Queries

The SQLite database provides comprehensive tracking:

```sql
-- Job success rates
SELECT 
    job_name,
    total_requests,
    successful_requests,
    (successful_requests * 100.0 / total_requests) as success_rate,
    duration_seconds
FROM job_history 
ORDER BY start_time DESC;

-- Error analysis
SELECT 
    status,
    COUNT(*) as count,
    AVG(response_time) as avg_response_time
FROM webhook_results 
WHERE job_id = 'your_job_id'
GROUP BY status;

-- Recent system metrics
SELECT * FROM system_metrics 
ORDER BY timestamp DESC 
LIMIT 100;
```

### JSON Reports

Each completed job generates a detailed JSON report in `/app/data/reports/`:

```json
{
  "job": {
    "id": "job_20241213_143022_abc123",
    "name": "Production Webhooks",
    "csv_file": "/app/data/csv/webhooks.csv",
    "start_time": "2024-12-13T14:30:22.123456",
    "end_time": "2024-12-13T14:35:45.789012",
    "duration_seconds": 323.67,
    "triggered_by": "watchdog"
  },
  "totals": {
    "total_requests": 1000,
    "successful": 985,
    "failed": 15,
    "success_rate": 98.5
  },
  "performance": {
    "average_response_time": 0.245,
    "status_breakdown": {
      "success": {"count": 985, "avg_response_time": 0.234},
      "error": {"count": 15, "avg_response_time": 1.234}
    },
    "metrics": {
      "throughput": 3.09,
      "total_request_size": 52480,
      "total_response_size": 1048576
    }
  },
  "errors_sample": [
    {
      "url": "https://api.example.com/webhook",
      "status_code": 429,
      "error_message": "Rate limit exceeded",
      "timestamp": "2024-12-13T14:32:15.123456"
    }
  ]
}
```

## 🐳 Cloud Deployment

### Docker Compose (Production Ready)

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
      MAX_RATE_LIMIT: "10.0"
      
      # Database
      DATABASE_ENABLED: "true"
      DATABASE_BACKUP_ENABLED: "true"
      DATABASE_BACKUP_INTERVAL_HOURS: "12"
      
      # Notifications
      EMAIL_NOTIFICATIONS: "${EMAIL_NOTIFICATIONS:-false}"
      EMAIL_SMTP_SERVER: "${EMAIL_SMTP_SERVER}"
      EMAIL_USERNAME: "${EMAIL_USERNAME}"
      EMAIL_PASSWORD: "${EMAIL_PASSWORD}"
      EMAIL_FROM: "${EMAIL_FROM}"
      EMAIL_RECIPIENTS: "${EMAIL_RECIPIENTS}"
      
      SLACK_NOTIFICATIONS: "${SLACK_NOTIFICATIONS:-false}"
      SLACK_WEBHOOK_URL: "${SLACK_WEBHOOK_URL}"
      
      # File Management
      ARCHIVE_PROCESSED: "true"
      ARCHIVE_ON_VALIDATION_FAILURE: "true"
      
      # Logging
      LOG_LEVEL: "INFO"
      REPORT_PATH: "/app/data/reports"
      
    volumes:
      - ./data:/app/data
      - ./config.yaml:/app/config.yaml:ro
    ports:
      - "8000:8000"  # Health check server
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

### Coolify Deployment

```yaml
# coolify.yaml
services:
  bulk-api-trigger:
    image: your-registry/bulk-api-trigger:latest
    environment:
      DEPLOYMENT_MODE: "true"
      KEEP_ALIVE: "true"
      WATCHDOG_ENABLED: "true"
      MAX_WORKERS: "3"
      # Add your specific configuration
    volumes:
      - bulk-api-data:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  bulk-api-data:
```

### Railway Deployment

```json
{
  "build": {
    "builder": "DOCKERFILE"
  },
  "deploy": {
    "startCommand": "python webhook_trigger.py",
    "healthcheckPath": "/health",
    "healthcheckTimeout": 10,
    "restartPolicyType": "ON_FAILURE"
  }
}
```

## 🚨 Troubleshooting

### Common Issues and Solutions

#### 1. Container Exits Immediately
```bash
# Check if keep-alive is enabled
docker logs bulk-api-trigger

# Solution: Enable keep-alive mode
export KEEP_ALIVE=true
```

#### 2. Files Not Being Processed
```bash
# Check watchdog status
curl http://localhost:8000/health

# Verify file permissions and format
python webhook_trigger.py /app/data/csv/your_file.csv --dry-run

# Check logs for validation errors
docker logs bulk-api-trigger | grep -i error
```

#### 3. High Memory Usage
```bash
# Reduce concurrent workers and chunk size
export MAX_WORKERS=2
export CSV_CHUNK_SIZE=500
export WINDOW_SIZE=10
```

#### 4. Rate Limiting Issues
```bash
# Adjust rate limits for your APIs
export BASE_RATE_LIMIT=1.0
export MAX_RATE_LIMIT=3.0
export ERROR_THRESHOLD=0.2
```

#### 5. Notification Failures
```bash
# Test notifications manually
export SEND_TEST_NOTIFICATIONS_ON_STARTUP=true

# Check SMTP settings for email
export LOG_LEVEL=DEBUG

# Verify Slack webhook URL format
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test message"}' \
  YOUR_SLACK_WEBHOOK_URL
```

### Health Check Commands

```bash
# System health
curl -s http://localhost:8000/health | jq .

# Database connectivity test
python -c "
import sqlite3
conn = sqlite3.connect('/app/data/webhook_results.db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM job_history')
print(f'Jobs in database: {cursor.fetchone()[0]}')
conn.close()
"

# CSV validation
python webhook_trigger.py your_file.csv --dry-run

# Check file permissions
ls -la /app/data/csv/
ls -la /app/data/csv/processed/
```

## 🔐 Security Best Practices

1. **Environment Variables**: Store all sensitive data (API keys, passwords) in environment variables
2. **File Permissions**: Ensure CSV directories have appropriate permissions
3. **HTTPS Only**: Use HTTPS URLs for all webhook endpoints
4. **Log Sanitization**: Sensitive data is automatically truncated in logs
5. **Database Security**: SQLite database contains response previews but not full sensitive payloads
6. **Network Security**: Consider firewall rules and VPN access for production deployments

## 📊 Performance Optimization

### For High-Volume Processing (10k+ webhooks):

```bash
# Increase concurrency and optimize for throughput
export MAX_WORKERS=20
export BASE_RATE_LIMIT=1.0
export MAX_RATE_LIMIT=15.0
export WINDOW_SIZE=50
export REQUEST_TIMEOUT=15

# Optimize file processing
export CSV_CHUNK_SIZE=2000
export PROCESSING_WORKERS=3

# Reduce logging overhead
export LOG_LEVEL=WARNING
export PROGRESS_EVERY_N=100

# Optimize database
export DATABASE_BACKUP_INTERVAL_HOURS=48
```

### Memory Optimization:

```bash
# For memory-constrained environments
export MAX_WORKERS=2
export CSV_CHUNK_SIZE=500
export REPORT_KEEP=50
export MAX_LOG_SIZE_MB=50
```

## 📝 CSV Templates and Examples

### Real-World Templates

#### User Onboarding Pipeline
```csv
webhook_url,method,payload,header,name,group
https://email-service.com/api/send,POST,"{""to"": ""{{user_email}}"", ""template"": ""welcome""}","{""Authorization"": ""Bearer email_token""}",Welcome Email,onboarding
https://analytics.com/api/track,POST,"{""event"": ""user_signup"", ""user_id"": ""{{user_id}}""}","{""X-API-Key"": ""analytics_key""}",Track Signup,onboarding
https://crm.com/api/contacts,POST,"{""email"": ""{{user_email}}"", ""source"": ""app_signup""}","{""Authorization"": ""Bearer crm_token""}",Add to CRM,onboarding
```

#### Deployment Notifications
```csv
webhook_url,method,payload,name,group
https://hooks.slack.com/services/xxx,POST,"{""text"": ""🚀 Deployment to production started"", ""channel"": ""#deployments""}",Slack Deploy Start,deployment
https://api.pagerduty.com/incidents,POST,"{""incident"": {""type"": ""incident"", ""title"": ""Deployment in progress""}}",PagerDuty Alert,deployment
https://discord.com/api/webhooks/xxx,POST,"{""content"": ""Build #{{build_number}} deployed successfully""}",Discord Success,deployment
```

#### System Health Monitoring
```csv
webhook_url,method,header,name,group
https://api.service1.com/health,GET,"{""Authorization"": ""Bearer monitor_token""}",Service 1 Health,monitoring
https://api.service2.com/status,GET,"{""X-API-Key"": ""monitor_key""}",Service 2 Status,monitoring
https://database.example.com/ping,GET,"{""Authorization"": ""Bearer db_token""}",Database Health,monitoring
```

---

## 📞 Support and Contributing

For issues, feature requests, or contributions, please:

1. Check the troubleshooting section above
2. Review the health check endpoints for system status
3. Enable debug logging with `LOG_LEVEL=DEBUG`
4. Check the generated JSON reports in `/app/data/reports/`

The Enhanced Bulk API Trigger Platform is designed to be robust, scalable, and production-ready for handling large-scale webhook operations with comprehensive monitoring and notification capabilities.