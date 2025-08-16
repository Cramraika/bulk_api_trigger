# 🚀 Bulk API Trigger Platform - Complete Guide

A production-ready platform for triggering thousands of webhooks/APIs with advanced features like rate limiting, retry logic, notifications, and job tracking.

## ✨ Features

- **🔥 Bulk Processing**: Handle thousands of API calls efficiently
- **⚡ Rate Limiting**: Dynamic rate adjustment based on error rates
- **🔄 Retry Logic**: Configurable retry attempts with exponential backoff
- **📊 Job Tracking**: SQLite database for storing results and job history
- **🔔 Notifications**: Email and Slack notifications for job completion
- **🎯 Multiple Formats**: Support for various CSV formats and authentication methods
- **🐳 Container Ready**: Docker support with health checks
- **☁️ Cloud Deploy**: Ready for Coolify, Railway, Heroku, and other platforms
- **📈 Progress Tracking**: Real-time progress bars and detailed logging
- **🛡️ Error Handling**: Comprehensive error handling and logging

## 🚀 Quick Start

### 1. Local Installation

```bash
# Clone or download the files
git clone <your-repo-url>
cd bulk-api-trigger

# Install dependencies
pip install -r requirements.txt

# Create sample configuration
python webhook_trigger.py --create-config

# Run with your CSV file
python webhook_trigger.py your_webhooks.csv --job-name "My First Job"
```

### 2. Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Or build manually
docker build -t bulk-api-trigger .
docker run -v ./csv:/app/csv -v ./data:/app/data bulk-api-trigger
```

## 📂 Project Structure
- `/app/data/csv` → Incoming CSV files to process  
- `/app/data/csv/processed` → Archived processed files  
- `/app/data/webhook_results.db` → SQLite database for job logs  
- `/app/data/logs` → JSON logs of webhook results  

---

## ⚙️ Environment Variables

All runtime configuration is handled via **environment variables**.  
Defaults are defined in code, but **Coolify env vars override both config.yaml and defaults**.

| Variable | Description | Default |
|----------|-------------|---------|
| `DEPLOYMENT_MODE` | Enable enhanced deployment | `true` |
| `KEEP_ALIVE` | Keep watchdog alive | `true` |
| `JOB_NAME` | Job display name | `webhook-runner` |
| `LOG_LEVEL` | Logging level (`INFO/DEBUG/ERROR`) | `INFO` |
| `MAX_LOG_SIZE_MB` | Max log size before rotation | `100` |
| `METRICS_ENABLED` | Enable Prometheus metrics | `true` |
| `HEALTH_CHECK_ENABLED` | Enable health check server | `true` |
| `WATCHDOG_ENABLED` | Enable watchdog | `true` |
| `WATCH_PATHS` | Comma-separated watch dirs | `/app/data/csv` |
| `AUTO_PROCESS` | Auto-process new files | `true` |
| `DEBOUNCE_DELAY` | Debounce delay (s) | `3.0` |
| `MAX_QUEUE_SIZE` | Max queued jobs | `100` |
| `CSV_REQUIRED_COLUMNS` | Required CSV columns | `webhook_url` |
| `CSV_OPTIONAL_COLUMNS` | Optional CSV columns | `method,payload,header,name,group` |
| `CSV_CHUNK_SIZE` | Rows per chunk | `1000` |
| `CSV_FILE_PATTERNS` | File glob pattern | `/app/data/csv/*.csv` |
| `ARCHIVE_PROCESSED` | Archive processed files | `true` |
| `ARCHIVE_PATH` | Path for archived CSVs | `/app/data/csv/processed` |
| `SKIP_ROWS` | Skip first N rows | `0` |
| `CSV_FILE` | File to process on startup (`AUTO` = scan) | `AUTO` |
| `MAX_WORKERS` | Worker threads | `3` |
| `BASE_RATE_LIMIT` | Base req/sec | `3.0` |
| `STARTING_RATE_LIMIT` | Starting req/sec | `3.0` |
| `MAX_RATE_LIMIT` | Max req/sec | `10.0` |
| `WINDOW_SIZE` | Rate limit window (s) | `20` |
| `ERROR_THRESHOLD` | Error ratio threshold | `0.3` |
| `MAX_RETRIES` | Max retry attempts | `3` |
| `RETRY_DELAY` | Retry delay (s) | `1.0` |
| `REQUEST_TIMEOUT` | Request timeout (s) | `30` |
| `DATABASE_ENABLED` | Enable DB persistence | `true` |
| `DATABASE_PATH` | SQLite DB path | `/app/data/webhook_results.db` |
| `DATABASE_BACKUP_ENABLED` | Enable DB backup | `true` |
| `DATABASE_BACKUP_INTERVAL_HOURS` | Backup interval | `24` |
| `EMAIL_NOTIFICATIONS` | Enable email | `false` |
| `EMAIL_SMTP_SERVER` | SMTP server | `smtp.gmail.com` |
| `EMAIL_SMTP_PORT` | SMTP port | `587` |
| `EMAIL_USERNAME` | SMTP username | — |
| `EMAIL_PASSWORD` | SMTP password | — |
| `EMAIL_FROM` | Sender email | — |
| `EMAIL_RECIPIENTS` | Comma-separated recipients | `admin@example.com` |
| `EMAIL_NOTIFY_COMPLETION` | Email on completion | `true` |
| `EMAIL_NOTIFY_ERRORS` | Email on errors | `true` |
| `EMAIL_NOTIFY_FILE_DETECTED` | Email on file detection | `false` |
| `SLACK_NOTIFICATIONS` | Enable Slack | `false` |
| `SLACK_WEBHOOK_URL` | Slack webhook URL | — |
| `SLACK_NOTIFY_COMPLETION` | Slack on completion | `true` |
| `SLACK_NOTIFY_ERRORS` | Slack on errors | `true` |
| `SLACK_NOTIFY_FILE_DETECTED` | Slack on file detection | `true` |

---

## 📦 Example `.env`

```env
DEPLOYMENT_MODE=true
KEEP_ALIVE=true
JOB_NAME=Bulk API Trigger
WATCH_PATHS=/app/data/csv
MAX_WORKERS=5
BASE_RATE_LIMIT=5.0
MAX_RATE_LIMIT=15.0
DATABASE_ENABLED=true
EMAIL_NOTIFICATIONS=true
EMAIL_USERNAME=bot@example.com
EMAIL_PASSWORD=xxxxxx
EMAIL_FROM=bot@example.com
EMAIL_RECIPIENTS=ops@example.com,dev@example.com
SLACK_NOTIFICATIONS=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXX/YYYY/ZZZZ
```

### Configuration File (config.yaml) - Priority: Medium

Create a `config.yaml` file for more detailed configuration:

```yaml
rate_limiting:
  base_rate_limit: 2.0
  max_workers: 5
  error_threshold: 0.25

notifications:
  email:
    enabled: true
    smtp_server: smtp.gmail.com
    recipients:
      - admin@example.com
      - team@example.com

  slack:
    enabled: true
    webhook_url: https://hooks.slack.com/services/YOUR/WEBHOOK
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
https://httpbin.org/post,POST,"{""message"": ""Hello""}","{""Authorization"": ""Bearer token""}",Test API,testing
https://api.example.com/users,GET,"","{""X-API-Key"": ""key123""}",Get Users,data
```

### Multiple CSV Files
The platform automatically discovers and processes multiple CSV files:
- `webhooks*.csv`
- `http_triggers*.csv` 
- `apis*.csv`
- Any `*.csv` files

## 🎯 Usage Examples

### 1. CLI Mode
```bash
# Basic usage
python webhook_trigger.py webhooks.csv

# With custom settings
python webhook_trigger.py webhooks.csv \
  --job-name "Production Deployment" \
  --workers 10 \
  --rate-limit 1.0 \
  --skip-rows 1

# Dry run (validate without sending)
python webhook_trigger.py webhooks.csv --dry-run

# Interactive mode
python webhook_trigger.py --interactive
```

### 2. Deployment Mode
```bash
# Set environment variables
export DEPLOYMENT_MODE=true
export JOB_NAME="Scheduled Job"
export MAX_WORKERS=5
export EMAIL_NOTIFICATIONS=true
export EMAIL_RECIPIENTS="admin@example.com"

# Run
python webhook_trigger.py
```

### 3. Docker Usage
```bash
# Quick run with mounted CSV
docker run -v ./csv:/app/csv \
  -v ./data:/app/data \
  -e DEPLOYMENT_MODE=true \
  -e MAX_WORKERS=5 \
  bulk-api-trigger

# With configuration file
docker run -v ./config.yaml:/app/config.yaml \
  -v ./csv:/app/csv \
  bulk-api-trigger
```

## 🔔 Notification Setup

### Email Notifications (Gmail)

1. **Enable 2FA** on your Gmail account
2. **Generate App Password**:
   - Go to Google Account settings
   - Security → 2-Step Verification → App passwords
   - Generate password for "Mail"
3. **Configure environment variables**:
   ```bash
   EMAIL_NOTIFICATIONS=true
   EMAIL_SMTP_SERVER=smtp.gmail.com
   EMAIL_SMTP_PORT=587
   EMAIL_USERNAME=your_email@gmail.com
   EMAIL_PASSWORD=your_16_char_app_password
   EMAIL_FROM=your_email@gmail.com
   EMAIL_RECIPIENTS=admin@example.com,team@example.com
   ```

### Slack Notifications

1. **Create Slack Webhook**:
   - Go to https://api.slack.com/messaging/webhooks
   - Create new webhook for your workspace
   - Copy webhook URL
2. **Configure environment variable**:
   ```bash
   SLACK_NOTIFICATIONS=true
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
   ```

## 📈 Monitoring and Analytics

### Job Tracking
All jobs are stored in SQLite database with:
- Job ID and name
- Start/end times and duration
- Success/failure counts and rates
- Individual request results
- Error messages and response times

### Database Queries
```sql
-- Get job history
SELECT * FROM job_history ORDER BY start_time DESC;

-- Get detailed results for a job
SELECT * FROM webhook_results WHERE job_id = 'job_20241213_143022_abc123';

-- Success rate by job
SELECT job_name, 
       (successful_requests * 100.0 / total_requests) as success_rate
FROM job_history 
WHERE total_requests > 0;
```

### Log Analysis
```bash
# Monitor real-time logs
tail -f webhook_trigger.log

# Filter successful requests
grep "✅ Success" webhook_trigger.log | wc -l

# Filter errors
grep "❌ Error" webhook_trigger.log
```

## 🐳 Platform-Specific Deployment

### Coolify
```yaml
# .coolify/config.yaml
services:
  bulk-api-trigger:
    build: .
    environment:
      DEPLOYMENT_MODE: "true"
      KEEP_ALIVE: "true"
      MAX_WORKERS: "3"
    volumes:
      - ./csv:/app/csv
      - ./data:/app/data
```

### Railway
```json
{
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "python webhook_trigger.py",
    "restartPolicyType": "ON_FAILURE"
  }
}
```

### Heroku
```yaml
# app.json
{
  "name": "bulk-api-trigger",
  "description": "Bulk webhook/API trigger platform",
  "keywords": ["webhook", "api", "bulk"],
  "env": {
    "DEPLOYMENT_MODE": "true",
    "MAX_WORKERS": "3",
    "KEEP_ALIVE": "true"
  },
  "formation": {
    "worker": {
      "quantity": 1,
      "size": "basic"
    }
  }
}
```

### DigitalOcean App Platform
```yaml
# .do/app.yaml
name: bulk-api-trigger
services:
- name: worker
  source_dir: /
  github:
    repo: your-username/bulk-api-trigger
    branch: main
  run_command: python webhook_trigger.py
  environment_slug: python
  instance_count: 1
  instance_size_slug: basic-xxs
  envs:
  - key: DEPLOYMENT_MODE
    value: "true"
  - key: MAX_WORKERS
    value: "3"
```

## 🚨 Troubleshooting

### Common Issues

1. **Container exits immediately**
   ```bash
   # Solution: Enable keep-alive
   export KEEP_ALIVE=true
   ```

2. **Rate limiting too aggressive**
   ```bash
   # Solution: Adjust rate limits
   export BASE_RATE_LIMIT=1.0
   export MAX_WORKERS=10
   ```

3. **JSON parsing errors in CSV**
   ```csv
   # Wrong
   webhook_url,payload
   https://api.com,{'key': 'value'}
   
   # Correct
   webhook_url,payload
   https://api.com,"{""key"": ""value""}"
   ```

4. **Memory issues with large CSV**
   ```bash
   # Solution: Use chunked processing (automatic)
   export CSV_CHUNK_SIZE=500
   ```

5. **Authentication failures**
   ```bash
   # Debug mode for more details
   export LOG_LEVEL=DEBUG
   ```

### Health Checks

```bash
# Check if database is accessible
python -c "import sqlite3; conn=sqlite3.connect('/app/data/webhook_results.db'); print('DB OK'); conn.close()"

# Check CSV file validity
python webhook_trigger.py your_file.csv --dry-run

# Test single webhook
curl -X POST https://httpbin.org/post -H "Content-Type: application/json" -d '{"test": true}'
```

## 🔐 Security Considerations

1. **API Keys**: Store in environment variables, not in CSV files
2. **Logs**: Sensitive data is automatically truncated in logs
3. **Database**: Contains response previews but not full sensitive data
4. **HTTPS**: Always use HTTPS URLs for webhook endpoints
5. **Secrets**: Use platform-specific secret management

## 📊 Performance Optimization

### For High-Volume Processing (10k+ webhooks):

```bash
# Increase workers and optimize rate limits
export MAX_WORKERS=20
export BASE_RATE_LIMIT=0.5
export WINDOW_SIZE=50
export REQUEST_TIMEOUT=15

# Use chunked processing
export CSV_CHUNK_SIZE=2000

# Disable some logging for performance
export LOG_LEVEL=WARNING
```

# CSV Templates for Bulk API Trigger

## Basic Template (minimal.csv)
```csv
webhook_url
https://httpbin.org/post
https://api.example.com/webhook
https://my-app.com/api/trigger
```

## Standard Template (webhooks.csv)
```csv
webhook_url,method,name,group
https://httpbin.org/post,POST,Test Webhook 1,testing
https://httpbin.org/get,GET,Test Webhook 2,testing
https://api.example.com/webhook,POST,Production Hook,production
```

## Advanced Template (advanced_webhooks.csv)
```csv
webhook_url,method,payload,header,name,group
https://httpbin.org/post,POST,"{""message"": ""Hello World""}","{""Content-Type"": ""application/json""}",JSON POST,api_calls
https://httpbin.org/put,PUT,"{""id"": 123, ""status"": ""active""}","{""Authorization"": ""Bearer token123""}",Update User,user_management
https://api.slack.com/hooks/xxx,POST,"{""text"": ""Deployment complete!""}","{""Content-Type"": ""application/json""}",Slack Notification,notifications
https://discord.com/api/webhooks/xxx,POST,"{""content"": ""Build finished""}","{}",Discord Alert,notifications
```

## Authentication Examples

### Bearer Token
```csv
webhook_url,method,header,name
https://api.example.com/data,GET,"{""Authorization"": ""Bearer your_token_here""}",Authenticated API
```

### API Key
```csv
webhook_url,method,header,name
https://api.example.com/data,GET,"{""X-API-Key"": ""your_api_key_here""}",API Key Auth
```

### Basic Auth (Base64 encoded)
```csv
webhook_url,method,header,name
https://api.example.com/data,GET,"{""Authorization"": ""Basic dXNlcm5hbWU6cGFzc3dvcmQ=""}",Basic Auth
```

### Custom Headers
```csv
webhook_url,method,header,payload,name
https://api.example.com/webhook,POST,"{""Content-Type"": ""application/json"", ""X-Custom-Header"": ""value"", ""User-Agent"": ""BulkTrigger/1.0""}","{""event"": ""user_signup"", ""user_id"": 12345}",Custom Headers
```

## Real-World Use Cases

### 1. User Onboarding Webhooks
```csv
webhook_url,method,payload,header,name,group
https://email-service.com/api/send,POST,"{""to"": ""user@example.com"", ""template"": ""welcome""}","{""Authorization"": ""Bearer email_token""}",Welcome Email,onboarding
https://analytics.com/api/track,POST,"{""event"": ""signup"", ""user_id"": ""123""}","{""X-API-Key"": ""analytics_key""}",Track Signup,onboarding
https://crm.com/api/contacts,POST,"{""email"": ""user@example.com"", ""status"": ""new""}","{""Authorization"": ""Bearer crm_token""}",Add to CRM,onboarding
```

### 2. Deployment Notifications
```csv
webhook_url,method,payload,name,group
https://hooks.slack.com/services/xxx,POST,"{""text"": ""🚀 Deployment started""}",Slack Deploy Start,deployment
https://discord.com/api/webhooks/xxx,POST,"{""content"": ""Build #123 completed successfully""}",Discord Deploy Success,deployment
https://teams.microsoft.com/webhook/xxx,POST,"{""text"": ""Deployment finished""}",Teams Deploy Complete,deployment
```

### 3. Data Sync Operations
```csv
webhook_url,method,payload,header,name,group
https://api.system1.com/sync,POST,"{""action"": ""sync_users""}","{""Authorization"": ""Bearer token1""}",Sync to System 1,data_sync
https://api.system2.com/import,POST,"{""type"": ""user_data""}","{""X-API-Key"": ""key2""}",Import to System 2,data_sync
https://warehouse.com/api/load,POST,"{""source"": ""app_db""}","{""Authorization"": ""Bearer warehouse_token""}",Load to Warehouse,data_sync
```

### 4. Monitoring and Health Checks
```csv
webhook_url,method,name,group
https://api.service1.com/health,GET,Service 1 Health,health_checks
https://api.service2.com/status,GET,Service 2 Status,health_checks
https://database.com/ping,GET,Database Ping,health_checks
https://cdn.com/health,GET,CDN Health,health_checks
```

## Column Descriptions

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `webhook_url` | ✅ | The URL to send the request to | `https://api.example.com/webhook` |
| `method` | ❌ | HTTP method (GET, POST, PUT, etc.) | `POST` |
| `payload` | ❌ | JSON payload for POST/PUT requests | `{"key": "value"}` |
| `header` | ❌ | JSON object with custom headers | `{"Authorization": "Bearer token"}` |
| `name` | ❌ | Friendly name for the request | `User Signup Webhook` |
| `group` | ❌ | Group/category for organization | `notifications` |

## Tips for CSV Creation

1. **Escape JSON properly**: Use double quotes inside JSON strings
2. **Test small batches first**: Start with 5-10 URLs to test your setup
3. **Use meaningful names**: Helps with tracking and debugging
4. **Group related webhooks**: Use the group column for organization
5. **Validate JSON**: Ensure payload and header columns contain valid JSON
6. **URL encode if needed**: Some special characters might need encoding

## Common Issues and Solutions

### Issue: JSON parsing errors
**Solution**: Ensure JSON in payload/header columns uses double quotes
```csv
❌ Wrong: {'key': 'value'}
✅ Correct: {"key": "value"}
```

### Issue: Authentication failures
**Solution**: Check your token format and header structure
```csv
❌ Wrong: "Bearer token123"
✅ Correct: "{\"Authorization\": \"Bearer token123\"}"
```

### Issue: Special characters in URLs
**Solution**: URL encode special characters or use proper escaping
