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

### 3. Coolify Deployment

1. **Create new project** in Coolify
2. **Upload files** to your repository:
   ```
   webhook_trigger.py
   requirements.txt
   Procfile
   config.yaml (optional)
   your_csv_files.csv
   ```
3. **Set environment variables**:
   ```bash
   DEPLOYMENT_MODE=true
   KEEP_ALIVE=true
   MAX_WORKERS=3
   BASE_RATE_LIMIT=2.0
   JOB_NAME=My Coolify Job
   ```
4. **Deploy** and monitor logs

### 4. Railway Deployment

1. **Connect your GitHub repo** to Railway
2. **Configure environment variables**:
   ```bash
   RAILWAY_ENVIRONMENT=production
   KEEP_ALIVE=true
   MAX_WORKERS=5
   EMAIL_NOTIFICATIONS=true
   EMAIL_SMTP_SERVER=smtp.gmail.com
   EMAIL_USERNAME=your_email@gmail.com
   EMAIL_PASSWORD=your_app_password
   EMAIL_RECIPIENTS=admin@example.com
   ```
3. **Deploy** from Railway dashboard

## 📁 File Structure

```
bulk-api-trigger/
├── webhook_trigger.py          # Main application
├── requirements.txt            # Python dependencies
├── Procfile                   # Process configuration
├── Dockerfile                 # Docker configuration
├── docker-compose.yml         # Docker Compose setup
├── config.yaml               # Configuration file (optional)
├── csv/                      # CSV files directory
│   ├── webhooks.csv          # Main webhook file
│   ├── http_triggers.csv     # Additional webhook file


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
