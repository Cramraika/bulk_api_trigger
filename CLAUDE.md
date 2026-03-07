# Bulk API Trigger

## Project Overview
- **Stack**: Python 3.11 + Docker + SQLite + YAML config
- **Description**: Production-ready platform for triggering thousands of webhooks/APIs with REST API endpoints, resume support, file watchdog, auto-processing, Slack/email notifications, and real-time health monitoring.
- **Tier**: C (Stable/Maintenance)

## File Organization
- Never save working files to root folder
- `webhook_trigger.py` - Main application (single-file architecture)
- `config.yaml` - Application configuration (watchdog, rate limiting, retry, notifications, database, CSV settings)
- `Dockerfile` / `docker-compose.yml` - Container deployment
- `data/` - Runtime data (CSV files, logs, reports, backups, SQLite DB)

## Build & Test
```bash
# Docker deployment (recommended)
docker-compose up -d                              # Start container
docker-compose logs -f bulk-api-trigger            # View logs

# Local development
pip install -r requirements.txt
python webhook_trigger.py --create-config          # Generate default config
python webhook_trigger.py --interactive            # Interactive mode
python webhook_trigger.py file.csv --dry-run       # Dry run to validate

# Health check
curl http://localhost:8000/health
curl http://localhost:8000/status
```

## Environment Variables
- See `config.yaml` for full configuration reference
- `WEBHOOK_AUTH_TOKEN` - Bearer token for API authentication
- `SLACK_WEBHOOK_URL` - Slack notification webhook
- `DATABASE_PATH` - SQLite database path (default: `/app/data/webhook_results.db`)

## Security Rules
- NEVER hardcode API keys, secrets, or credentials in any file
- NEVER pass credentials as inline env vars in Bash commands
- NEVER commit .env, .claude/settings.local.json, or .mcp.json to git
- Always validate user input at system boundaries
