# Environments - Bulk API Trigger

**Repo**: Cramraika/bulk
**Stack**: Python 3.11 + Docker + SQLite + YAML config

---

## Local Development

### Prerequisites

- Python 3.11+
- pip
- Docker + Docker Compose (optional, for containerized setup)

### Installation

```bash
git clone https://github.com/Cramraika/bulk.git
cd bulk

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

- `requests>=2.31.0` -- HTTP client
- `tqdm>=4.65.0` -- Progress bar
- `PyYAML>=6.0` -- YAML config parsing
- `watchdog>=3.0.0` -- File system monitoring
- `python-dateutil>=2.8.0` -- Date handling

### Running Locally

```bash
# Generate default config
python webhook_trigger.py --create-config

# Interactive mode (guided setup)
python webhook_trigger.py --interactive

# Process a specific CSV file
python webhook_trigger.py file.csv

# Dry run to validate without executing
python webhook_trigger.py file.csv --dry-run

# Custom job name and settings
python webhook_trigger.py file.csv --job-name "My Job" --workers 10 --rate-limit 1.0
```

### Configuration

The primary config file is `config.yaml`. Key sections:

- **watchdog**: File monitoring settings
- **rate_limiting**: Request throttling
- **retry**: Retry and timeout settings
- **deployment**: Keep-alive, skip rows, health check
- **notifications**: Email and Slack
- **database**: SQLite settings
- **csv**: File processing rules

---

## Docker Setup

### Quick Start

```bash
# Create data directory structure
mkdir -p data/{csv/{processed,duplicates,rejected},reports,logs,backups}

# Copy environment file
cp .env.example .env
# Edit .env with your settings

# Build and start
docker-compose up -d

# View logs
docker-compose logs -f bulk-api-trigger

# Check health
curl http://localhost:8000/health
```

### Docker Compose Services

| Service | Port | Description |
|---------|------|-------------|
| `bulk-api-trigger` | 8000 | Main application with health check endpoint |

### Key Docker Environment Variables

Set in `docker-compose.yml` or via `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| `DEPLOYMENT_MODE` | `true` | Enable deployment mode |
| `KEEP_ALIVE` | `true` | Keep container running after processing |
| `WATCHDOG_ENABLED` | `true` | Auto-detect new CSV files |
| `WATCH_PATHS` | `/app/data/csv` | Directory to watch for CSVs |
| `MAX_WORKERS` | `5` | Concurrent request threads |
| `BASE_RATE_LIMIT` | `2.0` | Min delay between requests (seconds) |
| `MAX_RATE_LIMIT` | `10.0` | Max delay on errors |
| `DATABASE_PATH` | `/app/data/webhook_results.db` | SQLite database location |
| `HEALTH_PORT` | `8000` | Health check HTTP port |

### Volumes

```yaml
volumes:
  - ./data:/app/data          # Persistent data (CSVs, DB, logs, reports)
  - ./config.yaml:/app/config.yaml:ro  # Read-only config
```

---

## Environment Variables

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_WORKERS` | `5` | Concurrent threads |
| `BASE_RATE_LIMIT` | `2.0` | Min request delay |
| `STARTING_RATE_LIMIT` | `2.0` | Initial delay |
| `MAX_RATE_LIMIT` | `10.0` | Max delay on errors |
| `ERROR_THRESHOLD` | `0.3` | Error rate to trigger slowdown |
| `WINDOW_SIZE` | `20` | Analysis window size |
| `SKIP_ROWS` | `0` | Rows to skip in CSV |

### API Security

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBHOOK_AUTH_TOKEN` | (none) | Bearer token for the REST API |
| `WEBHOOK_RATE_LIMIT` | `60` | API rate limit (requests/min) |

### Notifications (Optional)

| Variable | Description |
|----------|-------------|
| `SLACK_NOTIFICATIONS` | Enable Slack alerts |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `EMAIL_NOTIFICATIONS` | Enable email alerts |
| `EMAIL_SMTP_SERVER` | SMTP server address |
| `EMAIL_USERNAME` | SMTP username |
| `EMAIL_PASSWORD` | SMTP password (use app password) |
| `EMAIL_RECIPIENTS` | Comma-separated recipient list |

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_ENABLED` | `true` | Enable SQLite job tracking |
| `DATABASE_PATH` | `/app/data/webhook_results.db` | DB file path |
| `DATABASE_BACKUP_ENABLED` | `true` | Auto backup |
| `DATABASE_BACKUP_INTERVAL_HOURS` | `24` | Backup frequency |

---

## REST API Endpoints

When running (Docker or local with health check enabled):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | System health check |
| `/status` | GET | Active jobs status |
| `/trigger` | POST | Trigger single CSV processing |
| `/trigger/batch` | POST | Trigger multiple CSV files |
| `/jobs` | GET | Job history |
| `/jobs/{id}` | GET | Specific job details |
| `/jobs/{id}/errors` | GET | Job errors |
| `/metrics` | GET | System metrics |
| `/resume/status` | POST | Check resume status |
| `/resume/clear` | POST | Clear resume marker |
| `/resume/stats` | GET | All resume markers |

---

## Production / Deployment

**Current status**: GREEN. Runs via Docker.

Deployment options:
- **Docker Compose** (primary): `docker-compose up -d`
- **Kubernetes**: Deployment manifest example in README
- **Railway/Coolify**: Supported via Procfile and Dockerfile

---

## CI/CD

**Pipeline**: GitHub Actions on `ubuntu-latest` (Python 3.11)
**Triggers**: Push/PR to `main` or `master`

| Job | Steps |
|-----|-------|
| CI Quality Gates | Lint (flake8), Security audit (pip-audit), Env validation |
| Docker Build | Builds Docker image (no push) |
| CI Summary | Aggregates results |

---

## Troubleshooting

**Files not processing**
- Check watchdog status: `curl http://localhost:8000/health`
- Verify CSV has `webhook_url` column
- Check `data/csv/rejected/` for invalid files

**High memory usage**
- Reduce `MAX_WORKERS` and chunk size in `config.yaml`

**Rate limit errors**
- Increase `BASE_RATE_LIMIT` value
- The adaptive rate limiter adjusts automatically

**Resume not working**
- Verify `RESUME_ENABLED=true`
- Checkpoints expire after 7 days by default (`RESUME_MAX_AGE_DAYS`)
- Check: `curl http://localhost:8000/resume/stats`

**Container not starting**
- Check Docker logs: `docker-compose logs -f bulk-api-trigger`
- Verify port 8000 is not in use
- Ensure `data/` directory exists with proper permissions
