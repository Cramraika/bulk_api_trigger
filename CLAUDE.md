# Bulk API Trigger

## Claude Preamble (preloaded universal rules)
<!-- VERSION: 2026-04-18-v4 -->
<!-- SYNC-SOURCE: ~/.claude/conventions/universal-claudemd.md -->

### Laws
- Never hardcode secrets. Use env vars + `.env.example`.
- Don't commit unless asked. Passing tests ≠ permission to commit.
- Never skip hooks (`--no-verify`) unless user asks. Fix root cause.
- Never force-push to main. Prefer NEW commits over amending.
- Stage files by name, not `git add -A`. Avoids .env/credential leaks.
- Conventional Commits (`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:`). Subject ≤72 chars.
- Integration tests hit real systems (DB, APIs); mocks at unit level only.
- Never delete a failing test to make the build pass.
- Three similar lines > premature abstraction.
- Comments explain non-obvious WHY, never WHAT.
- Destructive ops (`rm -rf`, `git reset --hard`, force-push, drop table) → ask first.
- Visible actions (PRs, Slack, Stripe, Gmail) → confirm unless pre-authorized.

### Doc & scratch placement
- Plans: `docs/plans/YYYY-MM-DD-<slug>.md`
- Specs: `docs/specs/YYYY-MM-DD-<slug>.md`
- Architecture: `docs/architecture/`
- Runbooks: `docs/runbooks/`
- ADRs: `docs/adrs/ADR-NNN-<slug>.md`
- Scratch/temp: `/tmp/claude-scratch/<purpose>-YYYY-MM-DD.ext`
- Never create README unless explicitly asked.

### MCP routing (pull-tier — invoke when task signal matches)
**Design / UI:**
- Figma URL / design ref → `figma` / `claude_ai_Figma` (`get_design_context`)
- Design system / variants → `stitch`

**Engineer / SRE:**
- Prod error → `sentry`
- Grafana dashboard / Prometheus query / Loki logs / OnCall / Incidents → `grafana`
- Cloudflare Workers / D1 / R2 / KV / Hyperdrive → `claude_ai_Cloudflare_Developer_Platform`
- Supabase ops → `supabase`
- Stripe payment debugging → `stripe`

**Manager / Planner / Writer:**
- Linear issues → `linear`
- Slack comms → `slack` / `claude_ai_Slack`
- Gmail drafts/threads/labels → `claude_ai_Gmail`
- Calendar events → `claude_ai_Google_Calendar`
- Google Drive file access → `claude_ai_Google_Drive`

**Analyst / Marketer:**
- PostHog analytics/funnels → `posthog`
- Grafana time-series / Prometheus → `grafana`

**Security:**
- Secrets management → `infisical`

**Knowledge / Architecture:**
- Cross-repo knowledge ("which repos use X", "patterns across products") → `memory`
- Within-repo state → flat-file auto-memory (`~/.claude/projects/<id>/memory/`)

**Rule of thumb:** core tools (Read/Edit/Write/Glob/Grep/Bash) for local ops; MCPs for external-system state. Don't use MCPs as a slow alternative to core tools.

### Response discipline
- Tight responses — match detail to task.
- No "Let me..." / "I'll now...". Just do.
- End-of-turn summary: 1-2 sentences.
- Reference `file:line` when pointing to code.

### Drift detection
On first code-edit of the session, verify this preamble's VERSION tag matches `~/.claude/conventions/universal-claudemd.md` § 9. If stale, propose sync to user before proceeding.

### Re-audit status (check at session start in global workspace)
Last run: **2026-04-18-v1**. Next due: **2026-07-18** OR when `/context` > 50%, whichever first.
Methodology spec: `~/.claude/specs/2026-04-18-plugin-surface-audit.md`.
On session start in `~/Documents/Github/`, if today's date > next-due OR context feels heavy: remind user "Plugin audit overdue — want to run it per methodology spec?"

### Full detail
- Universal laws + architecture: `~/.claude/conventions/universal-claudemd.md`
- Doc placement + cleanup: `~/.claude/conventions/project-hygiene.md`
- Latest audit: `~/.claude/specs/2026-04-18-plugin-surface-audit.verdicts.md`

## Products

| Product | What It Does | Who Uses It | Status |
|---------|-------------|-------------|--------|
| CSV-to-Webhook Engine | Reads CSV files and fires thousands of HTTP requests to arbitrary webhook/API endpoints | Operations team, developers | Live |
| File Watchdog | Monitors a directory for new CSV files and auto-processes them without manual intervention | Automated pipelines | Live |
| Job Resume System | Tracks progress in SQLite so interrupted jobs resume from the last successful row | Operations team | Live |
| Adaptive Rate Limiter | Dynamically adjusts request pacing based on error rates to avoid overwhelming target APIs | Automated (internal) | Live |
| REST Status API | Health check and job status endpoints for monitoring containers and integrating with dashboards | Monitoring systems (Uptime Kuma, Netdata) | Live |
| Notification System | Slack and email alerts on job completion, file detection, and progress milestones | Operations team | Live |

## Product Details

### CSV-to-Webhook Engine
- **User journey**: Drop a CSV file with `webhook_url` column (+ optional method, payload, header, name, group) -> Engine validates columns -> Fires requests with configurable parallelism (up to 5 workers) -> Generates JSON report with success/failure counts
- **Success signals**: 10,000+ webhooks fired with < 1% failure rate; job reports show clean completion
- **Failure signals**: High failure rate from misconfigured CSVs; rejected files pile up without operator awareness

### File Watchdog
- **User journey**: Place CSV in `/app/data/csv/` -> Watchdog detects new file (3s debounce) -> Auto-queues for processing -> Processed files archived to `csv/processed/`, duplicates to `csv/duplicates/`, invalid to `csv/rejected/`
- **Success signals**: Zero manual intervention for routine webhook jobs; new files processed within seconds
- **Failure signals**: Watchdog misses files; queue backs up beyond 100 items; debounce too aggressive

### Job Resume System
- **User journey**: Job starts processing 5,000 rows -> Container restarts at row 3,200 -> Container comes back up -> Job resumes from row 3,201 automatically
- **Success signals**: No duplicate webhook calls after restart; zero data loss on crash recovery
- **Failure signals**: Duplicate calls sent; resume picks wrong offset; SQLite DB corrupted

### Adaptive Rate Limiter
- **User journey**: Engine starts at 2.0s delay between requests -> Target API starts returning 429s -> Engine detects > 30% error rate in 20-request window -> Delay ramps up toward 10.0s max -> Error rate drops -> Delay ramps back down
- **Success signals**: Target APIs never rate-limit the engine; throughput maximized within safe bounds
- **Failure signals**: Engine hammers a rate-limited API; delay never recovers after transient errors

### REST Status API
- **User journey**: `GET /health` returns container health -> `GET /status` returns current job progress, queue depth, and system metrics
- **Success signals**: Uptime Kuma shows green; dashboards display accurate job progress
- **Failure signals**: Health endpoint returns 200 but engine is stuck; status metrics are stale

### Notification System
- **User journey**: Configure Slack webhook and/or email SMTP in `config.yaml` -> Receive alerts on job completion (success/failure counts), new file detection, and progress every N items
- **Success signals**: Operator learns about failed jobs within minutes; no alert fatigue from excessive notifications
- **Failure signals**: Slack webhook expired silently; email alerts land in spam; progress notifications fire too frequently

## Tech Reference

### Stack
- **Runtime**: Python 3.11 (single-file architecture: `webhook_trigger.py`)
- **Database**: SQLite (job tracking, resume support, deduplication)
- **Config**: YAML (`config.yaml`)
- **Deployment**: Docker + docker-compose
- **File monitoring**: watchdog library
- **HTTP**: requests library with retry logic
- **Tier**: C (Stable/Maintenance)

### File Organization
- Never save working files to root folder
- `webhook_trigger.py` - Main application (single-file architecture)
- `config.yaml` - Application configuration (watchdog, rate limiting, retry, notifications, database, CSV settings)
- `Dockerfile` / `docker-compose.yml` - Container deployment
- `data/` - Runtime data (CSV files, logs, reports, backups, SQLite DB)

### Build & Test
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

### Environment Variables
- See `config.yaml` for full configuration reference
- `WEBHOOK_AUTH_TOKEN` - Bearer token for API authentication
- `SLACK_WEBHOOK_URL` - Slack notification webhook
- `DATABASE_PATH` - SQLite database path (default: `/app/data/webhook_results.db`)

### n8n Workflow Automation

This project can trigger and receive n8n workflows at `https://n8n.chinmayramraika.in`.

- **Webhook URL:** Set in `N8N_WEBHOOK_URL` env var
- **API Key:** Set in `N8N_API_KEY` env var (unique per project)
- **Auth Header:** `X-API-Key: <N8N_API_KEY>`
- **Workflow repo:** github.com/Cramraika/n8n-workflows (private)

## VPS Services Integration

This repo is wired into the following VPS services:

### Observability
- **GlitchTip** (errors.chinmayramraika.in): Project `bulk-api`, DSN in Infisical (no dedicated workspace — use Coolify env vars)
- **Loki** (grafana.chinmayramraika.in → Explore): Logs auto-collected via Promtail Docker SD
- **Grafana** (grafana.chinmayramraika.in): Dashboards for container logs, error rate, infrastructure
- **Uptime Kuma** (status.chinmayramraika.in): No dedicated monitor yet (add one for `/health` endpoint)
- **Netdata** (monitor.chinmayramraika.in): System metrics + custom alarms

### Notifications
- **Slack**: Deploys → #deploys, Errors → #errors, CI → #ci, Kuma alerts → #cron
- **Telegram**: Critical alerts → @vpsmgr_bot (chat 710228663)
- **Email**: Netdata + Uptime Kuma → chinu.ramraika@gmail.com

### Secrets
- **Infisical** (secrets.chinmayramraika.in): No dedicated workspace yet. Secrets managed via Coolify env vars.
- When Infisical workspace is created: Delivery via Agent on VPS → env file → docker-compose env_file mount.

### Staging
- Not yet configured. When ready:
- URL: `https://staging-bulk.chinmayramraika.in`
- Branch: `staging`
- Deploy: Coolify clone / docker-compose.staging.yml

### Security Rules
- NEVER hardcode API keys, secrets, or credentials in any file
- NEVER pass credentials as inline env vars in Bash commands
- NEVER commit .env, .claude/settings.local.json, or .mcp.json to git
- Always validate user input at system boundaries
