# Bulk (formerly Bulk API Trigger)

## Claude Preamble
<!-- VERSION: 2026-04-19-v12 -->
<!-- SYNC-SOURCE: ~/.claude/conventions/universal-claudemd.md -->

**Universal laws** (§4), **MCP routing** (§6), **Drift protocol** (§11), **Dynamic maintenance** (§14), **Capability resolution** (§15), **Subagent SKILL POLICY** (§16), **Session continuity** (§17), **Decision queue** (§17.a), **Attestation** (§18), **Cite format** (§19), **Three-way disagreement** (§20), **Pre-conditions** (§21), **Provenance markers** (§22), **Redaction rules** (§23), **Token budget** (§24), **Tool-failure fallback** (§25), **Prompt-injection rule** (§26), **Append-only discipline** (§27), **BLOCKED_BY markers** (§28), **Stop-loss ladder** (§29), **Business-invariant checks** (§30), **Plugin rent rubric** (§31), **Context ceilings** (§32), **Doc reference graph** (§33), **Anti-hallucination** (§34), **Past+Present+Future body** (§35), **Project trackers** (§36), **Doc ownership** (§37), **Archive-on-delete** (§38), **Sponsor + white-label** (§39), **Doc-vs-code drift** (§40), **Brand architecture** (§41), **Design system integration** (§42).

**Sources**: `~/.claude/conventions/universal-claudemd.md` (laws, MCP routing, lifecycle, rent rubric, doc-graph, anti-hallucination, brand architecture) + `~/.claude/conventions/project-hygiene.md` (doc placement, cleanup, archive-on-delete, ownership matrix) + `~/.claude/conventions/design-system.md` (per-repo Tier A/B/C design posture, Stitch wiring). Read relevant sections before significant work. Re-audit due **2026-07-19**. Sync: `~/.claude/scripts/sync-preambles.py`.

## Project Scope & Vision

Production-grade CSV-driven bulk webhook/API firing engine with adaptive throttling, checkpoint/resume, and operator observability. The single-file Python app (`webhook_trigger.py`, ~191 KB) supersedes the earlier `webhook_trigger` repo (archived on GitHub 2026-04-19) by adding SQLite job tracking, filesystem watchdog auto-processing, REST status API, Slack/email notifications, file deduplication, and container-grade health checks. Public repo under Cramraika org (renamed from `bulk_api_trigger` to `bulk` 2026-04-19) — positioned as a reusable operations tool (sponsor CTA in README) for triggering thousands of webhooks from CSV inputs without maintaining a bespoke queue worker.

**Vision at pinnacle**: Self-healing webhook fan-out service that drops into any ops pipeline: operator drops CSV into a watched dir, engine validates columns, fires requests at a target API with adaptive pace, recovers from container restarts at the exact row, and surfaces progress via Slack + REST. No queue broker, no Redis, no orchestration — just the Python, SQLite, and Docker.

## Status & Tier

- **Tier**: C (Stable / Maintenance) — live, in routine use, no active feature development; accepts dependency bumps via Renovate + security fixes
- **Deployment status**: GREEN — CI green, Docker image builds clean, production use via Coolify / local docker-compose
- **Activity**: Dependency / CI / hygiene commits only since 2026-03 (see Past / Phase History)
- **Public**: Yes — under Cramraika org, MIT license

## Stack

- **Runtime**: Python 3.11 (single-file architecture: `webhook_trigger.py`)
- **HTTP**: `requests>=2.31.0`, `urllib3>=2.6.3`, `certifi>=2023.0.0`
- **Progress / UX**: `tqdm>=4.65.0`
- **Config**: `PyYAML>=6.0` (`config.yaml`) + `.env` env var overrides
- **Filesystem monitoring**: `watchdog>=3.0.0`
- **Date handling**: `python-dateutil>=2.8.0`, `idna>=3.4`, `charset-normalizer>=3.0.0`
- **Observability**: `sentry-sdk>=2.18.0` (wired to GlitchTip via DSN)
- **Database**: SQLite (stdlib) — job history, per-request tracking, resume markers, file dedup
- **Packaging / Deploy**: Docker (`python:3.11-slim`) + docker-compose; health check on `:8000/health`
- **CI**: GitHub Actions — lint (flake8 E9,F63,F7), security audit (pip-audit), Docker build
- **Security**: CodeQL (Python, `security-and-quality` queries, weekly + on PR)
- **Dependency automation**: Renovate (self-hosted via GitHub Actions, weekly Sun 22:00 UTC)

## Build / Test / Deploy

```bash
# Docker (recommended)
docker-compose up -d                              # Build + start
docker-compose logs -f bulk-api-trigger            # Tail logs
docker-compose down                                # Stop

# Local dev
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python webhook_trigger.py --create-config          # Generate default config.yaml
python webhook_trigger.py --interactive            # Guided setup
python webhook_trigger.py file.csv --dry-run       # Validate without firing
python webhook_trigger.py file.csv --workers 10 --rate-limit 1.0

# Lint (matches CI)
flake8 *.py --max-line-length=120 --select=E9,F63,F7

# Security audit (matches CI)
pip-audit --desc

# Health check
curl http://localhost:8000/health
curl http://localhost:8000/status
```

**Deployment**: Coolify app (Dockerfile build pack) or docker-compose directly on a VPS. Container keeps itself alive with `KEEP_ALIVE=true` + watchdog loop. `Procfile` retained for Railway compatibility (legacy; not primary path).

## Key Directories

| Path | Purpose |
|---|---|
| `webhook_trigger.py` | Monolithic app — rate limiter, HTTP client, watchdog, REST API, SQLite, notifications |
| `config.yaml` | Default runtime config (watchdog, rate limit, retry, notifications, DB, CSV) |
| `.env.example` | Env var reference — 60+ tunables (rate limit, CSV paths, DB, notifications, n8n) |
| `Dockerfile` | `python:3.11-slim` base, creates `/app/data/{csv,reports,logs,backups}` tree |
| `docker-compose.yml` | Single-service stack, port 8000, volume-mounts `./data` and `config.yaml:ro` |
| `Procfile` | Railway legacy deploy hint (not primary) |
| `requirements.txt` | 10 pinned direct deps |
| `renovate.json` | Weekly dependency update config |
| `.github/workflows/` | `ci.yml` (lint + pip-audit + Docker build), `codeql.yml` (SAST), `renovate.yml` |
| `docs/ENVIRONMENTS.md` | Deployment environment reference (local / Docker / env vars / REST API) |
| `data/` | Runtime data root — `csv/{processed,duplicates,rejected}`, `reports/`, `logs/`, `backups/`, `webhook_results.db` (gitignored) |
| `SECURITY.md` | Security policy + reporting contact |
| `LICENSE` | MIT |

**Never save working files to the repo root.** Scratch goes to `/tmp/claude-scratch/` per `project-hygiene.md`.

## External Services

### Runtime integrations
- **Target webhooks / APIs** — whatever URLs land in the CSV `webhook_url` column (LeadSquared, Slack webhooks, n8n workflows, bespoke internal APIs)
- **Slack incoming webhook** (optional) — configured per-deploy via `SLACK_WEBHOOK_URL`. Notifies on completion, new-file detection, and progress milestones
- **SMTP server** (optional) — Gmail-compatible SMTP for email alerts; app password required
- **n8n** (`n8n.chinmayramraika.in`) — can trigger / receive workflows. Env: `N8N_WEBHOOK_URL`, `N8N_API_KEY` (unique per project). Workflow defs in private `github.com/Cramraika/n8n-workflows`

### MCPs (per universal-claudemd.md §6)
- `sentry` (local) — GlitchTip DSN surfaced via Sentry SDK; errors land in `errors.chinmayramraika.in`
- `slack` (local + claude.ai) — for ops comms / deploy channel posts
- `grafana` (claude.ai) — Loki logs (via Promtail Docker SD) + container metrics dashboards
- `infisical` (claude.ai) — secret management (pending dedicated workspace; currently Coolify env vars)
- **Disabled for this repo** (no UI): `figma` — per universal §6 session-aware disable guidance

## Observability

- **GlitchTip** (`errors.chinmayramraika.in`) — project `bulk-api`, DSN delivered via Coolify env var. Initialized in the app via `sentry-sdk` on startup.
- **Loki** (`grafana.chinmayramraika.in` → Explore) — container stdout/stderr collected by Promtail Docker SD; filter by `container_name=bulk-api-trigger`.
- **Grafana** (`grafana.chinmayramraika.in`) — infrastructure / error-rate / container dashboards.
- **Uptime Kuma** (`status.chinmayramraika.in`) — **no dedicated monitor yet** (Roadmap item: add `/health` HTTP monitor).
- **Netdata** (`monitor.chinmayramraika.in`) — system-level Docker / host metrics.
- **Internal endpoints**: `/health`, `/status`, `/metrics`, `/jobs`, `/jobs/{id}`, `/jobs/{id}/errors`, `/resume/stats` (see `docs/ENVIRONMENTS.md` full REST table).

### Notifications
- **Slack**: Deploys → #deploys, Errors → #errors, CI → #ci, Kuma alerts → #cron
- **Telegram**: Critical alerts → `@vpsmgr_bot` (chat 710228663)
- **Email**: Netdata + Uptime Kuma → chinu.ramraika@gmail.com

## Dependency Graph

**Upstream (what this depends on)**
- Python 3.11 runtime (containerized)
- Target webhook APIs (external — LeadSquared, Slack, n8n, etc.)
- SQLite (embedded, no external DB server)
- Coolify / docker-compose host (deployment)
- n8n (optional workflow hook)

**Downstream (what depends on this)**
- Ops workflows at Coding Ninjas (webhook bulk-fires)
- Any ad-hoc migration / notification campaign using CSV-driven HTTP triggers
- External users of the public repo (sponsor-facing)

**Supersession / relation**
- **Supersedes `webhook_trigger`** (`~/Documents/Github/webhook_trigger/` — archived on GitHub 2026-04-19) — earlier single-file script, same CSV-driven webhook idea. `webhook_trigger` local dir is retained as the "lite" reference implementation (no SQLite, no watchdog, no REST, no adaptive rate, no resume). All new work and deployments use this repo (`bulk`, renamed from `bulk_api_trigger` 2026-04-19).
- **Peer**: `tldv_downloader` — different domain (meeting downloads) but same single-file Python + Tier C posture.

## Roadmap / Future

1. **Uptime Kuma monitor** — add `/health` monitor on `status.chinmayramraika.in` so outages surface via Slack/Telegram alert paths already wired (Observability above).
2. **Infisical workspace** — create `bulk-api-trigger` workspace; switch from Coolify env vars to Infisical Agent → env file → docker-compose `env_file` mount. Blocker: general Infisical rollout cadence across the fleet.
3. **Staging environment** — not yet configured. Planned: `staging-bulk.chinmayramraika.in`, `staging` branch, Coolify clone or `docker-compose.staging.yml`. Low priority while the app is stable.
4. **Dashboard surface** — `docker-compose.yml` has a commented-out `dashboard` nginx service. Defer unless operator need emerges (status API + Grafana currently suffice).
5. **Adapter shelves (per README sponsor CTA)** — potential packaged adapters for Salesforce, HubSpot, LeadSquared (structured envelopes + auth helpers) if sponsor demand materializes. Not currently scoped.
6. **Single-file refactor audit** — application file (`webhook_trigger.py` — filename retained for internal-API stability) is ~191 KB in one module. Splitting into `core/`, `api/`, `notifications/`, `watchdog/` submodules deferred until a concrete maintenance pain emerges (readability vs. diff-stability trade).
7. **Repo file rename pending** — consider renaming the application file from `webhook_trigger.py` to `bulk.py` to match the repo rename. Blocker: breaking change for any operator with scripts invoking the file by name. Defer to next major version bump.

## Past / Phase History

- **2026-04-19** — Preamble synced to v8 (this pass).
- **2026-04-13 → 2026-04-18** — Repo hygiene wave: added CODEOWNERS, PR template, issue templates, SECURITY.md, CodeQL workflow, Dependabot, Renovate. Then pinned `renovatebot/github-action@v46.1.8`. CLAUDE.md preamble cadence: v3 → v4 (re-audit status) → v5 (dynamic maintenance) → v6 (stability/resilience) → v7 (full template-rules catalog) → v7 compact → v8.
- **2026-04-12** — Wired Sentry / GlitchTip SDK for bulk webhook worker. Added `.dockerignore` + Claude settings. Snyk PRs merged for dependency CVEs. Renovate GitHub Action enabled.
- **2026-03 → 2026-04-12** — Dependabot-driven dependency bumps (setup-python 5→6, codeql-action 3→4, buildx-action 3→4, build-push-action 5→7). VPS Services Integration block added to CLAUDE.md. n8n workflow automation note added.
- **Pre-2026-03** — Single-file app consolidated; Docker + docker-compose deployment settled. Adaptive rate limiter + SQLite job tracking + REST status API + watchdog auto-processing landed together (README calls this v2.0). GitHub Sponsors CTA added.
- **Supersession from `webhook_trigger`** — key lift from lite version: SQLite history, REST API, watchdog, resume, adaptive rate, notifications.

## Known Limitations

- **Single-file app** — `webhook_trigger.py` ~191 KB mixes concerns (rate limiter, HTTP, REST, watchdog, SQLite, notifications). Readable, but greenfield contributors need to navigate carefully.
- **Single-threaded rate limiter** — adaptive pacing uses a single `RateLimiter` instance; at very high `MAX_WORKERS` values it can be the bottleneck before the target API is.
- **No native queue backend** — works for "fire CSV → hundreds/low-thousands of requests". For jobs in the 100K+ range a real queue (Redis/Celery) would outperform SQLite checkpointing.
- **No staging env** — all production changes land on main → Coolify; rely on Docker local `--dry-run` + dry-run flag for validation.
- **No Uptime Kuma monitor yet** — outages only surface via GlitchTip / Loki / manual `curl /health`.
- **Infisical workspace pending** — secrets still managed via Coolify env vars.
- **File dedup via SHA hash only** — renaming a file after processing defeats dedup; operator must clean `processed/` periodically if rerunning historical batches.
- **Resume max-age 7 days default** — interrupted jobs older than a week are silently ignored on next run (per `RESUME_MAX_AGE_DAYS`).

## Doc Maintainers

- **Primary**: Chinmay (`chinu.ramraika@gmail.com`) — code + deployment + docs.
- **Auto-updated by Claude**: stack table, build commands, env var reference, VPS wiring (must match reality of `config.yaml` + `.env.example` + `docker-compose.yml`).
- **Frozen unless user asks**: README marketing copy + sponsor CTA (user owns positioning). Living per `project-hygiene.md` — Claude may update feature/config sections to track code, but preserve the marketing framing.

## Deviations from Universal Laws

None known. All universal-claudemd.md laws and project-hygiene.md conventions apply as-is:

- Single-file app is an accepted pattern for this domain (Tier C, stable, low change velocity) — not a deviation, just a concentration of code in one module.
- README is **living** per `project-hygiene.md` § README curation (public CLI tool → README is the user manual).
- Preamble is compact form per universal-claudemd.md § 9; no inline expansion.
