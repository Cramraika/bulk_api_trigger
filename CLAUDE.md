# bulk — CLAUDE.md v2

**Date:** 2026-04-28 (S11B authoring)
**Supersedes:** v1 (commit-sha pending S11C verification)
**Tier:** C (Stable / Maintenance)

## Identity & Role

`bulk` is a **production-grade CSV-driven bulk webhook/API firing engine** with adaptive throttling, checkpoint/resume, watchdog auto-processing, REST status API, and SQLite job tracking. Single-file Python app (~191 KB). Public Cramraika org repo, MIT. Renamed from `bulk_api_trigger` → `bulk` 2026-04-19. Supersedes archived `webhook_trigger`. Vagary Labs brand: **OSS Utilities** (sponsor-ready).

## Coverage Today (post-PCN-S6/S7/S11A)

Per matrix row `bulk`:

```
Mail | DNS | RP | Orch | Obs | Backup | Sup | Sec | Tun | Err | Wflw | Spec
 U   | U   | U  | U    | T   | U      | T   | U   | NA  | T   | NA   | NA
```

- USED: Mail (only repo currently using transactional SMTP via Postfix on Main per CX-MATRIX inline observation), DNS, RP (LiteSpeed), Orch (Coolify Main), Backup, Sec (Coolify env vars; Infisical pending).
- TRIGGER-TO-WIRE: Obs (per-app Prom + Loki dashboard — Cluster 4), Sup (Cosign post-PR-#50 — Cluster 3), Err (GlitchTip DSN migration to relay — Cluster 2).
- NA: Tun, Wflw, Spec.

## What's Wired

- **Production:** Coolify on Main (Dockerfile build pack).
- **CI:** GitHub Actions — lint (flake8 E9,F63,F7) + pip-audit + Docker build. **GREEN.**
- **CodeQL** weekly + on PR (Python, security-and-quality).
- **Renovate** weekly Sun 22:00 UTC.
- **GlitchTip:** project `bulk-api`; DSN via Coolify env.
- **Loki:** Promtail Docker SD; container `bulk-api-trigger`.
- **REST endpoints:** `/health`, `/status`, `/metrics`, `/jobs`, `/jobs/{id}`, `/jobs/{id}/errors`, `/resume/stats`.

## Stack

- **Runtime:** Python 3.11 (single-file `webhook_trigger.py`; filename retained for internal-API stability)
- **HTTP:** requests + urllib3 + certifi
- **UX:** tqdm
- **Config:** PyYAML + .env
- **Filesystem monitoring:** watchdog
- **Observability:** sentry-sdk → GlitchTip
- **DB:** SQLite (job history, resume markers, file dedup)
- **Packaging:** Docker (`python:3.11-slim`) + docker-compose; health on `:8000/health`

## Roadmap (post-S11A)

### Cluster 2 — GlitchTip DSN migration to relay
- T (DSN flip pending S11C); route via `glitchtip-relay` on Vagary (port 9095) instead of direct GlitchTip ingest.

### Cluster 3 — Cosign per-repo CI fanout
- T (post host_page PR #50 merge); workflow template at `vps_host/.github/workflow-templates/cosign-sign-image.yml`.

### Phase 9.2 — observability hardening (Cluster 4)
- Already exposes `/metrics` Prom-scrape-ready endpoint
- Add Loki label propagation
- Grafana dashboard

### Existing roadmap (carried forward)
- Uptime Kuma monitor on `/health` (no dedicated monitor yet).
- Infisical workspace `bulk-api-trigger` — switch from Coolify env vars to Agent-delivered env file.
- Staging environment (`staging-bulk.chinmayramraika.in`).
- Single-file refactor audit (~191 KB monolith; defer until concrete maintenance pain).
- Repo file rename `webhook_trigger.py` → `bulk.py` (breaking; defer to next major version).
- Adapter shelves (Salesforce / HubSpot / LeadSquared) if sponsor demand materializes.

## ADR Compliance

- **ADR-038 personal-scope:** ✓ — Cramraika org public; MIT.
- **ADR-033 Renovate canonical:** ✓ — `renovate.json` weekly Sun 22:00 UTC.
- **ADR-041 Trivy gate:** T — pending Cosign fanout post-PR-#50.
- **SOC2 risk-register cross-ref:** Sec/Sup track risks.

## Cross-references

- `platform-docs/05-architecture/part-B-service-appendices/products/bulk.md` (or automation tier)
- `vagary-platform/CLAUDE.md` § Dependency Graph (bulk being absorbed as platform notifications-fanout module; A1 refactor at commits `3ae5553` + `3f337a7`; standalone Coolify app keeps running until retirement workstream opens)
- `~/.claude/conventions/universal-claudemd.md` §41 brand architecture (OSS Utilities)

## Migration from v1

**Major v1 → v2 changes:**
1. Per-project-service-matrix row added (notably USED for Mail — surprising-cell flag per CX-MATRIX: only repo currently using transactional SMTP).
2. Cluster 2 GlitchTip relay migration queued S11C.
3. Cluster 3 Cosign CI fanout queued post-PR-#50.
4. Cluster 4 observability hardening flagged Phase 9.2.
5. Renamed-from-webhook_trigger trajectory cited (archived 2026-04-19).
6. Absorption trajectory by vagary-platform noted (A1 refactor done; standalone keeps running).
