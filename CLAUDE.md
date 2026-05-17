## Claude Preamble
<!-- VERSION: 2026-05-14-v46 -->
<!-- SYNC-SOURCE: ~/.claude/conventions/universal-claudemd.md -->

**Universal laws** (§4), **MCP routing** (§6), **Drift protocol** (§11), **Dynamic maintenance** (§14), **Capability resolution** (§15), **Subagent SKILL POLICY** (§16), **Session continuity** (§17), **Decision queue** (§17.a), **Attestation** (§18), **Cite format** (§19), **Three-way disagreement** (§20), **Pre-conditions** (§21), **Provenance markers** (§22), **Redaction rules** (§23), **Token budget** (§24), **Tool-failure fallback** (§25), **Prompt-injection rule** (§26), **Append-only discipline** (§27), **BLOCKED_BY markers** (§28), **Stop-loss ladder** (§29), **Business-invariant checks** (§30), **Plugin rent rubric** (§31), **Context ceilings** (§32), **Doc reference graph** (§33), **Anti-hallucination** (§34), **Past+Present+Future body** (§35), **Project trackers** (§36), **Doc ownership** (§37), **Archive-on-delete** (§38), **Sponsor + white-label** (§39 — moved to `playbooks/commercial-bound.md`), **Doc-vs-code drift** (§40), **Brand architecture** (§41), **Design system integration** (§42 — moved to `playbooks/tier-a-design.md`), **Session cognition** (§43), **Plugin dispatch** (§44), **Cross-repo clusters** (§45), **Tool-cascade workflow** (§46), **Multi-role agent matrix** (§47), **Parsimony / smallest-tool-first** (§48), **Audit triage discipline** (§49), **Source-of-truth matrix** (§50 — universal rows only; cluster-specific rows moved to playbooks), **Composite cascade catalog** (§51 — §51.2/51.4/51.6 moved to playbooks), **Session launch context + unattended-mode contract** (§52), **Recurrence detection + root-cause escalation** (§53). Sub-sections new in v44: **§4.5 cascade-commit exception**, **§17.b stale-P0 escalation**, **§32.5 canonical-doc size ceiling**, **§38.5 HANDOFF lifecycle enforcement**.

**Cluster playbooks** (per-repo `@-import` based on cluster membership): `~/.claude/conventions/playbooks/vps-infra.md` (DNS XOR for VPS-infra repos), `~/.claude/conventions/playbooks/deployed-service.md` (Sentry/Glitchtip XOR + production-incident triage + time-window correlation for repos with prod telemetry), `~/.claude/conventions/playbooks/tier-a-design.md` (Figma/Stitch + design system for Tier A/B), `~/.claude/conventions/playbooks/multi-lang.md` (cross-language refactor cascade for multi-language repos), `~/.claude/conventions/playbooks/commercial-bound.md` (sponsor-readiness + license-aware code-graph routing), `~/.claude/conventions/playbooks/brand-registry.md` (Vagary brand architecture for Vagary-family repos), `~/.claude/conventions/playbooks/bellring-cluster.md` (Bellring server↔extension; v1-stub), `~/.claude/conventions/playbooks/pulseboard-cluster.md` (Pulseboard Android↔Windows; v1-stub), `~/.claude/conventions/playbooks/vagary-cluster.md` (Vagary product cross-repo; v1-stub). **`tech-debt-audit.md`** is Read-on-demand (NOT @-imported) per ENTRY #169 §49 audit-triage discipline — invoked when user requests audit / tech-debt / dead-code work.

**Sources**: `~/.claude/conventions/universal-claudemd.md` (laws, MCP routing, lifecycle, rent rubric, doc-graph, anti-hallucination, brand architecture) + `~/.claude/conventions/project-hygiene.md` (doc placement, cleanup, archive-on-delete, ownership matrix) + cluster playbooks under `~/.claude/conventions/playbooks/` (loaded per-repo via `@-import` in `## Active Cluster Playbooks` section; see list above). Read relevant sections before significant work. Sync: `~/.claude/scripts/sync-preambles.py` (manual cadence; run after any source edit).

## Active Cluster Playbooks (per v40 cluster-split — content auto-inlined)
<!-- BEGIN PLAYBOOKS BLOCK (managed by sync-preambles.py — content inlined; source at ~/.claude/conventions/playbooks/) -->

Source @-imports (declarative pointer; content inlined below since Claude Code does not recursively expand `@-imports` in per-repo CLAUDE.md):
- `@~/.claude/conventions/playbooks/commercial-bound.md`
- `@~/.claude/conventions/playbooks/brand-registry.md`

### Playbook: commercial-bound.md (verbatim from `~/.claude/conventions/playbooks/commercial-bound.md`)

# Commercial-bound + Sponsor-readiness Playbook

**VERSION: 2026-05-06-v1**
Loaded only in repos that are sponsor-ready public OSS, or commercial-bound (sold, embedded in paid product, or redistributed under permissive license). Per-repo `CLAUDE.md` `@-imports` this file when applicable.

Source: extracted verbatim from `~/.claude/conventions/universal-claudemd.md` v39 §39 + §50.2 during v40 cluster-split refactor. No content changes — only relocation so non-commercial / non-sponsor repos don't carry these rules.

**Applies to repos**: `aakhara`, `bellring-server`, `bellring-extension`, `bulk`, `pulseboard` (Android), `pulseboard-desktop`, `tldv_downloader`, `portfolio`, `project-template`, `vagary-platform` (sponsor-ready, has public-vertical surfaces), `host_page` (sponsor-ready landing template).

---

## 1. Sponsor-readiness + white-label pivot (originally §39)

### Sponsor-ready checklist for public repos
- `.github/FUNDING.yml` pointing to `github.com/sponsors/<user>`
- README "Sponsor" section near the top (badge + 1-paragraph ask)
- `LICENSE` (MIT for utilities, AGPL for commercial pressure, other for proprietary)
- At least one GitHub Release (binary attached if applicable, e.g. APK)
- CI green badge

### White-label pivot pattern
When an internal tool goes OSS (e.g. NetworkMonitorCN → **Pulseboard** rebrand 2026-04-19) OR an OSS utility forks into SaaS (e.g. **Bellring** — formerly codenamed Salvo — from sales-notification):

1. **Fork or publish** — new repo with clean name, no internal branding in code
2. **Strip tenant-specific** — remove hardcoded emails/domains/org IDs; parameterize via env/config
3. **Document "Fork + rebrand"** — README section listing the edits a downstream forker makes
4. **Record sibling spec** — `~/.claude/specs/YYYY-MM-DD-<name>-whitelabel.md` if a SaaS pivot
5. **Update inventory** — add to `repo-inventory.md` with sponsor-ready / white-label flags

### Current inventory (2026-04-19)
- **Sponsor-ready public**: tldv_downloader, bulk (renamed from `bulk_api_trigger` 2026-04-19), **pulseboard** (renamed from `NetworkMonitorCN` 2026-04-19), portfolio, project-template, vagary-platform (renamed from `index-of-news` 2026-04-19; flagship vertical retains Index of News brand)
- **White-label pivot applied**: **Bellring** (formerly codenamed Salvo) — repos `bellring-server` + `bellring-extension` (renamed from `sales-notification-backend` / `sales-notification-extension` 2026-04-19). Spec: `~/.claude/specs/2026-04-19-sales-notification-whitelabel.md`.
- **Recently renamed (2026-04-19 Phase 3)**: `sales-notification-backend` → `bellring-server`, `sales-notification-extension` → `bellring-extension`, `NetworkMonitorCN` → `pulseboard`, `training-bot` → `aakhara`.
- **Recently renamed (2026-04-19 Phases 1-2)**: `AI_voice_builder` → `vagary-voice`, `chat-bot` → `anjaan-app`, `bulk_api_trigger` → `bulk`, `index-of-news` → `vagary-platform`. `webhook_trigger` archived (superseded by `bulk`). See `~/.claude/conventions/project-hygiene.md` § Rename Propagation Protocol.
- **Brand umbrella**: Vagary Labs (tech/R&D division of Vagary Life Pvt Ltd; see §41) holds the platform + products + OSS utilities.

## 2. License-aware tool routing (originally §50.2)

Repos categorized as **commercial-bound** (will be sold, embedded in paid product, or redistributed under permissive license):
- `bellring-server`, `bellring-extension` (Bellring SaaS — paid tiers)
- `aakhara` (paid sales-training product)
- `pulseboard`, `pulseboard-desktop` (Public OSS; permissive license required for derivatives)

When working in commercial-bound repos:
- `gitnexus` MCP MAY be used for **read-only investigation** (cypher queries, impact analysis in conversation)
- `gitnexus wiki`, `gitnexus group sync` derivatives, indexed JSON exports MUST NOT be committed/shipped (PolyForm-NC contamination)
- `codegraphcontext` MCP is the canonical graph-derivative source for these repos

When working in **personal/private repos** (vagary-platform, vagary-voice, vagary-earnings, ASM, anjaan-app, internal Cramraika): GitNexus permitted freely.

Per-repo CLAUDE.md should declare classification: `## License classification: commercial-bound` or `## License classification: personal/private`.

### Playbook: brand-registry.md (verbatim from `~/.claude/conventions/playbooks/brand-registry.md`)

# Brand Registry Playbook

**VERSION: 2026-05-07-v1**
Loaded only in Vagary-family repos (per `~/.claude/conventions/repo-inventory.md` §45). Per-repo `CLAUDE.md` `@-imports` this file when applicable.

Source: extracted verbatim from `~/.claude/conventions/universal-claudemd.md` §41 (Brand architecture) during 2026-05-07 cluster-split refinement (ENTRY #168). No content changes — only relocation so non-Vagary repos (e.g. `metabase-cn`, `tldv_downloader`, `torn-smart-scripts`) don't load 64 lines of Vagary brand registry they have no use for.

**Applies to repos**: `vagary-platform`, `vagary-voice`, `anjaan-app`, `aakhara`, `bellring-server`, `bellring-extension`, `bulk`, `pulseboard`, `pulseboard-desktop`, `project-template`, `portfolio` (cross-link only), `host_page`, `vps_host`, `vps-ansible`, `platform-docs`, `vagary-earnings`.

---

## 41. Brand architecture (originally §41 of universal-claudemd.md)

Vagary Life Pvt Ltd is the **corporate parent**. Below it, product and tech activity is organized into named divisions. As of 2026-04-19, one division is formalized: **Vagary Labs** (tech/R&D/platform).

### Structure

```
Vagary Life Pvt Ltd (parent company; registered entity)
└── Vagary Labs (tech/R&D/platform division — vagarylabs.com [PENDING])
    ├── Platform
    │   └── vagary-platform (20-vertical substrate; repo renamed from `index-of-news` 2026-04-19)
    │       └── Index of News (flagship vertical; keeps its own news sub-brand + 6 domains)
    ├── Product brands (each lives as an independent product under its own domain)
    │   ├── Vagary Voice (vagaryvoice.cloud) — commercial voice-AI SaaS
    │   ├── Anjaan (anjaan.online) — Hinglish consumer chat
    │   ├── Bellring (.io/.app/.ai TBD) — whitelabel sale-celebration SaaS; repos `bellring-server` + `bellring-extension` (renamed from `sales-notification-*` 2026-04-19; formerly codenamed Salvo)
    │   ├── Aakhara (aakhara.com pending) — voice sales-training roleplay for BDEs (Sanskrit "आखाड़ा" = practice arena; repo renamed from `training-bot` 2026-04-19). Positioning TBD: could sit as Vagary Voice sub-product or stand alone
    │   └── Hype / Mockline / Kohort (legacy proposed names, superseded by Bellring/Aakhara above)
    └── OSS Utilities
        ├── bulk (renamed from `bulk_api_trigger` 2026-04-19)
        ├── tldv_downloader
        ├── pulseboard (renamed from `NetworkMonitorCN` 2026-04-19; Android OSS, `pulseboard.build` pending)
        └── project-template
```

Additional divisions (media, ops, consulting, etc.) may be added later. Keep Vagary Labs scoped to **tech/platform/R&D**.

### Domain strategy

- **vagarylife.com / vagarylife.in** — corporate parent marketing + investor/careers. TO BE BUILT.
- **vagarylabs.com** — tech/R&D division site. Domain **PENDING PURCHASE** (user flagged). Will host platform docs + OSS index + R&D blog once acquired.
- **Per-product domains** — each commercial product keeps its own brand domain (`vagaryvoice.cloud`, `anjaan.online`, future `hype.sh`, etc.). Product domains do NOT nest under `vagarylabs.com`.
- **chinmayramraika.in** — founder's personal hub; cross-links each Vagary Life / Vagary Labs product in a "projects" section.

### Repo-to-brand mapping (authoritative)

| Repo | Vagary Labs home | Product / sub-brand |
|---|---|---|
| `vagary-platform` | Platform | Holds all 20 verticals; flagship vertical = **Index of News** (news sub-brand, 6 domains) |
| `vagary-voice` | Product brands | **Vagary Voice** (commercial product, `vagaryvoice.cloud`) |
| `anjaan-app` | Product brands | **Anjaan** (consumer product, `anjaan.online`) |
| `aakhara` | Product brands | **Aakhara** (voice sales-training roleplay; `aakhara.com` pending). Renamed from `training-bot` 2026-04-19. Positioning TBD (standalone OR Vagary Voice sub-product) |
| `bellring-server` | Product brands | **Bellring** server (whitelabel sale-celebration SaaS backend; `.io/.app/.ai` TBD). Renamed from `sales-notification-backend` 2026-04-19 (formerly codenamed Salvo) |
| `bellring-extension` | Product brands | **Bellring** extension (Chrome MV3 + Firefox/Edge portable; pairs with `bellring-server`). Renamed from `sales-notification-extension` 2026-04-19 |
| `bulk`, `tldv_downloader`, `pulseboard`, `project-template` | OSS Utilities | Each with its own GitHub + README brand. `pulseboard` renamed from `NetworkMonitorCN` 2026-04-19 (Android OSS; `pulseboard.build` pending) |
| `portfolio` | Personal hub (OUTSIDE Vagary Labs) | `chinmayramraika.in` founder site |
| `host_page`, `platform-docs`, `vps_host`, `n8n-workflows`, `metabase-cn` | Infrastructure (internal to Vagary Labs) | No external product brand |
| `Automated-sales-manager-main` | Client work (CN-internal) | ASM — CN-branded; Cadre whitelabel TBD |
| `google-sheet-sales-manager` | Client work (CN-internal) | Sheetpilot whitelabel TBD |
| `Expense tracker` | Absorbing → Platform (`budget` vertical) | No standalone brand going forward |

### How Claude uses this

- When a repo's description says "product," check the brand table above for positioning.
- The **platform repo** (`vagary-platform`) is *not* a product. It is substrate. Individual verticals (news, budget, …) are the products that ship.
- Don't reinvent brand positioning in per-repo CLAUDE.md — reference this section and defer details to `~/.claude/specs/2026-04-19-brand-rename-proposal.md` (for rationale) + `~/.claude/conventions/repo-inventory.md` (for current state).
- For any new repo: declare its division home in its CLAUDE.md § Status / Brand section and cross-reference here.

### Caveats

- `vagarylabs.com` is **not yet purchased** (2026-04-19). Until acquired, Vagary Labs is an internal organizational concept; do not publish external references to `vagarylabs.com` until DNS is live.
- Additional divisions (media, ops, consulting) may emerge. When they do, add a sibling subtree here + bump VERSION.

<!-- END PLAYBOOKS BLOCK -->

---

# bulk — CLAUDE.md v2

**Date:** 2026-04-28 (S11B authoring)
**Supersedes:** v1 (commit-sha pending S11C verification)
**Tier:** C (Stable / Maintenance)

## Identity & Role

`bulk` is a **production-grade CSV-driven bulk webhook/API firing engine** with adaptive throttling, checkpoint/resume, watchdog auto-processing, REST status API, and SQLite job tracking. Single-file Python app (~191 KB). Public Cramraika org repo, MIT. Renamed from `bulk_api_trigger` → `bulk` 2026-04-19. Supersedes archived `webhook_trigger`. Vagary Labs brand: **OSS Utilities** (sponsor-ready).

<!-- Coolify-DB live-truth as of platform-docs ENTRY #316-T20260512T140000Z (Wave 5-D Substrate Drift Compound, W5-D-CROSS-REPO-PRS subagent) — no `bulk` app present in Coolify (queried `applications` table via SSH fallback to vagary-core-1 since coolify MCP `localhost:3000` unreachable from this Mac session). Confirms DECOMMISSIONED. §53 R2 recurrence path-(a) preventive root-cause per spec `~/.claude/specs/R2-claudemd-coolifydb-drift-rootcause.md` + memory rule `feedback_claudemd_coolify_db_truth_sync.md`. Repo retained as historical artefact; bulk functionality absorbed into vagary-platform notifications-fanout module (A1 refactor commits `3ae5553` + `3f337a7`). -->

## Coverage Today (post-PCN-S6/S7/S11A; DECOMMISSIONED 2026-05-12)

Per matrix row `bulk` (T = decommissioned tombstone; standalone runtime retired):

```
Mail | DNS | RP | Orch | Obs | Backup | Sup | Sec | Tun | Err | Wflw | Spec
 NA  | NA  | NA | NA   | NA  | NA     | T   | NA  | NA  | NA  | NA   | NA
```

- **Production status: DECOMMISSIONED** — no Coolify app present (verified ENTRY #316 Coolify-DB query); all runtime dimensions therefore NA.
- TOMBSTONE (T): Sup dimension only — repo retained on GitHub as historical artefact; absorbed by `vagary-platform` notifications-fanout module.
- Prior matrix row (pre-decommission, retained for historical context only): `U | U | U | U | T | U | T | U | NA | T | NA | NA`.

## What's Wired

- **Production:** **DECOMMISSIONED.** No Coolify app exists (live verification via Coolify-DB query: zero `bulk*` rows in `applications` table). The prior claim "Coolify on Main (Dockerfile build pack)" was drift; the Coolify deployment was retired when `vagary-platform` absorbed bulk's notifications-fanout responsibility (A1 refactor commits `3ae5553` + `3f337a7`). See `vagary-platform/CLAUDE.md` § Dependency Graph for the absorbed module's live home.
- **Historical references (no longer live):** ~~GlitchTip project `bulk-api`; DSN via Coolify env~~ — N/A post-decommission; ~~Loki Promtail Docker SD container `bulk-api-trigger`~~ — N/A; ~~REST endpoints~~ — N/A (the application is no longer running anywhere).
- **CI:** GitHub Actions — lint (flake8 E9,F63,F7) + pip-audit + Docker build. **GREEN** — retained for any future reactivation but no images currently pushed to a live registry.
- **CodeQL** weekly + on PR (Python, security-and-quality) — repo-level scanning preserved.
- **Renovate** weekly Sun 22:00 UTC — dependency hygiene preserved on the artefact.

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

---

## VPS Service Navigation

`bulk` is **DECOMMISSIONED** (no Coolify app — verified ENTRY #316 Coolify-DB query; notifications-fanout absorbed by `vagary-platform`). It currently consumes no live dual-VPS service. This stub is retained so a future revival session has the navigation pattern; canonical service playbooks live in `platform-docs/02-governance/service-playbooks/`.

| Service | State | If revived |
|---|---|---|
| **Infisical** (secrets) | No active project — `bulk` is OSS; credentials are downstream-user-supplied (W12: nothing to onboard) | Create a `bulk` Infisical project per `service-playbooks/substrate/infisical.md` §6 wiring guide + §9.5 |
| **Coolify** (orchestration) | Decommissioned — no app row in Coolify-DB | Register via Coolify API per `service-playbooks/substrate/coolify.md` §5 |
| **Observability** | n/a — no running deployment | `service-playbooks/observability/*.md` |
