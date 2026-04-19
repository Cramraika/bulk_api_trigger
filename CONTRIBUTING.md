# Contributing to bulk

Thanks for your interest in contributing! This project is a production-ready bulk webhook/API trigger platform, and contributions of all sizes are welcome — bug reports, docs improvements, new adapters (Salesforce, HubSpot, LeadSquared, etc.), and core engine improvements.

## Ways to contribute

- **🐛 Bug reports** — open an issue with a minimal reproducer (CSV sample, config snippet, expected vs actual)
- **📝 Documentation** — typo fixes, clearer examples, runbook additions
- **🧩 New adapters** — wrap a third-party API as a CSV-driven webhook
- **⚡ Core improvements** — rate-limiting strategies, retry policies, observability hooks
- **🧪 Tests** — coverage for edge cases and regressions

## Development setup

```bash
git clone https://github.com/Cramraika/bulk.git
cd bulk
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then edit values
python webhook_trigger.py
```

## Pull request workflow

1. Fork the repo and create a feature branch from `main`:
   ```bash
   git checkout -b feat/your-change
   ```
2. Make your changes. Keep diffs focused — one concern per PR.
3. If you change behavior, update the README and any affected docs under `docs/`.
4. If you touch the engine, add or update a test that exercises the new path.
5. Run the project locally and confirm it still starts cleanly.
6. Push your branch and open a PR against `main`. Use the PR template; describe what changed and why.

## Code style

- **Python**: PEP 8, type hints encouraged on new code
- **YAML/JSON**: 2-space indent
- **Commit messages**: short imperative subject (`fix:`, `feat:`, `docs:`, `chore:`); body explains the *why*

## Reporting security issues

Please do **not** file public issues for security vulnerabilities. See [`SECURITY.md`](./SECURITY.md) for the responsible-disclosure process.

## Upstream relationship

The core engine of `bulk` has been absorbed upstream into the [vagary-platform](https://github.com/Cramraika/vagary-platform) monorepo as `backend/framework/notifications/fanout/`, where it powers vertical applications (news, budget, salary, …). This standalone repo continues as the OSS distribution; non-trivial engine changes should ideally land in both places. Open an issue to coordinate if you're unsure where a change belongs.

## License

By contributing, you agree your contributions will be licensed under the [MIT License](./LICENSE).

## Sponsorship

If `bulk` is useful to your work, please consider [sponsoring on GitHub](https://github.com/sponsors/Cramraika) or via [Buy Me a Coffee](https://buymeacoffee.com/vagarylife). Your support funds maintenance and new adapter development.
