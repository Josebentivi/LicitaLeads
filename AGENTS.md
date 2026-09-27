# AGENTS.md

LicitaLead Monitor: local FastAPI app that monitors Brazilian public procurement
(PNCP + Compras.gov.br), extracts official documents/events/deadlines, and
produces audit-ready **draft** legal leads. Portuguese domain terms and CLI/UI
output. See `README.md`, `docs/implementation_plan.md`,
`docs/data_source_capability_matrix.md`.

Workspace-level `/AGENTS.md` in the parent dir has generic conventions; this file
holds the repo-specific rules.

## Non-obvious constraints (do not break these)

- **Auditability is the product.** Every conclusion must trace to `SourceRecord`
  or `Evidence`. Never invent participants, events, deadlines, or values not
  present in an official source.
- `DataAvailability.EMPTY` means "supported source queried successfully, zero
  records". Never use it for a feature the source does not expose — use
  `NOT_SUPPORTED`. Semantics are distinct and tested.
- Homologated result = awarded winner, **not** the participant list. Losers are
  only derived from documents.
- Preferential dedup key is `numeroControlePNCP`; the composite
  (agency CNPJ + unit/UASG + number + year + modality) only when complete.
  Text similarity never auto-merges companies or processes.
- Outreach is `draft_only`; the system must not send messages. LLM and contact
  search are off by default. Secrets must never reach logs or `public_view()`.
- LLM detection is optional and evidence-bound: `LLM_ENABLED=true` +
  `LLM_PROVIDER=openai`/`LLM_MODEL`/`LLM_API_KEY` (see `services/llm/`). Output
  is rejected if any quote/CNPJ/name is absent from the document; LLM findings
  are stored with `source="llm"` and never bypass human review rules.
- Downloads accept public HTTP(S) only and validate DNS/IP, size, redirects,
  signatures; ZIP extraction guards zip-slip/zip-bomb. UI must not render raw HTML.
- No Docker / Node. `scripts/check_environment.py` errors if `Dockerfile` or
  `docker-compose.yml` exists. Keep it that way.

## Environment & commands

- **Python 3.12 only** is enforced by `scripts/setup.py` and
  `scripts/check_environment.py` (`run.py` only warns). Don't rely on 3.13+.
- Bootstrap: `pip install -e ".[dev]"`, then `python scripts/setup.py` (copies
  `.env` from `.env.example` and runs `alembic upgrade head`), then `python run.py`.
- `python run.py` refuses to start unless the DB revision equals the Alembic head;
  use `python run.py --migrate` to migrate first. Add `--reload` for dev.
- Makefile aliases: `make install|migrate|run|test|lint|format|crawl|pipeline`.
  `make lint` = `ruff check .` + `mypy app`. README also documents
  `ruff format --check .`. `ruff format .` is the formatter (line length 100).
- **`pytest` (bare) enforces `--cov-fail-under=80`** (coverage of `app/`,
  branch mode, omits `app/jobs/scheduler.py`). For a focused/quick run add
  `--no-cov`, e.g. `pytest tests/unit/test_cli.py --no-cov -q`.

## Testing quirks

- `asyncio_mode=auto`; tests are async. Everything is SQLAlchemy async
  (aiosqlite). Integration fixtures in `tests/integration/conftest.py` build an
  isolated tmp SQLite engine and override `get_db`.
- Default suite is offline. Live contract tests are **double-gated** (marker
  `live` AND env var): `RUN_LIVE_CONTRACT_TESTS=true pytest tests/contract -m live`.
- `tests/unit` needs no DB; `tests/integration` uses the fixtures above;
  `tests/fixtures/` holds OpenAPI snapshots + canned responses.

## Architecture map

- `app/connectors/` — `base.py` defines `ProcurementSourceConnector` and DTOs;
  `pncp.py`, `compras_gov.py` implement it; `capabilities.py` drives the
  capability matrix. Connectors return source-shaped DTOs + raw payloads, not ORM.
- Flow: connectors → `services/ingestion/` (persist raw, normalize, audit) →
  `services/documents/` (secure download/extract) → `event_detection/` →
  `deadlines/` → `contacts/` → `lead_scoring/` → `outreach/`. Optional
  `services/llm/`.
- `app/models/` (SQLAlchemy, provenance entities), `app/repositories/` (queries),
  `app/api/routes/` (`/api`, Swagger at `/docs`), `app/cli/` (Typer),
  `app/jobs/scheduler.py`.
- `app/web.py` + `app/templates/` + `app/static/` render Jinja2/HTMX pages
  (dashboard, procurements, leads, crawls, settings). Templates/static are shipped
  via `[tool.setuptools.package-data]` — keep that in sync when adding assets.
- Settings: pydantic-settings, `.env`, `get_settings()` is `lru_cache`d;
  sync DB URLs are auto-converted to async in `app/database.py`. A module-level
  async `engine` is created at import time.
- Alembic: single async env (`migrations/env.py`) using `render_as_batch` on
  SQLite; `mypy` and coverage ignore `migrations/`.

## Scheduler

Deliberately a **separate process** (`python -m app.jobs.scheduler`); the API
never imports or starts it. Jobs are guarded by DB leases
(`JobLeaseRepository`) so concurrent instances skip rather than race.

## Repo location gotcha

The project lives under a Google Drive path. Never run two instances against the
same SQLite file; SQLite uses WAL + `busy_timeout` pragmas per connection. For
larger loads move the DB to local disk or set `DATABASE_URL` to PostgreSQL.
