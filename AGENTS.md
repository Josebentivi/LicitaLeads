# AGENTS.md

LicitaLead Monitor: local FastAPI app that monitors Brazilian public procurement
(PNCP + Compras.gov.br), extracts official documents/events/deadlines, and
produces audit-ready **draft** legal leads. It also collects ARP details,
contracts and payment facts from official sources. Portuguese domain terms and
CLI/UI output. See `README.md`, `docs/plano_de_implementacao.md`,
`docs/matriz_de_fontes.md`.

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
- `PARTICIPATION_DETECTED` is evidence of documented participation in an active
  process and may create a lead. Never upgrade it to a winner/award conclusion
  without official award evidence. `Procurement.current_phase` is an inferred
  operator aid (status + dates + events), not a legal status; keep the source
  status visible.
- Payments are not proof of non-payment. A contract is not a payment, and
  `Payment.match_status="unlinked"` means only that no exact contract/process
  match was found. Never label a supplier as inadimplente without financial
  evidence and human review. Manual/municipal rows without CNPJ are stored
  unlinked for review; a row with neither CNPJ nor supplier name is rejected.
- Preferential dedup key is `numeroControlePNCP`; the composite
  (agency CNPJ + unit/UASG + number + year + modality) only when complete
  (`app/repositories/procurements.py`). `app/services/identifiers.py` only
  normalizes CNPJ/names — text similarity never auto-merges companies or processes.
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
- Windows one-click launcher: `iniciar.bat` → `scripts/launcher.py` (stdlib
  only; re-executes under 3.12, may install it via winget, recreates a broken
  `.venv`, installs `.[dev,ocr]` by default (use `--no-dev`/`--no-ocr` for a
  minimal install), creates `.env`, migrates, starts API + scheduler and opens
  the browser). Keep it working when touching `run.py`, `scripts/setup.py`,
  `scripts/check_environment.py` or `.env.example`; `--check` must not change
  repo state and the scheduler must never outlive it.
  On synced drives it uses `%LOCALAPPDATA%\LicitaLeads\venv` (repo `.venv` may be
  absent/unusable); `--venv CAMINHO` overrides.
- **Effective DB gotcha:** on this Google Drive workspace the launcher redirects
  the default relative `DATABASE_URL` to `%LOCALAPPDATA%\LicitaLeads\data\`
  (migrating an existing repo DB once) and uses it for API + scheduler. Running
  `alembic`, `python run.py`, or `python -m app.cli` directly uses the `.env`
  value instead and may hit a different database. Check
  `python scripts\launcher.py --check` and reuse the effective `DATABASE_URL`
  before inspecting or migrating data. Absolute URLs and PostgreSQL are respected.
- Run commands from the repo root: `.env`, `alembic.ini`, and the `app/static`
  mount are CWD-relative. `.env` is untracked/per-machine; add new settings to
  `.env.example`, not just `.env`.
- Bootstrap: `pip install -e ".[dev,ocr]"`, then `python scripts/setup.py` (copies
  `.env` from `.env.example` and runs `alembic upgrade head`), then `python run.py`.
- `python run.py` refuses to start unless the DB revision equals the Alembic head;
  use `python run.py --migrate` to migrate first. Add `--reload` for dev.
- Makefile aliases: `make install|migrate|run|test|lint|format|crawl|pipeline`.
  `make lint` = `ruff check .` + `mypy app`; `ruff format .` is the formatter
  (line length 100). README also documents `ruff format --check .`.
- CLI also exposes `crawl-atas` (ARP headers/items/details), `crawl-contracts`
  (PNCP contracts, items/terms), `import-payments` (federal Portal da
  Transparencia), `import-payments-csv` (state/municipal portal exports),
  `crawl-payments-municipal` (Maranhao municipal portals) and `backfill-fields`;
  use `python -m app.cli --help`. `TRANSPARENCIA_API_KEY` is required for the
  federal source and must never reach logs or `public_view()`.
- OCR is optional at runtime but its Python dependencies are installed by
  default by the launcher (`.[dev,ocr]`); `--no-ocr` skips them. Tesseract is
  detected in the PATH and may be installed via winget when missing.
  `OCR_ENABLED=false` remains the default; without the binary, scanned documents
  stay `OCR_REQUIRED` and must not be treated as empty.

## Testing quirks

- `pytest` (bare) enforces `--cov-fail-under=80` (coverage of `app/`, branch
  mode, omits `app/jobs/scheduler.py`; `mypy` excludes `migrations/`). For a
  focused/quick run add `--no-cov`, e.g.
  `pytest tests/unit/test_cli.py --no-cov -q`.
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
  `services/llm/`. ARP headers/items/units/commitments/adhesions are collected
  separately (`crawl-atas`) and linked to procurements via
  `linked_pncp_control_number`. Contracts (PNCP/Compras.gov.br), contract
  items/terms, and payment documents (Portal da Transparencia, configured portal
  adapters or manual CSV/JSON exports) are separate ingestion paths; never merge
  contract value, additive value, commitment and payment into one number.
  `app/connectors/municipal_payments/` implements the municipal adapters and
  `app/services/ingestion/municipal_payments.py` persists `CrawlRun` +
  `SourceRecord` before each payment; one source failure must not stop the
  other municipal sources.
- `app/services/procurement_phase.py` derives `Procurement.current_phase` from
  status/dates/events and records transitions in `FieldObservation`. Keep
  `procurement_is_active` (proposal window) distinct from
  `procurement_is_in_session` (later dispute/judgment/appeal phases).
- Companies are consolidated by CNPJ from participants/contracts/awards, never
  by name similarity; `/empresas` and `/atas` are the operator views.
- CPU-bound document extraction (PyMuPDF/BS4/openpyxl) must run via
  `asyncio.to_thread` — the UI-triggered crawl shares the API event loop, and
  sync extraction freezes the whole site. Crawl progress (`cursor.progress`) is
  persisted per record/document so `/crawls` shows movement during long runs.
- Crawl cancellation is cooperative: `crawl_runs.cancel_requested` is persisted
  and checked before each record/document; tasks launched by the API are also
  cancelled directly. Never swallow `PipelineCancelled`/`CancelledError` inside
  the pipeline — the run must finish as `cancelled` and release its lease.
- The settings reset (`POST /api/maintenance/clear-data`, `/settings`, implemented
  in `app/services/maintenance.py`) is an explicit, destructive operator action:
  it wipes every domain table and stored file, purges leases, truncates
  `data/scheduler.log`, preserves `.env`/Alembic/holidays, and leaves the
  scheduler paused. It must refuse while a crawl or scheduler lease is active.
  Tests must monkeypatch the maintenance module's `get_settings` to temporary
  paths so real storage directories are never deleted.
- `app/models/` (SQLAlchemy, provenance entities), `app/repositories/` (queries),
  `app/api/routes/` (`/api`, Swagger at `/docs`), `app/cli/` (Typer),
  `app/jobs/scheduler.py`.
- `app/web.py` + `app/templates/` + `app/static/` render Jinja2/HTMX pages
- Navigation is grouped in `app/navigation.py`; each group has a real-data hub built by `app/services/group_hubs.py`. Keep sidebar labels, active state and hub metrics aligned when adding a page.
  (dashboard, procurements, leads, crawls, settings, companies, price registries,
  contracts, payments).
  Templates/static are shipped via `[tool.setuptools.package-data]` — keep that
  in sync when adding assets.
- Settings: pydantic-settings, `.env`, `get_settings()` is `lru_cache`d; sync DB
  URLs are auto-converted to async in `app/database.py`. A module-level async
  `engine` is created at import time, so `.env` changes require a fresh process;
  tests should build `Settings(...)` directly.
- Alembic: single async env (`migrations/env.py`) using `render_as_batch` on
  SQLite; use `alembic revision --autogenerate` and check generated batch ops.

## Scheduler

Deliberately a **separate process** (`python -m app.jobs.scheduler`); the API
never imports or starts it. Jobs are guarded by DB leases
(`JobLeaseRepository`) so concurrent instances skip rather than race.
`data/scheduler.paused` (pause/resume in `/settings`) is checked before every
job; a data reset sets it so jobs do not repopulate the empty DB. Stale crawls
are reconciled at API startup and by the scheduler every 15 minutes. When
`SESSION_POLL_ENABLED=true`, `recheck_session_documents` re-downloads session
documents of processes in an active phase so in-place Ata de Sess?o updates are
not missed; it is scoped by `SESSION_POLL_MINUTES`/`SESSION_POLL_DOCUMENT_LIMIT`.
`CONTRACT_AGENCY_CNPJS` enables the daily contract crawl and `PAYMENT_POLL_CNPJS`
enables the daily payment poll; both skip when their list is empty.
`PAYMENT_SOURCES` lists federal/manual/portal adapters; unknown adapters are
reported as not configured instead of silently pretending to collect.
`MUNICIPAL_PAYMENT_SOURCES` enables the municipal adapters
(`sao_luis`, `imperatriz`, `timon`, `caxias`, `bacabal`). Fiorilli/SCPI
portals return commitment-level aggregates; `MUNICIPAL_PAYMENT_INCLUDE_DETAILS`
(limited by `MUNICIPAL_PAYMENT_DETAIL_LIMIT`) fetches individual payment rows.
Aggregate rows keep `payment_date=None` and `status=agregado_por_empenho`:
never turn the commitment date into a payment date. São Luís is CSV/Cloudflare
and may return `access_restricted`; the manual import remains the fallback.
Timon's identified Fiorilli endpoint returned HTTP 500 when tested; a failed
municipal collection is recorded as a failed `CrawlRun`, not `EMPTY`.

## Integrity and diagnostics

- Ingestion field merges use the per-session listener in
  `services/ingestion/field_updates.py`, including autoflushes. Keep canonical
  changes, `FieldState` and append-only `FieldChange` in one transaction.
  Missing/null/blank is not removal; zero/false are values. Explicit clears
  need complete responses. `observed_at` orders source observations;
  `recorded_at` is processing time, never a source update timestamp.
- `CollectionBatch` freezes filters/dates; `CollectionWorkItem` retains fetched
  responses and pending rows. Confirm work in the domain transaction, under
  the existing fence. Resume only limited scheduled batches automatically;
  failed/cancelled batches require an explicit continuation. Replay saved
  response pages locally; do not spend the new-page budget on the saved prefix.
  A cut payment detail group stays inactive until complete reconciliation.
- `records_updated` still counts revisited records. New changed/unchanged/
  stale/conflict counters exclude lookup/collection metadata and are NULL on
  legacy runs. Do not infer legacy counters or history. CSV payment official
  IDs are portal-scoped; without them, retain content identity and review.

- The `/testes` full-suite child must run with a temporary database/storage
  tree, `RUN_LIVE_CONTRACT_TESTS=false`, LLM/contact/OCR disabled, and
  `-m "not live"`; it must apply the Alembic schema before pytest. Never weaken
  this to rely on the real `.env` or effective database. Subprocesses must be
  killed on timeout/cancellation and JUnit failure text must be redacted.
- `python -m app.cli reconcile-data [--apply]` is the only automatic legacy
  data repair path. It must be backup-gated and may only derive changes from
  stored raw responses; ambiguous UASG merges are reported, never split.
- Municipal sources with only HTTP require explicit
  `ALLOW_INSECURE_HTTP_SOURCES=true`; prefer verified HTTPS.
- Crawl and municipal leases use heartbeats/fencing; recovery must use recent
  progress, not only `started_at`. Do not let a lost worker overwrite terminal
  state.
- Strong canonical identity requires agency CNPJ + UASG + number + year +
  modality; cancelled results and inactive documents must retract canonical
  facts only after complete, untruncated responses.

## Repo location gotcha

The project lives under a Google Drive path. Never run two instances against the
same SQLite file; SQLite uses WAL + `busy_timeout` pragmas per connection. For
larger loads move the DB to local disk or set `DATABASE_URL` to PostgreSQL.
Runtime state (`data/scheduler.paused`, `.pid`, `.log`, `*.db*`) must stay out of
commits; `.gitignore` covers logs/DBs but not every scheduler file.
