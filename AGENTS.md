# AGENTS.md

LicitaLead Monitor: local FastAPI app that monitors Brazilian public procurement
(PNCP + Compras.gov.br), extracts official documents/events/deadlines and
produces audit-ready **draft** legal leads. It also ingests ARPs, contracts and
payment facts. Portuguese domain terms and CLI/UI output. Start with `README.md`;
`docs/README.md` indexes architecture, data model, API/CLI, security and operation
guides. The parent `../AGENTS.md` has generic workspace conventions; this file
holds the repo-specific rules.

## Non-obvious constraints (do not break these)

- **Auditability is the product.** Every conclusion must trace to `SourceRecord`
  or `Evidence`. Never invent participants, events, deadlines or values absent
  from an official source.
- `DataAvailability.EMPTY` means "supported source queried successfully, zero
  records". Never use it for a feature the source does not expose — use
  `NOT_SUPPORTED`.
- Homologated result = awarded winner, **not** the participant list. Losers are
  derived only from documents. All award assertions (rules, LLM, stored-data
  repair) go through `services/event_detection/authority.py`; it rejects template
  documents, status legends, future/conditional rules, unconfirmed allegations
  and nonaward labels such as "não homologado".
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
- Preferential dedup key is `numeroControlePNCP`; the composite (agency CNPJ +
  unit/UASG + number + year + modality) is used only when complete
  (`app/repositories/procurements.py`). `app/services/identifiers.py` only
  normalizes CNPJ/names — text similarity never auto-merges companies or processes.
- Outreach is `draft_only`; the system must not send messages. LLM and contact
  search are off by default. Secrets must never reach logs or `public_view()`.
- LLM is optional and evidence-bound: enable with `LLM_ENABLED=true`,
  `LLM_PROVIDER=openai`, `LLM_MODEL` and `LLM_API_KEY`. Ingestion writes a
  persistent outbox in the same transaction (no HTTP there); only the separate
  scheduler drains it. API/web handlers must never call the provider
  synchronously. Output is rejected if any quote/CNPJ/name is absent from the
  document. `LLMSuggestion` rows are review candidates, not facts/leads/deadlines
  until explicitly confirmed; LLM findings still obey human-review rules. See
  `docs/fila_ia.md`.
- Downloads accept public HTTP(S) only and validate DNS/IP, size, redirects and
  signatures; ZIP extraction guards zip-slip/zip-bomb. UI must not render raw HTML.
- No Docker / Node. `scripts/check_environment.py` errors if `Dockerfile` or
  `docker-compose.yml` exists. Keep it that way.

## Environment & commands

- **Python 3.12 only**: enforced by `scripts/setup.py` and
  `scripts/check_environment.py`; `run.py` only warns.
- Windows one-click launcher: `iniciar.bat` → `scripts/launcher.py` (stdlib only;
  re-executes under 3.12, may install it via winget, recreates a broken `.venv`,
  installs `.[dev,ocr]` by default — `--no-dev`/`--no-ocr` for minimal — creates
  `.env`, migrates, starts API + scheduler and opens the browser). Keep it working
  when touching `run.py`, `scripts/setup.py`, `scripts/check_environment.py` or
  `.env.example`; `--check` must not change repo state and the scheduler must
  never outlive it. On synced drives it uses `%LOCALAPPDATA%\LicitaLeads\venv`;
  `--venv CAMINHO` overrides.
- **Effective DB gotcha:** on this Google Drive workspace the launcher redirects
  the default relative `DATABASE_URL` to `%LOCALAPPDATA%\LicitaLeads\data\`
  (migrating an existing repo DB once) and uses it for API + scheduler. Running
  `alembic`, `python run.py` or `python -m app.cli` directly uses the `.env` value
  instead and may hit a different database. Run
  `python scripts\launcher.py --check` and reuse the effective `DATABASE_URL`
  before inspecting or migrating data. Absolute URLs and PostgreSQL are respected.
- Run commands from the repo root: `.env`, `alembic.ini` and the `app/static`
  mount are CWD-relative. `.env` is untracked/per-machine; add new settings to
  `.env.example`, not just `.env`.
- Bootstrap: `pip install -e ".[dev,ocr]"`, then `python scripts/setup.py` (copies
  `.env` and runs `alembic upgrade head`), then `python run.py`. `python run.py`
  refuses to start unless the DB revision equals the Alembic head; use
  `python run.py --migrate` to migrate first, `--reload` for dev.
- `make install` installs only `.[dev]` (no OCR); `make
  migrate|run|test|lint|format`. `make lint` = `ruff check .` + `mypy app` (mypy
  excludes `migrations/`); `ruff format .` formats (line length 100); README also
  documents `ruff format --check .`.
- CLI is `python -m app.cli` (also `licita-lead`). Beyond crawl/pipeline, it
  exposes `crawl-atas`, `crawl-contracts`, `import-payments[-csv]`,
  `crawl-payments-municipal`, `continue-crawl`, `enqueue-llm`,
  `prepare-test-browser`, `backfill-fields` and `reconcile-data`.
  `TRANSPARENCIA_API_KEY` is required for federal payments and must never reach
  logs or `public_view()`.
- OCR is optional at runtime but its Python deps are installed by the launcher.
  Tesseract is detected in PATH and may be installed via winget.
  `OCR_ENABLED=false` remains the default; without the binary, scanned documents
  stay `OCR_REQUIRED` and must not be treated as empty.

## Testing quirks

- Bare `pytest` enforces `--cov-fail-under=80` over `app/` with branch coverage
  and runs every test directory. For focused runs add `--no-cov`, e.g.
  `pytest tests/unit/test_cli.py --no-cov -q`.
- `tests/conftest.py` rewrites `DATABASE_URL`, storage paths and `APP_ENV=test`
  before application imports and runs `alembic upgrade head` on a temporary
  database. Never assume the real `.env` or launcher database in tests.
  Integration fixtures still create a per-test SQLite schema and override
  `get_db`.
- `asyncio_mode=auto`; everything is SQLAlchemy async (aiosqlite).
  `tests/fixtures/` holds OpenAPI snapshots and canned responses.
- `tests/browser` needs Edge/Chrome plus a prepared driver
  (`python -m app.cli prepare-test-browser`); `tests/system` starts real API and
  scheduler subprocesses; `tests/ocr` needs local Tesseract `por`. Browser and
  OCR tests self-skip when the prerequisite is missing, but bare pytest still
  attempts them. See `docs/testes_locais.md`.
- Default suite is offline. Live contract tests are **double-gated** (marker
  `live` AND env var): `RUN_LIVE_CONTRACT_TESTS=true pytest tests/contract -m live`.
- The `/testes` full-suite child must run with temporary database/storage,
  `RUN_LIVE_CONTRACT_TESTS=false`, LLM/contact/OCR disabled and `-m "not live"`;
  it must apply the Alembic schema before pytest. Never weaken this to rely on the
  real `.env` or effective database. Kill subprocesses on timeout/cancellation and
  redact JUnit failure text.

## Architecture map

- `app/connectors/` — `base.py` defines `ProcurementSourceConnector` and DTOs;
  `pncp.py`/`compras_gov.py` implement it; `capabilities.py` drives the capability
  matrix. Connectors return source-shaped DTOs + raw payloads, not ORM.
- Flow: connectors → `services/ingestion/` (persist raw, normalize, audit) →
  `services/documents/` (secure download/extract) → `event_detection/` →
  `deadlines/` → `contacts/` → `lead_scoring/` → `outreach/`. ARP headers/items/
  units/commitments/adhesions are collected separately (`crawl-atas`) and linked
  to procurements via `linked_pncp_control_number`. Contracts, contract
  items/terms and payments are separate ingestion paths; never merge contract
  value, additive value, commitment and payment into one number.
  `app/connectors/municipal_payments/` implements municipal adapters and
  `app/services/ingestion/municipal_payments.py` persists `CrawlRun` +
  `SourceRecord` before each payment; one source failure must not stop the others.
- Optional AI: `app/services/llm/` (`queue.py` transactional outbox + lease,
  `central.py` shared read/action service), `app/api/routes/llm.py` and
  `app/ai_web.py` for `/ia`. Enqueue from requests; only the scheduler drains.
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
  in `app/services/maintenance.py`) is destructive: it wipes every domain table
  and stored file, purges leases, truncates `data/scheduler.log`, preserves
  `.env`/Alembic/holidays and leaves the scheduler paused. It must refuse while a
  crawl or scheduler lease is active. Tests must monkeypatch the maintenance
  module's `get_settings` to temporary paths so real storage is never deleted.
- Launcher-managed lifecycle: `app/runtime_control.py` is a stdlib JSON channel
  used by `scripts/launcher.py`, the scheduler and
  `app/services/application_shutdown.py`. `/application/shutdown` works only for
  a launcher-started instance via loopback + token; it pauses both components,
  cancels pending crawls/LLM, releases only this instance's recorded leases and
  preserves completed data. Never kill child processes out of band.
- `app/models/` (SQLAlchemy provenance entities), `app/repositories/` (queries),
  `app/api/routes/` (`/api`, Swagger at `/docs`), `app/cli/` (Typer),
  `app/jobs/scheduler.py`.
- `app/web.py` + `app/templates/` + `app/static/` render Jinja2/HTMX pages.
  Navigation is grouped in `app/navigation.py` (Visão geral, Compras públicas,
  Execução financeira, Empresas e oportunidades, Operação de dados, Sistema);
  each group hub is built by `app/services/group_hubs.py`. Keep sidebar labels,
  active state and hub metrics aligned when adding a page. Templates/static ship
  via `[tool.setuptools.package-data]` — keep that in sync when adding assets.
- Settings: pydantic-settings, `.env` (ignored when `APP_ENV=test`);
  `get_settings()` is `lru_cache`d; sync DB URLs are auto-converted to async in
  `app/database.py`. A module-level async `engine` is created at import time, so
  `.env` changes require a fresh process; tests should build `Settings(...)`
  directly.
- Alembic: single async env (`migrations/env.py`) using `render_as_batch` on
  SQLite; use `alembic revision --autogenerate` and check generated batch ops.

## Scheduler

Deliberately a **separate process** (`python -m app.jobs.scheduler`); the API
never imports or starts it. Jobs are guarded by DB leases
(`JobLeaseRepository`) so concurrent instances skip rather than race. Pause state
lives in `data/scheduler.controls.json`: `collections` and `llm` are independent;
legacy `data/scheduler.paused` means all paused. A data reset pauses both. Stale
crawls are reconciled at API startup and by the scheduler every 15 minutes.

`SESSION_POLL_ENABLED=true` makes `recheck_session_documents` re-download session
documents of active processes so in-place Ata de Sessão updates are not missed;
it is scoped by `SESSION_POLL_MINUTES`/`SESSION_POLL_DOCUMENT_LIMIT`.
`CONTRACT_AGENCY_CNPJS` enables the daily contract crawl and `PAYMENT_POLL_CNPJS`
the daily payment poll; both skip when empty. `PAYMENT_SOURCES` lists
federal/manual/portal adapters; unknown adapters are reported as not configured.
`MUNICIPAL_PAYMENT_SOURCES` enables `sao_luis`, `imperatriz`, `timon`, `caxias`,
`bacabal`. Fiorilli/SCPI portals return commitment-level aggregates;
`MUNICIPAL_PAYMENT_INCLUDE_DETAILS` (limited by `MUNICIPAL_PAYMENT_DETAIL_LIMIT`)
fetches individual rows. Aggregate rows keep `payment_date=None` and
`status=agregado_por_empenho`: never turn the commitment date into a payment
date. São Luís is CSV/Cloudflare and may return `access_restricted`; manual
import is the fallback. Timon's identified endpoint returned HTTP 500 when
tested; a failed municipal collection is a failed `CrawlRun`, not `EMPTY`.

## Integrity and diagnostics

- Ingestion field merges use the per-session listener in
  `services/ingestion/field_updates.py`, including autoflushes. Keep canonical
  changes, `FieldState` and append-only `FieldChange` in one transaction.
  Missing/null/blank is not removal; zero/false are values. Explicit clears need
  complete responses. `observed_at` orders source observations; `recorded_at` is
  processing time, never a source update timestamp.
- `CollectionBatch` freezes filters/dates; `CollectionWorkItem` retains fetched
  responses and pending rows. Confirm work in the domain transaction under the
  existing fence. Resume only limited scheduled batches automatically;
  failed/cancelled batches require explicit continuation. Replay saved response
  pages locally; do not spend the new-page budget on the saved prefix. A cut
  payment detail group stays inactive until complete reconciliation.
- `records_updated` still counts revisited records. Changed/unchanged/stale/
  conflict counters exclude lookup/collection metadata and are NULL on legacy
  runs; do not infer legacy counters or history. CSV payment official IDs are
  portal-scoped; without them, retain content identity and review.
- `python -m app.cli reconcile-data` is the only automatic legacy repair path.
  It is report-first and `--apply` requires a verified backup.
  `--participation-integrity` and `--flow-integrity` audit stored data only;
  ambiguous UASG merges are reported, never split.
- Municipal sources with only HTTP require explicit
  `ALLOW_INSECURE_HTTP_SOURCES=true`; prefer verified HTTPS.
- Crawl and municipal leases use heartbeats/fencing; recovery must use recent
  progress, not only `started_at`. Never let a lost worker overwrite terminal state.
- Strong canonical identity requires agency CNPJ + UASG + number + year +
  modality; cancelled results and inactive documents retract canonical facts only
  after complete, untruncated responses.

## Repo location gotcha

The project lives under a Google Drive path. Never run two instances against the
same SQLite file; SQLite uses WAL + `busy_timeout` per connection. For larger
loads move the DB to local disk or set `DATABASE_URL` to PostgreSQL. Runtime
state (`data/*.db*`, `scheduler.paused`, `scheduler.heartbeat`,
`scheduler.controls*`, logs) must stay out of commits; `.gitignore` covers it.
