"""CLI command dispatch without network or the production database."""

from __future__ import annotations

import csv
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from typer.testing import CliRunner

from app.cli.main import app
from app.models import CrawlRunStatus
from app.services.ingestion.audit import SourceAuditResult
from app.services.ingestion.pipeline import PipelineSummary


@pytest.mark.parametrize("available", [True, False])
def test_prepare_browser_cli_is_explicit_and_reports_unavailability(monkeypatch, available):
    def prepare():
        if not available:
            raise RuntimeError("driver ausente")
        return {"browser": "edge", "version": "1.2.3", "detail": "Preparado"}

    monkeypatch.setattr("app.services.test_browser.prepare_test_browser", prepare)
    response = CliRunner().invoke(app, ["prepare-test-browser"])
    assert response.exit_code == (0 if available else 1)
    assert ("edge" if available else "driver ausente") in response.stdout + response.stderr


class FakePipeline:
    requests = []

    async def run(self, request):
        self.requests.append(request)
        return PipelineSummary(
            run_id=UUID("00000000-0000-0000-0000-000000000001"),
            status=CrawlRunStatus.COMPLETED,
            records_found=2,
            records_created=1,
            records_updated=1,
        )


class FakeDocumentService:
    async def process_pending(self, *, limit=None):
        return SimpleNamespace(
            documents_processed=1,
            documents_failed=0,
            events_created=1,
            participants_created=1,
            leads_created=1,
        )

    async def detect_existing(self, *, limit=None):
        return await self.process_pending(limit=limit)

    async def recalculate_deadlines_and_leads(self):
        return SimpleNamespace(leads_created=1)

    async def aclose(self):
        return None


class FakeContactService:
    async def run(self, *, force=False):
        return SimpleNamespace(
            companies_checked=1 if force else 0,
            contacts_created=1 if force else 0,
            contacts_updated=0,
            errors=0,
        )


def test_all_operational_cli_commands_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from app.cli import main as cli

    async def fake_audit():
        return SourceAuditResult(tmp_path / "audit.md", 2, 0, ())

    FakePipeline.requests.clear()
    monkeypatch.setattr(cli, "run_source_audit", fake_audit)
    monkeypatch.setattr(cli, "IngestionPipeline", FakePipeline)
    monkeypatch.setattr(cli, "DocumentProcessingService", FakeDocumentService)
    monkeypatch.setattr(cli, "ContactEnrichmentService", FakeContactService)
    runner = CliRunner()

    commands = [
        ["audit-sources"],
        ["crawl", "pncp", "--uf", "MA", "--days", "7", "--max-pages", "1"],
        ["crawl", "compras-gov", "--uf", "MA", "--days", "7"],
        ["process-documents", "--limit", "2"],
        ["detect-events", "--limit", "2"],
        ["calculate-deadlines"],
        ["enrich-contacts", "--force"],
        ["create-leads"],
        [
            "run-pipeline",
            "--uf",
            "MA",
            "--days",
            "7",
            "--connector",
            "all",
            "--max-pages",
            "1",
            "--skip-documents",
        ],
    ]
    results = [runner.invoke(app, command) for command in commands]

    assert all(result.exit_code == 0 for result in results), [result.output for result in results]
    assert "Relatório" in results[0].output
    assert "status=completed" in results[1].output
    assert "processados=1" in results[3].output
    assert "eventos=1" in results[4].output
    assert "empresas=1" in results[6].output
    assert len(FakePipeline.requests) == 3
    assert FakePipeline.requests[-1].process_documents is False


def test_cli_rejects_unknown_connector() -> None:
    result = CliRunner().invoke(app, ["crawl", "unknown", "--days", "1"])
    assert result.exit_code != 0
    assert "pncp" in result.output


@pytest.mark.parametrize("command", ["crawl", "crawl-atas", "crawl-contracts", "run-pipeline"])
def test_force_refresh_reaches_every_collection_command(monkeypatch, command):
    from app.cli import main as cli

    FakePipeline.requests.clear()
    monkeypatch.setattr(cli, "IngestionPipeline", FakePipeline)
    result = CliRunner().invoke(
        app, [command, *(["pncp"] if command == "crawl" else []), "--force-refresh"]
    )
    assert result.exit_code == 0, result.output
    assert FakePipeline.requests[-1].force_refresh is True


def test_cli_runs_municipal_payment_source(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.cli import main as cli
    from app.services.ingestion.municipal_payments import MunicipalPaymentSummary

    calls: list[dict[str, object]] = []

    async def fake_run(**kwargs):
        calls.append(kwargs)
        return [
            MunicipalPaymentSummary(
                source_id="imperatriz",
                source_name="Imperatriz",
                payments_created=2,
                payments_linked=1,
                payments_unlinked=1,
                diagnostic="teste",
            )
        ]

    monkeypatch.setattr(cli, "run_selected_municipal_sources", fake_run)
    result = CliRunner().invoke(
        app,
        ["crawl-payments-municipal", "--source", "imperatriz", "--year", "2026", "--details"],
    )

    assert result.exit_code == 0, result.output
    assert "imperatriz" in result.output
    assert "criados=2" in result.output
    assert calls[0]["source_ids"] == ["imperatriz"]
    assert calls[0]["include_details"] is True


class _CLISession:
    def __init__(self, events, rows=()):
        self.events, self.rows = events, rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def rollback(self):
        self.events.append("rollback")

    async def commit(self):
        self.events.append("commit")

    async def scalars(self, statement):
        return SimpleNamespace(all=lambda: self.rows)


@pytest.mark.parametrize("apply", [False, True])
def test_reconcile_cli_is_dry_by_default_and_locks_before_backup(tmp_path, monkeypatch, apply):
    import app.database as database
    from app.services import backups, data_reconciliation, maintenance

    events = []
    session = _CLISession(events)
    backup = SimpleNamespace(path=tmp_path / "verified.sqlite3", as_dict=lambda: {"verified": True})

    @asynccontextmanager
    async def exclusive(factory):
        events.append("lock")
        try:
            yield "test-owner"
        finally:
            events.append("unlock")

    async def reconcile(supplied, **options):
        assert supplied is session
        if options.get("apply"):
            assert options["backup"] is backup
            assert options["maintenance_owner"] == "test-owner"
            assert "backup" in events
            events.append("apply")
        else:
            events.append("impact")
        return SimpleNamespace(as_dict=lambda: {"ambiguous": ["preserve"], "repairs": 1})

    def create(settings, destination, *, maintenance_owner):
        assert maintenance_owner == "test-owner"
        assert destination == tmp_path
        events.append("backup")
        return backup

    monkeypatch.setattr(database, "async_session_factory", lambda: session)
    monkeypatch.setattr(maintenance, "exclusive_maintenance", exclusive)
    monkeypatch.setattr(data_reconciliation, "reconcile_data", reconcile)
    monkeypatch.setattr(backups, "create_backup", create)
    result = CliRunner().invoke(
        app, ["reconcile-data", "--backup-dir", str(tmp_path)] + (["--apply"] if apply else [])
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["applied"] is apply
    assert payload["impact"]["ambiguous"] == ["preserve"]
    if apply:
        assert events == ["lock", "impact", "rollback", "backup", "apply", "commit", "unlock"]
        assert json.loads(backup.path.with_suffix(".impact.json").read_text())["repairs"] == 1
    else:
        assert events == ["impact"]
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("blocked", [False, True])
def test_reconcile_cli_never_applies_when_lock_or_backup_fails(tmp_path, monkeypatch, blocked):
    import app.database as database
    from app.services import backups, data_reconciliation, maintenance

    events = []
    session = _CLISession(events)

    @asynccontextmanager
    async def exclusive(factory):
        if blocked:
            raise maintenance.MaintenanceBlocked("coleta ativa")
        try:
            yield "test-owner"
        finally:
            events.append("unlock")

    async def reconcile(supplied, **options):
        assert not options.get("apply")
        events.append("impact")
        return SimpleNamespace(as_dict=lambda: {})

    def fail_backup(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(database, "async_session_factory", lambda: session)
    monkeypatch.setattr(maintenance, "exclusive_maintenance", exclusive)
    monkeypatch.setattr(data_reconciliation, "reconcile_data", reconcile)
    monkeypatch.setattr(backups, "create_backup", fail_backup)
    result = CliRunner().invoke(app, ["reconcile-data", "--apply", "--backup-dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "commit" not in events
    assert events == ([] if blocked else ["impact", "rollback", "unlock"])
    if blocked:
        assert "bloqueada" in result.output


def test_contract_cli_dates_scope_and_invalid_date(monkeypatch):
    from app.cli import main as cli

    FakePipeline.requests.clear()
    monkeypatch.setattr(cli, "IngestionPipeline", FakePipeline)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "crawl-contracts",
            "pncp",
            "--agency-cnpj",
            "00000000000191",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-02-01",
            "--skip-details",
            "--max-pages",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    request = FakePipeline.requests[-1]
    assert request.mode == "contracts" and not request.contract_details
    assert request.start_date.isoformat() == "2026-01-01"
    assert request.agency_cnpj == "00000000000191" and request.max_pages == 1
    result = runner.invoke(app, ["crawl-contracts", "--start-date", "31/01/2026"])
    assert result.exit_code != 0 and "AAAA-MM-DD" in result.output
    result = runner.invoke(app, ["crawl-atas", "compras-gov", "--max-pages", "1"])
    assert result.exit_code == 0
    assert FakePipeline.requests[-1].mode == "price_registries"


@pytest.mark.parametrize("availability", ["available", "temporary_error"])
def test_payment_cli_surfaces_failure_in_exit_code(monkeypatch, availability):
    from app.cli import main as cli

    calls = []

    async def run(**options):
        calls.append(options)
        return SimpleNamespace(
            availability=SimpleNamespace(value=availability),
            payments_created=1,
            payments_updated=0,
            payments_linked=0,
            payments_unlinked=1,
            diagnostic="fonte em revisão",
        )

    monkeypatch.setattr(cli, "PaymentIngestionService", lambda: SimpleNamespace(run=run))
    result = CliRunner().invoke(
        app,
        ["import-payments", "--cnpj", "00000000000191", "--year", "2026", "--phase", "pagamento"],
    )
    assert result.exit_code == (0 if availability == "available" else 1)
    assert "fonte em revisão" in result.output
    assert calls == [{"company_cnpj": "00000000000191", "year": 2026, "phase": "pagamento"}]


@pytest.mark.parametrize("fails", [False, True])
def test_manual_payment_cli_closes_resources_on_success_or_error(tmp_path, monkeypatch, fails):
    from app.cli import main as cli

    source = tmp_path / "pagamentos.csv"
    source.write_bytes(b"source bytes")
    events = []

    async def import_bytes(content, **options):
        assert content == b"source bytes"
        assert options == {"filename": source.name, "source_name": "Portal municipal"}
        if fails:
            raise ValueError("invalid source")
        return SimpleNamespace(
            created=1,
            updated=0,
            linked=0,
            unlinked=1,
            errors=[SimpleNamespace(row=2, message="fornecedor ausente")],
        )

    async def close():
        events.append("closed")

    monkeypatch.setattr(
        cli,
        "PaymentImportService",
        lambda: SimpleNamespace(import_bytes=import_bytes, aclose=close),
    )
    result = CliRunner().invoke(
        app, ["import-payments-csv", str(source), "--fonte", "Portal municipal"]
    )
    assert result.exit_code == (1 if fails else 0)
    assert events == ["closed"]
    if not fails:
        assert "linha 2" in result.output and "fornecedor ausente" in result.output


def test_optional_municipal_sources_and_backfill_are_explicit(monkeypatch):
    from app.cli import main as cli

    async def none(**options):
        assert options["source_ids"] is None
        return []

    async def backfill(**options):
        assert options["limit"] == 2
        return SimpleNamespace(
            scanned_records=2, updated_procurements=1, skipped=1, diagnostics=["ambíguo preservado"]
        )

    monkeypatch.setattr(cli, "run_selected_municipal_sources", none)
    monkeypatch.setattr(cli, "backfill_procurement_fields", backfill)
    runner = CliRunner()
    result = runner.invoke(app, ["crawl-payments-municipal"])
    assert result.exit_code == 0 and "Nenhuma fonte municipal" in result.output
    result = runner.invoke(app, ["crawl-payments-municipal", "--source", "unknown"])
    assert result.exit_code != 0
    result = runner.invoke(app, ["backfill-fields", "--limit", "2"])
    assert result.exit_code == 0 and "ambíguo preservado" in result.output


@pytest.mark.parametrize("has_deadline", [False, True])
def test_export_cli_preserves_csv_quotes_and_optional_deadlines(
    tmp_path, monkeypatch, has_deadline
):
    from app.cli import main as cli

    lead = SimpleNamespace(
        id="lead-1",
        company=SimpleNamespace(
            legal_name=None, normalized_name="Empresa normalizada", cnpj="00000000000191"
        ),
        procurement=SimpleNamespace(
            agency_name="Órgão", pncp_control_number=None, purchase_number="1/2026"
        ),
        triggering_event=SimpleNamespace(
            event_type=SimpleNamespace(value="participation_detected"),
            source_url="https://official.test/document",
        ),
        deadline=SimpleNamespace(
            explicit_deadline_at=None,
            estimated_deadline_at="2026-01-10",
            status=SimpleNamespace(value="estimated"),
        )
        if has_deadline
        else None,
        score=70,
        lead_status=SimpleNamespace(value="draft"),
        reason_summary='Motivo, com "aspas"\ne quebra',
    )
    session = _CLISession([], [lead])
    monkeypatch.setattr(cli, "async_session_factory", lambda: session)
    destination = tmp_path / "exports" / "leads.csv"
    result = CliRunner().invoke(app, ["export-leads", str(destination)])
    assert result.exit_code == 0, result.output
    with destination.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1 and rows[0]["motivo"] == lead.reason_summary
    assert rows[0]["empresa"] == "Empresa normalizada"
    assert rows[0]["processo"] == "1/2026"
    assert rows[0]["status_prazo"] == ("estimated" if has_deadline else "UNKNOWN")


# CLI enqueue is intentionally separate from all document/network processing.
def test_enqueue_llm_requires_explicit_scope(monkeypatch):
    from typer.testing import CliRunner

    from app.cli.main import app

    result = CliRunner().invoke(app, ["enqueue-llm"])
    assert result.exit_code != 0 and "--all-extracted" in result.output


def test_enqueue_llm_calls_queue_only(monkeypatch):
    from typer.testing import CliRunner

    from app.cli.main import app

    class Queue:
        async def backfill(self):
            return {"batch_id": "test", "documents": 2, "analyses": 3}

    monkeypatch.setattr("app.services.llm.queue.LLMQueueService", Queue)
    result = CliRunner().invoke(app, ["enqueue-llm", "--all-extracted"])
    assert result.exit_code == 0 and '"analyses": 3' in result.output
