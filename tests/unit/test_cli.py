"""CLI command dispatch without network or the production database."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from typer.testing import CliRunner

from app.cli.main import app
from app.models import CrawlRunStatus
from app.services.ingestion.audit import SourceAuditResult
from app.services.ingestion.pipeline import PipelineSummary


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
