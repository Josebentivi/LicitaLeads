"""Crawl recovery, honest status, and auditable event-to-company linking."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import func, select

from app import web as web_routes
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    Document,
    ExtractionStatus,
    JobLease,
    Lead,
    Participant,
    ParticipantRole,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
)
from app.services.ingestion.processor import DocumentProcessingService
from app.services.ingestion.recovery import reconcile_stale_crawls

from .conftest import DatabaseContext
from .test_document_processing import FakeDownloader, _settings


async def _procurement_with_awarded_company(
    database: DatabaseContext,
    *,
    suffix: str = "1",
    with_document: bool = True,
) -> tuple[object, object, object | None]:
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id=f"link-{suffix}",
            title="Pregão Eletrônico 14/2026",
            agency_name="Prefeitura Municipal de Exemplo",
            agency_cnpj="00000000000191",
            uf="MA",
            municipality="São Luís",
            estimated_value=180000,
            publication_at=datetime(2026, 9, 16, 14, 30, tzinfo=UTC),
            status="em julgamento",
            fingerprint=suffix * 64,
        )
        company = Company(
            cnpj="00000000000191",
            legal_name="Empresa Vencedora Ltda.",
            normalized_name="EMPRESA VENCEDORA",
            fingerprint=(suffix + "a") * 32,
        )
        session.add_all([procurement, company])
        await session.flush()
        participant = Participant(
            procurement_id=procurement.id,
            company_id=company.id,
            participation_role=ParticipantRole.AWARDED,
            source="pncp",
            confidence=Decimal("1"),
            fingerprint=(suffix + "b") * 32,
        )
        document = None
        if with_document:
            document = Document(
                procurement_id=procurement.id,
                document_type="resultado",
                title="Termo de homologação",
                original_url="https://pncp.gov.br/resultado.txt",
                extraction_status=ExtractionStatus.PENDING,
                fingerprint=(suffix + "c") * 32,
            )
            session.add(document)
        session.add(participant)
        await session.flush()
        return procurement.id, company.id, document.id if document else None


@pytest.mark.asyncio
async def test_reconcile_stale_crawls_fails_only_interrupted_runs(
    database: DatabaseContext,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        stale = CrawlRun(
            connector="all",
            status=CrawlRunStatus.RUNNING,
            started_at=now - timedelta(hours=5),
            created_at=now - timedelta(hours=5),
            filters={},
        )
        fresh = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.RUNNING,
            started_at=now,
            created_at=now,
            filters={},
        )
        session.add_all([stale, fresh])
        await session.flush()
        stale_id, fresh_id = stale.id, fresh.id

    reconciled = await reconcile_stale_crawls(
        session_factory=database.sessions, now=now, stale_after_minutes=180
    )

    assert reconciled == 1
    async with database.sessions() as session:
        stale_run = await session.get(CrawlRun, stale_id)
        fresh_run = await session.get(CrawlRun, fresh_id)
    assert stale_run is not None and stale_run.status is CrawlRunStatus.FAILED
    assert stale_run.finished_at is not None
    assert fresh_run is not None and fresh_run.status is CrawlRunStatus.RUNNING


@pytest.mark.asyncio
async def test_reconcile_marks_stale_cancelled_runs_as_cancelled(
    database: DatabaseContext,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        cancelled = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.RUNNING,
            started_at=now - timedelta(hours=5),
            created_at=now - timedelta(hours=5),
            cancel_requested=True,
            filters={},
        )
        session.add(cancelled)
        await session.flush()
        cancelled_id = cancelled.id

    reconciled = await reconcile_stale_crawls(
        session_factory=database.sessions, now=now, stale_after_minutes=180
    )

    assert reconciled == 1
    async with database.sessions() as session:
        stored = await session.get(CrawlRun, cancelled_id)
    assert stored is not None and stored.status is CrawlRunStatus.CANCELLED
    assert "cancelada" in (stored.diagnostic or "")


@pytest.mark.asyncio
async def test_reconcile_normalizes_cursor_and_releases_lease(
    database: DatabaseContext,
) -> None:
    """An interrupted run must not keep a source "running" nor hold its lease."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        stale = CrawlRun(
            connector="all",
            status=CrawlRunStatus.RUNNING,
            started_at=now - timedelta(hours=5),
            created_at=now - timedelta(hours=5),
            filters={"connector": "all", "uf": "MA"},
            cursor={
                "sources": {
                    "pncp": {
                        "status": "running",
                        "records_found": 10,
                        "progress": {"processed": 3, "total": 10},
                    }
                }
            },
        )
        session.add(stale)
        await session.flush()
        stale_id = stale.id
        session.add(
            JobLease(
                name="crawl:all:MA",
                owner_id=f"pipeline:{stale_id}",
                acquired_at=now - timedelta(hours=5),
                heartbeat_at=now - timedelta(hours=5),
                expires_at=now + timedelta(hours=1),
            )
        )

    reconciled = await reconcile_stale_crawls(
        session_factory=database.sessions, now=now, stale_after_minutes=180
    )

    assert reconciled == 1
    async with database.sessions() as session:
        stored = await session.get(CrawlRun, stale_id)
        lease = await session.get(JobLease, "crawl:all:MA")
    assert stored is not None
    source = stored.cursor["sources"]["pncp"]
    assert source["status"] == "failed"
    assert source["progress"] == {"processed": 3, "total": 10}
    assert any("interrompida" in item for item in source["diagnostics"])
    assert lease is None


@pytest.mark.asyncio
async def test_reconcile_does_not_release_a_newer_lease(
    database: DatabaseContext,
) -> None:
    """A lease held by another (newer) run is preserved."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        stale = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.RUNNING,
            started_at=now - timedelta(hours=5),
            created_at=now - timedelta(hours=5),
            filters={"connector": "pncp", "uf": "MA"},
        )
        session.add(stale)
        await session.flush()
        session.add(
            JobLease(
                name="crawl:pncp:MA",
                owner_id="pipeline:00000000-0000-0000-0000-000000000999",
                acquired_at=now,
                heartbeat_at=now,
                expires_at=now + timedelta(hours=1),
            )
        )

    await reconcile_stale_crawls(
        session_factory=database.sessions, now=now, stale_after_minutes=180
    )

    async with database.sessions() as session:
        lease = await session.get(JobLease, "crawl:pncp:MA")
    assert lease is not None


@pytest.mark.asyncio
async def test_terminal_run_shows_failed_source_and_can_be_repeated(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal run never shows a source "Em andamento" and offers full retry."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="all",
            status=CrawlRunStatus.FAILED,
            started_at=now - timedelta(hours=8),
            finished_at=now - timedelta(hours=1),
            filters={
                "connector": "all",
                "uf": "MA",
                "days": 7,
                "modalities": ["pregao_eletronico"],
                "process_documents": False,
            },
            cursor={
                "sources": {
                    "pncp": {
                        "status": "running",
                        "records_found": 479,
                        "progress": {"processed": 119, "total": 479},
                    }
                }
            },
        )
        session.add(run)
        await session.flush()
        run_id = run.id

    page = await api_client.get("/crawls")

    assert page.status_code == 200
    assert "Em andamento" not in page.text
    assert "Repetir coleta" in page.text

    captured: list[object] = []
    fake_retry = SimpleNamespace(id="00000000-0000-0000-0000-000000000324")

    async def create_retry(_self, request):
        captured.append(request)
        return fake_retry

    monkeypatch.setattr(web_routes.IngestionPipeline, "create_run", create_retry)
    monkeypatch.setattr(web_routes, "launch_crawl", lambda *args: captured.append(args))

    retry = await api_client.post(f"/crawls/{run_id}/retry-all", follow_redirects=False)

    assert retry.status_code == 303
    assert captured[0].connector == "all"
    assert captured[0].uf == "MA"
    assert captured[0].days == 7
    assert len(captured) == 2

    async with database.sessions() as session, session.begin():
        session.add(
            CrawlRun(
                connector="all",
                status=CrawlRunStatus.RUNNING,
                started_at=now,
                filters={},
            )
        )
    blocked = await api_client.post(f"/crawls/{run_id}/retry-all", follow_redirects=False)
    assert blocked.status_code == 409


@pytest.mark.asyncio
async def test_crawl_page_reports_running_sources_honestly(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="all",
            status=CrawlRunStatus.RUNNING,
            started_at=datetime.now(UTC),
            filters={},
        )
        session.add(run)
        await session.flush()

    page = await api_client.get("/crawls")
    fragment = await api_client.get("/crawls/table")

    assert page.status_code == 200
    assert "Em andamento" in page.text
    assert "Concluída" not in page.text
    assert "Encerrar coleta" in page.text
    assert 'hx-get="/crawls/table"' in page.text
    assert fragment.status_code == 200
    assert 'id="crawl-table"' in fragment.text


@pytest.mark.asyncio
async def test_single_winner_event_is_linked_and_deadline_is_idempotent(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    procurement_id, company_id, _ = await _procurement_with_awarded_company(database)
    text = "O objeto da contratação foi homologado e o resultado foi publicado.".encode()
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(text, filename="resultado.txt", mime="text/plain"),
    )

    summary = await service.process_pending()

    assert summary.events_created == 1
    assert summary.leads_created == 1
    async with database.sessions() as session:
        event = await session.scalar(select(ProcurementEvent))
        lead = await session.scalar(select(Lead))
        deadline_count = await session.scalar(select(func.count()).select_from(Deadline))

    assert event is not None and event.company_id == company_id
    assert event.requires_manual_review is True
    assert lead is not None and lead.company_id == company_id
    assert deadline_count == 1

    await service.detect_existing()
    await service.recalculate_deadlines_and_leads()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Deadline)) == 1
        assert await session.scalar(select(func.count()).select_from(Lead)) == 1


@pytest.mark.asyncio
async def test_manual_link_endpoint_links_participant_and_rejects_stranger(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    procurement_id, company_id, _ = await _procurement_with_awarded_company(
        database, suffix="2", with_document=False
    )
    async with database.sessions() as session, session.begin():
        event = ProcurementEvent(
            procurement_id=procurement_id,
            company_id=None,
            event_type=ProcurementEventType.INELIGIBLE,
            raw_description="Licitante inabilitada por ausência de certidão.",
            source="document",
            confidence=Decimal("0.9"),
            requires_manual_review=True,
            fingerprint="e" * 64,
        )
        stranger = Company(
            cnpj="11444777000161",
            legal_name="Empresa Estranha Ltda.",
            normalized_name="EMPRESA ESTRANHA",
            fingerprint="f" * 64,
        )
        session.add_all([event, stranger])
        await session.flush()
        event_id, stranger_id = event.id, stranger.id

    linked = await api_client.post(
        f"/api/events/{event_id}/link-company",
        json={"company_id": str(company_id), "reviewer": "Analista", "note": "conferido"},
    )
    rejected = await api_client.post(
        f"/api/events/{event_id}/link-company",
        json={"company_id": str(stranger_id)},
    )

    assert linked.status_code == 200 and linked.json()["linked"] is True
    assert rejected.status_code == 409
    async with database.sessions() as session:
        stored = await session.get(ProcurementEvent, event_id)
        lead = await session.scalar(select(Lead))
    assert stored is not None and stored.company_id == company_id
    assert lead is not None and lead.company_id == company_id
