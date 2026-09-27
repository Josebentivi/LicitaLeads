"""Crawl recovery, honest status, and auditable event-to-company linking."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select

from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    Document,
    ExtractionStatus,
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
