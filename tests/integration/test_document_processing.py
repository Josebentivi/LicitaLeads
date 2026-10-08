"""End-to-end document-to-lead processing scenarios."""

from __future__ import annotations

import asyncio
import io
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import (
    Company,
    Deadline,
    Document,
    DocumentChunk,
    Evidence,
    ExtractionStatus,
    Lead,
    Participant,
    Procurement,
    ProcurementEvent,
)
from app.services.ingestion.documents import DocumentDownloadError, DownloadedDocument
from app.services.ingestion.processor import DocumentProcessingService

from .conftest import DatabaseContext


@dataclass
class FakeDownloader:
    content: bytes
    filename: str = "ata.txt"
    mime: str = "text/plain"
    error: Exception | None = None
    calls: int = 0

    async def download(self, url: str) -> DownloadedDocument:
        self.calls += 1
        if self.error:
            raise self.error
        return DownloadedDocument(
            url=url,
            content=self.content,
            filename=self.filename,
            declared_mime=self.mime,
        )

    async def aclose(self) -> None:
        return None


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_env="test",
        document_storage_path=tmp_path / "documents",
        raw_data_storage_path=tmp_path / "raw",
        holiday_calendar_path=tmp_path / "holidays.csv",
        min_lead_score=0,
    )


async def _pending_document(
    database: DatabaseContext,
    *,
    url: str = "https://pncp.gov.br/ata.txt",
    status: str = "em julgamento",
    proposal_end_at: datetime | None = None,
):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="document-process-1",
            title="Pregão Eletrônico 14/2026",
            agency_name="Prefeitura Municipal de Exemplo",
            agency_cnpj="00000000000191",
            uf="MA",
            municipality="São Luís",
            estimated_value=180000,
            publication_at=datetime(2026, 9, 16, 14, 30, tzinfo=UTC),
            proposal_end_at=proposal_end_at,
            status=status,
            source_url="https://pncp.gov.br/processo/1",
            fingerprint="d" * 64,
        )
        session.add(procurement)
        await session.flush()
        document = Document(
            procurement_id=procurement.id,
            document_type="ata",
            title="Ata da sessão",
            original_url=url,
            extraction_status=ExtractionStatus.PENDING,
            published_at=datetime(2026, 9, 16, 14, 30, tzinfo=UTC),
            fingerprint="e" * 64,
        )
        session.add(document)
        await session.flush()
        return document.id


@pytest.mark.asyncio
async def test_text_document_creates_traceable_event_deadline_and_lead(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    """Scenario B: a participant found only in an ata remains evidence-backed."""

    document_id = await _pending_document(database)
    text = (
        "EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item 3, "
        "foi inabilitada por não apresentou a certidão exigida no item 8.4. "
        "Prazo até 20/09/2026 às 18:00."
    ).encode()
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(text),
    )

    summary = await service.process_pending()

    assert summary.documents_processed == 1
    assert summary.documents_failed == 0
    assert summary.participants_created == 1
    assert summary.events_created == 1
    assert summary.leads_created == 1
    async with database.sessions() as session:
        document = await session.get(Document, document_id)
        company = await session.scalar(select(Company))
        participant = await session.scalar(select(Participant))
        event = await session.scalar(select(ProcurementEvent))
        evidence = await session.scalar(select(Evidence))
        deadline = await session.scalar(select(Deadline))
        lead = await session.scalar(select(Lead))
        chunk_count = await session.scalar(select(func.count()).select_from(DocumentChunk))

    assert document is not None and document.extraction_status is ExtractionStatus.EXTRACTED
    assert document.local_path and Path(document.local_path).is_file()
    assert chunk_count == 1
    assert company is not None and company.cnpj == "00000000000191"
    assert participant is not None and participant.source_evidence_id is not None
    assert event is not None and event.event_type.value == "INELIGIBLE"
    assert event.reason_category.value == "MISSING_DOCUMENT"
    assert evidence is not None and evidence.document_id == document_id
    assert deadline is not None and deadline.calculation_method.value == "DOCUMENT_EXTRACTED"
    assert deadline.explicit_deadline_at is not None
    assert lead is not None and lead.triggering_event_id == event.id

    detection = await service.detect_existing()
    recalculation = await service.recalculate_deadlines_and_leads()
    assert detection.events_created == 0
    assert detection.participants_created == 0
    assert recalculation.leads_created == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Lead)) == 1


@pytest.mark.asyncio
async def test_download_failure_is_audited_on_document(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    document_id = await _pending_document(database)
    downloader = FakeDownloader(b"", error=DocumentDownloadError("temporarily unavailable"))
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=downloader,
    )

    summary = await service.process_pending(limit=1)

    assert summary.documents_failed == 1
    async with database.sessions() as session:
        document = await session.get(Document, document_id)
    assert document is not None and document.extraction_status is ExtractionStatus.FAILED
    assert "temporarily unavailable" in (document.extraction_error or "")


@pytest.mark.asyncio
async def test_zip_members_are_children_and_not_path_extracted(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    document_id = await _pending_document(database, url="https://pncp.gov.br/documentos.zip")
    archive = io.BytesIO()
    with ZipFile(archive, "w", ZIP_DEFLATED) as zipped:
        zipped.writestr(
            "atas/ata.txt",
            "EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante, "
            "foi desclassificada por proposta acima do valor estimado.",
        )
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(
            archive.getvalue(),
            filename="documentos.zip",
            mime="application/zip",
        ),
    )

    summary = await service.process_pending()

    assert summary.documents_processed == 1
    assert summary.events_created == 1
    async with database.sessions() as session:
        children = list(
            (
                await session.scalars(
                    select(Document).where(Document.parent_document_id == document_id)
                )
            ).all()
        )
    assert len(children) == 1
    assert children[0].title == "atas/ata.txt"
    assert children[0].local_path is None
    assert children[0].extraction_status is ExtractionStatus.EXTRACTED


@pytest.mark.asyncio
async def test_extraction_runs_off_the_event_loop(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    """Heavy document extraction must not freeze the API event loop."""

    await _pending_document(database)
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(b"documento sem evento relevante"),
    )
    real_extract = service.extractor.extract

    def slow_extract(*args: object, **kwargs: object):
        time.sleep(0.4)
        return real_extract(*args, **kwargs)

    service.extractor.extract = slow_extract
    processing = asyncio.create_task(service.process_pending())
    started = time.monotonic()
    await asyncio.sleep(0.05)
    elapsed = time.monotonic() - started
    assert elapsed < 0.3, "extraction must not block the event loop"
    summary = await asyncio.wait_for(processing, timeout=10)
    assert summary.documents_processed == 1


@pytest.mark.asyncio
async def test_open_process_participation_creates_evidence_bound_lead(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    """A documented participant in an open process triggers the new lead event."""

    await _pending_document(
        database,
        status="Divulgada no PNCP",
        proposal_end_at=datetime(2099, 1, 1, tzinfo=UTC),
    )
    text = b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item 3."
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(text),
    )

    summary = await service.process_pending()

    assert summary.documents_processed == 1
    assert summary.participants_created == 1
    assert summary.events_created == 1
    assert summary.leads_created == 1
    async with database.sessions() as session:
        event = await session.scalar(select(ProcurementEvent))
        lead = await session.scalar(select(Lead))

    assert event is not None
    assert event.event_type.value == "PARTICIPATION_DETECTED"
    assert event.evidence_id is not None
    assert lead is not None and lead.triggering_event_id == event.id


@pytest.mark.asyncio
async def test_closed_process_participation_does_not_trigger_lead(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    """The participation trigger only fires while the process can receive proposals."""

    await _pending_document(
        database,
        status="Homologada",
        proposal_end_at=datetime(2020, 1, 1, tzinfo=UTC),
    )
    text = b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item 3."
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(text),
    )

    summary = await service.process_pending()

    assert summary.participants_created == 1
    assert summary.events_created == 0
    assert summary.leads_created == 0
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Lead)) == 0
