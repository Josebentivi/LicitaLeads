"""End-to-end document-to-lead processing scenarios."""

from __future__ import annotations

import asyncio
import io
import threading
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
    ProcurementPhase,
)
from app.services.documents import processes
from app.services.ingestion.documents import DocumentDownloadError, DownloadedDocument
from app.services.ingestion.processor import DocumentProcessingService, ProcessingSummary

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
            # The bidder in the document is a different legal entity from its buyer.
            agency_cnpj="01612325000198",
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
    monkeypatch,
) -> None:
    """Heavy document extraction must not freeze the API event loop."""

    await _pending_document(database)
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(b"documento sem evento relevante"),
    )
    entered, release = threading.Event(), threading.Event()
    main_thread = threading.get_ident()
    real_supervise = processes._supervise

    def supervised(*args, **kwargs):
        assert threading.get_ident() != main_thread
        entered.set()
        if not release.wait(timeout=10):
            raise AssertionError("a API não respondeu enquanto o processo era preparado")
        return real_supervise(*args, **kwargs)

    monkeypatch.setattr(processes, "_supervise", supervised)
    processing = asyncio.create_task(service.process_pending())
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        await asyncio.sleep(0)
        assert not processing.done()
        release.set()
        summary = await asyncio.wait_for(processing, timeout=20)
        assert summary.documents_processed == 1
    finally:
        release.set()
        processing.cancel()
        await asyncio.gather(processing, return_exceptions=True)


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


@pytest.mark.asyncio
async def test_recheck_reprocesses_changed_session_document(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    document_id = await _pending_document(
        database,
        status="Em andamento",
        proposal_end_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    async with database.sessions() as session, session.begin():
        procurement = await session.scalar(select(Procurement))
        assert procurement is not None
        procurement.current_phase = ProcurementPhase.DISPUTE

    downloader = FakeDownloader(
        b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item 3."
    )
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=downloader,
    )
    first = await service.process_pending()
    assert first.participants_created == 1

    downloader.content = (
        b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, foi convocada para "
        b"diligencia e tera o prazo de 2 horas para apresentar documentos."
    )
    summary = await service.recheck_stale_session_documents()
    await service.aclose()

    assert summary.documents_processed == 1
    assert summary.events_created >= 1
    async with database.sessions() as session:
        document = await session.get(Document, document_id)
        events = list((await session.scalars(select(ProcurementEvent))).all())
    assert document is not None and document.file_hash is not None
    assert any(event.event_type.value == "DILIGENCE_REQUESTED" for event in events)


@pytest.mark.asyncio
async def test_recheck_unchanged_document_skips_extraction(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    document_id = await _pending_document(
        database, status="Em andamento", proposal_end_at=datetime(2026, 9, 1, tzinfo=UTC)
    )
    downloader = FakeDownloader(b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91.")
    service = DocumentProcessingService(
        _settings(tmp_path), session_factory=database.sessions, downloader=downloader
    )
    await service.process_pending()

    class CountingExtractor:
        calls = 0

        def extract(self, *args, **kwargs):
            self.calls += 1
            raise AssertionError("unchanged document must not be re-extracted")

    counter = CountingExtractor()
    service.extractor = counter
    summary = await service.recheck_document(document_id)
    await service.aclose()

    assert summary.documents_processed == 0
    assert counter.calls == 0


@pytest.mark.asyncio
async def test_session_recheck_rotates_failed_documents(
    database: DatabaseContext,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="recheck-rotation",
            status="Em andamento",
            current_phase=ProcurementPhase.DISPUTE,
            fingerprint="t" * 64,
        )
        session.add(procurement)
        await session.flush()
        documents = []
        for index in (1, 2):
            document = Document(
                procurement_id=procurement.id,
                document_type="ata_sessao",
                title=f"Ata da sessao {index}",
                original_url=f"https://pncp.gov.br/ata-{index}.txt",
                extraction_status=ExtractionStatus.PENDING,
                downloaded_at=datetime(2026, 9, 1, tzinfo=UTC),
                fingerprint=str(index) * 64,
            )
            session.add(document)
            documents.append(document)
        await session.flush()
        document_ids = [item.id for item in documents]

    service = DocumentProcessingService(_settings(tmp_path), session_factory=database.sessions)
    selected: list = []
    failed_ids: list = []

    async def fake_recheck(document_id):
        selected.append(document_id)
        if document_id == document_ids[0]:
            failed_ids.append(document_id)
            raise RuntimeError("download failed")
        # Successful rechecks now mark their accepted version themselves;
        # the polling wrapper only rotates failures with a version guard.
        await service._mark_checked(document_id)
        return ProcessingSummary()

    monkeypatch.setattr(service, "recheck_document", fake_recheck)
    first = await service.recheck_stale_session_documents(limit=1)
    second = await service.recheck_stale_session_documents(limit=1)
    await service.aclose()

    assert first.documents_failed + second.documents_failed == 1
    assert first.documents_processed + second.documents_processed == 1
    assert set(selected) == set(document_ids)
    async with database.sessions() as session:
        failed = await session.get(Document, failed_ids[0])
    assert failed is not None and failed.last_checked_at is not None


@pytest.mark.asyncio
async def test_transient_document_failure_is_retried(
    database: DatabaseContext,
    tmp_path: Path,
) -> None:
    document_id = await _pending_document(database)
    downloader = FakeDownloader(b"", error=DocumentDownloadError("temporary timeout"))
    service = DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=downloader,
    )
    first = await service.process_pending()
    assert first.documents_failed == 1
    async with database.sessions() as session, session.begin():
        document = await session.get(Document, document_id)
        assert document is not None
        assert document.extraction_attempts == 1
        assert document.next_attempt_at is not None
        document.next_attempt_at = datetime(2000, 1, 1, tzinfo=UTC)

    downloader.error = None
    downloader.content = b"EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91."
    second = await service.process_pending()
    await service.aclose()
    assert second.documents_processed == 1
    async with database.sessions() as session:
        document = await session.get(Document, document_id)
    assert document is not None
    assert document.extraction_attempts == 0
    assert document.next_attempt_at is None
