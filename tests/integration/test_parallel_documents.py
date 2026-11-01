"""Independent document resource limits and validation at the final commit."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.database import write_session
from app.models import Document, ExtractionStatus, ProcurementEvent
from app.services.documents import DocumentExtractor
from app.services.ingestion import processor
from app.services.ingestion.documents import DownloadedDocument
from app.services.ingestion.pipeline import IngestionPipeline

from .test_document_processing import FakeDownloader, _pending_document, _settings
from .test_document_update_ordering import _observe


@pytest.mark.parametrize(("downloads", "processes"), [(4, 2), (1, 3)])
async def test_download_and_process_limits_and_inactive_document_revalidation(
    database,
    tmp_path,
    monkeypatch,
    downloads,
    processes,
):
    first = await _pending_document(database)
    async with write_session(database.sessions) as session:
        document = await session.get_one(Document, first)
        for index in range(1, 8):
            session.add(
                Document(
                    procurement_id=document.procurement_id,
                    document_type="ata",
                    title=f"Ata {index}",
                    original_url=f"https://pncp.gov.br/{index}.txt",
                    extraction_status=ExtractionStatus.PENDING,
                    fingerprint=hashlib.sha256(f"document-{index}".encode()).hexdigest(),
                )
            )
    download_full, process_full = asyncio.Event(), asyncio.Event()
    release_download, release_process = asyncio.Event(), asyncio.Event()
    active_download = active_process = peak_download = peak_process = 0
    text = b"Documento oficial sem ocorrencias adicionais."
    extraction = DocumentExtractor().extract(text, filename="ata.txt")

    class Downloader:
        async def download(self, url):
            nonlocal active_download, peak_download
            active_download += 1
            peak_download = max(peak_download, active_download)
            if active_download == downloads:
                download_full.set()
            try:
                await release_download.wait()
                return DownloadedDocument(url, text, "ata.txt", "text/plain")
            finally:
                active_download -= 1

    async def extract(*args, **kwargs):
        nonlocal active_process, peak_process
        active_process += 1
        peak_process = max(peak_process, active_process)
        if active_process == processes:
            process_full.set()
        try:
            await release_process.wait()
            return extraction
        finally:
            active_process -= 1

    monkeypatch.setattr(processor, "extract_in_process", extract)
    settings = _settings(tmp_path).model_copy(
        update={
            "document_download_concurrency": downloads,
            "document_process_concurrency": processes,
        }
    )
    service = processor.DocumentProcessingService(
        settings, session_factory=database.sessions, downloader=Downloader()
    )
    failures = []
    mark_failed = service._mark_failed

    async def capture_failure(document_id, exc, **kwargs):
        failures.append(f"{type(exc).__name__}: {exc}")
        return await mark_failed(document_id, exc, **kwargs)

    service._mark_failed = capture_failure
    task = asyncio.create_task(service.process_pending())
    try:
        await asyncio.wait_for(download_full.wait(), timeout=10)
        assert active_process == 0
        release_download.set()
        await asyncio.wait_for(process_full.wait(), timeout=10)
        # CPU/network preparation holds no SQLite domain transaction.
        async with write_session(database.sessions) as session:
            document = await session.get_one(Document, first)
            document.is_active = False
        release_process.set()
        result = await asyncio.wait_for(task, timeout=20)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert (peak_download, peak_process) == (downloads, processes)
    assert (active_download, active_process) == (0, 0)
    async with database.sessions() as session:
        errors = list(
            (
                await session.scalars(
                    select(Document.extraction_error).where(Document.extraction_error.is_not(None))
                )
            ).all()
        )
    assert result.documents_failed == 0, (errors, failures)
    assert result.documents_processed == 7 and result.documents_skipped == 1
    assert result.documents_failed == 0
    async with database.sessions() as session:
        document = await session.get_one(Document, first)
        assert document.extraction_status is ExtractionStatus.PENDING and document.file_hash is None
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 0


def test_document_concurrency_defaults_env_and_validation(monkeypatch):
    monkeypatch.delenv("DOCUMENT_DOWNLOAD_CONCURRENCY", raising=False)
    monkeypatch.delenv("DOCUMENT_PROCESS_CONCURRENCY", raising=False)
    defaults = Settings(_env_file=None)
    assert (defaults.document_download_concurrency, defaults.document_process_concurrency) == (4, 2)
    monkeypatch.setenv("DOCUMENT_DOWNLOAD_CONCURRENCY", "8")
    monkeypatch.setenv("DOCUMENT_PROCESS_CONCURRENCY", "4")
    changed = Settings(_env_file=None)
    assert (changed.document_download_concurrency, changed.document_process_concurrency) == (8, 4)
    for field in ("document_download_concurrency", "document_process_concurrency"):
        for value in (0, -1):
            with pytest.raises(ValueError):
                Settings(_env_file=None, **{field: value})


async def test_activity_transition_back_to_active_invalidates_prepared_content(database, tmp_path):
    pipeline = IngestionPipeline(_settings(tmp_path), session_factory=database.sessions)
    now = datetime.now(UTC)
    await _observe(database, pipeline, active=True, stamp=now, observed=now)
    async with database.sessions() as session:
        document_id = await session.scalar(select(Document.id))
    service = processor.DocumentProcessingService(
        _settings(tmp_path),
        session_factory=database.sessions,
        downloader=FakeDownloader(b"Ata oficial anterior a nova ativacao."),
    )
    try:
        prepared = await service._prepare_document(document_id)
        await _observe(
            database,
            pipeline,
            active=False,
            stamp=now + timedelta(seconds=1),
            observed=now + timedelta(seconds=1),
        )
        await _observe(
            database,
            pipeline,
            active=True,
            stamp=now + timedelta(seconds=2),
            observed=now + timedelta(seconds=2),
        )
        result = await service._persist_prepared(prepared, processor.ProcessingSummary())
        assert result is False
        async with database.sessions() as session:
            document = await session.get_one(Document, document_id)
            assert document.is_active and document.extraction_status is ExtractionStatus.PENDING
            assert document.file_hash is None and document.extracted_text is None
    finally:
        await service.aclose()


async def test_llm_is_enqueued_without_waiting_for_network(database, tmp_path):
    document_id = await _pending_document(database)
    completed = []

    class Analyzer:
        async def analyze(self, text, *, pages=None):
            async def change():
                async with write_session(database.sessions) as session:
                    document = await session.get_one(Document, document_id)
                    document.title = "Título atualizado pelo operador"

            await asyncio.wait_for(change(), timeout=2)
            completed.append(True)
            return []

    settings = _settings(tmp_path).model_copy(
        update={
            "llm_enabled": True,
            "llm_provider": "openai",
            "llm_model": "gpt-6-luna",
            "llm_api_key": "test",
            "llm_min_chunk_characters": 1,
        }
    )
    service = processor.DocumentProcessingService(
        settings,
        session_factory=database.sessions,
        downloader=FakeDownloader(b"Ata oficial sem eventos."),
        llm_analyzer=Analyzer(),
    )
    result = await service.process_pending()
    assert completed == [] and result.documents_processed == 1
    async with database.sessions() as session:
        from app.models import LLMWorkItem

        assert (
            await session.get_one(Document, document_id)
        ).extraction_status is ExtractionStatus.EXTRACTED
        assert await session.scalar(select(func.count()).select_from(LLMWorkItem)) == 1
