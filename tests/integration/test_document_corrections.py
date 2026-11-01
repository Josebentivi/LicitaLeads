"""Corrective retries remain scoped, fenced and audited without resetting attempts."""

import pytest
from sqlalchemy import select

from app.config import Settings
from app.database import write_session
from app.models import CrawlRun, CrawlRunStatus, Document, ExtractionStatus
from app.services.ingestion.document_retry import (
    DocumentProcessingBusy,
    claim_corrective_attempt,
    corrective_scope,
    create_document_correction,
    create_document_retry,
)
from app.services.ingestion.documents import DownloadedDocument
from app.services.ingestion.pipeline import IngestionPipeline
from app.services.ingestion.processor import DocumentProcessingService

from .test_document_retry import seed


async def test_corrective_claim_is_scoped_single_use_and_keeps_backoff(database):
    parent, ids = await seed(database)
    settings = Settings(_env_file=None)
    async with write_session(database.sessions) as session:
        doc = await session.get_one(Document, ids[1])
        before = doc.extraction_attempts
        due = doc.next_attempt_at
        child, request = await create_document_correction(
            session,
            parent,
            settings,
            document_ids=(ids[1],),
            key="repair-zip-v1",
            reason="ZIP nesting and AppleDouble correction",
        )
        assert child.filters["_document_correction"]["attempts_before"][str(ids[1])] == before
        assert doc.extraction_attempts == before and doc.next_attempt_at == due
        assert await corrective_scope(session, child.id) == {ids[1]: before}
        assert await claim_corrective_attempt(session, child.id, doc)
        assert not await claim_corrective_attempt(session, child.id, doc)
        assert (await session.get_one(CrawlRun, child.id)).cursor["document_correction_claims"][
            str(ids[1])
        ]["wait_bypassed"]
        assert doc.next_attempt_at == due
    async with write_session(database.sessions) as session:
        with pytest.raises(DocumentProcessingBusy):
            await create_document_correction(
                session,
                parent,
                settings,
                document_ids=(ids[1],),
                key="repair-zip-v1",
                reason="duplicate",
            )


async def test_correction_cannot_bypass_maximum_or_activity_or_scope(database):
    parent, ids = await seed(database)
    async with write_session(database.sessions) as session:
        with pytest.raises(ValueError):
            await create_document_correction(
                session,
                parent,
                Settings(_env_file=None),
                document_ids=(ids[2], ids[3]),
                key="ineligible",
                reason="test",
            )
        with pytest.raises(ValueError):
            await create_document_correction(
                session,
                parent,
                Settings(_env_file=None),
                document_ids=(ids[6],),
                key="outside",
                reason="test",
            )


async def test_normal_retry_still_obeys_wait(database):
    parent, ids = await seed(database)
    async with write_session(database.sessions) as session:
        with pytest.raises(ValueError):
            await create_document_retry(
                session, parent, Settings(_env_file=None), document_ids=(ids[1],)
            )


async def test_corrective_pipeline_does_not_discover_and_preserves_parent(
    database, tmp_path, monkeypatch
):
    parent, ids = await seed(database)
    settings = Settings(
        _env_file=None,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )
    async with write_session(database.sessions) as session:
        child, request = await create_document_correction(
            session,
            parent,
            settings,
            document_ids=(ids[1],),
            key="repair-pipeline",
            reason="fixed parser",
        )
        child_id = child.id

    class Downloader:
        async def download(self, url):
            return DownloadedDocument(
                url, b"Documento oficial sem ocorrencias.", "ata.txt", "text/plain"
            )

        async def aclose(self):
            pass

    original = DocumentProcessingService

    def build(settings, **kwargs):
        return original(settings, downloader=Downloader(), **kwargs)

    monkeypatch.setattr("app.services.ingestion.processor.DocumentProcessingService", build)
    pipeline = IngestionPipeline(settings, session_factory=database.sessions)
    monkeypatch.setattr(
        pipeline, "_selected_connectors", lambda _: pytest.fail("must not rediscover APIs")
    )
    result = await pipeline.run(request, run_id=child_id)
    assert result.status is CrawlRunStatus.COMPLETED
    async with database.sessions() as session:
        assert (
            await session.get_one(Document, ids[1])
        ).extraction_status is ExtractionStatus.EXTRACTED
        assert (await session.get_one(CrawlRun, parent)).status is CrawlRunStatus.PARTIAL
        run = await session.get_one(CrawlRun, child_id)
        assert str(ids[1]) in run.cursor["document_correction_claims"]


async def test_templates_and_static_survive_changed_working_directory(
    api_client, tmp_path, monkeypatch
):
    from app.web import templates

    monkeypatch.chdir(tmp_path)
    templates.env.cache.clear()
    for path in ["/crawls", "/crawls/progress", "/crawls/table", "/static/app.css"]:
        response = await api_client.get(path)
        assert response.status_code == 200, (path, response.text[:200])


async def test_nested_archive_persists_only_leaf_evidence_and_llm_work(database, tmp_path):
    from uuid import uuid4

    from sqlalchemy import func

    from app.models import Evidence, LLMWorkItem, ProcurementEvent, SourceRecord
    from tests.unit.test_archive_corrections import zipped

    from .test_document_processing import FakeDownloader, _pending_document, _settings

    root_id = await _pending_document(database)
    async with write_session(database.sessions) as session:
        record = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/official",
            response_hash="c" * 64,
            raw_payload={},
            fingerprint=uuid4().hex,
        )
        session.add(record)
        await session.flush()
        doc = await session.get_one(Document, root_id)
        doc.source_record_id = record.id
        record_id = record.id
    text = "EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante, foi desclassificada por proposta acima do valor estimado."
    blob = zipped(("inner.zip", zipped(("ata.txt", text.encode()))))
    settings = _settings(tmp_path).model_copy(
        update={
            "llm_enabled": True,
            "llm_provider": "openai",
            "llm_api_key": "test",
            "llm_model": "test-model",
            "llm_min_chunk_characters": 1,
        }
    )
    service = DocumentProcessingService(
        settings,
        session_factory=database.sessions,
        downloader=FakeDownloader(blob, filename="root.zip", mime="application/zip"),
    )
    result = await service.process_pending()
    assert result.documents_processed == 1 and result.events_created == 1
    async with database.sessions() as session:
        inner = await session.scalar(select(Document).where(Document.parent_document_id == root_id))
        leaf = await session.scalar(select(Document).where(Document.parent_document_id == inner.id))
        assert leaf.title == "ata.txt" and leaf.source_record_id == record_id
        event = await session.scalar(select(ProcurementEvent))
        evidence = await session.get_one(Evidence, event.evidence_id)
        assert evidence.document_id == leaf.id
        assert (await session.scalar(select(LLMWorkItem))).document_id == leaf.id
        from app.services.llm.queue import enqueue_document

        assert (
            await enqueue_document(session, await session.get_one(Document, root_id), settings) == 0
        )
        assert await enqueue_document(session, inner, settings) == 0
        assert inner.is_active and leaf.is_active
        leaf_id = leaf.id
    await service.detect_existing()
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(LLMWorkItem)) == 1
        assert await session.scalar(select(func.count()).select_from(ProcurementEvent)) == 1
    async with write_session(database.sessions) as session:
        leaf = await session.get_one(Document, leaf_id)
        leaf.extraction_status = ExtractionStatus.FAILED
        leaf.next_attempt_at = None
    # An archive member is not an independently downloadable URL. Reprocessing
    # must start from the official container, never fetch its fragment URL.
    repeated = await service.process_pending()
    assert repeated.documents_processed == 0


async def test_structured_worker_error_is_recorded_on_run(database, tmp_path):
    from app.services.documents.errors import ExtractionFailure

    parent, ids = await seed(database)
    service = DocumentProcessingService(
        Settings(_env_file=None), session_factory=database.sessions, crawl_run_id=parent
    )
    async with database.sessions() as session:
        version = await service._document_version(session, await session.get_one(Document, ids[0]))
    await service._mark_failed(
        ids[0],
        ExtractionFailure("archive_depth_exceeded", member_path="nested.zip", input_hash="a" * 64),
        version=version,
    )
    await service.aclose()
    async with database.sessions() as session:
        doc = await session.get_one(Document, ids[0])
        run = await session.get_one(CrawlRun, parent)
        assert "archive_depth_exceeded" in doc.extraction_error
        assert run.cursor["document_errors"][str(ids[0])]["input_hash"] == "a" * 64
