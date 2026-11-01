import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    Document,
    ExtractionStatus,
    JobLease,
    Procurement,
    SourceRecord,
)
from app.services.ingestion.document_retry import (
    DOCUMENT_LEASE,
    DocumentProcessingBusy,
    create_document_retry,
    document_processing_lease,
    documents_for_run,
)
from app.services.ingestion.documents import DocumentDownloadError, DownloadedDocument
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.ingestion.processor import DocumentProcessingService


async def test_internal_retry_subset_preserves_attempts_and_rejects_other_runs(database):
    parent, ids = await seed(database)
    async with database.sessions() as session, session.begin():
        document = await session.get_one(Document, ids[5])
        document.extraction_status = ExtractionStatus.FAILED
        with pytest.raises(ValueError, match="lote original"):
            await create_document_retry(
                session, parent, Settings(_env_file=None), document_ids=(ids[6],)
            )
        child, request = await create_document_retry(
            session, parent, Settings(_env_file=None), document_ids=(ids[5],)
        )
        assert request.document_ids == (str(ids[5]),)
        assert child.filters["retry_of_run_id"] == str(parent)
        assert document.extraction_attempts == 1


async def seed(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        parent = CrawlRun(connector="pncp", status=CrawlRunStatus.PARTIAL, filters={"uf": "MA"})
        procurement = Procurement(source="pncp", external_id="document-retry", fingerprint="a" * 64)
        session.add_all([parent, procurement])
        await session.flush()
        documents = []
        for index in range(7):
            document = Document(
                procurement_id=procurement.id,
                title=f"arquivo-{index}.txt",
                original_url=f"https://pncp.gov.br/file/{index}",
                fingerprint=str(index) * 64,
                extraction_status=ExtractionStatus.FAILED,
                extraction_attempts=1,
                extraction_error="DocumentDownloadError: document source returned HTTP 422",
                next_attempt_at=now - timedelta(minutes=1),
            )
            if index == 1:
                document.next_attempt_at = now + timedelta(hours=1)
            if index == 2:
                document.extraction_attempts = 5
            if index == 3:
                document.is_active = False
            if index == 4:
                document.extraction_status = ExtractionStatus.EXTRACTED
            if index == 5:
                document.extraction_status = ExtractionStatus.PENDING
            session.add(document)
            documents.append(document)
        await session.flush()
        parent.cursor = {
            "sources": {
                "documents": {
                    "status": "partial",
                    "document_ids": [str(row.id) for row in documents[:6]],
                    "documents_failed": 4,
                }
            }
        }
        return parent.id, [row.id for row in documents]


async def test_retry_api_is_selective_atomic_and_does_not_reset_failures(
    database, api_client, monkeypatch
):
    parent, ids = await seed(database)
    launched = []
    monkeypatch.setattr(
        "app.api.routes.crawls.launch_crawl",
        lambda run_id, request: launched.append((run_id, request)),
    )
    responses = await asyncio.gather(
        *[api_client.post(f"/api/crawls/{parent}/retry-documents") for _ in range(2)]
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    assert len(launched) == 1
    child_id, request = launched[0]
    assert request.mode == "documents" and request.document_ids == (str(ids[0]),)
    assert request.retry_of_run_id == str(parent)
    async with database.sessions() as session:
        assert await session.scalar(select(func.count(CrawlRun.id))) == 2
        assert (await session.get(Document, ids[0])).extraction_attempts == 1
        assert (await session.get(Document, ids[1])).next_attempt_at > datetime.now(UTC)
        child = await session.get(CrawlRun, child_id)
        assert child.filters["document_ids"] == [str(ids[0])]


async def test_retry_page_displays_files_deadlines_limits_and_escaped_errors(database, api_client):
    parent, ids = await seed(database)
    async with database.sessions() as session, session.begin():
        (await session.get(Document, ids[0])).title = "<script>private-token</script>"
    response = await api_client.get(f"/crawls/{parent}/documents")
    assert response.status_code == 200
    assert "1</strong> documento(s) elegível" in response.text
    assert "Limite atingido" in response.text and "Próxima tentativa" in response.text
    assert "&lt;script&gt;private-token&lt;/script&gt;" in response.text
    assert "<script>private-token</script>" not in response.text
    assert "arquivo-6" not in response.text
    assert "Reprocessar documentos com falha" in (await api_client.get("/crawls/table")).text
    assert (await api_client.get(f"/crawls/{uuid4()}/documents")).status_code == 404


async def test_document_only_pipeline_never_discovers_and_preserves_unselected_documents(
    database, tmp_path, monkeypatch
):
    parent, ids = await seed(database)
    calls = []

    class Downloader:
        async def download(self, url):
            calls.append(url)
            return DownloadedDocument(
                url=url,
                content=b"Documento oficial sem eventos.",
                filename="arquivo.txt",
                declared_mime="text/plain",
            )

        async def aclose(self):
            pass

    original = DocumentProcessingService

    def build(settings, *, session_factory, http_budgets=None, crawl_run_id=None):
        return original(
            settings,
            session_factory=session_factory,
            downloader=Downloader(),
            http_budgets=http_budgets,
            crawl_run_id=crawl_run_id,
        )

    monkeypatch.setattr("app.services.ingestion.processor.DocumentProcessingService", build)
    settings = Settings(
        _env_file=None,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )
    pipeline = IngestionPipeline(settings, session_factory=database.sessions)
    monkeypatch.setattr(
        pipeline,
        "_selected_connectors",
        lambda _: pytest.fail("document retries cannot rediscover procurements"),
    )
    summary = await pipeline.run(
        PipelineRequest(
            connector="pncp",
            mode="documents",
            document_ids=tuple(str(value) for value in ids[:6]),
            retry_of_run_id=str(parent),
        )
    )
    assert summary.status == CrawlRunStatus.COMPLETED and summary.records_found == 1
    assert summary.records_updated == 1 and len(calls) == 1
    async with database.sessions() as session:
        assert (await session.get(Document, ids[0])).extraction_status == ExtractionStatus.EXTRACTED
        assert (await session.get(Document, ids[6])).extraction_status == ExtractionStatus.FAILED
        assert (await session.get(Document, ids[5])).extraction_status == ExtractionStatus.PENDING
        run = await session.get(CrawlRun, summary.run_id)
        assert run.cursor["sources"]["documents"]["document_ids"] == [str(ids[0])]
        assert await session.scalar(select(func.count(SourceRecord.id))) == 0
        assert await session.scalar(select(func.count(JobLease.name))) == 0


async def test_legacy_scope_never_includes_unrelated_failures_and_respects_empty_snapshots(
    database,
):
    parent, ids = await seed(database)
    async with database.sessions() as session, session.begin():
        run = await session.get(CrawlRun, parent)
        run.cursor = {}
        source = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/list",
            request_parameters={},
            response_hash="a" * 64,
            raw_payload=[],
            availability="available",
            fingerprint="b" * 64,
            crawl_run_id=parent,
            collected_at=datetime.now(UTC),
        )
        session.add(source)
        await session.flush()
        (await session.get(Document, ids[0])).source_record_id = source.id
    async with database.sessions() as session:
        run = await session.get(CrawlRun, parent)
        assert [row.id for row in await documents_for_run(session, run)] == [ids[0]]
        run.cursor = {"sources": {"documents": {"document_ids": []}}}
        assert await documents_for_run(session, run) == []


async def test_standalone_document_lease_excludes_retry_and_other_workers(database, api_client):
    parent, _ = await seed(database)
    async with document_processing_lease(database.sessions):
        assert (await api_client.post(f"/api/crawls/{parent}/retry-documents")).status_code == 409
        with pytest.raises(DocumentProcessingBusy):
            async with document_processing_lease(database.sessions):
                pytest.fail("two workers admitted")
    async with database.sessions() as session:
        assert await session.get(JobLease, DOCUMENT_LEASE) is None


async def test_retry_refuses_early_or_unknown_runs_without_creating_work(database, api_client):
    parent, ids = await seed(database)
    async with database.sessions() as session, session.begin():
        (await session.get(Document, ids[0])).next_attempt_at = datetime.now(UTC) + timedelta(
            hours=1
        )
    assert (await api_client.post(f"/api/crawls/{parent}/retry-documents")).status_code == 409
    assert (await api_client.post(f"/api/crawls/{uuid4()}/retry-documents")).status_code == 404
    async with database.sessions() as session:
        assert await session.scalar(select(func.count(CrawlRun.id))) == 1
        assert (await session.get(Document, ids[0])).extraction_attempts == 1


async def test_retry_failure_is_partial_and_keeps_backoff_without_rediscovery(
    database, tmp_path, monkeypatch
):
    parent, ids = await seed(database)
    original = DocumentProcessingService

    class UnavailableDownloader:
        async def download(self, url):
            raise DocumentDownloadError(
                "PNCP: falha temporária ao carregar o arquivo (HTTP 422; 3 tentativas)"
            )

        async def aclose(self):
            pass

    monkeypatch.setattr(
        "app.services.ingestion.processor.DocumentProcessingService",
        lambda settings, session_factory, http_budgets=None, crawl_run_id=None: original(
            settings,
            session_factory=session_factory,
            downloader=UnavailableDownloader(),
            http_budgets=http_budgets,
            crawl_run_id=crawl_run_id,
        ),
    )
    settings = Settings(
        _env_file=None,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )
    summary = await IngestionPipeline(settings, session_factory=database.sessions).run(
        PipelineRequest(
            connector="pncp",
            mode="documents",
            document_ids=(str(ids[0]),),
            retry_of_run_id=str(parent),
        )
    )
    assert summary.status == CrawlRunStatus.PARTIAL and summary.records_failed == 1
    async with database.sessions() as session:
        document = await session.get(Document, ids[0])
        assert (
            document.extraction_status == ExtractionStatus.FAILED
            and document.extraction_attempts == 2
        )
        assert document.next_attempt_at > datetime.now(UTC) + timedelta(minutes=29)
        assert document.downloaded_at is None
        run = await session.get(CrawlRun, summary.run_id)
        assert run.cursor["sources"]["documents"]["documents_failed"] == 1
        assert await session.scalar(select(func.count(SourceRecord.id))) == 0


async def test_cancelling_document_worker_releases_its_shared_lease(database):
    entered = asyncio.Event()

    async def operation():
        async with document_processing_lease(database.sessions):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with database.sessions() as session:
        assert await session.get(JobLease, DOCUMENT_LEASE) is None


async def test_document_deactivated_during_download_is_not_counted_as_processed(
    database, tmp_path, monkeypatch
):
    parent, ids = await seed(database)
    original = DocumentProcessingService

    class Downloader:
        async def download(self, url):
            async with database.sessions() as session, session.begin():
                (await session.get(Document, ids[0])).is_active = False
            return DownloadedDocument(
                url=url, content=b"Documento oficial", filename="x.txt", declared_mime="text/plain"
            )

        async def aclose(self):
            pass

    monkeypatch.setattr(
        "app.services.ingestion.processor.DocumentProcessingService",
        lambda settings, session_factory, http_budgets=None, crawl_run_id=None: original(
            settings,
            session_factory=session_factory,
            downloader=Downloader(),
            http_budgets=http_budgets,
            crawl_run_id=crawl_run_id,
        ),
    )
    settings = Settings(
        _env_file=None,
        document_storage_path=tmp_path / "docs",
        raw_data_storage_path=tmp_path / "raw",
    )
    summary = await IngestionPipeline(settings, session_factory=database.sessions).run(
        PipelineRequest(
            mode="documents",
            connector="pncp",
            document_ids=(str(ids[0]),),
            retry_of_run_id=str(parent),
        )
    )
    assert summary.status == CrawlRunStatus.PARTIAL and summary.records_updated == 0
    async with database.sessions() as session:
        run = await session.get(CrawlRun, summary.run_id)
        assert run.cursor["sources"]["documents"]["documents_skipped"] == 1
        assert (await session.get(Document, ids[0])).downloaded_at is None
