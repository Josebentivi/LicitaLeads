"""Resolved downloads keep source URLs and only commit accepted document versions."""

import asyncio
import hashlib

import httpx
import pytest
from sqlalchemy import select

from app.models import Document, Evidence, ExtractionStatus, SourceRecord
from app.services.ingestion.documents import SecureDocumentDownloader
from app.services.ingestion.processor import DocumentProcessingService

from .test_document_processing import _pending_document, _settings
from .test_document_recheck_integrity import thread_extraction  # noqa: F401

ORIGINAL = "https://pncp.gov.br:48707/pncp-api/v1/orgaos/05962421000117/compras/2026/63/arquivos/1"
CANONICAL = "https://pncp.gov.br/api/pncp/v1/orgaos/05962421000117/compras/2026/63/arquivos/1"


@pytest.mark.parametrize("deactivate", [False, True])
async def test_resolution_preserves_original_and_checks_activity_before_commit(
    database,
    tmp_path,
    deactivate,
    thread_extraction,  # noqa: F811
):
    identifier = await _pending_document(database, url=ORIGINAL)
    payload = [{"url": ORIGINAL, "sequencialDocumento": 1}]
    async with database.sessions() as session, session.begin():
        record = SourceRecord(
            source="pncp",
            endpoint=CANONICAL.rsplit("/", 1)[0],
            response_hash="a" * 64,
            fingerprint="b" * 64,
            raw_payload=payload,
            request_parameters={},
            http_status=200,
        )
        session.add(record)
        await session.flush()
        (await session.get_one(Document, identifier)).source_record_id = record.id
    entered, release = asyncio.Event(), asyncio.Event()
    requests = []

    async def handler(request):
        requests.append(str(request.url))
        entered.set()
        await release.wait()
        return httpx.Response(
            200,
            content=(
                "EMPRESA EXEMPLO LTDA., CNPJ 00.000.000/0001-91, licitante do item 1, "
                "foi inabilitada por não apresentou a certidão exigida no item 8.4."
            ).encode(),
            headers={
                "Content-Type": "text/plain",
                "Content-Disposition": 'attachment; filename="ata.txt"',
            },
        )

    async def public(host, port):
        return ["93.184.216.34"]

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = DocumentProcessingService(
            _settings(tmp_path),
            session_factory=database.sessions,
            downloader=SecureDocumentDownloader(
                allowed_hosts={"pncp.gov.br"}, client=client, resolver=public
            ),
        )
        task = asyncio.create_task(service.process_pending(document_ids=(identifier,)))
        try:
            await asyncio.wait_for(entered.wait(), 10)
            if deactivate:
                async with database.sessions() as session, session.begin():
                    (await session.get_one(Document, identifier)).is_active = False
            release.set()
            result = await asyncio.wait_for(task, 30)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert requests == [CANONICAL]
    async with database.sessions() as session:
        doc = await session.get_one(Document, identifier)
        assert doc.original_url == ORIGINAL
        assert (await session.get_one(SourceRecord, doc.source_record_id)).raw_payload == payload
        evidence = list((await session.scalars(select(Evidence))).all())
        if deactivate:
            assert doc.resolved_url is None and doc.file_hash is None and not evidence
        else:
            assert (
                result.documents_processed == 1
                and doc.extraction_status == ExtractionStatus.EXTRACTED
            )
            assert doc.resolved_url == CANONICAL and doc.file_hash
            assert hashlib.sha256(doc.extracted_text.encode()).hexdigest() == doc.file_hash
            assert evidence and all(row.source_url == CANONICAL for row in evidence)
