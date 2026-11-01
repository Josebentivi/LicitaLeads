"""Previously uncovered mutation handlers: dispatch, guards and durable effects."""

from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app import web
from app.api.routes import crawls, payments
from app.config import Settings
from app.connectors.base import DataAvailability
from app.models import ExtractionStatus, LLMBatch, LLMWorkItem, Payment
from app.services.ingestion.payment_import import PaymentImportService
from app.services.ingestion.payments import PaymentIngestionSummary
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from tests.integration.test_collection_continuation import CountingConnector, records
from tests.integration.test_llm_queue import seed


async def test_payment_import_handler_persists_and_repeats_without_duplicates(
    api_client, database, tmp_path, monkeypatch
):
    settings = Settings(
        _env_file=None,
        raw_data_storage_path=tmp_path / "raw",
        document_storage_path=tmp_path / "docs",
    )
    monkeypatch.setattr(
        payments,
        "PaymentImportService",
        lambda: PaymentImportService(settings, session_factory=database.sessions),
    )
    content = b"id_pagamento;cnpj;fornecedor;valor;data_pagamento\nOFICIAL-1;11222333000181;Empresa Exemplo;10,50;2026-10-01\n"

    async def upload(origin=None):
        return await api_client.post(
            "/api/payments/import",
            files={"file": ("payments.csv", content)},
            data={"source_name": "Portal sintético"},
            headers={"Origin": origin} if origin else {},
        )

    assert (await upload("https://untrusted.example")).status_code == 403
    first = await upload()
    assert first.status_code == 200 and first.json()["created"] == 1, first.text
    second = await upload()
    assert second.status_code == 200 and second.json()["updated"] == 1
    listed = await api_client.get("/api/payments", params={"supplier_cnpj": "11222333000181"})
    assert listed.json()["total"] == 1
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
    assert (await api_client.post("/api/payments/import")).status_code == 422
    invalid = await api_client.post(
        "/api/payments/import", files={"file": ("bad.json", b"invalid")}
    )
    assert invalid.status_code == 422


@pytest.mark.parametrize(
    "availability,status",
    [
        (DataAvailability.AVAILABLE, 200),
        (DataAvailability.EMPTY, 200),
        (DataAvailability.NOT_SUPPORTED, 422),
        (DataAvailability.ACCESS_RESTRICTED, 403),
        (DataAvailability.TEMPORARY_ERROR, 503),
    ],
)
async def test_payment_crawl_handler_maps_availability_without_inventing_facts(
    api_client, database, monkeypatch, availability, status
):
    calls = []

    class Service:
        async def run(self, **kwargs):
            calls.append(kwargs)
            return PaymentIngestionSummary(availability=availability, diagnostic="controle offline")

        async def aclose(self):
            calls.append("closed")

    monkeypatch.setattr(payments, "PaymentIngestionService", Service)
    response = await api_client.post(
        "/api/payments/crawl", json={"company_cnpj": "11222333000181", "year": 2026}
    )
    assert response.status_code == status, response.text
    assert calls[0]["company_cnpj"] == "11222333000181" and calls[-1] == "closed"
    assert (
        await api_client.post("/api/payments/crawl", json={"company_cnpj": "invalid"})
    ).status_code == 422
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_web_continuation_dispatch_and_duplicate_guard(api_client, database, monkeypatch):
    first = await IngestionPipeline(
        Settings(_env_file=None, crawl_max_records_per_source=1),
        session_factory=database.sessions,
        connectors={"pncp": CountingConnector(records())},
    ).run(PipelineRequest(connector="pncp", process_documents=False))
    launched = []
    monkeypatch.setattr(crawls, "launch_continuation", lambda *args: launched.append(args))
    url = f"/crawls/{first.run_id}/continue"
    assert (
        await api_client.post(url, headers={"Origin": "https://outside.example"})
    ).status_code == 403
    assert (await api_client.post(url)).status_code == 303 and len(launched) == 1
    assert (await api_client.post(url)).status_code == 409


@pytest.mark.parametrize("action", ["continue", "retry-documents", "retry-llm"])
async def test_web_run_actions_missing_invalid_and_cross_origin(api_client, action):
    url = f"/crawls/{uuid4()}/{action}"
    assert (await api_client.post(url)).status_code == 404
    assert (await api_client.post(f"/crawls/invalid/{action}")).status_code == 422
    assert (
        await api_client.post(url, headers={"Origin": "https://outside.example"})
    ).status_code == 403


async def test_web_document_retry_dispatches_only_eligible_work(api_client, database, monkeypatch):
    from app.models import CrawlRun, CrawlRunStatus, Document
    from tests.integration.test_document_processing import _pending_document

    doc_id = await _pending_document(database)
    async with database.sessions() as session, session.begin():
        doc = await session.get_one(Document, doc_id)
        run = CrawlRun(
            connector="pncp",
            status=CrawlRunStatus.COMPLETED,
            filters={"uf": "MA"},
            cursor={"document_ids": [str(doc_id)]},
        )
        session.add(run)
        await session.flush()
        doc.extraction_status = ExtractionStatus.FAILED
        run.cursor = {
            "sources": {"documents": {"status": "partial", "document_ids": [str(doc_id)]}}
        }
        run_id = run.id
        assert doc.extraction_attempts == 0
    launched = []
    monkeypatch.setattr(web, "launch_crawl", lambda *args: launched.append(args))
    response = await api_client.post(f"/crawls/{run_id}/retry-documents")
    assert response.status_code == 303, response.text
    assert len(launched) == 1
    async with database.sessions() as session:
        assert (await session.get_one(Document, doc_id)).extraction_attempts == 0


async def test_web_llm_actions_cancel_retry_and_run_retry(
    api_client, database, tmp_path, monkeypatch
):
    settings = Settings(
        _env_file=None,
        llm_enabled=True,
        llm_provider="openai",
        llm_model="controlled",
        llm_api_key="synthetic",
        llm_min_chunk_characters=1,
        raw_data_storage_path=tmp_path / "raw",
    )
    _, run_id = await seed(database, settings)
    monkeypatch.setattr(web, "get_settings", lambda: settings)
    monkeypatch.setattr(crawls, "get_settings", lambda: settings)
    async with database.sessions() as session:
        batch_id = (await session.scalar(select(LLMBatch))).id
    cancel = f"/crawls/llm/{batch_id}/cancel"
    assert (
        await api_client.post(cancel, headers={"Origin": "https://outside.example"})
    ).status_code == 403
    assert (await api_client.post(cancel)).status_code == 303
    async with database.sessions() as session:
        assert (await session.get_one(LLMBatch, batch_id)).cancelled
        assert (await session.scalar(select(LLMWorkItem))).status == "cancelled"
    assert (await api_client.post(f"/crawls/llm/{batch_id}/retry")).status_code == 303
    async with database.sessions() as session, session.begin():
        work = await session.scalar(select(LLMWorkItem))
        assert work.status == "pending"
        work.status = "failed"
    assert (await api_client.post(f"/crawls/{run_id}/retry-llm")).status_code == 303
    assert (await api_client.post(f"/crawls/{run_id}/retry-llm")).status_code == 409
    for action in ("cancel", "retry"):
        assert (await api_client.post(f"/crawls/llm/{uuid4()}/{action}")).status_code == 404
        assert (await api_client.post(f"/crawls/llm/invalid/{action}")).status_code == 422


@pytest.mark.parametrize(
    "failure,status",
    [(None, 303), (LookupError("missing"), 404), (ValueError("foreign event"), 409)],
)
async def test_web_link_company_dispatch_validates_identity_and_preserves_review(
    api_client, monkeypatch, failure, status
):
    captured = []

    class Service:
        async def link_event_company(self, **kwargs):
            captured.append(kwargs)
            if failure:
                raise failure

        async def aclose(self):
            captured.append("closed")

    monkeypatch.setattr(web, "DocumentProcessingService", Service)
    proc, event, company = uuid4(), uuid4(), uuid4()
    url = f"/procurements/{proc}/events/{event}/link-company"
    response = await api_client.post(
        url,
        data={
            "company_id": str(company),
            "reviewer": " Operador ",
            "note": " Evidência conferida ",
        },
    )
    assert response.status_code == status
    assert captured[0]["expected_procurement_id"] == proc and captured[0]["reviewer"] == "Operador"
    assert captured[-1] == "closed"
    assert (await api_client.post(url, data={"company_id": "invalid"})).status_code == 422
    assert (
        await api_client.post(
            url, data={"company_id": str(company)}, headers={"Origin": "https://outside.example"}
        )
    ).status_code == 403
