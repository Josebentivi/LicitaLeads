"""Municipal provenance distinguishes identical responses to different requests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability, RawSourceRecord
from app.connectors.http import AsyncHTTPClient
from app.connectors.municipal_payments import build_municipal_connector
from app.models import CrawlRun, CrawlRunStatus, Payment, SourceRecord
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService

from .conftest import DatabaseContext

SOURCE = "municipal:imperatriz"
ENDPOINT = "https://transparencia.test/VersaoJson/Despesas/"
COLLECTED_AT = datetime(2026, 1, 1, tzinfo=UTC)
DETAIL = {
    "LIQ": "1",
    "PAR": "1",
    "VALOR": "100,00",
    "DATAE": "10/03/2026 00:00:00",
    "FORNECEDOR": "Empresa Teste Ltda",
}


def _response(parameters: dict[str, Any], payload: Any) -> RawSourceRecord:
    return RawSourceRecord.build(
        source=SOURCE,
        endpoint=ENDPOINT,
        request_parameters=parameters,
        raw_payload=payload,
        http_status=200,
        collected_at=COLLECTED_AT,
    )


@pytest.mark.parametrize("payload", [[], [DETAIL]], ids=["empty", "same-payment-row"])
async def test_distinct_municipal_requests_preserve_each_response(
    database: DatabaseContext, payload: list[dict[str, str]]
) -> None:
    service = MunicipalPaymentIngestionService(
        Settings(_env_file=None), session_factory=database.sessions
    )
    records = [
        _response({"Ano": "2026", "intNumeroEmpenho": number}, payload) for number in ("100", "200")
    ]
    availability = DataAvailability.AVAILABLE if payload else DataAvailability.EMPTY
    try:
        run_id = await service.create_run("imperatriz", 2026, None, True)
        identifiers = await service._persist_source_records(
            run_id,
            ConnectorResult(data=[], raw_records=records, availability=availability),
        )
    finally:
        await service.aclose()

    assert len(set(identifiers)) == 2
    async with database.sessions() as session:
        stored = (await session.scalars(select(SourceRecord))).all()
    assert len(stored) == 2
    by_id = {record.id: record for record in stored}
    for identifier, raw in zip(identifiers, records, strict=True):
        record = by_id[identifier]
        assert record.crawl_run_id == run_id
        assert record.source == raw.source
        assert record.endpoint == raw.endpoint
        assert record.request_parameters == raw.request_parameters
        assert record.raw_payload == payload
        assert record.response_hash == raw.response_hash
        assert record.http_status == 200
        assert record.availability.value == availability.value
        assert record.collected_at == COLLECTED_AT


async def test_identical_municipal_request_deduplicates_reordered_parameters(
    database: DatabaseContext,
) -> None:
    service = MunicipalPaymentIngestionService(
        Settings(_env_file=None), session_factory=database.sessions
    )
    first = _response({"Ano": "2026", "Empresa": "1", "intNumeroEmpenho": "100"}, [DETAIL])
    repeated = _response(
        {"intNumeroEmpenho": "100", "Empresa": "1", "Ano": "2026"}, [DETAIL]
    ).model_copy(update={"collected_at": COLLECTED_AT + timedelta(minutes=1)})
    try:
        run_id = await service.create_run("imperatriz", 2026, None, True)
        identifiers = await service._persist_source_records(
            run_id,
            ConnectorResult(
                data=[],
                raw_records=[first, repeated],
                availability=DataAvailability.AVAILABLE,
            ),
        )
    finally:
        await service.aclose()

    assert identifiers[0] == identifiers[1]
    async with database.sessions() as session:
        stored = (await session.scalars(select(SourceRecord))).all()
    assert len(stored) == 1
    assert stored[0].request_parameters == first.request_parameters
    assert stored[0].raw_payload == [DETAIL]
    assert stored[0].collected_at == COLLECTED_AT


async def test_identical_detail_rows_link_payments_to_their_own_requests(
    database: DatabaseContext,
) -> None:
    settings = Settings(
        _env_file=None,
        http_min_request_interval_seconds=0,
        http_max_retries=0,
        allow_insecure_http_sources=True,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["Listagem"] == "DespesasGerais":
            payload = [
                {
                    "EMPRESA": request.url.params["Empresa"],
                    "PKEMP": number,
                    "CODIGO": number,
                    "TPEM": "OR",
                    "CPFFORMATADO": "52.246.769/0001-98",
                    "NOMEFOR": "Empresa Teste Ltda",
                    "PAGO": "100,00",
                }
                for number in ("100", "200")
            ]
        else:
            payload = [DETAIL]
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = build_municipal_connector(
            "imperatriz", settings, http_client=AsyncHTTPClient(settings, client=client)
        )
        service = MunicipalPaymentIngestionService(
            settings, session_factory=database.sessions, connector=connector
        )
        try:
            summary = await service.run("imperatriz", year=2026, include_details=True)
        finally:
            await service.aclose()

    assert summary.payments_created == 2, summary.diagnostic
    async with database.sessions() as session:
        payments = (await session.scalars(select(Payment))).all()
        sources = {record.id: record for record in (await session.scalars(select(SourceRecord)))}
        run = await session.get(CrawlRun, summary.crawl_run_id)

    assert run is not None and run.status is CrawlRunStatus.COMPLETED
    assert len(payments) == 2
    assert len({payment.source_record_id for payment in payments}) == 2
    assert {payment.commitment_number for payment in payments} == {"100", "200"}
    for payment in payments:
        record = sources[payment.source_record_id]
        assert record.crawl_run_id == summary.crawl_run_id
        assert record.raw_payload == [DETAIL]
        assert record.request_parameters["intNumeroEmpenho"] == payment.commitment_number
        assert payment.source_url is not None
        assert dict(httpx.URL(payment.source_url).params) == record.request_parameters
        assert payment.amount == Decimal("100")
        assert payment.status == "detalhe_empenho"
