"""Municipal payment ingestion, API and operator page."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

from app.config import Settings
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    RawPayment,
    RawSourceRecord,
)
from app.connectors.municipal_payments import build_municipal_connector
from app.connectors.municipal_payments.base import MunicipalSource
from app.models import Contract, CrawlRun, CrawlRunStatus, Payment, SourceRecord
from app.services.ingestion.municipal_payments import (
    MunicipalPaymentIngestionService,
    MunicipalPaymentSummary,
    latest_municipal_runs,
)
from app.services.ingestion.payments import PaymentIngestionService

from .conftest import DatabaseContext

SUPPLIER_CNPJ = "52246769000198"
AGENCY_CNPJ = "12345678000199"
SOURCE = MunicipalSource(
    id="imperatriz",
    name="Imperatriz",
    municipality="Imperatriz",
    uf="MA",
    adapter="fiorilli_json",
    base_url="https://transparencia.test",
    mode="json",
    note="test",
    supports_details=True,
)


class FakeMunicipalConnector:
    def __init__(self, *, fail: bool = False) -> None:
        self.source = SOURCE
        self.fail = fail
        self.closed = False

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]:
        del company_cnpj, include_details
        if self.fail:
            raise RuntimeError("portal fora do ar")
        raw = RawSourceRecord.build(
            source="municipal:imperatriz",
            endpoint="https://transparencia.test/VersaoJson/Despesas/",
            request_parameters={"ano": year},
            raw_payload=[{"CODIGO": "100"}],
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=[
                RawPayment(
                    source="municipal:imperatriz",
                    source_url="https://transparencia.test/despesas",
                    raw_payload={"CODIGO": "100"},
                    external_id="imperatriz:2026:100",
                    contract_number="0001",
                    agency_cnpj=AGENCY_CNPJ,
                    payment_date=datetime.now(UTC),
                    amount=Decimal("250.00"),
                    phase="pago",
                    document_number="1/1",
                    commitment_number="100",
                    commitment_external_id="imperatriz:2026:100",
                    commitment_details_complete=True,
                    status="detalhe_empenho",
                    supplier_cnpj=SUPPLIER_CNPJ,
                    supplier_name="Empresa Teste Ltda",
                )
            ],
            availability=DataAvailability.AVAILABLE,
            source_urls=[raw.endpoint],
            raw_records=[raw],
        )

    async def aclose(self) -> None:
        self.closed = True


def _settings() -> Settings:
    return Settings(
        app_env="test", http_min_request_interval_seconds=0, allow_insecure_http_sources=True
    )


@pytest.mark.asyncio
async def test_municipal_run_tracks_progress_stages_and_callback(
    database: DatabaseContext,
) -> None:
    settings = _settings()
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=FakeMunicipalConnector(),
        payment_ingestion=PaymentIngestionService(settings, session_factory=database.sessions),
    )
    events: list[dict[str, object]] = []

    summary = await service.run("imperatriz", year=2026, progress_callback=events.append)
    await service.aclose()

    stages = [event["stage"] for event in events]
    assert "discovery" in stages
    assert "persisting" in stages
    assert "finalizing" in stages
    assert any(
        event["stage"] == "persisting" and event["processed"] == 0 and event["total"] == 1
        for event in events
    )
    assert any(event["stage"] == "finalizing" and event["processed"] == 1 for event in events)
    async with database.sessions() as session:
        run = await session.get(CrawlRun, summary.crawl_run_id)
    assert run is not None
    assert run.cursor is not None
    assert run.cursor["stage"] == "completed"
    assert run.cursor["processed"] == 1


@pytest.mark.asyncio
async def test_municipal_payment_is_persisted_with_provenance_and_contract_link(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            Contract(
                source="pncp",
                external_id="contract-1",
                contract_number="0001",
                year=2026,
                agency_name="Orgao de Exemplo",
                agency_cnpj=AGENCY_CNPJ,
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Teste Ltda",
                signed_at=now,
                published_at=now,
                valid_from=now,
                valid_until=now + timedelta(days=365),
                global_value=Decimal("1000.00"),
                fingerprint="a" * 64,
            )
        )

    connector = FakeMunicipalConnector()
    settings = _settings()
    ingestion = PaymentIngestionService(settings, session_factory=database.sessions)
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
        payment_ingestion=ingestion,
    )
    summary = await service.run("imperatriz", year=2026)
    await service.aclose()

    assert summary.availability is DataAvailability.AVAILABLE
    assert summary.payments_created == 1
    assert summary.payments_linked == 1
    assert connector.closed is True

    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
        source_record = await session.scalar(select(SourceRecord))
        run = await session.scalar(select(CrawlRun))
        contract = await session.scalar(select(Contract))
        latest = await latest_municipal_runs(session)

    assert payment is not None and payment.contract_id == contract.id
    assert payment.match_status == "linked"
    assert payment.source_record_id == source_record.id
    assert payment.source == "municipal:imperatriz"
    assert source_record.crawl_run_id == run.id
    assert run.status.value == "completed"
    assert latest["imperatriz"].id == run.id

    listing = await api_client.get("/api/payments", params={"supplier_cnpj": SUPPLIER_CNPJ})
    page = await api_client.get("/pagamentos")
    sources = await api_client.get("/api/payments/municipal/sources")
    assert listing.status_code == 200 and listing.json()["total"] == 1
    assert page.status_code == 200
    assert "Portais municipais do Maranhão" in page.text
    assert "Imperatriz" in page.text
    assert sources.status_code == 200
    assert {item["id"] for item in sources.json()} == {
        "sao_luis",
        "imperatriz",
        "timon",
        "caxias",
        "bacabal",
    }


@pytest.mark.asyncio
async def test_sao_luis_commitment_date_cannot_link_a_payment_to_a_valid_contract(
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agency_cnpj = "00000000000191"
    commitment_date = datetime(2026, 2, 1, tzinfo=UTC)
    contract = Contract(
        source="pncp",
        external_id="sao-luis-contract",
        contract_number="0001",
        year=2026,
        agency_cnpj=agency_cnpj,
        supplier_cnpj=SUPPLIER_CNPJ,
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
        valid_until=datetime(2026, 12, 31, tzinfo=UTC),
        global_value=Decimal("1000.00"),
        fingerprint="sao-luis-contract",
    )
    async with database.sessions() as session, session.begin():
        session.add(contract)

    assert contract.valid_from <= commitment_date <= contract.valid_until
    url = "https://saoluis.test/despesas.csv"
    content = (
        "Fornecedor;CNPJ;CNPJ Orgao;Numero Contrato;Valor Pago;Data Empenho;Empenho\n"
        f"Empresa Teste Ltda;{SUPPLIER_CNPJ};{agency_cnpj};0001;250,00;01/02/2026;123\n"
    )

    async def resolve(_host: str, _port: int) -> list[str]:
        return ["8.8.8.8"]

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", resolve)

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == url
        return httpx.Response(200, text=content, headers={"content-type": "text/csv"})

    settings = _settings().model_copy(update={"sao_luis_payments_url": url})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = build_municipal_connector("sao_luis", settings, http_client=client)
        service = MunicipalPaymentIngestionService(
            settings,
            session_factory=database.sessions,
            connector=connector,
        )
        try:
            summary = await service.run("sao_luis", year=2026)
        finally:
            await service.aclose()

    assert summary.payments_created == 1
    assert summary.payments_linked == 0
    assert summary.payments_unlinked == 1
    assert "sem data; periodo nao confirmado" in (summary.diagnostic or "")
    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
        assert payment is not None
        source_record = await session.get(SourceRecord, payment.source_record_id)

    assert payment.supplier_cnpj == SUPPLIER_CNPJ
    assert payment.agency_cnpj == agency_cnpj
    assert payment.contract_number == contract.contract_number
    assert payment.amount == Decimal("250.00")
    assert payment.process_number is None
    assert payment.payment_date is None
    assert payment.contract_id is None
    assert payment.match_status == "unlinked"
    assert payment.requires_manual_review
    assert source_record is not None
    assert source_record.source == "municipal:sao_luis"
    assert source_record.endpoint == url
    assert source_record.crawl_run_id == summary.crawl_run_id
    assert source_record.raw_payload["rows"][0]["Data Empenho"] == "01/02/2026"


class SequenceMunicipalConnector(FakeMunicipalConnector):
    def __init__(self, payments: list[RawPayment]) -> None:
        super().__init__()
        self.payments = payments

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]:
        del company_cnpj, include_details
        raw = RawSourceRecord.build(
            source="municipal:imperatriz",
            endpoint="https://transparencia.test/VersaoJson/Despesas/",
            request_parameters={"ano": year},
            raw_payload=[item.raw_payload for item in self.payments],
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=list(self.payments),
            availability=DataAvailability.AVAILABLE,
            source_urls=[raw.endpoint],
            raw_records=[raw],
        )


def _municipal_raw(*, status: str, external_id: str, amount: str = "500.00") -> RawPayment:
    return RawPayment(
        source="municipal:imperatriz",
        source_url="https://transparencia.test/despesas",
        raw_payload={"CODIGO": "100", "status": status},
        external_id=external_id,
        amount=Decimal(amount),
        phase="pago" if status == "detalhe_empenho" else "pago_agregado",
        commitment_number="100",
        commitment_external_id="imperatriz:2026:100",
        commitment_details_complete=status == "detalhe_empenho",
        status=status,
        supplier_cnpj=SUPPLIER_CNPJ,
        supplier_name="Empresa Teste Ltda",
    )


@pytest.mark.asyncio
async def test_detail_does_not_double_count_previous_aggregate(
    database: DatabaseContext,
) -> None:
    settings = _settings()
    aggregate = _municipal_raw(
        status="agregado_por_empenho",
        external_id="imperatriz:2026:100:agregado",
    )
    detail = _municipal_raw(
        status="detalhe_empenho",
        external_id="imperatriz:2026:100:1:1:detalhe",
    )
    connector = SequenceMunicipalConnector([aggregate])
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
    )
    await service.run("imperatriz", year=2026)
    await service.aclose()

    connector = SequenceMunicipalConnector([detail])
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
    )
    await service.run("imperatriz", year=2026)
    await service.aclose()

    connector = SequenceMunicipalConnector([aggregate])
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
    )
    await service.run("imperatriz", year=2026)
    await service.aclose()

    async with database.sessions() as session:
        payments = list(
            (await session.scalars(select(Payment).where(Payment.is_current.is_(True)))).all()
        )
    assert len(payments) == 1
    assert payments[0].status == "detalhe_empenho"


@pytest.mark.asyncio
async def test_municipal_failure_is_recorded_as_failed_run(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    settings = _settings()
    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=FakeMunicipalConnector(fail=True),
    )
    summary = await service.run("imperatriz", year=2026)
    await service.aclose()

    assert summary.availability is DataAvailability.TEMPORARY_ERROR
    assert "RuntimeError" in (summary.diagnostic or "")
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
    assert run is not None and run.status.value == "failed"
    assert "portal fora do ar" in (run.diagnostic or "")
    page = await api_client.get("/crawls")
    assert page.status_code == 200
    assert "Imperatriz (municipal)" in page.text
    assert "Falhou" in page.text
    assert "Tentar Imperatriz novamente" not in page.text
    retry = await api_client.post(
        f"/crawls/{run.id}/retry",
        data={"connector": "municipal:imperatriz"},
    )
    assert retry.status_code == 409


@pytest.mark.asyncio
async def test_web_municipal_collection_starts_in_background(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launched: list[object] = []

    def fake_launch(
        run_id: object,
        service: object,
        source_id: str,
        year: int,
        include_details: bool,
    ) -> None:
        launched.append((run_id, service, source_id, year, include_details))

    monkeypatch.setattr("app.web._launch_municipal_collection", fake_launch)

    response = await api_client.post(
        "/pagamentos/municipal",
        data={"source": "imperatriz", "year": 2026},
        follow_redirects=False,
    )
    duplicate = await api_client.post(
        "/pagamentos/municipal",
        data={"source": "imperatriz", "year": 2026},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/pagamentos?municipal_started=1#portais"
    assert len(launched) == 1
    assert duplicate.status_code == 303
    assert duplicate.headers["location"] == "/pagamentos?municipal_busy=1#portais"
    async with database.sessions() as session:
        run = await session.scalar(
            select(CrawlRun).where(CrawlRun.connector == "municipal:imperatriz")
        )
    assert run is not None
    assert run.status is CrawlRunStatus.RUNNING
    assert run.cursor is not None and run.cursor["stage"] == "queued"


@pytest.mark.asyncio
async def test_payments_page_shows_live_municipal_progress(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            CrawlRun(
                connector="municipal:imperatriz",
                status=CrawlRunStatus.RUNNING,
                started_at=now - timedelta(seconds=12),
                filters={"connector": "municipal:imperatriz", "year": 2026},
                cursor={
                    "stage": "persisting",
                    "message": "Gravando pagamentos: 10 de 100",
                    "processed": 10,
                    "total": 100,
                },
            )
        )

    page = await api_client.get("/pagamentos")
    partial = await api_client.get("/pagamentos/portais")

    assert page.status_code == 200
    assert partial.status_code == 200
    assert 'hx-get="/pagamentos/portais"' in page.text
    assert "Gravando pagamentos: 10 de 100" in page.text
    assert "10 de 100 pagamento(s) processado(s)" in page.text
    assert "Coletando..." in page.text
    assert 'hx-get="/pagamentos/portais"' in partial.text


@pytest.mark.asyncio
async def test_payments_page_stops_polling_after_municipal_completion(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            CrawlRun(
                connector="municipal:imperatriz",
                status=CrawlRunStatus.COMPLETED,
                started_at=now - timedelta(minutes=2),
                finished_at=now,
                records_found=1,
                filters={"connector": "municipal:imperatriz", "year": 2026},
                cursor={
                    "stage": "completed",
                    "message": "Coleta concluída",
                    "processed": 1,
                    "total": 1,
                },
            )
        )

    page = await api_client.get("/pagamentos")

    assert page.status_code == 200
    assert 'hx-get="/pagamentos/portais"' not in page.text
    assert "Coletar Imperatriz" in page.text


@pytest.mark.asyncio
async def test_municipal_run_api_returns_summary(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del database
    summary = MunicipalPaymentSummary(
        source_id="imperatriz",
        source_name="Imperatriz",
        availability=DataAvailability.EMPTY,
        crawl_run_id=UUID("00000000-0000-0000-0000-000000000001"),
        diagnostic="sem despesas pagas",
    )

    async def fake_run(
        self: MunicipalPaymentIngestionService,
        source_id: str,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool | None = None,
    ) -> MunicipalPaymentSummary:
        del self, year, company_cnpj, include_details
        if source_id == "unknown":
            raise ValueError("unknown municipal payment source: unknown")
        return summary

    async def fake_close(self: MunicipalPaymentIngestionService) -> None:
        del self

    monkeypatch.setattr(MunicipalPaymentIngestionService, "run", fake_run)
    monkeypatch.setattr(MunicipalPaymentIngestionService, "aclose", fake_close)

    response = await api_client.post(
        "/api/payments/municipal/run",
        json={"source": "imperatriz", "year": 2026},
    )
    missing = await api_client.post(
        "/api/payments/municipal/run",
        json={"source": "unknown", "year": 2026},
    )

    assert response.status_code == 200
    assert response.json()["source"] == "imperatriz"
    assert response.json()["availability"] == "empty"
    assert response.json()["crawl_run_id"] == str(summary.crawl_run_id)
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_municipal_constructor_failure_terminates_the_run(
    database: DatabaseContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.ingestion.municipal_payments as municipal_module

    def broken_build(*args, **kwargs):
        raise RuntimeError("adapter cannot be built")

    monkeypatch.setattr(municipal_module, "build_municipal_connector", broken_build)
    service = MunicipalPaymentIngestionService(
        _settings(),
        session_factory=database.sessions,
    )
    summary = await service.run("imperatriz", year=2026)
    await service.aclose()

    assert summary.availability is DataAvailability.TEMPORARY_ERROR
    assert "adapter cannot be built" in (summary.diagnostic or "")
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
    assert run is not None and run.status.value == "failed"


class MultiRecordMunicipalConnector(FakeMunicipalConnector):
    def __init__(self, payments: list[RawPayment], records: list[RawSourceRecord]) -> None:
        super().__init__()
        self.payments = payments
        self.records = records

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]:
        del year, company_cnpj, include_details
        return ConnectorResult(
            data=self.payments,
            availability=DataAvailability.AVAILABLE,
            source_urls=[record.endpoint for record in self.records],
            raw_records=self.records,
        )


@pytest.mark.asyncio
async def test_municipal_provenance_uses_each_response(
    database: DatabaseContext,
) -> None:
    first = RawSourceRecord.build(
        source="municipal:imperatriz",
        endpoint="https://transparencia.test/list",
        request_parameters={"pagina": 1},
        raw_payload=[{"CODIGO": "1"}],
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    second = RawSourceRecord.build(
        source="municipal:imperatriz",
        endpoint="https://transparencia.test/detail",
        request_parameters={"empenho": "2"},
        raw_payload=[{"EMPENHO": "2"}],
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    payments = [
        RawPayment(
            source="municipal:imperatriz",
            source_record_ref=first.record_key,
            external_id="aggregate-1",
            amount=Decimal("10.00"),
            phase="pago_agregado",
            commitment_number="1",
            status="agregado_por_empenho",
        ),
        RawPayment(
            source="municipal:imperatriz",
            source_record_ref=second.record_key,
            external_id="detail-2",
            amount=Decimal("20.00"),
            phase="pago",
            commitment_number="2",
            status="detalhe_empenho",
        ),
    ]
    service = MunicipalPaymentIngestionService(
        _settings(),
        session_factory=database.sessions,
        connector=MultiRecordMunicipalConnector(payments, [first, second]),
    )
    await service.run("imperatriz", year=2026)
    await service.aclose()

    async with database.sessions() as session:
        stored = list((await session.scalars(select(Payment).order_by(Payment.external_id))).all())
        sources = {
            record.id: record for record in (await session.scalars(select(SourceRecord))).all()
        }
    assert len(stored) == 2
    assert sources[stored[0].source_record_id].endpoint.endswith("/list")
    assert sources[stored[1].source_record_id].endpoint.endswith("/detail")


@pytest.mark.asyncio
async def test_http_municipal_source_requires_explicit_opt_in(
    database: DatabaseContext,
) -> None:
    settings = Settings(
        app_env="test",
        municipal_payment_sources=["imperatriz"],
        allow_insecure_http_sources=False,
        database_url="sqlite:///./data/test.sqlite3",
    )
    service = MunicipalPaymentIngestionService(settings, session_factory=database.sessions)

    summary = await service.run("imperatriz", year=2026)
    await service.aclose()

    assert summary.availability is DataAvailability.NOT_SUPPORTED
    assert "ALLOW_INSECURE_HTTP_SOURCES" in (summary.diagnostic or "")
    async with database.sessions() as session:
        run = await session.get(CrawlRun, summary.crawl_run_id)
    assert run is not None and run.status is CrawlRunStatus.FAILED
