"""Contract ingestion, payment read models and operator pages."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    ProcurementFilters,
    RawContract,
    RawContractItem,
    RawContractTerm,
    RawPayment,
    RawSourceRecord,
)
from app.models import (
    Company,
    Contract,
    ContractItem,
    ContractTerm,
    Payment,
    Procurement,
    SourceRecord,
)
from app.repositories.contracts import ContractRepository, contract_value
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.payment_import import PaymentImportService
from app.services.ingestion.payments import PaymentIngestionService

from .conftest import DatabaseContext
from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement, _result

SUPPLIER_CNPJ = "00000000000191"


@pytest.mark.parametrize(
    "global_value,accumulated,initial,expected",
    [
        ("0", "50", "100", "0"),
        (None, "0", "100", "0"),
        (None, None, "0", "0"),
        (None, None, "100", "100"),
        ("25", "50", "100", "25"),
        (None, None, None, None),
    ],
)
async def test_contract_value_fallback_preserves_official_zero_in_api_and_pages(
    api_client, database, global_value, accumulated, initial, expected
):
    from bs4 import BeautifulSoup

    def amount(value):
        return Decimal(value) if value is not None else None

    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp",
            external_id="zero-contract",
            contract_number="ZERO-1",
            year=2026,
            supplier_name="Empresa contratada",
            signed_at=datetime.now(UTC) - timedelta(days=120),
            global_value=amount(global_value),
            accumulated_value=amount(accumulated),
            initial_value=amount(initial),
        )
        session.add(contract)
        await session.flush()
        identifier = contract.id
        assert contract_value(contract) == amount(expected)
    detail = await api_client.get(f"/api/contracts/{identifier}")
    assert detail.status_code == 200
    summary = detail.json()["payment_summary"]
    assert summary["contracted_value"] == (float(expected) if expected is not None else None)
    assert summary["balance"] == (float(expected) if expected is not None else None)
    if expected == "0":
        assert summary["status"] != "sem_pagamento_localizado"
    page = await api_client.get(f"/contratos/{identifier}")
    listing = await api_client.get("/contratos")
    assert page.status_code == listing.status_code == 200
    if expected == "0":
        html = BeautifulSoup(page.text, "html.parser")
        assert (
            html.find("dt", string="Valor global").find_next_sibling("dd").get_text(strip=True)
            == "R$ 0,00"
        )
        row = (
            BeautifulSoup(listing.text, "html.parser")
            .find("a", href=f"/contratos/{identifier}")
            .find_parent("tr")
        )
        assert "R$ 0,00" in row.get_text()
    async with database.sessions() as session:
        stored = await session.get(Contract, identifier)
        assert stored.global_value == amount(global_value)  # reads never repair stored values


def _raw_contract() -> RawContract:
    now = datetime.now(UTC)
    return RawContract(
        source="pncp",
        source_url="https://pncp.official.test/contracts/1",
        raw_payload={"numeroControlePNCP": "contract-control-1"},
        external_id="contract-control-1",
        pncp_control_number="contract-control-1",
        procurement_control_number=CONTROL_NUMBER,
        contract_number="0001",
        year=2026,
        process_number="90001/2026",
        agency_name="Orgao de Exemplo",
        agency_cnpj="12345678000199",
        uasg="980921",
        object_description="Contrato de cadeiras",
        signed_at=now,
        published_at=now,
        valid_from=now,
        valid_until=now + timedelta(days=365),
        initial_value=Decimal("1000.00"),
        global_value=Decimal("1000.00"),
        accumulated_value=Decimal("1000.00"),
        supplier_cnpj=SUPPLIER_CNPJ,
        supplier_name="Empresa Contratada Ltda.",
    )


class ContractConnector(StaticConnector):
    def __init__(
        self,
        contract: RawContract,
        *,
        items: list[RawContractItem] | None = None,
        terms: list[RawContractTerm] | None = None,
    ) -> None:
        super().__init__("pncp", _procurement("pncp", CONTROL_NUMBER))
        self.contract = contract
        self.items = items or []
        self.terms = terms or []

    async def discover_contracts(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawContract]]:
        del filters
        return _result(
            [self.contract], DataAvailability.AVAILABLE, source=self.name, operation="discovery"
        )

    async def fetch_contract_items(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractItem]]:
        assert contract.external_id == self.contract.external_id
        return _result(
            self.items,
            DataAvailability.AVAILABLE,
            source=self.name,
            operation="contract_items",
        )

    async def fetch_contract_terms(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractTerm]]:
        assert contract.external_id == self.contract.external_id
        return _result(
            self.terms,
            DataAvailability.AVAILABLE,
            source=self.name,
            operation="contract_terms",
        )


@pytest.mark.asyncio
async def test_contract_ingestion_links_procurement_and_company(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    async with database.sessions() as session, session.begin():
        session.add(
            Procurement(
                source="pncp",
                external_id=CONTROL_NUMBER,
                pncp_control_number=CONTROL_NUMBER,
                modality="Pregao - Eletronico",
                procurement_type="licitacao",
                current_phase="homologation",
                fingerprint="c" * 64,
            )
        )

    item = RawContractItem(
        source="pncp",
        external_id="contract-control-1:item:1",
        contract_external_id="contract-control-1",
        item_number="1",
        description="Cadeira",
        quantity=Decimal("10"),
        unit_value=Decimal("100"),
        total_value=Decimal("1000"),
        supplier_cnpj=SUPPLIER_CNPJ,
        supplier_name="Empresa Contratada Ltda.",
    )
    term = RawContractTerm(
        source="pncp",
        external_id="contract-control-1:term:1",
        contract_external_id="contract-control-1",
        term_number="1",
        term_type="Aditivo de valor",
        value_delta=Decimal("100"),
        object_description="Acrescimo",
    )
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"pncp": ContractConnector(_raw_contract(), items=[item], terms=[term])},
    )
    summary = await pipeline.run(
        PipelineRequest(connector="pncp", mode="contracts", days=30, process_documents=False)
    )

    assert summary.records_created == 1
    async with database.sessions() as session:
        contract = await session.scalar(select(Contract))
        procurement = await session.scalar(select(Procurement))
        contract_item = await session.scalar(select(ContractItem))
        contract_term = await session.scalar(select(ContractTerm))
    assert contract is not None and procurement is not None
    assert contract.procurement_id == procurement.id
    assert contract.company_id is not None
    assert float(contract.global_value) == 1000.0
    assert contract_item is not None and contract_item.description == "Cadeira"
    assert contract_term is not None and float(contract_term.value_delta) == 100.0

    listing = await api_client.get("/api/contracts")
    detail = await api_client.get(f"/api/contracts/{contract.id}")
    page = await api_client.get("/contratos")
    detail_page = await api_client.get(f"/contratos/{contract.id}")
    phase_filter = await api_client.get("/procurements", params={"phase": "homologation"})

    assert listing.status_code == 200 and listing.json()["total"] == 1
    assert detail.status_code == 200
    assert detail.json()["payment_summary"]["status"] == "nao_avaliado"
    assert detail.json()["items"][0]["item_number"] == "1"
    assert detail.json()["terms"][0]["term_type"] == "Aditivo de valor"
    assert page.status_code == 200 and "Contrato de cadeiras" in page.text
    assert detail_page.status_code == 200 and "Pagamentos localizados" in detail_page.text
    assert "Itens do contrato" in detail_page.text
    assert "Termos e aditivos" in detail_page.text
    assert phase_filter.status_code == 200
    assert "<th>Fase</th>" in phase_filter.text
    assert "Homologa" in phase_filter.text


@pytest.mark.asyncio
async def test_payment_api_and_page_show_unlinked_payment(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        payment = Payment(
            source="transparencia",
            external_id="payment-1",
            supplier_cnpj=SUPPLIER_CNPJ,
            supplier_name="Empresa Contratada Ltda.",
            agency_name="Orgao de Exemplo",
            payment_date=now,
            amount=Decimal("250.00"),
            phase="Pagamento",
            document_number="OB-1",
            match_status="unlinked",
            requires_manual_review=True,
            fingerprint="d" * 64,
        )
        session.add(payment)
        await session.flush()
        payment_id = payment.id

    listing = await api_client.get("/api/payments")
    page = await api_client.get("/pagamentos")
    sources = await api_client.get("/api/payments/sources")
    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    assert sources.status_code == 200
    assert {item["id"] for item in sources.json()} >= {"transparencia", "manual"}
    assert listing.json()["items"][0]["match_status"] == "unlinked"
    assert str(payment_id) in listing.json()["items"][0]["id"]
    assert page.status_code == 200 and "Nao conciliado" in page.text


class FakePaymentConnector:
    name = "transparencia"

    def __init__(self, payments: list[RawPayment]) -> None:
        self.payments = payments

    async def discover_payments(self, *, company_cnpj: str, year: int, phase: str | None = None):
        del company_cnpj, year, phase
        raw = RawSourceRecord.build(
            source=self.name,
            endpoint="https://portaldatransparencia.test/payments",
            request_parameters={"ano": 2026},
            raw_payload={"payments": len(self.payments)},
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=self.payments,
            availability=DataAvailability.AVAILABLE,
            source_urls=[raw.endpoint],
            raw_records=[raw],
        )

    async def aclose(self) -> None:
        return None


class MultiRecordPaymentConnector:
    name = "transparencia"

    def __init__(self, payments: list[RawPayment], records: list[RawSourceRecord]) -> None:
        self.payments = payments
        self.records = records

    async def discover_payments(self, *, company_cnpj: str, year: int, phase: str | None = None):
        del company_cnpj, year, phase
        return ConnectorResult(
            data=self.payments,
            availability=DataAvailability.AVAILABLE,
            source_urls=[record.endpoint for record in self.records],
            raw_records=self.records,
        )

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_payment_provenance_uses_the_response_that_contains_the_row(
    database: DatabaseContext,
) -> None:
    first = RawSourceRecord.build(
        source="transparencia",
        endpoint="https://portaldatransparencia.test/payments?pagina=1",
        request_parameters={"pagina": 1},
        raw_payload=[{"documento": "OB-1"}],
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    second = RawSourceRecord.build(
        source="transparencia",
        endpoint="https://portaldatransparencia.test/payments?pagina=2",
        request_parameters={"pagina": 2},
        raw_payload=[{"documento": "OB-2"}],
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    payments = [
        RawPayment(
            source="transparencia",
            source_record_ref=first.record_key,
            external_id="OB-1",
            amount=Decimal("10.00"),
            phase="Pagamento",
        ),
        RawPayment(
            source="transparencia",
            source_record_ref=second.record_key,
            external_id="OB-2",
            amount=Decimal("20.00"),
            phase="Pagamento",
        ),
    ]
    service = PaymentIngestionService(
        session_factory=database.sessions,
        connector=MultiRecordPaymentConnector(payments, [first, second]),
    )
    await service.run(company_cnpj=SUPPLIER_CNPJ, year=2026)
    await service.aclose()

    async with database.sessions() as session:
        stored = list((await session.scalars(select(Payment).order_by(Payment.external_id))).all())
        sources = {
            record.id: record for record in (await session.scalars(select(SourceRecord))).all()
        }
    assert len(stored) == 2
    assert sources[stored[0].source_record_id].endpoint.endswith("pagina=1")
    assert sources[stored[1].source_record_id].endpoint.endswith("pagina=2")


@pytest.mark.asyncio
async def test_payment_service_links_exact_contract_and_keeps_provenance(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp",
            external_id="contract-control-1",
            pncp_control_number="contract-control-1",
            contract_number="0001",
            year=2026,
            agency_cnpj="12345678000199",
            supplier_cnpj=SUPPLIER_CNPJ,
            supplier_name="Empresa Contratada Ltda.",
            global_value=Decimal("1000.00"),
            fingerprint="e" * 64,
        )
        session.add(contract)
        await session.flush()
        contract_id = contract.id

    payment = RawPayment(
        source="transparencia",
        source_url="https://portaldatransparencia.test/payments",
        raw_payload={"documento": "OB-1"},
        external_id="OB-1:Pagamento:00000000000191:250.00",
        contract_number="0001",
        agency_cnpj="12345678000199",
        payment_date=now,
        amount=Decimal("250.00"),
        phase="Pagamento",
        document_number="OB-1",
        supplier_cnpj=SUPPLIER_CNPJ,
        supplier_name="Empresa Contratada Ltda.",
    )
    service = PaymentIngestionService(
        session_factory=database.sessions,
        connector=FakePaymentConnector([payment]),
    )
    summary = await service.run(company_cnpj=SUPPLIER_CNPJ, year=2026)
    await service.aclose()

    assert summary.payments_created == 1
    assert summary.payments_linked == 1
    async with database.sessions() as session:
        stored = await session.scalar(select(Payment))
        assert stored is not None
        assert stored.contract_id == contract_id
        assert stored.match_status == "linked"
        assert stored.source_record_id is not None
        source = await session.get(SourceRecord, stored.source_record_id)
        assert source is not None
        assert len(stored.fingerprint or "") <= 64
        assert len(source.fingerprint or "") <= 64

    detail = await api_client.get(f"/api/contracts/{contract_id}")
    assert detail.status_code == 200
    assert detail.json()["payment_summary"]["paid_value"] == 250.0
    assert detail.json()["payment_summary"]["balance"] == 750.0
    assert detail.json()["payment_summary"]["status"] == "parcial"


@pytest.mark.asyncio
async def test_contract_api_filters_and_missing_id(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            Contract(
                source="pncp",
                external_id="contract-control-2",
                contract_number="0002",
                year=2026,
                agency_name="Orgao de Exemplo",
                agency_cnpj="12345678000199",
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Contratada Ltda.",
                object_description="Cadeiras",
                global_value=Decimal("100.00"),
                valid_from=now - timedelta(days=1),
                valid_until=now + timedelta(days=1),
                fingerprint="f" * 64,
            )
        )

    search = await api_client.get("/api/contracts", params={"search": "Cadeiras"})
    agency = await api_client.get("/api/contracts", params={"agency": "Exemplo"})
    supplier = await api_client.get("/api/contracts", params={"supplier_cnpj": SUPPLIER_CNPJ})
    valid = await api_client.get("/api/contracts", params={"valid_on": now.date().isoformat()})
    missing = await api_client.get("/api/contracts/00000000-0000-0000-0000-000000000000")

    assert search.status_code == 200 and search.json()["total"] == 1
    assert agency.status_code == 200 and agency.json()["total"] == 1
    assert supplier.status_code == 200 and supplier.json()["total"] == 1
    assert valid.status_code == 200 and valid.json()["total"] == 1
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_manual_payment_import_validates_and_links_rows(
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        session.add(
            Contract(
                source="pncp",
                external_id="import-contract",
                contract_number="0001",
                year=2026,
                agency_cnpj="11222333000181",
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Contratada Ltda.",
                global_value=Decimal("1000.00"),
                fingerprint="g" * 64,
            )
        )
    content = (
        "cnpj;valor;data;orgao_cnpj;contrato;fase;documento\n"
        f"{SUPPLIER_CNPJ};250,00;2026-09-01;11222333000181;0001;Pagamento;OB-1\n"
        "00000000000000;100,00;2026-09-01;11222333000181;0001;Pagamento;OB-2\n"
    ).encode()
    service = PaymentImportService(session_factory=database.sessions)
    summary = await service.import_bytes(
        content, filename="pagamentos.csv", source_name="Portal Municipal"
    )
    await service.aclose()

    assert summary.created == 1
    assert summary.linked == 1
    assert len(summary.errors) == 1
    assert summary.errors[0].row == 3
    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
        source = await session.scalar(
            select(Payment.source_record_id).where(Payment.id.is_not(None))
        )
    assert payment is not None
    assert payment.source == "manual"
    assert payment.match_status == "linked"
    assert source is not None

    repeated = PaymentImportService(session_factory=database.sessions)
    second = await repeated.import_bytes(
        content, filename="pagamentos.csv", source_name="Portal Municipal"
    )
    await repeated.aclose()
    assert second.created == 0
    assert second.updated == 1


@pytest.mark.asyncio
async def test_contract_payment_summary_ignores_commitment_and_liquidation(
    database: DatabaseContext,
    api_client: httpx.AsyncClient,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp",
            external_id="phase-contract",
            contract_number="0002",
            year=2026,
            agency_cnpj="11222333000181",
            supplier_cnpj=SUPPLIER_CNPJ,
            global_value=Decimal("1000.00"),
            signed_at=now,
            fingerprint="i" * 64,
        )
        session.add(contract)
        await session.flush()
        contract_id = contract.id
        for index, (phase, amount) in enumerate(
            (("Empenho", "1000.00"), ("Liquidacao", "500.00"), ("Pagamento", "250.00")),
            start=1,
        ):
            session.add(
                Payment(
                    source="transparencia",
                    external_id=f"phase-{index}",
                    contract_id=contract_id,
                    supplier_cnpj=SUPPLIER_CNPJ,
                    payment_date=now,
                    amount=Decimal(amount),
                    phase=phase,
                    match_status="linked",
                    fingerprint=chr(96 + index) * 64,
                )
            )

    detail = await api_client.get(f"/api/contracts/{contract_id}")

    assert detail.status_code == 200
    summary = detail.json()["payment_summary"]
    assert summary["paid_value"] == 250.0
    assert summary["balance"] == 750.0
    assert summary["status"] == "parcial"


@pytest.mark.asyncio
async def test_manual_import_accepts_supplier_without_cnpj_for_review(
    database: DatabaseContext,
) -> None:
    content = b"fornecedor;valor;data;documento\nFornecedor Sem CNPJ;100,00;2026-09-01;OB-9\n"
    service = PaymentImportService(session_factory=database.sessions)
    summary = await service.import_bytes(
        content, filename="pagamentos.csv", source_name="Portal Sem CNPJ"
    )
    await service.aclose()

    assert summary.created == 1
    assert summary.unlinked == 1
    assert summary.errors == []
    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
    assert payment is not None
    assert payment.supplier_cnpj is None
    assert payment.supplier_name == "Fornecedor Sem CNPJ"
    assert payment.requires_manual_review is True


@pytest.mark.asyncio
async def test_contract_matching_requires_a_proven_year(
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        for year, start, end, fingerprint in (
            (2024, datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 12, 31, tzinfo=UTC), "j" * 64),
            (2026, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 12, 31, tzinfo=UTC), "k" * 64),
        ):
            session.add(
                Contract(
                    source="pncp",
                    external_id=f"contract-{year}",
                    contract_number="0001",
                    year=year,
                    agency_cnpj="11222333000181",
                    supplier_cnpj=SUPPLIER_CNPJ,
                    valid_from=start,
                    valid_until=end,
                    fingerprint=fingerprint,
                )
            )
    service = PaymentIngestionService(session_factory=database.sessions)
    payments = [
        RawPayment(
            source="manual",
            external_id="dated-2026",
            contract_number="0001",
            agency_cnpj="11222333000181",
            payment_date=datetime(2026, 3, 1, tzinfo=UTC),
            amount=Decimal("100.00"),
            phase="Pagamento",
            supplier_cnpj=SUPPLIER_CNPJ,
        ),
        RawPayment(
            source="manual",
            external_id="undated",
            contract_number="0001",
            agency_cnpj="11222333000181",
            payment_date=None,
            amount=Decimal("100.00"),
            phase="Pagamento",
            supplier_cnpj=SUPPLIER_CNPJ,
        ),
    ]
    await service.persist_raw_payments(payments)
    await service.aclose()

    async with database.sessions() as session:
        dated = await session.scalar(select(Payment).where(Payment.external_id == "dated-2026"))
        undated = await session.scalar(select(Payment).where(Payment.external_id == "undated"))
        contract_2026 = await session.scalar(select(Contract).where(Contract.year == 2026))
    assert dated is not None and dated.contract_id == contract_2026.id
    assert undated is not None and undated.contract_id is None
    assert undated.requires_manual_review is True
    assert "payment date" in (undated.match_reason or "")


@pytest.mark.asyncio
async def test_payment_reingestion_clears_stale_contract_link(
    database: DatabaseContext,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp",
            external_id="stale-contract",
            contract_number="0001",
            year=2026,
            agency_cnpj="11222333000181",
            supplier_cnpj=SUPPLIER_CNPJ,
            fingerprint="h" * 64,
        )
        session.add(contract)
        await session.flush()
        contract_id = contract.id
    payment = RawPayment(
        source="manual",
        external_id="stale-payment",
        contract_number="0001",
        agency_cnpj="11222333000181",
        payment_date=now,
        amount=Decimal("100.00"),
        supplier_cnpj=SUPPLIER_CNPJ,
    )
    service = PaymentIngestionService(session_factory=database.sessions)
    await service.persist_raw_payments([payment])
    changed = payment.model_copy(update={"contract_number": None, "agency_cnpj": None})
    await service.persist_raw_payments([changed])
    await service.aclose()

    async with database.sessions() as session:
        stored = await session.scalar(select(Payment))
    assert stored is not None
    assert stored.contract_id is None
    assert stored.match_status == "unlinked"
    assert stored.contract_id != contract_id


@pytest.mark.asyncio
async def test_payment_ingestion_updates_company_normalized_name(
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        session.add(
            Company(
                cnpj=SUPPLIER_CNPJ,
                legal_name=None,
                normalized_name=SUPPLIER_CNPJ,
                fingerprint="z" * 64,
            )
        )
    service = PaymentIngestionService(session_factory=database.sessions)
    await service.persist_raw_payments(
        [
            RawPayment(
                source="manual",
                external_id="company-name",
                amount=Decimal("10.00"),
                phase="Pagamento",
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Contratada Ltda.",
            )
        ]
    )
    await service.aclose()

    async with database.sessions() as session:
        company = await session.scalar(select(Company).where(Company.cnpj == SUPPLIER_CNPJ))
    assert company is not None
    assert company.normalized_name == "EMPRESA CONTRATADA"


@pytest.mark.asyncio
async def test_manual_payment_without_phase_is_a_payment(
    database: DatabaseContext,
) -> None:
    async with database.sessions() as session, session.begin():
        contract = Contract(
            source="pncp",
            external_id="no-phase-contract",
            contract_number="0003",
            year=2026,
            agency_cnpj="11222333000181",
            supplier_cnpj=SUPPLIER_CNPJ,
            global_value=Decimal("1000.00"),
            valid_from=datetime(2026, 1, 1, tzinfo=UTC),
            valid_until=datetime(2026, 12, 31, tzinfo=UTC),
            fingerprint="l" * 64,
        )
        session.add(contract)
        await session.flush()
        contract_id = contract.id
    content = (
        "cnpj;valor;data;orgao_cnpj;contrato\n"
        f"{SUPPLIER_CNPJ};250,00;2026-04-01;11222333000181;0003\n"
    ).encode()
    service = PaymentImportService(session_factory=database.sessions)
    summary_import = await service.import_bytes(
        content, filename="pagamentos.csv", source_name="Portal sem fase"
    )
    await service.aclose()
    assert summary_import.created == 1
    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
        summary = await ContractRepository(session).payment_summary(
            await session.get(Contract, contract_id)
        )
    assert payment is not None
    assert summary["paid_value"] == 250.0


@pytest.mark.asyncio
async def test_payment_without_cnpj_is_not_auto_linked(database: DatabaseContext) -> None:
    async with database.sessions() as session, session.begin():
        session.add(
            Contract(
                source="pncp",
                external_id="name-only-contract",
                contract_number="0004",
                year=2026,
                agency_cnpj="11222333000181",
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Contratada Ltda.",
                valid_from=datetime(2026, 1, 1, tzinfo=UTC),
                valid_until=datetime(2026, 12, 31, tzinfo=UTC),
                fingerprint="m" * 64,
            )
        )
    service = PaymentIngestionService(session_factory=database.sessions)
    await service.persist_raw_payments(
        [
            RawPayment(
                source="manual",
                external_id="name-only",
                contract_number="0004",
                agency_cnpj="11222333000181",
                payment_date=datetime(2026, 4, 1, tzinfo=UTC),
                amount=Decimal("100.00"),
                phase="Pagamento",
                supplier_name="Empresa Contratada Ltda.",
            )
        ]
    )
    await service.aclose()
    async with database.sessions() as session:
        payment = await session.scalar(select(Payment))
    assert payment is not None
    assert payment.contract_id is None
    assert payment.requires_manual_review is True
    assert "CNPJ" in (payment.match_reason or "")
