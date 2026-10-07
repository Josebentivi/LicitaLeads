"""Price registry (ARP/ata) ingestion, API and page tests."""

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
    RawPriceRegistry,
    RawPriceRegistryItem,
)
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
)
from app.services.ingestion import IngestionPipeline, PipelineRequest

from .conftest import DatabaseContext
from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement, _result

REGISTRY_CONTROL = "12345678000199-1-000001/2026"
SUPPLIER_CNPJ = "00000000000191"
SECOND_SUPPLIER_CNPJ = "11222333000181"


def _registry() -> RawPriceRegistry:
    now = datetime.now(UTC)
    return RawPriceRegistry(
        source="compras_gov",
        source_url="https://compras.official.test/arp/1",
        raw_payload={"numeroControlePncpAta": REGISTRY_CONTROL},
        external_id=REGISTRY_CONTROL,
        pncp_control_number=REGISTRY_CONTROL,
        linked_pncp_control_number=CONTROL_NUMBER,
        registry_number="0001",
        year=2026,
        agency_name="Órgão de Exemplo",
        agency_cnpj="12345678000199",
        uasg="980921",
        object_description="Registro de preços de cadeiras",
        status="Vigente",
        signed_at=now,
        valid_from=now - timedelta(days=30),
        valid_until=now + timedelta(days=30),
        total_value=Decimal("1000.00"),
        allows_adhesion=True,
    )


def _registry_item() -> RawPriceRegistryItem:
    return RawPriceRegistryItem(
        source="compras_gov",
        source_url="https://compras.official.test/arp/1/items",
        raw_payload={"numeroItem": "1"},
        external_id=f"{REGISTRY_CONTROL}:item:1:{SUPPLIER_CNPJ}",
        price_registry_external_id=REGISTRY_CONTROL,
        item_number="1",
        description="Cadeira escolar",
        quantity=Decimal("10"),
        unit_value=Decimal("50.00"),
        total_value=Decimal("500.00"),
        max_adhesion_quantity=Decimal("20"),
        supplier_cnpj=SUPPLIER_CNPJ,
        supplier_name="Empresa Registrada Ltda.",
    )


class PriceRegistryConnector(StaticConnector):
    """Procurement double extended with the price registry contract."""

    def __init__(self, registry: RawPriceRegistry, items: list[RawPriceRegistryItem]) -> None:
        super().__init__("compras_gov", _procurement("compras_gov", "98092105900012026"))
        self.registry = registry
        self.items = items

    async def discover_price_registries(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawPriceRegistry]]:
        del filters
        return _result(
            [self.registry],
            DataAvailability.AVAILABLE,
            source=self.name,
            operation="discovery",
        )

    async def fetch_price_registry_items(
        self, external_id: str
    ) -> ConnectorResult[list[RawPriceRegistryItem]]:
        assert external_id == self.registry.external_id
        return _result(
            self.items,
            DataAvailability.AVAILABLE,
            source=self.name,
            operation="price_registry_items",
        )


@pytest.mark.asyncio
async def test_price_registry_pipeline_is_idempotent_and_links_procurement(
    database: DatabaseContext,
) -> None:
    """ARPs persist with items, supplier companies and the linked procurement."""

    async with database.sessions() as session, session.begin():
        session.add(
            Procurement(
                source="pncp",
                external_id=CONTROL_NUMBER,
                pncp_control_number=CONTROL_NUMBER,
                modality="Pregão - Eletrônico",
                procurement_type="licitacao",
                fingerprint="p" * 64,
            )
        )

    connector = PriceRegistryConnector(_registry(), [_registry_item()])
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"compras_gov": connector},
    )
    request = PipelineRequest(
        connector="compras_gov", mode="price_registries", days=365, process_documents=False
    )

    first = await pipeline.run(request)
    second = await pipeline.run(request)

    assert first.records_created == 1
    assert second.records_created == 0
    assert second.records_updated == 1
    async with database.sessions() as session:
        registry = await session.scalar(select(PriceRegistry))
        item = await session.scalar(select(PriceRegistryItem))
        company = await session.scalar(select(Company))
        procurement = await session.scalar(select(Procurement))

    assert registry is not None and procurement is not None
    assert registry.procurement_id == procurement.id
    assert registry.source_record_id is not None
    assert float(registry.total_value) == 1000.0
    assert registry.allows_adhesion is True
    assert item is not None and company is not None
    assert item.company_id == company.id
    assert item.supplier_cnpj == SUPPLIER_CNPJ
    assert float(item.quantity) == 10.0


@pytest.mark.asyncio
async def test_price_registry_keeps_multiple_suppliers_per_item(
    database: DatabaseContext,
) -> None:
    """Two suppliers registered on the same item stay as two auditable rows."""

    first = _registry_item()
    second = _registry_item().model_copy(
        update={
            "external_id": f"{REGISTRY_CONTROL}:item:1:{SECOND_SUPPLIER_CNPJ}",
            "supplier_cnpj": SECOND_SUPPLIER_CNPJ,
            "supplier_name": "Segunda Empresa Registrada Ltda.",
        }
    )
    connector = PriceRegistryConnector(_registry(), [first, second])
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"compras_gov": connector},
    )

    summary = await pipeline.run(
        PipelineRequest(
            connector="compras_gov", mode="price_registries", days=365, process_documents=False
        )
    )

    assert summary.records_created == 1
    async with database.sessions() as session:
        items = list(
            (
                await session.scalars(
                    select(PriceRegistryItem).order_by(PriceRegistryItem.supplier_cnpj)
                )
            ).all()
        )
        companies = list((await session.scalars(select(Company))).all())

    assert len(items) == 2
    assert {item.supplier_cnpj for item in items} == {SUPPLIER_CNPJ, SECOND_SUPPLIER_CNPJ}
    assert {company.cnpj for company in companies} == {SUPPLIER_CNPJ, SECOND_SUPPLIER_CNPJ}
    assert all(item.company_id is not None for item in items)


@pytest.mark.asyncio
async def test_price_registry_item_zero_values_are_applied(
    database: DatabaseContext,
) -> None:
    """A newer payload with zeroed quantities/values is not treated as missing."""

    item = _registry_item()
    connector = PriceRegistryConnector(_registry(), [item])
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"compras_gov": connector},
    )
    request = PipelineRequest(
        connector="compras_gov", mode="price_registries", days=365, process_documents=False
    )
    await pipeline.run(request)

    connector.items = [
        item.model_copy(
            update={
                "quantity": Decimal("0"),
                "unit_value": Decimal("0"),
                "total_value": Decimal("0"),
                "max_adhesion_quantity": Decimal("0"),
            }
        )
    ]
    await pipeline.run(request)

    async with database.sessions() as session:
        stored = await session.scalar(select(PriceRegistryItem))

    assert stored is not None
    assert float(stored.quantity) == 0.0
    assert float(stored.unit_value) == 0.0
    assert float(stored.total_value) == 0.0
    assert float(stored.max_adhesion_quantity) == 0.0


@pytest.mark.asyncio
async def test_price_registry_endpoints_and_pages(
    api_client: httpx.AsyncClient,
    database: DatabaseContext,
) -> None:
    """The ARP list/detail filters by vigência, agency and supplier."""

    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        registry = PriceRegistry(
            source="compras_gov",
            external_id=REGISTRY_CONTROL,
            pncp_control_number=REGISTRY_CONTROL,
            registry_number="0001",
            year=2026,
            agency_name="Órgão de Exemplo",
            agency_cnpj="12345678000199",
            object_description="Registro de preços de cadeiras",
            status="Vigente",
            valid_from=now - timedelta(days=10),
            valid_until=now + timedelta(days=10),
            total_value=Decimal("1000.00"),
            fingerprint="q" * 64,
        )
        session.add(registry)
        await session.flush()
        session.add(
            PriceRegistryItem(
                price_registry_id=registry.id,
                item_number="1",
                description="Cadeira escolar",
                quantity=Decimal("10"),
                unit_value=Decimal("50.00"),
                total_value=Decimal("500.00"),
                supplier_cnpj=SUPPLIER_CNPJ,
                supplier_name="Empresa Registrada Ltda.",
                fingerprint="r" * 64,
            )
        )
        session.add(
            CrawlRun(
                connector="compras_gov",
                status=CrawlRunStatus.COMPLETED,
                filters={"mode": "price_registries"},
            )
        )
        registry_id = registry.id

    listing = await api_client.get("/api/price-registries")
    valid = await api_client.get(
        "/api/price-registries", params={"valid_on": now.date().isoformat()}
    )
    by_supplier = await api_client.get(
        "/api/price-registries", params={"supplier_cnpj": SUPPLIER_CNPJ}
    )
    search = await api_client.get("/api/price-registries", params={"search": "cadeiras"})
    agency = await api_client.get("/api/price-registries", params={"agency": "Exemplo"})
    by_status = await api_client.get("/api/price-registries", params={"status": "Vigente"})
    no_match = await api_client.get(
        "/api/price-registries", params={"supplier_cnpj": "00000000000000"}
    )
    detail = await api_client.get(f"/api/price-registries/{registry_id}")
    items = await api_client.get(f"/api/price-registries/{registry_id}/items")
    missing = await api_client.get("/api/price-registries/00000000-0000-0000-0000-000000000000")
    missing_items = await api_client.get(
        "/api/price-registries/00000000-0000-0000-0000-000000000000/items"
    )
    page = await api_client.get("/atas", params={"search": "cadeiras", "agency": "Exemplo"})
    detail_page = await api_client.get(f"/atas/{registry_id}")
    crawls_page = await api_client.get("/crawls")

    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    assert valid.status_code == 200 and valid.json()["total"] == 1
    assert by_supplier.status_code == 200 and by_supplier.json()["total"] == 1
    assert search.status_code == 200 and search.json()["total"] == 1
    assert agency.status_code == 200 and agency.json()["total"] == 1
    assert by_status.status_code == 200 and by_status.json()["total"] == 1
    assert no_match.status_code == 200 and no_match.json()["total"] == 0
    assert detail.status_code == 200
    assert detail.json()["items"][0]["item"]["supplier_cnpj"] == SUPPLIER_CNPJ
    assert items.status_code == 200 and len(items.json()["items"]) == 1
    assert missing.status_code == 404
    assert missing_items.status_code == 404
    assert page.status_code == 200 and "Atas de Registro de Preços" in page.text
    assert detail_page.status_code == 200 and "Cadeira escolar" in detail_page.text
    assert crawls_page.status_code == 200 and "Atas (ARP)" in crawls_page.text
