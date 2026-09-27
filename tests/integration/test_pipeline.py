"""Database-backed ingestion tests with deterministic source doubles."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    ProcurementFilters,
    RawDocument,
    RawEvent,
    RawParticipant,
    RawProcurement,
    RawProcurementDetail,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    Participant,
    Procurement,
    ProcurementSource,
)
from app.services.ingestion import IngestionPipeline, PipelineRequest

from .conftest import DatabaseContext

COLLECTED_AT = datetime(2026, 9, 16, 12, tzinfo=UTC)
CONTROL_NUMBER = "00000000000191-1-000001/2026"


def _result(data, availability: DataAvailability, *, source: str, operation: str):
    raw_records = []
    if operation == "discovery":
        raw_records = [
            RawSourceRecord.build(
                source=source,
                endpoint=f"https://{source}.official.test/{operation}",
                request_parameters={"uf": "MA"},
                raw_payload={"records": [CONTROL_NUMBER]},
                http_status=200,
                collected_at=COLLECTED_AT,
            )
        ]
    return ConnectorResult(
        data=data,
        availability=availability,
        source_urls=[f"https://{source}.official.test/{operation}"],
        raw_records=raw_records,
        collected_at=COLLECTED_AT,
    )


def _procurement(source: str, external_id: str) -> RawProcurement:
    return RawProcurement(
        source=source,
        source_url=f"https://{source}.official.test/process/{external_id}",
        raw_payload={"id": external_id},
        external_id=external_id,
        pncp_control_number=CONTROL_NUMBER,
        uasg="980921",
        purchase_number="90001",
        purchase_year=2026,
        modality="pregao_eletronico",
        title="Pregão Eletrônico 90001/2026",
        object_description="Aquisição de equipamentos",
        agency_name="Prefeitura de Exemplo",
        agency_cnpj="00000000000191",
        uf="MA",
        municipality="São Luís",
        estimated_value=Decimal("180000.00"),
        publication_at=COLLECTED_AT,
        status="publicada",
    )


class StaticConnector:
    """Complete connector double with optional discovery synchronization."""

    def __init__(
        self,
        name: str,
        procurement: RawProcurement,
        *,
        results: list[RawResult] | None = None,
        entered: asyncio.Event | None = None,
        resume: asyncio.Event | None = None,
    ) -> None:
        self.name = name
        self.procurement = procurement
        self.results = results or []
        self.entered = entered
        self.resume = resume
        self.closed = False

    async def discover_procurements(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawProcurement]]:
        assert filters.uf == "MA"
        if self.entered is not None:
            self.entered.set()
        if self.resume is not None:
            await self.resume.wait()
        return _result(
            [self.procurement], DataAvailability.AVAILABLE, source=self.name, operation="discovery"
        )

    async def fetch_procurement(
        self, external_id: str
    ) -> ConnectorResult[RawProcurementDetail | None]:
        assert external_id == self.procurement.external_id
        detail = RawProcurementDetail(
            **self.procurement.model_dump(), additional_data={"audited": True}
        )
        return _result(detail, DataAvailability.AVAILABLE, source=self.name, operation="detail")

    async def fetch_items(self, external_id: str) -> ConnectorResult[list[RawProcurementItem]]:
        assert external_id == self.procurement.external_id
        return _result([], DataAvailability.EMPTY, source=self.name, operation="items")

    async def fetch_documents(self, external_id: str) -> ConnectorResult[list[RawDocument]]:
        assert external_id == self.procurement.external_id
        return _result([], DataAvailability.NOT_SUPPORTED, source=self.name, operation="documents")

    async def fetch_results(self, external_id: str) -> ConnectorResult[list[RawResult]]:
        assert external_id == self.procurement.external_id
        availability = (
            DataAvailability.AVAILABLE if self.results else DataAvailability.NOT_PUBLISHED
        )
        return _result(self.results, availability, source=self.name, operation="results")

    async def fetch_participants(self, external_id: str) -> ConnectorResult[list[RawParticipant]]:
        assert external_id == self.procurement.external_id
        return _result(
            [], DataAvailability.NOT_SUPPORTED, source=self.name, operation="participants"
        )

    async def fetch_events(self, external_id: str) -> ConnectorResult[list[RawEvent]]:
        assert external_id == self.procurement.external_id
        return _result([], DataAvailability.NOT_SUPPORTED, source=self.name, operation="events")

    async def aclose(self) -> None:
        self.closed = True


def _winner_results(external_id: str) -> list[RawResult]:
    common = {
        "source": "pncp",
        "source_url": "https://pncp.official.test/result",
        "procurement_external_id": external_id,
        "raw_payload": {},
    }
    return [
        RawResult(
            **common,
            external_id="winner-1",
            supplier_cnpj="00000000000191",
            supplier_name="Empresa Vencedora Ltda.",
            person_type="PJ",
            role="awarded",
            total_value=Decimal("170000.00"),
            result_status="homologado",
        ),
        RawResult(
            **common,
            external_id="natural-person",
            supplier_cnpj="12345678901",
            supplier_name="Pessoa Física",
            person_type="PF",
            role="awarded",
            result_status="homologado",
        ),
        RawResult(
            **common,
            external_id="cancelled-company",
            supplier_cnpj="00000000000191",
            supplier_name="Resultado Cancelado Ltda.",
            person_type="PJ",
            role="awarded",
            result_status="cancelado",
            is_cancelled=True,
        ),
    ]


@pytest.mark.asyncio
async def test_pipeline_reprocessing_is_idempotent_and_does_not_invent_losers(
    database: DatabaseContext,
) -> None:
    """Scenario C: a winner-only result creates exactly that awarded company."""

    raw = _procurement("pncp", CONTROL_NUMBER)
    connector = StaticConnector("pncp", raw, results=_winner_results(raw.external_id))
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"pncp": connector},
    )
    request = PipelineRequest(connector="pncp", uf="MA", process_documents=False)

    first = await pipeline.run(request)
    second = await pipeline.run(request)

    assert first.status is CrawlRunStatus.COMPLETED
    assert first.records_created == 1
    assert second.status is CrawlRunStatus.COMPLETED
    assert second.records_created == 0
    assert second.records_updated == 1

    async with database.sessions() as session:
        procurement_count = await session.scalar(select(func.count()).select_from(Procurement))
        participant_count = await session.scalar(select(func.count()).select_from(Participant))
        company_count = await session.scalar(select(func.count()).select_from(Company))
        participant = await session.scalar(select(Participant))

    assert procurement_count == 1
    assert participant_count == 1
    assert company_count == 1
    assert participant is not None
    assert participant.participation_role.value == "awarded"


@pytest.mark.asyncio
async def test_same_pncp_control_number_merges_sources_without_merging_provenance(
    database: DatabaseContext,
) -> None:
    """Scenario G: PNCP and Compras become one canonical record with two source links."""

    pncp = StaticConnector("pncp", _procurement("pncp", CONTROL_NUMBER))
    compras = StaticConnector("compras_gov", _procurement("compras_gov", "98092105900012026"))
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"pncp": pncp, "compras_gov": compras},
    )

    summary = await pipeline.run(PipelineRequest(connector="all", uf="MA", process_documents=False))

    assert summary.status is CrawlRunStatus.COMPLETED
    assert summary.records_found == 2
    assert summary.records_created == 1
    assert summary.records_updated == 1
    async with database.sessions() as session:
        procurements = list((await session.scalars(select(Procurement))).all())
        persisted_run = await session.get(CrawlRun, summary.run_id)
        sources = list(
            (
                await session.scalars(select(ProcurementSource).order_by(ProcurementSource.source))
            ).all()
        )

    assert len(procurements) == 1
    assert persisted_run is not None
    assert persisted_run.cursor is not None
    assert persisted_run.cursor["sources"]["pncp"]["status"] == "completed"
    assert persisted_run.cursor["sources"]["pncp"]["records_found"] == 1
    assert persisted_run.cursor["sources"]["compras_gov"]["records_found"] == 1
    assert procurements[0].pncp_control_number == CONTROL_NUMBER
    assert [(source.source, source.external_id) for source in sources] == [
        ("compras_gov", "98092105900012026"),
        ("pncp", CONTROL_NUMBER),
    ]
    assert {source.procurement_id for source in sources} == {procurements[0].id}


@pytest.mark.asyncio
async def test_equivalent_concurrent_pipeline_is_rejected_by_database_lease(
    database: DatabaseContext,
) -> None:
    """A held lease prevents a second equivalent crawl from entering its connector."""

    entered = asyncio.Event()
    resume = asyncio.Event()
    connector = StaticConnector(
        "pncp",
        _procurement("pncp", CONTROL_NUMBER),
        entered=entered,
        resume=resume,
    )
    pipeline = IngestionPipeline(
        session_factory=database.sessions,
        connectors={"pncp": connector},
    )
    request = PipelineRequest(connector="pncp", uf="MA", process_documents=False)
    first_run = await pipeline.create_run(request)
    second_run = await pipeline.create_run(request)

    first_task = asyncio.create_task(pipeline.run(request, run_id=first_run.id))
    await asyncio.wait_for(entered.wait(), timeout=2)
    second_summary = await asyncio.wait_for(pipeline.run(request, run_id=second_run.id), timeout=2)
    resume.set()
    first_summary = await asyncio.wait_for(first_task, timeout=2)

    assert first_summary.status is CrawlRunStatus.COMPLETED
    assert second_summary.status is CrawlRunStatus.FAILED
    assert "holds the lease" in second_summary.diagnostics[0]
    async with database.sessions() as session:
        persisted_first = await session.get(CrawlRun, first_run.id)
        persisted_second = await session.get(CrawlRun, second_run.id)
    assert persisted_first is not None and persisted_first.status is CrawlRunStatus.COMPLETED
    assert persisted_second is not None and persisted_second.status is CrawlRunStatus.FAILED
