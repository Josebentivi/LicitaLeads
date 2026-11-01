"""Crawls preserve provenance, partial failures and terminal state across modes."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawDocument,
    RawSourceRecord,
)
from app.models import (
    Contract,
    ContractItem,
    ContractTerm,
    CrawlRun,
    CrawlRunStatus,
    Document,
    ExtractionStatus,
    JobLease,
    Lead,
    PriceRegistry,
    PriceRegistryAdhesion,
    PriceRegistryCommitment,
    PriceRegistryItem,
    PriceRegistryUnit,
    SourceRecord,
)
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.ingestion.processor import DocumentProcessingService

from .test_contracts_payments import ContractConnector, _raw_contract
from .test_document_processing import FakeDownloader, _settings
from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement, _result
from .test_price_registry_pipeline import (
    PriceRegistryConnector,
    _registry,
    _registry_adhesion,
    _registry_commitment,
    _registry_item,
    _registry_unit,
)


def _paged(rows, source, operation):
    records = []
    attached = []
    for page, page_rows in enumerate((rows[:2], rows[2:]), start=1):
        record = RawSourceRecord.build(
            source=source,
            endpoint=f"https://official.test/{operation}",
            request_parameters={"pagina": page},
            raw_payload=[row.model_dump(mode="json") for row in page_rows],
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        records.append(record)
        attached.extend(
            row.model_copy(update={"source_record_ref": record.record_key}) for row in page_rows
        )
    return ConnectorResult(
        data=attached,
        availability=DataAvailability.AVAILABLE,
        raw_records=records,
        pagination=PaginationInfo(total_pages=2, fetched_pages=2),
    )


async def test_contract_items_and_terms_keep_page_provenance_after_updates(database):
    from app.connectors.base import RawContractItem, RawContractTerm

    class Connector(ContractConnector):
        revision = 1

        async def fetch_contract_items(self, contract):
            return _paged(
                [
                    RawContractItem(
                        source=self.name,
                        external_id=f"i{n}",
                        contract_external_id=contract.external_id,
                        item_number=str(n),
                        description=f"Item {n} revisão {self.revision}",
                        quantity=Decimal("1"),
                        total_value=Decimal(100 + self.revision),
                    )
                    for n in range(1, 4)
                ],
                self.name,
                "items",
            )

        async def fetch_contract_terms(self, contract):
            return _paged(
                [
                    RawContractTerm(
                        source=self.name,
                        external_id=f"t{n}",
                        contract_external_id=contract.external_id,
                        term_number=str(n),
                        term_type="aditivo",
                        value_delta=Decimal(self.revision),
                    )
                    for n in range(1, 4)
                ],
                self.name,
                "terms",
            )

    connector = Connector(_raw_contract())
    pipeline = IngestionPipeline(
        session_factory=database.sessions, connectors={connector.name: connector}
    )
    request = PipelineRequest(connector=connector.name, mode="contracts", process_documents=False)
    assert (await pipeline.run(request)).records_created == 1
    connector.revision = 2
    assert (await pipeline.run(request)).records_updated == 1
    async with database.sessions() as session:
        items = list(
            (await session.scalars(select(ContractItem).order_by(ContractItem.item_number))).all()
        )
        terms = list(
            (await session.scalars(select(ContractTerm).order_by(ContractTerm.term_number))).all()
        )
        sources = {row.id: row for row in (await session.scalars(select(SourceRecord))).all()}
    assert [sources[row.source_record_id].request_parameters["pagina"] for row in items] == [
        1,
        1,
        2,
    ]
    assert [sources[row.source_record_id].request_parameters["pagina"] for row in terms] == [
        1,
        1,
        2,
    ]
    assert all("revisão 2" in row.description for row in items)
    assert all(row.value_delta == Decimal("2") for row in terms)


async def test_arp_details_keep_page_provenance_and_update_without_duplicates(database):
    class Connector(PriceRegistryConnector):
        revision = 1

        def response(self, template, operation):
            rows = [
                template.model_copy(
                    update={"external_id": f"{operation}-{n}", "item_number": str(n)}
                )
                for n in range(1, 4)
            ]
            if operation == "units":
                rows = [
                    row.model_copy(
                        update={"adhesion_balance": Decimal(self.revision), "unit_code": str(n)}
                    )
                    for n, row in enumerate(rows, start=1)
                ]
            elif operation == "commitments":
                rows = [row.model_copy(update={"balance": Decimal(self.revision)}) for row in rows]
            elif operation == "adhesions":
                rows = [
                    row.model_copy(update={"approved_quantity": Decimal(self.revision)})
                    for row in rows
                ]
            return _paged(rows, self.name, operation)

        async def fetch_price_registry_items(self, external_id):
            return self.response(_registry_item(), "items")

        async def fetch_price_registry_units(self, registry):
            return self.response(_registry_unit(), "units")

        async def fetch_price_registry_commitments(self, registry):
            return self.response(_registry_commitment(), "commitments")

        async def fetch_price_registry_adhesions(self, registry):
            return self.response(_registry_adhesion(), "adhesions")

    connector = Connector(_registry(), [])
    pipeline = IngestionPipeline(
        session_factory=database.sessions, connectors={connector.name: connector}
    )
    request = PipelineRequest(
        connector=connector.name, mode="price_registries", process_documents=False
    )
    assert (await pipeline.run(request)).records_created == 1
    connector.revision = 2
    assert (await pipeline.run(request)).records_updated == 1
    async with database.sessions() as session:
        sources = {row.id: row for row in (await session.scalars(select(SourceRecord))).all()}
        for model in (
            PriceRegistryItem,
            PriceRegistryUnit,
            PriceRegistryCommitment,
            PriceRegistryAdhesion,
        ):
            rows = list((await session.scalars(select(model).order_by(model.item_number))).all())
            assert len(rows) == 3
            assert [sources[row.source_record_id].request_parameters["pagina"] for row in rows] == [
                1,
                1,
                2,
            ]
        assert (await session.scalar(select(PriceRegistryUnit))).adhesion_balance == Decimal("2")
        assert (await session.scalar(select(PriceRegistryCommitment))).balance == Decimal("2")
        assert (await session.scalar(select(PriceRegistryAdhesion))).approved_quantity == Decimal(
            "2"
        )


@pytest.mark.parametrize("mode", ["contracts", "price_registries"])
@pytest.mark.parametrize(
    "availability", [DataAvailability.TEMPORARY_ERROR, DataAvailability.ACCESS_RESTRICTED]
)
async def test_auxiliary_discovery_failure_never_reports_complete(database, mode, availability):
    async def discover(filters):
        return ConnectorResult(
            data=[], availability=availability, diagnostic="official endpoint unavailable"
        )

    async def close():
        return None

    connector = SimpleNamespace(
        name="pncp", discover_contracts=discover, discover_price_registries=discover, aclose=close
    )
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    result = await pipeline.run(
        PipelineRequest(connector="pncp", mode=mode, process_documents=False)
    )
    async with database.sessions() as session:
        run = await session.get(CrawlRun, result.run_id)
        assert run.status is CrawlRunStatus.PARTIAL and run.finished_at is not None
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
        assert await session.scalar(select(func.count()).select_from(Contract)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceRegistry)) == 0


@pytest.mark.parametrize("mode", ["contracts", "price_registries"])
async def test_auxiliary_source_without_optional_details_still_persists_header(database, mode):
    name = "pncp" if mode == "contracts" else "compras_gov"
    raw = _raw_contract() if mode == "contracts" else _registry()

    async def discover(filters):
        return _result([raw], DataAvailability.AVAILABLE, source=name, operation="discovery")

    async def items(external_id):
        return ConnectorResult(data=[], availability=DataAvailability.NOT_SUPPORTED)

    async def close():
        return None

    connector = SimpleNamespace(
        name=name,
        discover_contracts=discover,
        discover_price_registries=discover,
        fetch_price_registry_items=items,
        aclose=close,
    )
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={name: connector})
    result = await pipeline.run(PipelineRequest(connector=name, mode=mode, process_documents=False))
    assert result.status is CrawlRunStatus.COMPLETED and result.records_created == 1


async def test_connector_cleanup_failure_does_not_leave_running_crawl_or_lease(database):
    class BrokenClose(StaticConnector):
        async def aclose(self):
            raise RuntimeError("connector cleanup failed")

    connector = BrokenClose("pncp", _procurement("pncp", CONTROL_NUMBER))
    result = await IngestionPipeline(
        session_factory=database.sessions, connectors={"pncp": connector}
    ).run(PipelineRequest(connector="pncp", process_documents=False))
    async with database.sessions() as session:
        run = await session.get(CrawlRun, result.run_id)
        assert run.status is CrawlRunStatus.COMPLETED and run.finished_at is not None
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0


async def test_terminal_commit_releases_source_lease_before_final_cleanup(database, monkeypatch):
    connector = StaticConnector("pncp", _procurement("pncp", CONTROL_NUMBER))
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    observed = []

    async def interrupted_cleanup(name, owner):
        async with database.sessions() as session:
            run = await session.get(CrawlRun, owner.removeprefix("pipeline:"))
            lease = await session.get(JobLease, name)
            observed.append((run.status, lease is None))
        raise RuntimeError("process stopped before the fallback cleanup")

    monkeypatch.setattr(pipeline, "_release_lease", interrupted_cleanup)
    result = await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
    assert result.status is CrawlRunStatus.COMPLETED
    assert observed and all(
        status is CrawlRunStatus.COMPLETED and released for status, released in observed
    )


@pytest.mark.parametrize("download_fails", [False, True])
async def test_pipeline_document_stage_persists_progress_and_honest_terminal_status(
    database, tmp_path, monkeypatch, download_fails
):
    import app.services.ingestion.processor as processor_module

    class Connector(StaticConnector):
        async def fetch_documents(self, external_id):
            return _result(
                [
                    RawDocument(
                        source=self.name,
                        external_id="doc",
                        procurement_external_id=external_id,
                        download_url="https://official.test/ata.txt",
                        document_type="ata",
                    )
                ],
                DataAvailability.AVAILABLE,
                source=self.name,
                operation="documents",
            )

    text = (
        "EMPRESA, CNPJ 04.252.011/0001-10, foi inabilitada por falta da certidão exigida.".encode()
    )
    downloader = FakeDownloader(
        text, error=RuntimeError("download failed") if download_fails else None
    )
    real_processor = DocumentProcessingService

    def processor(settings, *, session_factory, http_budgets=None, crawl_run_id=None):
        return real_processor(
            settings,
            session_factory=session_factory,
            downloader=downloader,
            http_budgets=http_budgets,
            crawl_run_id=crawl_run_id,
        )

    monkeypatch.setattr(processor_module, "DocumentProcessingService", processor)
    connector = Connector("pncp", _procurement("pncp", CONTROL_NUMBER))
    result = await IngestionPipeline(
        _settings(tmp_path), session_factory=database.sessions, connectors={"pncp": connector}
    ).run(PipelineRequest(connector="pncp", process_documents=True))
    assert result.status is (CrawlRunStatus.PARTIAL if download_fails else CrawlRunStatus.COMPLETED)
    async with database.sessions() as session:
        run = await session.get(CrawlRun, result.run_id)
        document = await session.scalar(select(Document))
        assert run.cursor["sources"]["documents"]["documents_processed"] == (
            0 if download_fails else 1
        )
        assert document.extraction_status is (
            ExtractionStatus.FAILED if download_fails else ExtractionStatus.EXTRACTED
        )
        lead_count = await session.scalar(select(func.count()).select_from(Lead))
        assert lead_count == 0 if download_fails else lead_count > 0
        assert run.cursor["sources"]["documents"]["leads_created"] == lead_count
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
