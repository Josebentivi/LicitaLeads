"""Exercise municipal commits and continuation against a shared SQLite file."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import event, func, select

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability, RawPayment, RawSourceRecord
from app.database import write_session
from app.models import CollectionBatch, CollectionWorkItem, CrawlRun, CrawlRunStatus, Payment
from app.services.ingestion import municipal_payments
from app.services.ingestion.continuation import create_continuation
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_collection_continuation import CountingConnector
from .test_pipeline import _procurement


class ImperatrizSnapshot:
    name = "municipal:imperatriz"

    def __init__(self, size: int) -> None:
        self.size = size
        self.calls = 0

    async def discover_payments(self, *, year, **_filters):
        self.calls += 1
        entries = [{"pkemp": index, "valor": 25} for index in range(self.size)]
        record = RawSourceRecord.build(
            source=self.name,
            endpoint="https://transparencia.test/despesas",
            request_parameters={"ano": year},
            raw_payload=entries,
            http_status=200,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=[
                RawPayment(
                    source=self.name,
                    external_id=f"imperatriz:{year}:{index}",
                    commitment_external_id=f"PKEMP:{index}",
                    supplier_cnpj="52246769000198",
                    amount=Decimal("25"),
                    phase="pago",
                    status="agregado_por_empenho",
                    source_record_ref=record.record_key,
                    raw_payload=entry,
                )
                for index, entry in enumerate(entries)
            ],
            raw_records=[record],
            availability=DataAvailability.AVAILABLE,
        )

    async def aclose(self):
        pass


async def test_large_municipal_run_releases_sqlite_between_commits(database, monkeypatch):
    settings = Settings(_env_file=None, allow_insecure_http_sources=True)
    first_commit = asyncio.Event()
    release_municipal = asyncio.Event()
    block_sizes = []

    class ObservedPayments(PaymentIngestionService):
        async def persist_raw_payments(self, payments, **kwargs):
            result = await super().persist_raw_payments(payments, **kwargs)
            kwargs["session"].info["test_payment_block"] = len(payments)
            return result

    @asynccontextmanager
    async def observe_commit(factory):
        async with write_session(factory) as session:
            yield session
            size = session.info.get("test_payment_block")
        if size is not None:
            block_sizes.append(size)
            if len(block_sizes) == 1:
                first_commit.set()
                await release_municipal.wait()

    monkeypatch.setattr(municipal_payments, "write_session", observe_commit)
    connector = ImperatrizSnapshot(4563)
    municipal = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
        payment_ingestion=ObservedPayments(settings, session_factory=database.sessions),
    )
    procurements = [
        _procurement("pncp", f"concurrent-{index}").model_copy(
            update={
                "pncp_control_number": f"00000000000191-1-{index:06}/2026",
                "purchase_number": str(index),
            }
        )
        for index in range(1, 88)
    ]
    pipeline = IngestionPipeline(
        settings,
        session_factory=database.sessions,
        connectors={"pncp": CountingConnector(procurements)},
    )
    worker = asyncio.create_task(municipal.run("imperatriz", year=2026))
    try:
        await asyncio.wait_for(first_commit.wait(), 90)
        async with database.sessions() as db:
            run = (
                await db.scalars(select(CrawlRun).where(CrawlRun.connector == connector.name))
            ).one()
            assert run.status is CrawlRunStatus.RUNNING
            assert run.records_found == 4563 and run.records_created == 50
            assert run.cursor["municipal_write"]["processed"] == 50
            assert run.cursor["municipal_write"]["total"] == 4563
        async with asyncio.timeout(90):
            procurement = await pipeline.run(
                PipelineRequest(connector="pncp", process_documents=False, days=365)
            )
        assert procurement.status is CrawlRunStatus.COMPLETED
        assert procurement.records_created == 87 and not worker.done()
        release_municipal.set()
        summary = await worker
        assert summary.payments_created == 4563
        assert sum(block_sizes) == 4563 and max(block_sizes) == 50
        async with database.sessions() as db:
            final = await db.get_one(CrawlRun, summary.crawl_run_id)
            assert final.status is CrawlRunStatus.COMPLETED
            assert final.cursor["municipal_write"]["processed"] == 4563
            assert (await db.get_one(CollectionBatch, final.batch_id)).status == "complete"
    finally:
        release_municipal.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await municipal.aclose()


async def test_second_municipal_block_rolls_back_and_resumes_from_original_evidence(database):
    settings = Settings(
        _env_file=None, payment_ingestion_batch_size=2, allow_insecure_http_sources=True
    )
    connector = ImperatrizSnapshot(4)

    class CancelSecondBlock(PaymentIngestionService):
        blocks = 0

        async def persist_raw_payments(self, payments, **kwargs):
            result = await super().persist_raw_payments(payments, **kwargs)
            self.blocks += 1
            if self.blocks == 2:
                raise asyncio.CancelledError
            return result

    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=connector,
        payment_ingestion=CancelSecondBlock(settings, session_factory=database.sessions),
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await service.run("imperatriz", year=2026)
    finally:
        await service.aclose()

    async with database.sessions.begin() as db:
        first = (await db.scalars(select(CrawlRun))).one()
        assert first.status is CrawlRunStatus.CANCELLED
        assert first.records_found == 4 and first.records_created == 2
        assert first.cursor["municipal_write"]["processed"] == 2
        assert first.cursor["municipal_write"]["total"] == 4
        batch_id = first.batch_id
        work = (
            await db.scalars(select(CollectionWorkItem).where(CollectionWorkItem.kind == "record"))
        ).all()
        assert sorted(item.status for item in work) == [
            "pending",
            "pending",
            "succeeded",
            "succeeded",
        ]
        origin = {item.payload["external_id"]: item.source_record_id for item in work}
        assert all(origin.values())
        child, _filters = await create_continuation(db, first.id)
        child_id = child.id

    resumed = MunicipalPaymentIngestionService(
        settings, session_factory=database.sessions, connector=connector
    )
    origin_reads = []

    def count_origin_reads(_connection, _cursor, statement, *_args):
        if "FROM source_records" in statement and statement.startswith("SELECT"):
            origin_reads.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", count_origin_reads)
    try:
        summary = await resumed.run("imperatriz", year=2026, run_id=child_id)
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", count_origin_reads)
        await resumed.aclose()
    assert summary.payments_created == 2 and connector.calls == 1
    assert len(origin_reads) == 1
    assert "raw_payload" not in origin_reads[0]
    async with database.sessions() as db:
        rows = (await db.scalars(select(Payment))).all()
        assert len(rows) == 4
        assert {row.external_id: row.source_record_id for row in rows} == origin
        assert (await db.get_one(CollectionBatch, batch_id)).status == "complete"
        assert (
            await db.scalar(
                select(func.count())
                .select_from(CollectionWorkItem)
                .where(
                    CollectionWorkItem.kind == "record", CollectionWorkItem.status == "succeeded"
                )
            )
            == 4
        )
