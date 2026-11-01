"""Repeated source items and telemetry writers cannot abort procurement ingestion."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    RawProcurementItem,
    RawSourceRecord,
)
from app.models import CrawlRunStatus, FieldChange, ProcurementItem, SourceRecord
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_pipeline import StaticConnector, _procurement
from .test_sqlite_write_queue import short_busy_timeout


class RepeatedItems(StaticConnector):
    def __init__(self, versions):
        super().__init__("compras_gov", _procurement("compras_gov", "16010305000542026"))
        self.versions = versions

    async def fetch_items(self, external_id):
        record = RawSourceRecord.build(
            source=self.name,
            endpoint="https://official.test/items",
            request_parameters={},
            raw_payload=[row.model_dump(mode="json") for row in self.versions],
            http_status=200,
            collected_at=datetime(2026, 10, 28, tzinfo=UTC),
        )
        return ConnectorResult(
            data=[
                row.model_copy(update={"source_record_ref": record.record_key})
                for row in self.versions
            ],
            availability=DataAvailability.AVAILABLE,
            raw_records=[record],
        )


@pytest.mark.parametrize("official_dates", [False, True])
async def test_duplicate_official_item_identity_preserves_versions_and_rejects_older_values(
    database, official_dates
):
    start = datetime(2026, 10, 28, tzinfo=UTC)
    values = ["B", "A", "C"] if official_dates else ["A", "B", "A"]
    stamps = [2, 1, 3]
    versions = [
        RawProcurementItem(
            source="compras_gov",
            external_id="1601030500054202600006",
            procurement_external_id="16010305000542026",
            item_number="6",
            description="Item oficial",
            result_status=value,
            observed_at=start + timedelta(minutes=index),
            source_updated_at=start + timedelta(days=stamps[index]) if official_dates else None,
            source_update_field="dataAtualizacao" if official_dates else None,
        )
        for index, value in enumerate(values)
    ]
    source = RepeatedItems(versions)
    pipeline = IngestionPipeline(
        session_factory=database.sessions, connectors={source.name: source}
    )
    request = PipelineRequest(connector=source.name, process_documents=False)
    first = await pipeline.run(request)
    assert first.status is CrawlRunStatus.COMPLETED and first.records_failed == 0
    async with database.sessions() as session:
        items = (await session.scalars(select(ProcurementItem))).all()
        assert len(items) == 1 and items[0].result_status == values[-1]
        identifier = items[0].id
        changes = (
            await session.scalars(
                select(FieldChange)
                .where(
                    FieldChange.entity_id == identifier,
                    FieldChange.field_name == "result_status",
                )
                .order_by(FieldChange.observed_at, FieldChange.recorded_at, FieldChange.id)
            )
        ).all()
        assert [row.received_value for row in changes] == values
        if official_dates:
            assert changes[1].decision == "stale_ignored" and not changes[1].applied
        else:
            assert [row.previous_value for row in changes] == [None, "A", "B"]
        for change in changes:
            stored = await session.get(SourceRecord, change.source_record_id)
            assert len(stored.raw_payload) == 3
            assert change.crawl_run_id == first.run_id
    # A later crawl reuses the canonical item and its fingerprint.
    second = await pipeline.run(request)
    assert second.status is CrawlRunStatus.COMPLETED
    async with database.sessions() as session:
        items = (await session.scalars(select(ProcurementItem))).all()
        assert len(items) == 1 and items[0].id == identifier


async def test_progress_checkpoint_and_heartbeat_wait_for_domain_transaction(database):
    short_busy_timeout(database.engine)
    await database.engine.dispose()

    class ContendedPipeline(IngestionPipeline):
        signals = ()

        async def _upsert_items(self, session, procurement, items, *args):
            owner = f"pipeline:{self._execution().run_id}"
            self.signals = (
                asyncio.create_task(self._persist_activity()),
                asyncio.create_task(self._checkpoint()),
                asyncio.create_task(self._renew_lease("crawl:pncp:MA", owner)),
            )
            await asyncio.sleep(0.08)
            assert all(not task.done() for task in self.signals)
            return await super()._upsert_items(session, procurement, items, *args)

    source = StaticConnector("pncp", _procurement("pncp", "one"))
    pipeline = ContendedPipeline(session_factory=database.sessions, connectors={"pncp": source})
    result = await asyncio.wait_for(
        pipeline.run(PipelineRequest(connector="pncp", process_documents=False)), 5
    )
    assert result.status is CrawlRunStatus.COMPLETED
    assert result.diagnostics == [] and result.records_created == 1
    values = await asyncio.gather(*pipeline.signals)
    assert values[-1] is True
