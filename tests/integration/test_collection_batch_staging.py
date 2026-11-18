"""Staging keeps saved work and provenance while avoiding a database round trip per row."""

from datetime import UTC, datetime

from sqlalchemy import event, select

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawPayment,
    RawSourceRecord,
)
from app.models import CollectionBatch, CollectionWorkItem, CrawlRun, SourceRecord
from app.services.ingestion.collection_batches import CollectionCoordinator, row_identity


async def test_stage_reuses_saved_work_and_hydrates_original_payment_sources(database):
    source = "municipal:imperatriz"
    raw = RawSourceRecord.build(
        source=source,
        endpoint="https://example.com/payments",
        request_parameters={"page": 1},
        raw_payload={"rows": []},
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    async with database.sessions.begin() as session:
        run = CrawlRun(connector=source)
        original = SourceRecord(
            source=source, endpoint=raw.endpoint, response_hash=raw.response_hash
        )
        alternate = SourceRecord(source=source, endpoint=raw.endpoint, response_hash="a" * 64)
        session.add_all([run, original, alternate])
        await session.flush()
        run_id, source_id, alternate_id = run.id, original.id, alternate.id

    async def fence(_session):
        pass

    coordinator = CollectionCoordinator(database.sessions, run_id, fence)
    await coordinator.start({"connector": source}, {"connector": source})
    saved = RawPayment(source=source, external_id="saved")
    explicit = RawPayment(source=source, external_id="explicit", source_record_id=alternate_id)
    await coordinator.stage(
        source,
        ConnectorResult(
            data=[saved, explicit],
            availability=DataAvailability.AVAILABLE,
            raw_records=[raw],
            pagination=PaginationInfo(truncated=True, total_records=505),
        ),
        {raw.record_key: source_id},
    )
    saved_work_id = coordinator.items[row_identity(saved)]

    additions = [RawPayment(source=source, external_id=str(index)) for index in range(503)]
    statements = []

    def record_queries(_conn, _cursor, statement, _parameters, _context, _executemany):
        if "collection_work_items" in statement.lower():
            statements.append(statement)

    event.listen(database.engine.sync_engine, "before_cursor_execute", record_queries)
    try:
        await coordinator.stage(
            source,
            ConnectorResult(
                data=[
                    saved.model_copy(update={"source_record_id": alternate_id}),
                    additions[0],
                    additions[0].model_copy(update={"source_record_id": alternate_id}),
                    *additions[1:],
                ],
                availability=DataAvailability.AVAILABLE,
                raw_records=[raw],
            ),
            {raw.record_key: source_id},
        )
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", record_queries)
    assert len(statements) < 20

    async with database.sessions.begin() as session:
        item = await session.get_one(CollectionWorkItem, saved_work_id)
        # Older snapshots may keep provenance in the work item only.
        item.payload = {
            key: value for key, value in item.payload.items() if key != "source_record_id"
        }
        explicit_item = await session.get_one(
            CollectionWorkItem, coordinator.items[row_identity(explicit)]
        )
        explicit_item.source_record_id = source_id
        batch = await session.get_one(CollectionBatch, coordinator.batch_id)
        assert batch.cursors[source]["resumable"]
        assert batch.cursors[source]["total_records"] == 505

    replay = CollectionCoordinator(database.sessions, run_id, fence)
    replay.batch_id = coordinator.batch_id
    pending = await replay.pending_result(source, RawPayment)
    assert pending is not None
    assert [row.external_id for row in pending.data] == [
        "saved",
        "explicit",
        *(row.external_id for row in additions),
    ]
    assert [row.source_record_id for row in pending.data[:3]] == [
        source_id,
        alternate_id,
        source_id,
    ]
    assert replay.items[row_identity(saved)] == saved_work_id
    async with database.sessions() as session:
        assert (
            await session.scalars(
                select(CollectionWorkItem.position).order_by(CollectionWorkItem.position)
            )
        ).all() == list(range(1, 506))
