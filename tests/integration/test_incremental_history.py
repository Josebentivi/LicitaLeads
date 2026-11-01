"""Merge decisions are transactional, typed, chronological and auditable."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.connectors.base import RawProcurement
from app.models import FieldChange, Procurement
from app.services.ingestion.field_updates import install_audit
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest

from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement


async def update(
    database,
    *,
    status="A",
    amount=Decimal("10"),
    flag=True,
    stamp=None,
    observed=None,
    complete=False,
    clear=False,
):
    raw = RawProcurement(
        source="pncp",
        external_id="audit",
        status=status,
        estimated_value=amount,
        is_srp=flag,
        last_source_update_at=stamp,
        cleared_fields=["status"] if clear else [],
    )
    async with database.sessions() as session, session.begin():
        entity = await session.scalar(select(Procurement))
        tracker = install_audit(session, raw, observed_at=observed, complete=complete)
        if entity is None:
            entity = Procurement(source="pncp", external_id="audit", fingerprint="audit")
            session.add(entity)
        entity.status = status
        entity.estimated_value = amount
        entity.is_srp = flag
        entity.last_source_update_at = stamp
        await session.flush()
        identifier = entity.id
    return identifier, tracker.outcome


async def test_older_source_cannot_replace_newer_values(database):
    now = datetime.now(UTC)
    await update(database, status="new", stamp=now)
    _, outcome = await update(database, status="old", stamp=now - timedelta(days=1))
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        changes = list(
            (
                await session.scalars(
                    select(FieldChange).where(FieldChange.decision == "stale_ignored")
                )
            ).all()
        )
    assert entity.status == "new"
    assert outcome.stale and changes and not changes[0].applied


async def test_legacy_record_with_official_clock_is_also_protected(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(
            Procurement(
                source="pncp",
                external_id="audit",
                fingerprint="audit",
                status="new",
                last_source_update_at=now,
            )
        )
    await update(database, status="old", stamp=now - timedelta(days=1))
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        assert entity.status == "new" and entity.last_source_update_at == now


async def test_inferred_phase_clock_is_independent_from_old_source_version(database, monkeypatch):
    from app.models import ProcurementPhase
    from app.services import procurement_phase
    from app.services.ingestion import pipeline as pipeline_module

    now = datetime.now(UTC)
    stamp = datetime(2026, 9, 15, tzinfo=UTC)
    raw = _procurement("pncp", CONTROL_NUMBER).model_copy(
        update={"last_source_update_at": stamp, "proposal_end_at": now + timedelta(days=1)}
    )
    connector = StaticConnector("pncp", raw)
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    request = PipelineRequest(connector="pncp", process_documents=False)
    await pipeline.run(request)

    async def refresh(session, entity):
        return await procurement_phase.refresh_procurement_phase(
            session, entity, now=now + timedelta(days=2)
        )

    monkeypatch.setattr(pipeline_module, "refresh_procurement_phase", refresh)
    connector.procurement = raw.model_copy(
        update={"last_source_update_at": stamp - timedelta(days=1)}
    )
    await pipeline.run(request)
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        assert entity.current_phase is not ProcurementPhase.PROPOSALS
        assert entity.last_source_update_at == stamp


async def test_undated_last_observation_wins_and_a_b_a_is_preserved(database):
    now = datetime.now(UTC)
    for index, status in enumerate(["A", "B", "A"]):
        await update(database, status=status, observed=now + timedelta(seconds=index))
    async with database.sessions() as session:
        rows = list(
            (
                await session.scalars(
                    select(FieldChange)
                    .where(FieldChange.field_name == "status")
                    .order_by(FieldChange.observed_at)
                )
            ).all()
        )
    assert [row.received_value for row in rows] == ["A", "B", "A"]
    assert [row.previous_value for row in rows] == [None, "A", "B"]
    assert rows[-1].decision == "conflict_applied"


async def test_observation_order_is_not_transaction_completion_order(database):
    now = datetime.now(UTC)
    await update(database, status="new", observed=now)
    await update(database, status="late transaction", observed=now - timedelta(minutes=1))
    async with database.sessions() as session:
        assert (await session.scalar(select(Procurement))).status == "new"


@pytest.mark.parametrize("missing", [None, "", "   "])
async def test_missing_fields_do_not_erase_and_zero_false_are_valid(database, missing):
    await update(database)
    await update(database, status=missing, amount=Decimal("0"), flag=False)
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        assert entity.status == "A"
        assert entity.estimated_value == 0
        assert entity.is_srp is False


async def test_clear_requires_explicit_complete_response(database):
    await update(database)
    await update(database, status=None, clear=True)
    async with database.sessions() as session:
        assert (await session.scalar(select(Procurement))).status == "A"
    await update(database, status=None, clear=True, complete=True)
    async with database.sessions() as session:
        assert (await session.scalar(select(Procurement))).status is None


async def test_rollback_discards_history_and_values_together(database):
    await update(database)
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        install_audit(session, RawProcurement(source="pncp", external_id="audit"))
        entity.status = "rolled back"
        await session.flush()
        await session.rollback()
    async with database.sessions() as session:
        assert (await session.scalar(select(Procurement))).status == "A"
        assert not (
            await session.scalars(
                select(FieldChange).where(FieldChange.received_value == "rolled back")
            )
        ).all()


async def test_repeated_collection_counts_unchanged_and_history_api_escapes(api_client, database):
    connector = StaticConnector("pncp", _procurement("pncp", CONTROL_NUMBER))
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    request = PipelineRequest(connector="pncp", process_documents=False)
    first = await pipeline.run(request)
    second = await pipeline.run(request)
    assert first.records_created == 1
    assert second.records_updated == 1
    async with database.sessions() as session:
        changes = list(
            (
                await session.scalars(
                    select(FieldChange).where(FieldChange.crawl_run_id == second.run_id)
                )
            ).all()
        )
    assert second.records_unchanged == 1 and second.records_changed == 0, [
        (row.field_name, row.previous_value, row.received_value) for row in changes
    ]
    async with database.sessions() as session:
        entity = await session.scalar(select(Procurement))
        identifier = entity.id
        install_audit(session, RawProcurement(source="pncp", external_id="audit"))
        entity.status = '<script>alert("x")</script>'
        await session.commit()
    response = await api_client.get(f"/api/history/procurement/{identifier}")
    assert response.status_code == 200 and response.json()["total"] > 0
    fragment = await api_client.get(f"/history/procurement/{identifier}")
    assert fragment.status_code == 200
    assert '<script>alert("x")</script>' not in fragment.text
    assert "&lt;script&gt;" in fragment.text
    assert (await api_client.get(f"/api/history/secret/{identifier}")).status_code == 422
