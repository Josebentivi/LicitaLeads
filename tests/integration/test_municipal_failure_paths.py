"""Municipal workers remain independent, cancellable and fenced on failure."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability, RawPayment
from app.models import CrawlRun, CrawlRunStatus, JobLease, Payment
from app.services.ingestion.municipal_payments import (
    MunicipalPaymentIngestionService,
    MunicipalPaymentSummary,
    cancel_all_municipal_tasks,
    cancel_municipal_task,
    discard_municipal_task,
    latest_municipal_runs,
    register_municipal_task,
    run_selected_municipal_sources,
)
from app.services.ingestion.payments import PaymentIngestionService


async def test_municipal_sources_continue_after_run_and_cleanup_failures(monkeypatch):
    import app.services.ingestion.municipal_payments as module

    calls = []

    class FailingSource:
        source_id = None

        def __init__(self, settings):
            pass

        async def run(self, source_id, **kwargs):
            self.source_id = source_id
            calls.append(source_id)
            if source_id == "imperatriz":
                raise RuntimeError("official endpoint unavailable")
            return MunicipalPaymentSummary(
                source_id=source_id, source_name=source_id, availability=DataAvailability.EMPTY
            )

        async def aclose(self):
            if self.source_id == "imperatriz":
                raise RuntimeError("cleanup failed")

    monkeypatch.setattr(module, "MunicipalPaymentIngestionService", FailingSource)
    results = await run_selected_municipal_sources(
        Settings(_env_file=None), year=2026, source_ids=["imperatriz", "bacabal"]
    )
    assert calls == ["imperatriz", "bacabal"]
    assert [row.availability for row in results] == [
        DataAvailability.TEMPORARY_ERROR,
        DataAvailability.EMPTY,
    ]
    assert "official endpoint unavailable" in results[0].diagnostic
    assert "cleanup failed" in results[0].diagnostic


async def test_shutdown_cancels_and_drains_all_registered_municipal_tasks():
    async def wait_for_portal():
        await asyncio.Event().wait()

    first_id, second_id = uuid4(), uuid4()
    first, second = asyncio.create_task(wait_for_portal()), asyncio.create_task(wait_for_portal())
    register_municipal_task(first_id, first)
    register_municipal_task(second_id, second)
    assert not cancel_municipal_task(uuid4())
    assert cancel_municipal_task(first_id)
    await cancel_all_municipal_tasks()
    assert first.cancelled() and second.cancelled()
    assert not cancel_municipal_task(first_id)
    discard_municipal_task(second_id)


async def test_latest_municipal_run_filter_ignores_older_history_and_other_sources(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        for index, source in enumerate(
            ["municipal:imperatriz", "municipal:bacabal", "municipal:imperatriz", "pncp"]
        ):
            session.add(
                CrawlRun(
                    connector=source,
                    created_at=now + timedelta(seconds=index),
                    status=CrawlRunStatus.COMPLETED,
                    records_found=index,
                )
            )
    async with database.sessions() as session:
        latest = await latest_municipal_runs(session, ["imperatriz"])
    assert set(latest) == {"imperatriz"}
    assert latest["imperatriz"].records_found == 2


async def test_municipal_worker_losing_lease_cannot_persist_or_overwrite_recovery(database):
    class LosingConnector:
        async def discover_payments(self, **kwargs):
            async with database.sessions() as session, session.begin():
                await session.execute(
                    update(JobLease).values(owner_id="replacement", version=JobLease.version + 1)
                )
                await session.execute(
                    update(CrawlRun).values(
                        status=CrawlRunStatus.FAILED, diagnostic="recovered by another worker"
                    )
                )
            return ConnectorResult(
                data=[
                    RawPayment(
                        source="municipal:imperatriz",
                        external_id="lost",
                        amount=Decimal("10"),
                        supplier_name="Empresa",
                    )
                ],
                availability=DataAvailability.AVAILABLE,
            )

        async def aclose(self):
            return None

    service = MunicipalPaymentIngestionService(
        Settings(_env_file=None, allow_insecure_http_sources=True),
        session_factory=database.sessions,
        connector=LosingConnector(),
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await service.run("imperatriz", year=2026)
        async with database.sessions() as session:
            run = await session.scalar(select(CrawlRun))
            assert (
                run.status is CrawlRunStatus.FAILED
                and run.diagnostic == "recovered by another worker"
            )
            assert await session.scalar(select(func.count()).select_from(Payment)) == 0
            assert (await session.get(JobLease, "municipal:imperatriz")).owner_id == "replacement"
    finally:
        await service.aclose()


async def test_legacy_aggregate_without_pkemp_is_not_merged_by_commitment_number(database):
    ingestion = PaymentIngestionService(session_factory=database.sessions)
    service = MunicipalPaymentIngestionService(
        session_factory=database.sessions, payment_ingestion=ingestion
    )
    try:
        await ingestion.persist_raw_payments(
            [
                RawPayment(
                    source="municipal:imperatriz",
                    external_id="legacy:2026:10",
                    amount=Decimal("100"),
                    supplier_name="Empresa",
                    status="agregado_por_empenho",
                    commitment_number="10",
                )
            ]
        )
        detail = RawPayment(
            source="municipal:imperatriz",
            external_id="detail:2026:PK2",
            amount=Decimal("100"),
            supplier_name="Empresa",
            status="detalhe_empenho",
            commitment_number="10",
            commitment_external_id="imperatriz:2026:PK2",
            commitment_details_complete=True,
        )
        async with database.sessions() as session, session.begin():
            staged = await service._supersede_aggregates(session, [detail], year=2026)
            await ingestion.persist_raw_payments(staged, session=session)
        async with database.sessions() as session:
            rows = list((await session.scalars(select(Payment))).all())
        assert len(rows) == 2
        assert [row.external_id for row in rows if row.is_current] == ["legacy:2026:10"]
        assert rows[0].commitment_external_id is None
    finally:
        await service.aclose()
