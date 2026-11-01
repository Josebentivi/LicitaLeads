"""Canonical facts, lease fencing and transactions under audit failure scenarios."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, update

from app.config import Settings
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    RawDocument,
    RawPayment,
    RawSourceRecord,
)
from app.models import (
    CrawlRun,
    CrawlRunStatus,
    Document,
    JobLease,
    Participant,
    ParticipantStatus,
    Payment,
    Procurement,
)
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payments import PaymentIngestionService

from .test_pipeline import CONTROL_NUMBER, StaticConnector, _procurement, _winner_results


@pytest.mark.parametrize("incomplete", [True, False])
async def test_only_complete_collections_retract_documents_and_awards(database, incomplete):
    raw = _procurement("pncp", CONTROL_NUMBER)

    class Connector(StaticConnector):
        incomplete = False
        active = True

        async def fetch_documents(self, external_id):
            document = RawDocument(
                source="pncp",
                external_id="doc",
                procurement_external_id=external_id,
                download_url="https://official.test/ata.pdf",
                title="Ata",
                active=self.active,
            )
            return self.response([document])

        async def fetch_results(self, external_id):
            return self.response(self.results)

        def response(self, data):
            record = RawSourceRecord.build(
                source="pncp",
                endpoint="https://official.test/details",
                request_parameters={},
                raw_payload=[row.model_dump(mode="json") for row in data],
                http_status=200,
                collected_at=datetime.now(UTC),
            )
            return ConnectorResult(
                data=[
                    row.model_copy(update={"source_record_ref": record.record_key}) for row in data
                ],
                availability=DataAvailability.AVAILABLE,
                partial=self.incomplete,
                raw_records=[record],
            )

    connector = Connector("pncp", raw, results=_winner_results(raw.external_id))
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    request = PipelineRequest(connector="pncp", uf="MA", process_documents=False)
    await pipeline.run(request)
    connector.active = False
    connector.incomplete = incomplete
    connector.results = [row.model_copy(update={"is_cancelled": True}) for row in connector.results]
    await pipeline.run(request)
    async with database.sessions() as session:
        document = await session.scalar(select(Document))
        participant = await session.scalar(select(Participant))
        assert document.is_active is incomplete
        assert (participant.status_code is ParticipantStatus.CANCELLED) is not incomplete


async def test_lost_worker_cannot_persist_or_overwrite_terminal_run(database):
    class Connector(StaticConnector):
        async def discover_procurements(self, filters):
            result = await super().discover_procurements(filters)
            async with database.sessions() as session, session.begin():
                await session.execute(
                    update(JobLease).values(owner_id="new-worker", version=JobLease.version + 1)
                )
                await session.execute(
                    update(CrawlRun).values(status=CrawlRunStatus.FAILED, diagnostic="recovered")
                )
            return result

    connector = Connector("pncp", _procurement("pncp", CONTROL_NUMBER))
    pipeline = IngestionPipeline(session_factory=database.sessions, connectors={"pncp": connector})
    summary = await pipeline.run(
        PipelineRequest(connector="pncp", uf="MA", process_documents=False)
    )
    async with database.sessions() as session:
        run = await session.get(CrawlRun, summary.run_id)
        assert run.status is CrawlRunStatus.FAILED and run.diagnostic == "recovered"
        assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
        assert (await session.get(JobLease, "crawl:pncp:MA")).owner_id == "new-worker"


async def test_generation_and_expiry_fence_stale_owners(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        repository = JobLeaseRepository(session)
        assert await repository.acquire(
            name="test", owner_id="owner", now=now, expires_at=now + timedelta(minutes=1)
        )
        generation = (await session.get(JobLease, "test")).version
        assert await repository.renew(
            name="test",
            owner_id="owner",
            generation=generation,
            now=now,
            expires_at=now + timedelta(minutes=2),
        )
        assert (await session.get(JobLease, "test")).version == generation
        assert not await repository.fence(
            name="test", owner_id="owner", generation=generation + 1, now=now
        )
        assert not await repository.renew(
            name="test",
            owner_id="owner",
            generation=generation,
            now=now + timedelta(hours=1),
            expires_at=now + timedelta(hours=2),
        )


@pytest.mark.parametrize("cancelled", [False, True])
async def test_municipal_detail_failure_rolls_back_aggregate_replacement(database, cancelled):
    settings = Settings(app_env="test", allow_insecure_http_sources=True)
    initial = RawPayment(
        source="municipal:imperatriz",
        external_id="agg:2026:100",
        amount=Decimal("10"),
        supplier_name="Fornecedor",
        commitment_number="100",
        status="agregado_por_empenho",
    )
    payment_service = PaymentIngestionService(settings, session_factory=database.sessions)
    await payment_service.persist_raw_payments([initial])

    class Connector:
        async def discover_payments(self, **kwargs):
            return ConnectorResult(
                data=[
                    initial.model_copy(
                        update={"external_id": "detail:2026:100", "status": "detalhe_empenho"}
                    )
                ],
                availability=DataAvailability.AVAILABLE,
            )

        async def aclose(self):
            return None

    class FailingPayments(PaymentIngestionService):
        async def _persist_payment(self, session, payment, source):
            await super()._persist_payment(session, payment, source)
            if cancelled:
                raise asyncio.CancelledError
            raise RuntimeError("falha após a exclusão do agregado")

    service = MunicipalPaymentIngestionService(
        settings,
        session_factory=database.sessions,
        connector=Connector(),
        payment_ingestion=FailingPayments(settings, session_factory=database.sessions),
    )
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await service.run("imperatriz", year=2026)
    else:
        await service.run("imperatriz", year=2026)
    async with database.sessions() as session:
        payments = list((await session.scalars(select(Payment))).all())
        assert [row.external_id for row in payments] == [initial.external_id]
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
    await payment_service.aclose()
    await service.aclose()


async def test_concurrent_municipal_calls_do_not_share_fencing_state(database):
    from .test_municipal_payments import FakeMunicipalConnector

    entered, resume = asyncio.Event(), asyncio.Event()

    class Connector(FakeMunicipalConnector):
        async def discover_payments(self, **kwargs):
            entered.set()
            await resume.wait()
            return await super().discover_payments(**kwargs)

    service = MunicipalPaymentIngestionService(
        Settings(_env_file=None, app_env="test", allow_insecure_http_sources=True),
        session_factory=database.sessions,
        connector=Connector(),
    )
    task = asyncio.create_task(service.run("imperatriz", year=2026))
    await asyncio.wait_for(entered.wait(), timeout=3)
    second = await service.run("imperatriz", year=2026)
    resume.set()
    first = await asyncio.wait_for(task, timeout=3)
    async with database.sessions() as session:
        assert (await session.get(CrawlRun, first.crawl_run_id)).status is CrawlRunStatus.COMPLETED
        assert (await session.get(CrawlRun, second.crawl_run_id)).status is CrawlRunStatus.FAILED
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
    await service.aclose()


async def test_municipal_cleanup_failure_still_releases_lease(database):
    from .test_municipal_payments import FakeMunicipalConnector

    class Connector(FakeMunicipalConnector):
        async def aclose(self):
            raise RuntimeError("cleanup failed")

    service = MunicipalPaymentIngestionService(
        Settings(_env_file=None, app_env="test", allow_insecure_http_sources=True),
        session_factory=database.sessions,
        connector=Connector(),
    )
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await service.run("imperatriz", year=2026)
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(JobLease)) == 0
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await service.aclose()


async def test_only_one_worker_acquires_an_expired_lease(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        session.add(JobLease(name="race", owner_id="old", expires_at=now - timedelta(seconds=1)))

    async def acquire(owner):
        async with database.sessions() as session, session.begin():
            return await JobLeaseRepository(session).acquire(
                name="race", owner_id=owner, now=now, expires_at=now + timedelta(minutes=1)
            )

    results = await asyncio.gather(acquire("one"), acquire("two"))
    assert sorted(results) == [False, True]


async def test_payment_batches_commit_only_completed_batches(database):
    settings = Settings(_env_file=None, app_env="test", payment_ingestion_batch_size=1)

    class Payments(PaymentIngestionService):
        async def _persist_payment(self, session, payment, source):
            result = await super()._persist_payment(session, payment, source)
            if payment.external_id == "second":
                raise RuntimeError("late failure")
            return result

    service = Payments(settings, session_factory=database.sessions)
    rows = [
        RawPayment(
            source="manual", external_id=key, amount=Decimal("1"), supplier_name="Fornecedor"
        )
        for key in ("first", "second")
    ]
    with pytest.raises(RuntimeError, match="late failure"):
        await service.persist_raw_payments(rows)
    async with database.sessions() as session:
        assert [row.external_id for row in (await session.scalars(select(Payment))).all()] == [
            "first"
        ]
    await service.aclose()
