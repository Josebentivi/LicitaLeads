"""Federal coverage progresses by confirmed archive or beneficiary, independently of root runs."""

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.connectors.federal_payment_archives import (
    FederalArchivePayment,
    FederalArchiveUnavailable,
    FederalPaymentArchive,
)
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    Company,
    CrawlRun,
    CrawlRunStatus,
    JobLease,
    Payment,
    SourceRecord,
)
from app.services.ingestion.complete_collection import (
    CollectionScope,
    CompleteCollection,
    StepResult,
)
from app.services.ingestion.continuation import create_continuation
from app.services.ingestion.federal_payments import FEDERAL_LEASE, FederalCoverageService
from app.services.ingestion.recovery import finalize_cancelled_run
from app.services.payment_tracking import set_payment_tracking

SUPPLIER = "12345678000195"
SECOND = "00000000000191"
DAYS = (date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22))
DOCUMENT = "153173152532026OB000001"


class Archives:
    def __init__(self) -> None:
        self.days = {DAYS[1], DAYS[2]}
        self.calls: list[tuple[date, frozenset[str]]] = []
        self.dates: list[tuple[date, date]] = []

    async def aclose(self) -> None:
        pass

    async def list_days(self) -> frozenset[date]:
        return frozenset(self.days)

    async def fetch(
        self, day: date, targets: frozenset[str], *, start_date: date, end_date: date
    ) -> FederalPaymentArchive:
        self.calls.append((day, targets))
        self.dates.append((start_date, end_date))
        records = ()
        if day == DAYS[2]:
            records = (
                FederalArchivePayment(
                    DOCUMENT,
                    SUPPLIER,
                    "issued",
                    day,
                    Decimal("30.00"),
                    "Pagamento.csv",
                    (2,),
                    ({"Código Favorecido": SUPPLIER},),
                    is_intermediary=True,
                ),
                FederalArchivePayment(
                    DOCUMENT,
                    SUPPLIER,
                    "final",
                    day,
                    Decimal("12.00"),
                    "FavorecidosFinais.csv",
                    (2,),
                    ({"Código Favorecido": SUPPLIER},),
                    list_code="2026LC000001",
                ),
            )
        return FederalPaymentArchive(
            day,
            f"https://portal.example/{day}",
            f"https://dados.example/{day}?signed_download=temporary",
            "a" * 64,
            datetime.now(UTC),
            records,
        )


def company(cnpj: str) -> Company:
    return Company(cnpj=cnpj, normalized_name=cnpj, fingerprint=cnpj)


async def continue_run(service, run_id):
    async with service.sessions.begin() as db:
        run, _ = await create_continuation(db, run_id)
        child_id = run.id
    return child_id, await service.run(child_id)


async def test_federal_archive_replays_saved_blocks_without_another_download(database, monkeypatch):
    settings = Settings(
        _env_file=None, federal_payment_archives_per_pass=1, payment_ingestion_batch_size=1
    )
    archives = Archives()
    service = FederalCoverageService(settings, sessions=database.sessions, archives=archives)
    async with database.sessions.begin() as db:
        db.add(company(SUPPLIER))
    scope = CollectionScope("MA", DAYS[2], DAYS[2], federal_scope="acervo")
    ticket = await service.enqueue(scope, uuid4())
    persist = service.payments.persist_raw_payments
    calls = 0

    async def interrupt_second_block(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("Interrupção após salvar o primeiro bloco")
        return await persist(*args, **kwargs)

    monkeypatch.setattr(service.payments, "persist_raw_payments", interrupt_second_block)
    try:
        assert await service.run(ticket.run_id) == CrawlRunStatus.FAILED
        async with database.sessions() as db:
            item = await db.scalar(
                select(CollectionWorkItem).where(CollectionWorkItem.batch_id == ticket.batch_id)
            )
            assert item.status == "pending" and item.payload["archive_processed"] == 1
            assert await db.scalar(select(func.count()).select_from(Payment)) == 1
        assert (await continue_run(service, ticket.run_id))[1] == CrawlRunStatus.COMPLETED
        assert archives.calls == [(DAYS[2], frozenset({SUPPLIER}))]
        async with database.sessions() as db:
            batch = await db.get_one(CollectionBatch, ticket.batch_id)
            assert batch.cursors["federal"]["archives"]["records_created"] == 2
            assert batch.cursors["federal"]["archives"]["records_updated"] == 0
            assert await db.scalar(select(func.count()).select_from(Payment)) == 2
    finally:
        await service.aclose()


async def test_general_coverage_freezes_new_suppliers_and_resumes_by_day(database, monkeypatch):
    settings = Settings(_env_file=None, federal_payment_archives_per_pass=1)
    archives = Archives()
    coverage = FederalCoverageService(settings, sessions=database.sessions, archives=archives)
    collection = CompleteCollection(settings, sessions=database.sessions)
    scope = CollectionScope("MA", DAYS[0], DAYS[2], federal_scope="acervo")

    async def skip(*_args):
        return StepResult("skipped")

    async def discover_companies(*_args):
        async with database.sessions.begin() as db:
            db.add_all([company(SUPPLIER), company(SECOND)])
            await set_payment_tracking(db, SECOND, active=False)
        return StepResult("completed")

    monkeypatch.setattr(
        "app.services.ingestion.complete_collection.FederalCoverageService",
        lambda *_a, **_k: coverage,
    )
    for name in ("_procurements", "_tce", "_registries", "_municipal"):
        monkeypatch.setattr(collection, name, skip)
    monkeypatch.setattr(collection, "_contracts", discover_companies)

    root = await collection.enqueue(scope)
    await collection.run(root, scope)
    async with database.sessions() as db:
        run = await db.get_one(CrawlRun, root.run_id)
        federal = next(item for item in run.cursor["complete_stages"] if item["key"] == "federal")
        assert federal["status"] == "partial"
        batch_id = UUID(federal["batch_id"])
        batch = await db.get_one(CollectionBatch, batch_id)
        assert str(batch.id) == federal["batch_id"]
        assert batch.filters["cnpjs"] == [SUPPLIER]
        assert batch.cursors["federal"]["completed"] == 1
        first_id = batch.last_run_id
    async with database.sessions.begin() as db:
        await set_payment_tracking(db, SECOND, active=True)

    coverage = FederalCoverageService(settings, sessions=database.sessions, archives=archives)
    try:
        await coverage.resume_next()
        await coverage.resume_next()
        async with database.sessions() as db:
            batch = await db.get_one(CollectionBatch, batch_id)
            assert batch.status == "limited"
            assert batch.last_run_id != first_id
            assert batch.cursors["federal"]["empty"] == 1
            assert batch.cursors["federal"]["waiting"] == 1
            waiting_id = batch.last_run_id
        await coverage.resume_next()
        assert [day for day, _ in archives.calls] == [DAYS[2], DAYS[1]]
        archives.days.add(DAYS[0])
        _, status = await continue_run(coverage, waiting_id)
        assert status is CrawlRunStatus.COMPLETED
        assert archives.calls == [(day, frozenset({SUPPLIER})) for day in reversed(DAYS)]
        assert archives.dates == [(DAYS[0], DAYS[2])] * 3
        async with database.sessions() as db:
            batch = await db.get_one(CollectionBatch, batch_id)
            assert batch.status == "complete"
            assert batch.cursors["federal"]["archives"]["found"] == 1
            assert batch.cursors["federal"]["records_created"] == 2
            payments = (await db.scalars(select(Payment).order_by(Payment.amount))).all()
            assert [(p.federal_beneficiary_role, p.amount) for p in payments] == [
                ("final", Decimal("12.00")),
                ("issued", Decimal("30.00")),
            ]
            sources = (await db.scalars(select(SourceRecord))).all()
            assert {source.availability.value for source in sources} == {
                "available",
                "empty",
                "not_published",
            }
            assert all(
                "signed_download" not in source.raw_payload.get("resolved_url", "")
                for source in sources
            )
    finally:
        await coverage.aclose()


async def test_federal_busy_cancel_and_explicit_retry_preserve_checkpoint(database):
    class Interrupted(Archives):
        entered = asyncio.Event()
        proceed = asyncio.Event()
        rejected = True

        async def fetch(self, *args, **kwargs):
            self.entered.set()
            await self.proceed.wait()
            if self.rejected:
                raise FederalArchiveUnavailable("Arquivo oficial indisponível", http_status=403)
            return await super().fetch(*args, **kwargs)

    async with database.sessions.begin() as db:
        db.add(company(SUPPLIER))
    settings = Settings(_env_file=None)
    archive = Interrupted()
    service = FederalCoverageService(settings, sessions=database.sessions, archives=archive)
    scope = CollectionScope("MA", DAYS[2], DAYS[2], federal_scope="acervo")
    try:
        first = await service.enqueue(scope, uuid4())
        other = await service.enqueue(scope, uuid4())
        task = asyncio.create_task(service.run(first.run_id))
        await asyncio.wait_for(archive.entered.wait(), 5)
        assert await service.run(other.run_id) is CrawlRunStatus.PARTIAL
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with database.sessions() as db:
            assert (await db.get_one(CollectionBatch, first.batch_id)).status == "suspended"
            assert await db.get(JobLease, FEDERAL_LEASE) is None
            assert await db.scalar(select(func.count()).select_from(Payment)) == 0
        archive.proceed.set()
        failed_id, status = await continue_run(service, first.run_id)
        assert status is CrawlRunStatus.FAILED
        async with database.sessions() as db:
            item = await db.scalar(
                select(CollectionWorkItem).where(CollectionWorkItem.batch_id == first.batch_id)
            )
            assert item.status == "failed"
            source = await db.get_one(SourceRecord, item.source_record_id)
            assert source.http_status == 403
            assert not source.collection_complete
        archive.rejected = False
        await (
            service.resume_next()
        )  # The separate limited batch may run. The failed one is unchanged.
        async with database.sessions() as db:
            assert (await db.get_one(CollectionBatch, first.batch_id)).last_run_id == failed_id
        _, status = await continue_run(service, failed_id)
        assert status is CrawlRunStatus.COMPLETED
        async with database.sessions() as db:
            assert await db.scalar(select(func.count()).select_from(Payment)) == 2
    finally:
        await service.aclose()


async def test_federal_api_budget_uses_same_beneficiary_ledger(database):
    class UnitApi:
        calls: list[tuple[str, int]] = []

        async def aclose(self) -> None:
            pass

        async def run(self, *, company_cnpj, year, coverage_batch_id, **_kwargs):
            self.calls.append((company_cnpj, year))
            async with database.sessions.begin() as db:
                run = CrawlRun(
                    id=uuid4(),
                    connector="payments:transparencia",
                    status=CrawlRunStatus.COMPLETED,
                    filters={
                        "company_cnpj": company_cnpj,
                        "year": year,
                        "coverage_batch_id": str(coverage_batch_id),
                    },
                )
                db.add(run)
            return SimpleNamespace(crawl_run_id=run.id)

    settings = Settings(
        _env_file=None, transparencia_api_key="test", federal_payment_targets_per_pass=1
    )
    api = UnitApi()
    async with database.sessions.begin() as db:
        await set_payment_tracking(db, SUPPLIER, active=True)
        await set_payment_tracking(db, SECOND, active=True)
    service = FederalCoverageService(
        settings, sessions=database.sessions, archives=Archives(), payments=api
    )
    scope = CollectionScope("MA", DAYS[0], DAYS[2], federal_scope="acompanhadas")
    try:
        queued = await service.enqueue(scope, uuid4())
        assert await service.run(queued.run_id) is CrawlRunStatus.PARTIAL
        await service.resume_next()
        assert api.calls == [(SECOND, 2026), (SUPPLIER, 2026)]
        async with database.sessions() as db:
            batch = await db.get_one(CollectionBatch, queued.batch_id)
            assert batch.status == "complete"
            assert batch.cursors["federal"]["api"]["empty"] == 2
    finally:
        await service.aclose()


async def test_explicit_federal_retry_reuses_suspended_api_pages(database):
    class UnitApi:
        resumed: list[UUID] = []

        async def aclose(self) -> None:
            pass

        async def run(self, **_kwargs):
            pytest.fail("A consulta deve continuar o lote anterior")

        async def continue_collection(self, run_id, filters):
            self.resumed.append(UUID(filters["continuation_batch_id"]))
            async with database.sessions.begin() as db:
                run = await db.get_one(CrawlRun, run_id)
                batch = await db.get_one(CollectionBatch, run.batch_id)
                batch.status = "complete"
                run.status = CrawlRunStatus.COMPLETED
            return SimpleNamespace(crawl_run_id=run_id)

    settings = Settings(_env_file=None, transparencia_api_key="test")
    api = UnitApi()
    async with database.sessions.begin() as db:
        await set_payment_tracking(db, SUPPLIER, active=True)
    service = FederalCoverageService(
        settings, sessions=database.sessions, archives=Archives(), payments=api
    )
    scope = CollectionScope("MA", DAYS[0], DAYS[2], federal_scope="acompanhadas")
    try:
        queued = await service.enqueue(scope, uuid4())
        api_batch_id, api_run_id = uuid4(), uuid4()
        filters = {
            "connector": "payments:transparencia",
            "mode": "payments",
            "source": "transparencia",
            "coverage_batch_id": str(queued.batch_id),
            "company_cnpj": SUPPLIER,
            "year": 2026,
        }
        async with database.sessions.begin() as db:
            root = await db.get_one(CrawlRun, queued.run_id)
            await finalize_cancelled_run(db, root)
            db.add(
                CollectionBatch(
                    id=api_batch_id,
                    stream_key="test-api",
                    filters=filters,
                    status="suspended",
                    last_run_id=api_run_id,
                )
            )
            await db.flush()
            db.add_all(
                [
                    CrawlRun(
                        id=api_run_id,
                        connector=filters["connector"],
                        status=CrawlRunStatus.CANCELLED,
                        batch_id=api_batch_id,
                        records_created=1,
                        filters=filters,
                    ),
                    CollectionWorkItem(
                        batch_id=api_batch_id,
                        source="transparencia",
                        kind="record",
                        identity=DOCUMENT,
                        status="succeeded",
                        payload={},
                    ),
                ]
            )
        _, status = await continue_run(service, queued.run_id)
        assert status is CrawlRunStatus.COMPLETED
        assert api.resumed == [api_batch_id]
        async with database.sessions() as db:
            batch = await db.get_one(CollectionBatch, queued.batch_id)
            assert batch.cursors["federal"]["api"]["empty"] == 0
            assert batch.cursors["federal"]["api"]["found"] == 1
            assert batch.cursors["federal"]["api"]["records_created"] == 0
    finally:
        await service.aclose()
