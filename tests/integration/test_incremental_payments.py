"""Payment identity and staged commitments retain their financial meaning."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select

from app.config import Settings
from app.connectors.base import ConnectorResult, DataAvailability, RawPayment, RawSourceRecord
from app.models import CollectionBatch, CrawlRun, FieldChange, Payment
from app.services.ingestion.continuation import create_continuation
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payment_import import PaymentImportService
from app.services.ingestion.payments import PaymentIngestionService

SUPPLIER = "52246769000198"


async def test_failed_later_batch_keeps_committed_counts_and_queue(database):
    import pytest

    from app.models import CrawlRun, CrawlRunStatus

    class Failing(PaymentIngestionService):
        async def _persist_payment(self, session, raw, source):
            if raw.external_id == "p2":
                raise RuntimeError("failed second batch")
            return await super()._persist_payment(session, raw, source)

    settings = Settings(_env_file=None, payment_ingestion_batch_size=1)
    service = Failing(settings, session_factory=database.sessions)

    async def fetch():
        return ConnectorResult(
            data=[
                RawPayment(
                    source="manual",
                    external_id=f"p{i}",
                    supplier_name="Fornecedor",
                    amount=Decimal(i),
                )
                for i in range(1, 4)
            ],
            availability=DataAvailability.AVAILABLE,
        )

    with pytest.raises(RuntimeError, match="second batch"):
        await service.collect_result(
            {"connector": "payments:manual", "mode": "payment_import", "source": "manual"}, fetch
        )
    async with database.sessions() as session:
        run = await session.scalar(select(CrawlRun))
        assert run.status is CrawlRunStatus.FAILED
        assert run.records_created == 1 and run.records_found == 1
        assert len((await session.scalars(select(Payment))).all()) == 1
        assert (await session.get(CollectionBatch, run.batch_id)).status == "failed"
    await service.aclose()


async def test_cancelled_before_start_finalizes_without_creating_a_batch(database):
    import asyncio

    import pytest

    from app.models import CrawlRun, CrawlRunStatus

    filters = {"connector": "payments:manual", "mode": "payment_import", "source": "manual"}
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="payments:manual",
            status=CrawlRunStatus.PENDING,
            cancel_requested=True,
            filters=filters,
        )
        session.add(run)
        await session.flush()
        identifier = run.id
    service = PaymentIngestionService(session_factory=database.sessions)
    with pytest.raises(asyncio.CancelledError):
        await service.continue_collection(identifier, filters)
    async with database.sessions() as session:
        assert (await session.get(CrawlRun, identifier)).status is CrawlRunStatus.CANCELLED
    await service.aclose()


async def test_manual_official_id_updates_same_payment_and_records_history(database):
    importer = PaymentImportService(session_factory=database.sessions)
    first = await importer.import_bytes(
        f"id_pagamento;cnpj;valor\nOB1;{SUPPLIER};10,00\n".encode(),
        filename="payments.csv",
        source_name="Portal A",
    )
    second = await importer.import_bytes(
        f"id_pagamento;cnpj;valor\nOB1;{SUPPLIER};20,00\n".encode(),
        filename="payments.csv",
        source_name="Portal A",
    )
    third = await importer.import_bytes(
        f"id_pagamento;cnpj;valor\nOB1;{SUPPLIER};20,00\n".encode(),
        filename="payments.csv",
        source_name="Portal A",
    )
    assert first.created == 1 and second.updated == 1 and second.changed == 1
    assert third.unchanged == 1 and third.changed == 0
    async with database.sessions() as session:
        rows = (await session.scalars(select(Payment))).all()
        assert len(rows) == 1 and rows[0].amount == Decimal("20")
        changes = (
            await session.scalars(select(FieldChange).where(FieldChange.field_name == "amount"))
        ).all()
        assert [row.received_value for row in changes] == ["10", "20"]
    await importer.aclose()


async def test_manual_without_id_never_merges_changed_rows_by_name(database):
    importer = PaymentImportService(session_factory=database.sessions)
    for amount in (10, 20):
        result = await importer.import_bytes(
            f"fornecedor;valor\nFornecedor sem CNPJ;{amount}\n".encode(),
            filename="payments.csv",
            source_name="Portal A",
        )
        assert result.created == 1 and "sem identificador" in result.diagnostic
    async with database.sessions() as session:
        rows = (await session.scalars(select(Payment))).all()
        assert len(rows) == 2 and all(row.requires_manual_review for row in rows)
    await importer.aclose()


async def test_manual_file_continues_from_saved_rows_without_upload(database):
    settings = Settings(_env_file=None, crawl_max_records_per_source=1)
    importer = PaymentImportService(settings, session_factory=database.sessions)
    content = (
        "id_pagamento;cnpj;valor\n" + "".join(f"OB{i};{SUPPLIER};{i}\n" for i in range(1, 4))
    ).encode()
    result = await importer.import_bytes(content, filename="payments.csv", source_name="Portal A")
    assert result.created == 1 and result.crawl_run_id
    previous = UUID(result.crawl_run_id)
    for _ in range(2):
        async with database.sessions() as session:
            child, filters = await create_continuation(session, previous)
            await session.commit()
        service = PaymentIngestionService(settings, session_factory=database.sessions)
        result = await service.continue_collection(child.id, filters)
        assert result.payments_created == 1
        previous = child.id
        await service.aclose()
    async with database.sessions() as session:
        assert len((await session.scalars(select(Payment))).all()) == 3
    await importer.aclose()


async def test_cut_commitment_details_do_not_replace_aggregate_until_complete(database):
    settings = Settings(
        _env_file=None, municipal_payment_max_records=1, allow_insecure_http_sources=True
    )
    payment_service = PaymentIngestionService(settings, session_factory=database.sessions)
    aggregate = RawPayment(
        source="municipal:imperatriz",
        external_id="aggregate",
        supplier_cnpj=SUPPLIER,
        amount=Decimal("30"),
        commitment_external_id="PKEMP:1",
        status="agregado_por_empenho",
    )
    await payment_service.persist_raw_payments([aggregate])

    class Connector:
        name = "municipal:imperatriz"
        calls = 0

        async def discover_payments(self, **_kwargs):
            self.calls += 1
            raw = RawSourceRecord.build(
                source=self.name,
                endpoint="https://official.test/details",
                request_parameters={},
                raw_payload=[{"id": i, "amount": 10} for i in range(3)],
                http_status=200,
                collected_at=datetime.now(UTC),
            )
            data = [
                RawPayment(
                    source=self.name,
                    external_id=f"detail-{i}",
                    supplier_cnpj=SUPPLIER,
                    amount=Decimal("10"),
                    payment_date=datetime(2026, 1, 1, tzinfo=UTC),
                    commitment_external_id="PKEMP:1",
                    status="detalhe_empenho",
                    commitment_details_complete=True,
                    source_record_ref=raw.record_key,
                )
                for i in range(3)
            ]
            return ConnectorResult(
                data=data, raw_records=[raw], availability=DataAvailability.AVAILABLE
            )

        async def aclose(self):
            pass

    connector = Connector()
    previous = None
    batch_id = None
    for index in range(3):
        service = MunicipalPaymentIngestionService(
            settings,
            session_factory=database.sessions,
            connector=connector,
            payment_ingestion=payment_service,
        )
        child_id = None
        if previous:
            async with database.sessions() as session:
                child, _filters = await create_continuation(session, previous)
                await session.commit()
                child_id, batch_id = child.id, child.batch_id
        summary = await service.run("imperatriz", year=2026, include_details=True, run_id=child_id)
        assert summary.availability is DataAvailability.AVAILABLE, summary.diagnostic
        previous = summary.crawl_run_id
        async with database.sessions() as session:
            current = (
                await session.scalars(select(Payment).where(Payment.is_current.is_(True)))
            ).all()
            assert sum(row.amount for row in current) == Decimal("30")
            if index < 2:
                assert len(current) == 1 and current[0].status == "agregado_por_empenho"
                run = await session.get(CrawlRun, previous)
                assert (await session.get(CollectionBatch, run.batch_id)).status == "limited"
            else:
                assert len(current) == 3 and all(row.status == "detalhe_empenho" for row in current)
                assert (await session.get(CollectionBatch, batch_id)).status == "complete"
    assert connector.calls == 1
    await payment_service.aclose()
