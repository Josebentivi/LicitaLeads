"""Payment ingestion with explicit matching and provenance."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import ConnectorResult, DataAvailability, RawPayment, RawSourceRecord
from app.connectors.transparencia import TransparenciaPaymentsConnector
from app.database import async_session_factory
from app.models import (
    CollectionBatch,
    Company,
    Contract,
    CrawlRun,
    CrawlRunStatus,
    JobLease,
    Payment,
    Procurement,
    SourceRecord,
)
from app.models import DataAvailability as StoredAvailability
from app.repositories.crawls import JobLeaseRepository
from app.services.identifiers import is_valid_cnpj, normalize_cnpj, normalize_company_name
from app.services.ingestion.collection_batches import (
    CollectionCoordinator,
    latest_limited_batch,
    row_identity,
)
from app.services.ingestion.field_updates import install_audit
from app.services.ingestion.pipeline import stable_fingerprint


@dataclass(slots=True)
class PaymentIngestionSummary:
    availability: DataAvailability = DataAvailability.EMPTY
    payments_created: int = 0
    payments_updated: int = 0
    payments_changed: int = 0
    payments_unchanged: int = 0
    payments_stale_ignored: int = 0
    payments_conflicted: int = 0
    payments_linked: int = 0
    payments_unlinked: int = 0
    diagnostic: str | None = None
    crawl_run_id: UUID | None = None
    can_continue: bool = False


@dataclass(frozen=True, slots=True)
class _PaymentLease:
    name: str
    owner: str
    generation: int


class PaymentIngestionService:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        connector: TransparenciaPaymentsConnector | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self.connector = connector or TransparenciaPaymentsConnector(self.settings)
        self._collection_run: ContextVar[UUID | None] = ContextVar(
            "payment_collection_run", default=None
        )
        self._coordinator: ContextVar[CollectionCoordinator | None] = ContextVar(
            "payment_coordinator", default=None
        )
        self._lease_context: ContextVar[_PaymentLease | None] = ContextVar(
            "payment_ingestion_lease", default=None
        )

    @asynccontextmanager
    async def ingestion_lease(self) -> AsyncIterator[None]:
        """Keep imports and federal ingestion mutually exclusive with maintenance."""
        if self._lease_context.get() is not None:
            yield
            return
        owner = f"payments:{uuid4()}"
        name = owner
        now = datetime.now(UTC)
        async with self.session_factory() as session, session.begin():
            repository = JobLeaseRepository(session)
            if not await repository.acquire(
                name=name, owner_id=owner, now=now, expires_at=now + timedelta(hours=2)
            ):
                raise RuntimeError("ingestão de pagamentos bloqueada por manutenção")
            generation = int(
                await session.scalar(select(JobLease.version).where(JobLease.name == name)) or 1
            )
        lease = _PaymentLease(name, owner, generation)
        token = self._lease_context.set(lease)
        worker = asyncio.current_task()

        async def renew() -> None:
            try:
                while True:
                    await asyncio.sleep(60)
                    moment = datetime.now(UTC)
                    async with self.session_factory() as session, session.begin():
                        renewed = await JobLeaseRepository(session).renew(
                            name=name,
                            owner_id=owner,
                            generation=generation,
                            now=moment,
                            expires_at=moment + timedelta(hours=2),
                        )
                    if not renewed:
                        if worker is not None:
                            worker.cancel()
                        return
            except Exception:
                if worker is not None:
                    worker.cancel()
                raise

        heartbeat = asyncio.create_task(renew(), name=f"payment-heartbeat:{owner}")
        try:
            yield
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat
            try:
                async with self.session_factory() as session, session.begin():
                    await JobLeaseRepository(session).release(
                        name=name, owner_id=owner, generation=generation
                    )
            finally:
                self._lease_context.reset(token)

    async def assert_ingestion_fence(self, session: AsyncSession) -> None:
        lease = self._lease_context.get()
        if lease is not None and not await JobLeaseRepository(session).fence(
            name=lease.name,
            owner_id=lease.owner,
            generation=lease.generation,
            now=datetime.now(UTC),
        ):
            raise asyncio.CancelledError("lease de pagamentos perdida")

    async def run(
        self,
        *,
        company_cnpj: str,
        year: int,
        phase: str | None = None,
        resume_automatically: bool = False,
    ) -> PaymentIngestionSummary:
        filters = {
            "connector": "payments:transparencia",
            "mode": "payments",
            "source": self.connector.name,
            "company_cnpj": company_cnpj,
            "year": year,
            "phase": phase,
        }

        async def fetch() -> ConnectorResult[list[RawPayment]]:
            return await self.connector.discover_payments(
                company_cnpj=company_cnpj, year=year, phase=phase
            )

        return await self.collect_result(filters, fetch, resume_automatically=resume_automatically)

    async def continue_collection(
        self, run_id: UUID, filters: dict[str, Any]
    ) -> PaymentIngestionSummary:
        async def fetch() -> ConnectorResult[list[RawPayment]]:
            if filters.get("mode") == "payment_import":
                raise ValueError("Arquivo sem linhas pendentes para continuar")
            return await self.connector.discover_payments(
                company_cnpj=filters["company_cnpj"],
                year=int(filters["year"]),
                phase=filters.get("phase"),
            )

        return await self.collect_result(filters, fetch, run_id=run_id)

    async def _collection_checkpoint(self, session: AsyncSession) -> None:
        await self.assert_ingestion_fence(session)
        run_id = self._collection_run.get()
        if run_id:
            run = await session.get(CrawlRun, run_id)
            if (
                run is None
                or run.cancel_requested
                or run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
            ):
                raise asyncio.CancelledError("coleta de pagamentos encerrada")

    async def collect_result(
        self,
        filters: dict[str, Any],
        fetch: Callable[[], Awaitable[ConnectorResult[list[RawPayment]]]],
        *,
        run_id: UUID | None = None,
        resume_automatically: bool = False,
    ) -> PaymentIngestionSummary:
        async with self.ingestion_lease():
            lease = self._lease_context.get()
            assert lease is not None
            batch_id = (
                UUID(str(filters["continuation_batch_id"]))
                if filters.get("continuation_batch_id")
                else None
            )
            if batch_id is None and resume_automatically:
                batch_id = await latest_limited_batch(self.session_factory, filters)
            async with self.session_factory() as session, session.begin():
                await self.assert_ingestion_fence(session)
                run = await session.get(CrawlRun, run_id) if run_id else None
                if run is None:
                    run = CrawlRun(
                        connector=filters["connector"],
                        status=CrawlRunStatus.RUNNING,
                        filters={**filters, "lease_name": lease.name},
                        counting_version=2,
                        records_changed=0,
                        records_unchanged=0,
                        records_stale_ignored=0,
                        records_conflicted=0,
                        batch_id=batch_id,
                        started_at=datetime.now(UTC),
                        heartbeat_at=datetime.now(UTC),
                    )
                    session.add(run)
                    await session.flush()
                else:
                    run.filters = {**filters, "lease_name": lease.name}
                    run.status = CrawlRunStatus.RUNNING
                    run.started_at = datetime.now(UTC)
                run_id = run.id
            token = self._collection_run.set(run_id)
            coordinator = CollectionCoordinator(
                self.session_factory, run_id, self._collection_checkpoint
            )
            coordinator_token = self._coordinator.set(coordinator)
            summary = PaymentIngestionSummary(crawl_run_id=run_id)
            status = CrawlRunStatus.COMPLETED
            try:
                await coordinator.start(filters, filters, batch_id)
                if filters.get("mode") != "payment_import":
                    await coordinator.attach(self.connector)
                source = filters["source"]
                result = await coordinator.pending_result(source, RawPayment)
                if result is None:
                    result = await fetch()
                    async with self.session_factory() as session, session.begin():
                        await self._collection_checkpoint(session)
                        ids = []
                        for raw in result.raw_records:
                            record = await self._source_record(
                                session,
                                raw,
                                result.availability,
                                result.diagnostic,
                                collection_complete=result.is_complete,
                                pagination_metadata=result.pagination.model_dump()
                                if result.pagination
                                else None,
                            )
                            ids.append(record.id)
                    attached = self._attach_source_records(result.data, result.raw_records, ids)
                    result = result.model_copy(update={"data": attached})
                    await coordinator.stage(
                        source,
                        result,
                        {
                            raw.record_key: value
                            for raw, value in zip(result.raw_records, ids, strict=True)
                        },
                    )
                records = await coordinator.select_records(source, RawPayment)
                limit = self.settings.crawl_max_records_per_source
                selected = await coordinator.payment_slice(source, records, limit)
                summary = await self.persist_raw_payments(
                    selected, checkpoint=self._collection_checkpoint
                )
                summary.crawl_run_id = run_id
                summary.availability = result.availability
                summary.diagnostic = result.diagnostic
                if limit and len(records) > limit:
                    summary.can_continue = True
                    summary.diagnostic = (
                        summary.diagnostic + "; " if summary.diagnostic else ""
                    ) + f"limite atingido: {len(records) - len(selected)} registro(s) pendentes"
                if not result.is_complete or summary.can_continue:
                    status = CrawlRunStatus.PARTIAL
                if result.availability not in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}:
                    status = CrawlRunStatus.FAILED
            except asyncio.CancelledError:
                status = CrawlRunStatus.CANCELLED
                summary.diagnostic = "coleta de pagamentos cancelada"
                raise
            except Exception as exc:
                status = CrawlRunStatus.FAILED
                summary.availability = DataAvailability.TEMPORARY_ERROR
                summary.diagnostic = str(exc)
                raise
            finally:
                try:
                    async with self.session_factory() as session, session.begin():
                        await self.assert_ingestion_fence(session)
                        run = await session.get_one(CrawlRun, run_id)
                        if run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                            # Counts are committed with each domain batch, including on failure.
                            summary.payments_created = run.records_created or 0
                            summary.payments_updated = run.records_updated or 0
                            summary.payments_changed = run.records_changed or 0
                            summary.payments_unchanged = run.records_unchanged or 0
                            summary.payments_stale_ignored = run.records_stale_ignored or 0
                            summary.payments_conflicted = run.records_conflicted or 0
                            if coordinator.batch_id is None:
                                coordinator.batch_id = run.batch_id
                            if coordinator.batch_id is not None:
                                await coordinator.finish_in_session(session, status)
                                batch = await session.get_one(CollectionBatch, coordinator.batch_id)
                                summary.can_continue = batch.status in {
                                    "limited",
                                    "failed",
                                    "suspended",
                                }
                                if batch.status == "limited" and status is CrawlRunStatus.COMPLETED:
                                    status = CrawlRunStatus.PARTIAL
                            run.status, run.finished_at = status, datetime.now(UTC)
                            run.records_created, run.records_updated = (
                                summary.payments_created,
                                summary.payments_updated,
                            )
                            run.records_found = summary.payments_created + summary.payments_updated
                            run.records_changed, run.records_unchanged = (
                                summary.payments_changed,
                                summary.payments_unchanged,
                            )
                            run.records_stale_ignored, run.records_conflicted = (
                                summary.payments_stale_ignored,
                                summary.payments_conflicted,
                            )
                            run.diagnostic = summary.diagnostic
                finally:
                    self._collection_run.reset(token)
                    self._coordinator.reset(coordinator_token)
            return summary

    @staticmethod
    def _attach_source_records(
        payments: list[RawPayment],
        raw_records: list[RawSourceRecord],
        source_ids: list[UUID],
    ) -> list[RawPayment]:
        """Resolve each payment to the response that actually contains its row."""

        mapping = {
            raw.record_key: source_id
            for raw, source_id in zip(raw_records, source_ids, strict=False)
        }
        fallback = source_ids[0] if len(source_ids) == 1 else None
        attached: list[RawPayment] = []
        source_id: UUID | None
        for payment in payments:
            if payment.source_record_ref:
                if payment.source_record_ref not in mapping:
                    raise ValueError("source_record_ref sem resposta correspondente")
                source_id = mapping[payment.source_record_ref]
            else:
                if payment.source_record_id is None and len(source_ids) > 1:
                    raise ValueError("proveniência ambígua do pagamento")
                source_id = payment.source_record_id or fallback
            observed = next(
                (
                    raw.collected_at
                    for raw in raw_records
                    if raw.record_key == payment.source_record_ref
                ),
                None,
            )
            attached.append(
                payment.model_copy(
                    update={
                        "source_record_id": source_id,
                        "observed_at": observed or datetime.now(UTC),
                    }
                )
            )
        return attached

    async def persist_raw_payments(
        self,
        payments: list[RawPayment],
        *,
        source_record_id: UUID | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        session: AsyncSession | None = None,
        checkpoint: Callable[[AsyncSession], Awaitable[None]] | None = None,
    ) -> PaymentIngestionSummary:
        """Persist already-normalized payments, sharing the matching rules.

        ``session`` allows the caller to replace aggregates and persist the new
        detail rows in one atomic transaction.
        """

        total = len(payments)
        if session is None and self._lease_context.get() is None:
            async with self.ingestion_lease():
                return await self.persist_raw_payments(
                    payments,
                    source_record_id=source_record_id,
                    progress_callback=progress_callback,
                    checkpoint=checkpoint,
                )
        if progress_callback is not None:
            progress_callback(0, total)
        if session is not None:
            return await self._persist_in_session(
                session,
                payments,
                source_record_id=source_record_id,
                progress_callback=progress_callback,
                checkpoint=checkpoint,
            )
        summary = PaymentIngestionSummary()
        batch_size = self.settings.payment_ingestion_batch_size
        for offset in range(0, total, batch_size):
            async with self.session_factory() as own_session, own_session.begin():
                batch = await self._persist_in_session(
                    own_session,
                    payments[offset : offset + batch_size],
                    source_record_id=source_record_id,
                    progress_callback=None,
                    checkpoint=checkpoint,
                )
            for key in (
                "payments_created",
                "payments_updated",
                "payments_changed",
                "payments_unchanged",
                "payments_stale_ignored",
                "payments_conflicted",
                "payments_linked",
                "payments_unlinked",
            ):
                setattr(summary, key, getattr(summary, key) + getattr(batch, key))
            if progress_callback is not None:
                progress_callback(min(offset + batch_size, total), total)
        return summary

    async def _persist_in_session(
        self,
        session: AsyncSession,
        payments: list[RawPayment],
        *,
        source_record_id: UUID | None,
        progress_callback: Callable[[int, int], None] | None,
        checkpoint: Callable[[AsyncSession], Awaitable[None]] | None = None,
    ) -> PaymentIngestionSummary:
        summary = PaymentIngestionSummary()
        session.info["crawl_run_id"] = self._collection_run.get() or session.info.get(
            "crawl_run_id"
        )
        total = len(payments)
        for index, payment in enumerate(payments, start=1):
            await self.assert_ingestion_fence(session)
            if checkpoint is not None:
                await checkpoint(session)
            payment_source: SourceRecord | None = None
            linked_source_id = payment.source_record_id or source_record_id
            if linked_source_id:
                payment_source = await session.get(SourceRecord, linked_source_id)
            tracker = install_audit(
                session,
                payment,
                run_id=session.info.get("crawl_run_id"),
                source_record_id=linked_source_id,
                complete=bool(payment_source and payment_source.collection_complete),
            )
            created, linked = await self._persist_payment(session, payment, payment_source)
            if coordinator := session.info.get("collection_coordinator") or self._coordinator.get():
                work_id = getattr(payment, "collection_work_id", None)
                coordinator.current_item_id = (
                    UUID(str(work_id)) if work_id else coordinator.items.get(row_identity(payment))
                )
                await coordinator.confirm(session, payment)
            tracker.outcome.count(summary, created, prefix="payments")
            run_id = session.info.get("crawl_run_id")
            if run_id:
                run = await session.get(CrawlRun, run_id)
                if run is not None and run.status in {
                    CrawlRunStatus.PENDING,
                    CrawlRunStatus.RUNNING,
                }:
                    run.counting_version = 2
                    run.records_created = (run.records_created or 0) + int(created)
                    run.records_updated = (run.records_updated or 0) + int(not created)
                    run.records_found = run.records_created + run.records_updated
                    run.records_changed = (run.records_changed or 0) + int(
                        not created and tracker.outcome.changed
                    )
                    run.records_unchanged = (run.records_unchanged or 0) + int(
                        not created and not tracker.outcome.changed
                    )
                    run.records_stale_ignored = (run.records_stale_ignored or 0) + int(
                        tracker.outcome.stale
                    )
                    run.records_conflicted = (run.records_conflicted or 0) + int(
                        tracker.outcome.conflicted
                    )
                    run.heartbeat_at = datetime.now(UTC)

            summary.payments_created += int(created)
            summary.payments_updated += int(not created)
            summary.payments_linked += int(linked)
            summary.payments_unlinked += int(not linked)
            if progress_callback is not None and (index % 25 == 0 or index == total):
                progress_callback(index, total)
        return summary

    async def _source_record(
        self,
        session: AsyncSession,
        raw: RawSourceRecord,
        availability: DataAvailability,
        diagnostic: str | None,
        *,
        collection_complete: bool | None = None,
        pagination_metadata: dict[str, Any] | None = None,
    ) -> SourceRecord:
        await self.assert_ingestion_fence(session)
        fingerprint = stable_fingerprint(
            "payment_source", raw.source, raw.record_key, collection_complete, pagination_metadata
        )
        existing = await session.scalar(
            select(SourceRecord).where(SourceRecord.fingerprint == fingerprint)
        )
        if existing is not None:
            return existing
        stored = StoredAvailability.AVAILABLE
        try:
            stored = StoredAvailability(availability.value)
        except ValueError:
            pass
        entity = SourceRecord(
            crawl_run_id=None,
            source=raw.source,
            endpoint=raw.endpoint,
            request_parameters=raw.request_parameters,
            response_hash=raw.response_hash,
            raw_payload=raw.raw_payload,
            http_status=raw.http_status,
            availability=stored,
            collected_at=raw.collected_at or datetime.now(UTC),
            diagnostic=diagnostic,
            collection_complete=collection_complete,
            pagination_metadata=pagination_metadata,
            fingerprint=fingerprint,
        )
        session.add(entity)
        await session.flush()
        return entity

    async def _persist_payment(
        self,
        session: AsyncSession,
        raw: RawPayment,
        source: SourceRecord | None,
    ) -> tuple[bool, bool]:
        raw_document = normalize_cnpj(raw.supplier_cnpj)
        cnpj = raw_document if is_valid_cnpj(raw_document) else None
        company = await self._company(session, cnpj, raw.supplier_name)
        contract, confidence, match_reason, ambiguous = await self._match_contract(
            session, raw, cnpj
        )
        procurement = await self._match_procurement(session, raw)
        fingerprint = stable_fingerprint("payment", raw.source, raw.external_id)
        payment = await session.scalar(select(Payment).where(Payment.fingerprint == fingerprint))
        created = payment is None
        if payment is None:
            payment = Payment(
                source=raw.source,
                external_id=raw.external_id,
                fingerprint=fingerprint,
            )
            session.add(payment)
        for name, value in {
            "contract_id": contract.id if contract else None,
            "procurement_id": procurement.id if procurement else None,
            "company_id": company.id if company else None,
            "source_record_id": source.id if source else None,
            "contract_number": raw.contract_number,
            "procurement_control_number": raw.procurement_control_number,
            "agency_name": raw.agency_name,
            "agency_cnpj": normalize_cnpj(raw.agency_cnpj),
            "uasg": raw.uasg,
            "process_number": raw.process_number,
            "payment_date": raw.payment_date,
            "amount": raw.amount,
            "phase": raw.phase,
            "document_number": raw.document_number,
            "commitment_number": raw.commitment_number,
            "commitment_external_id": raw.commitment_external_id,
            "is_current": bool(getattr(raw, "is_current", True)),
            "status": raw.status,
            "supplier_cnpj": cnpj,
            "supplier_name": raw.supplier_name,
            "source_url": raw.source_url,
            "match_status": (
                "linked"
                if contract is not None
                else (
                    "ambiguous"
                    if ambiguous
                    else ("procurement_linked" if procurement is not None else "unlinked")
                )
            ),
            "match_confidence": confidence,
            "match_reason": match_reason,
            "requires_manual_review": contract is None,
        }.items():
            if value is not None:
                setattr(payment, name, value)
        # Never keep a stale link when a later run cannot reproduce the match.
        payment.contract_id = contract.id if contract else None
        payment.procurement_id = procurement.id if procurement else None
        payment.company_id = company.id if company else None
        payment.match_confidence = confidence
        payment.match_reason = match_reason
        payment.requires_manual_review = contract is None
        await session.flush()
        return created, contract is not None

    async def _company(
        self, session: AsyncSession, cnpj: str | None, name: str | None
    ) -> Company | None:
        if cnpj and is_valid_cnpj(cnpj):
            company = await session.scalar(select(Company).where(Company.cnpj == cnpj))
            if company is None:
                company = Company(
                    cnpj=cnpj,
                    legal_name=name,
                    normalized_name=normalize_company_name(name) or cnpj,
                    fingerprint=f"company:{cnpj}",
                )
                session.add(company)
                await session.flush()
            elif name:
                if not company.legal_name:
                    company.legal_name = name
                if not company.normalized_name or company.normalized_name == cnpj:
                    company.normalized_name = normalize_company_name(name) or cnpj
            return company
        return None

    async def _match_contract(
        self, session: AsyncSession, raw: RawPayment, cnpj: str | None
    ) -> tuple[Contract | None, Decimal | None, str, bool]:
        """Match only an unambiguous contract and a proven supplier."""

        agency_cnpj = normalize_cnpj(raw.agency_cnpj)
        contract_date_missing = bool(raw.contract_number and cnpj and raw.payment_date is None)
        if raw.contract_number and cnpj and raw.payment_date is not None:
            base = select(Contract).where(
                Contract.contract_number == raw.contract_number,
                Contract.supplier_cnpj == cnpj,
            )
            if agency_cnpj:
                base = base.where(Contract.agency_cnpj == agency_cnpj)
            # Count only contracts valid on the documented payment day.  A limit
            # before this filter can hide a second valid contract and turn an
            # ambiguous match into an apparently exact one.
            day_start = datetime.combine(raw.payment_date.date(), datetime.min.time(), tzinfo=UTC)
            day_end = datetime.combine(raw.payment_date.date(), datetime.max.time(), tzinfo=UTC)
            base = base.where(
                or_(Contract.valid_from.is_(None), Contract.valid_from <= day_end),
                or_(Contract.valid_until.is_(None), Contract.valid_until >= day_start),
            )
            candidates = list((await session.scalars(base.limit(2))).all())
            if not candidates:
                return (
                    None,
                    None,
                    "contract number found but supplier/year does not match",
                    False,
                )
            valid = [
                contract
                for contract in candidates
                if self._contract_covers_payment(contract, raw.payment_date)
            ]
            if len(valid) == 1:
                return (
                    valid[0],
                    Decimal("0.99"),
                    "contract_number+validity+agency+supplier",
                    False,
                )
            if len(valid) > 1:
                return None, None, "multiple valid contracts share number/agency/supplier", True
            return None, None, "payment date is outside contract validity", False
        if raw.contract_number and not cnpj:
            return (
                None,
                None,
                "contract number without supplier CNPJ; manual review required",
                False,
            )
        if raw.process_number and cnpj:
            base = select(Contract).where(
                Contract.process_number == raw.process_number,
                Contract.supplier_cnpj == cnpj,
            )
            if agency_cnpj:
                base = base.where(Contract.agency_cnpj == agency_cnpj)
            matches = list((await session.scalars(base.limit(2))).all())
            if len(matches) == 1:
                return matches[0], Decimal("0.85"), "process+supplier", False
            if len(matches) > 1:
                return None, None, "multiple contracts share process/supplier", True
        if contract_date_missing:
            return (
                None,
                None,
                "contract number without proven payment date; manual review required",
                False,
            )
        return None, None, "no exact contract match", False

    @staticmethod
    def _contract_covers_payment(contract: Contract, payment_date: datetime) -> bool:
        payment_day = payment_date.date()
        if contract.valid_from is not None and payment_day < contract.valid_from.date():
            return False
        if contract.valid_until is not None and payment_day > contract.valid_until.date():
            return False
        return True

    async def _match_procurement(
        self, session: AsyncSession, raw: RawPayment
    ) -> Procurement | None:
        if raw.procurement_control_number:
            return await session.scalar(
                select(Procurement).where(
                    Procurement.pncp_control_number == raw.procurement_control_number
                )
            )
        return None

    async def aclose(self) -> None:
        await self.connector.aclose()
