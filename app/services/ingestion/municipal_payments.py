"""Auditable ingestion of payments from municipal transparency portals."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import ConnectorResult, DataAvailability, RawPayment, RawSourceRecord
from app.connectors.municipal_payments import build_municipal_connector
from app.connectors.municipal_payments.base import MunicipalPaymentConnector
from app.connectors.municipal_payments.sources import (
    get_municipal_source,
    selected_municipal_sources,
)
from app.connectors.progress import ProgressEvent
from app.database import async_session_factory, write_session
from app.models import CollectionBatch, CrawlRun, CrawlRunStatus, JobLease, Payment, SourceRecord
from app.models import DataAvailability as StoredAvailability
from app.repositories.crawls import JobLeaseRepository
from app.services.crawl_progress import STAGE_LABELS, CrawlProgressRecorder
from app.services.ingestion.collection_batches import CollectionCoordinator, latest_limited_batch
from app.services.ingestion.field_updates import install_audit
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import stable_fingerprint

MunicipalProgressCallback = Callable[[dict[str, Any]], None]

_municipal_tasks: dict[UUID, asyncio.Task[None]] = {}
_MUNICIPAL_WRITE_LIMIT = 50


def _payment_blocks(payments: list[RawPayment], limit: int) -> Iterator[list[RawPayment]]:
    """Bound write transactions without splitting the representation of a commitment."""
    groups: dict[tuple[str, str] | int, list[RawPayment]] = {}
    for index, row in enumerate(payments):
        key = (row.source, row.commitment_external_id) if row.commitment_external_id else index
        groups.setdefault(key, []).append(row)

    block: list[RawPayment] = []
    for group in groups.values():
        if block and len(block) + len(group) > limit:
            yield block
            block = []
        block.extend(group)
    if block:
        yield block


def register_municipal_task(run_id: UUID, task: asyncio.Task[None]) -> None:
    _municipal_tasks[run_id] = task


def discard_municipal_task(run_id: UUID) -> None:
    _municipal_tasks.pop(run_id, None)


def cancel_municipal_task(run_id: UUID) -> bool:
    task = _municipal_tasks.get(run_id)
    if task is None or task.done():
        return False
    task.cancel()
    return True


async def cancel_all_municipal_tasks() -> None:
    """Cancel and drain municipal background tasks during shutdown."""

    tasks = [task for task in _municipal_tasks.values() if not task.done()]
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    _municipal_tasks.clear()


@dataclass(slots=True)
class MunicipalPaymentSummary:
    source_id: str
    source_name: str
    availability: DataAvailability = DataAvailability.EMPTY
    payments_created: int = 0
    payments_updated: int = 0
    payments_changed: int = 0
    payments_unchanged: int = 0
    payments_stale_ignored: int = 0
    payments_conflicted: int = 0
    payments_linked: int = 0
    payments_unlinked: int = 0
    records_found: int = 0
    crawl_run_id: UUID | None = None
    diagnostic: str | None = None


@dataclass(slots=True)
class _MunicipalExecution:
    generation: int | None = None
    run_id: UUID | None = None
    lease: str | None = None
    owner: str | None = None
    worker: asyncio.Task[Any] | None = None
    progress: CrawlProgressRecorder | None = None
    coordinator: CollectionCoordinator | None = None


def municipal_payment_lease_name(source_id: str) -> str:
    return f"municipal:{source_id}"


class MunicipalPaymentIngestionService:
    """Run one municipal adapter and persist raw responses before payments."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        connector: MunicipalPaymentConnector | None = None,
        payment_ingestion: PaymentIngestionService | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self.connector = connector
        self._execution_context: ContextVar[_MunicipalExecution | None] = ContextVar(
            "municipal_execution", default=None
        )
        self.payment_ingestion = payment_ingestion or PaymentIngestionService(
            self.settings, session_factory=self.session_factory
        )

    def _execution(self) -> _MunicipalExecution:
        return self._execution_context.get() or _MunicipalExecution()

    async def run(
        self,
        source_id: str,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool | None = None,
        run_id: UUID | None = None,
        progress_callback: MunicipalProgressCallback | None = None,
        resume_automatically: bool = False,
    ) -> MunicipalPaymentSummary:
        token = self._execution_context.set(_MunicipalExecution())
        try:
            return await self._run(
                source_id,
                year=year,
                company_cnpj=company_cnpj,
                include_details=include_details,
                run_id=run_id,
                progress_callback=progress_callback,
                resume_automatically=resume_automatically,
            )
        finally:
            self._execution_context.reset(token)

    async def _run(
        self,
        source_id: str,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool | None = None,
        run_id: UUID | None = None,
        progress_callback: MunicipalProgressCallback | None = None,
        resume_automatically: bool = False,
    ) -> MunicipalPaymentSummary:
        self._execution().run_id = None
        self._execution().lease = self._execution().owner = None
        self._execution().generation = None
        source = get_municipal_source(source_id)
        include = (
            self.settings.municipal_payment_include_details
            if include_details is None
            else include_details
        )
        unsupported_details = include and not source.supports_details
        if unsupported_details:
            include = False
        batch_id = None
        collection_filters = {
            "connector": f"municipal:{source.id}",
            "mode": "municipal_payments",
            "source_id": source.id,
            "year": year,
            "company_cnpj": company_cnpj,
            "include_details": include,
        }
        if run_id is None and resume_automatically:
            batch_id = await latest_limited_batch(self.session_factory, collection_filters)
        if run_id is None:
            run_id = await self.create_run(source_id, year, company_cnpj, include)
        if batch_id is None:
            async with self.session_factory() as session:
                saved_run = await session.get_one(CrawlRun, run_id)
                batch_id = saved_run.batch_id
        summary = MunicipalPaymentSummary(
            source_id=source.id,
            source_name=source.name,
            crawl_run_id=run_id,
        )
        if (
            source.base_url.lower().startswith("http://")
            and not self.settings.allow_insecure_http_sources
        ):
            summary.availability = DataAvailability.NOT_SUPPORTED
            summary.diagnostic = (
                f"{source.name}: fonte publica apenas HTTP; "
                "defina ALLOW_INSECURE_HTTP_SOURCES=true explicitamente para usa-la"
            )
            await self._finish_run(run_id, summary, CrawlRunStatus.FAILED)
            return summary
        lease_name = self._lease_name(source.id)
        owner = f"municipal:{run_id}"
        try:
            if not await self._acquire_lease(lease_name, owner):
                summary.diagnostic = "já existe coleta em andamento para esta fonte"
                await self._finish_run(run_id, summary, CrawlRunStatus.FAILED)
                return summary
        except (asyncio.CancelledError, Exception) as exc:
            cleanup = asyncio.create_task(
                self._abort_admission(
                    run_id, lease_name, owner, cancelled=isinstance(exc, asyncio.CancelledError)
                )
            )
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
            raise
        self._execution().run_id, self._execution().lease, self._execution().owner = (
            run_id,
            lease_name,
            owner,
        )
        self._execution().worker = asyncio.current_task()
        self._execution().progress = CrawlProgressRecorder()
        heartbeat = asyncio.create_task(
            self._heartbeat_lease(lease_name, owner, run_id),
            name=f"municipal-heartbeat:{run_id}",
        )
        connector = self.connector
        try:
            await self._set_run_progress(
                run_id,
                stage="discovery",
                message="Consultando o portal oficial...",
                progress_callback=progress_callback,
            )
            if connector is None:
                connector = build_municipal_connector(source.id, self.settings)
            connector_name = getattr(connector, "name", f"municipal:{source.id}")
            coordinator = CollectionCoordinator(self.session_factory, run_id, self._assert_fence)
            await coordinator.start(collection_filters, collection_filters, batch_id)
            self._execution().coordinator = coordinator
            await coordinator.attach(connector)
            if hasattr(connector, "settings"):
                connector.settings = self.settings.model_copy(
                    update={"municipal_payment_max_records": 0}
                )
            transport = getattr(connector, "_http", None)
            if transport is not None:
                transport.checkpoint = self._checkpoint
                transport.on_activity = self._report_activity
            if hasattr(connector, "checkpoint"):
                connector.checkpoint = self._checkpoint
            if hasattr(connector, "on_activity"):
                connector.on_activity = self._report_activity
            await self._checkpoint()
            result = await coordinator.pending_result(
                connector_name, RawPayment, include_raw_records=False
            )
            replaying = result is not None
            if result is None:
                result = await connector.discover_payments(
                    year=year, company_cnpj=company_cnpj, include_details=include
                )
            summary.availability = result.availability
            summary.records_found = len(result.data)
            summary.diagnostic = result.diagnostic
            if unsupported_details:
                summary.diagnostic = (
                    f"{summary.diagnostic}; detalhes n?o suportados por esta fonte"
                    if summary.diagnostic
                    else "detalhes n?o suportados por esta fonte"
                )
            await self._set_run_progress(
                run_id,
                stage="preparing",
                message="Preparando registros recebidos...",
                total=len(result.data),
                progress_callback=progress_callback,
            )
            if await self._cancel_requested(run_id):
                raise asyncio.CancelledError
            if replaying:
                pending_payments = result.data
            else:
                source_record_ids = await self._persist_source_records(run_id, result)
                attached = self._attach_source_records(
                    result.data, result.raw_records, source_record_ids
                )
                await coordinator.stage(
                    connector_name,
                    result.model_copy(update={"data": attached}),
                    {
                        raw.record_key: value
                        for raw, value in zip(result.raw_records, source_record_ids, strict=True)
                    },
                )
                pending_payments = await coordinator.select_records(connector_name, RawPayment)
            limit = self.settings.municipal_payment_max_records
            payments = await coordinator.payment_slice(connector_name, pending_payments, limit)
            if limit and len(pending_payments) > limit:
                summary.diagnostic = (
                    (summary.diagnostic + "; " if summary.diagnostic else "")
                    + f"limite atingido; {len(pending_payments) - len(payments)} pagamento(s) pendentes"
                )
                result = result.model_copy(update={"partial": True})

            await self._set_run_progress(
                run_id,
                stage="persisting",
                message="Gravando pagamentos em blocos...",
                total=len(payments),
                progress_callback=progress_callback,
            )
            processed, needs_review = await self._write_payment_blocks(
                run_id, coordinator, payments, year, progress_callback
            )
            if needs_review:
                result.partial = True
                review = "detalhes incompletos ou identidade legada ambígua; histórico preservado para revisão"
                summary.diagnostic = (
                    f"{summary.diagnostic}; {review}" if summary.diagnostic else review
                )
            await self._set_run_progress(
                run_id,
                stage="finalizing",
                message="Finalizando coleta...",
                processed=processed,
                total=len(payments),
                progress_callback=progress_callback,
            )
            status = (
                CrawlRunStatus.PARTIAL
                if result.availability in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}
                and not result.is_complete
                else (
                    CrawlRunStatus.COMPLETED
                    if result.availability in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}
                    else CrawlRunStatus.FAILED
                )
            )
            await self._finish_run(run_id, summary, status)
        except asyncio.CancelledError:
            summary.diagnostic = "coleta cancelada pelo operador"
            await self._finish_run(run_id, summary, CrawlRunStatus.CANCELLED, progress_callback)
            raise
        except Exception as exc:  # noqa: BLE001 - the run must record the failure
            summary.availability = DataAvailability.TEMPORARY_ERROR
            summary.diagnostic = f"{type(exc).__name__}: {exc}"
            await self._finish_run(run_id, summary, CrawlRunStatus.FAILED, progress_callback)
        finally:
            recorder = self._execution().progress
            if recorder is not None:
                await recorder.aclose()
            heartbeat.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat
            try:
                if connector is not None:
                    await connector.aclose()
            finally:
                await self._release_lease(lease_name, owner)
        return summary

    async def create_run(
        self,
        source_id: str,
        year: int,
        company_cnpj: str | None,
        include_details: bool,
    ) -> UUID:
        """Create the auditable run before the background task starts."""

        created: list[UUID] = []
        try:
            return await self._create_run_record(
                source_id, year, company_cnpj, include_details, created
            )
        except asyncio.CancelledError:
            if created:
                cleanup = asyncio.create_task(
                    self._abort_admission(
                        created[0],
                        self._lease_name(source_id),
                        f"municipal:{created[0]}",
                        cancelled=True,
                    )
                )
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await cleanup
            raise

    async def _create_run_record(
        self,
        source_id: str,
        year: int,
        company_cnpj: str | None,
        include_details: bool,
        created: list[UUID],
    ) -> UUID:

        async with self.session_factory() as session, session.begin():
            await JobLeaseRepository(session).assert_writes_allowed(now=datetime.now(UTC))
            run = CrawlRun(
                connector=f"municipal:{source_id}",
                status=CrawlRunStatus.RUNNING,
                started_at=datetime.now(UTC),
                filters={
                    "connector": f"municipal:{source_id}",
                    "mode": "municipal_payments",
                    "source_id": source_id,
                    "year": year,
                    "company_cnpj": company_cnpj,
                    "include_details": include_details,
                },
                heartbeat_at=datetime.now(UTC),
                cursor={
                    "stage": "queued",
                    "message": "Aguardando início...",
                    "processed": 0,
                    "total": None,
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
            session.add(run)
            await session.flush()
            created.append(run.id)
            return run.id

    async def _cancel_requested(self, run_id: UUID) -> bool:
        async with self.session_factory() as session:
            return bool(
                await session.scalar(select(CrawlRun.cancel_requested).where(CrawlRun.id == run_id))
            )

    async def _write_payment_blocks(
        self,
        run_id: UUID,
        coordinator: CollectionCoordinator,
        payments: list[RawPayment],
        year: int,
        progress_callback: MunicipalProgressCallback | None,
    ) -> tuple[int, bool]:
        processed = 0
        total = len(payments)
        needs_review = False
        limit = min(self.settings.payment_ingestion_batch_size, _MUNICIPAL_WRITE_LIMIT)
        recorder = self._execution().progress
        message = "Gravando pagamentos em blocos..."
        for block in _payment_blocks(payments, limit):
            async with write_session(self.session_factory) as session:
                await self._assert_fence(session)
                run = await session.get_one(CrawlRun, run_id)
                records_found = run.records_found
                session.info["crawl_run_id"] = run_id
                session.info["collection_coordinator"] = coordinator
                reconciled = await self._supersede_aggregates(session, block, year=year)
                current_groups = {
                    (row.source, row.commitment_external_id)
                    for row in reconciled
                    if getattr(row, "is_current", True)
                }
                needs_review = needs_review or any(
                    not getattr(row, "is_current", True)
                    and (
                        row.status == "detalhe_empenho"
                        or (row.source, row.commitment_external_id) not in current_groups
                    )
                    for row in reconciled
                )
                persisted = await self.payment_ingestion.persist_raw_payments(
                    reconciled, session=session, checkpoint=self._assert_fence
                )
                now = datetime.now(UTC)
                run.records_found = records_found
                processed = run.records_created + run.records_updated
                run.heartbeat_at = now
                payload = {
                    "stage": "persisting",
                    "message": message,
                    "processed": processed,
                    "total": total,
                    "updated_at": now.isoformat(),
                }
                cursor = dict(run.cursor or {})
                cursor.update(payload)
                cursor["municipal_write"] = {"processed": processed, "total": total}
                cursor["progress"] = {**payload, "source": self._execution().lease}
                cursor["payments_linked"] = (
                    int(cursor.get("payments_linked") or 0) + persisted.payments_linked
                )
                cursor["payments_unlinked"] = (
                    int(cursor.get("payments_unlinked") or 0) + persisted.payments_unlinked
                )
                run.cursor = cursor
            if recorder is not None:
                recorder.record(
                    ProgressEvent(
                        stage="persisting",
                        source=self._execution().lease,
                        message=message,
                        processed=processed,
                        total=total,
                    )
                )
            if progress_callback is not None:
                progress_callback(payload)
        return processed, needs_review

    async def _set_run_progress(
        self,
        run_id: UUID,
        *,
        stage: str,
        message: str,
        processed: int = 0,
        total: int | None = None,
        progress_callback: MunicipalProgressCallback | None = None,
    ) -> None:
        recorder = self._execution().progress
        if recorder is not None:
            await recorder.aclose()
            recorder.record(
                ProgressEvent(
                    stage=stage,
                    source=self._execution().lease,
                    message=message,
                    processed=processed,
                    total=total,
                )
            )
        payload = {
            "stage": stage,
            "message": message,
            "processed": processed,
            "total": total,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            run = await session.get(CrawlRun, run_id)
            if run is None:
                return
            cursor = dict(run.cursor or {})
            cursor.update(payload)
            if stage == "persisting":
                cursor["municipal_write"] = {"processed": processed, "total": total}
            run.cursor = cursor
            if stage == "preparing" and total is not None:
                run.records_found = total
            if recorder is not None:
                recorder.apply(run)
            run.heartbeat_at = datetime.now(UTC)
        if progress_callback is not None:
            progress_callback(payload)

    async def _report_activity(self, event: ProgressEvent) -> None:
        recorder = self._execution().progress
        if recorder is not None:
            await recorder.observe(event, self._persist_activity)

    async def _persist_activity(self) -> None:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            run = await session.get(CrawlRun, self._execution().run_id)
            recorder = self._execution().progress
            if run is not None and recorder is not None:
                recorder.apply(run)
                run.heartbeat_at = datetime.now(UTC)

    async def _supersede_aggregates(
        self,
        session: AsyncSession,
        payments: list[RawPayment],
        *,
        year: int,
    ) -> list[RawPayment]:
        """Choose a complete representation per official commitment identity.

        Historical rows stay stored.  A partial detail collection cannot replace
        its aggregate, and matching commitment numbers never join distinct PKEMP.
        """
        if not payments:
            return payments
        del year  # the adapter's stable identity already contains source/entity/year/PKEMP
        groups: dict[tuple[str, str], list[RawPayment]] = {}
        for item in payments:
            if item.commitment_external_id:
                groups.setdefault((item.source, item.commitment_external_id), []).append(item)
        identities: set[str] = set()
        numbers_by_source: dict[str, set[str]] = {}
        for (source, identity), rows in groups.items():
            if not any(row.status in {"detalhe_empenho", "agregado_por_empenho"} for row in rows):
                continue
            identities.add(identity)
            numbers_by_source.setdefault(source, set()).update(
                row.commitment_number for row in rows if row.commitment_number
            )
        current_by_identity: dict[tuple[str, str], list[Payment]] = {}
        legacy_by_number: dict[tuple[str, str], set[str]] = {}
        # Read the affected commitments together. On SQLite, IS TRUE makes the
        # planner prefer the low-selectivity is_current index over the PKEMP
        # index. IS NOT FALSE is equivalent for this non-nullable column.
        if identities:
            for row in await session.scalars(
                select(Payment).where(
                    Payment.commitment_external_id.in_(identities),
                    Payment.is_current.is_not(False),
                )
            ):
                official_id = row.commitment_external_id
                if official_id is not None and (key := (row.source, official_id)) in groups:
                    current_by_identity.setdefault(key, []).append(row)
        for source, source_numbers in numbers_by_source.items():
            ordered_numbers = sorted(source_numbers)
            for offset in range(0, len(ordered_numbers), 500):
                for number, external_id in await session.execute(
                    select(Payment.commitment_number, Payment.external_id).where(
                        Payment.source == source,
                        Payment.commitment_external_id.is_(None),
                        Payment.commitment_number.in_(ordered_numbers[offset : offset + 500]),
                        Payment.is_current.is_not(False),
                    )
                ):
                    if number is not None:
                        legacy_by_number.setdefault((source, number), set()).add(external_id)
        selected: dict[tuple[str, str], str] = {}
        for key, rows in groups.items():
            details = [row for row in rows if row.status == "detalhe_empenho"]
            complete = bool(details) and all(row.commitment_details_complete for row in details)
            aggregate = any(row.status == "agregado_por_empenho" for row in rows)
            if not complete and not aggregate:
                continue
            source, identity = key
            # Legacy rows lacking PKEMP cannot safely be retired by number alone.
            numbers = {row.commitment_number for row in rows if row.commitment_number}
            incoming_ids = {row.external_id for row in rows}
            if any(
                legacy_by_number.get((source, number), set()) - incoming_ids for number in numbers
            ):
                continue
            current_rows = current_by_identity.get((source, identity), [])
            if not complete:
                existing_details = [row for row in current_rows if row.status == "detalhe_empenho"]
                total = sum((row.amount or Decimal("0") for row in existing_details), Decimal("0"))
                aggregate_amounts = {
                    row.amount for row in rows if row.status == "agregado_por_empenho"
                }
                if existing_details and aggregate_amounts == {total}:
                    selected[key] = "detalhe_empenho"
                    continue
            selected[key] = "detalhe_empenho" if complete else "agregado_por_empenho"
            incoming = {row.external_id for row in rows if row.status == selected[key]}
            for existing in current_rows:
                if existing.status != selected[key] or existing.external_id not in incoming:
                    trigger = rows[0]
                    install_audit(
                        session,
                        trigger,
                        run_id=self._execution().run_id,
                        source_record_id=trigger.source_record_id,
                    )
                    existing.source_record_id = (
                        trigger.source_record_id or existing.source_record_id
                    )
                    existing.is_current = False
                    await session.flush()
        staged: list[RawPayment] = []
        for item in payments:
            row_identity = item.commitment_external_id
            if item.status in {"detalhe_empenho", "agregado_por_empenho"} and row_identity:
                current = selected.get((item.source, row_identity)) == item.status
            elif item.status == "detalhe_empenho":
                current = False
            else:
                current = True
            staged.append(item.model_copy(update={"is_current": current}))
        return staged

    _attach_source_records = staticmethod(PaymentIngestionService._attach_source_records)

    async def _persist_source_records(
        self, run_id: UUID, result: ConnectorResult[list[RawPayment]]
    ) -> list[UUID]:
        from app.services.municipal_budget import BudgetResponse, projection_rows, store_projection

        identifiers: list[UUID] = []
        for raw in result.raw_records:
            async with write_session(self.session_factory) as session:
                await self._assert_fence(session)
                entity = await self._source_record(
                    session,
                    run_id,
                    raw,
                    result.availability,
                    result.diagnostic,
                    collection_complete=result.is_complete,
                    pagination_metadata=result.pagination.model_dump()
                    if result.pagination
                    else None,
                )
                identifiers.append(entity.id)
        responses = [
            BudgetResponse(
                key, raw.source, raw.request_parameters, raw.raw_payload, raw.http_status
            )
            for key, raw in zip(identifiers, result.raw_records, strict=True)
        ]
        rows = await asyncio.to_thread(lambda: list(projection_rows(responses)))
        if rows:
            async with write_session(self.session_factory) as session:
                await self._assert_fence(session)
                connection = await session.connection()
                await connection.run_sync(store_projection, rows)
        return identifiers

    async def _source_record(
        self,
        session: AsyncSession,
        run_id: UUID,
        raw: RawSourceRecord,
        availability: DataAvailability,
        diagnostic: str | None,
        *,
        collection_complete: bool | None = None,
        pagination_metadata: dict[str, Any] | None = None,
    ) -> SourceRecord:
        fingerprint = stable_fingerprint(
            "municipal_payment_source",
            run_id,
            raw.source,
            raw.record_key,
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
            crawl_run_id=run_id,
            source=raw.source,
            endpoint=raw.endpoint,
            request_parameters=raw.request_parameters,
            response_hash=raw.response_hash,
            raw_payload=raw.raw_payload,
            http_status=raw.http_status,
            availability=stored,
            collected_at=raw.collected_at,
            diagnostic=diagnostic,
            collection_complete=collection_complete,
            pagination_metadata=pagination_metadata,
            fingerprint=fingerprint,
        )
        session.add(entity)
        await session.flush()
        return entity

    async def _finish_run(
        self,
        run_id: UUID,
        summary: MunicipalPaymentSummary,
        status: CrawlRunStatus,
        progress_callback: MunicipalProgressCallback | None = None,
    ) -> None:
        recorder = self._execution().progress
        if recorder is not None:
            await recorder.aclose()
        async with write_session(self.session_factory) as session:
            run = await session.get(CrawlRun, run_id)
            if run is None:
                return
            if run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                return
            if self._execution().run_id is not None and not await self._fence_owned(session):
                return
            if coordinator := self._execution().coordinator:
                await coordinator.finish_in_session(session, status)
                batch = await session.get_one(CollectionBatch, coordinator.batch_id)
                if batch.status in {"limited", "failed"} and status is CrawlRunStatus.COMPLETED:
                    status = CrawlRunStatus.PARTIAL
                    if batch.status == "failed" and summary.diagnostic is None:
                        summary.diagnostic = "O lote ainda contém pendências da consulta original."
            cursor = dict(run.cursor or {})
            written = cursor.get("municipal_write")
            if isinstance(written, dict):
                summary.records_found = run.records_found
                summary.payments_created = run.records_created
                summary.payments_updated = run.records_updated
                summary.payments_changed = run.records_changed or 0
                summary.payments_unchanged = run.records_unchanged or 0
                summary.payments_stale_ignored = run.records_stale_ignored or 0
                summary.payments_conflicted = run.records_conflicted or 0
                summary.payments_linked = int(cursor.get("payments_linked") or 0)
                summary.payments_unlinked = int(cursor.get("payments_unlinked") or 0)
                total = written.get("total")
            else:
                total = summary.records_found or None
            processed = summary.payments_created + summary.payments_updated
            run.status = status
            run.finished_at = datetime.now(UTC)
            run.heartbeat_at = datetime.now(UTC)
            run.records_found = summary.records_found
            run.records_created = summary.payments_created
            run.records_updated = summary.payments_updated
            run.counting_version = 2
            run.records_changed = summary.payments_changed
            run.records_unchanged = summary.payments_unchanged
            run.records_stale_ignored = summary.payments_stale_ignored
            run.records_conflicted = summary.payments_conflicted
            run.diagnostic = summary.diagnostic
            run.errors = [{"message": summary.diagnostic}] if summary.diagnostic else []
            message = summary.diagnostic or STAGE_LABELS[status.value]
            progress = {
                "stage": status.value,
                "message": message,
                "processed": processed,
                "total": total,
                "updated_at": run.finished_at.isoformat(),
            }
            run.cursor = {
                **cursor,
                **progress,
                "source": summary.source_id,
                "availability": summary.availability.value,
                "payments_created": summary.payments_created,
                "payments_updated": summary.payments_updated,
                "payments_linked": summary.payments_linked,
                "payments_unlinked": summary.payments_unlinked,
            }
            if recorder is not None:
                recorder.record(
                    ProgressEvent(
                        stage=status.value,
                        message=message,
                        processed=processed,
                        total=total,
                    )
                )
                recorder.apply(run)
        if progress_callback is not None:
            progress_callback(progress)

    @staticmethod
    def _lease_name(source_id: str) -> str:
        return municipal_payment_lease_name(source_id)

    async def _acquire_lease(self, name: str, owner: str) -> bool:
        now = datetime.now(UTC)
        async with self.session_factory() as session, session.begin():
            acquired = await JobLeaseRepository(session).acquire(
                name=name,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(hours=2),
            )
            if acquired:
                self._execution().generation = int(
                    await session.scalar(select(JobLease.version).where(JobLease.name == name)) or 1
                )
            return acquired

    async def _release_lease(self, name: str, owner: str) -> None:
        async with self.session_factory() as session, session.begin():
            await JobLeaseRepository(session).release(
                name=name, owner_id=owner, generation=self._execution().generation
            )

    async def _abort_admission(
        self, run_id: UUID, name: str, owner: str, *, cancelled: bool
    ) -> None:
        async with self.session_factory() as session, session.begin():
            run = await session.get(CrawlRun, run_id)
            if run is not None and run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                run.status = CrawlRunStatus.CANCELLED if cancelled else CrawlRunStatus.FAILED
                run.cancel_requested = cancelled
                run.finished_at = datetime.now(UTC)
                run.diagnostic = (
                    "coleta cancelada durante a admissão"
                    if cancelled
                    else "falha durante a admissão da coleta"
                )
                run.errors = [{"message": run.diagnostic}]
            await JobLeaseRepository(session).release(
                name=name, owner_id=owner, generation=self._execution().generation
            )

    async def _fence_owned(self, session: AsyncSession) -> bool:
        execution = self._execution()
        if execution.run_id is None:
            return True
        return bool(
            execution.lease
            and execution.owner
            and execution.generation is not None
            and await JobLeaseRepository(session).fence(
                name=execution.lease,
                owner_id=execution.owner,
                generation=execution.generation,
                now=datetime.now(UTC),
            )
        )

    async def _assert_fence(self, session: AsyncSession) -> None:
        if self._execution().run_id is None:
            return
        if not await self._fence_owned(session):
            raise asyncio.CancelledError("lease municipal perdida")
        run = await session.scalar(
            select(CrawlRun)
            .where(CrawlRun.id == self._execution().run_id)
            .execution_options(populate_existing=True)
        )
        if (
            run is None
            or run.cancel_requested
            or run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
        ):
            raise asyncio.CancelledError("coleta municipal encerrada")

    async def _checkpoint(self) -> None:
        async with self.session_factory() as session, session.begin():
            await self._assert_fence(session)

    async def _heartbeat_lease(self, name: str, owner: str, run_id: UUID) -> None:
        while True:
            await asyncio.sleep(60)
            now = datetime.now(UTC)
            async with self.session_factory() as session, session.begin():
                renewed = await JobLeaseRepository(session).renew(
                    name=name,
                    owner_id=owner,
                    now=now,
                    expires_at=now + timedelta(hours=2),
                    generation=self._execution().generation,
                )
            if not renewed:
                worker = self._execution().worker
                if worker is not None:
                    worker.cancel()
                return
            async with self.session_factory() as session, session.begin():
                run = await session.get(CrawlRun, run_id)
                if (
                    run is not None
                    and run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
                    and await self._fence_owned(session)
                ):
                    run.heartbeat_at = now

    async def aclose(self) -> None:
        try:
            if self.connector is not None:
                await self.connector.aclose()
        finally:
            await self.payment_ingestion.aclose()


async def latest_municipal_runs(
    session: AsyncSession, source_ids: list[str] | None = None
) -> dict[str, CrawlRun]:
    """Return the most recent crawl for each municipal source id."""

    ranked = (
        select(
            CrawlRun.id.label("run_id"),
            func.row_number()
            .over(
                partition_by=CrawlRun.connector,
                order_by=(CrawlRun.created_at.desc(), CrawlRun.id.desc()),
            )
            .label("position"),
        )
        .where(CrawlRun.connector.like("municipal:%"))
        .subquery()
    )
    rows = list(
        (
            await session.scalars(
                select(CrawlRun)
                .join(ranked, CrawlRun.id == ranked.c.run_id)
                .where(ranked.c.position == 1)
            )
        ).all()
    )
    selected = set(source_ids or [])
    return {
        row.connector.removeprefix("municipal:"): row
        for row in rows
        if not selected or row.connector.removeprefix("municipal:") in selected
    }


async def run_selected_municipal_sources(
    settings: Settings | None = None,
    *,
    year: int,
    source_ids: list[str] | None = None,
    include_details: bool | None = None,
    company_cnpj: str | None = None,
    resume_automatically: bool = False,
) -> list[MunicipalPaymentSummary]:
    """Run each enabled source in sequence; one failure does not stop the rest."""

    selected = settings or get_settings()
    sources = (
        [get_municipal_source(item) for item in source_ids]
        if source_ids
        else selected_municipal_sources(selected)
    )
    results: list[MunicipalPaymentSummary] = []
    for source in sources:
        service = MunicipalPaymentIngestionService(selected)
        summary: MunicipalPaymentSummary | None = None
        try:
            summary = await service.run(
                source.id,
                year=year,
                company_cnpj=company_cnpj,
                include_details=include_details,
                **cast(
                    dict[str, Any], {"resume_automatically": True} if resume_automatically else {}
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            summary = MunicipalPaymentSummary(
                source_id=source.id,
                source_name=source.name,
                availability=DataAvailability.TEMPORARY_ERROR,
                diagnostic=f"{type(exc).__name__}: {exc}",
            )
        finally:
            try:
                await service.aclose()
            except Exception as exc:
                if summary is not None:
                    summary.diagnostic = "; ".join(
                        filter(
                            None,
                            (
                                summary.diagnostic,
                                f"falha no encerramento: {type(exc).__name__}: {exc}",
                            ),
                        )
                    )
        assert summary is not None
        results.append(summary)
    return results
