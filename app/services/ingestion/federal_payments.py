"""Bounded, resumable federal coverage for a frozen general-collection scope."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import RawPayment
from app.connectors.federal_payment_archives import (
    FederalArchivePayment,
    FederalArchiveUnavailable,
    FederalPaymentArchive,
    FederalPaymentArchivesConnector,
)
from app.database import async_session_factory, write_session
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    DataAvailability,
    JobLease,
    SourceRecord,
)
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion.collection_batches import digest
from app.services.ingestion.payments import PaymentIngestionService, PaymentIngestionSummary
from app.services.payment_tracking import federal_coverage_targets

if TYPE_CHECKING:
    from app.services.ingestion.complete_collection import CollectionScope

FederalScope = Literal["acervo", "acompanhadas", "pular"]
FEDERAL_BATCH_MODE = "federal_payments"
FEDERAL_LEASE = "payments:federal-coverage"
FEDERAL_CONNECTOR = "payments:transparencia-coverage"
_ARCHIVE_CATALOGUE = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"
_LEASE_DURATION = timedelta(minutes=15)
_WORKING = {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}


def federal_lease_owner(run_id: UUID) -> str:
    return f"federal-coverage:{run_id}"


@dataclass(frozen=True, slots=True)
class FederalCoverageRun:
    run_id: UUID | None = None
    batch_id: UUID | None = None
    note: str = ""
    skip_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _Unit:
    id: UUID
    kind: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ApiExecution:
    run: CrawlRun
    batch_status: str | None
    confirmed_records: int

    def reusable(self) -> bool:
        return self.run.status is CrawlRunStatus.COMPLETED or (
            self.run.status is CrawlRunStatus.PARTIAL and self.batch_status == "limited"
        )


class _Stopped(RuntimeError):
    pass


class FederalCoverageService:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        sessions: async_sessionmaker[AsyncSession] | None = None,
        archives: FederalPaymentArchivesConnector | None = None,
        payments: PaymentIngestionService | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.sessions = sessions or async_session_factory
        self.archives = archives or FederalPaymentArchivesConnector(self.settings)
        self.payments = payments or PaymentIngestionService(
            self.settings, session_factory=self.sessions
        )

    async def enqueue(self, scope: CollectionScope, parent_run_id: UUID) -> FederalCoverageRun:
        if scope.federal_scope == "pular":
            return FederalCoverageRun(
                note="Etapa pulada no escopo da coleta", skip_reason="excluded"
            )
        if "transparencia" not in self.settings.payment_sources:
            return FederalCoverageRun(
                note="Fonte federal desativada nas configurações", skip_reason="configuration"
            )

        now = datetime.now(UTC)
        run_id, batch_id = uuid4(), uuid4()
        async with write_session(self.sessions) as db:
            await JobLeaseRepository(db).assert_writes_allowed(now=now)
            tracked, archive_targets = await federal_coverage_targets(
                db, self.settings.payment_poll_cnpjs, include_acervo=scope.federal_scope == "acervo"
            )
            api_notice = ""
            if tracked and not self.settings.transparencia_api_key.strip():
                api_notice = "Chave do Portal da Transparência pendente para consulta pela API"
            if not archive_targets and (not tracked or api_notice):
                return FederalCoverageRun(
                    note=api_notice or "Nenhum CNPJ ativo em Pagamentos › Acompanhamento federal",
                    skip_reason="configuration",
                )
            filters = {
                "connector": FEDERAL_CONNECTOR,
                "mode": FEDERAL_BATCH_MODE,
                "uf": scope.uf,
                "parent_run_id": str(parent_run_id),
                "start_date": scope.start_date.isoformat(),
                "end_date": scope.end_date.isoformat(),
                "federal_scope": scope.federal_scope,
                "cnpjs": archive_targets,
                "tracked_cnpjs": tracked,
                "api_notice": api_notice,
            }
            batch = CollectionBatch(
                id=batch_id,
                stream_key=digest([FEDERAL_BATCH_MODE, filters]),
                filters=filters,
                status="pending",
                last_run_id=run_id,
            )
            db.add(batch)
            await db.flush()
            run = CrawlRun(
                id=run_id,
                connector=FEDERAL_CONNECTOR,
                status=CrawlRunStatus.PENDING,
                batch_id=batch_id,
                filters=filters,
                counting_version=2,
                records_changed=0,
                records_unchanged=0,
                records_stale_ignored=0,
                records_conflicted=0,
            )
            db.add(run)
            if archive_targets:
                for offset in range((scope.end_date - scope.start_date).days + 1):
                    day = (scope.end_date - timedelta(days=offset)).isoformat()
                    db.add(_work(batch_id, "archive", day, {"day": day}, offset))
            if not api_notice:
                position = 0
                for year in range(scope.start_date.year, scope.end_date.year + 1):
                    for cnpj in tracked:
                        db.add(
                            _work(
                                batch_id,
                                "beneficiary",
                                f"{cnpj}:{year}",
                                {"cnpj": cnpj, "year": year},
                                position,
                            )
                        )
                        position += 1
            await db.flush()
            await self._progress(db, batch, run)
        return FederalCoverageRun(run_id, batch_id)

    async def run(self, run_id: UUID) -> CrawlRunStatus:
        generation = await self._start(run_id)
        if generation is None:
            async with self.sessions() as db:
                return (await db.get_one(CrawlRun, run_id)).status
        worker = asyncio.current_task()
        assert worker is not None
        heartbeat = asyncio.create_task(self._heartbeat(run_id, generation, worker))
        terminal: CrawlRunStatus | None = None
        try:
            await self._pass(run_id, generation)
        except _Stopped:
            terminal = CrawlRunStatus.CANCELLED
        except asyncio.CancelledError:
            terminal = CrawlRunStatus.CANCELLED
            raise
        except Exception as exc:
            terminal = CrawlRunStatus.FAILED
            await self._record_error(run_id, generation, type(exc).__name__)
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat
            finalizer = asyncio.create_task(self._finish(run_id, generation, terminal))
            try:
                status = await asyncio.shield(finalizer)
            except asyncio.CancelledError:
                await asyncio.shield(finalizer)
                raise
        return status

    async def _start(self, run_id: UUID) -> int | None:
        now = datetime.now(UTC)
        async with write_session(self.sessions) as db:
            repository = JobLeaseRepository(db)
            await repository.lock_acquisitions()
            run = await db.get_one(CrawlRun, run_id)
            if run.status is not CrawlRunStatus.PENDING or not run.batch_id:
                return None
            batch = await db.get_one(CollectionBatch, run.batch_id)
            if batch.last_run_id != run_id:
                return None
            if run.cancel_requested:
                run.status = CrawlRunStatus.CANCELLED
                run.started_at = run.finished_at = now
                batch.status = "suspended"
                await self._progress(db, batch, run)
                return None
            acquired = await repository.acquire(
                name=FEDERAL_LEASE,
                owner_id=federal_lease_owner(run_id),
                now=now,
                expires_at=now + _LEASE_DURATION,
            )
            if not acquired:
                run.status = CrawlRunStatus.PARTIAL
                run.started_at = run.finished_at = now
                run.diagnostic = "Aguardando a liberação da coleta federal"
                batch.status = "limited"
                await self._progress(db, batch, run)
                return None
            lease = await db.get_one(JobLease, FEDERAL_LEASE, populate_existing=True)
            run.status = CrawlRunStatus.RUNNING
            run.started_at = run.heartbeat_at = now
            batch.status = "running"
            await self._progress(db, batch, run)
            return lease.version

    @asynccontextmanager
    async def _fenced(
        self, run_id: UUID, generation: int, *, finishing: bool = False
    ) -> AsyncIterator[tuple[AsyncSession, CrawlRun, CollectionBatch]]:
        async with write_session(self.sessions) as db:
            if not await self._owns(db, run_id, generation):
                raise _Stopped
            run = await db.get_one(CrawlRun, run_id)
            if (
                run.batch_id is None
                or run.status not in _WORKING
                or (run.cancel_requested and not finishing)
            ):
                raise _Stopped
            batch = await db.get_one(CollectionBatch, run.batch_id)
            if batch.last_run_id != run_id:
                raise _Stopped
            yield db, run, batch

    async def _owns(self, db: AsyncSession, run_id: UUID, generation: int) -> bool:
        return await JobLeaseRepository(db).fence(
            name=FEDERAL_LEASE,
            owner_id=federal_lease_owner(run_id),
            generation=generation,
            now=datetime.now(UTC),
        )

    async def _heartbeat(self, run_id: UUID, generation: int, worker: asyncio.Task[Any]) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                async with self._fenced(run_id, generation) as (db, run, _):
                    now = datetime.now(UTC)
                    if not await JobLeaseRepository(db).renew(
                        name=FEDERAL_LEASE,
                        owner_id=federal_lease_owner(run_id),
                        generation=generation,
                        now=now,
                        expires_at=now + _LEASE_DURATION,
                    ):
                        raise _Stopped
                    run.heartbeat_at = now
        except Exception:
            worker.cancel()
            raise

    async def _pass(self, run_id: UUID, generation: int) -> None:
        if "transparencia" not in self.settings.payment_sources:
            async with self._fenced(run_id, generation) as (_, run, __):
                run.diagnostic = "Fonte federal desativada nas configurações"
            return
        deadline = asyncio.get_running_loop().time() + self.settings.federal_payment_pass_seconds
        now = datetime.now(UTC).isoformat()
        async with self._fenced(run_id, generation) as (db, run, batch):
            batch_id = batch.id
            explicit_retry = "continuation_batch_id" in run.filters and not run.filters.get(
                "automatic_federal_continuation"
            )
            targets = frozenset(batch.filters.get("cnpjs", ()))
            from_date = date.fromisoformat(batch.filters["start_date"])
            to_date = date.fromisoformat(batch.filters["end_date"])
            query = (
                select(CollectionWorkItem)
                .where(
                    CollectionWorkItem.batch_id == batch_id,
                    or_(
                        CollectionWorkItem.status == "pending",
                        (CollectionWorkItem.status == "waiting")
                        & (CollectionWorkItem.payload["next_attempt_at"].as_string() <= now),
                    ),
                )
                .order_by(CollectionWorkItem.position)
            )
            archives = query.where(CollectionWorkItem.kind == "archive")
            staged = CollectionWorkItem.payload["archive_staged"].as_boolean().is_(True)
            saved = (await db.scalars(archives.where(staged))).all()
            fresh = (
                await db.scalars(
                    archives.where(~staged).limit(self.settings.federal_payment_archives_per_pass)
                )
            ).all()
            api = (
                await db.scalars(
                    query.where(CollectionWorkItem.kind == "beneficiary").limit(
                        self.settings.federal_payment_targets_per_pass
                    )
                )
            ).all()
            selected = [
                _Unit(row.id, row.kind, dict(row.payload))
                for row in [
                    *saved,
                    *sorted([*fresh, *api], key=lambda row: (row.position, row.kind)),
                ]
            ]
        catalogue: frozenset[date] | None = None
        catalogue_failed = False
        for unit in selected:
            if asyncio.get_running_loop().time() >= deadline:
                break
            async with self._fenced(run_id, generation) as (db, run, _):
                item = await db.get_one(CollectionWorkItem, unit.id)
                item.attempts += 1
                run.heartbeat_at = datetime.now(UTC)
            if unit.kind == "beneficiary":
                await self._collect_api(
                    run_id, generation, batch_id, unit, explicit_retry=explicit_retry
                )
                continue
            if unit.payload.get("archive_staged"):
                await self._persist_archive(run_id, generation, unit, deadline)
                continue
            if catalogue is None and not catalogue_failed:
                try:
                    catalogue = await self.archives.list_days()
                except Exception:
                    catalogue_failed = True
            day = date.fromisoformat(unit.payload["day"])
            if catalogue is None:
                await self._update_unit(
                    run_id, generation, unit, "failed", "Catálogo oficial indisponível"
                )
            elif day not in catalogue:
                await self._archive_unavailable(run_id, generation, unit, unpublished=True)
            else:
                await self._collect_archive(
                    run_id, generation, unit, day, targets, from_date, to_date, deadline
                )

    async def _collect_archive(
        self,
        run_id: UUID,
        generation: int,
        unit: _Unit,
        day: date,
        targets: frozenset[str],
        start_date: date,
        end_date: date,
        deadline: float,
    ) -> None:
        try:
            archive = await self.archives.fetch(
                day, targets, start_date=start_date, end_date=end_date
            )
        except FederalArchiveUnavailable as exc:
            await self._archive_unavailable(
                run_id, generation, unit, unpublished=False, http_status=exc.http_status
            )
            return
        except Exception:
            await self._update_unit(
                run_id,
                generation,
                unit,
                "failed",
                "Falha na leitura ou validação do arquivo oficial",
            )
            return
        payments = [self._raw_payment(entry) for entry in archive.records]
        async with self._fenced(run_id, generation) as (db, run, _):
            item = await db.get_one(CollectionWorkItem, unit.id)
            source = self._archive_source(run, archive, payments)
            db.add(source)
            await db.flush()
            item.source_record_id = source.id
            item.payload = {
                **item.payload,
                "archive_staged": True,
                "archive_processed": 0,
                "archive_conflicts": 0,
                "archive_issues": list(archive.issues),
            }
        await self._persist_archive(run_id, generation, unit, deadline, payments=payments)

    async def _persist_archive(
        self,
        run_id: UUID,
        generation: int,
        unit: _Unit,
        deadline: float,
        *,
        payments: list[RawPayment] | None = None,
    ) -> None:
        if payments is None:
            async with self.sessions() as db:
                snapshot = await db.scalar(
                    select(SourceRecord.raw_payload)
                    .join(
                        CollectionWorkItem, CollectionWorkItem.source_record_id == SourceRecord.id
                    )
                    .where(CollectionWorkItem.id == unit.id)
                )
            if not isinstance(snapshot, dict) or not isinstance(
                snapshot.get("normalized_payments"), list
            ):
                raise ValueError("A cópia local do arquivo federal não está disponível.")
            payments = [RawPayment.model_validate(row) for row in snapshot["normalized_payments"]]
        block_size = min(250, self.settings.payment_ingestion_batch_size)
        while asyncio.get_running_loop().time() < deadline:
            async with self._fenced(run_id, generation) as (db, run, batch):
                item = await db.get_one(CollectionWorkItem, unit.id)
                processed = item.payload["archive_processed"]
                block = payments[processed : processed + block_size]
                db.info["crawl_run_id"] = run.id
                result = await self.payments.persist_raw_payments(
                    block, source_record_id=item.source_record_id, session=db
                )
                processed += len(block)
                conflicts = item.payload["archive_conflicts"] + result.payments_conflicted
                found = set(item.payload.get("found_cnpjs", ()))
                found.update(row.supplier_cnpj for row in block if row.supplier_cnpj)
                finished = processed == len(payments)
                problems = list(item.payload["archive_issues"])
                if conflicts:
                    problems.append(f"{conflicts} identidades para revisão")
                status = "pending"
                if finished:
                    status = "failed" if problems else "succeeded" if payments else "empty"
                item.payload = {
                    **item.payload,
                    "archive_staged": not finished,
                    "archive_processed": processed,
                    "archive_conflicts": conflicts,
                }
                self._set_item(
                    item,
                    status,
                    " · ".join(problems) if finished else "",
                    result=result,
                    found_cnpjs=sorted(found),
                    records=processed,
                )
                run.heartbeat_at = datetime.now(UTC)
                await self._progress(db, batch, run)
            if finished:
                return
            await asyncio.sleep(0)

    def _archive_source(
        self, run: CrawlRun, archive: FederalPaymentArchive, payments: list[RawPayment]
    ) -> SourceRecord:
        available = DataAvailability.AVAILABLE if archive.records else DataAvailability.EMPTY
        if archive.issues and not archive.records:
            available = DataAvailability.TEMPORARY_ERROR
        resolved_url = urlsplit(archive.resolved_url)._replace(query="", fragment="")
        return SourceRecord(
            crawl_run_id=run.id,
            source="transparencia",
            endpoint=archive.url,
            request_parameters={"day": archive.day.isoformat(), "transport": "official_archive"},
            response_hash=archive.sha256,
            raw_payload={
                "resolved_url": resolved_url.geturl(),
                "issues": list(archive.issues),
                "normalized_payments": [row.model_dump(mode="json") for row in payments],
                "rows": [
                    {
                        "member": row.member,
                        "lines": list(row.line_numbers),
                        "values": row.raw_rows,
                        "related_evidence": [asdict(ref) for ref in row.related_evidence],
                    }
                    for row in archive.records
                ],
                "review_rows": [
                    {
                        "reason": row.reason,
                        "member": row.member,
                        "lines": list(row.line_numbers),
                        "values": row.raw_rows,
                    }
                    for row in archive.review_rows
                ],
            },
            collected_at=archive.collected_at,
            http_status=200,
            availability=available,
            collection_complete=not archive.issues,
            fingerprint=digest([FEDERAL_BATCH_MODE, run.id, archive.day, archive.sha256]),
        )

    def _raw_payment(self, entry: FederalArchivePayment) -> RawPayment:
        issued = datetime.combine(entry.issued_on, time.min, ZoneInfo(self.settings.timezone))
        return RawPayment(
            source="transparencia",
            external_id=":".join((entry.role, entry.document, entry.cnpj, entry.list_code or "")),
            source_url=f"https://portaldatransparencia.gov.br/despesas/pagamento/{entry.document}",
            raw_payload={"member": entry.member, "lines": list(entry.line_numbers)},
            agency_name=entry.agency_name,
            uasg=entry.management_unit,
            process_number=entry.process_number,
            payment_date=issued.astimezone(UTC),
            amount=entry.amount,
            phase="Pagamento",
            document_number=entry.document,
            supplier_cnpj=entry.cnpj,
            supplier_name=entry.supplier_name,
            federal_beneficiary_role=entry.role,
            federal_intermediary=entry.is_intermediary,
            federal_creditor_list=entry.list_code,
        )

    async def _archive_unavailable(
        self,
        run_id: UUID,
        generation: int,
        unit: _Unit,
        *,
        unpublished: bool,
        http_status: int | None = None,
    ) -> None:
        day = unit.payload["day"]
        note = (
            "Aguardando publicação no catálogo oficial"
            if unpublished
            else "Arquivo oficial indisponível"
        )
        async with self._fenced(run_id, generation) as (db, run, batch):
            item = await db.get_one(CollectionWorkItem, unit.id)
            catalogue_hash = (
                getattr(self.archives, "catalogue_sha256", None) if unpublished else None
            )
            catalogue_observed_at = (
                getattr(self.archives, "catalogue_collected_at", None) if unpublished else None
            )
            source = SourceRecord(
                crawl_run_id=run.id,
                source="transparencia",
                endpoint=_ARCHIVE_CATALOGUE
                if unpublished
                else (_ARCHIVE_CATALOGUE + day.replace("-", "")),
                request_parameters={"day": day, "transport": "official_archive"},
                raw_payload={
                    "listed": not unpublished,
                    "hash_scope": "catalogue" if catalogue_hash else "request_metadata",
                },
                response_hash=catalogue_hash or digest([day, unpublished, http_status]),
                collected_at=catalogue_observed_at or datetime.now(UTC),
                availability=(
                    DataAvailability.NOT_PUBLISHED
                    if unpublished
                    else DataAvailability.ACCESS_RESTRICTED
                    if http_status == 403
                    else DataAvailability.TEMPORARY_ERROR
                ),
                collection_complete=False,
                http_status=200 if unpublished else http_status,
                diagnostic=note,
                fingerprint=digest([FEDERAL_BATCH_MODE, run.id, day, note]),
            )
            db.add(source)
            await db.flush()
            item.source_record_id = source.id
            self._set_item(item, "waiting" if unpublished else "failed", note)
            await self._progress(db, batch, run)

    async def _collect_api(
        self,
        run_id: UUID,
        generation: int,
        batch_id: UUID,
        unit: _Unit,
        *,
        explicit_retry: bool,
    ) -> None:
        if not self.settings.transparencia_api_key.strip():
            await self._update_unit(
                run_id, generation, unit, "waiting", "Chave do Portal da Transparência pendente"
            )
            return
        cnpj, year = str(unit.payload["cnpj"]), int(unit.payload["year"])
        previous = await self._latest_api_run(batch_id, cnpj, year)
        outcome: _ApiExecution | None
        if (
            previous is not None
            and previous.reusable()
            and str(previous.run.id) != unit.payload.get("api_run_id")
        ):
            outcome = previous
        else:
            try:
                if (
                    explicit_retry
                    and previous is not None
                    and previous.batch_status in {"failed", "suspended"}
                ):
                    result = await self._retry_api_unit(
                        run_id, generation, batch_id, previous.run.id
                    )
                else:
                    result = await self.payments.run(
                        company_cnpj=cnpj,
                        year=year,
                        resume_automatically=True,
                        parent_run_id=run_id,
                        coverage_batch_id=batch_id,
                    )
            except _Stopped:
                raise
            except Exception:
                await self._update_unit(
                    run_id, generation, unit, "failed", "Falha na consulta pela API"
                )
                return
            outcome = await self._read_api_run(result.crawl_run_id)
        if outcome is None:
            await self._update_unit(
                run_id, generation, unit, "failed", "Consulta sem execução confirmada"
            )
            return
        api_run, api_batch_status = outcome.run, outcome.batch_status
        records = (api_run.records_created or 0) + (api_run.records_updated or 0)
        found = max(records, outcome.confirmed_records, int(unit.payload.get("records", 0)))
        if api_batch_status == "limited":
            state, note = "pending", "Páginas ou registros da API aguardando continuação"
        elif api_run.status is CrawlRunStatus.COMPLETED and api_batch_status in {None, "complete"}:
            state, note = "succeeded" if found else "empty", ""
        else:
            state, note = "failed", "Consulta da API incompleta ou indisponível"
        if api_run.records_conflicted:
            state, note = "failed", "Consulta da API com identidades para revisão"
        summary = PaymentIngestionSummary(
            payments_created=api_run.records_created or 0,
            payments_updated=api_run.records_updated or 0,
            payments_changed=api_run.records_changed or 0,
            payments_unchanged=api_run.records_unchanged or 0,
            payments_stale_ignored=api_run.records_stale_ignored or 0,
            payments_conflicted=api_run.records_conflicted or 0,
        )
        async with self._fenced(run_id, generation) as (db, run, batch):
            item = await db.get_one(CollectionWorkItem, unit.id)
            self._set_item(item, state, note, result=summary, records=found, api_run_id=api_run.id)
            run.records_found += records
            run.records_created += summary.payments_created
            run.records_updated += summary.payments_updated
            run.records_changed = (run.records_changed or 0) + summary.payments_changed
            run.records_unchanged = (run.records_unchanged or 0) + summary.payments_unchanged
            run.records_stale_ignored = (
                run.records_stale_ignored or 0
            ) + summary.payments_stale_ignored
            run.records_conflicted = (run.records_conflicted or 0) + summary.payments_conflicted
            await self._progress(db, batch, run)

    async def _retry_api_unit(
        self, run_id: UUID, generation: int, batch_id: UUID, failed_run_id: UUID
    ) -> PaymentIngestionSummary:
        from app.services.ingestion.continuation import create_continuation

        child_id: UUID | None = None
        try:
            async with self._fenced(run_id, generation) as (db, _, __):
                child, filters = await create_continuation(db, failed_run_id, require_latest=True)
                filters.update(parent_run_id=str(run_id), coverage_batch_id=str(batch_id))
                child.filters = filters
                child_id = child.id
            return await self.payments.continue_collection(child_id, filters)
        except asyncio.CancelledError:
            await self._cancel_pending(child_id)
            raise

    async def _latest_api_run(
        self, coverage_batch_id: UUID, cnpj: str, year: int
    ) -> _ApiExecution | None:
        async with self.sessions() as db:
            run = await db.scalar(
                select(CrawlRun)
                .where(
                    CrawlRun.filters["coverage_batch_id"].as_string() == str(coverage_batch_id),
                    CrawlRun.filters["company_cnpj"].as_string() == cnpj,
                    CrawlRun.filters["year"].as_integer() == year,
                    CrawlRun.status.not_in(_WORKING),
                )
                .order_by(CrawlRun.created_at.desc(), CrawlRun.id.desc())
                .limit(1)
            )
            run_id = run.id if run else None
        return await self._read_api_run(run_id)

    async def _read_api_run(self, run_id: UUID | None) -> _ApiExecution | None:
        if run_id is None:
            return None
        async with self.sessions() as db:
            run = await db.get(CrawlRun, run_id)
            if run is None:
                return None
            batch = await db.get(CollectionBatch, run.batch_id) if run.batch_id else None
            if batch is not None and batch.last_run_id != run_id:
                return None
            confirmed = 0
            if batch is not None:
                confirmed = (
                    await db.scalar(
                        select(func.count())
                        .select_from(CollectionWorkItem)
                        .where(
                            CollectionWorkItem.batch_id == batch.id,
                            CollectionWorkItem.kind == "record",
                            CollectionWorkItem.status == "succeeded",
                        )
                    )
                    or 0
                )
            return _ApiExecution(run, batch.status if batch else None, confirmed)

    async def _update_unit(
        self, run_id: UUID, generation: int, unit: _Unit, status: str, diagnostic: str
    ) -> None:
        async with self._fenced(run_id, generation) as (db, run, batch):
            item = await db.get_one(CollectionWorkItem, unit.id)
            self._set_item(item, status, diagnostic)
            await self._progress(db, batch, run)

    @staticmethod
    def _set_item(
        item: CollectionWorkItem,
        status: str,
        diagnostic: str,
        *,
        records: int | None = None,
        result: PaymentIngestionSummary | None = None,
        found_cnpjs: list[str] | None = None,
        api_run_id: UUID | None = None,
    ) -> None:
        payload = dict(item.payload)
        payload["diagnostic"] = diagnostic
        if records is not None:
            payload["records"] = records
        if result is not None:
            payload["records_created"] = (
                int(payload.get("records_created", 0)) + result.payments_created
            )
            payload["records_updated"] = (
                int(payload.get("records_updated", 0)) + result.payments_updated
            )
        if found_cnpjs is not None:
            payload["found_cnpjs"] = found_cnpjs
        if api_run_id is not None:
            payload["api_run_id"] = str(api_run_id)
        if status == "waiting":
            payload["next_attempt_at"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        else:
            payload.pop("next_attempt_at", None)
        item.status = status
        item.payload = payload

    async def _progress(
        self, db: AsyncSession, batch: CollectionBatch, run: CrawlRun
    ) -> dict[str, Any]:
        await db.flush()
        subtotals = {kind: _progress_counter() for kind in ("archive", "beneficiary")}
        found_by_kind: dict[str, set[str]] = {"archive": set(), "beneficiary": set()}
        next_attempts = []
        rows = await db.execute(
            select(
                CollectionWorkItem.kind, CollectionWorkItem.status, CollectionWorkItem.payload
            ).where(CollectionWorkItem.batch_id == batch.id)
        )
        for row in rows:
            kind, status, payload = row.kind, row.status, row.payload
            tally = subtotals[kind]
            tally["total"] += 1
            tally["completed" if status == "succeeded" else status] += 1
            if status == "empty":
                tally["completed"] += 1
            if status == "waiting" and payload.get("next_attempt_at"):
                next_attempts.append(payload["next_attempt_at"])
            tally["records_created"] += payload.get("records_created", 0)
            tally["records_updated"] += payload.get("records_updated", 0)
            tally["review_required"] += int(
                bool(payload.get("archive_issues") or payload.get("archive_conflicts"))
            )
            found = payload.get("found_cnpjs", [])
            if kind == "beneficiary" and payload.get("records", 0):
                found = [payload["cnpj"]]
            found_by_kind[kind].update(found)
        for kind, found_cnpjs in found_by_kind.items():
            subtotals[kind]["found"] = len(found_cnpjs)
        snapshot: dict[str, Any] = {
            k: subtotals["archive"][k] + subtotals["beneficiary"][k] for k in _progress_counter()
        }
        snapshot.update(
            archives=subtotals["archive"],
            api=subtotals["beneficiary"],
            found=len(found_by_kind["archive"] | found_by_kind["beneficiary"]),
            api_notice=batch.filters.get("api_notice", ""),
            next_attempt_at=min(next_attempts, default=None),
        )
        batch.cursors = {**batch.cursors, "federal": snapshot}
        run.cursor = {
            "federal": snapshot,
            "progress": {
                "stage": run.status.value if run.status not in _WORKING else "federal_payments",
                "message": "Pagamentos federais",
                "processed": snapshot["completed"],
                "total": snapshot["total"],
                "updated_at": datetime.now(UTC).isoformat(),
            },
        }
        return snapshot

    async def _record_error(self, run_id: UUID, generation: int, error: str) -> None:
        with suppress(_Stopped):
            async with self._fenced(run_id, generation, finishing=True) as (_, run, __):
                run.diagnostic = f"Falha na etapa federal ({error})"
                run.errors = [{"message": run.diagnostic}]

    async def _finish(
        self, run_id: UUID, generation: int, terminal: CrawlRunStatus | None
    ) -> CrawlRunStatus:
        try:
            async with self._fenced(run_id, generation, finishing=True) as (db, run, batch):
                snapshot = await self._progress(db, batch, run)
                if terminal == CrawlRunStatus.CANCELLED or run.cancel_requested:
                    run.status = CrawlRunStatus.CANCELLED
                    batch.status = "suspended"
                elif terminal == CrawlRunStatus.FAILED:
                    run.status = terminal
                    batch.status = "failed"
                elif snapshot["pending"] or snapshot["waiting"]:
                    run.status = CrawlRunStatus.PARTIAL
                    batch.status = "limited"
                elif snapshot["failed"]:
                    run.status = (
                        CrawlRunStatus.PARTIAL if snapshot["completed"] else CrawlRunStatus.FAILED
                    )
                    batch.status = "failed"
                else:
                    run.status = CrawlRunStatus.COMPLETED
                    batch.status = "complete"
                moment = datetime.now(UTC)
                run.finished_at = run.heartbeat_at = moment
                cursor = run.cursor or {}
                run.cursor = {
                    **cursor,
                    "progress": {**cursor.get("progress", {}), "stage": run.status.value},
                }
                await JobLeaseRepository(db).release(
                    name=FEDERAL_LEASE, owner_id=federal_lease_owner(run_id), generation=generation
                )
                return run.status
        except _Stopped:
            async with self.sessions() as db:
                return (await db.get_one(CrawlRun, run_id)).status

    async def resume_next(self) -> None:
        from app.services.ingestion.continuation import ContinuationBusy, create_continuation

        if "transparencia" not in self.settings.payment_sources:
            return
        now = datetime.now(UTC).isoformat()
        child_id: UUID | None = None
        try:
            async with write_session(self.sessions) as db:
                progress = CollectionBatch.cursors["federal"]
                run_id = await db.scalar(
                    select(CollectionBatch.last_run_id)
                    .join(CrawlRun, CrawlRun.id == CollectionBatch.last_run_id)
                    .where(
                        CollectionBatch.filters["mode"].as_string() == FEDERAL_BATCH_MODE,
                        CollectionBatch.status == "limited",
                        CrawlRun.status.not_in(_WORKING),
                        or_(
                            progress["pending"].as_integer() > 0,
                            progress["next_attempt_at"].as_string() <= now,
                        ),
                    )
                    .order_by(CollectionBatch.updated_at, CollectionBatch.id)
                    .limit(1)
                )
                if run_id is None:
                    return
                child, _ = await create_continuation(
                    db, run_id, require_latest=True, automatic=True
                )
                child_id = child.id
            await self.run(child_id)
        except ContinuationBusy:
            return
        except asyncio.CancelledError:
            await self._cancel_pending(child_id)
            raise

    async def _cancel_pending(self, run_id: UUID | None) -> None:
        if run_id is None:
            return
        from app.services.ingestion.recovery import finalize_cancelled_run

        async with write_session(self.sessions) as db:
            await JobLeaseRepository(db).lock_acquisitions()
            run = await db.get(CrawlRun, run_id)
            if run is not None and run.status is CrawlRunStatus.PENDING:
                await finalize_cancelled_run(db, run)

    async def aclose(self) -> None:
        try:
            await self.payments.aclose()
        finally:
            await self.archives.aclose()


def _work(
    batch_id: UUID, kind: str, identity: str, payload: dict[str, Any], position: int
) -> CollectionWorkItem:
    return CollectionWorkItem(
        batch_id=batch_id,
        source="transparencia",
        kind=kind,
        identity=identity,
        status="pending",
        position=position,
        payload=payload,
    )


def _progress_counter() -> dict[str, int]:
    return dict.fromkeys(
        (
            "total",
            "completed",
            "empty",
            "pending",
            "waiting",
            "failed",
            "records_created",
            "records_updated",
            "review_required",
            "found",
        ),
        0,
    )
