"""Run the manual collection in one auditable sequence using the existing collectors."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.municipal_payments.sources import (
    list_municipal_sources,
    selected_municipal_sources,
)
from app.database import async_session_factory, write_session
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    JobLease,
    Procurement,
)
from app.repositories.crawls import JobLeaseRepository
from app.services.collection_periods import use_calendar_slices
from app.services.identifiers import is_valid_cnpj
from app.services.ingestion.continuation import (
    ContinuationBusy,
    create_continuation,
    pipeline_request_from_filters,
)
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest, PipelineSummary
from app.services.ingestion.recovery import finalize_cancelled_run
from app.services.payment_tracking import tracked_payment_cnpjs
from app.services.price_registry_scopes import known_price_registry_scopes
from app.services.procurement_search import ObjectScope

COMPLETE_COLLECTION_LEASE = "complete-collection"
COMPLETE_COLLECTION_MODE = "complete"


def close_collection_stages(stages: list[Any], terminal: str) -> list[Any]:
    """Record which stages were running when the collection ended."""

    result = []
    for stage in stages:
        if isinstance(stage, dict):
            status = stage.get("status")
            if status == "pending":
                stage = {**stage, "status": "not_started"}
            elif status == "running":
                stage = {**stage, "status": terminal}
        result.append(stage)
    return result


COLLECTION_STAGES = (
    ("procurements", "Contratações e documentos"),
    ("tce_ma", "TCE-MA · São Luís"),
    ("price_registries", "Atas de registro de preços"),
    ("contracts", "Contratos"),
    ("municipal", "Pagamentos municipais"),
    ("federal", "Pagamentos federais"),
)
_ACTIVE = {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}


@dataclass(frozen=True, slots=True)
class CollectionScope:
    uf: str
    start_date: date
    end_date: date
    force_refresh: bool = False
    modalities: tuple[str, ...] = ()
    object_scope: ObjectScope | None = None
    procurements_only: bool = False

    def as_json(self) -> dict[str, Any]:
        return {
            "connector": COMPLETE_COLLECTION_MODE,
            "mode": COMPLETE_COLLECTION_MODE,
            "uf": self.uf,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "force_refresh": self.force_refresh,
            **({"modalities": list(self.modalities)} if self.modalities else {}),
            **({"object_scope": self.object_scope.as_json()} if self.object_scope else {}),
            **({"procurements_only": True} if self.procurements_only else {}),
        }


@dataclass(frozen=True, slots=True)
class CollectionTicket:
    run_id: UUID
    generation: int | None

    @property
    def owner(self) -> str:
        return f"complete:{self.run_id}"


@dataclass(slots=True)
class StepResult:
    status: str
    note: str = ""
    run_ids: list[UUID] = field(default_factory=list)


class _CollectionStopped(RuntimeError):
    pass


def _combined_status(statuses: list[str]) -> str:
    executed = [status for status in statuses if status != "skipped"]
    if executed and all(status == "failed" for status in executed):
        return "failed"
    if any(status in {"failed", "partial"} for status in statuses):
        return "partial"
    return "completed"


class CompleteCollection:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        sessions: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.sessions = sessions or async_session_factory
        self.pipeline = IngestionPipeline(self.settings, session_factory=self.sessions)

    async def enqueue(self, scope: CollectionScope) -> CollectionTicket:
        """Reserve a single manual collection and reuse it on duplicate submission."""
        now = datetime.now(UTC)
        ticket = CollectionTicket(uuid4(), None)
        filters = scope.as_json()
        async with write_session(self.sessions) as db:
            leases = JobLeaseRepository(db)
            await leases.assert_writes_allowed(now=now)
            acquired = await leases.acquire(
                name=COMPLETE_COLLECTION_LEASE,
                owner_id=ticket.owner,
                now=now,
                expires_at=now + timedelta(hours=2),
            )
            lease = await db.get(JobLease, COMPLETE_COLLECTION_LEASE, populate_existing=True)
            if not acquired:
                if lease is not None and lease.owner_id.startswith("complete:"):
                    previous = await db.get(
                        CrawlRun, UUID(lease.owner_id.removeprefix("complete:"))
                    )
                    if (
                        previous is not None
                        and previous.status in _ACTIVE
                        and previous.filters == filters
                    ):
                        return CollectionTicket(previous.id, None)
                raise RuntimeError("Já há outra coleta em andamento.")
            assert lease is not None
            db.add(
                CrawlRun(
                    id=ticket.run_id,
                    connector=COMPLETE_COLLECTION_MODE,
                    status=CrawlRunStatus.PENDING,
                    filters=filters,
                    cursor={
                        "progress": {"stage": "queued", "updated_at": now.isoformat()},
                        "complete_stages": [
                            {"key": key, "label": label, "status": "pending", "run_ids": []}
                            for key, label in COLLECTION_STAGES
                            if key == "procurements" or not scope.procurements_only
                        ],
                    },
                )
            )
            return CollectionTicket(ticket.run_id, lease.version)

    @asynccontextmanager
    async def _locked(
        self, ticket: CollectionTicket, *, finishing: bool = False
    ) -> AsyncIterator[tuple[AsyncSession, CrawlRun]]:
        assert ticket.generation is not None
        now = datetime.now(UTC)
        async with write_session(self.sessions) as db:
            if not await JobLeaseRepository(db).fence(
                name=COMPLETE_COLLECTION_LEASE,
                owner_id=ticket.owner,
                generation=ticket.generation,
                now=now,
            ):
                raise _CollectionStopped("Autorização de execução perdida")
            run = await db.get(CrawlRun, ticket.run_id)
            if run is None or run.status not in _ACTIVE or (run.cancel_requested and not finishing):
                raise _CollectionStopped("Coleta encerrada")
            yield db, run

    async def _heartbeat(self, ticket: CollectionTicket, worker: asyncio.Task[Any]) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                async with self._locked(ticket) as (db, run):
                    now = datetime.now(UTC)
                    if not await JobLeaseRepository(db).renew(
                        name=COMPLETE_COLLECTION_LEASE,
                        owner_id=ticket.owner,
                        generation=ticket.generation,
                        now=now,
                        expires_at=now + timedelta(hours=2),
                    ):
                        raise _CollectionStopped("Autorização de execução perdida")
                    run.heartbeat_at = now
        except Exception:
            worker.cancel()
            raise

    async def _stage(self, ticket: CollectionTicket, key: str, result: StepResult) -> None:
        async with self._locked(ticket) as (_, run):
            now = datetime.now(UTC)
            run.status = CrawlRunStatus.RUNNING
            run.started_at = run.started_at or now
            run.heartbeat_at = now
            cursor = dict(run.cursor or {})
            stages = [dict(stage) for stage in cursor["complete_stages"]]
            stage = next(stage for stage in stages if stage["key"] == key)
            stage.update(
                status=result.status,
                note=result.note,
            )
            if result.run_ids or result.status != "running":
                stage["run_ids"] = [str(item) for item in result.run_ids]
            if result.status == "running":
                stage.setdefault("started_at", now.isoformat())
            else:
                stage["finished_at"] = now.isoformat()
            cursor["complete_stages"] = stages
            cursor["progress"] = {
                "stage": "complete",
                "updated_at": now.isoformat(),
                "message": stage["label"],
                "processed": sum(item["status"] not in {"pending", "running"} for item in stages),
                "total": len(stages),
            }
            run.cursor = cursor

    async def _finish(self, ticket: CollectionTicket, status: CrawlRunStatus) -> None:
        try:
            async with self._locked(ticket, finishing=True) as (db, run):
                if run.cancel_requested:
                    status = CrawlRunStatus.CANCELLED
                now = datetime.now(UTC)
                cursor = dict(run.cursor or {})
                cursor["complete_stages"] = close_collection_stages(
                    cursor["complete_stages"], status.value
                )
                cursor["progress"] = {"stage": status.value, "updated_at": now.isoformat()}
                run.cursor = cursor
                run.status = status
                run.finished_at = now
                if status is CrawlRunStatus.CANCELLED:
                    run.cancel_requested = True
                    # The procurement child may already have queued analyses before cancellation.
                    from app.services.llm.queue import cancel_for_run

                    children = await db.scalars(
                        select(CrawlRun.id).where(
                            CrawlRun.filters["parent_run_id"].as_string() == str(ticket.run_id)
                        )
                    )
                    for child in children.all():
                        await cancel_for_run(db, child)
                await JobLeaseRepository(db).release(
                    name=COMPLETE_COLLECTION_LEASE,
                    owner_id=ticket.owner,
                    generation=ticket.generation,
                )
        except _CollectionStopped:
            # Recovery or another worker owns the terminal state.
            return

    async def run(self, ticket: CollectionTicket, scope: CollectionScope) -> None:
        if ticket.generation is None:
            return
        worker = asyncio.current_task()
        assert worker is not None
        heartbeat = asyncio.create_task(self._heartbeat(ticket, worker))
        stages = {
            "procurements": self._procurements,
            "price_registries": self._registries,
            "contracts": self._contracts,
            "municipal": self._municipal,
            "federal": self._federal,
            "tce_ma": self._tce,
        }
        results: list[str] = []
        status = CrawlRunStatus.FAILED

        async def chain(keys: tuple[str, ...]) -> None:
            for key in keys:
                await self._stage(ticket, key, StepResult("running"))
                try:
                    outcome = await stages[key](ticket, scope)
                except _CollectionStopped:
                    raise
                except Exception as exc:
                    outcome = StepResult("failed", f"Falha nesta etapa ({type(exc).__name__})")
                await self._stage(ticket, key, outcome)
                results.append(outcome.status)

        tasks: tuple[asyncio.Task[None], ...]
        if scope.procurements_only:
            tasks = (asyncio.create_task(chain(("procurements",))),)
        else:
            tasks = (
                asyncio.create_task(chain(("municipal", "federal"))),
                asyncio.create_task(
                    chain(("procurements", "tce_ma", "price_registries", "contracts"))
                ),
            )
        try:
            await asyncio.gather(*tasks)
            status = CrawlRunStatus(_combined_status(results))
        except _CollectionStopped:
            status = CrawlRunStatus.CANCELLED
        except asyncio.CancelledError:
            status = CrawlRunStatus.CANCELLED
            raise
        finally:

            async def finalize() -> None:
                for task in tasks:
                    # Cancellation of the gather has already reached these
                    # tasks. A second cancellation can interrupt their rollback.
                    if not task.done() and not task.cancelling():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                heartbeat.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await heartbeat
                await self._finish(ticket, status)

            cleanup = asyncio.create_task(finalize())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await asyncio.shield(cleanup)
                raise

    def _request(self, scope: CollectionScope, **kwargs: Any) -> PipelineRequest:
        return PipelineRequest(
            connector="all",
            uf=scope.uf,
            days=None,
            start_date=scope.start_date,
            end_date=scope.end_date,
            modalities=tuple(self.settings.default_modalities),
            force_refresh=scope.force_refresh,
            **kwargs,
        )

    async def _collect_pipeline(
        self, ticket: CollectionTicket, request: PipelineRequest, *, resume_from: UUID | None = None
    ) -> PipelineSummary:
        lease_names = self.pipeline._lease_names(request)
        waiting = False
        while True:
            async with self.sessions() as db:
                busy = await db.scalar(
                    select(JobLease.name)
                    .where(JobLease.name.in_(lease_names), JobLease.expires_at > datetime.now(UTC))
                    .limit(1)
                )
            if not busy:
                break
            if not waiting:
                await self._stage(
                    ticket,
                    request.mode,
                    StepResult("running", "Aguardando a coleta que já usa a fonte"),
                )
                waiting = True
            await asyncio.sleep(3)
        if waiting:
            await self._stage(ticket, request.mode, StepResult("running"))
        child_id: UUID | None = None
        try:
            async with self._locked(ticket) as (db, _):
                if resume_from is not None:
                    child, filters = await create_continuation(db, resume_from, require_latest=True)
                    child_id = child.id
                    request = pipeline_request_from_filters(filters)
            request = replace(request, parent_run_id=ticket.run_id)
            summary = (
                await self.pipeline.run(request, run_id=child_id)
                if child_id is not None
                else await self.pipeline.run(request)
            )
        except asyncio.CancelledError:
            if child_id is not None:
                await self._cancel_unstarted_child(child_id)
            raise
        if summary.status is CrawlRunStatus.CANCELLED:
            raise _CollectionStopped("Etapa cancelada")
        return summary

    async def _cancel_unstarted_child(self, run_id: UUID) -> None:
        async with write_session(self.sessions) as db:
            run = await db.get(CrawlRun, run_id)
            if run is not None and run.status is CrawlRunStatus.PENDING:
                await finalize_cancelled_run(db, run)

    async def _procurements(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        request = replace(
            self._request(scope, process_documents=True),
            modalities=scope.modalities or tuple(self.settings.default_modalities),
            object_scope=scope.object_scope,
            calendar_months=use_calendar_slices(scope.start_date, scope.end_date),
        )
        summary = await self._collect_pipeline(ticket, request)
        docs = summary.source_results.get("documents", {})
        return StepResult(
            summary.status.value,
            f"{summary.records_created} contratações novas · "
            f"{docs.get('documents_processed', 0)} documentos analisados · "
            f"{docs.get('leads_created', 0)} oportunidades geradas",
            [summary.run_id],
        )

    async def _registries(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        async with self.sessions() as db:
            scopes = tuple(await known_price_registry_scopes(db, scope.uf))
        if not scopes:
            return StepResult("skipped", "Nenhuma unidade conhecida nesta UF")
        request = self._request(
            scope,
            mode="price_registries",
            process_documents=False,
            price_registry_scopes=scopes,
        )
        run_ids: list[UUID] = []
        records_created = 0
        seen = set()
        last_revalidation_progress: int | None = None
        summary = await self._collect_pipeline(ticket, request)
        while True:
            run_ids.append(summary.run_id)
            records_created += summary.records_created
            note = f"{records_created} atas novas · {len(scopes)} unidades"
            result = StepResult(summary.status.value, note, run_ids)
            if summary.status is not CrawlRunStatus.PARTIAL:
                return result
            checkpoint = await self._registry_checkpoint(summary.run_id)
            if checkpoint is None:
                return result
            batch, work = checkpoint
            state = (
                batch.status,
                tuple(
                    (source, cursor.get("discovery_complete"), cursor.get("total_records"))
                    for source, cursor in sorted(batch.cursors.items())
                ),
                work,
            )
            succeeded = sum(
                count
                for _, kind, status, count in work
                if kind == "record" and status == "succeeded"
            )
            if state in seen or (
                batch.status == "failed"
                and last_revalidation_progress is not None
                and succeeded <= last_revalidation_progress
            ):
                result.note += " · continuação sem progresso"
                return result
            seen.add(state)
            if batch.status == "failed":
                last_revalidation_progress = succeeded
            await self._stage(
                ticket,
                "price_registries",
                StepResult("running", f"{note} · retomando o lote salvo", run_ids),
            )
            try:
                summary = await self._collect_pipeline(ticket, request, resume_from=summary.run_id)
            except (ContinuationBusy, IntegrityError):
                result.note += " · lote retomado por outra execução ou fonte ocupada"
                return result

    async def _registry_checkpoint(
        self, run_id: UUID
    ) -> tuple[CollectionBatch, tuple[tuple[str, str, str, int], ...]] | None:
        async with self.sessions() as db:
            batch = await db.scalar(
                select(CollectionBatch)
                .where(CollectionBatch.last_run_id == run_id)
                .where(CollectionBatch.status.in_(["limited", "failed"]))
            )
            if batch is None:
                return None
            failures = [cursor for cursor in batch.cursors.values() if cursor.get("failed")]
            # A changed page total invalidates the partition. The coordinator marks
            # that discovery failure resumable because it can restart from page 1.
            if batch.status == "failed" and (
                not failures or not all(cursor.get("resumable") for cursor in failures)
            ):
                return None
            identity = (
                CollectionWorkItem.source,
                CollectionWorkItem.kind,
                CollectionWorkItem.status,
            )
            counts = await db.execute(
                select(*identity, func.count())
                .where(
                    CollectionWorkItem.batch_id == batch.id,
                    CollectionWorkItem.kind.in_(["record", "response"]),
                )
                .group_by(*identity)
                .order_by(*identity)
            )
            work = tuple((source, kind, status, count) for source, kind, status, count in counts)
            if any(kind == "record" and status == "failed" for _, kind, status, _ in work):
                return None
            return batch, work

    async def _contracts(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        agencies = self.settings.contract_agency_cnpjs
        if not agencies:
            async with self.sessions() as db:
                agencies = sorted(
                    {
                        cnpj
                        for cnpj in (
                            await db.scalars(
                                select(Procurement.agency_cnpj)
                                .where(
                                    Procurement.uf == scope.uf,
                                    Procurement.agency_cnpj.is_not(None),
                                )
                                .distinct()
                            )
                        ).all()
                        if cnpj and is_valid_cnpj(cnpj)
                    }
                )
        if not agencies:
            return StepResult("skipped", "Nenhum órgão conhecido nesta UF")
        summaries = []
        for agency in dict.fromkeys(agencies):
            summaries.append(
                await self._collect_pipeline(
                    ticket,
                    self._request(
                        scope,
                        mode="contracts",
                        process_documents=False,
                        agency_cnpj=agency,
                    ),
                )
            )
        return StepResult(
            _combined_status([summary.status.value for summary in summaries]),
            f"{sum(item.records_created for item in summaries)} contratos novos · {len(summaries)} órgãos",
            [summary.run_id for summary in summaries],
        )

    async def _payment_step(self, run_ids: list[UUID], missing: bool, label: str) -> StepResult:
        async with self.sessions() as db:
            runs = (await db.scalars(select(CrawlRun).where(CrawlRun.id.in_(run_ids)))).all()
        statuses = [run.status.value for run in runs]
        if missing:
            statuses.append("failed")
        return StepResult(
            _combined_status(statuses),
            f"{sum(run.records_created for run in runs)} registros novos · {label}",
            run_ids,
        )

    async def _municipal(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        sources = selected_municipal_sources(self.settings) or list_municipal_sources(self.settings)
        sources = [source for source in sources if source.uf == scope.uf]
        if not sources:
            return StepResult("skipped", "Nenhum portal municipal integrado nesta UF")
        ids, issues = [], []
        missing = False
        for year in range(scope.start_date.year, scope.end_date.year + 1):
            for source in sources:
                async with self._locked(ticket):
                    pass
                service = MunicipalPaymentIngestionService(
                    self.settings, session_factory=self.sessions
                )
                try:
                    summary = await service.run(source.id, year=year)
                    if summary.crawl_run_id:
                        ids.append(summary.crawl_run_id)
                    else:
                        missing = True
                    if summary.availability.value not in {"available", "empty"}:
                        issues.append(source.name)
                except Exception:
                    missing = True
                    issues.append(source.name)
                finally:
                    await service.aclose()
        note = f"{len(sources)} portais consultados por exercício"
        if issues:
            note += " · verificar " + ", ".join(dict.fromkeys(issues))
        return await self._payment_step(ids, missing, note)

    async def _federal(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        if "transparencia" not in self.settings.payment_sources:
            return StepResult("skipped", "Fonte federal desativada nas configurações")
        if not self.settings.transparencia_api_key.strip():
            return StepResult("skipped", "Chave do Portal da Transparência pendente")
        async with self.sessions() as db:
            targets = await tracked_payment_cnpjs(db, self.settings.payment_poll_cnpjs)
        if not targets:
            return StepResult("skipped", "Nenhum CNPJ ativo em Pagamentos › Acompanhamento federal")
        ids = []
        missing = False
        service = PaymentIngestionService(self.settings, session_factory=self.sessions)
        try:
            for year in range(scope.start_date.year, scope.end_date.year + 1):
                for cnpj in targets:
                    async with self._locked(ticket):
                        pass
                    try:
                        summary = await service.run(company_cnpj=cnpj, year=year)
                        if summary.crawl_run_id:
                            ids.append(summary.crawl_run_id)
                        else:
                            missing = True
                    except Exception:
                        missing = True
        finally:
            await service.aclose()
        return await self._payment_step(ids, missing, f"{len(targets)} favorecidos por exercício")

    async def _tce(self, ticket: CollectionTicket, scope: CollectionScope) -> StepResult:
        from app.services.ingestion.tce_ma_observations import collect_tce_observations

        return await collect_tce_observations(self, ticket, scope)
