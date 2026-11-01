"""APScheduler entrypoint; the FastAPI process never imports or starts it."""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
from sqlalchemy import and_, func, or_, select, update

from app.config import get_settings
from app.database import async_session_factory, write_session
from app.logging import configure_logging
from app.models import Procurement, ProcurementPhase, ProcurementSource
from app.repositories.crawls import JobLeaseRepository
from app.repositories.procurements import _status_category_condition
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.collection_rechecks import recheck_compras
from app.services.ingestion.contacts import ContactEnrichmentService
from app.services.ingestion.municipal_payments import run_selected_municipal_sources
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.pipeline import PipelineBusy
from app.services.ingestion.processor import DocumentProcessingService
from app.services.ingestion.recovery import reconcile_stale_crawls
from app.services.ingestion.scheduled_collections import run_scheduled_collection
from app.services.price_registry_scopes import known_price_registry_scopes
from app.services.scheduler_control import (
    clear_scheduler_heartbeat,
    scheduler_paused,
    write_scheduler_heartbeat,
)

logger = logging.getLogger(__name__)


class _ScheduledPipeline:
    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.pipeline = IngestionPipeline(settings)

    async def run(self, request: PipelineRequest) -> Any:
        if request.external_ids:
            return await self.pipeline.run(request)
        await run_scheduled_collection(self.pipeline, request, self.settings, async_session_factory)
        if request.mode in {"price_registries", "contracts"}:
            await recheck_compras(self.pipeline, request, self.settings, async_session_factory)
        return None


JobCallable = Callable[[], Awaitable[object]]


async def _leased(name: str, callback: JobCallable, *, minutes: int = 55) -> None:
    if scheduler_paused():
        logger.info(
            "scheduled job skipped because automatic collections are paused",
            extra={"job": name},
        )
        return
    owner = f"{socket.gethostname()}:{uuid4()}"
    now = datetime.now(UTC)
    async with write_session(async_session_factory) as session:
        acquired = await JobLeaseRepository(session).acquire(
            name=f"scheduler:{name}",
            owner_id=owner,
            now=now,
            expires_at=now + timedelta(minutes=minutes),
        )
    if not acquired:
        logger.info("scheduled job skipped because its database lease is held", extra={"job": name})
        return
    lease_name = f"scheduler:{name}"
    heartbeat = asyncio.create_task(
        _renew_scheduler_lease(lease_name, owner, minutes=minutes, worker=asyncio.current_task()),
        name=f"lease-heartbeat:{name}",
    )
    try:
        await callback()
    except Exception:
        logger.exception("scheduled job failed", extra={"job": name})
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await heartbeat
        async with write_session(async_session_factory) as session:
            await JobLeaseRepository(session).release(name=lease_name, owner_id=owner)


async def _renew_scheduler_lease(
    lease_name: str, owner: str, *, minutes: int, worker: asyncio.Task[object] | None = None
) -> None:
    while True:
        await asyncio.sleep(60)
        now = datetime.now(UTC)
        async with write_session(async_session_factory) as session:
            renewed = await JobLeaseRepository(session).renew(
                name=lease_name,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(minutes=minutes),
            )
        if not renewed:
            logger.error("scheduler lease lost", extra={"lease": lease_name})
            if worker is not None:
                worker.cancel()
            return


async def discover_new_procurements() -> None:
    settings = get_settings()
    await _ScheduledPipeline(settings).run(
        PipelineRequest(
            resume_automatically=True,
            connector="all",
            uf=settings.default_uf,
            days=settings.default_lookback_days,
            modalities=tuple(settings.default_modalities),
            process_documents=False,
        )
    )


async def discover_price_registries() -> None:
    settings = get_settings()
    async with async_session_factory() as session:
        scopes = tuple(await known_price_registry_scopes(session, settings.default_uf))
    if not scopes:
        logger.warning("automatic ARP discovery skipped: no known agency/unit in configured UF")
        return
    await _ScheduledPipeline(settings).run(
        PipelineRequest(
            resume_automatically=True,
            connector="all",
            mode="price_registries",
            uf=settings.default_uf,
            days=settings.effective_price_registry_lookback_days,
            modalities=(),
            process_documents=False,
            price_registry_scopes=scopes,
        )
    )


async def refresh_active_procurements() -> None:
    settings = get_settings()
    lookback = min(settings.default_lookback_days, 30)
    pipeline = _ScheduledPipeline(settings)
    await pipeline.run(
        PipelineRequest(
            resume_automatically=True,
            connector="all",
            uf=settings.default_uf,
            days=lookback,
            modalities=tuple(settings.default_modalities),
            process_documents=False,
            discovery_kind="update",
        )
    )

    # Publication windows cannot find older processes whose details changed.
    # Rotate oldest observations so bounded jobs eventually revisit every
    # active process, including one published outside the discovery window.
    now = datetime.now(UTC)
    for source in ("pncp",):
        async with write_session(async_session_factory) as session:
            identifiers = tuple(
                (
                    await session.scalars(
                        select(ProcurementSource.external_id)
                        .join(Procurement, Procurement.id == ProcurementSource.procurement_id)
                        .where(
                            ProcurementSource.source == source,
                            Procurement.uf == settings.default_uf.upper(),
                            ~and_(*_status_category_condition("cancelada", now=now)),
                            or_(
                                and_(*_status_category_condition("aberta", now=now)),
                                Procurement.current_phase.in_(
                                    [
                                        ProcurementPhase.DISPUTE,
                                        ProcurementPhase.DILIGENCE,
                                        ProcurementPhase.JUDGMENT,
                                        ProcurementPhase.APPEAL,
                                        ProcurementPhase.COUNTERARGUMENT,
                                        ProcurementPhase.QUALIFICATION,
                                        ProcurementPhase.ADJUDICATION,
                                    ]
                                ),
                            ),
                        )
                        .order_by(
                            func.coalesce(
                                ProcurementSource.last_refresh_attempt_at,
                                ProcurementSource.collected_at,
                            ),
                            ProcurementSource.id,
                        )
                        .limit(settings.active_refresh_batch_size)
                    )
                ).all()
            )
        if identifiers:
            try:
                await pipeline.run(
                    PipelineRequest(
                        resume_automatically=True,
                        connector=source,
                        uf=settings.default_uf,
                        external_ids=identifiers,
                        force_refresh=True,
                        process_documents=False,
                    )
                )
            except PipelineBusy:
                logger.info("scheduled refresh deferred because its source is busy")
                continue
            async with write_session(async_session_factory) as session:
                await session.execute(
                    update(ProcurementSource)
                    .where(
                        ProcurementSource.source == source,
                        ProcurementSource.external_id.in_(identifiers),
                    )
                    .values(last_refresh_attempt_at=now)
                )
    await recheck_compras(
        pipeline.pipeline,
        PipelineRequest(
            connector="compras_gov",
            uf=settings.default_uf,
            days=lookback,
            modalities=tuple(settings.default_modalities),
            process_documents=False,
        ),
        settings,
        async_session_factory,
    )


async def process_pending_documents() -> None:
    await DocumentProcessingService().process_pending()


async def recheck_session_documents() -> None:
    service = DocumentProcessingService()
    try:
        await service.recheck_stale_session_documents()
    finally:
        await service.aclose()


async def detect_new_events() -> None:
    service = DocumentProcessingService()
    try:
        await service.detect_existing()
    finally:
        await service.aclose()


async def process_llm_queue() -> None:
    from app.services.llm.queue import LLMQueueService

    await LLMQueueService().drain()


async def recalculate_deadlines() -> None:
    service = DocumentProcessingService()
    try:
        await service.recalculate_deadlines_and_leads()
    finally:
        await service.aclose()


async def enrich_pending_companies() -> None:
    await ContactEnrichmentService().run()


async def create_or_update_leads() -> None:
    await recalculate_deadlines()


async def crawl_configured_contracts() -> None:
    settings = get_settings()
    for agency_cnpj in settings.contract_agency_cnpjs:
        await _ScheduledPipeline(settings).run(
            PipelineRequest(
                resume_automatically=True,
                connector="all",
                mode="contracts",
                days=max(settings.default_lookback_days, 30),
                agency_cnpj=agency_cnpj,
                process_documents=False,
            )
        )


async def poll_configured_payments() -> None:
    settings = get_settings()
    year = datetime.now(ZoneInfo(settings.timezone)).year
    for company_cnpj in settings.payment_poll_cnpjs:
        service = PaymentIngestionService(settings)
        try:
            result = await service.run(
                company_cnpj=company_cnpj, year=year, resume_automatically=True
            )
            if result.availability.value not in {"available", "empty"}:
                raise RuntimeError(
                    f"federal payment collection failed: {result.diagnostic or result.availability}"
                )
        finally:
            await service.aclose()


async def poll_municipal_payments() -> None:
    settings = get_settings()
    await run_selected_municipal_sources(
        settings,
        year=datetime.now(ZoneInfo(settings.timezone)).year,
        resume_automatically=True,
    )


def build_scheduler() -> AsyncIOScheduler:
    """Build the scheduler without starting it, which keeps imports testable."""

    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone=settings.timezone)
    common = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 300}
    scheduler.add_job(
        partial(_leased, "discover_price_registries", discover_price_registries, minutes=120),
        "interval",
        hours=settings.scheduler_price_registry_hours,
        next_run_time=datetime.now(UTC),
        id="discover_price_registries",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "discover_new_procurements", discover_new_procurements, minutes=120),
        "interval",
        hours=settings.scheduler_discovery_hours,
        id="discover_new_procurements",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "refresh_active_procurements", refresh_active_procurements),
        "interval",
        minutes=settings.scheduler_active_refresh_minutes,
        id="refresh_active_procurements",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "process_pending_documents", process_pending_documents, minutes=120),
        "interval",
        hours=settings.scheduler_documents_hours,
        id="process_pending_documents",
        **common,
    )
    if settings.session_poll_enabled:
        scheduler.add_job(
            partial(
                _leased,
                "recheck_session_documents",
                recheck_session_documents,
                minutes=120,
            ),
            "interval",
            minutes=settings.session_poll_minutes,
            id="recheck_session_documents",
            **common,
        )
    scheduler.add_job(
        partial(_leased, "detect_new_events", detect_new_events, minutes=120),
        "interval",
        hours=settings.scheduler_documents_hours,
        id="detect_new_events",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "process_llm_queue", process_llm_queue, minutes=15),
        "interval",
        seconds=5,
        id="process_llm_queue",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "recalculate_deadlines", recalculate_deadlines),
        "interval",
        minutes=settings.scheduler_deadlines_minutes,
        id="recalculate_deadlines",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "enrich_pending_companies", enrich_pending_companies, minutes=120),
        "interval",
        hours=settings.scheduler_contacts_hours,
        id="enrich_pending_companies",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "reconcile_stale_crawls", reconcile_stale_crawls, minutes=10),
        "interval",
        minutes=15,
        id="reconcile_stale_crawls",
        **common,
    )
    if settings.contract_agency_cnpjs:
        scheduler.add_job(
            partial(_leased, "crawl_configured_contracts", crawl_configured_contracts, minutes=120),
            "interval",
            hours=settings.scheduler_contract_hours,
            id="crawl_configured_contracts",
            **common,
        )
    if settings.payment_poll_cnpjs:
        scheduler.add_job(
            partial(_leased, "poll_configured_payments", poll_configured_payments, minutes=120),
            "interval",
            hours=settings.scheduler_payment_hours,
            id="poll_configured_payments",
            **common,
        )
    if settings.municipal_payment_sources:
        scheduler.add_job(
            partial(_leased, "poll_municipal_payments", poll_municipal_payments, minutes=180),
            "interval",
            hours=settings.scheduler_municipal_payment_hours,
            id="poll_municipal_payments",
            **common,
        )
    return scheduler


async def _heartbeat_loop() -> None:
    while True:
        write_scheduler_heartbeat()
        await asyncio.sleep(30)


async def serve() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    if not settings.scheduler_enabled:
        logger.warning("scheduler is disabled by SCHEDULER_ENABLED=false")
        return
    scheduler = build_scheduler()
    scheduler.start()
    heartbeat = asyncio.create_task(_heartbeat_loop(), name="scheduler-heartbeat")
    logger.info("separate scheduler started", extra={"jobs": len(scheduler.get_jobs())})
    try:
        await asyncio.Event().wait()
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await heartbeat
        clear_scheduler_heartbeat()
        scheduler.shutdown(wait=False)


def main() -> None:
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
