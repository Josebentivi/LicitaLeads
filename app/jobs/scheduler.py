"""APScheduler entrypoint; the FastAPI process never imports or starts it."""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]

from app.config import get_settings
from app.database import async_session_factory
from app.logging import configure_logging
from app.repositories.crawls import JobLeaseRepository
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.contacts import ContactEnrichmentService
from app.services.ingestion.processor import DocumentProcessingService
from app.services.ingestion.recovery import reconcile_stale_crawls
from app.services.scheduler_control import scheduler_paused

logger = logging.getLogger(__name__)
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
    async with async_session_factory() as session, session.begin():
        acquired = await JobLeaseRepository(session).acquire(
            name=f"scheduler:{name}",
            owner_id=owner,
            now=now,
            expires_at=now + timedelta(minutes=minutes),
        )
    if not acquired:
        logger.info("scheduled job skipped because its database lease is held", extra={"job": name})
        return
    try:
        await callback()
    except Exception:
        logger.exception("scheduled job failed", extra={"job": name})
    finally:
        async with async_session_factory() as session, session.begin():
            await JobLeaseRepository(session).release(name=f"scheduler:{name}", owner_id=owner)


async def discover_new_procurements() -> None:
    settings = get_settings()
    await IngestionPipeline(settings).run(
        PipelineRequest(
            connector="all",
            uf=settings.default_uf,
            days=settings.default_lookback_days,
            modalities=tuple(settings.default_modalities),
            process_documents=False,
        )
    )


async def refresh_active_procurements() -> None:
    settings = get_settings()
    await IngestionPipeline(settings).run(
        PipelineRequest(
            connector="all",
            uf=settings.default_uf,
            days=min(settings.default_lookback_days, 30),
            modalities=tuple(settings.default_modalities),
            process_documents=False,
        )
    )


async def process_pending_documents() -> None:
    await DocumentProcessingService().process_pending()


async def detect_new_events() -> None:
    service = DocumentProcessingService()
    try:
        await service.detect_existing()
    finally:
        await service.aclose()


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


def build_scheduler() -> AsyncIOScheduler:
    """Build the scheduler without starting it, which keeps imports testable."""

    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone=settings.timezone)
    common = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 300}
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
        partial(_leased, "download_new_documents", process_pending_documents, minutes=120),
        "interval",
        hours=settings.scheduler_documents_hours,
        id="download_new_documents",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "process_pending_documents", process_pending_documents, minutes=120),
        "interval",
        hours=settings.scheduler_documents_hours,
        id="process_pending_documents",
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
        partial(_leased, "create_or_update_leads", create_or_update_leads),
        "interval",
        minutes=settings.scheduler_deadlines_minutes,
        id="create_or_update_leads",
        **common,
    )
    scheduler.add_job(
        partial(_leased, "reconcile_stale_crawls", reconcile_stale_crawls, minutes=10),
        "interval",
        minutes=15,
        id="reconcile_stale_crawls",
        **common,
    )
    return scheduler


async def serve() -> None:
    settings = get_settings()
    configure_logging(logging.INFO)
    if not settings.scheduler_enabled:
        logger.warning("scheduler is disabled by SCHEDULER_ENABLED=false")
        return
    scheduler = build_scheduler()
    scheduler.start()
    logger.info("separate scheduler started", extra={"jobs": len(scheduler.get_jobs())})
    try:
        await asyncio.Event().wait()
    finally:
        scheduler.shutdown(wait=False)


def main() -> None:
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
