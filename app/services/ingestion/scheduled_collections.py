"""Durable schedule windows; only fenced pipeline commits advance coverage."""

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import write_session
from app.models import CollectionBatch, CollectionSyncState, CrawlRunStatus
from app.services.ingestion.collection_batches import digest, latest_limited_batch
from app.services.ingestion.continuation import pipeline_request_from_filters

logger = logging.getLogger(__name__)


def sync_identity(request: Any, settings: Any) -> dict[str, Any]:
    values = request.as_json()
    for key in (
        "start_date",
        "end_date",
        "continuation_batch_id",
        "resume_automatically",
        "force_refresh",
        "sync_context",
        "external_ids",
        "document_ids",
        "retry_of_run_id",
        "recheck_key",
    ):
        values.pop(key, None)
    if request.connector == "pncp" and request.mode == "procurements":
        values["discovery_kind"] = "update"
    values["api"] = (
        [settings.pncp_base_url, settings.pncp_integration_base_url]
        if request.connector == "pncp"
        else [settings.compras_gov_base_url]
    )
    values["modalities"] = sorted(set(values.get("modalities") or []))
    if values.get("price_registry_scopes"):
        values["price_registry_scopes"] = sorted(values["price_registry_scopes"], key=digest)
    return values


async def prepare_scheduled(
    sessions: Any, settings: Any, request: Any, *, now: datetime | None = None
) -> Any:
    now = now or datetime.now(UTC)
    today = now.astimezone(ZoneInfo(settings.timezone)).date()
    identity = sync_identity(request, settings)
    key = digest(identity)
    async with write_session(sessions) as session:
        state = await session.scalar(
            select(CollectionSyncState).where(CollectionSyncState.stream_key == key)
        )
        if state is None:
            state = CollectionSyncState(stream_key=key, filters=identity, bootstrap_started_at=now)
            session.add(state)
            await session.flush()
        if state.pending_request:
            pending = dict(state.pending_request)
            if state.pending_batch_id:
                batch = await session.get(CollectionBatch, state.pending_batch_id)
                if batch is None or batch.status != "limited":
                    logger.info(
                        "scheduled stream awaits explicit continuation",
                        extra={
                            "context": {"stream_key": key, "batch_id": str(state.pending_batch_id)}
                        },
                    )
                    return None
                pending["continuation_batch_id"] = str(batch.id)
            return pipeline_request_from_filters(pending)
        review = bool(
            state.covered_until
            and (
                state.last_reconciliation_at is None
                or now - state.last_reconciliation_at
                >= timedelta(days=settings.collection_reconciliation_days)
            )
        )
        bootstrap = state.covered_until is None
        if bootstrap or review:
            end = request.end_date or today
            start = request.start_date or end - timedelta(
                days=(request.days or settings.default_lookback_days) - 1
            )
            kind = request.discovery_kind
            strategy = "bootstrap" if bootstrap else "reconciliation"
        else:
            assert state.covered_until is not None
            start = state.covered_until - timedelta(
                days=settings.collection_incremental_overlap_days - 1
            )
            end = today
            kind = "update" if request.connector == "pncp" else request.discovery_kind
            strategy = "incremental"
        context = {
            "key": key,
            "strategy": strategy,
            "phase": strategy,
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "initial_kind": kind,
            "review": review or bootstrap,
            "timezone": settings.timezone,
            "started_at": now.isoformat(),
            "overlap_days": settings.collection_incremental_overlap_days,
        }
        planned = replace(
            request,
            start_date=start,
            end_date=end,
            discovery_kind=kind,
            resume_automatically=True,
            sync_context=context,
        )
        state.pending_request = planned.as_json()
    if bootstrap:
        old = await latest_limited_batch(sessions, request.as_json())
        if old is not None:
            async with write_session(sessions) as session:
                state = await session.scalar(
                    select(CollectionSyncState).where(CollectionSyncState.stream_key == key)
                )
                assert state is not None
                batch = await session.get_one(CollectionBatch, old)
                restored = pipeline_request_from_filters(batch.filters)
                context = {
                    **context,
                    "period_start": batch.filters["start_date"],
                    "period_end": batch.filters["end_date"],
                    "initial_kind": restored.discovery_kind,
                }
                planned = replace(
                    restored,
                    continuation_batch_id=old,
                    resume_automatically=True,
                    sync_context=context,
                )
                state.pending_request = planned.as_json()
                state.pending_batch_id = old
    return planned


async def bind_sync_batch(
    session: AsyncSession, filters: dict[str, Any], batch: CollectionBatch
) -> None:
    context = filters.get("sync_context")
    if not isinstance(context, dict):
        return
    state = await session.scalar(
        select(CollectionSyncState).where(CollectionSyncState.stream_key == context["key"])
    )
    if state is None or state.pending_request is None:
        raise ValueError("estado da sincronização automática não encontrado")
    if state.pending_batch_id is not None and state.pending_batch_id != batch.id:
        raise ValueError("outro lote já possui esta sincronização")
    state.pending_batch_id = batch.id
    batch.filters = {**batch.filters, "sync_context": context}


async def finish_sync(
    session: AsyncSession, run: Any, batch: CollectionBatch, status: CrawlRunStatus
) -> None:
    context = batch.filters.get("sync_context")
    if (
        not isinstance(context, dict)
        or status is not CrawlRunStatus.COMPLETED
        or batch.status != "complete"
    ):
        return
    state = await session.scalar(
        select(CollectionSyncState).where(CollectionSyncState.stream_key == context["key"])
    )
    if state is None or state.pending_batch_id != batch.id:
        return
    now = datetime.now(UTC)
    if (
        batch.filters.get("connector") == "pncp"
        and context["phase"] in {"bootstrap", "reconciliation"}
        and context["initial_kind"] != "update"
    ):
        end = now.astimezone(ZoneInfo(context["timezone"])).date()
        started = (
            datetime.fromisoformat(context["started_at"])
            .astimezone(ZoneInfo(context["timezone"]))
            .date()
        )
        start = started - timedelta(days=context["overlap_days"] - 1)
        next_context = {
            **context,
            "phase": "catchup",
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
        }
        state.pending_request = {
            **batch.filters,
            "sync_context": next_context,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "discovery_kind": "update",
            "continuation_batch_id": None,
        }
        state.pending_batch_id = None
        return
    end = date.fromisoformat(context["period_end"])
    state.covered_until = max(state.covered_until, end) if state.covered_until else end
    if context["review"]:
        state.last_reconciliation_at = now
    state.pending_request, state.pending_batch_id = None, None


async def run_scheduled_collection(
    pipeline: Any, request: Any, settings: Any, sessions: Any
) -> None:
    async def one(source: str) -> None:
        from app.services.ingestion.pipeline import PipelineBusy

        scopes = tuple(scope for scope in request.price_registry_scopes if source in scope.sources)
        if request.mode == "price_registries" and not scopes:
            logger.info("scheduled ARP source skipped: no eligible agency/unit")
            return
        original = replace(
            request,
            connector=source,
            process_documents=False,
            discovery_kind="publication" if source == "compras_gov" else request.discovery_kind,
            price_registry_scopes=scopes,
        )
        for _ in range(2):
            planned = await prepare_scheduled(sessions, settings, original)
            if planned is None:
                return
            try:
                result = await pipeline.run(planned)
            except PipelineBusy:
                logger.info("scheduled collection deferred because its source is busy")
                return
            if result is None or result.status is not CrawlRunStatus.COMPLETED:
                return
            async with sessions() as session:
                state = await session.scalar(
                    select(CollectionSyncState).where(
                        CollectionSyncState.stream_key == planned.sync_context["key"]
                    )
                )
                if not state or not state.pending_request or state.pending_batch_id:
                    return
                if state.pending_request.get("sync_context", {}).get("phase") != "catchup":
                    return

    sources = ["pncp", "compras_gov"] if request.connector == "all" else [request.connector]
    tasks = [asyncio.create_task(one(source)) for source in sources]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                logger.error(
                    "scheduled collection failed",
                    exc_info=(type(result), result, result.__traceback__),
                )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
