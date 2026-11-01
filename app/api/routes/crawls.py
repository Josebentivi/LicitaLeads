"""Manual crawl creation and status endpoints."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.config import get_settings
from app.dependencies import get_db
from app.models import CrawlRun, CrawlRunStatus, JobLease
from app.repositories.crawls import JobLeaseRepository
from app.schemas import CrawlRunRequest
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.continuation import (
    ContinuationBusy,
    create_continuation,
    execute_continuation,
)
from app.services.ingestion.document_retry import DOCUMENT_LEASE, create_document_retry
from app.services.ingestion.municipal_payments import cancel_municipal_task
from app.services.ingestion.recovery import finalize_cancelled_run
from app.services.llm.queue import cancel_for_run, queue_status, queue_statuses, retry_for_run

router = APIRouter(prefix="/crawls", tags=["crawls"])
_running_tasks: dict[UUID, asyncio.Task[None]] = {}


def pipeline_request(value: CrawlRunRequest) -> PipelineRequest:
    """Translate the public schema into the service's immutable request."""

    return PipelineRequest(
        connector=value.connector,
        mode=value.mode,
        uf=value.uf,
        days=value.days,
        start_date=value.start_date,
        end_date=value.end_date,
        modalities=tuple(value.modalities),
        municipality=value.municipality,
        agency=value.agency,
        agency_cnpj=value.agency_cnpj,
        unit_code=value.unit_code,
        keyword=value.keyword,
        document_batch_size=value.document_batch_size,
        discovery_kind=value.discovery_kind,
        force_refresh=value.force_refresh,
    )


async def _execute(run_id: UUID, request: PipelineRequest) -> None:
    await IngestionPipeline().run(request, run_id=run_id)


def launch_crawl(run_id: UUID, request: PipelineRequest) -> None:
    """Schedule work on the server loop while retaining a strong task reference."""

    task = asyncio.create_task(_execute(run_id, request), name=f"crawl:{run_id}")
    _running_tasks[run_id] = task

    def _discard(finished: asyncio.Task[None], run_id: UUID = run_id) -> None:
        del finished
        _running_tasks.pop(run_id, None)

    task.add_done_callback(_discard)


def _crawl_lease_names(run: CrawlRun) -> list[str]:
    filters = run.filters if isinstance(run.filters, dict) else {}
    payment_lease = filters.get("lease_name")
    if (
        filters.get("mode") in {"payments", "payment_import"}
        and isinstance(payment_lease, str)
        and payment_lease.startswith("payments:")
    ):
        return [payment_lease]
    connector = str(filters.get("connector") or run.connector).lower()
    if filters.get("mode") == "documents":
        return [DOCUMENT_LEASE]
    if connector.startswith("municipal:"):
        return [f"municipal:{connector.removeprefix('municipal:')}"]
    uf = str(filters.get("uf") or "").upper()
    if not uf:
        return []
    connectors = ["pncp", "compras_gov"] if connector == "all" else [connector]
    names = [f"crawl:{name}:{uf}" for name in connectors]
    if filters.get("process_documents") and filters.get("mode", "procurements") == "procurements":
        names.append(DOCUMENT_LEASE)
    return names


@router.post("/{run_id}/retry-documents", status_code=status.HTTP_202_ACCEPTED)
async def retry_documents(run_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    try:
        run, request = await create_document_retry(db, run_id, get_settings())
        await db.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    launch_crawl(run.id, request)
    return {
        "run_id": str(run.id),
        "parent_run_id": str(run_id),
        "status": "pending",
        "documents_selected": len(request.document_ids),
    }


async def cancel_crawl(run_id: UUID, db: AsyncSession) -> bool:
    """Persist a cancellation request and stop the local task, when present.

    A run left behind by a restarted API has no in-memory task. In that case
    (and only when no live lease belongs to the run) it is finalized immediately
    instead of remaining stuck as ``running`` forever.
    """

    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Crawl run not found")
    if run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
        accepted = await cancel_for_run(db, run_id)
        await db.commit()
        return accepted
    run.cancel_requested = True
    await cancel_for_run(db, run_id)
    task = _running_tasks.get(run_id)
    if task is not None and not task.done():
        await db.commit()
        task.cancel()
        return True
    if cancel_municipal_task(run_id):
        await db.commit()
        return True

    lease_names = _crawl_lease_names(run)
    leases = []
    if lease_names:
        leases = list(
            (
                await db.scalars(select(JobLease.owner_id).where(JobLease.name.in_(lease_names)))
            ).all()
        )
    if not leases:
        await finalize_cancelled_run(db, run)
    await db.commit()
    return True


async def cancel_all_running_crawls() -> None:
    """Cancel and drain background crawl tasks during application shutdown."""

    tasks = [task for task in _running_tasks.values() if not task.done()]
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    _running_tasks.clear()


@router.post("/run", status_code=status.HTTP_202_ACCEPTED)
async def run_crawl(
    payload: CrawlRunRequest,
) -> dict[str, object]:
    """Create an auditable run and execute it after the 202 response."""

    request = pipeline_request(payload)
    run = await IngestionPipeline().create_run(request)
    launch_crawl(run.id, request)
    return {
        "id": str(run.id),
        "status": run.status.value,
        "connector": run.connector,
        "created_at": run.created_at.isoformat(),
    }


@router.post("/{run_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_crawl_endpoint(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """Request cooperative cancellation of a pending/running crawl."""

    accepted = await cancel_crawl(run_id, db)
    if not accepted:
        raise HTTPException(
            status_code=409,
            detail="Crawl run is not pending or running",
        )
    return {"id": str(run_id), "status": "cancellation_requested"}


@router.get("")
async def list_crawls(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    connector: str | None = None,
    run_status: CrawlRunStatus | None = Query(None, alias="status"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List crawl runs in reverse chronological order."""

    statement = select(CrawlRun)
    count = select(func.count()).select_from(CrawlRun)
    if connector:
        statement = statement.where(CrawlRun.connector == connector)
        count = count.where(CrawlRun.connector == connector)
    if run_status:
        statement = statement.where(CrawlRun.status == run_status)
        count = count.where(CrawlRun.status == run_status)
    total = int(await db.scalar(count) or 0)
    rows = list(
        (
            await db.scalars(
                statement.order_by(CrawlRun.created_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        ).all()
    )
    summaries = await queue_statuses(db, get_settings(), run_ids=[row.id for row in rows])
    return {
        "items": [{**model_dict(row), "llm": summaries[row.id]} for row in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@router.get("/{run_id}")
async def get_crawl(run_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Crawl run not found")
    return {**model_dict(run), "llm": await queue_status(db, get_settings(), run_id=run.id)}


@router.post("/{run_id}/retry-llm", status_code=202)
async def retry_llm(run_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    try:
        count = await retry_for_run(db, run_id, get_settings())
        await db.commit()
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    return {"run_id": str(run_id), "status": "pending", "analyses_selected": count}


@router.post("/{run_id}/continue", status_code=202)
async def continue_crawl_endpoint(
    run_id: UUID, db: AsyncSession = Depends(get_db)
) -> dict[str, object]:
    try:
        run, filters = await create_continuation(db, run_id)
        await db.commit()
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ContinuationBusy, IntegrityError) as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Não é possível continuar: coleta ativa ou sem trabalho pendente"
            if isinstance(exc, IntegrityError)
            else str(exc),
        ) from exc
    launch_continuation(run.id, filters)
    return {
        "id": str(run.id),
        "status": run.status.value,
        "connector": run.connector,
        "batch_id": str(run.batch_id),
    }


def launch_continuation(run_id: UUID, filters: dict[str, object]) -> None:
    task = asyncio.create_task(execute_continuation(run_id, filters), name=f"crawl:{run_id}")
    _running_tasks[run_id] = task
    task.add_done_callback(lambda _task: _running_tasks.pop(run_id, None))
