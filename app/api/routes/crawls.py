"""Manual crawl creation and status endpoints."""

from __future__ import annotations

import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.dependencies import get_db
from app.models import CrawlRun
from app.schemas import CrawlRunRequest
from app.services.ingestion import IngestionPipeline, PipelineRequest

router = APIRouter(prefix="/crawls", tags=["crawls"])
_running_tasks: set[asyncio.Task[None]] = set()


def pipeline_request(value: CrawlRunRequest) -> PipelineRequest:
    """Translate the public schema into the service's immutable request."""

    return PipelineRequest(
        connector=value.connector,
        uf=value.uf,
        days=value.days,
        start_date=value.start_date,
        end_date=value.end_date,
        modalities=tuple(value.modalities),
        municipality=value.municipality,
        agency=value.agency,
        keyword=value.keyword,
        document_batch_size=value.document_batch_size,
    )


async def _execute(run_id: UUID, request: PipelineRequest) -> None:
    await IngestionPipeline().run(request, run_id=run_id)


def launch_crawl(run_id: UUID, request: PipelineRequest) -> None:
    """Schedule work on the server loop while retaining a strong task reference."""

    task = asyncio.create_task(_execute(run_id, request), name=f"crawl:{run_id}")
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)


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


@router.get("")
async def list_crawls(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    connector: str | None = None,
    run_status: str | None = Query(None, alias="status"),
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
    return {
        "items": [model_dict(row) for row in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


@router.get("/{run_id}")
async def get_crawl(run_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Crawl run not found")
    return model_dict(run)
