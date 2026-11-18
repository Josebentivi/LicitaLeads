"""Local AI operations; these endpoints never call a provider synchronously."""

import asyncio
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import get_db
from app.repositories.crawls import JobLeaseRepository
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.llm import central
from app.services.llm.queue import LLMQueueBusy
from app.services.scheduler_control import control_state, set_component_paused

router = APIRouter(prefix="/llm", tags=["llm"])


class ControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    paused: bool


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    company_id: UUID | None = None
    reviewer: str | None = Field(default=None, max_length=200)
    note: str | None = Field(default=None, max_length=1000)


def filters(
    run: UUID | None = None,
    procurement: UUID | None = None,
    company: str = Query(default="", max_length=30),
    source: str = Query(default="", max_length=50),
    status: str = Query(default="", max_length=30),
) -> dict[str, str]:
    if company and not is_valid_cnpj(normalize_cnpj(company)):
        raise HTTPException(422, "CNPJ inválido")
    if status and status not in {
        "pending",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "obsolete",
        "confirmed",
        "rejected",
        "blocked",
        "reused",
    }:
        raise HTTPException(422, "Estado desconhecido")
    return {
        "run": str(run) if run else "",
        "procurement": str(procurement) if procurement else "",
        "company": company,
        "source": source,
        "status": status,
    }


def action_error(exc: Exception) -> HTTPException:
    return HTTPException(404 if isinstance(exc, LookupError) else 409, str(exc))


@router.get("/overview")
async def overview(db: AsyncSession = Depends(get_db)):
    return await central.overview(db, get_settings())


@router.get("/documents")
async def documents(
    params: dict[str, str] = Depends(filters),
    page: int = Query(default=1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    return await central.documents(db, params, page)


@router.get("/documents/{identifier}")
async def document_detail(
    identifier: UUID, page: int = Query(default=1, ge=1), db: AsyncSession = Depends(get_db)
):
    try:
        return await central.document_detail(db, identifier, page)
    except LookupError as exc:
        raise action_error(exc) from None


@router.get("/jobs/{identifier}")
async def job_detail(identifier: UUID, db: AsyncSession = Depends(get_db)):
    try:
        return await central.job_detail(db, identifier)
    except LookupError as exc:
        raise action_error(exc) from None


@router.get("/suggestions")
async def suggestions(
    params: dict[str, str] = Depends(filters),
    page: int = Query(default=1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    return await central.suggestions(db, params, page)


@router.get("/suggestions/{identifier}")
async def suggestion_detail(identifier: UUID, db: AsyncSession = Depends(get_db)):
    try:
        return await central.suggestion_detail(db, identifier)
    except LookupError as exc:
        raise action_error(exc) from None


@router.post("/controls/{component}")
async def control(
    component: Literal["collections", "llm"],
    payload: ControlRequest,
    db: AsyncSession = Depends(get_db),
):
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    await asyncio.to_thread(
        set_component_paused, component, payload.paused, settings=get_settings()
    )
    await db.commit()
    return control_state(get_settings())


@router.post("/documents/{identifier}/enqueue", status_code=202)
async def enqueue(identifier: UUID, db: AsyncSession = Depends(get_db)):
    try:
        count = await central.enqueue(db, identifier, get_settings())
        await db.commit()
        return {"enqueued": count}
    except (LookupError, ValueError, LLMQueueBusy) as exc:
        raise action_error(exc) from None


@router.post("/batches/{identifier}/{action}", status_code=202)
async def batch_action(
    identifier: UUID, action: Literal["retry", "cancel"], db: AsyncSession = Depends(get_db)
):
    try:
        count = await central.batch_action(db, identifier, action, get_settings())
        await db.commit()
        return {"accepted": count}
    except (LookupError, ValueError, LLMQueueBusy) as exc:
        raise action_error(exc) from None


@router.post("/suggestions/{identifier}/{action}")
async def review(
    identifier: UUID,
    action: Literal["confirm", "reject"],
    payload: ReviewRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        await central.review_suggestion(
            db,
            identifier,
            confirm=action == "confirm",
            company_id=payload.company_id,
            reviewer=payload.reviewer,
            note=payload.note,
            settings=get_settings(),
        )
        await db.commit()
        return {"status": "confirmed" if action == "confirm" else "rejected"}
    except (LookupError, ValueError) as exc:
        raise action_error(exc) from None
