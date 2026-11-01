"""Bounded company dossiers and explicit, idempotent priority recalculation."""

from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.dependencies import get_db
from app.models import JobLease, Lead, LeadScoreRequest
from app.repositories.crawls import JobLeaseRepository
from app.services.company_overview import CompanyOverviewService, Section, find_company
from app.services.lead_scoring.evaluation import evaluate_lead

router = APIRouter(prefix="/companies", tags=["companies"])


class RescoreRequest(BaseModel):
    request_id: UUID
    lead_ids: Annotated[list[UUID], Field(min_length=1, max_length=50)]


@router.get("/{cnpj}/overview")
async def overview(cnpj: str, db: AsyncSession = Depends(get_db)):
    return await CompanyOverviewService(db, await find_company(db, cnpj)).overview()


@router.get("/{cnpj}/sections/{section}")
async def section_page(
    cnpj: str,
    section: Section,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    historical: bool = False,
    status: str | None = None,
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    return await CompanyOverviewService(db, await find_company(db, cnpj)).page(
        section,
        page=page,
        page_size=page_size,
        historical=historical,
        status=status,
        active_only=active_only,
    )


@router.get("/{cnpj}/sections/{section}/{identifier}")
async def section_detail(
    cnpj: str,
    section: Section,
    identifier: UUID,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    return await CompanyOverviewService(db, await find_company(db, cnpj)).detail(
        section, identifier, page=page
    )


@router.post("/{cnpj}/rescore")
async def rescore(cnpj: str, payload: RescoreRequest, db: AsyncSession = Depends(get_db)):
    company = await find_company(db, cnpj)
    company_id = company.id
    ids = sorted(set(payload.lead_ids), key=str)
    receipt = await db.get(LeadScoreRequest, payload.request_id)
    if receipt is not None:
        if receipt.company_id != company_id or receipt.lead_ids != [str(x) for x in ids]:
            raise HTTPException(409, "Identificador de requisição já usado para outra seleção")
        return {
            "request_id": str(payload.request_id),
            "items": [dict(item, reused=True) for item in receipt.results],
        }
    rows = list(
        (
            await db.scalars(
                select(Lead)
                .where(Lead.id.in_(ids), Lead.company_id == company_id)
                .order_by(Lead.id)
            )
        ).all()
    )
    if len(rows) != len(ids):
        raise HTTPException(404, "Lead não encontrado para esta empresa")
    # End the read snapshot before acquiring the writer admission (important on SQLite).
    await db.rollback()
    factory = async_sessionmaker(db.bind, expire_on_commit=False)
    owner, name = str(uuid4()), f"rescore:company:{company_id}"
    now = datetime.now(UTC)
    try:
        async with factory() as lease_session, lease_session.begin():
            repository = JobLeaseRepository(lease_session)
            if not await repository.acquire(
                name=name, owner_id=owner, now=now, expires_at=now + timedelta(minutes=2)
            ):
                raise HTTPException(409, "Recálculo concorrente ou manutenção em andamento")
            lease = await lease_session.get(JobLease, name)
            assert lease is not None
            generation = lease.version
    except OperationalError as exc:
        raise HTTPException(409, "Escrita concorrente; tente novamente") from exc
    try:
        async with db.begin():
            if not await JobLeaseRepository(db).fence(
                name=name, owner_id=owner, generation=generation, now=datetime.now(UTC)
            ):
                raise HTTPException(409, "A execução perdeu a lease")
            rows = list(
                (
                    await db.scalars(
                        select(Lead)
                        .where(Lead.id.in_(ids), Lead.company_id == company_id)
                        .order_by(Lead.id)
                        .with_for_update()
                    )
                ).all()
            )
            if len(rows) != len(ids):
                raise HTTPException(409, "A seleção foi alterada; atualize a página")
            results = [
                await evaluate_lead(
                    db,
                    lead,
                    explicit=True,
                    request_id=payload.request_id,
                    origin="company_explicit",
                )
                for lead in rows
            ]
            db.add(
                LeadScoreRequest(
                    id=payload.request_id,
                    company_id=company_id,
                    lead_ids=[str(x) for x in ids],
                    results=results,
                )
            )
            if not await JobLeaseRepository(db).fence(
                name=name, owner_id=owner, generation=generation, now=datetime.now(UTC)
            ):
                raise HTTPException(409, "A execução perdeu a lease")
        return {"request_id": str(payload.request_id), "items": results}
    except (OperationalError, IntegrityError) as exc:
        raise HTTPException(
            409, "Dados ou requisição alterados por outra execução; atualize e tente novamente"
        ) from exc
    finally:
        async with factory() as lease_session, lease_session.begin():
            await JobLeaseRepository(lease_session).release(
                name=name, owner_id=owner, generation=generation
            )
