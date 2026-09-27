"""Read-only procurement endpoints."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import false, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.dependencies import get_db
from app.models import Document, Participant, Procurement, ProcurementEvent, ProcurementItem
from app.services.identifiers import canonical_modality

router = APIRouter(prefix="/procurements", tags=["procurements"])


def _filters(
    statement,
    *,
    uf: str | None,
    municipality: str | None,
    agency: str | None,
    modality: str | None,
    published_from,
    published_to,
):
    if uf:
        statement = statement.where(Procurement.uf == uf.upper())
    if municipality:
        statement = statement.where(Procurement.municipality.ilike(f"%{municipality}%"))
    if agency:
        statement = statement.where(Procurement.agency_name.ilike(f"%{agency}%"))
    if modality:
        modality_key = canonical_modality(modality)
        statement = statement.where(
            Procurement.modality_key == modality_key if modality_key is not None else false()
        )
    if published_from:
        statement = statement.where(Procurement.publication_at >= published_from)
    if published_to:
        statement = statement.where(Procurement.publication_at <= published_to)
    return statement


@router.get("")
async def list_procurements(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    uf: str | None = Query(None, min_length=2, max_length=2),
    municipality: str | None = None,
    agency: str | None = None,
    modality: str | None = None,
    published_from: datetime | None = None,
    published_to: datetime | None = None,
    procurement_status: str | None = Query(None, alias="status"),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List canonical procurements with common operational filters."""
    base = _filters(
        select(Procurement),
        uf=uf,
        municipality=municipality,
        agency=agency,
        modality=modality,
        published_from=published_from,
        published_to=published_to,
    )
    if procurement_status:
        base = base.where(Procurement.status == procurement_status)
    count_statement = _filters(
        select(func.count()).select_from(Procurement),
        uf=uf,
        municipality=municipality,
        agency=agency,
        modality=modality,
        published_from=published_from,
        published_to=published_to,
    )
    if procurement_status:
        count_statement = count_statement.where(Procurement.status == procurement_status)
    total = int((await db.scalar(count_statement)) or 0)
    rows = (
        await db.scalars(
            base.order_by(Procurement.publication_at.desc().nullslast())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    return {
        "items": [model_dict(row) for row in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }


async def _get_procurement(procurement_id: UUID, db: AsyncSession) -> Procurement:
    procurement = await db.get(Procurement, procurement_id)
    if procurement is None:
        raise HTTPException(status_code=404, detail="Procurement not found")
    return procurement


@router.get("/{procurement_id}")
async def get_procurement(
    procurement_id: UUID, db: AsyncSession = Depends(get_db)
) -> dict[str, object]:
    """Return a canonical procurement and its source links."""
    procurement = await _get_procurement(procurement_id, db)
    payload = model_dict(procurement)
    payload["sources"] = [model_dict(item) for item in procurement.sources]
    return payload


async def _children(model, procurement_id: UUID, db: AsyncSession) -> list[dict[str, object]]:
    await _get_procurement(procurement_id, db)
    rows = (await db.scalars(select(model).where(model.procurement_id == procurement_id))).all()
    return [model_dict(row) for row in rows]


@router.get("/{procurement_id}/items")
async def get_items(procurement_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _children(ProcurementItem, procurement_id, db)


@router.get("/{procurement_id}/documents")
async def get_documents(procurement_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _children(Document, procurement_id, db)


@router.get("/{procurement_id}/participants")
async def get_participants(procurement_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _children(Participant, procurement_id, db)


@router.get("/{procurement_id}/events")
async def get_events(procurement_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _children(ProcurementEvent, procurement_id, db)
