"""Read-only price registry (ARP/ata) endpoints."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.dependencies import get_db
from app.repositories.price_registries import PriceRegistryRepository
from app.services.identifiers import normalize_cnpj

router = APIRouter(prefix="/price-registries", tags=["price-registries"])

_COMPANY_FIELDS = {"id", "cnpj", "legal_name", "trade_name", "uf", "municipality"}


@router.get("")
async def list_price_registries(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str | None = Query(None, max_length=120),
    agency: str | None = None,
    agency_cnpj: str | None = None,
    registry_number: str | None = None,
    registry_status: str | None = Query(None, alias="status"),
    supplier_cnpj: str | None = None,
    valid_on: date | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List price registries with their official vigência and provenance."""

    result = await PriceRegistryRepository(db).list_filtered(
        page=page,
        page_size=page_size,
        search=search,
        agency=agency,
        agency_cnpj=normalize_cnpj(agency_cnpj) if agency_cnpj else None,
        registry_number=registry_number,
        status=registry_status,
        supplier_cnpj=normalize_cnpj(supplier_cnpj) if supplier_cnpj else None,
        valid_on=valid_on,
    )
    return {
        "items": [model_dict(row) for row in result.items],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.get("/{registry_id}")
async def get_price_registry(
    registry_id: UUID, db: AsyncSession = Depends(get_db)
) -> dict[str, object]:
    registry = await PriceRegistryRepository(db).get_detail(registry_id)
    if registry is None:
        raise HTTPException(status_code=404, detail="Ata não encontrada")
    return {
        "registry": model_dict(registry),
        "items": [
            {
                "item": model_dict(item),
                "company": (
                    model_dict(item.company, include=_COMPANY_FIELDS) if item.company else None
                ),
            }
            for item in registry.items
        ],
        "procurement": (
            model_dict(registry.procurement) if registry.procurement is not None else None
        ),
    }


@router.get("/{registry_id}/items")
async def list_price_registry_items(
    registry_id: UUID, db: AsyncSession = Depends(get_db)
) -> dict[str, object]:
    registry = await PriceRegistryRepository(db).get_detail(registry_id)
    if registry is None:
        raise HTTPException(status_code=404, detail="Ata não encontrada")
    return {
        "items": [
            {
                "item": model_dict(item),
                "company": (
                    model_dict(item.company, include=_COMPANY_FIELDS) if item.company else None
                ),
            }
            for item in registry.items
        ]
    }
