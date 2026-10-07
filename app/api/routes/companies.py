"""Read-only company history endpoints built on auditable participation facts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.dependencies import get_db
from app.models.enums import ContractingType, ParticipantStatus, ProcurementEventType
from app.repositories.procurements import (
    CompanyRepository,
    ParticipantRepository,
    ProcurementEventRepository,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

router = APIRouter(prefix="/companies", tags=["companies"])

_PROCUREMENT_SUMMARY_FIELDS = {
    "id",
    "source",
    "external_id",
    "pncp_control_number",
    "purchase_number",
    "purchase_year",
    "modality",
    "procurement_type",
    "is_srp",
    "agency_name",
    "uf",
    "municipality",
    "estimated_value",
    "publication_at",
    "proposal_end_at",
    "status",
    "source_url",
}


async def _get_company(cnpj: str, db: AsyncSession):
    normalized = normalize_cnpj(cnpj)
    if normalized is None or not is_valid_cnpj(normalized):
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    company = await CompanyRepository(db).find_by_cnpj(normalized)
    if company is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    return company


@router.get("")
async def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str | None = Query(None, max_length=120),
    uf: str | None = Query(None, min_length=2, max_length=2),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List companies with auditable participation counters."""

    result = await CompanyRepository(db).list_with_stats(
        page=page, page_size=page_size, search=search, uf=uf
    )
    return {
        "items": [
            {"company": model_dict(company), "stats": stats} for company, stats in result.items
        ],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.get("/{cnpj}")
async def get_company(cnpj: str, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    """Return one company and its participation counters."""

    company = await _get_company(cnpj, db)
    stats = await CompanyRepository(db).stats_for_company(company.id)
    return {"company": model_dict(company), "stats": stats}


@router.get("/{cnpj}/participations")
async def list_company_participations(
    cnpj: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: list[ParticipantStatus] | None = Query(None),
    uf: str | None = Query(None, min_length=2, max_length=2),
    agency: str | None = None,
    modality: str | None = None,
    procurement_type: ContractingType | None = None,
    is_srp: bool | None = None,
    value_min: Decimal | None = Query(None, ge=0),
    value_max: Decimal | None = Query(None, ge=0),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List one company's participation facts with procurement context."""

    company = await _get_company(cnpj, db)
    result = await ParticipantRepository(db).list_for_company(
        company.id,
        page=page,
        page_size=page_size,
        statuses=[item.value for item in status] if status else None,
        uf=uf,
        agency=agency,
        modality=modality,
        procurement_type=procurement_type.value if procurement_type else None,
        is_srp=is_srp,
        value_min=value_min,
        value_max=value_max,
        date_from=date_from,
        date_to=date_to,
        active_only=active_only,
    )
    return {
        "company": model_dict(company),
        "items": [
            {
                "participant": model_dict(participant),
                "procurement": model_dict(
                    participant.procurement, include=_PROCUREMENT_SUMMARY_FIELDS
                ),
            }
            for participant in result.items
        ],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.get("/{cnpj}/events")
async def list_company_events(
    cnpj: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    event_type: ProcurementEventType | None = None,
    reason_category: str | None = None,
    requires_manual_review: bool | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List document-derived events attributed to one company."""

    company = await _get_company(cnpj, db)
    result = await ProcurementEventRepository(db).list_for_company(
        company.id,
        page=page,
        page_size=page_size,
        event_type=event_type,
        reason_category=reason_category,
        requires_manual_review=requires_manual_review,
        date_from=date_from,
        date_to=date_to,
    )
    return {
        "company": model_dict(company),
        "items": [
            {
                "event": model_dict(event),
                "procurement": model_dict(event.procurement, include=_PROCUREMENT_SUMMARY_FIELDS),
            }
            for event in result.items
        ],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }
