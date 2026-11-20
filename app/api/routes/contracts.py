"""Read-only contract endpoints."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.config import get_settings
from app.dependencies import get_db
from app.repositories.contracts import ContractRepository, published_term_dates

router = APIRouter(prefix="/contracts", tags=["contracts"])


@router.get("")
async def list_contracts(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str | None = Query(None, max_length=120),
    agency: str | None = None,
    agency_cnpj: str | None = None,
    supplier_cnpj: str | None = None,
    contract_number: str | None = None,
    valid_on: date | None = None,
    expiring_within_days: int | None = Query(None, ge=1, le=365),
    status: str | None = Query(None, alias="status"),
    source: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List contracts with auditable source and payment links."""

    result = await ContractRepository(db).list_filtered(
        page=page,
        page_size=page_size,
        search=search,
        agency=agency,
        agency_cnpj=agency_cnpj,
        supplier_cnpj=supplier_cnpj,
        contract_number=contract_number,
        valid_on=valid_on,
        expiring_within_days=expiring_within_days,
        status=status,
        source=source,
    )
    return {
        "items": [
            {
                **model_dict(row),
                "term_expirations": [
                    model_dict(
                        term,
                        include={
                            "id",
                            "term_number",
                            "new_valid_until",
                            "status",
                            "source_url",
                            "source_record_id",
                        },
                    )
                    for term in published_term_dates(row)
                ],
            }
            for row in result.items
        ],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.get("/{contract_id}")
async def get_contract(contract_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    repository = ContractRepository(db)
    contract = await repository.get_detail(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="Contrato nao encontrado")
    summary = await repository.payment_summary(
        contract, grace_days=get_settings().payment_grace_days
    )
    terms_observation = await repository.latest_terms_observation(contract.id)
    terms_evidence = terms_observation.source_record if terms_observation is not None else None
    return {
        "contract": model_dict(contract),
        "company": model_dict(contract.company) if contract.company else None,
        "procurement": model_dict(contract.procurement) if contract.procurement else None,
        "price_registry": (
            model_dict(contract.price_registry) if contract.price_registry else None
        ),
        "payments": [model_dict(payment) for payment in contract.payments],
        "items": [model_dict(item) for item in contract.items],
        "terms": [model_dict(term) for term in contract.terms],
        "terms_availability": (
            {
                "availability": terms_observation.value,
                "source": terms_observation.source,
                "source_record_id": terms_evidence.id,
                "http_status": terms_evidence.http_status,
                "collected_at": terms_observation.collected_at,
            }
            if terms_observation is not None and terms_evidence is not None
            else None
        ),
        "payment_summary": summary,
    }
