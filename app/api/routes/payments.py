"""Read-only payment/execution endpoints."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.config import get_settings
from app.connectors.municipal_payments.sources import list_municipal_sources
from app.connectors.payment_sources import list_payment_sources
from app.dependencies import get_db
from app.repositories.contracts import PaymentRepository
from app.schemas import MunicipalPaymentRunRequest, PaymentCrawlRequest
from app.services.ingestion.municipal_payments import (
    MunicipalPaymentIngestionService,
    latest_municipal_runs,
)
from app.services.ingestion.payment_import import PaymentImportErrorList, PaymentImportService
from app.services.ingestion.payments import PaymentIngestionService

router = APIRouter(prefix="/payments", tags=["payments"])
_MAX_IMPORT_BYTES = 10 * 1024 * 1024


@router.get("")
async def list_payments(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    supplier_cnpj: str | None = None,
    agency_cnpj: str | None = None,
    contract_id: UUID | None = None,
    phase: str | None = None,
    match_status: str | None = Query(None, max_length=50),
    source: str | None = Query(None, max_length=50),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """List payment documents without assuming an unlinked payment is unpaid."""

    result = await PaymentRepository(db).list_filtered(
        page=page,
        page_size=page_size,
        supplier_cnpj=supplier_cnpj,
        agency_cnpj=agency_cnpj,
        contract_id=contract_id,
        phase=phase,
        match_status=match_status,
        source=source,
        date_from=date_from,
        date_to=date_to,
    )
    return {
        "items": [model_dict(row) for row in result.items],
        "page": result.page,
        "page_size": result.page_size,
        "total": result.total,
    }


@router.post("/import")
async def import_payments(
    file: UploadFile = File(...),
    source_name: str = Form("Portal"),
) -> dict[str, object]:
    """Import a CSV/JSON export from a state or municipal portal."""

    content = await file.read(_MAX_IMPORT_BYTES + 1)
    if len(content) > _MAX_IMPORT_BYTES:
        raise HTTPException(status_code=413, detail="arquivo maior que 10 MB")
    service = PaymentImportService()
    try:
        result = await service.import_bytes(
            content, filename=file.filename or "pagamentos.csv", source_name=source_name
        )
    except PaymentImportErrorList as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        await service.aclose()
    return {
        "created": result.created,
        "updated": result.updated,
        "changed": result.changed,
        "unchanged": result.unchanged,
        "stale_ignored": result.stale_ignored,
        "conflicted": result.conflicted,
        "crawl_run_id": result.crawl_run_id,
        "diagnostic": result.diagnostic,
        "linked": result.linked,
        "unlinked": result.unlinked,
        "errors": [{"row": item.row, "message": item.message} for item in result.errors],
        "source_record_id": result.source_record_id,
    }


@router.post("/crawl")
async def crawl_payments(payload: PaymentCrawlRequest) -> dict[str, object]:
    """Query the configured federal payment source for one supplier."""

    service = PaymentIngestionService()
    try:
        result = await service.run(
            company_cnpj=payload.company_cnpj, year=payload.year, phase=payload.phase
        )
    finally:
        await service.aclose()
    if result.availability.value not in {"available", "empty"}:
        status_code = (
            403
            if result.availability.value == "access_restricted"
            else 422
            if result.availability.value == "not_supported"
            else 503
        )
        raise HTTPException(
            status_code=status_code,
            detail={
                "availability": result.availability.value,
                "diagnostic": result.diagnostic,
                "created": result.payments_created,
                "updated": result.payments_updated,
                "changed": result.payments_changed,
                "unchanged": result.payments_unchanged,
                "stale_ignored": result.payments_stale_ignored,
                "conflicted": result.payments_conflicted,
            },
        )
    return {
        "availability": result.availability.value,
        "crawl_run_id": str(result.crawl_run_id) if result.crawl_run_id else None,
        "can_continue": result.can_continue,
        "created": result.payments_created,
        "updated": result.payments_updated,
        "changed": result.payments_changed,
        "unchanged": result.payments_unchanged,
        "stale_ignored": result.payments_stale_ignored,
        "conflicted": result.payments_conflicted,
        "linked": result.payments_linked,
        "unlinked": result.payments_unlinked,
        "diagnostic": result.diagnostic,
    }


@router.get("/sources")
async def payment_sources() -> list[dict[str, object]]:
    """List configured payment sources and whether they are usable."""

    return [
        {
            "id": source.id,
            "name": source.name,
            "scope": source.scope,
            "configured": source.configured,
            "mode": source.mode,
            "note": source.note,
        }
        for source in list_payment_sources()
    ]


@router.get("/municipal/sources")
async def municipal_payment_sources(
    db: AsyncSession = Depends(get_db),
) -> list[dict[str, object]]:
    """List the Maranhão municipal catalogue and the latest run of each source."""

    settings = get_settings()
    enabled = set(settings.municipal_payment_sources)
    latest = await latest_municipal_runs(db)
    return [
        {
            "id": source.id,
            "name": source.name,
            "municipality": source.municipality,
            "uf": source.uf,
            "mode": source.mode,
            "base_url": source.base_url,
            "supports_details": source.supports_details,
            "configured": source.id in enabled,
            "note": source.note,
            "last_run": (
                {
                    "id": str(run.id),
                    "status": run.status.value,
                    "started_at": run.started_at.isoformat() if run.started_at else None,
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                    "records_found": run.records_found,
                    "records_created": run.records_created,
                    "records_updated": run.records_updated,
                    "diagnostic": run.diagnostic,
                }
                if (run := latest.get(source.id)) is not None
                else None
            ),
        }
        for source in list_municipal_sources(settings)
    ]


@router.post("/municipal/run")
async def run_municipal_payment_source(
    payload: MunicipalPaymentRunRequest,
) -> dict[str, object]:
    """Collect one municipal source synchronously and preserve every raw response."""

    service = MunicipalPaymentIngestionService()
    try:
        result = await service.run(
            payload.source,
            year=payload.year,
            company_cnpj=payload.company_cnpj,
            include_details=payload.include_details,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        await service.aclose()
    if result.availability.value not in {"available", "empty"}:
        status_code = (
            403
            if result.availability.value == "access_restricted"
            else 422
            if result.availability.value == "not_supported"
            else 503
        )
        raise HTTPException(
            status_code=status_code,
            detail={
                "availability": result.availability.value,
                "diagnostic": result.diagnostic,
                "crawl_run_id": str(result.crawl_run_id) if result.crawl_run_id else None,
            },
        )
    return {
        "source": result.source_id,
        "source_name": result.source_name,
        "crawl_run_id": str(result.crawl_run_id) if result.crawl_run_id else None,
        "availability": result.availability.value,
        "records_found": result.records_found,
        "created": result.payments_created,
        "updated": result.payments_updated,
        "changed": result.payments_changed,
        "unchanged": result.payments_unchanged,
        "stale_ignored": result.payments_stale_ignored,
        "conflicted": result.payments_conflicted,
        "linked": result.payments_linked,
        "unlinked": result.payments_unlinked,
        "diagnostic": result.diagnostic,
    }
