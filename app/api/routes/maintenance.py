"""Destructive maintenance endpoints guarded by explicit confirmation."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.schemas import ClearDataRequest, ClearDataResponse
from app.services.maintenance import MaintenanceBlocked, clear_all_data

router = APIRouter(prefix="/maintenance", tags=["maintenance"])


@router.post("/clear-data", response_model=ClearDataResponse)
async def clear_data_endpoint(
    payload: ClearDataRequest,
    db: AsyncSession = Depends(get_db),
) -> ClearDataResponse:
    """Delete all collected data after explicit confirmation."""

    if not payload.confirm:
        raise HTTPException(
            status_code=422,
            detail="Confirmation required",
        )
    try:
        result = await clear_all_data(db)
    except MaintenanceBlocked as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return ClearDataResponse(counts=result.counts, files_removed=result.files_removed)
