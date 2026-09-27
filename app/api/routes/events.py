"""Manual, auditable event-to-company attribution endpoints."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.schemas import EventCompanyLink
from app.services.ingestion.processor import DocumentProcessingService

router = APIRouter(prefix="/events", tags=["events"])


@router.post("/{event_id}/link-company")
async def link_event_company(
    event_id: UUID,
    payload: EventCompanyLink,
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    """Link an unattributed event to an evidenced participant and score its lead.

    The company must already participate in the procurement, so this never
    invents a relationship; it only records a human decision over official data.
    """

    service = DocumentProcessingService()
    try:
        await service.link_event_company(
            event_id=event_id,
            company_id=payload.company_id,
            reviewer=payload.reviewer,
            note=payload.note,
            session=db,
        )
        await db.commit()
    except LookupError:
        raise HTTPException(status_code=404, detail="Evento ou empresa não encontrados") from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    finally:
        await service.aclose()
    return {"event_id": str(event_id), "company_id": str(payload.company_id), "linked": True}
