"""Evidence metadata and safe local document viewing."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.config import get_settings
from app.dependencies import get_db
from app.models import Document, Evidence

router = APIRouter(tags=["evidence"])


@router.get("/evidence/{evidence_id}")
async def get_evidence(evidence_id: UUID, db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    """Return evidence and direct, traceable viewing links."""
    evidence = await db.get(Evidence, evidence_id)
    if evidence is None:
        raise HTTPException(status_code=404, detail="Evidence not found")
    payload = model_dict(evidence)
    if evidence.document_id:
        payload["document_view_url"] = f"/api/documents/{evidence.document_id}/view"
    return payload


@router.get("/documents/{document_id}/view")
async def view_document(document_id: UUID, db: AsyncSession = Depends(get_db)):
    """Serve only files located inside the configured document store."""
    document = await db.get(Document, document_id)
    if document is None:
        raise HTTPException(status_code=404, detail="Document not found")
    if not document.local_path:
        if document.extracted_text:
            return PlainTextResponse(document.extracted_text)
        raise HTTPException(status_code=404, detail="Local document is not available")
    root = get_settings().document_storage_path.resolve()
    candidate = Path(document.local_path).resolve()
    if root != candidate and root not in candidate.parents:
        raise HTTPException(status_code=403, detail="Invalid document path")
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="Local document is not available")
    return FileResponse(
        candidate, media_type=document.mime_type, filename=document.title or candidate.name
    )
