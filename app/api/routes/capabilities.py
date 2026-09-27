"""Expose the audited source capability registry."""

from typing import Any

from fastapi import APIRouter

from app.connectors.capabilities import get_source_capabilities

router = APIRouter(tags=["sources"])


@router.get("/source-capabilities")
async def source_capabilities() -> list[dict[str, Any]]:
    """Return machine-readable availability without probing live services."""
    capabilities = get_source_capabilities()
    return [item.model_dump(mode="json") for item in capabilities]
