"""Health endpoint."""

from fastapi import APIRouter

from app import __version__
from app.config import get_settings
from app.database import check_database

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, object]:
    """Report process and database readiness without contacting upstream APIs."""
    settings = get_settings()
    database_ok = await check_database(raise_on_error=False)
    return {
        "status": "ok" if database_ok else "degraded",
        "version": __version__,
        "database": "ok" if database_ok else "error",
        "scheduler": "separate_process",
        "environment": settings.app_env,
    }
