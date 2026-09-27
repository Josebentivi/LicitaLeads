"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app import __version__
from app.api.router import api_router
from app.config import get_settings
from app.logging import configure_logging
from app.services.ingestion.recovery import reconcile_stale_crawls
from app.web import router as web_router
from app.web import templates


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(logging.DEBUG if settings.app_env == "development" else logging.INFO)
    try:
        await reconcile_stale_crawls()
    except Exception:  # pragma: no cover - startup must not be blocked by recovery
        logging.getLogger(__name__).exception("crawl reconciliation failed at startup")
    yield


app = FastAPI(
    title="LicitaLead Monitor",
    description="Monitor auditável de oportunidades jurídicas em contratações públicas.",
    version=__version__,
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(api_router)
app.include_router(web_router)


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    if request.url.path.startswith("/api/") or request.url.path == "/health":
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
    return templates.TemplateResponse(
        request,
        "error.html",
        {"status_code": exc.status_code, "message": exc.detail},
        status_code=exc.status_code,
    )


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port)
