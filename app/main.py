"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app import __version__
from app.api.router import api_router
from app.company_web import router as company_web_router
from app.config import get_settings
from app.logging import configure_logging
from app.repositories.crawls import MaintenanceInProgress
from app.security import FormOriginMiddleware, SecurityHeadersMiddleware
from app.services.ingestion.recovery import reconcile_stale_crawls
from app.web import router as web_router
from app.web import templates


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level)
    try:
        await reconcile_stale_crawls()
    except Exception:  # pragma: no cover - startup must not be blocked by recovery
        logging.getLogger(__name__).exception("crawl reconciliation failed at startup")
    try:
        yield
    finally:
        from app.api.routes.crawls import cancel_all_running_crawls
        from app.database import dispose_database
        from app.services.ingestion.municipal_payments import cancel_all_municipal_tasks
        from app.web import _application_tests

        try:
            await _application_tests.aclose()
        finally:
            try:
                await cancel_all_running_crawls()
            finally:
                try:
                    await cancel_all_municipal_tasks()
                finally:
                    await dispose_database()


app = FastAPI(
    title="LicitaLead Monitor",
    description="Monitor auditável de oportunidades jurídicas em contratações públicas.",
    version=__version__,
    lifespan=lifespan,
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=get_settings().allowed_hosts)
app.add_middleware(FormOriginMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.mount(
    "/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static"
)
app.include_router(api_router)
app.include_router(web_router)
app.include_router(company_web_router)


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


@app.exception_handler(MaintenanceInProgress)
async def maintenance_error(_request: Request, exc: MaintenanceInProgress):
    return JSONResponse({"detail": str(exc)}, status_code=503)


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port)
