"""FastAPI application entrypoint."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app import __version__
from app.ai_web import router as ai_web_router
from app.api.router import api_router
from app.company_web import router as company_web_router
from app.config import get_settings
from app.logging import configure_logging
from app.repositories.crawls import MaintenanceInProgress
from app.security import (
    FormOriginMiddleware,
    SecurityHeadersMiddleware,
    ShutdownAdmissionMiddleware,
)
from app.services.application_shutdown import ApplicationShutdown
from app.services.ingestion.recovery import reconcile_stale_crawls
from app.shutdown_web import router as shutdown_router
from app.web import COLLECTION_FORM_PATHS, collection_form_error, templates
from app.web import router as web_router


@asynccontextmanager
async def lifespan(application: FastAPI):
    settings = get_settings()
    settings.ensure_directories()
    configure_logging(settings.log_level)
    shutdown = ApplicationShutdown(settings)
    application.state.shutdown = shutdown
    await shutdown.recover()
    if shutdown.channel is not None:
        shutdown.task = asyncio.create_task(
            shutdown.watch(application), name="application-shutdown"
        )
    try:
        await reconcile_stale_crawls()
    except Exception:  # pragma: no cover - startup must not be blocked by recovery
        logging.getLogger(__name__).exception("crawl reconciliation failed at startup")
    try:
        yield
    finally:
        await shutdown.close()
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
app.add_middleware(ShutdownAdmissionMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.mount(
    "/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static"
)
app.include_router(api_router)
app.include_router(web_router)
app.include_router(company_web_router)
app.include_router(ai_web_router)
app.include_router(shutdown_router)


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    if exc.status_code == 422 and request.url.path in COLLECTION_FORM_PATHS:
        return await collection_form_error(request, str(exc.detail))
    if request.url.path.startswith("/api/") or request.url.path == "/health":
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
    return templates.TemplateResponse(
        request,
        "error.html",
        {"status_code": exc.status_code, "message": exc.detail},
        status_code=exc.status_code,
    )


@app.exception_handler(RequestValidationError)
async def form_validation_error(request: Request, exc: RequestValidationError):
    if request.url.path in COLLECTION_FORM_PATHS:
        return await collection_form_error(
            request,
            "Confira o período, a UF e os campos informados. As datas devem ser válidas e os limites de cada campo devem ser respeitados.",
        )
    return await request_validation_exception_handler(request, exc)


@app.exception_handler(MaintenanceInProgress)
async def maintenance_error(_request: Request, exc: MaintenanceInProgress):
    return JSONResponse({"detail": str(exc)}, status_code=503)


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port)
