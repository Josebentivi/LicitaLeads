"""Aggregate all REST routers under one import."""

from fastapi import APIRouter

from app.api.routes import (
    capabilities,
    companies,
    contacts,
    crawls,
    events,
    evidence,
    health,
    leads,
    maintenance,
    price_registries,
    procurements,
)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(procurements.router, prefix="/api")
api_router.include_router(companies.router, prefix="/api")
api_router.include_router(price_registries.router, prefix="/api")
api_router.include_router(leads.router, prefix="/api")
api_router.include_router(crawls.router, prefix="/api")
api_router.include_router(contacts.router, prefix="/api")
api_router.include_router(capabilities.router, prefix="/api")
api_router.include_router(evidence.router, prefix="/api")
api_router.include_router(events.router, prefix="/api")
api_router.include_router(maintenance.router, prefix="/api")
