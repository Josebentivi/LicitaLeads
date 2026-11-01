"""Admission and dispatch of explicit continuation requests."""

from __future__ import annotations

from dataclasses import fields
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CollectionBatch, CollectionWorkItem, CrawlRun, CrawlRunStatus, JobLease
from app.repositories.crawls import JobLeaseRepository


class ContinuationBusy(ValueError):
    pass


async def create_continuation(
    session: AsyncSession, run_id: UUID
) -> tuple[CrawlRun, dict[str, Any]]:
    await JobLeaseRepository(session).assert_writes_allowed(now=datetime.now(UTC))
    parent = await session.get(CrawlRun, run_id)
    if parent is None:
        raise LookupError("Coleta não encontrada")
    batch = (
        await session.scalar(
            select(CollectionBatch).where(CollectionBatch.id == parent.batch_id).with_for_update()
        )
        if parent.batch_id
        else None
    )
    if batch is None or batch.status == "complete":
        raise ContinuationBusy("Esta coleta não possui trabalho pendente")
    active = await session.scalar(
        select(CrawlRun.id)
        .where(
            CrawlRun.batch_id == batch.id,
            CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
        )
        .limit(1)
    )
    if active is not None:
        raise ContinuationBusy("Já existe uma execução deste lote em andamento")
    filters = dict(batch.filters)
    filters["continuation_batch_id"] = str(batch.id)
    filters["resume_automatically"] = False
    connector = str(filters.get("connector") or parent.connector)
    uf = str(filters.get("uf") or "MA").upper()
    names = [
        f"crawl:{source}:{uf}"
        for source in (["pncp", "compras_gov"] if connector == "all" else [connector])
    ]
    if connector.startswith("municipal:"):
        names = [connector]
    if await session.scalar(
        select(JobLease.name)
        .where(JobLease.name.in_(names), JobLease.expires_at > datetime.now(UTC))
        .limit(1)
    ):
        raise ContinuationBusy("Outra coleta desta fonte está em andamento")
    run = CrawlRun(
        connector=connector,
        status=CrawlRunStatus.PENDING,
        batch_id=batch.id,
        filters=filters,
        counting_version=2,
        records_changed=0,
        records_unchanged=0,
        records_stale_ignored=0,
        records_conflicted=0,
        cursor={"progress": {"stage": "queued", "message": "Aguardando continuação da coleta"}},
    )
    session.add(run)
    await session.flush()
    await session.execute(
        update(CollectionWorkItem)
        .where(
            CollectionWorkItem.batch_id == batch.id,
            CollectionWorkItem.status == "failed",
        )
        .values(status="pending")
    )
    batch.last_run_id = run.id
    return run, filters


def pipeline_request_from_filters(filters: dict[str, Any]) -> Any:
    from app.services.ingestion.pipeline import PipelineRequest
    from app.services.price_registry_scopes import restore_price_registry_scopes

    names = {item.name for item in fields(PipelineRequest)}
    values = {key: value for key, value in filters.items() if key in names}
    for name in ("start_date", "end_date"):
        if isinstance(values.get(name), str):
            values[name] = date.fromisoformat(values[name])
    for name in ("modalities", "external_ids", "document_ids"):
        if name in values:
            values[name] = tuple(values[name] or [])
    if values.get("continuation_batch_id"):
        values["continuation_batch_id"] = UUID(str(values["continuation_batch_id"]))
    values["price_registry_scopes"] = restore_price_registry_scopes(filters)
    return PipelineRequest(**values)


async def execute_continuation(run_id: UUID, filters: dict[str, Any]) -> Any:
    mode = filters.get("mode")
    if mode in {"payments", "payment_import"}:
        from app.services.ingestion.payments import PaymentIngestionService

        service = PaymentIngestionService()
        try:
            return await service.continue_collection(run_id, filters)
        finally:
            await service.aclose()
    if mode == "municipal_payments":
        from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService

        municipal_service = MunicipalPaymentIngestionService()
        try:
            return await municipal_service.run(
                filters["source_id"],
                year=int(filters["year"]),
                company_cnpj=filters.get("company_cnpj"),
                include_details=filters.get("include_details"),
                run_id=run_id,
            )
        finally:
            await municipal_service.aclose()
    from app.services.ingestion.pipeline import IngestionPipeline

    return await IngestionPipeline().run(pipeline_request_from_filters(filters), run_id=run_id)
