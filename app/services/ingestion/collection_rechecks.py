"""Bounded revisits by exact official identity, independent from date coverage."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, func, or_, select, update

from app.database import write_session
from app.models import (
    CollectionBatch,
    CollectionRecheckState,
    Contract,
    CrawlRun,
    PriceRegistry,
    Procurement,
    ProcurementSource,
    SourceRecord,
    VerifiedCollectionVersion,
)
from app.services.identifiers import canonical_modality
from app.services.ingestion.collection_batches import digest
from app.services.ingestion.continuation import pipeline_request_from_filters
from app.services.ingestion.scheduled_collections import sync_identity


async def recheck_compras(pipeline: Any, request: Any, settings: Any, sessions: Any) -> None:
    from app.services.ingestion.pipeline import PipelineBusy

    if request.connector not in {"all", "compras_gov"}:
        return
    if request.price_registry_scopes and not any(
        "compras_gov" in scope.sources for scope in request.price_registry_scopes
    ):
        return
    request = replace(request, connector="compras_gov", sync_context=None)
    key = digest(["recheck-v1", sync_identity(request, settings)])
    async with sessions() as session:
        pending = await session.scalar(
            select(CollectionBatch)
            .where(
                CollectionBatch.filters["recheck_key"].as_string() == key,
                CollectionBatch.status != "complete",
            )
            .order_by(CollectionBatch.created_at)
            .limit(1)
        )
        if pending is not None:
            if pending.status != "limited":
                return
            resumed = replace(
                pipeline_request_from_filters(pending.filters), continuation_batch_id=pending.id
            )
        else:
            resumed = None
    if resumed is not None:
        try:
            await pipeline.run(resumed)
        except PipelineBusy:
            pass
        return
    now = datetime.now(UTC)
    limit = min(
        settings.compras_recheck_batch_size,
        settings.crawl_max_records_per_source or settings.compras_recheck_batch_size,
    )
    async with write_session(sessions) as session:
        query: Any
        if request.mode == "procurements":
            query = (
                select(ProcurementSource.external_id)
                .join(Procurement, Procurement.id == ProcurementSource.procurement_id)
                .where(
                    ProcurementSource.source == "compras_gov",
                    Procurement.uf == request.uf.upper(),
                )
                .order_by(
                    func.coalesce(
                        ProcurementSource.last_refresh_attempt_at, ProcurementSource.collected_at
                    ),
                    ProcurementSource.id,
                )
                .limit(limit)
            )
            if request.agency_cnpj:
                query = query.where(Procurement.agency_cnpj == request.agency_cnpj)
            if request.unit_code:
                query = query.where(Procurement.uasg == request.unit_code)
            if request.modalities:
                query = query.where(
                    Procurement.modality_key.in_(
                        [canonical_modality(str(value)) for value in request.modalities]
                    )
                )
        else:
            model = PriceRegistry if request.mode == "price_registries" else Contract
            identifier_column = model.pncp_control_number
            query = (
                select(identifier_column)
                .outerjoin(SourceRecord, SourceRecord.id == model.source_record_id)
                .outerjoin(CrawlRun, CrawlRun.id == SourceRecord.crawl_run_id)
                .outerjoin(Procurement, Procurement.id == model.procurement_id)
                .outerjoin(
                    CollectionRecheckState,
                    and_(
                        CollectionRecheckState.stream_key == key,
                        CollectionRecheckState.external_id == identifier_column,
                    ),
                )
                .where(
                    identifier_column.is_not(None),
                    or_(
                        model.source == "compras_gov",
                        SourceRecord.source == "compras_gov",
                        select(VerifiedCollectionVersion.id)
                        .where(
                            VerifiedCollectionVersion.source == "compras_gov",
                            VerifiedCollectionVersion.mode == request.mode,
                            VerifiedCollectionVersion.external_id == identifier_column,
                        )
                        .exists(),
                    ),
                    or_(
                        Procurement.uf == request.uf.upper(),
                        CrawlRun.filters["uf"].as_string() == request.uf.upper(),
                    ),
                )
            )
            if request.agency_cnpj:
                query = query.where(model.agency_cnpj == request.agency_cnpj)
            if request.unit_code:
                query = query.where(model.uasg == request.unit_code)
            if request.price_registry_scopes:
                query = query.where(
                    or_(
                        *(
                            and_(
                                model.agency_cnpj == scope.agency_cnpj,
                                model.uasg == scope.unit_code,
                            )
                            for scope in request.price_registry_scopes
                            if "compras_gov" in scope.sources
                        )
                    )
                )
            query = query.order_by(
                CollectionRecheckState.attempted_at.asc().nulls_first(), model.id
            ).limit(limit)
        identifiers = tuple(dict.fromkeys((await session.scalars(query)).all()))
    if not identifiers:
        return
    try:
        await pipeline.run(
            replace(
                request,
                external_ids=identifiers,
                start_date=None,
                end_date=None,
                recheck_key=key,
                resume_automatically=True,
                process_documents=False,
            )
        )
    except PipelineBusy:
        return
    async with write_session(sessions) as session:
        for identifier in identifiers:
            state = await session.scalar(
                select(CollectionRecheckState).where(
                    CollectionRecheckState.stream_key == key,
                    CollectionRecheckState.external_id == identifier,
                )
            )
            if state is None:
                session.add(
                    CollectionRecheckState(stream_key=key, external_id=identifier, attempted_at=now)
                )
            else:
                state.attempted_at = now
        if request.mode == "procurements" and identifiers:
            await session.execute(
                update(ProcurementSource)
                .where(
                    ProcurementSource.source == "compras_gov",
                    ProcurementSource.external_id.in_(identifiers),
                )
                .values(last_refresh_attempt_at=now)
            )
