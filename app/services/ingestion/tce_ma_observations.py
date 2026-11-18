"""Link official TCE-MA observations to São Luís procurements by exact identity."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.base import RawProcurement
from app.connectors.http import AsyncHTTPClient
from app.connectors.tce_ma import SincContrataResource, TCEMAConnector
from app.models import FieldObservation, Procurement, SourceRecord
from app.models.enums import DataAvailability
from app.services.ingestion.complete_collection import StepResult
from app.services.ingestion.pipeline import stable_fingerprint
from app.services.tce_ma_pilot import (
    CNPJ,
    ENTE,
    UASG,
    match_sinc_procedure,
    project_tce_row,
    sinc_has_control,
    sinc_procedure_key,
    sinc_row_matches,
)

if TYPE_CHECKING:
    from app.services.ingestion.complete_collection import (
        CollectionScope,
        CollectionTicket,
        CompleteCollection,
    )

_PREFIX = "tce_ma."


async def collect_tce_observations(
    collection: CompleteCollection, ticket: CollectionTicket, scope: CollectionScope
) -> StepResult:
    if scope.uf != "MA":
        return StepResult(
            "skipped", "Integração disponível para os pregões da Prefeitura de São Luís"
        )
    local = ZoneInfo(collection.settings.timezone)
    start = datetime.combine(scope.start_date, time.min, local).astimezone(UTC)
    end = datetime.combine(scope.end_date + timedelta(days=1), time.min, local).astimezone(UTC)
    async with collection.sessions() as db:
        procurements = (
            await db.scalars(
                select(Procurement).where(
                    Procurement.uf == scope.uf,
                    Procurement.agency_cnpj == CNPJ,
                    Procurement.uasg == UASG,
                    Procurement.pncp_control_number.is_not(None),
                    Procurement.modality_key == "pregao_eletronico",
                    Procurement.publication_at >= start,
                    Procurement.publication_at < end,
                )
            )
        ).all()
    if not procurements:
        return StepResult("skipped", "Nenhum pregão da Prefeitura de São Luís no período")

    http = AsyncHTTPClient(collection.settings, public_only=True)
    source = TCEMAConnector(http)
    try:
        procedures = await source.fetch(SincContrataResource.PROCEDURES, ente_id=ENTE)
        inventory = {SincContrataResource.PROCEDURES: procedures}
        matches: dict[tuple[str, str], tuple[UUID, str]] = {}
        confirmed_procedures: set[tuple[tuple[str, str], str, str]] = set()
        conflicts: set[tuple[str, str]] = set()
        identity_review: dict[str, list[tuple[UUID, str, dict[str, Any]]]] = defaultdict(list)
        review_ids: set[UUID] = set()
        for procurement in procurements:
            if not procurement.pncp_control_number:
                continue
            candidate = RawProcurement(
                source=procurement.source,
                external_id=procurement.pncp_control_number,
                pncp_control_number=procurement.pncp_control_number,
                agency_cnpj=procurement.agency_cnpj,
                uasg=procurement.uasg,
                purchase_number=procurement.purchase_number,
                purchase_year=procurement.purchase_year,
            )
            match = match_sinc_procedure(candidate, procedures)
            if match.status == "ambiguo":
                for row in match.rows:
                    if sinc_has_control(row, procurement.pncp_control_number):
                        identity_review[row.source_record_ref].append(
                            (
                                procurement.id,
                                "identidade_para_revisao",
                                project_tce_row(row.payload),
                            )
                        )
                        review_ids.add(procurement.id)
            if match.key and match.status == "exato":
                if match.key in matches:
                    conflicts.add(match.key)
                matches[match.key] = (procurement.id, procurement.pncp_control_number)
                confirmed_procedures.update(
                    (match.key, str(row.payload.get("id")), row.source_record_ref)
                    for row in match.rows
                )
        for key in conflicts:
            matches.pop(key)
        # The last pilot found exact controls in a minority of cases. A single
        # inventory read establishes coverage and avoids child scans without a match.
        if matches:
            resources = (
                SincContrataResource.BIDDERS,
                SincContrataResource.RESULTS,
                SincContrataResource.CONTRACTS,
            )
            additional = await asyncio.gather(
                *(source.fetch(resource, ente_id=ENTE) for resource in resources)
            )
            inventory.update(zip(resources, additional, strict=True))
    finally:
        await http.aclose()

    observations = identity_review
    conflicting_rows = 0
    for resource, result in inventory.items():
        for row in result.data:
            procedure_key = sinc_procedure_key(row)
            if procedure_key is not None and procedure_key in matches:
                procurement_id, control = matches[procedure_key]
                if (
                    resource == SincContrataResource.PROCEDURES
                    and (procedure_key, str(row.payload.get("id")), row.source_record_ref)
                    not in confirmed_procedures
                ):
                    continue
                if not sinc_row_matches(row, procedure_key, control):
                    conflicting_rows += 1
                    continue
                projected = project_tce_row(row.payload)
                if resource == SincContrataResource.BIDDERS and not projected.get("licitante"):
                    continue
                observations[row.source_record_ref].append(
                    (procurement_id, resource.value, projected)
                )

    saved: list[FieldObservation] = []
    async with collection._locked(ticket) as (db, run):
        for resource, result in inventory.items():
            for raw in result.raw_records:
                related = observations.get(raw.record_key, [])
                page = raw.raw_payload if isinstance(raw.raw_payload, dict) else {}
                record = SourceRecord(
                    id=uuid4(),
                    crawl_run_id=run.id,
                    source=raw.source,
                    endpoint=raw.endpoint,
                    request_parameters=raw.request_parameters,
                    response_hash=raw.response_hash,
                    raw_payload={
                        **{
                            name: page.get(name)
                            for name in ("number", "size", "totalPages", "totalElements")
                        },
                        "content": [value for _, _, value in related],
                    },
                    http_status=raw.http_status or None,
                    availability=DataAvailability(result.availability.value),
                    collected_at=raw.collected_at,
                    diagnostic=result.diagnostic,
                    collection_complete=result.is_complete,
                    pagination_metadata=result.pagination.model_dump()
                    if result.pagination
                    else None,
                    fingerprint=stable_fingerprint(
                        "tce_ma_page", run.id, resource.value, raw.record_key
                    ),
                )
                db.add(record)
                saved.extend(
                    FieldObservation(
                        entity_type="procurement",
                        entity_id=procurement_id,
                        field_name=_PREFIX + kind,
                        source=raw.source,
                        source_record_id=record.id,
                        value=value,
                        collected_at=raw.collected_at,
                        fingerprint=stable_fingerprint(
                            "tce_ma_observation", procurement_id, kind, value
                        ),
                    )
                    for procurement_id, kind, value in related
                )
        if saved:
            await db.flush()
            candidates = {item.fingerprint: item for item in saved}
            known = set(
                (
                    await db.scalars(
                        select(FieldObservation.fingerprint).where(
                            FieldObservation.fingerprint.in_(candidates)
                        )
                    )
                ).all()
            )
            db.add_all(item for key, item in candidates.items() if key not in known)

    status = (
        "completed"
        if not conflicting_rows and all(result.is_complete for result in inventory.values())
        else "partial"
    )
    if not procedures.data and not procedures.is_complete:
        status = "failed"
    conflicts_note = (
        f" · {conflicting_rows} registros com controle divergente" if conflicting_rows else ""
    )
    review_note = f" · {len(review_ids)} pregões com identidade para revisão" if review_ids else ""
    return StepResult(
        status,
        f"{len(matches)} de {len(procurements)} pregões vinculados · "
        f"{sum(map(len, observations.values()))} registros com origem{review_note}{conflicts_note}",
    )


async def tce_observations_for(db: AsyncSession, procurement_id: UUID) -> list[FieldObservation]:
    rows = (
        await db.scalars(
            select(FieldObservation)
            .where(
                FieldObservation.entity_type == "procurement",
                FieldObservation.entity_id == procurement_id,
                FieldObservation.source == "tce_ma",
                FieldObservation.field_name.startswith(_PREFIX),
            )
            .order_by(FieldObservation.collected_at.desc(), FieldObservation.id)
            .limit(200)
        )
    ).all()
    latest: dict[tuple[str, str], FieldObservation] = {}
    for row in rows:
        if isinstance(row.value, dict):
            latest.setdefault((row.field_name, str(row.value.get("id"))), row)
    return list(latest.values())
