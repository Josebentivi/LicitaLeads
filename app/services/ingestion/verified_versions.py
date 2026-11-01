"""Reuse only complete, committed observations covered by a documented clock."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors._common import to_datetime
from app.connectors.base import ConnectorResult, DataAvailability
from app.models import ProcurementSource, SourceRecord, VerifiedCollectionVersion

ADAPTER_VERSION = 1
PROCUREMENT_OPERATIONS = {"detail", "items", "documents", "results", "version_check"}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def adapter_key(connector: Any) -> str:
    return digest(
        [
            getattr(connector, "adapter_version", ADAPTER_VERSION),
            connector.name,
            getattr(connector, "consulta_base_url", None),
            getattr(connector, "integration_base_url", None),
            getattr(connector, "base_url", None),
        ]
    )


def official_global_time(raw: Any, timezone: str) -> datetime | None:
    return to_datetime(raw.raw_payload.get("dataAtualizacaoGlobal"), timezone)


def complete_operation(result: ConnectorResult[Any]) -> bool:
    # NOT_PUBLISHED can describe a fully queried empty document/result catalog.
    # It is never promoted to EMPTY and never grants permission to retract facts.
    return result.is_complete or bool(
        result.availability is DataAvailability.NOT_PUBLISHED
        and not result.partial
        and result.raw_records
        and not (result.pagination and result.pagination.truncated)
        and not (result.pagination and (result.pagination.pages_remaining or 0) > 0)
        and not (
            result.pagination
            and (result.pagination.total_pages or 0) > result.pagination.fetched_pages
        )
    )


async def evidence_exists(session: AsyncSession, proof: VerifiedCollectionVersion) -> bool:
    if not proof.source_records:
        return False
    ids = [UUID(value) for value in proof.source_records]
    records = list(
        (await session.scalars(select(SourceRecord).where(SourceRecord.id.in_(ids)))).all()
    )
    return len(records) == len(ids) and all(
        proof.source_records.get(str(record.id)) == record.response_hash for record in records
    )


async def find_reusable(
    session: AsyncSession, connector: Any, mode: str, raw: Any, *, timezone: str
) -> VerifiedCollectionVersion | None:
    # Other adapters currently expose header clocks, not proof for all children.
    if connector.name != "pncp" or mode != "procurements":
        return None
    stamp = official_global_time(raw, timezone)
    if stamp is None:
        return None
    proof = await session.scalar(
        select(VerifiedCollectionVersion)
        .where(
            VerifiedCollectionVersion.source == connector.name,
            VerifiedCollectionVersion.mode == mode,
            VerifiedCollectionVersion.external_id == raw.external_id,
        )
        .order_by(VerifiedCollectionVersion.verified_at.desc(), VerifiedCollectionVersion.id.desc())
        .limit(1)
    )
    if proof is None or (
        proof.adapter_key != adapter_key(connector)
        or proof.source_updated_at != stamp
        or proof.timestamp_origin != "dataAtualizacaoGlobal"
        or proof.header_hash != digest(raw.raw_payload)
        or not PROCUREMENT_OPERATIONS.issubset(proof.operations)
    ):
        return None
    exists = await session.scalar(
        select(ProcurementSource.id).where(
            ProcurementSource.source == connector.name,
            ProcurementSource.external_id == raw.external_id,
        )
    )
    return proof if exists is not None and await evidence_exists(session, proof) else None


async def record_verified(
    session: AsyncSession,
    *,
    connector: Any,
    mode: str,
    raw: Any,
    values: dict[str, ConnectorResult[Any]],
    references: dict[str, list[UUID]],
    discovery_id: UUID | None,
    run_id: UUID,
    timezone: str,
) -> None:
    stamp = official_global_time(raw, timezone)
    origin = "dataAtualizacaoGlobal"
    if stamp is None:
        stamp = getattr(raw, "source_updated_at", None) or getattr(
            raw, "last_source_update_at", None
        )
        origin = getattr(raw, "source_update_field", None) or "last_source_update_at"
    if stamp is None or discovery_id is None:
        return
    newest = await session.scalar(
        select(VerifiedCollectionVersion.source_updated_at)
        .where(
            VerifiedCollectionVersion.source == connector.name,
            VerifiedCollectionVersion.mode == mode,
            VerifiedCollectionVersion.external_id == raw.external_id,
            VerifiedCollectionVersion.adapter_key == adapter_key(connector),
            VerifiedCollectionVersion.timestamp_origin == origin,
        )
        .order_by(VerifiedCollectionVersion.source_updated_at.desc())
        .limit(1)
    )
    if newest is not None and stamp < newest:
        return
    operations = ["header"]
    if connector.name == "pncp" and mode == "procurements" and origin == "dataAtualizacaoGlobal":
        if not PROCUREMENT_OPERATIONS.issubset(values):
            return
        items = values["items"]
        for key in PROCUREMENT_OPERATIONS:
            result = values[key]
            if (
                key == "results"
                and not result.data
                and result.availability is DataAvailability.NOT_PUBLISHED
            ):
                if (
                    not result.partial
                    and items.is_complete
                    and all(item.raw_payload.get("temResultado") is False for item in items.data)
                ):
                    continue
            if not complete_operation(result):
                return
        for key in ("detail", "version_check"):
            if (
                values[key].data is None
                or official_global_time(values[key].data, timezone) != stamp
            ):
                return
        operations.extend(sorted(PROCUREMENT_OPERATIONS))
    ids = {discovery_id, *(identifier for group in references.values() for identifier in group)}
    snapshots = [
        digest([str(run_id), record.record_key, record.collected_at.isoformat()])
        for result in values.values()
        for record in result.raw_records
    ]
    if snapshots:
        ids.update(
            (
                await session.scalars(
                    select(SourceRecord.id).where(SourceRecord.fingerprint.in_(snapshots))
                )
            ).all()
        )
    records = (await session.scalars(select(SourceRecord).where(SourceRecord.id.in_(ids)))).all()
    if len(records) != len(ids):
        return
    session.add(
        VerifiedCollectionVersion(
            source=connector.name,
            mode=mode,
            external_id=raw.external_id,
            adapter_key=adapter_key(connector),
            header_hash=digest(raw.raw_payload),
            source_updated_at=stamp,
            timestamp_origin=origin,
            operations=operations,
            source_records={str(record.id): record.response_hash for record in records},
            crawl_run_id=run_id,
            verified_at=datetime.now(UTC),
        )
    )
