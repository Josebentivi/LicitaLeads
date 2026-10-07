"""Backfill structured procurement fields from already-collected raw payloads.

The auditability rule is that conclusions come from official sources.  Raw
responses are persisted in ``SourceRecord.raw_payload`` before normalization,
so fields introduced after an initial crawl can be recovered without hitting
the source APIs again.  This module never invents data: it only reads keys that
are literally present in the stored payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import async_session_factory
from app.models import FieldObservation, FieldValueStatus, Procurement, SourceRecord
from app.services.identifiers import modality_category
from app.services.ingestion.pipeline import _json_value, stable_fingerprint


@dataclass(slots=True)
class BackfillSummary:
    """Counters for one idempotent backfill execution."""

    scanned_records: int = 0
    updated_procurements: int = 0
    skipped: int = 0
    diagnostics: list[str] = field(default_factory=list)


def _pncp_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if payload.get("numeroControlePNCP"):
        return [payload]
    return []


def _compras_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    resultado = payload.get("resultado")
    if isinstance(resultado, list):
        return [row for row in resultado if isinstance(row, dict)]
    if payload.get("numeroControlePNCP") or payload.get("idCompra"):
        return [payload]
    return []


def _pncp_legal_basis(row: dict[str, Any]) -> str | None:
    amparo = row.get("amparoLegal")
    if isinstance(amparo, dict):
        return amparo.get("nome") or amparo.get("descricao")
    if isinstance(amparo, str) and amparo.strip():
        return amparo
    return row.get("amparoLegalNome")


def _observed_fields(source: str, row: dict[str, Any]) -> dict[str, Any]:
    if source == "pncp":
        return {
            "is_srp": row.get("srp"),
            "legal_basis": _pncp_legal_basis(row),
        }
    if source == "compras_gov":
        return {
            "is_srp": row.get("srp"),
            "legal_basis": (
                row.get("amparoLegalNome")
                or row.get("amparoLegalDescricao")
                or row.get("amparoLegalCodigoPncp")
            ),
        }
    return {}


def _row_identifiers(source: str, row: dict[str, Any]) -> tuple[str | None, str | None]:
    control = row.get("numeroControlePNCP")
    control = str(control).strip().upper() if control else None
    if source == "pncp":
        if control:
            return control, None
        agency = row.get("orgaoEntidade") or {}
        cnpj = agency.get("cnpj")
        year = row.get("anoCompra")
        sequence = row.get("sequencialCompra")
        if cnpj and year and sequence:
            return None, f"{cnpj}:{year}:{sequence}"
        return None, None
    external = row.get("idCompra")
    return control, str(external) if external else None


async def _find_procurement(
    session: AsyncSession,
    source: str,
    control: str | None,
    external_id: str | None,
) -> Procurement | None:
    if control:
        matched = await session.scalar(
            select(Procurement).where(Procurement.pncp_control_number == control)
        )
        if matched is not None:
            return matched
    if external_id:
        return await session.scalar(
            select(Procurement).where(
                Procurement.source == source,
                Procurement.external_id == external_id,
            )
        )
    return None


async def _observe(
    session: AsyncSession,
    procurement: Procurement,
    source: str,
    source_record_id: UUID,
    field_name: str,
    value: Any,
) -> None:
    normalized = _json_value(value)
    fingerprint = stable_fingerprint(
        "field_observation",
        procurement.id,
        source,
        field_name,
        normalized,
        FieldValueStatus.OBSERVED.value,
    )
    exists = await session.scalar(
        select(FieldObservation.id).where(FieldObservation.fingerprint == fingerprint)
    )
    if exists is not None:
        return
    session.add(
        FieldObservation(
            entity_type="procurement",
            entity_id=procurement.id,
            field_name=field_name,
            value=normalized,
            value_status=FieldValueStatus.OBSERVED,
            source=source,
            source_record_id=source_record_id,
            confidence=Decimal("1"),
            collected_at=datetime.now(UTC),
            fingerprint=fingerprint,
        )
    )


async def backfill_procurement_fields(
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    limit: int | None = None,
    batch_size: int = 200,
) -> BackfillSummary:
    """Recover ``is_srp``/``legal_basis``/``procurement_type`` from raw payloads.

    Stored responses are walked with keyset pagination so a database with many
    crawls is never fully materialized in memory.
    """

    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    factory = session_factory or async_session_factory
    summary = BackfillSummary()
    last_collected_at: datetime | None = None
    last_id: UUID | None = None
    while True:
        remaining = None if limit is None else limit - summary.scanned_records
        if remaining is not None and remaining <= 0:
            break
        batch_limit = batch_size if remaining is None else min(batch_size, remaining)
        async with factory() as session:
            statement = select(SourceRecord).where(
                SourceRecord.source.in_(("pncp", "compras_gov")),
                SourceRecord.raw_payload.is_not(None),
            )
            if last_collected_at is not None and last_id is not None:
                statement = statement.where(
                    tuple_(SourceRecord.collected_at, SourceRecord.id)
                    > (last_collected_at, last_id)
                )
            statement = statement.order_by(SourceRecord.collected_at, SourceRecord.id).limit(
                batch_limit
            )
            records = list((await session.scalars(statement)).all())
        if not records:
            break
        for record in records:
            summary.scanned_records += 1
            rows = (
                _pncp_rows(record.raw_payload)
                if record.source == "pncp"
                else _compras_rows(record.raw_payload)
            )
            if not rows:
                summary.skipped += 1
                continue
            async with factory() as session, session.begin():
                for row in rows:
                    control, external_id = _row_identifiers(record.source, row)
                    if not control and not external_id:
                        summary.skipped += 1
                        continue
                    procurement = await _find_procurement(
                        session, record.source, control, external_id
                    )
                    if procurement is None:
                        summary.skipped += 1
                        continue
                    observed = _observed_fields(record.source, row)
                    changed = False
                    if procurement.is_srp is None and observed.get("is_srp") is not None:
                        procurement.is_srp = bool(observed["is_srp"])
                        await _observe(
                            session,
                            procurement,
                            record.source,
                            record.id,
                            "is_srp",
                            procurement.is_srp,
                        )
                        changed = True
                    if procurement.legal_basis is None and observed.get("legal_basis"):
                        procurement.legal_basis = str(observed["legal_basis"])[:255]
                        await _observe(
                            session,
                            procurement,
                            record.source,
                            record.id,
                            "legal_basis",
                            procurement.legal_basis,
                        )
                        changed = True
                    if procurement.procurement_type is None:
                        category = modality_category(procurement.modality)
                        if category is not None:
                            procurement.procurement_type = category
                            await _observe(
                                session,
                                procurement,
                                record.source,
                                record.id,
                                "procurement_type",
                                category,
                            )
                            changed = True
                    if changed:
                        summary.updated_procurements += 1
                    else:
                        summary.skipped += 1
        last = records[-1]
        last_collected_at, last_id = last.collected_at, last.id
        if len(records) < batch_limit:
            break
    return summary
