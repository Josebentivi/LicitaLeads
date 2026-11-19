"""Recover searchable text only from linked, identifiable official rows."""

from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors._common import to_int
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.pncp import parse_pncp_external_id
from app.models import (
    FieldChange,
    FieldState,
    Procurement,
    ProcurementItem,
    ProcurementSource,
    SourceRecord,
    VerifiedCollectionVersion,
)
from app.services.ingestion.verified_versions import digest
from app.services.procurement_search import published_object_complement

if TYPE_CHECKING:
    from app.services.participation_reconciliation import IntegrityReport


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("data", payload.get("resultado", payload))
    if isinstance(payload, dict):
        return [payload]
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _identifier(value: Any) -> str:
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return str(value).strip().upper()
    return ""


def _pncp_reference(link: ProcurementSource):
    try:
        return parse_pncp_external_id(link.pncp_control_number or link.external_id)
    except ValueError:
        return None


def _is_procurement_row(row: dict[str, Any], link: ProcurementSource, entity: Procurement) -> bool:
    published = _identifier(row.get("numeroControlePNCP"))
    known = _identifier(entity.pncp_control_number or link.pncp_control_number)
    if published and known:
        return published == known
    if link.source == "compras_gov":
        return _identifier(row.get("idCompra")) == _identifier(link.external_id)
    if link.source == "pncp":
        if published == _identifier(link.external_id):
            return True
        reference = _pncp_reference(link)
        agency = row.get("orgaoEntidade")
        return bool(
            reference
            and isinstance(agency, dict)
            and _identifier(agency.get("cnpj")) == reference.cnpj
            and to_int(row.get("anoCompra")) == reference.year
            and to_int(row.get("sequencialCompra")) == reference.sequence
        )
    return False


def _item_record_matches(record: SourceRecord, link: ProcurementSource) -> bool:
    try:
        path = urlsplit(record.endpoint).path.rstrip("/")
    except ValueError:
        return False
    if link.source == "pncp":
        reference = _pncp_reference(link)
        return bool(
            reference
            and path.endswith(
                f"/orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}/itens"
            )
        )
    if link.source == "compras_gov" and path.endswith("/" + ComprasGovConnector.ITEM_ID_PATH):
        key = _identifier(record.request_parameters.get("codigo"))
        kind = record.request_parameters.get("tipo")
        return bool(
            key
            and (
                (kind == "idCompra" and key == _identifier(link.external_id))
                or (
                    kind == "numeroControlePNCPCompra"
                    and key == _identifier(link.pncp_control_number or link.external_id)
                )
            )
        )
    return False


def _is_item_row(row: dict[str, Any], entity: ProcurementItem, source: str) -> bool:
    if source == "pncp":
        number = row.get("numeroItem")
    else:
        number = row.get("numeroItemPncp") or row.get("numeroItemCompra") or row.get("numeroItem")
        external_id = _identifier(row.get("idCompraItem"))
        if external_id and external_id != _identifier(entity.external_item_id):
            return False
    return _identifier(number) == _identifier(entity.item_number)


async def _verified_complements(
    db: AsyncSession, link: ProcurementSource, entity: Procurement
) -> list[tuple[str, SourceRecord]]:
    proof = await db.scalar(
        select(VerifiedCollectionVersion)
        .where(
            VerifiedCollectionVersion.source == link.source,
            VerifiedCollectionVersion.external_id == link.external_id,
            VerifiedCollectionVersion.mode == "procurements",
        )
        .order_by(VerifiedCollectionVersion.verified_at.desc(), VerifiedCollectionVersion.id.desc())
        .limit(1)
    )
    if proof is None or not proof.source_records:
        return []
    # Only the exact stored header, not another procurement on the same page,
    # can restore a field that the detail omitted.
    records = await db.scalars(
        select(SourceRecord)
        .where(
            SourceRecord.id.in_(tuple(proof.source_records)),
            SourceRecord.source == link.source,
            SourceRecord.availability == "available",
        )
        .order_by(SourceRecord.collected_at.desc(), SourceRecord.id)
    )
    candidates = []
    for record in records:
        if proof.source_records.get(str(record.id)) != record.response_hash:
            continue
        matched = [
            row for row in _rows(record.raw_payload) if _is_procurement_row(row, link, entity)
        ]
        if len(matched) == 1 and digest(matched[0]) == proof.header_hash:
            if value := published_object_complement(matched[0]):
                candidates.append((value, record))
    return candidates


async def _retained_text(
    db: AsyncSession, entity: Procurement | ProcurementItem
) -> list[tuple[str, SourceRecord]]:
    if isinstance(entity, ProcurementItem):
        record = (
            await db.get(SourceRecord, entity.source_record_id) if entity.source_record_id else None
        )
        if record is None or record.availability != "available":
            return []
        links = await db.scalars(
            select(ProcurementSource).where(
                ProcurementSource.procurement_id == entity.procurement_id,
                ProcurementSource.source == record.source,
            )
        )
        if not any(_item_record_matches(record, link) for link in links):
            return []
        rows = [
            row for row in _rows(record.raw_payload) if _is_item_row(row, entity, record.source)
        ]
        if len(rows) != 1:
            return []
        value = rows[0].get("descricao" if record.source == "pncp" else "descricaoResumida")
        return [(value, record)] if isinstance(value, str) and value.strip() else []

    linked = await db.execute(
        select(ProcurementSource, SourceRecord)
        .outerjoin(SourceRecord, SourceRecord.id == ProcurementSource.source_record_id)
        .where(
            ProcurementSource.procurement_id == entity.id,
            ProcurementSource.source.in_(("pncp", "compras_gov")),
        )
        .order_by(SourceRecord.collected_at.desc(), SourceRecord.id)
    )
    candidates = []
    for link, record in linked:
        if record and record.source == link.source and record.availability == "available":
            rows = [
                row for row in _rows(record.raw_payload) if _is_procurement_row(row, link, entity)
            ]
            if len(rows) == 1 and (value := published_object_complement(rows[0])):
                candidates.append((value, record))
                continue
        candidates.extend(await _verified_complements(db, link, entity))
    return candidates


async def inspect_legacy_search_text(
    db: AsyncSession,
    entity: Procurement | ProcurementItem,
    *,
    apply: bool,
    report: "IntegrityReport",
) -> None:
    if isinstance(entity, Procurement):
        kind, field, parent_id = "procurement", "object_complement", entity.id
    else:
        kind, field, parent_id = "procurement_item", "summary_description", entity.procurement_id
    if getattr(entity, field) is not None:
        return

    candidates = await _retained_text(db, entity)
    if not candidates:
        return
    if len({value for value, _ in candidates}) != 1 or await db.scalar(
        select(FieldState.id).where(
            FieldState.entity_type == kind,
            FieldState.entity_id == entity.id,
            FieldState.field_name == field,
        )
    ):
        report.add("search_text_conflicts_requiring_review")
        return
    report.add("search_text_requiring_repair")
    if not apply:
        return

    value, record = candidates[0]
    setattr(entity, field, value)
    provenance = {
        "entity_type": kind,
        "entity_id": entity.id,
        "field_name": field,
        "source": record.source,
        "source_record_id": record.id,
        "observed_at": record.collected_at,
    }
    db.add(FieldState(value=value, **provenance))
    db.add(
        FieldChange(
            previous_value=None,
            received_value=value,
            parent_type="procurement",
            parent_id=parent_id,
            decision="accepted_search_text_repair",
            applied=True,
            **provenance,
        )
    )
