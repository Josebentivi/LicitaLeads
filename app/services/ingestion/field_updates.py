"""One transactional merge policy for all ingestion paths.

The listener is installed on an individual ingestion session, never globally.
It runs before *every* flush, including SQLAlchemy autoflush, so a later query
cannot accidentally commit an unaudited intermediate value. No network work is
performed here. Ordinary operator edits and document processing are unaffected.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Numeric, event, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.models import GUID, FieldChange, FieldState, SourceRecord, UTCDateTime

ROOTS = {
    "procurements": "procurement",
    "price_registries": "price_registry",
    "contracts": "contract",
    "payments": "payment",
}
CHILDREN = {
    "procurement_items": ("procurement_item", "procurement", "procurement_id"),
    "documents": ("document", "procurement", "procurement_id"),
    "participants": ("participant", "procurement", "procurement_id"),
    "price_registry_items": ("price_registry_item", "price_registry", "price_registry_id"),
    "price_registry_units": ("price_registry_unit", "price_registry", "price_registry_id"),
    "price_registry_commitments": (
        "price_registry_commitment",
        "price_registry",
        "price_registry_id",
    ),
    "price_registry_adhesions": ("price_registry_adhesion", "price_registry", "price_registry_id"),
    "contract_items": ("contract_item", "contract", "contract_id"),
    "contract_terms": ("contract_term", "contract", "contract_id"),
}
METADATA = {
    "resolved_url",
    "id",
    "fingerprint",
    "created_at",
    "updated_at",
    "phase_observed_at",
    # This operator aid has its own inferred FieldObservation trail and clock.
    "current_phase",
    "source",
    "external_id",
    "source_record_id",
    "source_url",
    "last_source_update_at",
    "updated_at_source",
    "extraction_status",
    "extraction_error",
    "extraction_attempts",
    "next_attempt_at",
    "downloaded_at",
    "last_checked_at",
    "local_path",
    "file_hash",
    "extracted_text",
    "match_confidence",
    "match_reason",
    "requires_manual_review",
    "confidence",
}
# These values are recalculated from identity/evidence, not missing source fields.
DERIVED = {"contract_id", "procurement_id", "company_id", "match_status", "deactivated_at"}
# DTO and canonical names differ here; unchanged observations must refresh their clock too.
SOURCE_FIELDS = {"documents": {"is_active": "active"}}


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value


@dataclass
class RecordOutcome:
    changed: bool = False
    stale: bool = False
    conflicted: bool = False

    def count(self, summary: Any, created: bool, *, prefix: str = "records") -> None:
        for suffix, increment in (
            ("changed", not created and self.changed),
            ("unchanged", not created and not self.changed),
            ("stale_ignored", self.stale),
            ("conflicted", self.conflicted),
        ):
            name = f"{prefix}_{suffix}"
            setattr(summary, name, (getattr(summary, name, 0) or 0) + int(increment))


@dataclass
class AuditTracker:
    raw: Any
    run_id: UUID | None
    source_record_id: UUID | None
    complete: bool
    observed_at: datetime
    outcome: RecordOutcome = field(default_factory=RecordOutcome)
    registered: dict[int, Any] = field(default_factory=dict)
    states: dict[tuple[str, UUID], dict[str, FieldState]] = field(default_factory=dict)

    def register(self, entity: Any, raw: Any) -> None:
        self.registered[id(entity)] = raw


def install_audit(
    session: AsyncSession,
    raw: Any,
    *,
    run_id: UUID | None = None,
    source_record_id: UUID | None = None,
    complete: bool = False,
    observed_at: datetime | None = None,
) -> AuditTracker:
    tracker = AuditTracker(
        raw=raw,
        run_id=run_id,
        source_record_id=source_record_id,
        complete=complete,
        observed_at=utc(observed_at or getattr(raw, "observed_at", None) or datetime.now(UTC)),
    )
    session.info["field_update_tracker"] = tracker
    if not session.info.get("field_update_listener"):
        event.listen(session.sync_session, "before_flush", _before_flush)
        session.info["field_update_listener"] = True
    return tracker


def _before_flush(session: Session, _flush_context: Any, _instances: Any) -> None:
    tracker: AuditTracker = session.info["field_update_tracker"]
    for entity in list(session.new) + list(session.dirty):
        table = getattr(entity, "__tablename__", "")
        if table not in ROOTS and table not in CHILDREN:
            continue
        if entity.id is None:
            entity.id = uuid4()
        root = table in ROOTS
        if root:
            entity_type = parent_type = ROOTS[table]
            parent_id = entity.id
        else:
            entity_type, parent_type, parent_field = CHILDREN[table]
            parent_id = getattr(entity, parent_field)
        if parent_id is None:
            continue
        raw = tracker.registered.get(id(entity), tracker.raw if root else None)
        source_id = getattr(entity, "source_record_id", None) or tracker.source_record_id
        source_record = session.get(SourceRecord, source_id) if source_id else None
        source = str(
            getattr(raw, "source", None)
            or (
                source_record.source if source_record else getattr(tracker.raw, "source", "unknown")
            )
            or "unknown"
        )
        observed_at = utc(
            getattr(raw, "observed_at", None)
            or (source_record.collected_at if source_record else tracker.observed_at)
        )
        source_at = getattr(raw, "source_updated_at", None)
        origin = getattr(raw, "source_update_field", None)
        if source_at is None and raw is not None:
            source_at = getattr(raw, "last_source_update_at", None)
            origin = "last_source_update_at" if source_at else None
        if source_at is None and raw is not None and hasattr(raw, "updated_at"):
            source_at = raw.updated_at
            origin = "updated_at" if source_at else None
        source_at = utc(source_at) if source_at else None
        key = (entity_type, entity.id)
        inspection = inspect(entity)
        if key not in tracker.states:
            # Serialize competing writers before loading the winning field state.
            if inspection.persistent:
                session.execute(
                    select(type(entity).id).where(type(entity).id == entity.id).with_for_update()
                )
            tracker.states[key] = {
                item.field_name: item
                for item in session.scalars(
                    select(FieldState)
                    .where(FieldState.entity_type == entity_type, FieldState.entity_id == entity.id)
                    .with_for_update()
                )
            }
        states = tracker.states[key]
        clock_history = (
            inspection.attrs["last_source_update_at"].history if table == "procurements" else None
        )
        legacy_clock = None
        if table == "procurements" and inspection.persistent:
            legacy_clock = (
                clock_history.deleted[0]
                if clock_history and clock_history.deleted
                else entity.last_source_update_at
            )
            legacy_clock = utc(legacy_clock) if legacy_clock else None
        if clock_history and clock_history.deleted and source_at:
            prior_clock = clock_history.deleted[0]
            if prior_clock and source_at < utc(prior_clock):
                entity.last_source_update_at = prior_clock
        for attribute in inspection.mapper.column_attrs:
            name = attribute.key
            if name in METADATA:
                continue
            history = inspection.attrs[name].history
            received = getattr(entity, name)
            source_name = SOURCE_FIELDS.get(table, {}).get(name, name)
            source_value = getattr(raw, source_name, None)
            provided = (
                raw is not None
                and source_value is not None
                and json_value(source_value) == json_value(received)
            )
            if not history.has_changes() and not provided:
                continue
            previous = history.deleted[0] if history.deleted else None
            if not history.has_changes():
                previous = received
            state = states.get(name)
            old = state.value if state is not None else json_value(previous)
            candidate = json_value(received)
            if candidate == old:
                # Refresh freshness even if the business value did not change.
                if state is None and candidate is not None:
                    legacy_newer = bool(source_at and legacy_clock and source_at < legacy_clock)
                    state = FieldState(
                        entity_type=entity_type,
                        entity_id=entity.id,
                        field_name=name,
                        value=candidate,
                        source="legacy" if legacy_newer else source,
                        source_record_id=None if legacy_newer else source_id,
                        source_updated_at=max(source_at, legacy_clock)
                        if source_at and legacy_clock
                        else source_at,
                        timestamp_origin="last_source_update_at (legacy)"
                        if legacy_newer
                        else origin,
                        observed_at=observed_at,
                        uncertain=False,
                    )
                    states[name] = state
                    session.add(state)
                if (
                    state is not None
                    and observed_at > utc(state.observed_at)
                    and not (
                        source_at
                        and state.source_updated_at
                        and source_at < utc(state.source_updated_at)
                    )
                ):
                    state.observed_at = observed_at
                    if source_at is not None and (
                        state.source_updated_at is None or source_at >= utc(state.source_updated_at)
                    ):
                        state.source = source
                        state.source_record_id = source_id
                        state.source_updated_at = source_at
                        state.timestamp_origin = origin
                continue
            explicit_clear = name in getattr(raw, "cleared_fields", []) and (
                tracker.complete or bool(source_record and source_record.collection_complete)
            )
            missing = received is None or (isinstance(received, str) and not received.strip())
            if missing and not explicit_clear and name not in DERIVED:
                setattr(entity, name, _restore(old, previous, attribute.columns[0].type))
                continue
            decision = "created" if old is None else "updated"
            applied = True
            if state is not None:
                if (
                    source_at is not None
                    and state.source_updated_at is not None
                    and source_at < utc(state.source_updated_at)
                ):
                    decision, applied = "stale_ignored", False
                    tracker.outcome.stale = True
                elif not (
                    source_at is not None
                    and state.source_updated_at is not None
                    and source_at > utc(state.source_updated_at)
                ) and observed_at < utc(state.observed_at):
                    decision, applied = "observation_ignored", False
                    tracker.outcome.stale = True
                elif old is not None and not (
                    source_at is not None
                    and state.source_updated_at is not None
                    and source_at > utc(state.source_updated_at)
                ):
                    decision = "conflict_applied"
                    tracker.outcome.conflicted = True
            elif old is not None:
                if source_at is not None and legacy_clock is not None and source_at < legacy_clock:
                    decision, applied = "stale_ignored", False
                    tracker.outcome.stale = True
                else:
                    decision = "conflict_applied"
                    tracker.outcome.conflicted = True
            session.add(
                FieldChange(
                    entity_type=entity_type,
                    entity_id=entity.id,
                    parent_type=parent_type,
                    parent_id=parent_id,
                    field_name=name,
                    previous_value=old,
                    received_value=candidate,
                    decision=decision,
                    applied=applied,
                    source=source,
                    previous_source=state.source if state else None,
                    previous_source_record_id=state.source_record_id if state else None,
                    source_record_id=source_id,
                    crawl_run_id=tracker.run_id,
                    evidence_id=getattr(entity, "evidence_id", None),
                    previous_source_updated_at=state.source_updated_at if state else legacy_clock,
                    source_updated_at=source_at,
                    timestamp_origin=origin,
                    observed_at=observed_at,
                )
            )
            if not applied:
                # Column history retains the correctly typed previous Python value.
                # Concurrent writers are fenced by the collection leases.
                setattr(entity, name, _restore(old, previous, attribute.columns[0].type))
                continue
            tracker.outcome.changed = True
            if state is None:
                state = FieldState(
                    entity_type=entity_type,
                    entity_id=entity.id,
                    field_name=name,
                )
                states[name] = state
                session.add(state)
            state.value = candidate
            state.source = source
            state.source_record_id = source_id
            state.source_updated_at = source_at
            state.timestamp_origin = origin
            state.observed_at = observed_at
            state.uncertain = decision == "conflict_applied"


def _restore(value: Any, previous: Any, column_type: Any) -> Any:
    """Restore the accepted state, even if this transaction loaded an older row."""
    if value is None:
        return None
    enum_class = getattr(column_type, "enum_class", None)
    if enum_class is not None:
        return enum_class(value)
    if isinstance(column_type, GUID):
        return UUID(str(value))
    if isinstance(column_type, UTCDateTime):
        return datetime.fromisoformat(value)
    if isinstance(column_type, Numeric):
        return Decimal(str(value))
    return value
