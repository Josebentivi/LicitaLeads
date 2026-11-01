# ruff: noqa: S608
"""Add inferred procurement phase and session event types.

Revision ID: 0010_procurement_phase
Revises: 0009_participation_event
Create Date: 2026-10-21

The phase is an operational aid derived from official status, dates and
document-backed events.  It never replaces the source status.  The event enum
gains the session facts used by the dispute radar.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import NAMING_CONVENTION
from app.models.enums import ProcurementEventType, ProcurementPhase, database_enum

revision: str = "0010_procurement_phase"
down_revision: str | None = "0009_participation_event"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "procurements"
_PHASE_COLUMN = "current_phase"
_OBSERVED_COLUMN = "phase_observed_at"
_PHASE_INDEX = "ix_procurements_current_phase"
_EVENT_TABLE = "procurement_events"
_EVENT_CONSTRAINT = "enum_procurementeventtype"
_EVENT_RENDERED_CONSTRAINT = "ck_procurement_events_enum_procurementeventtype"
_NEW_EVENT_VALUES = (
    "SESSION_OPENED",
    "BEST_OFFER_DECLARED",
    "NEGOTIATION_REQUESTED",
    "DILIGENCE_REQUESTED",
    "DOCUMENTATION_REQUESTED",
)


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def _event_check() -> str | None:
    for item in _inspector().get_check_constraints(_EVENT_TABLE):
        if item["name"] == _EVENT_RENDERED_CONSTRAINT:
            return item.get("sqltext") or ""
    return None


def _event_expression(values: list[str]) -> str:
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"event_type IN ({rendered})"


def upgrade() -> None:
    """Add the phase columns/index and expand the event enum."""

    inspector = _inspector()
    columns = {column["name"] for column in inspector.get_columns(_TABLE)}
    if _PHASE_COLUMN not in columns:
        with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.add_column(
                sa.Column(
                    _PHASE_COLUMN,
                    database_enum(ProcurementPhase),
                    nullable=False,
                    server_default="unknown",
                )
            )
            if _OBSERVED_COLUMN not in columns:
                batch_op.add_column(sa.Column(_OBSERVED_COLUMN, sa.DateTime(timezone=True)))
    elif _OBSERVED_COLUMN not in columns:
        with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.add_column(sa.Column(_OBSERVED_COLUMN, sa.DateTime(timezone=True)))

    indexes = {index["name"] for index in _inspector().get_indexes(_TABLE)}
    if _PHASE_INDEX not in indexes:
        op.create_index(_PHASE_INDEX, _TABLE, [_PHASE_COLUMN])

    current = _event_check()
    if current is not None and not all(value in current for value in _NEW_EVENT_VALUES):
        with op.batch_alter_table(_EVENT_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.drop_constraint(_EVENT_CONSTRAINT, type_="check")
            batch_op.create_check_constraint(
                _EVENT_CONSTRAINT,
                _event_expression([member.value for member in ProcurementEventType]),
            )


def _purge_new_events() -> None:
    """Remove dependent rows before the downgraded check constraint is rebuilt."""

    values = ", ".join(f"'{value}'" for value in _NEW_EVENT_VALUES)
    event_ids = f"SELECT id FROM procurement_events WHERE event_type IN ({values})"
    lead_ids = f"SELECT id FROM leads WHERE triggering_event_id IN ({event_ids})"
    bind = op.get_bind()
    bind.execute(sa.text(f"DELETE FROM outreach_drafts WHERE lead_id IN ({lead_ids})"))
    bind.execute(sa.text(f"DELETE FROM lead_reviews WHERE lead_id IN ({lead_ids})"))
    bind.execute(sa.text(f"DELETE FROM leads WHERE triggering_event_id IN ({event_ids})"))
    bind.execute(sa.text(f"DELETE FROM deadlines WHERE event_id IN ({event_ids})"))
    bind.execute(sa.text(f"DELETE FROM procurement_events WHERE event_type IN ({values})"))


def downgrade() -> None:
    """Remove the phase columns and the new event values."""

    indexes = {index["name"] for index in _inspector().get_indexes(_TABLE)}
    if _PHASE_INDEX in indexes:
        op.drop_index(_PHASE_INDEX, table_name=_TABLE)

    columns = {column["name"] for column in _inspector().get_columns(_TABLE)}
    with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
        if _PHASE_COLUMN in columns:
            batch_op.drop_column(_PHASE_COLUMN)
        if _OBSERVED_COLUMN in columns:
            batch_op.drop_column(_OBSERVED_COLUMN)

    current = _event_check()
    if current is not None and any(value in current for value in _NEW_EVENT_VALUES):
        _purge_new_events()
        with op.batch_alter_table(_EVENT_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.drop_constraint(_EVENT_CONSTRAINT, type_="check")
            batch_op.create_check_constraint(
                _EVENT_CONSTRAINT,
                _event_expression(
                    [
                        member.value
                        for member in ProcurementEventType
                        if member.value not in _NEW_EVENT_VALUES
                    ]
                ),
            )
