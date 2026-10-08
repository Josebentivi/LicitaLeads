"""Allow the PARTICIPATION_DETECTED event type on procurement events.

Revision ID: 0009_participation_event
Revises: 0008_price_registry_source_record
Create Date: <PII type="DATE" id="31"/>

The enum is persisted as a constrained VARCHAR, so the new value must be added
to the check constraint.  The first revision builds tables from the live
SQLAlchemy metadata, which makes this upgrade idempotent: databases freshly
created from the updated models already carry the new value.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import NAMING_CONVENTION
from app.models.enums import ProcurementEventType

revision: str = "0009_participation_event"
down_revision: str | None = "0008_price_registry_source_record"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "procurement_events"
_ENUM_CONSTRAINT = "enum_procurementeventtype"
_RENDERED_CONSTRAINT = "ck_procurement_events_enum_procurementeventtype"
_NEW_VALUE = "PARTICIPATION_DETECTED"


def _check_expression(values: list[str]) -> str:
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"event_type IN ({rendered})"


def _current_check() -> str | None:
    inspector = sa.inspect(op.get_bind())
    for item in inspector.get_check_constraints(_TABLE):
        if item["name"] == _RENDERED_CONSTRAINT:
            return item.get("sqltext") or ""
    return None


def _purge_participation_events() -> None:
    """Delete the programmatic events and dependent rows before the rebuild.

    The downgraded check constraint cannot hold ``PARTICIPATION_DETECTED`` rows,
    so they (and the leads/deadlines derived from them) are removed explicitly,
    without relying on the connection's foreign-key pragma.
    """

    bind = op.get_bind()
    bind.execute(
        sa.text(
            "DELETE FROM outreach_drafts WHERE lead_id IN ("
            "SELECT id FROM leads WHERE triggering_event_id IN ("
            "SELECT id FROM procurement_events WHERE event_type = :value))"
        ),
        {"value": _NEW_VALUE},
    )
    bind.execute(
        sa.text(
            "DELETE FROM lead_reviews WHERE lead_id IN ("
            "SELECT id FROM leads WHERE triggering_event_id IN ("
            "SELECT id FROM procurement_events WHERE event_type = :value))"
        ),
        {"value": _NEW_VALUE},
    )
    bind.execute(
        sa.text(
            "DELETE FROM leads WHERE triggering_event_id IN ("
            "SELECT id FROM procurement_events WHERE event_type = :value)"
        ),
        {"value": _NEW_VALUE},
    )
    bind.execute(
        sa.text(
            "DELETE FROM deadlines WHERE event_id IN ("
            "SELECT id FROM procurement_events WHERE event_type = :value)"
        ),
        {"value": _NEW_VALUE},
    )
    bind.execute(
        sa.text("DELETE FROM procurement_events WHERE event_type = :value"),
        {"value": _NEW_VALUE},
    )


def upgrade() -> None:
    """Add the new value to the event-type check constraint."""

    current = _current_check()
    if current is None or _NEW_VALUE in current:
        return
    with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
        batch_op.drop_constraint(_ENUM_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(
            _ENUM_CONSTRAINT,
            _check_expression([member.value for member in ProcurementEventType]),
        )


def downgrade() -> None:
    """Remove the value from the check constraint."""

    current = _current_check()
    if current is None or _NEW_VALUE not in current:
        return
    _purge_participation_events()
    with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
        batch_op.drop_constraint(_ENUM_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(
            _ENUM_CONSTRAINT,
            _check_expression(
                [member.value for member in ProcurementEventType if member.value != _NEW_VALUE]
            ),
        )
