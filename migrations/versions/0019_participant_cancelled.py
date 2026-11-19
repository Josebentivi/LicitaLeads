"""Add the cancelled participation outcome to the status constraint.

Revision ID: 0019_participant_cancelled
Revises: 0018_integrity_heartbeat
Create Date: 2026-10-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import NAMING_CONVENTION

revision: str = "0019_participant_cancelled"
down_revision: str | None = "0018_integrity_heartbeat"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_participants_enum_participantstatus"
_VALUES = (
    "('winner', 'awarded', 'participant', 'disqualified', 'ineligible', 'cancelled', 'unknown')"
)
_PREVIOUS_VALUES = "('winner', 'awarded', 'participant', 'disqualified', 'ineligible', 'unknown')"


def _replace_status_constraint(values: str) -> None:
    # SQLite rebuilds the table. PostgreSQL can alter the check in place and
    # retain foreign keys from tables that already reference participants.
    with op.batch_alter_table("participants", naming_convention=NAMING_CONVENTION) as batch:
        batch.drop_constraint(op.f(_CONSTRAINT), type_="check")
        batch.create_check_constraint(op.f(_CONSTRAINT), f"status_code IN {values}")


def upgrade() -> None:
    _replace_status_constraint(_VALUES)


def downgrade() -> None:
    op.execute(
        sa.text("UPDATE participants SET status_code = 'unknown' WHERE status_code = 'cancelled'")
    )
    _replace_status_constraint(_PREVIOUS_VALUES)
