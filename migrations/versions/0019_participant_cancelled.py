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

_CONSTRAINT = "enum_participantstatus"
_VALUES = (
    "('winner', 'awarded', 'participant', 'disqualified', 'ineligible', 'cancelled', 'unknown')"
)
_PREVIOUS_VALUES = "('winner', 'awarded', 'participant', 'disqualified', 'ineligible', 'unknown')"


def upgrade() -> None:
    with op.batch_alter_table(
        "participants", recreate="always", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.drop_constraint(_CONSTRAINT, type_="check")
        batch.create_check_constraint(_CONSTRAINT, f"status_code IN {_VALUES}")


def downgrade() -> None:
    op.execute(
        sa.text("UPDATE participants SET status_code = 'unknown' WHERE status_code = 'cancelled'")
    )
    with op.batch_alter_table(
        "participants", recreate="always", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.drop_constraint(_CONSTRAINT, type_="check")
        batch.create_check_constraint(_CONSTRAINT, f"status_code IN {_PREVIOUS_VALUES}")
