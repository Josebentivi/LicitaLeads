"""Add the cooperative cancellation flag to crawl runs.

Revision ID: 0003_crawl_cancel
Revises: 0002_modality_key
Create Date: 2026-10-05

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the column already exists) as well as
databases created before the cancellation flag was introduced.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_crawl_cancel"
down_revision: str | None = "0002_modality_key"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "crawl_runs"
_COLUMN = "cancel_requested"


def _columns() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(_TABLE)}


def upgrade() -> None:
    """Add ``cancel_requested`` with a safe server default for existing rows."""

    if _COLUMN not in _columns():
        op.add_column(
            _TABLE,
            sa.Column(_COLUMN, sa.Boolean(), nullable=False, server_default=sa.false()),
        )


def downgrade() -> None:
    """Drop the cancellation flag."""

    if _COLUMN in _columns():
        with op.batch_alter_table(_TABLE) as batch_op:
            batch_op.drop_column(_COLUMN)
