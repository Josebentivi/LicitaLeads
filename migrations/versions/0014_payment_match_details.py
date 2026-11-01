"""Add payment match confidence and reason.

Revision ID: 0014_payment_match_details
Revises: 0013_contract_items_terms
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import NAMING_CONVENTION

revision: str = "0014_payment_match_details"
down_revision: str | None = "0013_contract_items_terms"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = {
    "match_confidence": sa.Column("match_confidence", sa.Numeric(5, 4), nullable=True),
    "match_reason": sa.Column("match_reason", sa.String(255), nullable=True),
}


def upgrade() -> None:
    """Add the columns when they are absent."""

    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("payments")}
    with op.batch_alter_table("payments", naming_convention=NAMING_CONVENTION) as batch:
        for name, column in _COLUMNS.items():
            if name not in columns:
                batch.add_column(column)


def downgrade() -> None:
    """Drop the columns when present."""

    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("payments")}
    with op.batch_alter_table("payments", naming_convention=NAMING_CONVENTION) as batch:
        for name in _COLUMNS:
            if name in columns:
                batch.drop_column(name)
