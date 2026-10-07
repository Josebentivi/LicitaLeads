"""Add SRP/legal-basis fields and filter indexes to procurements.

Revision ID: 0004_procurement_filters
Revises: 0003_crawl_cancel
Create Date: <PII type="DATE" id="31"/>

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the columns and indexes already exist) as well
as databases created before the contracting-route fields were introduced.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.services.identifiers import canonical_modality, modality_category

revision: str = "0004_procurement_filters"
down_revision: str | None = "0003_crawl_cancel"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "procurements"
_COLUMNS = (
    ("is_srp", sa.Column("is_srp", sa.Boolean(), nullable=True)),
    ("legal_basis", sa.Column("legal_basis", sa.String(length=255), nullable=True)),
)
_INDEXES = (
    ("ix_procurements_estimated_value", ["estimated_value"]),
    ("ix_procurements_procurement_type", ["procurement_type"]),
)


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def upgrade() -> None:
    """Add the columns/indexes and backfill the derived contracting route."""

    inspector = _inspector()
    columns = {column["name"] for column in inspector.get_columns(_TABLE)}
    for name, column in _COLUMNS:
        if name not in columns:
            op.add_column(_TABLE, column)

    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    for name, columns_ in _INDEXES:
        if name not in indexes:
            op.create_index(name, _TABLE, columns_)

    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT id, modality, modality_key FROM procurements WHERE modality IS NOT NULL")
    ).fetchall()
    for row in rows:
        category = modality_category(row.modality)
        if category is not None:
            bind.execute(
                sa.text("UPDATE procurements SET procurement_type = :type WHERE id = :identifier"),
                {"type": category, "identifier": row.id},
            )
        key = canonical_modality(row.modality)
        if key is not None and key != row.modality_key:
            bind.execute(
                sa.text("UPDATE procurements SET modality_key = :key WHERE id = :identifier"),
                {"key": key, "identifier": row.id},
            )


def downgrade() -> None:
    """Drop the contracting-route fields and their indexes."""

    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    for name, _ in _INDEXES:
        if name in indexes:
            op.drop_index(name, table_name=_TABLE)

    columns = {column["name"] for column in _inspector().get_columns(_TABLE)}
    with op.batch_alter_table(_TABLE) as batch_op:
        for name, _ in _COLUMNS:
            if name in columns:
                batch_op.drop_column(name)
