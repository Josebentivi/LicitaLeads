"""Add canonical procurement modality key and reindex the strong identity.

Revision ID: 0002_modality_key
Revises: 0001_initial
Create Date: 2026-09-26

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the column and index already exist) as well as
databases created before the canonical modality key was introduced.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.services.identifiers import canonical_modality

revision: str = "0002_modality_key"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "procurements"
_COLUMN = "modality_key"
_INDEX = "ix_procurements_modality_key"
_OLD_IDENTITY_INDEX = "ix_procurements_purchase_identity"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def upgrade() -> None:
    """Add ``modality_key``, backfill it, and rebuild the identity index."""

    inspector = _inspector()
    columns = {column["name"] for column in inspector.get_columns(_TABLE)}
    if _COLUMN not in columns:
        op.add_column(_TABLE, sa.Column(_COLUMN, sa.String(length=100), nullable=True))
        inspector = _inspector()

    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    if _INDEX not in indexes:
        op.create_index(_INDEX, _TABLE, [_COLUMN])

    if _OLD_IDENTITY_INDEX in indexes:
        op.drop_index(_OLD_IDENTITY_INDEX, table_name=_TABLE)
    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    if _OLD_IDENTITY_INDEX not in indexes:
        op.create_index(
            _OLD_IDENTITY_INDEX,
            _TABLE,
            ["agency_cnpj", "uasg", "purchase_number", "purchase_year", _COLUMN],
        )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT id, modality FROM procurements WHERE modality IS NOT NULL")
    ).fetchall()
    for row in rows:
        key = canonical_modality(row.modality)
        if key is not None:
            bind.execute(
                sa.text("UPDATE procurements SET modality_key = :key WHERE id = :identifier"),
                {"key": key, "identifier": row.id},
            )


def downgrade() -> None:
    """Restore the raw-modality identity index and drop the canonical key."""

    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    if _INDEX in indexes:
        op.drop_index(_INDEX, table_name=_TABLE)

    if _OLD_IDENTITY_INDEX in indexes:
        op.drop_index(_OLD_IDENTITY_INDEX, table_name=_TABLE)
    op.create_index(
        _OLD_IDENTITY_INDEX,
        _TABLE,
        ["agency_cnpj", "uasg", "purchase_number", "purchase_year", "modality"],
    )

    columns = {column["name"] for column in inspector.get_columns(_TABLE)}
    if _COLUMN in columns:
        with op.batch_alter_table(_TABLE) as batch_op:
            batch_op.drop_column(_COLUMN)
