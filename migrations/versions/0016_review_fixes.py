"""Add the session recheck rotation timestamp.

Revision ID: 0016_review_fixes
Revises: 0015_contract_index_cleanup
Create Date: 2026-10-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016_review_fixes"
down_revision: str | None = "0015_contract_index_cleanup"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DOCUMENT_COLUMN = "last_checked_at"
_DOCUMENT_INDEX = "ix_documents_last_checked_at"


def upgrade() -> None:
    """Add the recheck timestamp used to rotate session documents."""

    inspector = sa.inspect(op.get_bind())
    document_columns = {column["name"] for column in inspector.get_columns("documents")}
    if _DOCUMENT_COLUMN not in document_columns:
        with op.batch_alter_table("documents") as batch:
            batch.add_column(sa.Column(_DOCUMENT_COLUMN, sa.DateTime(timezone=True), nullable=True))
    document_indexes = {index["name"] for index in inspector.get_indexes("documents")}
    if _DOCUMENT_INDEX not in document_indexes:
        op.create_index(_DOCUMENT_INDEX, "documents", [_DOCUMENT_COLUMN])


def downgrade() -> None:
    """Remove the recheck timestamp and its index."""

    inspector = sa.inspect(op.get_bind())
    document_indexes = {index["name"] for index in inspector.get_indexes("documents")}
    if _DOCUMENT_INDEX in document_indexes:
        op.drop_index(_DOCUMENT_INDEX, table_name="documents")
    document_columns = {column["name"] for column in inspector.get_columns("documents")}
    if _DOCUMENT_COLUMN in document_columns:
        with op.batch_alter_table("documents") as batch:
            batch.drop_column(_DOCUMENT_COLUMN)
