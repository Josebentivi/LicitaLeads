"""Add retry bookkeeping for failed document extractions.

Revision ID: 0017_document_retry
Revises: 0016_review_fixes
Create Date: 2026-10-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017_document_retry"
down_revision: str | None = "0016_review_fixes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ATTEMPTS = "extraction_attempts"
_NEXT = "next_attempt_at"
_INDEX = "ix_documents_next_attempt_at"


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("documents")}
    with op.batch_alter_table("documents") as batch:
        if _ATTEMPTS not in columns:
            batch.add_column(sa.Column(_ATTEMPTS, sa.Integer(), nullable=False, server_default="0"))
        if _NEXT not in columns:
            batch.add_column(sa.Column(_NEXT, sa.DateTime(timezone=True), nullable=True))
    indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("documents")}
    if _INDEX not in indexes:
        op.create_index(_INDEX, "documents", [_NEXT])


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    indexes = {index["name"] for index in inspector.get_indexes("documents")}
    if _INDEX in indexes:
        op.drop_index(_INDEX, table_name="documents")
    columns = {column["name"] for column in inspector.get_columns("documents")}
    with op.batch_alter_table("documents") as batch:
        if _NEXT in columns:
            batch.drop_column(_NEXT)
        if _ATTEMPTS in columns:
            batch.drop_column(_ATTEMPTS)
