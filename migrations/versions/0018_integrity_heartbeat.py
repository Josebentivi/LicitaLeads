"""Add document retraction and crawl heartbeat fields.

Revision ID: 0018_integrity_heartbeat
Revises: 0017_document_retry
Create Date: 2026-10-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018_integrity_heartbeat"
down_revision: str | None = "0017_document_retry"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    document_columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("documents")
    }
    with op.batch_alter_table("documents") as batch:
        if "is_active" not in document_columns:
            batch.add_column(
                sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true())
            )
        if "deactivated_at" not in document_columns:
            batch.add_column(sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True))
    document_indexes = {
        index["name"] for index in sa.inspect(op.get_bind()).get_indexes("documents")
    }
    if "ix_documents_is_active" not in document_indexes:
        op.create_index("ix_documents_is_active", "documents", ["is_active"])

    crawl_columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("crawl_runs")
    }
    with op.batch_alter_table("crawl_runs") as batch:
        if "heartbeat_at" not in crawl_columns:
            batch.add_column(sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True))
    crawl_indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("crawl_runs")}
    if "ix_crawl_runs_heartbeat_at" not in crawl_indexes:
        op.create_index("ix_crawl_runs_heartbeat_at", "crawl_runs", ["heartbeat_at"])


def downgrade() -> None:
    crawl_indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("crawl_runs")}
    if "ix_crawl_runs_heartbeat_at" in crawl_indexes:
        op.drop_index("ix_crawl_runs_heartbeat_at", table_name="crawl_runs")
    crawl_columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("crawl_runs")
    }
    if "heartbeat_at" in crawl_columns:
        with op.batch_alter_table("crawl_runs") as batch:
            batch.drop_column("heartbeat_at")

    document_indexes = {
        index["name"] for index in sa.inspect(op.get_bind()).get_indexes("documents")
    }
    if "ix_documents_is_active" in document_indexes:
        op.drop_index("ix_documents_is_active", table_name="documents")
    document_columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("documents")
    }
    with op.batch_alter_table("documents") as batch:
        if "deactivated_at" in document_columns:
            batch.drop_column("deactivated_at")
        if "is_active" in document_columns:
            batch.drop_column("is_active")
