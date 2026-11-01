"""Prospective collection coverage, verification proofs and resolved document URLs."""

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID, UTCDateTime

revision = "0023_collection_sync"
down_revision = "0022_incremental_audit"
branch_labels = None
depends_on = None


def _create_table(name, *columns, **kwargs):
    if name not in sa.inspect(op.get_bind()).get_table_names():
        op.create_table(name, *columns, **kwargs)


def upgrade() -> None:
    _create_table(
        "collection_recheck_states",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("stream_key", sa.String(64), nullable=False, index=True),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("attempted_at", UTCDateTime(), nullable=False),
        sa.UniqueConstraint("stream_key", "external_id"),
    )
    if "resolved_url" not in {
        c["name"] for c in sa.inspect(op.get_bind()).get_columns("documents")
    }:
        op.add_column("documents", sa.Column("resolved_url", sa.Text(), nullable=True))
    _create_table(
        "collection_sync_states",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("stream_key", sa.String(64), nullable=False, unique=True),
        sa.Column("filters", sa.JSON(), nullable=False),
        sa.Column("covered_until", sa.Date(), nullable=True),
        sa.Column("bootstrap_started_at", UTCDateTime(), nullable=False),
        sa.Column("last_reconciliation_at", UTCDateTime(), nullable=True),
        sa.Column("pending_request", sa.JSON(), nullable=True),
        sa.Column(
            "pending_batch_id", GUID(), sa.ForeignKey("collection_batches.id", ondelete="SET NULL")
        ),
    )
    _create_table(
        "verified_collection_versions",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("mode", sa.String(40), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("adapter_key", sa.String(64), nullable=False),
        sa.Column("header_hash", sa.String(64), nullable=False),
        sa.Column("source_updated_at", UTCDateTime(), nullable=False),
        sa.Column("timestamp_origin", sa.String(100), nullable=False),
        sa.Column("operations", sa.JSON(), nullable=False),
        sa.Column("source_records", sa.JSON(), nullable=False),
        sa.Column("verified_at", UTCDateTime(), nullable=False),
        sa.Column("crawl_run_id", GUID(), sa.ForeignKey("crawl_runs.id", ondelete="SET NULL")),
    )
    if "ix_verified_collection_identity" not in {
        i["name"] for i in sa.inspect(op.get_bind()).get_indexes("verified_collection_versions")
    }:
        op.create_index(
            "ix_verified_collection_identity",
            "verified_collection_versions",
            ["source", "mode", "external_id", "verified_at"],
        )
    _create_table(
        "source_response_validators",
        sa.Column("id", GUID(), primary_key=True),
        sa.Column("validator_key", sa.String(64), nullable=False, unique=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column(
            "source_record_id", GUID(), sa.ForeignKey("source_records.id", ondelete="SET NULL")
        ),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("validated_at", UTCDateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("collection_recheck_states")
    op.drop_table("source_response_validators")
    op.drop_index("ix_verified_collection_identity", table_name="verified_collection_versions")
    op.drop_table("verified_collection_versions")
    op.drop_table("collection_sync_states")
    op.drop_column("documents", "resolved_url")
