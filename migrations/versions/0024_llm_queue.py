"""Durable optional analysis, validated results and actual request usage."""

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID, UTCDateTime

revision = "0024_llm_queue"
down_revision = "0023_collection_sync"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Initial revision creates current metadata. Explicit definitions also
    # support upgrades of existing databases without importing future models.
    tables = {
        "llm_batches": [
            sa.Column(
                "crawl_run_id",
                GUID(),
                sa.ForeignKey("crawl_runs.id", ondelete="CASCADE"),
                unique=True,
            ),
            sa.Column("label", sa.String(100), nullable=False),
            sa.Column("cancelled", sa.Boolean(), nullable=False),
            sa.Column("created_at", UTCDateTime(), nullable=False),
        ],
        "llm_results": [
            sa.Column("cache_key", sa.String(64), unique=True, nullable=False),
            sa.Column("analyses", sa.JSON(), nullable=False),
            sa.Column("created_at", UTCDateTime(), nullable=False),
        ],
        "llm_work_items": [
            sa.Column("job_key", sa.String(64), unique=True, nullable=False),
            sa.Column("cache_key", sa.String(64), nullable=False, index=True),
            sa.Column("config_key", sa.String(64), nullable=False, index=True),
            sa.Column(
                "document_id",
                GUID(),
                sa.ForeignKey("documents.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("chunk_id", GUID(), sa.ForeignKey("document_chunks.id", ondelete="SET NULL")),
            sa.Column("snapshot", sa.JSON(), nullable=False),
            sa.Column("status", sa.String(20), nullable=False, index=True),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("next_attempt_at", UTCDateTime()),
            sa.Column("result_id", GUID(), sa.ForeignKey("llm_results.id", ondelete="SET NULL")),
            sa.Column("reused", sa.Boolean(), nullable=False),
            sa.Column("error", sa.JSON()),
            sa.Column("created_at", UTCDateTime(), nullable=False),
            sa.Column("completed_at", UTCDateTime()),
        ],
        "llm_demands": [
            sa.Column("reused", sa.Boolean(), nullable=False),
            sa.Column(
                "batch_id",
                GUID(),
                sa.ForeignKey("llm_batches.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column(
                "work_id",
                GUID(),
                sa.ForeignKey("llm_work_items.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.UniqueConstraint("batch_id", "work_id"),
        ],
        "llm_provider_states": [
            sa.Column("config_key", sa.String(64), unique=True, nullable=False),
            sa.Column("blocked_at", UTCDateTime()),
            sa.Column("cooldown_until", UTCDateTime()),
            sa.Column("error", sa.JSON()),
        ],
        "llm_attempts": [
            sa.Column("result_id", GUID(), sa.ForeignKey("llm_results.id", ondelete="SET NULL")),
            sa.Column("work_id", GUID(), sa.ForeignKey("llm_work_items.id", ondelete="SET NULL")),
            sa.Column("batch_id", GUID(), sa.ForeignKey("llm_batches.id", ondelete="SET NULL")),
            sa.Column("started_at", UTCDateTime(), nullable=False, index=True),
            sa.Column("finished_at", UTCDateTime()),
            sa.Column("model", sa.String(200), nullable=False),
            sa.Column("status", sa.String(30), nullable=False),
            sa.Column("diagnostic", sa.JSON()),
            sa.Column("input_tokens", sa.Integer()),
            sa.Column("output_tokens", sa.Integer()),
            sa.Column("input_price", sa.Numeric(24, 12), nullable=False),
            sa.Column("output_price", sa.Numeric(24, 12), nullable=False),
            sa.Column("cost_usd", sa.Numeric(30, 15)),
        ],
    }
    for name, columns in tables.items():
        if name not in sa.inspect(op.get_bind()).get_table_names():
            op.create_table(name, sa.Column("id", GUID(), primary_key=True), *columns)


def downgrade() -> None:
    for name in [
        "llm_attempts",
        "llm_provider_states",
        "llm_demands",
        "llm_work_items",
        "llm_results",
        "llm_batches",
    ]:
        op.drop_table(name)
