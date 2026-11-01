"""Add field history and durable continuation without changing legacy data."""

import sqlalchemy as sa
from alembic import op

import app.models.base

revision = "0022_incremental_audit"
down_revision = "0021_integrity_guards"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    columns = {column["name"] for column in inspector.get_columns("crawl_runs")}
    indexes = {index["name"] for index in inspector.get_indexes("crawl_runs")}
    if "collection_batches" not in tables:
        op.create_table(
            "collection_batches",
            sa.Column("stream_key", sa.String(length=64), nullable=False),
            sa.Column("filters", sa.JSON(), nullable=False),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("cursors", sa.JSON(), nullable=False),
            sa.Column("last_run_id", app.models.base.GUID(), nullable=True),
            sa.Column("id", app.models.base.GUID(), nullable=False),
            sa.Column("created_at", app.models.base.UTCDateTime(), nullable=False),
            sa.Column("updated_at", app.models.base.UTCDateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_collection_batches")),
        )
    if "collection_batches" not in tables:
        with op.batch_alter_table("collection_batches", schema=None) as batch_op:
            batch_op.create_index(
                "ix_collection_batches_stream", ["stream_key", "status"], unique=False
            )
    if "collection_work_items" not in tables:
        op.create_table(
            "collection_work_items",
            sa.Column("batch_id", app.models.base.GUID(), nullable=False),
            sa.Column("source", sa.String(length=100), nullable=False),
            sa.Column("kind", sa.String(length=60), nullable=False),
            sa.Column("identity", sa.String(length=255), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("source_record_id", app.models.base.GUID(), nullable=True),
            sa.Column("status", sa.String(length=30), nullable=False),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("position", sa.Integer(), nullable=False),
            sa.Column("observed_at", app.models.base.UTCDateTime(), nullable=False),
            sa.Column("id", app.models.base.GUID(), nullable=False),
            sa.ForeignKeyConstraint(
                ["batch_id"],
                ["collection_batches.id"],
                name=op.f("fk_collection_work_items_batch_id_collection_batches"),
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["source_record_id"],
                ["source_records.id"],
                name=op.f("fk_collection_work_items_source_record_id_source_records"),
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_collection_work_items")),
            sa.UniqueConstraint(
                "batch_id",
                "source",
                "kind",
                "identity",
                name=op.f("uq_collection_work_items_batch_id"),
            ),
        )
    if "collection_work_items" not in tables:
        with op.batch_alter_table("collection_work_items", schema=None) as batch_op:
            batch_op.create_index(
                "ix_collection_work_pending", ["batch_id", "source", "status"], unique=False
            )
    if "field_states" not in tables:
        op.create_table(
            "field_states",
            sa.Column("entity_type", sa.String(length=60), nullable=False),
            sa.Column("entity_id", app.models.base.GUID(), nullable=False),
            sa.Column("field_name", sa.String(length=100), nullable=False),
            sa.Column("value", sa.JSON(), nullable=True),
            sa.Column("source", sa.String(length=100), nullable=False),
            sa.Column("source_record_id", app.models.base.GUID(), nullable=True),
            sa.Column("source_updated_at", app.models.base.UTCDateTime(), nullable=True),
            sa.Column("timestamp_origin", sa.String(length=100), nullable=True),
            sa.Column("observed_at", app.models.base.UTCDateTime(), nullable=False),
            sa.Column("uncertain", sa.Boolean(), nullable=False),
            sa.Column("id", app.models.base.GUID(), nullable=False),
            sa.ForeignKeyConstraint(
                ["source_record_id"],
                ["source_records.id"],
                name=op.f("fk_field_states_source_record_id_source_records"),
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_field_states")),
            sa.UniqueConstraint(
                "entity_type", "entity_id", "field_name", name=op.f("uq_field_states_entity_type")
            ),
        )
    if "field_changes" not in tables:
        op.create_table(
            "field_changes",
            sa.Column("entity_type", sa.String(length=60), nullable=False),
            sa.Column("entity_id", app.models.base.GUID(), nullable=False),
            sa.Column("parent_type", sa.String(length=60), nullable=False),
            sa.Column("parent_id", app.models.base.GUID(), nullable=False),
            sa.Column("field_name", sa.String(length=100), nullable=False),
            sa.Column("previous_value", sa.JSON(), nullable=True),
            sa.Column("received_value", sa.JSON(), nullable=True),
            sa.Column("decision", sa.String(length=40), nullable=False),
            sa.Column("applied", sa.Boolean(), nullable=False),
            sa.Column("source", sa.String(length=100), nullable=False),
            sa.Column("previous_source", sa.String(length=100), nullable=True),
            sa.Column("previous_source_record_id", app.models.base.GUID(), nullable=True),
            sa.Column("source_record_id", app.models.base.GUID(), nullable=True),
            sa.Column("evidence_id", app.models.base.GUID(), nullable=True),
            sa.Column("crawl_run_id", app.models.base.GUID(), nullable=True),
            sa.Column("previous_source_updated_at", app.models.base.UTCDateTime(), nullable=True),
            sa.Column("source_updated_at", app.models.base.UTCDateTime(), nullable=True),
            sa.Column("timestamp_origin", sa.String(length=100), nullable=True),
            sa.Column("observed_at", app.models.base.UTCDateTime(), nullable=False),
            sa.Column("recorded_at", app.models.base.UTCDateTime(), nullable=False),
            sa.Column("id", app.models.base.GUID(), nullable=False),
            sa.ForeignKeyConstraint(
                ["crawl_run_id"],
                ["crawl_runs.id"],
                name=op.f("fk_field_changes_crawl_run_id_crawl_runs"),
                ondelete="SET NULL",
            ),
            sa.ForeignKeyConstraint(
                ["evidence_id"],
                ["evidence.id"],
                name=op.f("fk_field_changes_evidence_id_evidence"),
                ondelete="SET NULL",
            ),
            sa.ForeignKeyConstraint(
                ["previous_source_record_id"],
                ["source_records.id"],
                name=op.f("fk_field_changes_previous_source_record_id_source_records"),
                ondelete="SET NULL",
            ),
            sa.ForeignKeyConstraint(
                ["source_record_id"],
                ["source_records.id"],
                name=op.f("fk_field_changes_source_record_id_source_records"),
                ondelete="SET NULL",
            ),
            sa.PrimaryKeyConstraint("id", name=op.f("pk_field_changes")),
        )
    if "field_changes" not in tables:
        with op.batch_alter_table("field_changes", schema=None) as batch_op:
            batch_op.create_index(
                "ix_field_changes_entity", ["entity_type", "entity_id"], unique=False
            )
            batch_op.create_index(
                "ix_field_changes_parent_time",
                ["parent_type", "parent_id", "observed_at"],
                unique=False,
            )
    with op.batch_alter_table("crawl_runs") as batch_op:
        if "records_changed" not in columns:
            batch_op.add_column(sa.Column("records_changed", sa.Integer(), nullable=True))
        if "records_unchanged" not in columns:
            batch_op.add_column(sa.Column("records_unchanged", sa.Integer(), nullable=True))
        if "records_stale_ignored" not in columns:
            batch_op.add_column(sa.Column("records_stale_ignored", sa.Integer(), nullable=True))
        if "records_conflicted" not in columns:
            batch_op.add_column(sa.Column("records_conflicted", sa.Integer(), nullable=True))
        if "counting_version" not in columns:
            batch_op.add_column(sa.Column("counting_version", sa.Integer(), nullable=True))
        if "batch_id" not in columns:
            batch_op.add_column(sa.Column("batch_id", app.models.base.GUID(), nullable=True))
        if "ix_crawl_runs_batch_id" not in indexes:
            batch_op.create_index(batch_op.f("ix_crawl_runs_batch_id"), ["batch_id"], unique=False)
        if "batch_id" not in columns:
            batch_op.create_foreign_key(
                batch_op.f("fk_crawl_runs_batch_id_collection_batches"),
                "collection_batches",
                ["batch_id"],
                ["id"],
                ondelete="SET NULL",
            )
        if "uq_crawl_runs_active_batch" not in indexes:
            batch_op.create_index(
                "uq_crawl_runs_active_batch",
                ["batch_id"],
                unique=True,
                sqlite_where=sa.text("batch_id IS NOT NULL AND status IN ('pending', 'running')"),
                postgresql_where=sa.text(
                    "batch_id IS NOT NULL AND status IN ('pending', 'running')"
                ),
            )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("crawl_runs", schema=None) as batch_op:
        batch_op.drop_index("uq_crawl_runs_active_batch")
        batch_op.drop_constraint(
            batch_op.f("fk_crawl_runs_batch_id_collection_batches"), type_="foreignkey"
        )
        batch_op.drop_index(batch_op.f("ix_crawl_runs_batch_id"))
        batch_op.drop_column("batch_id")
        batch_op.drop_column("counting_version")
        batch_op.drop_column("records_conflicted")
        batch_op.drop_column("records_stale_ignored")
        batch_op.drop_column("records_unchanged")
        batch_op.drop_column("records_changed")

    with op.batch_alter_table("field_changes", schema=None) as batch_op:
        batch_op.drop_index("ix_field_changes_parent_time")
        batch_op.drop_index("ix_field_changes_entity")

    op.drop_table("field_changes")
    op.drop_table("field_states")
    with op.batch_alter_table("collection_work_items", schema=None) as batch_op:
        batch_op.drop_index("ix_collection_work_pending")

    op.drop_table("collection_work_items")
    with op.batch_alter_table("collection_batches", schema=None) as batch_op:
        batch_op.drop_index("ix_collection_batches_stream")

    op.drop_table("collection_batches")
    # ### end Alembic commands ###
