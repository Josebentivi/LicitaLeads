"""Keep commitment identity, current facts and independent refresh-attempt cursors."""

import sqlalchemy as sa
from alembic import op

revision = "0021_integrity_guards"
down_revision = "0020_source_completeness"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("payments")}
    indexes = {index["name"] for index in inspector.get_indexes("payments")}
    with op.batch_alter_table("payments") as batch:
        if "commitment_external_id" not in columns:
            batch.add_column(sa.Column("commitment_external_id", sa.String(255), nullable=True))
        if "is_current" not in columns:
            batch.add_column(
                sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.true())
            )
        if "ix_payments_commitment_external_id" not in indexes:
            batch.create_index("ix_payments_commitment_external_id", ["commitment_external_id"])
        if "ix_payments_is_current" not in indexes:
            batch.create_index("ix_payments_is_current", ["is_current"])
    inspector = sa.inspect(op.get_bind())
    source_columns = {column["name"] for column in inspector.get_columns("procurement_sources")}
    source_indexes = {index["name"] for index in inspector.get_indexes("procurement_sources")}
    with op.batch_alter_table("procurement_sources") as batch:
        if "last_refresh_attempt_at" not in source_columns:
            batch.add_column(
                sa.Column("last_refresh_attempt_at", sa.DateTime(timezone=True), nullable=True)
            )
        if "ix_procurement_sources_last_refresh_attempt_at" not in source_indexes:
            batch.create_index(
                "ix_procurement_sources_last_refresh_attempt_at", ["last_refresh_attempt_at"]
            )


def downgrade() -> None:
    with op.batch_alter_table("procurement_sources") as batch:
        batch.drop_index("ix_procurement_sources_last_refresh_attempt_at")
        batch.drop_column("last_refresh_attempt_at")
    with op.batch_alter_table("payments") as batch:
        batch.drop_index("ix_payments_is_current")
        batch.drop_index("ix_payments_commitment_external_id")
        batch.drop_column("is_current")
        batch.drop_column("commitment_external_id")
