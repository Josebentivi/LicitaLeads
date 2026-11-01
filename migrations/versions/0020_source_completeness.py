"""Keep collection completeness separate from the unmodified source payload."""

import sqlalchemy as sa
from alembic import op

revision = "0020_source_completeness"
down_revision = "0019_participant_cancelled"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("source_records")}
    with op.batch_alter_table("source_records") as batch:
        if "collection_complete" not in columns:
            batch.add_column(sa.Column("collection_complete", sa.Boolean(), nullable=True))
        if "pagination_metadata" not in columns:
            batch.add_column(sa.Column("pagination_metadata", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("source_records") as batch:
        batch.drop_column("pagination_metadata")
        batch.drop_column("collection_complete")
