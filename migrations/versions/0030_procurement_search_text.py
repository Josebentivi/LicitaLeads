"""Preserve the published search text separately from the display description."""

import sqlalchemy as sa
from alembic import op

revision = "0030_procurement_search_text"
down_revision = "0029_opportunity_response"
branch_labels = None
depends_on = None

_COLUMNS = (("procurements", "object_complement"), ("procurement_items", "summary_description"))


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    for table, name in _COLUMNS:
        if name not in {column["name"] for column in inspector.get_columns(table)}:
            op.add_column(table, sa.Column(name, sa.Text(), nullable=True))


def downgrade() -> None:
    for table, name in reversed(_COLUMNS):
        with op.batch_alter_table(table) as batch:
            batch.drop_column(name)
