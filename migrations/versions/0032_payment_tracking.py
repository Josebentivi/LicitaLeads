"""Store the operator's federal payment tracking selections."""

import sqlalchemy as sa
from alembic import op

revision = "0032_payment_tracking"
down_revision = "0031_normalized_search_text"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fresh databases may already have the table from the base metadata.
    op.create_table(
        "payment_tracking_targets",
        sa.Column("cnpj", sa.String(length=14), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("cnpj", name=op.f("pk_payment_tracking_targets")),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table("payment_tracking_targets")
