"""Keep distinct identities for federal issued documents and final payments."""

import sqlalchemy as sa
from alembic import op

revision = "0033_federal_payment_identity"
down_revision = "0032_payment_tracking"
branch_labels = None
depends_on = None

_COLUMNS = (
    ("federal_identity", sa.String(length=160)),
    ("federal_beneficiary_role", sa.String(length=10)),
    ("federal_intermediary", sa.Boolean()),
    ("federal_creditor_list", sa.String(length=100)),
    ("federal_identity_conflict", sa.Boolean()),
)
_INDEX = "ux_payments_federal_identity"


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    existing = {column["name"] for column in inspector.get_columns("payments")}
    for name, column_type in _COLUMNS:
        if name not in existing:
            op.add_column("payments", sa.Column(name, column_type, nullable=True))
    if _INDEX not in {index["name"] for index in inspector.get_indexes("payments")}:
        op.create_index(_INDEX, "payments", ["federal_identity"], unique=True)


def downgrade() -> None:
    op.drop_index(_INDEX, table_name="payments")
    with op.batch_alter_table("payments") as batch:
        for name, _ in reversed(_COLUMNS):
            batch.drop_column(name)
