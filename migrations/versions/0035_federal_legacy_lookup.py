"""Index legacy federal identity lookups during archive ingestion."""

import sqlalchemy as sa
from alembic import op

revision = "0035_federal_legacy_lookup"
down_revision = "0034_tce_ma_verified_links"
branch_labels = None
depends_on = None


def upgrade() -> None:
    unclassified = sa.text("federal_beneficiary_role IS NULL")
    op.create_index(
        "ix_payments_federal_legacy_document",
        "payments",
        ["source", "supplier_cnpj", sa.text("upper(trim(document_number))")],
        sqlite_where=unclassified,
        postgresql_where=unclassified,
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index("ix_payments_federal_legacy_document", table_name="payments")
