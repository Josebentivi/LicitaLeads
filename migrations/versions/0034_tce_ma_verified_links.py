"""Persist links confirmed by documentary review."""

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID

revision = "0034_tce_ma_verified_links"
down_revision = "0033_federal_payment_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tce_ma_verified_links",
        sa.Column("procurement_id", GUID(), nullable=False),
        sa.Column("confirmation_id", GUID(), nullable=False),
        sa.Column("tce_cnpj", sa.String(14), nullable=False),
        sa.Column("tce_procedure_id", sa.String(100), nullable=False),
        sa.Column("pncp_control", sa.String(100), nullable=False),
        sa.Column("pncp_number", sa.String(60), nullable=False),
        sa.Column("tce_number", sa.String(60), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("evidence_id", GUID(), nullable=False),
        sa.Column("pncp_agency_evidence_id", GUID(), nullable=True),
        sa.Column("tce_agency_evidence_id", GUID(), nullable=True),
        sa.PrimaryKeyConstraint("procurement_id"),
        sa.UniqueConstraint("tce_cnpj", "tce_procedure_id"),
        sa.UniqueConstraint("pncp_control"),
        sa.ForeignKeyConstraint(["procurement_id"], ["procurements.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["evidence_id"], ["evidence.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pncp_agency_evidence_id"], ["evidence.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tce_agency_evidence_id"], ["evidence.id"], ondelete="CASCADE"),
        # The initial revision can create this table from current metadata on a fresh database.
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table("tce_ma_verified_links")
