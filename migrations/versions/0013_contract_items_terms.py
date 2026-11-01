"""Add contract items and additive terms.

Revision ID: 0013_contract_items_terms
Revises: 0012_contracts_payments
Create Date: 2026-10-21

Items and terms are separate provenance-bearing rows.  Contract value,
additive value and payment value must never be merged into one number.
"""

from collections.abc import Sequence

from alembic import op

from app.models import ContractItem, ContractTerm

revision: str = "0013_contract_items_terms"
down_revision: str | None = "0012_contracts_payments"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the contract item and term tables."""

    bind = op.get_bind()
    ContractItem.__table__.create(bind=bind, checkfirst=True)
    ContractTerm.__table__.create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """Drop the contract detail tables."""

    bind = op.get_bind()
    ContractTerm.__table__.drop(bind=bind, checkfirst=True)
    ContractItem.__table__.drop(bind=bind, checkfirst=True)
