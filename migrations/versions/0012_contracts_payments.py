"""Create contract and payment tables.

Revision ID: 0012_contracts_payments
Revises: 0011_price_registry_details
Create Date: 2026-10-21

Contracts and payments are separate provenance-bearing entities.  A payment is
only linked to a contract when the published identifiers allow an exact match.
"""

from collections.abc import Sequence

from alembic import op

from app.models import Contract, Payment

revision: str = "0012_contracts_payments"
down_revision: str | None = "0011_price_registry_details"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the contract and payment tables."""

    bind = op.get_bind()
    Contract.__table__.create(bind=bind, checkfirst=True)
    Payment.__table__.create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """Drop the payment table before the contract table."""

    bind = op.get_bind()
    Payment.__table__.drop(bind=bind, checkfirst=True)
    Contract.__table__.drop(bind=bind, checkfirst=True)
