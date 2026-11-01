"""Drop redundant contract indexes created by column-level index flags.

Revision ID: 0015_contract_index_cleanup
Revises: 0014_payment_match_details
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015_contract_index_cleanup"
down_revision: str | None = "0014_payment_match_details"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REDUNDANT = ("ix_contracts_agency_cnpj", "ix_contracts_company_id")


def upgrade() -> None:
    """Remove duplicate indexes when an older schema created them."""

    indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("contracts")}
    for name in _REDUNDANT:
        if name in indexes:
            op.drop_index(name, table_name="contracts")


def downgrade() -> None:
    """Recreate the column-level indexes for older deployments."""

    indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes("contracts")}
    if "ix_contracts_agency_cnpj" not in indexes:
        op.create_index("ix_contracts_agency_cnpj", "contracts", ["agency_cnpj"])
    if "ix_contracts_company_id" not in indexes:
        op.create_index("ix_contracts_company_id", "contracts", ["company_id"])
