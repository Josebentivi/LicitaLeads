"""Key price registry items by supplier, not only by item number.

Revision ID: 0007_price_registry_item_supplier
Revises: 0006_price_registries
Create Date: <PII type="DATE" id="31"/>

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the new constraint already exists) as well as
databases created before suppliers were part of the item identity.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_price_registry_item_supplier"
down_revision: str | None = "0006_price_registries"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "price_registry_items"
_OLD_CONSTRAINT = "uq_price_registry_items_registry_number"
_NEW_CONSTRAINT = "uq_price_registry_items_registry_item_supplier"
_NEW_COLUMNS = ["price_registry_id", "item_number", "supplier_cnpj"]


def _unique_constraints() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {item["name"] for item in inspector.get_unique_constraints(_TABLE)}


def upgrade() -> None:
    """Replace the item-only unique key with (registry, item, supplier)."""

    existing = _unique_constraints()
    if _NEW_CONSTRAINT in existing:
        return
    with op.batch_alter_table(_TABLE) as batch_op:
        if _OLD_CONSTRAINT in existing:
            batch_op.drop_constraint(_OLD_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(_NEW_CONSTRAINT, _NEW_COLUMNS)


def downgrade() -> None:
    """Restore the item-only unique key."""

    existing = _unique_constraints()
    if _OLD_CONSTRAINT in existing:
        return
    with op.batch_alter_table(_TABLE) as batch_op:
        if _NEW_CONSTRAINT in existing:
            batch_op.drop_constraint(_NEW_CONSTRAINT, type_="unique")
        batch_op.create_unique_constraint(_OLD_CONSTRAINT, ["price_registry_id", "item_number"])
