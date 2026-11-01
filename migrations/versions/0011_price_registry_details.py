"""Add ARP unit, commitment and adhesion detail tables.

Revision ID: 0011_price_registry_details
Revises: 0010_procurement_phase
Create Date: 2026-10-21

These are separate auditable rows because the official API publishes them per
item/unit and they can change independently of the ARP header and item.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models import PriceRegistryAdhesion, PriceRegistryCommitment, PriceRegistryUnit
from app.models.base import NAMING_CONVENTION

revision: str = "0011_price_registry_details"
down_revision: str | None = "0010_procurement_phase"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ITEM_COLUMNS = {
    "catalog_code": sa.Column("catalog_code", sa.String(100), nullable=True),
    "item_type": sa.Column("item_type", sa.String(100), nullable=True),
}
_ITEM_INDEX = "ix_price_registry_items_catalog_code"


def upgrade() -> None:
    """Create the detail tables and the optional item catalog fields."""

    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("price_registry_items")}
    with op.batch_alter_table("price_registry_items", naming_convention=NAMING_CONVENTION) as batch:
        for name, column in _ITEM_COLUMNS.items():
            if name not in columns:
                batch.add_column(column)

    indexes = {
        index["name"] for index in sa.inspect(op.get_bind()).get_indexes("price_registry_items")
    }
    if _ITEM_INDEX not in indexes:
        op.create_index(_ITEM_INDEX, "price_registry_items", ["catalog_code"])

    bind = op.get_bind()
    PriceRegistryUnit.__table__.create(bind=bind, checkfirst=True)
    PriceRegistryCommitment.__table__.create(bind=bind, checkfirst=True)
    PriceRegistryAdhesion.__table__.create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """Drop the detail tables and the optional item catalog fields."""

    bind = op.get_bind()
    PriceRegistryAdhesion.__table__.drop(bind=bind, checkfirst=True)
    PriceRegistryCommitment.__table__.drop(bind=bind, checkfirst=True)
    PriceRegistryUnit.__table__.drop(bind=bind, checkfirst=True)

    indexes = {
        index["name"] for index in sa.inspect(op.get_bind()).get_indexes("price_registry_items")
    }
    if _ITEM_INDEX in indexes:
        op.drop_index(_ITEM_INDEX, table_name="price_registry_items")

    columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("price_registry_items")
    }
    with op.batch_alter_table("price_registry_items", naming_convention=NAMING_CONVENTION) as batch:
        for name in _ITEM_COLUMNS:
            if name in columns:
                batch.drop_column(name)
