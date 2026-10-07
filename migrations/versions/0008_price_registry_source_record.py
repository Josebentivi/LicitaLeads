"""Link price registries to the source record that produced them.

Revision ID: 0008_price_registry_source_record
Revises: 0007_price_registry_item_supplier
Create Date: <PII type="DATE" id="31"/>

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the column already exists) as well as databases
created before the audit link was introduced.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID, NAMING_CONVENTION

revision: str = "0008_price_registry_source_record"
down_revision: str | None = "0007_price_registry_item_supplier"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "price_registries"
_COLUMN = "source_record_id"
_INDEX = "ix_price_registries_source_record_id"
_FK = "fk_price_registries_source_record_id_source_records"


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def upgrade() -> None:
    """Add the nullable source-record link, its index and foreign key."""

    columns = {column["name"] for column in _inspector().get_columns(_TABLE)}
    if _COLUMN not in columns:
        with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.add_column(sa.Column(_COLUMN, GUID(), nullable=True))
            batch_op.create_foreign_key(
                _FK,
                "source_records",
                [_COLUMN],
                ["id"],
                ondelete="SET NULL",
            )

    indexes = {index["name"] for index in _inspector().get_indexes(_TABLE)}
    if _INDEX not in indexes:
        op.create_index(_INDEX, _TABLE, [_COLUMN])


def downgrade() -> None:
    """Drop the audit link and its index."""

    indexes = {index["name"] for index in _inspector().get_indexes(_TABLE)}
    if _INDEX in indexes:
        op.drop_index(_INDEX, table_name=_TABLE)

    columns = {column["name"] for column in _inspector().get_columns(_TABLE)}
    if _COLUMN in columns:
        foreign_keys = {item["name"] for item in _inspector().get_foreign_keys(_TABLE)}
        with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            if _FK in foreign_keys:
                batch_op.drop_constraint(_FK, type_="foreignkey")
            batch_op.drop_column(_COLUMN)
