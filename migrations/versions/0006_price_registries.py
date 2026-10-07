"""Create price registry (ARP/ata) tables.

Revision ID: 0006_price_registries
Revises: 0005_participant_status
Create Date: <PII type="DATE" id="31"/>

The first revision builds tables from the live SQLAlchemy metadata, so this
upgrade is intentionally idempotent: it tolerates databases freshly created
from the updated models (where the tables already exist) as well as databases
created before price registry ingestion was introduced.
"""

from collections.abc import Sequence

from alembic import op

from app.models import PriceRegistry, PriceRegistryItem

revision: str = "0006_price_registries"
down_revision: str | None = "0005_participant_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the price registry header and item tables."""

    bind = op.get_bind()
    PriceRegistry.__table__.create(bind=bind, checkfirst=True)
    PriceRegistryItem.__table__.create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """Drop the price registry tables in dependency order."""

    bind = op.get_bind()
    PriceRegistryItem.__table__.drop(bind=bind, checkfirst=True)
    PriceRegistry.__table__.drop(bind=bind, checkfirst=True)
