"""Create the initial auditable procurement schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-09-16

The first revision delegates table ordering to SQLAlchemy's dependency sorter.
All names, constraints, indexes, and portable custom types are declared in the
revisioned model metadata; subsequent schema changes must receive a new Alembic
revision rather than modifying this file's revision identifier.
"""

from collections.abc import Sequence

from alembic import op

from app.models import Base

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create tables, constraints, and indexes in foreign-key-safe order."""

    bind = op.get_bind()
    Base.metadata.create_all(bind=bind, checkfirst=True)


def downgrade() -> None:
    """Remove the complete initial schema in reverse dependency order."""

    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind, checkfirst=True)
