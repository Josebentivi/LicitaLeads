"""Add the normalized participant status code used by company filters.

Revision ID: 0005_participant_status
Revises: 0004_procurement_filters
Create Date: <PII type="DATE" id="31"/>

The raw source/detection text stays in ``participants.status``; the new column
only normalizes the outcome so filters never depend on free-form strings.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from app.models.base import NAMING_CONVENTION
from app.models.enums import ParticipantStatus, database_enum
from app.services.identifiers import participant_status_code

revision: str = "0005_participant_status"
down_revision: str | None = "0004_procurement_filters"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "participants"
_COLUMN = "status_code"
_ENUM_CONSTRAINT = "enum_participantstatus"
_RENDERED_CONSTRAINT = "ck_participants_enum_participantstatus"
_INDEXES = (
    ("ix_participants_status_code", [_COLUMN]),
    ("ix_participants_company_status", ["company_id", _COLUMN]),
)


def _inspector() -> sa.Inspector:
    return sa.inspect(op.get_bind())


def upgrade() -> None:
    """Add the column/indexes and backfill from the preserved raw text."""

    inspector = _inspector()
    columns = {column["name"] for column in inspector.get_columns(_TABLE)}
    if _COLUMN not in columns:
        with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
            batch_op.add_column(sa.Column(_COLUMN, database_enum(ParticipantStatus), nullable=True))

    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    for name, columns_ in _INDEXES:
        if name not in indexes:
            op.create_index(name, _TABLE, columns_)

    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT id, participation_role, status FROM participants WHERE status_code IS NULL")
    ).fetchall()
    for row in rows:
        code = participant_status_code(row.participation_role, row.status)
        bind.execute(
            sa.text("UPDATE participants SET status_code = :code WHERE id = :identifier"),
            {"code": code.value, "identifier": row.id},
        )


def downgrade() -> None:
    """Drop the normalized status and its indexes."""

    inspector = _inspector()
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE)}
    for name, _ in _INDEXES:
        if name in indexes:
            op.drop_index(name, table_name=_TABLE)

    columns = {column["name"] for column in _inspector().get_columns(_TABLE)}
    if _COLUMN in columns:
        checks = {item["name"] for item in _inspector().get_check_constraints(_TABLE)}
        if _RENDERED_CONSTRAINT in checks:
            with op.batch_alter_table(_TABLE, naming_convention=NAMING_CONVENTION) as batch_op:
                batch_op.drop_constraint(_ENUM_CONSTRAINT, type_="check")
                batch_op.drop_column(_COLUMN)
        else:
            with op.batch_alter_table(_TABLE) as batch_op:
                if _ENUM_CONSTRAINT in checks:
                    batch_op.drop_constraint(_ENUM_CONSTRAINT, type_="check")
                batch_op.drop_column(_COLUMN)
