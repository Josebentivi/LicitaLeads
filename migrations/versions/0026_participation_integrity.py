"""Participation monetary evidence, verification, document identity and repairs."""

from alembic import op

from app.models.participation import (
    DocumentAlias,
    DocumentIdentity,
    FactVerification,
    ParticipantValue,
    ReconciliationRun,
)

revision = "0026_participation_integrity"
down_revision = "0025_company_scoring"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for model in (
        FactVerification,
        ParticipantValue,
        DocumentIdentity,
        DocumentAlias,
        ReconciliationRun,
    ):
        model.__table__.create(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for model in (
        ReconciliationRun,
        DocumentAlias,
        DocumentIdentity,
        ParticipantValue,
        FactVerification,
    ):
        model.__table__.drop(op.get_bind(), checkfirst=True)
