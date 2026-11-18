"""Index published municipal budget classifications from existing local evidence."""

from alembic import op

from app.models.entities import MunicipalBudgetEntry
from app.services.municipal_budget import backfill_budget_projection

revision = "0028_municipal_spending"
down_revision = "0027_ai_central"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    MunicipalBudgetEntry.__table__.create(connection, checkfirst=True)
    backfill_budget_projection(connection)


def downgrade() -> None:
    MunicipalBudgetEntry.__table__.drop(op.get_bind())
