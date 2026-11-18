"""AI review, citation provenance and exact payment contract identity."""

import sqlalchemy as sa
from alembic import op

from app.models.llm_review import LLMEventCitation, LLMSuggestion

revision = "0027_ai_central"
down_revision = "0026_participation_integrity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("llm_work_items")}
    for name in ("context", "coverage"):
        if name not in columns:
            op.add_column("llm_work_items", sa.Column(name, sa.JSON(), nullable=True))
    if "contract_control_number" not in {c["name"] for c in inspector.get_columns("payments")}:
        op.add_column(
            "payments", sa.Column("contract_control_number", sa.String(100), nullable=True)
        )
    if "ix_payments_contract_control_number" not in {
        i["name"] for i in inspector.get_indexes("payments")
    }:
        op.create_index(
            "ix_payments_contract_control_number", "payments", ["contract_control_number"]
        )
    for model in (LLMSuggestion, LLMEventCitation):
        model.__table__.create(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    for model in (LLMEventCitation, LLMSuggestion):
        model.__table__.drop(op.get_bind())
    op.drop_index("ix_payments_contract_control_number", table_name="payments")
    with op.batch_alter_table("payments") as batch:
        batch.drop_column("contract_control_number")
    with op.batch_alter_table("llm_work_items") as batch:
        batch.drop_column("coverage")
        batch.drop_column("context")
