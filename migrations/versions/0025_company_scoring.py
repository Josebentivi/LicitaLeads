"""Company dossier scoring snapshots and expanded component limits."""

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID, UTCDateTime

revision = "0025_company_scoring"
down_revision = "0024_llm_queue"
branch_labels = None
depends_on = None


def _limits(evidence: int, legal: int) -> None:
    with op.batch_alter_table("leads") as batch:
        batch.drop_constraint(op.f("ck_leads_evidence_score_range"), type_="check")
        batch.drop_constraint(op.f("ck_leads_legal_score_range"), type_="check")
        batch.create_check_constraint(
            "evidence_score_range", f"evidence_score >= 0 AND evidence_score <= {evidence}"
        )
        batch.create_check_constraint(
            "legal_score_range", f"legal_relevance_score >= 0 AND legal_relevance_score <= {legal}"
        )


def upgrade() -> None:
    bind = op.get_bind()
    if "lead_score_requests" not in sa.inspect(bind).get_table_names():
        op.create_table(
            "lead_score_requests",
            sa.Column("id", GUID(), primary_key=True),
            sa.Column(
                "company_id",
                GUID(),
                sa.ForeignKey("companies.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("lead_ids", sa.JSON(), nullable=False),
            sa.Column("results", sa.JSON(), nullable=False),
            sa.Column("created_at", UTCDateTime(), nullable=False),
        )
    # Initial schema uses current metadata; existing installations need the additive table.
    if "lead_score_revisions" not in sa.inspect(bind).get_table_names():
        op.create_table(
            "lead_score_revisions",
            sa.Column("id", GUID(), primary_key=True),
            sa.Column(
                "lead_id", GUID(), sa.ForeignKey("leads.id", ondelete="CASCADE"), nullable=False
            ),
            sa.Column("request_id", GUID()),
            sa.Column("scoring_version", sa.String(50), nullable=False),
            sa.Column("previous_score", sa.Integer()),
            sa.Column("previous_version", sa.String(50)),
            sa.Column("score", sa.Integer(), nullable=False),
            sa.Column("components", sa.JSON(), nullable=False),
            sa.Column("inputs", sa.JSON(), nullable=False),
            sa.Column("references", sa.JSON(), nullable=False),
            sa.Column("origin", sa.String(60), nullable=False),
            sa.Column("calculated_at", UTCDateTime(), nullable=False),
            sa.UniqueConstraint("lead_id", "request_id", name="uq_lead_score_revision_request"),
        )
        op.create_index(
            "ix_lead_score_revisions_lead_time",
            "lead_score_revisions",
            ["lead_id", "calculated_at"],
        )
    checks = sa.inspect(bind).get_check_constraints("leads")
    if not any("evidence_score <= 30" in c["sqltext"] for c in checks):
        _limits(30, 25)


def downgrade() -> None:
    bind = op.get_bind()
    # Refuse to silently discard scores that the old schema cannot represent.
    if bind.execute(
        sa.text(
            "SELECT count(*) FROM leads WHERE evidence_score > 25 OR legal_relevance_score > 20"
        )
    ).scalar():
        raise RuntimeError(
            "Pontuações 2.0 excedem os limites antigos; restaure um backup compatível."
        )
    op.drop_table("lead_score_revisions")
    op.drop_table("lead_score_requests")
    _limits(25, 20)
