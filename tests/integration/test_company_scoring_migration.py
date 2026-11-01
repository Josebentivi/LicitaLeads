"""Upgrading the existing schema must preserve reviews and the old score."""

import asyncio
import os
import sqlite3
import subprocess
import sys

from app.database import create_database_engine, create_session_factory
from app.models import Company, Lead, LeadReview, Procurement, ProcurementEvent


def test_scoring_upgrade_preserves_existing_reviews(tmp_path):
    path = tmp_path / "legacy-scores.sqlite3"
    url = f"sqlite:///{path.as_posix()}"
    environment = {**os.environ, "DATABASE_URL": url}

    def migrate(*arguments):
        result = subprocess.run(  # noqa: S603 - fixed isolated migration command
            [sys.executable, "-m", "alembic", *arguments],
            env=environment,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert result.returncode == 0, result.stderr

    migrate("upgrade", "head")
    migrate("downgrade", "0024_llm_queue")

    async def seed():
        engine = create_database_engine(url)
        try:
            async with create_session_factory(engine)() as db, db.begin():
                company = Company(cnpj="04252011000110", normalized_name="EMPRESA")
                procurement = Procurement(source="pncp", external_id="legacy")
                db.add_all([company, procurement])
                await db.flush()
                event = ProcurementEvent(
                    procurement_id=procurement.id,
                    company_id=company.id,
                    event_type="DISQUALIFIED",
                    source="document",
                    raw_description="Registro legado",
                    confidence=1,
                )
                db.add(event)
                await db.flush()
                lead = Lead(
                    procurement_id=procurement.id,
                    company_id=company.id,
                    triggering_event_id=event.id,
                    score=95,
                    urgency_score=30,
                    evidence_score=25,
                    legal_relevance_score=20,
                    contact_score=10,
                    economic_value_score=10,
                    fit_score=95,
                    scoring_version="1.0",
                    lead_status="approved",
                    assigned_to="Responsável",
                    reason_summary="Sentinela",
                )
                db.add(lead)
                await db.flush()
                db.add(
                    LeadReview(
                        lead_id=lead.id, decision="approved", reviewer="Revisor", notes="Preservar"
                    )
                )
        finally:
            await engine.dispose()

    asyncio.run(seed())
    migrate("upgrade", "head")
    with sqlite3.connect(path) as db:
        assert db.execute(
            "select score,scoring_version,lead_status,assigned_to from leads"
        ).fetchone() == (95, "1.0", "approved", "Responsável")
        assert db.execute("select reviewer,notes from lead_reviews").fetchone() == (
            "Revisor",
            "Preservar",
        )
        assert db.execute("select count(*) from lead_score_revisions").fetchone()[0] == 0
        assert db.execute("pragma foreign_key_check").fetchall() == []
