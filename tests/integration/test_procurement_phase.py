"""Phase refresh must prefer the latest documented event."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models import (
    FieldObservation,
    FieldValueStatus,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementPhase,
)
from app.services.procurement_phase import refresh_procurement_phase

from .conftest import DatabaseContext


@pytest.mark.asyncio
async def test_refresh_uses_latest_documented_event(database: DatabaseContext) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="phase-process",
            status="Em andamento",
            proposal_end_at=now - timedelta(days=1),
            current_phase=ProcurementPhase.DISPUTE,
            fingerprint="p" * 64,
        )
        session.add(procurement)
        await session.flush()
        session.add(
            ProcurementEvent(
                procurement_id=procurement.id,
                event_type=ProcurementEventType.DILIGENCE_REQUESTED,
                occurred_at=now,
                raw_description="Empresa convocada para diligencia",
                source="document",
                confidence=Decimal("0.90"),
                fingerprint="e" * 64,
            )
        )
        await session.flush()
        await refresh_procurement_phase(session, procurement)
        observation = await session.scalar(
            select(FieldObservation)
            .where(FieldObservation.field_name == "current_phase")
            .order_by(FieldObservation.collected_at.desc())
        )
        assert procurement.current_phase is ProcurementPhase.DILIGENCE
        assert observation is not None
        assert observation.source == "document"
        assert observation.value_status is FieldValueStatus.OBSERVED


@pytest.mark.asyncio
async def test_phase_observation_does_not_claim_an_unrelated_event(
    database: DatabaseContext,
) -> None:
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        procurement = Procurement(
            source="pncp",
            external_id="phase-proposals",
            status="Em andamento",
            proposal_end_at=now + timedelta(days=3),
            current_phase=ProcurementPhase.UNKNOWN,
            fingerprint="q" * 64,
        )
        session.add(procurement)
        await session.flush()
        session.add(
            ProcurementEvent(
                procurement_id=procurement.id,
                event_type=ProcurementEventType.PARTICIPATION_DETECTED,
                occurred_at=now,
                raw_description="Participacao documentada",
                source="document",
                confidence=Decimal("0.90"),
                fingerprint="r" * 64,
            )
        )
        await session.flush()
        await refresh_procurement_phase(session, procurement)
        observation = await session.scalar(
            select(FieldObservation)
            .where(FieldObservation.field_name == "current_phase")
            .order_by(FieldObservation.collected_at.desc())
        )

    assert procurement.current_phase is ProcurementPhase.PROPOSALS
    assert observation is not None
    assert observation.source == "pncp"
    assert observation.value_status is FieldValueStatus.REQUIRES_MANUAL_REVIEW
    assert observation.source_record_id is None
    assert observation.evidence_id is None
