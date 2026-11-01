"""Unit tests for the operational phase derived from dated source facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.models import Procurement, ProcurementEvent, ProcurementEventType, ProcurementPhase
from app.services.procurement_phase import (
    derive_procurement_phase,
    procurement_is_in_session,
)

NOW = datetime(2026, 10, 21, 15, tzinfo=UTC)


def _procurement(**kwargs) -> Procurement:
    values = {
        "source": "pncp",
        "external_id": "process-1",
        "fingerprint": "a" * 64,
        "status": "Em andamento",
    }
    values.update(kwargs)
    return Procurement(**values)


def _event(event_type: ProcurementEventType, *, occurred_at=None) -> ProcurementEvent:
    return ProcurementEvent(
        procurement_id=None,
        event_type=event_type,
        raw_description="event",
        source="document",
        confidence=1,
        occurred_at=occurred_at,
        fingerprint="b" * 64,
    )


def test_future_proposal_window_is_proposals_not_session() -> None:
    procurement = _procurement(proposal_end_at=NOW + timedelta(days=1))

    assert derive_procurement_phase(procurement, now=NOW) is ProcurementPhase.PROPOSALS
    assert procurement_is_in_session(procurement, now=NOW) is False


def test_open_status_after_proposal_window_is_a_dispute() -> None:
    procurement = _procurement(proposal_end_at=NOW - timedelta(days=1))

    assert derive_procurement_phase(procurement, now=NOW) is ProcurementPhase.DISPUTE
    assert procurement_is_in_session(procurement, now=NOW) is True


def test_session_event_wins_over_proposal_window() -> None:
    procurement = _procurement(proposal_end_at=NOW - timedelta(days=1))
    event = _event(ProcurementEventType.DILIGENCE_REQUESTED, occurred_at=NOW)

    assert (
        derive_procurement_phase(procurement, last_event=event, now=NOW)
        is ProcurementPhase.DILIGENCE
    )
    assert procurement_is_in_session(procurement, last_event=event, now=NOW) is True


def test_closed_status_wins_over_old_session_event() -> None:
    procurement = _procurement(status="Homologada", proposal_end_at=NOW - timedelta(days=3))
    event = _event(ProcurementEventType.DILIGENCE_REQUESTED, occurred_at=NOW)

    assert (
        derive_procurement_phase(procurement, last_event=event, now=NOW)
        is ProcurementPhase.HOMOLOGATION
    )
    assert procurement_is_in_session(procurement, last_event=event, now=NOW) is False


def test_cancelled_status_is_terminal() -> None:
    procurement = _procurement(status="Cancelada", proposal_end_at=NOW + timedelta(days=1))

    assert derive_procurement_phase(procurement, now=NOW) is ProcurementPhase.CANCELLED
    assert procurement_is_in_session(procurement, now=NOW) is False


def test_unknown_without_status_or_dates() -> None:
    procurement = _procurement(status=None)

    assert derive_procurement_phase(procurement, now=NOW) is ProcurementPhase.UNKNOWN
    assert procurement_is_in_session(procurement, now=NOW) is False
