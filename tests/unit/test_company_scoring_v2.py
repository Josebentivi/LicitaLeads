from decimal import Decimal

import pytest

from app.services.lead_scoring.v2 import LeadScoreInput, LeadScorer


@pytest.mark.parametrize(
    "hours,expected",
    [
        (0, 30),
        (24, 30),
        (24.001, 24),
        (72, 24),
        (72.001, 18),
        (168, 18),
        (168.001, 12),
        (-1, 0),
        (None, 0),
    ],
)
def test_urgency_boundaries(hours, expected):
    result = LeadScorer().score(LeadScoreInput(event_type="INELIGIBLE", hours_remaining=hours))
    assert result.urgency_score == expected


@pytest.mark.parametrize(
    "event,points",
    [
        ("INELIGIBLE", 25),
        ("PROPOSAL_REJECTED", 25),
        ("DISQUALIFIED", 25),
        ("APPEAL_SUBMITTED", 25),
        ("DILIGENCE_REQUESTED", 18),
        ("COUNTERARGUMENT_SUBMITTED", 18),
        ("SESSION_REOPENED", 8),
        ("PARTICIPATION_DETECTED", 4),
        ("HOMOLOGATED", 2),
        ("UNKNOWN", 0),
    ],
)
def test_adverse_event_focus(event, points):
    assert LeadScorer().score(LeadScoreInput(event_type=event)).legal_relevance_score == points


@pytest.mark.parametrize(
    "value,points",
    [
        (None, 0),
        ("0", 0),
        ("-1", 0),
        ("0.01", 3),
        ("49999.99", 3),
        ("50000", 6),
        ("199999.99", 6),
        ("200000", 8),
        ("1000000", 10),
    ],
)
def test_supplier_economic_boundaries(value, points):
    result = LeadScorer().score(
        LeadScoreInput(
            event_type="INELIGIBLE", estimated_value=Decimal(value) if value is not None else None
        )
    )
    assert result.economic_value_score == points


def test_weighted_score_and_decimal_rounding():
    result = LeadScorer().score(
        LeadScoreInput(
            event_type="INELIGIBLE",
            hours_remaining=12,
            evidence_confidence=1,
            has_direct_evidence=True,
            verified_contact_types=("email",),
            estimated_value=Decimal("1000000"),
        )
    )
    assert result.score == 100
    assert result.scoring_version == "2.0"
    assert result.evidence_score == 30
    assert (
        LeadScorer()
        .score(LeadScoreInput(event_type="UNKNOWN", evidence_confidence=0.5))
        .evidence_score
        == 13
    )


def test_estimated_deadline_contacts_and_penalties():
    result = LeadScorer().score(
        LeadScoreInput(
            event_type="INELIGIBLE",
            hours_remaining=1,
            deadline_estimated=True,
            pending_contact_types=("email",),
            evidence_confidence=1,
        )
    )
    assert result.urgency_score == 12
    assert result.contact_score == 1
    assert result.penalties == 5
    assert (
        LeadScorer()
        .score(LeadScoreInput(event_type="UNKNOWN", contact_types=("email",)))
        .contact_score
        == 0
    )
