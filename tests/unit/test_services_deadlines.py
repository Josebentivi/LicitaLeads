from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from app.services.deadlines import (
    BrazilBusinessCalendar,
    DeadlineEngine,
    DeadlineMethod,
    DeadlineRequest,
    DeadlineStatus,
)

SP = ZoneInfo("America/Sao_Paulo")


def test_legal_estimate_excludes_weekend_and_national_holiday() -> None:
    result = DeadlineEngine().calculate(
        DeadlineRequest(
            event_type="INELIGIBLE",
            trigger_at=datetime(2026, 9, 4, 14, 30, tzinfo=SP),
            trigger_source="ata da sessão",
            uf="MA",
            now=datetime(2026, 9, 4, 18, tzinfo=UTC),
        )
    )
    assert result.method is DeadlineMethod.LEGAL_ESTIMATE
    assert result.deadline_at == datetime(2026, 9, 11, 2, 59, 59, tzinfo=UTC)
    assert result.status is DeadlineStatus.OPEN
    assert result.requires_manual_review
    assert any("2026-09-07" in item for item in result.excluded_dates)
    assert "3 dias úteis" in result.calculation_explanation


def test_explicit_deadline_takes_priority_and_can_be_expired() -> None:
    result = DeadlineEngine().calculate(
        DeadlineRequest(
            event_type="INELIGIBLE",
            trigger_at=datetime(2026, 9, 10, tzinfo=UTC),
            explicit_deadline_at=datetime(2026, 9, 11, 18, tzinfo=UTC),
            document_deadline_at=datetime(2026, 9, 12, 18, tzinfo=UTC),
            now=datetime(2026, 9, 12, tzinfo=UTC),
        )
    )
    assert result.method is DeadlineMethod.EXPLICIT
    assert result.status is DeadlineStatus.EXPIRED
    assert result.explicit_deadline_at == datetime(2026, 9, 11, 18, tzinfo=UTC)


def test_unknown_when_event_is_not_legally_eligible() -> None:
    result = DeadlineEngine().calculate(
        DeadlineRequest(event_type="HOMOLOGATED", trigger_at=datetime(2026, 9, 10, tzinfo=UTC))
    )
    assert result.method is DeadlineMethod.UNKNOWN
    assert result.status is DeadlineStatus.UNKNOWN
    assert result.requires_manual_review


def test_custom_municipal_holiday_csv() -> None:
    calendar = BrazilBusinessCalendar.from_csv(
        "date,name,scope,uf,municipality_ibge\n2026-09-08,Aniversário,municipal,MA,2111300\n"
    )
    due, excluded = calendar.add_business_days(
        date(2026, 9, 4), 1, uf="MA", municipality_ibge="2111300"
    )
    assert due == date(2026, 9, 9)
    assert any(reason == "Aniversário" for _, reason in excluded)


def test_suspended_session_requires_review() -> None:
    result = DeadlineEngine().calculate(
        DeadlineRequest(
            event_type="INELIGIBLE",
            trigger_at=datetime(2026, 9, 10, tzinfo=UTC),
            explicit_deadline_at=datetime(2026, 9, 20, tzinfo=UTC),
            session_suspended=True,
        )
    )
    assert result.status is DeadlineStatus.REQUIRES_REVIEW
    assert result.confidence == 0.4
