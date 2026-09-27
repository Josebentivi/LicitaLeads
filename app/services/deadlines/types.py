from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class DeadlineMethod(StrEnum):
    EXPLICIT = "EXPLICIT"
    DOCUMENT_EXTRACTED = "DOCUMENT_EXTRACTED"
    EDITAL_RULE = "EDITAL_RULE"
    LEGAL_ESTIMATE = "LEGAL_ESTIMATE"
    MANUAL = "MANUAL"
    UNKNOWN = "UNKNOWN"


class DeadlineStatus(StrEnum):
    OPEN = "OPEN"
    DUE_TODAY = "DUE_TODAY"
    DUE_WITHIN_24H = "DUE_WITHIN_24H"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"
    REQUIRES_REVIEW = "REQUIRES_REVIEW"


@dataclass(frozen=True, slots=True)
class DeadlineRequest:
    event_type: str
    trigger_at: datetime | None
    explicit_deadline_at: datetime | None = None
    document_deadline_at: datetime | None = None
    edital_business_days: int | None = None
    legal_business_days: int = 3
    legal_estimate_applicable: bool | None = None
    legal_basis: str | None = None
    trigger_source: str | None = None
    uf: str | None = None
    municipality_ibge: str | None = None
    timezone: str = "America/Sao_Paulo"
    timezone_inferred: bool = False
    local_holiday_calendar_complete: bool = False
    session_suspended: bool = False
    reopened_at: datetime | None = None
    now: datetime | None = None


@dataclass(frozen=True, slots=True)
class DeadlineResult:
    method: DeadlineMethod
    trigger_at: datetime | None
    deadline_at: datetime | None
    explicit_deadline_at: datetime | None
    estimated_deadline_at: datetime | None
    remaining_seconds: int | None
    status: DeadlineStatus
    confidence: float
    requires_manual_review: bool
    legal_basis: str | None
    calculation_explanation: str
    excluded_dates: tuple[str, ...] = ()
