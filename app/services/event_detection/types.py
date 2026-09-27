from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class EventType(StrEnum):
    PROPOSAL_SUBMITTED = "PROPOSAL_SUBMITTED"
    PROPOSAL_ACCEPTED = "PROPOSAL_ACCEPTED"
    PROPOSAL_REJECTED = "PROPOSAL_REJECTED"
    DISQUALIFIED = "DISQUALIFIED"
    QUALIFIED = "QUALIFIED"
    INELIGIBLE = "INELIGIBLE"
    INTENT_TO_APPEAL = "INTENT_TO_APPEAL"
    APPEAL_SUBMITTED = "APPEAL_SUBMITTED"
    COUNTERARGUMENT_OPENED = "COUNTERARGUMENT_OPENED"
    COUNTERARGUMENT_SUBMITTED = "COUNTERARGUMENT_SUBMITTED"
    APPEAL_DECIDED = "APPEAL_DECIDED"
    WINNER_DECLARED = "WINNER_DECLARED"
    ADJUDICATED = "ADJUDICATED"
    HOMOLOGATED = "HOMOLOGATED"
    SESSION_SUSPENDED = "SESSION_SUSPENDED"
    SESSION_REOPENED = "SESSION_REOPENED"
    UNKNOWN = "UNKNOWN"


class ReasonCategory(StrEnum):
    TECHNICAL_SPECIFICATION = "TECHNICAL_SPECIFICATION"
    MISSING_DOCUMENT = "MISSING_DOCUMENT"
    INVALID_DOCUMENT = "INVALID_DOCUMENT"
    FISCAL_REGULARITY = "FISCAL_REGULARITY"
    LABOR_REGULARITY = "LABOR_REGULARITY"
    ECONOMIC_FINANCIAL = "ECONOMIC_FINANCIAL"
    TECHNICAL_QUALIFICATION = "TECHNICAL_QUALIFICATION"
    PRICE_INEXEQUIBILITY = "PRICE_INEXEQUIBILITY"
    PRICE_ABOVE_ESTIMATE = "PRICE_ABOVE_ESTIMATE"
    LATE_SUBMISSION = "LATE_SUBMISSION"
    PROPOSAL_FORMAT = "PROPOSAL_FORMAT"
    SAMPLE_REJECTED = "SAMPLE_REJECTED"
    BRAND_OR_MODEL_NONCOMPLIANT = "BRAND_OR_MODEL_NONCOMPLIANT"
    FAILURE_TO_RESPOND = "FAILURE_TO_RESPOND"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


class ParticipationRole(StrEnum):
    PARTICIPANT = "participant"
    WINNER = "winner"
    AWARDED = "awarded"


@dataclass(frozen=True, slots=True)
class CompanyReference:
    external_id: str | None
    legal_name: str
    cnpj: str | None = None


@dataclass(frozen=True, slots=True)
class DocumentEvidence:
    text: str
    source_url: str | None = None
    page_number: int | None = None
    locator: str | None = None
    document_type: str | None = None
    published_at: datetime | None = None
    start_offset: int | None = None
    end_offset: int | None = None


@dataclass(frozen=True, slots=True)
class DetectedParticipant:
    company_name: str
    company_cnpj: str | None
    role: ParticipationRole
    status: str | None
    item_number: str | None
    evidence: DocumentEvidence
    confidence: float
    requires_manual_review: bool
    company_external_id: str | None = None


@dataclass(frozen=True, slots=True)
class DetectedEvent:
    event_type: EventType
    raw_description: str
    normalized_reason: str | None
    reason_category: ReasonCategory
    evidence: DocumentEvidence
    confidence: float
    requires_manual_review: bool
    company_name: str | None = None
    company_cnpj: str | None = None
    company_external_id: str | None = None
    item_number: str | None = None
    occurred_at: datetime | None = None
