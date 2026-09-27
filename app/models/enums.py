"""Domain enums persisted by SQLAlchemy and shared with services/schemas."""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import Enum as SQLAlchemyEnum


class DataAvailability(StrEnum):
    AVAILABLE = "available"
    EMPTY = "empty"
    NOT_SUPPORTED = "not_supported"
    NOT_PUBLISHED = "not_published"
    ACCESS_RESTRICTED = "access_restricted"
    TEMPORARY_ERROR = "temporary_error"


class FieldValueStatus(StrEnum):
    OBSERVED = "observed"
    UNKNOWN = "unknown"
    NOT_AVAILABLE = "not_available"
    REQUIRES_MANUAL_REVIEW = "requires_manual_review"


class ProcurementEventType(StrEnum):
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


class ParticipantRole(StrEnum):
    PARTICIPANT = "participant"
    WINNER = "winner"
    AWARDED = "awarded"
    CONTRACTOR = "contractor"
    UNKNOWN = "unknown"


class ExtractionStatus(StrEnum):
    PENDING = "pending"
    EXTRACTED = "extracted"
    OCR_REQUIRED = "ocr_required"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"


class DeadlineCalculationMethod(StrEnum):
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


class ContactType(StrEnum):
    EMAIL = "email"
    PHONE = "phone"
    WHATSAPP = "whatsapp"
    FORM = "form"
    WEBSITE = "website"
    LINKEDIN = "linkedin"


class ContactStatus(StrEnum):
    DISCOVERED = "discovered"
    VERIFIED = "verified"
    INVALID = "invalid"
    REQUIRES_REVIEW = "requires_review"


class LeadStatus(StrEnum):
    NEW = "new"
    BELOW_THRESHOLD = "below_threshold"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_CHANGES = "needs_changes"
    ARCHIVED = "archived"


class ReviewDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_CHANGES = "needs_changes"


class CrawlRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OutreachChannel(StrEnum):
    EMAIL = "email"
    WHATSAPP = "whatsapp"
    PHONE = "phone"
    GENERIC = "generic"


def database_enum[EnumT: StrEnum](enum_class: type[EnumT]) -> SQLAlchemyEnum:
    """Create a portable, constrained VARCHAR enum using enum values."""

    values = [str(member.value) for member in enum_class]
    return SQLAlchemyEnum(
        enum_class,
        values_callable=lambda members: [str(member.value) for member in members],
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        name=f"enum_{enum_class.__name__.lower()}",
        length=max(map(len, values)),
    )
