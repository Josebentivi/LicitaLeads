"""Conservative deterministic participant and procurement-event detection."""

from .detector import EventDetector, ParticipantDetector, associate_company
from .types import (
    CompanyReference,
    DetectedEvent,
    DetectedParticipant,
    DocumentEvidence,
    EventType,
    ParticipationRole,
    ReasonCategory,
)

__all__ = [
    "CompanyReference",
    "DetectedEvent",
    "DetectedParticipant",
    "DocumentEvidence",
    "EventDetector",
    "EventType",
    "ParticipantDetector",
    "ParticipationRole",
    "ReasonCategory",
    "associate_company",
]
