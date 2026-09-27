"""Pure domain services used by the LicitaLead processing pipeline.

The modules in this package deliberately do not depend on SQLAlchemy models.  They
accept small immutable value objects so they can be used from the API, CLI and
background jobs without keeping database sessions open.
"""

from .contacts import ManualContactProvider, PublicWebsiteContactProvider
from .deadlines import DeadlineEngine
from .documents import DocumentExtractor
from .event_detection import EventDetector, ParticipantDetector
from .identifiers import is_valid_cnpj, normalize_cnpj, normalize_company_name
from .lead_scoring import LeadScorer
from .llm import DisabledLLMEventAnalyzer, EvidenceBoundLLMEventAnalyzer, LLMEventAnalyzer
from .outreach import OutreachDraftService

__all__ = [
    "DeadlineEngine",
    "DisabledLLMEventAnalyzer",
    "DocumentExtractor",
    "EventDetector",
    "EvidenceBoundLLMEventAnalyzer",
    "LLMEventAnalyzer",
    "LeadScorer",
    "ManualContactProvider",
    "OutreachDraftService",
    "ParticipantDetector",
    "PublicWebsiteContactProvider",
    "is_valid_cnpj",
    "normalize_cnpj",
    "normalize_company_name",
]
