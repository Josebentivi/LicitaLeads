"""Repository exports."""

from app.repositories.base import BaseRepository, Page, pagination_bounds
from app.repositories.crawls import CrawlRunRepository, JobLeaseRepository
from app.repositories.leads import (
    CompanyContactRepository,
    LeadRepository,
    LeadReviewRepository,
    OutreachDraftRepository,
)
from app.repositories.procurements import (
    CompanyRepository,
    EvidenceRepository,
    ProcurementEventRepository,
    ProcurementRepository,
    ProcurementSourceRepository,
    SourceRecordRepository,
)

__all__ = [
    "BaseRepository",
    "CompanyContactRepository",
    "CompanyRepository",
    "CrawlRunRepository",
    "EvidenceRepository",
    "JobLeaseRepository",
    "LeadRepository",
    "LeadReviewRepository",
    "OutreachDraftRepository",
    "Page",
    "ProcurementEventRepository",
    "ProcurementRepository",
    "ProcurementSourceRepository",
    "SourceRecordRepository",
    "pagination_bounds",
]
