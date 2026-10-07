"""HTTP request and response schemas."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ApiModel(BaseModel):
    """Base schema with ORM attribute support."""

    model_config = ConfigDict(from_attributes=True, use_enum_values=True)


class Page[T](ApiModel):
    """Stable page envelope for list endpoints."""

    items: list[T]
    page: int = 1
    page_size: int = 50
    total: int


class CrawlRunRequest(ApiModel):
    """Parameters for a manually-triggered crawl."""

    connector: Literal["pncp", "compras_gov", "all"] = "all"
    mode: Literal["procurements", "price_registries"] = "procurements"
    uf: str = Field(default="MA", min_length=2, max_length=2)
    days: int | None = Field(default=7, ge=1, le=365)
    start_date: date | None = None
    end_date: date | None = None
    modalities: list[str] = Field(default_factory=lambda: ["pregao_eletronico"])
    municipality: str | None = None
    agency: str | None = None
    keyword: str | None = None
    document_batch_size: int | None = Field(default=None, ge=1, le=500)

    @model_validator(mode="after")
    def validate_dates(self) -> CrawlRunRequest:
        """Require a coherent date range when explicit dates are supplied."""
        self.uf = self.uf.upper()
        if bool(self.start_date) != bool(self.end_date):
            raise ValueError("start_date and end_date must be supplied together")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must not be later than end_date")
        return self


class CrawlRunResponse(ApiModel):
    """Acknowledgement for an asynchronous crawl."""

    id: UUID
    status: str
    connector: str
    started_at: datetime


class LeadReviewRequest(ApiModel):
    """Human review decision attached to a lead."""

    decision: Literal["approved", "rejected", "needs_changes"]
    notes: str | None = Field(default=None, max_length=4000)
    reviewer: str | None = Field(default=None, max_length=255)


class LeadPatch(ApiModel):
    """Whitelisted mutable lead fields."""

    lead_status: (
        Literal[
            "new",
            "below_threshold",
            "pending_review",
            "approved",
            "rejected",
            "needs_changes",
            "archived",
        ]
        | None
    ) = None
    assigned_to: str | None = Field(default=None, max_length=255)
    reason_summary: str | None = Field(default=None, max_length=4000)
    recommended_action: str | None = Field(default=None, max_length=4000)


class OutreachRequest(ApiModel):
    """Request for an idempotent outreach draft."""

    channel: Literal["email", "whatsapp", "phone"] = "email"
    force_regenerate: bool = False


class EventCompanyLink(ApiModel):
    """Human-confirmed attribution of an event to an existing participant."""

    company_id: UUID
    reviewer: str | None = Field(default=None, max_length=255)
    note: str | None = Field(default=None, max_length=500)


class ContactImportError(ApiModel):
    """One rejected CSV row."""

    row: int
    message: str


class ContactImportResult(ApiModel):
    """Summary of an atomic contact CSV import."""

    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[ContactImportError] = Field(default_factory=list)


class ClearDataRequest(ApiModel):
    """Explicit confirmation for the destructive maintenance endpoint."""

    confirm: bool = False


class ClearDataResponse(ApiModel):
    """Rows and files removed by the maintenance reset."""

    counts: dict[str, int] = Field(default_factory=dict)
    files_removed: int = 0
