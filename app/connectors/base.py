"""Public contracts shared by procurement source connectors.

The connector layer deliberately returns source-shaped DTOs instead of ORM
objects.  Ingestion is then able to persist the original response before it
normalizes any field, which keeps every conclusion auditable.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DataAvailability(StrEnum):
    """Meaning of the data returned by a connector call.

    ``EMPTY`` is reserved for a supported collection that was queried
    successfully and contained no records.  It must not be used for a feature
    the source does not expose.
    """

    AVAILABLE = "available"
    EMPTY = "empty"
    NOT_SUPPORTED = "not_supported"
    NOT_PUBLISHED = "not_published"
    ACCESS_RESTRICTED = "access_restricted"
    TEMPORARY_ERROR = "temporary_error"


class PaginationInfo(BaseModel):
    """Pagination information observed while collecting a result."""

    model_config = ConfigDict(extra="forbid")

    page: int = 1
    page_size: int | None = None
    total_records: int | None = None
    total_pages: int | None = None
    pages_remaining: int | None = None
    fetched_pages: int = 0
    truncated: bool = False


class RawSourceRecord(BaseModel):
    """One unmodified HTTP response and the request that produced it."""

    model_config = ConfigDict(extra="forbid")

    source: str
    endpoint: str
    request_parameters: dict[str, Any] = Field(default_factory=dict)
    response_hash: str
    raw_payload: Any
    http_status: int
    collected_at: datetime

    @classmethod
    def build(
        cls,
        *,
        source: str,
        endpoint: str,
        request_parameters: dict[str, Any] | None,
        raw_payload: Any,
        http_status: int,
        collected_at: datetime,
    ) -> RawSourceRecord:
        """Create a source record with a stable SHA-256 response fingerprint."""

        encoded = json.dumps(
            raw_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return cls(
            source=source,
            endpoint=endpoint,
            request_parameters=request_parameters or {},
            response_hash=hashlib.sha256(encoded).hexdigest(),
            raw_payload=raw_payload,
            http_status=http_status,
            collected_at=collected_at,
        )


class ConnectorResult[T](BaseModel):
    """Data plus an explicit statement about its availability and provenance."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data: T
    availability: DataAvailability
    source_urls: list[str] = Field(default_factory=list)
    raw_records: list[RawSourceRecord] = Field(default_factory=list)
    collected_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pagination: PaginationInfo | None = None
    diagnostic: str | None = None

    @property
    def is_available(self) -> bool:
        """Return whether the result contains usable source data."""

        return self.availability is DataAvailability.AVAILABLE


class ProcurementFilters(BaseModel):
    """Source-independent filters accepted by discovery operations."""

    model_config = ConfigDict(extra="forbid")

    start_date: date | None = None
    end_date: date | None = None
    days: int | None = Field(default=None, ge=1, le=3650)
    uf: str | None = None
    municipality: str | None = None
    municipality_code: str | None = None
    agency: str | None = None
    agency_cnpj: str | None = None
    unit_code: str | None = None
    keyword: str | None = None
    modalities: list[str | int] = Field(default_factory=list)
    page_size: int | None = Field(default=None, ge=1, le=500)
    max_pages: int | None = Field(default=None, ge=1)
    discovery_kind: str = "publication"

    @field_validator("uf")
    @classmethod
    def normalize_uf(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if len(normalized) != 2 or not normalized.isalpha():
            raise ValueError("uf must be a two-letter state abbreviation")
        return normalized

    @field_validator("agency_cnpj")
    @classmethod
    def normalize_agency_cnpj(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = "".join(char for char in value.upper() if char.isalnum())
        if len(normalized) != 14:
            raise ValueError("agency_cnpj must contain 14 alphanumeric positions")
        return normalized

    def resolved_period(self, *, lookback_days: int = 30) -> tuple[date, date]:
        """Resolve omitted dates without mutating the caller's filter object."""

        end = self.end_date or datetime.now(UTC).date()
        days = self.days or lookback_days
        start = self.start_date or (end - timedelta(days=days - 1))
        if start > end:
            raise ValueError("start_date cannot be after end_date")
        return start, end


class TraceableModel(BaseModel):
    """Base DTO which accepts new source fields while preserving the raw body."""

    model_config = ConfigDict(extra="allow")

    source: str
    source_url: str | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class RawProcurement(TraceableModel):
    external_id: str
    pncp_control_number: str | None = None
    uasg: str | None = None
    purchase_number: str | None = None
    purchase_year: int | None = None
    process_number: str | None = None
    modality: str | None = None
    modality_code: int | str | None = None
    title: str | None = None
    object_description: str | None = None
    agency_name: str | None = None
    agency_cnpj: str | None = None
    government_sphere: str | None = None
    government_branch: str | None = None
    uf: str | None = None
    municipality: str | None = None
    municipality_code: str | None = None
    estimated_value: Decimal | None = None
    homologated_value: Decimal | None = None
    is_srp: bool | None = None
    legal_basis: str | None = None
    proposal_start_at: datetime | None = None
    proposal_end_at: datetime | None = None
    session_start_at: datetime | None = None
    publication_at: datetime | None = None
    last_source_update_at: datetime | None = None
    status: str | None = None


class RawProcurementDetail(RawProcurement):
    additional_data: dict[str, Any] = Field(default_factory=dict)


class RawProcurementItem(TraceableModel):
    external_id: str
    procurement_external_id: str
    item_number: str | None = None
    description: str | None = None
    detailed_description: str | None = None
    quantity: Decimal | None = None
    unit: str | None = None
    estimated_unit_value: Decimal | None = None
    estimated_total_value: Decimal | None = None
    result_status: str | None = None
    has_result: bool | None = None


class RawDocument(TraceableModel):
    external_id: str
    procurement_external_id: str
    document_type: str | None = None
    title: str | None = None
    download_url: str
    published_at: datetime | None = None
    active: bool | None = None
    mime_type: str | None = None


class RawResult(TraceableModel):
    external_id: str
    procurement_external_id: str
    item_external_id: str | None = None
    item_number: str | None = None
    supplier_cnpj: str | None = None
    supplier_name: str | None = None
    person_type: str | None = None
    role: str = "awarded"
    quantity: Decimal | None = None
    unit_value: Decimal | None = None
    total_value: Decimal | None = None
    rank: int | None = None
    result_status: str | None = None
    result_at: datetime | None = None
    is_cancelled: bool = False
    cancellation_reason: str | None = None


class RawParticipant(TraceableModel):
    external_id: str
    procurement_external_id: str
    item_external_id: str | None = None
    company_cnpj: str
    company_name: str | None = None
    participation_role: str
    proposal_value: Decimal | None = None
    final_value: Decimal | None = None
    rank: int | None = None
    status: str | None = None
    confidence: float = Field(default=1.0, ge=0, le=1)


class RawEvent(TraceableModel):
    external_id: str
    procurement_external_id: str
    event_type: str
    description: str | None = None
    occurred_at: datetime | None = None
    published_at: datetime | None = None
    confidence: float = Field(default=1.0, ge=0, le=1)
    requires_manual_review: bool = False


class RawPriceRegistry(TraceableModel):
    """Source-shaped price registry (ata de registro de preços) header."""

    external_id: str
    pncp_control_number: str | None = None
    linked_pncp_control_number: str | None = None
    registry_number: str | None = None
    year: int | None = None
    agency_name: str | None = None
    agency_cnpj: str | None = None
    uasg: str | None = None
    object_description: str | None = None
    status: str | None = None
    signed_at: datetime | None = None
    published_at: datetime | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    total_value: Decimal | None = None
    allows_adhesion: bool | None = None


class RawPriceRegistryItem(TraceableModel):
    """One registered item/supplier of a price registry, when the source publishes it."""

    external_id: str
    price_registry_external_id: str
    item_number: str | None = None
    description: str | None = None
    unit: str | None = None
    quantity: Decimal | None = None
    unit_value: Decimal | None = None
    total_value: Decimal | None = None
    max_adhesion_quantity: Decimal | None = None
    supplier_cnpj: str | None = None
    supplier_name: str | None = None


@runtime_checkable
class ProcurementSourceConnector(Protocol):
    """Interface implemented by every public procurement source."""

    name: str

    async def discover_procurements(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawProcurement]]: ...

    async def fetch_procurement(
        self, external_id: str
    ) -> ConnectorResult[RawProcurementDetail | None]: ...

    async def fetch_items(self, external_id: str) -> ConnectorResult[list[RawProcurementItem]]: ...

    async def fetch_documents(self, external_id: str) -> ConnectorResult[list[RawDocument]]: ...

    async def fetch_results(self, external_id: str) -> ConnectorResult[list[RawResult]]: ...

    async def fetch_participants(
        self, external_id: str
    ) -> ConnectorResult[list[RawParticipant]]: ...

    async def fetch_events(self, external_id: str) -> ConnectorResult[list[RawEvent]]: ...

    async def aclose(self) -> None: ...


@runtime_checkable
class PriceRegistryConnector(Protocol):
    """Interface for sources that publish price registries (ARP/atas)."""

    name: str

    async def discover_price_registries(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawPriceRegistry]]: ...

    async def fetch_price_registry_items(
        self, external_id: str
    ) -> ConnectorResult[list[RawPriceRegistryItem]]: ...

    async def aclose(self) -> None: ...
