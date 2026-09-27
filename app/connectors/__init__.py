"""Auditable, read-only connectors for official procurement sources."""

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    ProcurementFilters,
    ProcurementSourceConnector,
    RawDocument,
    RawEvent,
    RawParticipant,
    RawProcurement,
    RawProcurementDetail,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.connectors.capabilities import (
    SOURCE_CAPABILITIES,
    CapabilityAvailability,
    SourceCapability,
    get_source_capabilities,
    render_capability_matrix_markdown,
)
from app.connectors.compras_gov import COMPRAS_MODALITY_CODES, ComprasGovConnector
from app.connectors.pncp import PNCP_MODALITY_CODES, PNCPConnector

__all__ = [
    "COMPRAS_MODALITY_CODES",
    "PNCP_MODALITY_CODES",
    "SOURCE_CAPABILITIES",
    "CapabilityAvailability",
    "ComprasGovConnector",
    "ConnectorResult",
    "DataAvailability",
    "PNCPConnector",
    "PaginationInfo",
    "ProcurementFilters",
    "ProcurementSourceConnector",
    "RawDocument",
    "RawEvent",
    "RawParticipant",
    "RawProcurement",
    "RawProcurementDetail",
    "RawProcurementItem",
    "RawResult",
    "RawSourceRecord",
    "SourceCapability",
    "get_source_capabilities",
    "render_capability_matrix_markdown",
]
