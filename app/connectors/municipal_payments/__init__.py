"""Payment connectors for municipal transparency portals.

The municipal landscape is not standardized.  Each adapter returns the same
``RawPayment`` DTOs as the federal connector, while the source catalogue keeps
the portal-specific limitations visible to the operator.
"""

from app.connectors.municipal_payments.base import (
    MunicipalPaymentConnector,
    MunicipalSource,
    parse_brazilian_date,
    parse_brazilian_money,
)
from app.connectors.municipal_payments.registry import build_municipal_connector
from app.connectors.municipal_payments.sources import (
    MUNICIPAL_SOURCES,
    get_municipal_source,
    list_municipal_sources,
)

__all__ = [
    "MUNICIPAL_SOURCES",
    "MunicipalPaymentConnector",
    "MunicipalSource",
    "build_municipal_connector",
    "get_municipal_source",
    "list_municipal_sources",
    "parse_brazilian_date",
    "parse_brazilian_money",
]
