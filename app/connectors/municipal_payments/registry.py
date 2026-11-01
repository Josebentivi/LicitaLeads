"""Factory for municipal payment adapters."""

from __future__ import annotations

from typing import Any

from app.config import Settings, get_settings
from app.connectors.municipal_payments.base import MunicipalPaymentConnector
from app.connectors.municipal_payments.fiorilli import FiorilliPaymentConnector
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.connectors.municipal_payments.sources import get_municipal_source


def build_municipal_connector(
    source_id: str,
    settings: Settings | None = None,
    *,
    http_client: Any | None = None,
) -> MunicipalPaymentConnector:
    """Build the adapter for ``source_id``.

    ``http_client`` is injectable for tests.  A Fiorilli adapter expects an
    ``AsyncHTTPClient``; the São Luís CSV adapter expects ``httpx.AsyncClient``.
    Production callers should leave it unset.
    """

    selected = settings or get_settings()
    source = get_municipal_source(source_id)
    if source.is_fiorilli:
        return FiorilliPaymentConnector(source, selected, http_client=http_client)
    if source.adapter == "sao_luis_csv":
        return SaoLuisCsvConnector(source, selected, client=http_client)
    raise ValueError(f"unsupported municipal payment adapter: {source.adapter}")
