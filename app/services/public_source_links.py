"""Browser links for official records, independent of their API provenance."""

import ipaddress
import re
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncScalarResult, AsyncSession

from app.connectors.pncp import parse_pncp_external_id
from app.models import Procurement, ProcurementSource, SourceRecord
from app.services.identifiers import is_valid_cnpj

_PROCUREMENT_CONTROL = re.compile(r"[A-Z0-9]{14}-1-[0-9]{6}/[0-9]{4}", re.I)


def public_link(value: str | None) -> str | None:
    if not value or "\\" in value or any(ord(char) < 32 for char in value):
        return None
    try:
        parts = urlsplit(value)
        host = (parts.hostname or "").rstrip(".")
        if (
            parts.scheme not in {"http", "https"}
            or not host
            or parts.username is not None
            or parts.password is not None
            or parts.port == 0
            or host == "localhost"
            or host.endswith(".localhost")
        ):
            return None
    except ValueError:
        return None
    try:
        return value if ipaddress.ip_address(host).is_global else None
    except ValueError:
        return value if "." in host else None


def procurement_public_url(control_number: str | None) -> str | None:
    if not control_number or not _PROCUREMENT_CONTROL.fullmatch(control_number):
        return None
    reference = parse_pncp_external_id(control_number)
    if not reference.sequence or not reference.year or not is_valid_cnpj(reference.cnpj):
        return None
    return f"https://pncp.gov.br/app/editais/{reference.cnpj}/{reference.year}/{reference.sequence}"


async def origin_platform_url(db: AsyncSession, procurement: Procurement) -> str | None:
    """Read the published platform link from the matched PNCP response."""
    control = procurement.pncp_control_number
    if not control:
        return None
    payloads: AsyncScalarResult[Any] = await db.stream_scalars(
        select(SourceRecord.raw_payload)
        .join(ProcurementSource, ProcurementSource.source_record_id == SourceRecord.id)
        .where(ProcurementSource.procurement_id == procurement.id, SourceRecord.source == "pncp")
        .order_by(SourceRecord.collected_at.desc(), SourceRecord.id)
    )
    try:
        async for payload in payloads:
            rows = payload.get("data", [payload]) if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                continue
            for row in rows:
                if isinstance(row, dict) and row.get("numeroControlePNCP") == control:
                    link = row.get("linkSistemaOrigem")
                    if isinstance(link, str) and (official := public_link(link)):
                        return official
        return None
    finally:
        await payloads.close()


def is_source_api_url(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.hostname or "").rstrip(".")
    path = parts.path.lower()
    return host in {"api.pncp.gov.br", "dadosabertos.compras.gov.br"} or (
        host == "pncp.gov.br" and path.startswith(("/api/", "/pncp-api/"))
    )


def procurement_source_page(procurement: Procurement) -> str | None:
    page = procurement_public_url(procurement.pncp_control_number)
    if page:
        return page
    source = public_link(procurement.source_url)
    return source if source and not is_source_api_url(source) else None


def source_link_label(url: str) -> str:
    if is_source_api_url(url):
        return "Consultar dados técnicos da fonte"
    return "Abrir fonte"
