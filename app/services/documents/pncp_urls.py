"""Resolve PNCP's internal document URLs using the stored official listing only."""

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

_FILE = re.compile(
    r"/(?:api/pncp|pncp-api)/v1/orgaos/(?P<cnpj>[A-Z0-9]{14})"
    r"/compras/(?P<year>\d{4})/(?P<procurement>\d+)/arquivos/(?P<document>\d+)",
    re.IGNORECASE,
)


def _resource(url: str) -> tuple[str, str, str, str] | None:
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "pncp.gov.br"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            return None
        # Validate malformed port syntax without ever connecting to that port.
        _ = parsed.port
    except ValueError:
        return None
    match = _FILE.fullmatch(parsed.path)
    if match is None:
        return None
    return (
        match["cnpj"].upper(),
        str(int(match["year"])),
        str(int(match["procurement"])),
        str(int(match["document"])),
    )


@dataclass(frozen=True)
class PNCPDocumentResolution:
    original_url: str
    canonical_url: str
    resource: tuple[str, str, str, str]

    def resolve(self, url: str) -> str:
        identity = _resource(url)
        if identity is None:
            if urlsplit(url).hostname == "pncp.gov.br":
                raise ValueError("ambiguous PNCP document redirect")
            return url
        if identity != self.resource:
            raise ValueError("PNCP redirect changed the official document identity")
        return self.canonical_url


def resolve_pncp_document(url: str, record: Any) -> PNCPDocumentResolution | None:
    identity = _resource(url)
    if identity is None or record is None or record.source != "pncp":
        return None
    try:
        endpoint = urlsplit(record.endpoint)
        endpoint_port = endpoint.port
    except ValueError:
        return None
    cnpj, year, purchase, document = identity
    listing = f"/v1/orgaos/{cnpj}/compras/{year}/{purchase}/arquivos"
    if (
        endpoint.scheme != "https"
        or endpoint.hostname != "pncp.gov.br"
        or endpoint.username is not None
        or endpoint.password is not None
        or endpoint.fragment
        or endpoint_port not in {None, 443}
        or endpoint.path.upper()
        not in {("/api/pncp" + listing).upper(), ("/pncp-api" + listing).upper()}
    ):
        return None
    payload = record.raw_payload
    rows = (
        payload
        if isinstance(payload, list)
        else next(
            (
                payload[key]
                for key in ("data", "documentos", "itens")
                if isinstance(payload, dict) and isinstance(payload.get(key), list)
            ),
            [],
        )
    )
    matched = any(
        isinstance(row, dict)
        and str(row.get("url") or row.get("uri")) == url
        and str(row.get("sequencialDocumento")) == document
        for row in rows
    )
    if not matched:
        return None
    canonical = f"https://pncp.gov.br/api/pncp{listing}/{document}"
    return PNCPDocumentResolution(url, canonical, identity)
