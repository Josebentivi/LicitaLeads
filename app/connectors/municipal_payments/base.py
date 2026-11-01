"""Shared types and normalizers for municipal payment portals."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from app.connectors.base import ConnectorResult, RawPayment


@dataclass(frozen=True, slots=True)
class MunicipalSource:
    """Static description of one municipal payment source."""

    id: str
    name: str
    municipality: str
    uf: str
    adapter: str
    base_url: str
    mode: str
    note: str
    entity_code: str = "1"
    filter_client_entity: bool = False
    supports_details: bool = False
    default_enabled: bool = True
    portal_url: str = ""
    export_url: str = ""
    open_data_url: str = ""
    import_steps: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_fiorilli(self) -> bool:
        return self.adapter == "fiorilli_json"


@runtime_checkable
class MunicipalPaymentConnector(Protocol):
    """Interface implemented by municipal payment adapters."""

    source: MunicipalSource

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]: ...

    async def aclose(self) -> None: ...


_MONEY_RE = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d+)?$")
_SPACES_RE = re.compile(r"\s+")


def normalize_key(value: str | None) -> str:
    """Fold a header/field name into a stable snake_case key."""

    folded = unicodedata.normalize("NFKD", value or "")
    folded = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", folded.lower()).strip("_")


def clean_text(value: Any) -> str | None:
    """Return a compact, non-empty text value without changing its meaning."""

    if value is None:
        return None
    text = _SPACES_RE.sub(" ", str(value)).strip()
    return text or None


def parse_brazilian_money(value: Any) -> Decimal | None:
    """Parse ``1.234,56`` and plain values without introducing float rounding."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).replace("\xa0", " ").replace("R$", "").strip()
    if not text or text in {"-", "--"}:
        return None
    negative = text.startswith("-")
    text = text.lstrip("-").strip()
    if "," in text:
        if _MONEY_RE.match(text):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", ".")
    elif text.count(".") > 1:
        text = text.replace(".", "")
    elif text.count(".") == 1:
        integer, fraction = text.split(".", 1)
        if len(fraction) == 3 and integer.isdigit():
            text = integer + fraction
    try:
        result = Decimal(text)
    except InvalidOperation:
        return None
    return -result if negative else result


def parse_brazilian_date(value: Any) -> datetime | None:
    """Parse the date formats published by Fiorilli/SCPI portals."""

    text = clean_text(value)
    if not text:
        return None
    for pattern in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d-%m-%Y",
    ):
        try:
            parsed = datetime.strptime(text[:19], pattern)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


def row_fingerprint(source_id: str, row: dict[str, Any]) -> str:
    """Create a stable external id for a row without a source key."""

    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(f"{source_id}:{payload}".encode()).hexdigest()[:32]


def source_name(source: MunicipalSource) -> str:
    return f"municipal:{source.id}"


def source_url(url: str, params: dict[str, Any] | None = None) -> str:
    """Return a human-openable URL for provenance (not used for HTTP calls)."""

    if not params:
        return url
    from urllib.parse import urlencode

    return f"{url}?{urlencode(params)}"
