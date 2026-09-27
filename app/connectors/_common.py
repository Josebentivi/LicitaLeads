"""Internal mapping helpers shared by the official source connectors."""

from __future__ import annotations

import re
import unicodedata
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    RawSourceRecord,
)
from app.connectors.http import (
    ConnectorDecodeError,
    ConnectorHTTPError,
    HTTPJSONResponse,
)


def runtime_settings(settings: object | None) -> object | None:
    """Load application settings lazily while keeping connectors reusable."""

    if settings is not None:
        return settings
    try:
        from app.config import get_settings

        return get_settings()
    except (ImportError, RuntimeError):
        return None


def source_record(source: str, response: HTTPJSONResponse) -> RawSourceRecord:
    return RawSourceRecord.build(
        source=source,
        endpoint=response.url,
        request_parameters=response.request_parameters,
        raw_payload=response.payload,
        http_status=response.status_code,
        collected_at=response.collected_at,
    )


def error_availability(exc: Exception) -> DataAvailability:
    if isinstance(exc, ConnectorHTTPError):
        if exc.status_code in {401, 403}:
            return DataAvailability.ACCESS_RESTRICTED
        if exc.status_code == 404:
            return DataAvailability.NOT_PUBLISHED
    return DataAvailability.TEMPORARY_ERROR


def error_diagnostic(exc: Exception) -> str:
    if isinstance(exc, ConnectorHTTPError):
        return f"official source returned HTTP {exc.status_code}"
    if isinstance(exc, ConnectorDecodeError):
        return "official source returned a non-JSON response"
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return f"temporary network failure: {type(exc).__name__}"
    return f"connector failure: {type(exc).__name__}"


def failed_result[T](empty_data: T, exc: Exception) -> ConnectorResult[T]:
    return ConnectorResult(
        data=empty_data,
        availability=error_availability(exc),
        diagnostic=error_diagnostic(exc),
    )


def normalized_text(value: Any) -> str:
    if value is None:
        return ""
    decomposed = unicodedata.normalize("NFKD", str(value))
    return " ".join(
        "".join(char for char in decomposed if not unicodedata.combining(char)).casefold().split()
    )


def contains_text(haystacks: list[Any], needle: str | None) -> bool:
    if not needle:
        return True
    normalized_needle = normalized_text(needle)
    return any(normalized_needle in normalized_text(value) for value in haystacks)


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_datetime(value: Any, source_timezone: str = "America/Sao_Paulo") -> datetime | None:
    """Parse source timestamps and normalize them to UTC.

    Official APIs often omit an offset.  In that case the configured display
    timezone is used explicitly instead of silently relying on the host clock.
    The unmodified value remains available in ``raw_payload``.
    """

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(source_timezone))
        except Exception:  # pragma: no cover - invalid zones are blocked by Settings
            parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalized_identifier(value: Any) -> str | None:
    if value is None:
        return None
    result = "".join(char for char in str(value).upper() if char.isalnum())
    return result or None


def is_corporate_identifier(value: str | None, person_type: str | None = None) -> bool:
    """Accept a 14-position CNPJ only; never promote CPF/person records."""

    normalized = normalized_identifier(value)
    if normalized is None or len(normalized) != 14:
        return False
    if not re.fullmatch(r"[A-Z0-9]{14}", normalized):
        return False
    if person_type and normalized_text(person_type) not in {
        "pj",
        "j",
        "juridica",
        "pessoa juridica",
    }:
        return False
    return True


def join_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def unique_urls(records: list[RawSourceRecord]) -> list[str]:
    return list(dict.fromkeys(record.endpoint for record in records))


def list_payload(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []
