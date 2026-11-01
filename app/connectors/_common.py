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
    PaginationInfo,
    RawSourceRecord,
)
from app.connectors.http import (
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    HTTPJSONResponse,
    response_validators,
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


def combine_discoveries(results: list[ConnectorResult[Any]]) -> ConnectorResult[Any]:
    """Combine independent official partitions without claiming missing coverage."""
    data = [row for result in results for row in result.data]
    pages = [result.pagination for result in results if result.pagination is not None]
    incomplete = any(not result.is_complete for result in results)
    failure = next(
        (
            result
            for result in results
            if result.availability
            not in {
                DataAvailability.AVAILABLE,
                DataAvailability.EMPTY,
            }
        ),
        None,
    )
    return ConnectorResult(
        data=data,
        availability=DataAvailability.AVAILABLE
        if data
        else failure.availability
        if failure
        else DataAvailability.EMPTY,
        raw_records=[raw for result in results for raw in result.raw_records],
        source_urls=list(dict.fromkeys(url for result in results for url in result.source_urls)),
        partial=incomplete,
        diagnostic="; ".join(result.diagnostic for result in results if result.diagnostic) or None,
        pagination=PaginationInfo(
            fetched_pages=sum(page.fetched_pages for page in pages),
            total_pages=sum(page.total_pages or 0 for page in pages),
            total_records=sum(page.total_records or 0 for page in pages),
            pages_remaining=sum(page.pages_remaining or 0 for page in pages),
            truncated=any(page.truncated for page in pages) and failure is None,
        ),
    )


def source_record(source: str, response: HTTPJSONResponse) -> RawSourceRecord:
    return RawSourceRecord.build(
        source=source,
        endpoint=response.url,
        request_parameters=response.request_parameters,
        raw_payload=response.payload,
        http_status=response.status_code,
        collected_at=response.collected_at,
        response_headers=response_validators(response.headers),
    )


def error_availability(exc: Exception) -> DataAvailability:
    if isinstance(exc, ConnectorContractError):
        return DataAvailability.TEMPORARY_ERROR
    if isinstance(exc, ConnectorHTTPError):
        if exc.status_code in {401, 403}:
            return DataAvailability.ACCESS_RESTRICTED
        if exc.status_code == 404:
            return DataAvailability.NOT_PUBLISHED
        if exc.status_code in {400, 422}:
            return DataAvailability.NOT_SUPPORTED
    return DataAvailability.TEMPORARY_ERROR


def _content_type_suffix(content_type: str | None) -> str:
    return f", {content_type}" if content_type else ""


def error_diagnostic(exc: Exception) -> str:
    if isinstance(exc, ConnectorContractError):
        return f"official source contract mismatch: {exc}"
    if isinstance(exc, ConnectorHTTPError):
        if exc.status_code in {400, 422} and exc.body:
            return (
                f"official source rejected the request (HTTP {exc.status_code}): {exc.body[:300]}"
            )
        if 500 <= exc.status_code <= 599:
            return (
                "temporary official source error: "
                f"HTTP {exc.status_code}{_content_type_suffix(exc.content_type)}"
            )
        return (
            f"official source returned HTTP {exc.status_code}"
            f"{_content_type_suffix(exc.content_type)}"
        )
    if isinstance(exc, ConnectorDecodeError):
        if exc.status_code is not None:
            return (
                "temporary official source response: non-JSON "
                f"(HTTP {exc.status_code}{_content_type_suffix(exc.content_type)})"
            )
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


def nonnegative_decimal(value: Any) -> Decimal | None:
    """Map source decimals to the DB invariant, treating negatives as unavailable.

    Some sources publish sentinel/invalid values such as ``-0.0001``.  The
    original value stays available in ``raw_payload`` for auditability; the
    canonical column must respect its nonnegative check constraint.
    """

    parsed = to_decimal(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def positive_int(value: Any) -> int | None:
    """Map source integers to the positive-only invariants (e.g. participant rank)."""

    parsed = to_int(value)
    if parsed is None or parsed < 1:
        return None
    return parsed


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


def list_payload(payload: Any, *keys: str, operation: str = "response") -> list[dict[str, Any]]:
    """Return rows only for a recognized envelope.

    An unexpected successful payload is a contract mismatch, never an
    authoritative empty result. Invalid rows must never disappear into EMPTY.
    """

    if isinstance(payload, list):
        if any(not isinstance(item, dict) for item in payload):
            raise ConnectorContractError(f"{operation}: collection contains a non-object row")
        return payload
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                value = payload[key]
                if not isinstance(value, list):
                    raise ConnectorContractError(
                        f"{operation}: field {key!r} is not a list", url=None
                    )
                return list_payload(value, operation=operation)
        raise ConnectorContractError(
            f"{operation}: expected one of {', '.join(keys)} in the response envelope"
        )
    raise ConnectorContractError(
        f"{operation}: expected a list or object envelope, got {type(payload).__name__}"
    )


def pagination_number(payload: Any, key: str) -> int | None:
    if not isinstance(payload, dict) or key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if isinstance(value, bool) or not str(value).isdigit():
        raise ConnectorContractError(f"invalid pagination metadata: {key}")
    return int(value)


def observed_pages(
    payload: Any,
    page: int,
    *,
    row_count: int | None = None,
    collected_rows: int | None = None,
    page_size: int = 50,
) -> int:
    """Read both official pagination conventions without inventing an end."""
    total = pagination_number(payload, "totalPaginas")
    remaining = pagination_number(payload, "paginasRestantes")
    records = pagination_number(payload, "totalRegistros")
    if total is not None and remaining is not None and remaining != max(total - page, 0):
        raise ConnectorContractError("contradictory pagination metadata")
    if total is not None and total < page and row_count:
        raise ConnectorContractError("page outside declared pagination")
    observed = total if total is not None else page + (remaining or 0)
    if row_count is not None and collected_rows is not None:
        if records is not None and records < collected_rows:
            raise ConnectorContractError("collected rows exceed totalRegistros")
        if not row_count and (observed > page or (records or 0) > collected_rows):
            raise ConnectorContractError("empty page before the declared end")
        if total is None and remaining is None:
            if records is not None and records > collected_rows:
                observed = page + 1
            elif records is None and row_count >= page_size:
                observed = page + 1
        if observed <= page and records is not None and records != collected_rows:
            raise ConnectorContractError("final collection does not match totalRegistros")
    return observed


def repeated_page(records: list[RawSourceRecord], current: RawSourceRecord) -> bool:
    """Detect ignored page parameters, while keeping independent filters distinct."""
    params = {key: value for key, value in current.request_parameters.items() if key != "pagina"}
    return any(
        record.response_hash == current.response_hash
        and record.endpoint.split("?", 1)[0] == current.endpoint.split("?", 1)[0]
        and {key: value for key, value in record.request_parameters.items() if key != "pagina"}
        == params
        for record in records
    )
