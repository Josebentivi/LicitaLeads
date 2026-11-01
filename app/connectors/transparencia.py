"""Portal da Transparência payment connector (federal executive branch).

The API requires a free token and only covers federal payment documents.  A
missing token is represented as ``ACCESS_RESTRICTED``; it is never treated as
"no payments".
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.connectors._common import failed_result, join_url, normalized_identifier, source_record
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawPayment,
    RawSourceRecord,
)
from app.connectors.continuation import page_budget_exhausted
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    setting,
)

_HTTP_EXCEPTIONS = (
    ConnectorHTTPError,
    ConnectorContractError,
    ConnectorDecodeError,
    httpx.TimeoutException,
    httpx.TransportError,
)
_MAX_PAGES = 200
_BRL = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d+)?$")
_PHASE_CODES = {
    "1": 1,
    "empenho": 1,
    "2": 2,
    "liquidacao": 2,
    "liquida\u00e7\u00e3o": 2,
    "3": 3,
    "pagamento": 3,
}


def _amount(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace("R$", "").strip()
    if not text:
        return None
    if _BRL.match(text):
        text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _datetime(value: object, source_timezone: str = "America/Sao_Paulo") -> datetime | None:
    """Parse federal date/timestamps using the source's configured timezone."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for pattern in ("%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        else:
            return None
    if parsed.tzinfo is None:
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(source_timezone))
        except Exception:  # pragma: no cover - Settings validates the zone
            parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def payment_external_id(row: dict[str, Any]) -> str:
    """Build a stable payment identity without mutable monetary values."""

    document = str(row.get("documento") or row.get("documentoResumido") or "")
    phase = row.get("fase")
    supplier = normalized_identifier(row.get("codigoFavorecido"))
    agency = normalized_identifier(
        row.get("codigoOrgao") or row.get("codigoOrgaoSuperior") or row.get("orgaoCodigo")
    )
    uasg = row.get("codigoUg") or row.get("ug")
    payment_date = _datetime(
        row.get("data") or row.get("dataDocumento") or row.get("dataMovimento")
    )
    date_key = (
        payment_date.date().isoformat()
        if payment_date is not None
        else normalized_identifier(row.get("data") or row.get("dataDocumento"))
    )
    return ":".join(
        part or "-"
        for part in (document, str(phase or ""), supplier, agency, str(uasg or ""), date_key)
    )


class TransparenciaPaymentsConnector:
    """Read-only payment documents for one company and year."""

    name = "transparencia"

    def __init__(
        self,
        settings: object | None = None,
        *,
        http_client: AsyncHTTPClient | None = None,
    ) -> None:
        self.settings = settings
        self.base_url = str(
            setting(
                settings,
                "transparencia_base_url",
                "https://api.portaldatransparencia.gov.br",
            )
        ).rstrip("/")
        self.api_key = str(setting(settings, "transparencia_api_key", "") or "")
        self._http = http_client or AsyncHTTPClient(
            settings,
            timeout_seconds=float(setting(settings, "transparencia_timeout_seconds", 30)),
            max_retries=int(setting(settings, "transparencia_max_retries", 1)),
        )
        self._owns_http = http_client is None
        self.timezone = str(setting(settings, "timezone", "America/Sao_Paulo"))

    async def discover_payments(
        self,
        *,
        company_cnpj: str,
        year: int,
        phase: str | None = None,
    ) -> ConnectorResult[list[RawPayment]]:
        if not self.api_key.strip():
            return ConnectorResult(
                data=[],
                availability=DataAvailability.ACCESS_RESTRICTED,
                diagnostic="TRANSPARENCIA_API_KEY is not configured",
            )
        cnpj = normalized_identifier(company_cnpj)
        if not cnpj:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_SUPPORTED,
                diagnostic="a company CNPJ is required",
            )
        phase_code = self._phase_code(phase)
        if phase_code is None:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_SUPPORTED,
                diagnostic="phase must be empenho, liquidacao or pagamento (1/2/3)",
            )
        endpoint = join_url(self.base_url, "api-de-dados/despesas/documentos-por-favorecido")
        records: list[RawSourceRecord] = []
        payments: list[RawPayment] = []
        seen: set[str] = set()
        page = 1
        partial = False
        diagnostic: str | None = None
        headers = {"chave-api-dados": self.api_key}
        try:
            while True:
                response = await self._http.request_json(
                    "GET",
                    endpoint,
                    params={
                        "codigoPessoa": cnpj,
                        "ano": year,
                        "fase": phase_code,
                        "pagina": page,
                    },
                    headers=headers,
                )
                record = source_record(self.name, response)
                records.append(record)
                if response.status_code in {204, 205}:
                    rows: list[dict[str, Any]] = []
                elif isinstance(response.payload, list):
                    if any(not isinstance(row, dict) for row in response.payload):
                        raise ConnectorContractError(
                            "Transparência collection contains a non-object row", url=response.url
                        )
                    rows = response.payload
                else:
                    raise ConnectorContractError(
                        "Transparência response is not a list", url=response.url
                    )
                if not rows:
                    break
                mapped = [
                    self._map_payment(
                        row,
                        response.url,
                        source_record_ref=record.record_key,
                    )
                    for row in rows
                    if isinstance(row, dict) and self._has_payment_identity(row)
                ]
                if len(mapped) != len(rows):
                    partial = True
                    diagnostic = "official response contained rows without payment identity"
                fresh = [payment for payment in mapped if payment.external_id not in seen]
                if not fresh:
                    if len(mapped) == len(rows):
                        diagnostic = "official response repeated the same payment page"
                        partial = True
                    break
                seen.update(payment.external_id for payment in fresh)
                payments.extend(fresh)
                if page_budget_exhausted(self._http, page, _MAX_PAGES):
                    partial = True
                    diagnostic = f"payment pagination stopped at the {_MAX_PAGES}-page limit"
                    break
                page += 1
        except _HTTP_EXCEPTIONS as exc:
            if not payments:
                result: ConnectorResult[list[RawPayment]] = failed_result([], exc)
                result.source_urls = [record.endpoint for record in records]
                result.raw_records = records
                return result
            partial = True
            diagnostic = f"partial result: payment pagination failed ({type(exc).__name__})"
        return ConnectorResult(
            data=payments,
            availability=(
                DataAvailability.AVAILABLE
                if payments
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=[record.endpoint for record in records],
            raw_records=records,
            diagnostic=(
                diagnostic
                if payments or partial
                else "no payment documents matched the official response"
            ),
            partial=partial,
            pagination=PaginationInfo(
                fetched_pages=len(records),
                truncated=bool(diagnostic and "pagination stopped at" in diagnostic),
            ),
        )

    @staticmethod
    def _phase_code(phase: str | int | None) -> int | None:
        if phase is None:
            return 3
        return _PHASE_CODES.get(str(phase).strip().lower())

    def _map_payment(
        self,
        row: dict[str, Any],
        source_url: str,
        *,
        source_record_ref: str | None = None,
    ) -> RawPayment:
        document = str(row.get("documento") or row.get("documentoResumido") or "")
        phase = row.get("fase")
        supplier = normalized_identifier(row.get("codigoFavorecido"))
        amount = _amount(row.get("valor"))
        payment_date = _datetime(
            row.get("data") or row.get("dataDocumento") or row.get("dataMovimento"),
            self.timezone,
        )
        uasg = row.get("codigoUg") or row.get("ug")
        external_id = payment_external_id(row)
        return RawPayment(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=external_id,
            source_record_ref=source_record_ref,
            agency_name=row.get("orgao") or row.get("orgaoSuperior"),
            uasg=uasg,
            process_number=row.get("numeroProcesso"),
            payment_date=payment_date,
            amount=amount,
            phase=str(phase) if phase else None,
            document_number=document or None,
            supplier_cnpj=supplier,
            supplier_name=row.get("nomeFavorecido"),
        )

    @staticmethod
    def _has_payment_identity(row: dict[str, Any]) -> bool:
        document = row.get("documento") or row.get("documentoResumido")
        supplier = normalized_identifier(row.get("codigoFavorecido"))
        return bool(document and supplier)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()
