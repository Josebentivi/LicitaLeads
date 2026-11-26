"""Portal da Transparência payment connector (federal executive branch).

The API requires a free token and only covers federal payment documents.  A
missing token is represented as ``ACCESS_RESTRICTED``; it is never treated as
"no payments".
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
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
from app.connectors.federal_payment_identity import (
    creditor_list_code,
    federal_payment_identity,
    siafi_payment_document,
)
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    setting,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

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
_FINAL_ROW_FIELDS = (
    "codigoPagamento",
    "codigoListaCredor",
    "codigoFavorecidoFinal",
    "nomeFavorecidoFinal",
    "valorFinal",
    "dataCarga",
    "codigoOrgaoVinculado",
    "orgaoVinculado",
    "codigoUnidadeGestora",
)


def _final_page_projection(payload: Any) -> dict[str, Any]:
    """Retain only corporate beneficiaries on shared creditor-list pages."""
    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        raise ConnectorContractError("Transparência final-beneficiary page is not a list of rows")
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    rows = [
        {name: row[name] for name in _FINAL_ROW_FIELDS if name in row}
        for row in payload
        if is_valid_cnpj(normalized_identifier(row.get("codigoFavorecidoFinal")))
    ]
    return {
        "corporate_beneficiaries": rows,
        "row_count": len(payload),
        "original_response_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _intermediary_marker(row: dict[str, Any]) -> bool | None:
    markers = (row.get("favorecidoIntermediario"), row.get("favorecidoListaFaturas"))
    if any(value is True for value in markers):
        return True
    if all(value is False for value in markers):
        return False
    return None


def _amount(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace("R$", "").strip()
    if not text:
        return None
    if _BRL.match(text):
        text = text.replace(".", "").replace(",", ".")
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    return amount if amount.is_finite() else None


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
        cnpj = normalize_cnpj(company_cnpj)
        if not cnpj or not is_valid_cnpj(cnpj):
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
        issued_documents: dict[str, RawPayment] = {}
        seen: set[str] = set()
        page = 1
        partial = False
        truncated = False
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
                        queried_phase=phase_code,
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
                for payment in fresh:
                    if phase_code == 3 and _intermediary_marker(payment.raw_payload) is True:
                        if document := siafi_payment_document(payment.document_number):
                            issued_documents.setdefault(document, payment)
                        else:
                            partial = True
                            diagnostic = "pagamento com repasse e código SIAFI incompleto"
                    if normalize_cnpj(payment.supplier_cnpj) == cnpj:
                        payments.append(payment)
                if page_budget_exhausted(self._http, page, _MAX_PAGES):
                    partial = True
                    truncated = True
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
        for document, issued in issued_documents.items():
            finals = await self._final_beneficiaries(document, cnpj, issued, headers)
            records.extend(finals.raw_records)
            if finals.is_complete:
                payments.extend(finals.data)
            else:
                partial = True
                truncated = truncated or bool(finals.pagination and finals.pagination.truncated)
                diagnostic = " · ".join(filter(None, (diagnostic, finals.diagnostic)))
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
                truncated=truncated,
            ),
        )

    async def _final_beneficiaries(
        self, document: str, cnpj: str, issued: RawPayment, headers: dict[str, str]
    ) -> ConnectorResult[list[RawPayment]]:
        endpoint = join_url(self.base_url, "api-de-dados/despesas/favorecidos-finais-por-documento")
        records: list[RawSourceRecord] = []
        rows_by_list: dict[str, list[tuple[RawSourceRecord, dict[str, Any]]]] = defaultdict(list)
        fingerprints: set[str] = set()
        page = 1
        incomplete: str | None = None
        truncated = False
        try:
            while True:
                response = await self._http.request_json(
                    "GET",
                    endpoint,
                    params={"codigoDocumento": document, "pagina": page},
                    headers=headers,
                    project_payload=_final_page_projection,
                )
                record = source_record(self.name, response)
                records.append(record)
                projected = response.payload
                if not isinstance(projected, dict) or "corporate_beneficiaries" not in projected:
                    raise ConnectorContractError("invalid retained final-beneficiary response")
                if projected["row_count"] == 0:
                    break
                fingerprint = projected["original_response_sha256"]
                if fingerprint in fingerprints:
                    incomplete = "a fonte repetiu a página de favorecidos finais"
                    break
                fingerprints.add(fingerprint)
                for row in projected["corporate_beneficiaries"]:
                    if normalized_identifier(row.get("codigoFavorecidoFinal")) != cnpj:
                        continue
                    creditor_list = creditor_list_code(row.get("codigoListaCredor"))
                    if (
                        siafi_payment_document(row.get("codigoPagamento")) != document
                        or not creditor_list
                    ):
                        incomplete = "repasse sem identificação completa do documento e da lista"
                        continue
                    rows_by_list[creditor_list].append((record, row))
                if page_budget_exhausted(self._http, page, _MAX_PAGES):
                    truncated = True
                    incomplete = "limite de páginas de favorecidos finais atingido"
                    break
                page += 1
        except _HTTP_EXCEPTIONS as exc:
            incomplete = f"coleta de favorecidos finais falhou ({type(exc).__name__})"

        payments: list[RawPayment] = []
        source_pages = len(records)
        if incomplete is None:
            for creditor_list, group in rows_by_list.items():
                aggregation = self._final_payment(document, cnpj, creditor_list, issued, group)
                records.append(aggregation[0])
                payments.append(aggregation[1])
        return ConnectorResult(
            data=payments,
            availability=(
                DataAvailability.TEMPORARY_ERROR
                if incomplete
                else DataAvailability.AVAILABLE
                if payments
                else DataAvailability.EMPTY
            ),
            partial=incomplete is not None,
            diagnostic=f"{document}: {incomplete}" if incomplete else None,
            source_urls=list(dict.fromkeys(record.endpoint for record in records)),
            raw_records=records,
            pagination=PaginationInfo(fetched_pages=source_pages, truncated=truncated),
        )

    def _final_payment(
        self,
        document: str,
        cnpj: str,
        creditor_list: str,
        issued: RawPayment,
        group: list[tuple[RawSourceRecord, dict[str, Any]]],
    ) -> tuple[RawSourceRecord, RawPayment]:
        original, first_row = group[0]
        rows = [row for _, row in group]
        values = [_amount(row.get("valorFinal")) for row in rows]
        # The CSV and API share no line identifier. Identical rows cannot be safely added.
        confirmed_values = {value for value in values if value is not None and value.is_finite()}
        conflict = len(confirmed_values) != len(rows)
        source = RawSourceRecord.build(
            source=self.name,
            endpoint=original.endpoint,
            request_parameters={"codigoDocumento": document},
            raw_payload={
                "aggregation": "final_beneficiary_by_creditor_list",
                "rows": rows,
                "official_responses": list(dict.fromkeys(record.record_key for record, _ in group)),
                "issued_document_response": issued.source_record_ref,
            },
            http_status=original.http_status,
            collected_at=max(record.collected_at for record, _ in group),
        )
        identity = federal_payment_identity(document, cnpj, "final", creditor_list)
        assert identity is not None
        payment = RawPayment(
            source=self.name,
            source_url=issued.source_url,
            raw_payload=source.raw_payload,
            source_record_ref=source.record_key,
            external_id=identity,
            document_number=document,
            phase="Pagamento",
            supplier_cnpj=cnpj,
            supplier_name=first_row.get("nomeFavorecidoFinal"),
            agency_name=first_row.get("orgaoVinculado") or issued.agency_name,
            uasg=first_row.get("codigoUnidadeGestora") or issued.uasg,
            process_number=issued.process_number,
            payment_date=issued.payment_date,
            amount=None if conflict else sum(confirmed_values, Decimal(0)),
            federal_beneficiary_role="final",
            federal_creditor_list=creditor_list,
            federal_identity_conflict=conflict,
        )
        return source, payment

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
        queried_phase: int | None = None,
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
        phase_code = self._phase_code(str(phase)) if phase else queried_phase
        federal = phase_code == 3 and federal_payment_identity(document, supplier, "issued")
        return RawPayment(
            source=self.name,
            source_url=(
                f"https://portaldatransparencia.gov.br/despesas/pagamento/{siafi_payment_document(document)}"
                if federal
                else source_url
            ),
            raw_payload=row,
            external_id=external_id,
            source_record_ref=source_record_ref,
            agency_name=row.get("orgao") or row.get("orgaoSuperior"),
            uasg=uasg,
            process_number=row.get("numeroProcesso"),
            payment_date=payment_date,
            amount=amount,
            phase=str(phase) if phase else ("Pagamento" if federal else None),
            document_number=document or None,
            federal_beneficiary_role="issued" if federal else None,
            federal_intermediary=_intermediary_marker(row) if federal else None,
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
