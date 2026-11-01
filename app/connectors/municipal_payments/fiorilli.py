"""Fiorilli/SCPI open-data JSON adapter used by several Maranhão portals."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import httpx

from app.config import Settings, get_settings
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawPayment,
    RawSourceRecord,
)
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorDecodeError,
    ConnectorHTTPError,
)
from app.connectors.municipal_payments.base import (
    MunicipalSource,
    clean_text,
    parse_brazilian_date,
    parse_brazilian_money,
    source_name,
    source_url,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

_COMMON_PARAMS: dict[str, str] = {
    "DiaInicioPeriodo": "01",
    "MesInicialPeriodo": "01",
    "DiaFinalPeriodo": "31",
    "MesFinalPeriodo": "12",
    "MostraDadosConsolidado": "False",
}


class FiorilliPaymentConnector:
    """Read the Fiorilli ``VersaoJson/Despesas`` endpoints.

    The list endpoint is an official aggregate per commitment (empenho).  The
    optional detail endpoint returns individual payment/liquidation rows.  The
    adapter never turns an aggregate into a fake payment date: aggregate rows
    keep ``payment_date=None`` and an explicit status.
    """

    def __init__(
        self,
        source: MunicipalSource,
        settings: Settings | None = None,
        *,
        http_client: AsyncHTTPClient | None = None,
    ) -> None:
        self.source = source
        self.settings = settings or get_settings()
        self._http = http_client or AsyncHTTPClient(
            self.settings,
            max_retries=min(self.settings.http_max_retries, 2),
            public_only=True,
            allow_http=self.settings.allow_insecure_http_sources,
        )

    @property
    def name(self) -> str:
        return source_name(self.source)

    @property
    def endpoint(self) -> str:
        return f"{self.source.base_url.rstrip('/')}/VersaoJson/Despesas/"

    def _params(self, year: int, listing: str) -> dict[str, str]:
        return {
            **_COMMON_PARAMS,
            "ConectarExercicio": str(year),
            "Exercicio": str(year),
            "Ano": str(year),
            "Empresa": self.source.entity_code,
            "Listagem": listing,
        }

    def _list_params(self, year: int) -> dict[str, str]:
        return {
            **self._params(year, "DespesasGerais"),
            "MostrarFornecedor": "True",
            "MostrarCNPJFornecedor": "True",
            "ApenasIDEmpenho": "False",
            "UFParaFiltroCOVID": "",
        }

    def _detail_params(self, year: int, row: dict[str, Any]) -> dict[str, str]:
        return {
            **self._params(year, "EmpenhosDespesas_Pago_PorNumeroEmpenho"),
            "intNumeroEmpenho": str(clean_text(row.get("CODIGO")) or ""),
            "strTipoEmpenho": str(clean_text(row.get("TPEM")) or ""),
            "IDButton": "lnkDespesasPor_NotaEmpenho",
            "MostrarFornecedor": "True",
        }

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]:
        if year < 2000 or year > 2100:
            return self._error(
                DataAvailability.NOT_SUPPORTED,
                "ano invalido para a fonte municipal",
                http_status=400,
            )
        normalized_cnpj = normalize_cnpj(company_cnpj) if company_cnpj else None
        if normalized_cnpj and not is_valid_cnpj(normalized_cnpj):
            return self._error(
                DataAvailability.NOT_SUPPORTED,
                "CNPJ do fornecedor invalido",
                http_status=400,
            )

        params = self._list_params(year)
        url = self.endpoint
        try:
            response = await self._http.request_json("GET", url, params=params)
        except ConnectorHTTPError as exc:
            return self._http_error(exc, params=params)
        except (ConnectorDecodeError, httpx.TransportError, TimeoutError) as exc:
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                f"{self.source.name}: falha de transporte/JSON ({type(exc).__name__})",
                http_status=None,
            )

        if not isinstance(response.payload, list):
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                f"{self.source.name}: resposta JSON inesperada (nao e uma lista)",
                http_status=response.status_code,
                params=params,
                raw_payload=response.payload,
            )

        if any(not isinstance(row, dict) for row in response.payload):
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                "coleção contém linha inválida",
                http_status=response.status_code,
                params=params,
                raw_payload=response.payload,
            )
        rows = response.payload
        if any(
            not row.get("CODIGO") or parse_brazilian_money(row.get("PAGO")) is None for row in rows
        ):
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                "despesa sem identificador ou valor pago reconhecível",
                http_status=response.status_code,
                params=params,
                raw_payload=response.payload,
            )
        if self.source.filter_client_entity:
            if any(row.get("EMPRESA") is None for row in rows):
                return self._error(
                    DataAvailability.TEMPORARY_ERROR,
                    "despesa sem entidade; filtro municipal não pode ser confirmado",
                    http_status=response.status_code,
                    params=params,
                    raw_payload=response.payload,
                )
            rows = [
                row
                for row in rows
                if str(clean_text(row.get("EMPRESA")) or "") == self.source.entity_code
            ]
        if normalized_cnpj:
            rows = [row for row in rows if self._row_cnpj(row) == normalized_cnpj]

        commitments = [row for row in rows if (parse_brazilian_money(row.get("PAGO")) or 0) > 0]
        if not commitments:
            raw = self._raw_record(url, params, response.payload, response.status_code)
            return ConnectorResult(
                data=[],
                availability=DataAvailability.EMPTY,
                source_urls=[source_url(url, params)],
                raw_records=[raw],
                diagnostic=(
                    f"{self.source.name}: nenhuma despesa paga para "
                    f"{normalized_cnpj or 'o municipio'} em {year}"
                ),
            )

        payments: list[RawPayment] = []
        list_raw = self._raw_record(url, params, response.payload, response.status_code)
        list_ref = list_raw.record_key
        raw_records = [list_raw]
        diagnostics: list[str] = []
        replay = getattr(self._http, "collection_resume", None)
        known_details = (
            sum(
                1
                for raw in replay.cache.values()
                if raw.request_parameters.get("Listagem")
                == "EmpenhosDespesas_Pago_PorNumeroEmpenho"
            )
            if replay
            else 0
        )
        detail_limit = (
            min(self.settings.municipal_payment_detail_limit + known_details, len(commitments))
            if include_details
            else 0
        )
        detailed = 0
        for position, row in enumerate(commitments):
            if include_details and position < detail_limit:
                detail = await self._fetch_detail(year, row)
                if detail is not None:
                    detail_rows, detail_raw, detail_diagnostic = detail
                    detail_ref = list_ref
                    if detail_raw is not None:
                        raw_records.append(detail_raw)
                        detail_ref = detail_raw.record_key
                    mapped = [
                        payment
                        for detail_row in detail_rows
                        if (
                            payment := self._map_detail(
                                detail_row,
                                parent=row,
                                year=year,
                                source_url_value=source_url(url, self._detail_params(year, row)),
                                source_record_ref=detail_ref,
                            )
                        )
                        is not None
                    ]
                    expected_amount = parse_brazilian_money(row.get("PAGO"))
                    complete = (
                        bool(mapped)
                        and len(mapped) == len(detail_rows)
                        and self._commitment_external_id(year, row) is not None
                        and len({payment.external_id for payment in mapped}) == len(mapped)
                        and all(payment.payment_date is not None for payment in mapped)
                        and sum((payment.amount or 0) for payment in mapped) == expected_amount
                        and not detail_diagnostic
                    )
                    if complete:
                        for payment in mapped:
                            payment.commitment_details_complete = True
                        payments.extend(mapped)
                        detailed += 1
                        continue
                    self._note(diagnostics, "detalhe incompleto ou divergente; agregado preservado")
                    if detail_diagnostic:
                        self._note(diagnostics, detail_diagnostic)
                else:
                    self._note(
                        diagnostics,
                        f"{self.source.name}: detalhe indisponivel para empenho "
                        f"{clean_text(row.get('CODIGO')) or '?'}",
                    )
            aggregate = self._map_aggregate(
                row,
                year=year,
                source_url_value=source_url(url, params),
                source_record_ref=list_ref,
            )
            if aggregate is not None:
                payments.append(aggregate)

        if include_details and len(commitments) > detail_limit:
            diagnostics.append(
                f"{self.source.name}: detalhe limitado a {detail_limit} de "
                f"{len(commitments)} empenho(s); os demais ficaram como agregado"
            )

        max_records = self.settings.municipal_payment_max_records
        if max_records and len(payments) > max_records:
            diagnostics.append(
                f"{self.source.name}: limitado a {max_records} pagamento(s) "
                f"({len(payments)} encontrados)"
            )
            retained = payments[:max_records]
            cut_groups = {payment.commitment_external_id for payment in payments[max_records:]}
            for payment in retained:
                if payment.commitment_external_id in cut_groups:
                    payment.commitment_details_complete = False
            replacements = {
                self._commitment_external_id(year, row): self._map_aggregate(
                    row,
                    year=year,
                    source_url_value=source_url(url, params),
                    source_record_ref=list_ref,
                )
                for row in commitments
            }
            payments = []
            replaced: set[str | None] = set()
            for payment in retained:
                group = payment.commitment_external_id
                if group in cut_groups and group is not None:
                    replacement = replacements.get(group)
                    if group not in replaced and replacement is not None:
                        payments.append(replacement)
                        replaced.add(group)
                else:
                    payments.append(payment)

        partial = bool(diagnostics)

        if not payments:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.EMPTY,
                source_urls=[source_url(url, params)],
                raw_records=raw_records,
                diagnostic="; ".join(diagnostics) or None,
            )

        return ConnectorResult(
            data=payments,
            availability=DataAvailability.AVAILABLE,
            source_urls=[source_url(url, params)],
            raw_records=raw_records,
            diagnostic=(
                f"{self.source.name}: {len(payments)} pagamento(s); "
                f"{detailed} empenho(s) detalhado(s)"
                + (f"; {'; '.join(diagnostics)}" if diagnostics else "")
            ),
            partial=partial,
            pagination=PaginationInfo(
                fetched_pages=detail_limit,
                total_pages=len(commitments),
                pages_remaining=max(len(commitments) - detail_limit, 0),
                truncated=detail_limit < len(commitments),
            )
            if include_details
            else None,
        )

    async def _fetch_detail(
        self, year: int, row: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], RawSourceRecord | None, str | None] | None:
        params = self._detail_params(year, row)
        try:
            response = await self._http.request_json("GET", self.endpoint, params=params)
        except (ConnectorHTTPError, ConnectorDecodeError, httpx.TransportError, TimeoutError):
            return None
        if not isinstance(response.payload, list):
            return (
                [],
                self._raw_record(self.endpoint, params, response.payload, response.status_code),
                f"{self.source.name}: detalhe retornou formato inesperado",
            )
        invalid = any(not isinstance(item, dict) for item in response.payload)
        return (
            [item for item in response.payload if isinstance(item, dict)],
            self._raw_record(self.endpoint, params, response.payload, response.status_code),
            "detalhe contém linha inválida" if invalid else None,
        )

    @staticmethod
    def _note(diagnostics: list[str], message: str) -> None:
        if len(diagnostics) < 5:
            diagnostics.append(message)
        elif len(diagnostics) == 5:
            diagnostics.append("outros detalhes falharam; consulte os registros brutos")

    def _row_cnpj(self, row: dict[str, Any]) -> str | None:
        value = normalize_cnpj(str(row.get("CPFFORMATADO") or ""))
        return value if is_valid_cnpj(value) else None

    @staticmethod
    def _commitment_key(value: str) -> str:
        return re.sub(r"\D", "", value).lstrip("0")

    def _external_prefix(self) -> str:
        if self.source.entity_code == "1":
            return self.source.id
        return f"{self.source.id}:e{self.source.entity_code}"

    def _commitment_external_id(self, year: int, row: dict[str, Any]) -> str | None:
        key = clean_text(row.get("PKEMP"))
        return f"{self._external_prefix()}:{year}:{key}" if key else None

    def _map_aggregate(
        self,
        row: dict[str, Any],
        *,
        year: int,
        source_url_value: str,
        source_record_ref: str,
    ) -> RawPayment | None:
        amount = parse_brazilian_money(row.get("PAGO"))
        if amount is None or amount <= 0:
            return None
        commitment = clean_text(row.get("CODIGO"))
        key = clean_text(row.get("PKEMP")) or commitment or "sem-chave"
        return RawPayment(
            source=self.name,
            source_url=source_url_value,
            raw_payload=row,
            source_record_ref=source_record_ref,
            external_id=f"{self._external_prefix()}:{year}:{key}:agregado",
            agency_name=clean_text(row.get("NOMEEMPRESA")),
            payment_date=None,
            amount=amount,
            phase="pago_agregado",
            document_number=commitment,
            commitment_number=commitment,
            commitment_external_id=self._commitment_external_id(year, row),
            status="agregado_por_empenho",
            supplier_cnpj=self._row_cnpj(row),
            supplier_name=clean_text(row.get("NOMEFOR")),
        )

    def _map_detail(
        self,
        row: dict[str, Any],
        *,
        parent: dict[str, Any],
        year: int,
        source_url_value: str,
        source_record_ref: str,
    ) -> RawPayment | None:
        amount = parse_brazilian_money(row.get("VALOR"))
        if amount is None or amount <= 0:
            return None
        detail_cnpj = self._row_cnpj(row)
        parent_cnpj = self._row_cnpj(parent)
        if detail_cnpj and parent_cnpj and detail_cnpj != parent_cnpj:
            return None
        detail_commitment = clean_text(row.get("EMPENHO"))
        parent_commitment = clean_text(parent.get("CODIGO"))
        if (
            row.get("PKEMP") is not None
            and parent.get("PKEMP") is not None
            and str(row["PKEMP"]) != str(parent["PKEMP"])
        ):
            return None
        if (
            detail_commitment
            and parent_commitment
            and self._commitment_key(detail_commitment) != self._commitment_key(parent_commitment)
        ):
            return None
        commitment = detail_commitment or parent_commitment
        liquidation = clean_text(row.get("LIQ")) or "?"
        installment = clean_text(row.get("PAR")) or "?"
        if liquidation == "?" or installment == "?":
            return None
        key = (
            f"{clean_text(row.get('PKEMP')) or clean_text(parent.get('PKEMP')) or 'sem-chave'}"
            f":{commitment or 'sem-empenho'}:{liquidation}:{installment}"
        )
        return RawPayment(
            source=self.name,
            source_url=source_url_value,
            raw_payload={"commitment": parent, "payment": row},
            source_record_ref=source_record_ref,
            external_id=f"{self._external_prefix()}:{year}:{key}:detalhe",
            agency_name=clean_text(parent.get("NOMEEMPRESA")),
            payment_date=parse_brazilian_date(row.get("DATAE")),
            amount=amount,
            phase="pago",
            document_number=f"{liquidation}/{installment}",
            commitment_number=commitment,
            commitment_external_id=self._commitment_external_id(year, parent),
            status="detalhe_empenho",
            supplier_cnpj=self._row_cnpj(parent),
            supplier_name=clean_text(row.get("FORNECEDOR")) or clean_text(parent.get("NOMEFOR")),
        )

    def _raw_record(
        self,
        url: str,
        params: dict[str, Any],
        payload: Any,
        http_status: int | None,
    ) -> RawSourceRecord:
        return RawSourceRecord.build(
            source=self.name,
            endpoint=url,
            request_parameters=params,
            raw_payload=payload,
            http_status=http_status or 0,
            collected_at=datetime.now(UTC),
        )

    def _http_error(
        self, exc: ConnectorHTTPError, *, params: dict[str, Any] | None = None
    ) -> ConnectorResult[list[RawPayment]]:
        if exc.status_code in {401, 403}:
            availability = DataAvailability.ACCESS_RESTRICTED
            message = f"{self.source.name}: acesso restrito (HTTP {exc.status_code})"
        elif exc.status_code in {400, 404, 405, 422}:
            availability = DataAvailability.NOT_SUPPORTED
            message = f"{self.source.name}: endpoint nao suportado (HTTP {exc.status_code})"
        else:
            availability = DataAvailability.TEMPORARY_ERROR
            message = f"{self.source.name}: fonte indisponivel (HTTP {exc.status_code})"
        return self._error(
            availability,
            message,
            http_status=exc.status_code,
            body=exc.body,
            params=params,
        )

    def _error(
        self,
        availability: DataAvailability,
        diagnostic: str,
        *,
        http_status: int | None,
        body: str = "",
        params: dict[str, Any] | None = None,
        raw_payload: Any = None,
    ) -> ConnectorResult[list[RawPayment]]:
        raw = self._raw_record(
            self.endpoint,
            params or {},
            raw_payload if raw_payload is not None else {"error": diagnostic, "body": body[:2000]},
            http_status,
        )
        return ConnectorResult(
            data=[],
            availability=availability,
            source_urls=[source_url(self.endpoint, params)],
            raw_records=[raw],
            diagnostic=diagnostic,
        )

    async def aclose(self) -> None:
        await self._http.aclose()
