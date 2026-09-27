"""Connector for the current Compras.gov.br Dados Abertos API."""

from __future__ import annotations

import re
from typing import Any

import httpx

from app.connectors._common import (
    contains_text,
    failed_result,
    is_corporate_identifier,
    join_url,
    list_payload,
    normalized_identifier,
    normalized_text,
    runtime_settings,
    source_record,
    to_datetime,
    to_decimal,
    to_int,
    unique_urls,
)
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    ProcurementFilters,
    RawDocument,
    RawEvent,
    RawParticipant,
    RawProcurement,
    RawProcurementDetail,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorDecodeError,
    ConnectorHTTPError,
    setting,
)

# These are Compras.gov.br modality codes, not PNCP modalidadeId values.
COMPRAS_MODALITY_CODES: dict[str, int] = {
    "convite": 1,
    "tomada_de_precos": 2,
    "concorrencia": 3,
    "concorrencia_eletronica": 3,
    "concorrencia_presencial": 3,
    "pregao": 5,
    "pregao_eletronico": 5,
    "pregao_presencial": 5,
    "dispensa": 6,
    "dispensa_eletronica": 6,
    "inexigibilidade": 7,
    "concurso": 20,
}

_PNCP_CONTROL_NUMBER = re.compile(r"^[A-Z0-9]{14}-\d-\d{1,9}/\d{4}$", re.IGNORECASE)
_HTTP_EXCEPTIONS = (
    ConnectorHTTPError,
    ConnectorDecodeError,
    httpx.TimeoutException,
    httpx.TransportError,
)


def _canonical_modality(value: str) -> str:
    return normalized_text(value).replace("-", "_").replace(" ", "_")


class ComprasGovConnector:
    """Read-only connector for Lei 14.133 procurement modules 1, 2 and 3."""

    name = "compras_gov"

    PROCUREMENT_LIST_PATH = "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
    PROCUREMENT_ID_PATH = "modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id"
    ITEM_ID_PATH = "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
    RESULT_ID_PATH = "modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"

    def __init__(
        self,
        settings: object | None = None,
        *,
        http_client: AsyncHTTPClient | None = None,
    ) -> None:
        self.settings = runtime_settings(settings)
        self.base_url = str(
            setting(
                self.settings,
                "compras_gov_base_url",
                "https://dadosabertos.compras.gov.br",
            )
        ).rstrip("/")
        self.timezone = str(setting(self.settings, "timezone", "America/Sao_Paulo"))
        self._http = http_client or AsyncHTTPClient(self.settings)
        self._owns_http = http_client is None

    async def discover_procurements(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawProcurement]]:
        if _canonical_modality(filters.discovery_kind) not in {"publication", "publicacao"}:
            raise ValueError("Compras.gov.br discovery currently supports publication dates")
        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        modality_values = filters.modalities or self._configured_modalities()
        modalities = list(dict.fromkeys(self._modality_code(value) for value in modality_values))
        endpoint = join_url(self.base_url, self.PROCUREMENT_LIST_PATH)
        page_size = min(max(filters.page_size or 500, 1), 500)
        records: list[RawSourceRecord] = []
        procurements: list[RawProcurement] = []
        errors: list[Exception] = []
        fetched_pages = 0
        total_pages = 0
        total_records = 0
        truncated = False

        for modality in modalities:
            page = 1
            while True:
                params: dict[str, Any] = {
                    "pagina": page,
                    "tamanhoPagina": page_size,
                    "dataPublicacaoPncpInicial": start.isoformat(),
                    "dataPublicacaoPncpFinal": end.isoformat(),
                    "codigoModalidade": modality,
                    "unidadeOrgaoCodigoUnidade": filters.unit_code,
                    "orgaoEntidadeCnpj": filters.agency_cnpj,
                    "unidadeOrgaoCodigoIbge": filters.municipality_code,
                    "unidadeOrgaoUfSigla": filters.uf,
                }
                try:
                    response = await self._http.request_json("GET", endpoint, params=params)
                except _HTTP_EXCEPTIONS as exc:
                    errors.append(exc)
                    break
                records.append(source_record(self.name, response))
                payload = response.payload if isinstance(response.payload, dict) else {}
                rows = list_payload(payload, "resultado")
                for row in rows:
                    if self._matches_client_filters(row, filters):
                        procurements.append(self._map_procurement(row, response.url))

                fetched_pages += 1
                observed_total_pages = to_int(payload.get("totalPaginas")) or page
                total_pages += observed_total_pages if page == 1 else 0
                if page == 1:
                    total_records += to_int(payload.get("totalRegistros")) or len(rows)
                has_more = page < observed_total_pages
                if filters.max_pages is not None and page >= filters.max_pages and has_more:
                    truncated = True
                    break
                if not has_more:
                    break
                page += 1

        procurements = self._deduplicate(procurements)
        pagination = PaginationInfo(
            page=1,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            pages_remaining=max(total_pages - fetched_pages, 0),
            fetched_pages=fetched_pages,
            truncated=truncated,
        )
        if procurements:
            return ConnectorResult(
                data=procurements,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    "partial result: one or more pages were not collected"
                    if errors or truncated
                    else None
                ),
            )
        if errors:
            result: ConnectorResult[list[RawProcurement]] = failed_result([], errors[0])
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
        return ConnectorResult(
            data=[],
            availability=DataAvailability.EMPTY,
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="no procurements matched the official response and client-side filters",
        )

    async def fetch_procurement(
        self, external_id: str
    ) -> ConnectorResult[RawProcurementDetail | None]:
        endpoint = join_url(self.base_url, self.PROCUREMENT_ID_PATH)
        params = self._identifier_params(external_id)
        try:
            response = await self._http.request_json("GET", endpoint, params=params)
        except _HTTP_EXCEPTIONS as exc:
            return failed_result(None, exc)
        records = [source_record(self.name, response)]
        rows = list_payload(response.payload, "resultado")
        if not rows:
            return ConnectorResult(
                data=None,
                availability=DataAvailability.NOT_PUBLISHED,
                source_urls=[response.url],
                raw_records=records,
            )
        return ConnectorResult(
            data=self._map_procurement(rows[0], response.url, detail=True),
            availability=DataAvailability.AVAILABLE,
            source_urls=[response.url],
            raw_records=records,
        )

    async def fetch_items(self, external_id: str) -> ConnectorResult[list[RawProcurementItem]]:
        endpoint = join_url(self.base_url, self.ITEM_ID_PATH)
        try:
            response = await self._http.request_json(
                "GET", endpoint, params=self._identifier_params(external_id)
            )
        except _HTTP_EXCEPTIONS as exc:
            return failed_result([], exc)
        records = [source_record(self.name, response)]
        rows = list_payload(response.payload, "resultado")
        items = [self._map_item(row, external_id, response.url) for row in rows]
        return ConnectorResult(
            data=items,
            availability=DataAvailability.AVAILABLE if items else DataAvailability.EMPTY,
            source_urls=[response.url],
            raw_records=records,
            pagination=self._pagination(response.payload, len(items)),
        )

    async def fetch_documents(self, external_id: str) -> ConnectorResult[list[RawDocument]]:
        del external_id
        return ConnectorResult(
            data=[],
            availability=DataAvailability.NOT_SUPPORTED,
            diagnostic="the audited Compras.gov.br OpenAPI has no public document endpoint",
        )

    async def fetch_results(self, external_id: str) -> ConnectorResult[list[RawResult]]:
        endpoint = join_url(self.base_url, self.RESULT_ID_PATH)
        try:
            response = await self._http.request_json(
                "GET", endpoint, params=self._identifier_params(external_id)
            )
        except _HTTP_EXCEPTIONS as exc:
            return failed_result([], exc)
        records = [source_record(self.name, response)]
        rows = list_payload(response.payload, "resultado")
        results = [self._map_result(row, external_id, response.url) for row in rows]
        return ConnectorResult(
            data=results,
            availability=(
                DataAvailability.AVAILABLE if results else DataAvailability.NOT_PUBLISHED
            ),
            source_urls=[response.url],
            raw_records=records,
            pagination=self._pagination(response.payload, len(results)),
        )

    async def fetch_participants(self, external_id: str) -> ConnectorResult[list[RawParticipant]]:
        """Expose awarded PJ suppliers while clearly avoiding bidder inference."""

        result = await self.fetch_results(external_id)
        participants = [
            RawParticipant(
                source=self.name,
                source_url=item.source_url,
                raw_payload=item.raw_payload,
                external_id=f"{item.external_id}:participant",
                procurement_external_id=external_id,
                item_external_id=item.item_external_id,
                company_cnpj=normalized_identifier(item.supplier_cnpj) or "",
                company_name=item.supplier_name,
                participation_role="awarded",
                final_value=item.total_value,
                rank=item.rank,
                status=item.result_status,
                confidence=1.0,
            )
            for item in result.data
            if not item.is_cancelled
            and is_corporate_identifier(item.supplier_cnpj, item.person_type)
        ]
        availability = result.availability
        if result.is_available and not participants:
            availability = DataAvailability.NOT_PUBLISHED
        return ConnectorResult(
            data=participants,
            availability=DataAvailability.AVAILABLE if participants else availability,
            source_urls=result.source_urls,
            raw_records=result.raw_records,
            pagination=result.pagination,
            diagnostic=(
                result.diagnostic
                or "only awarded legal entities are exposed; this is not a complete bidder list"
            ),
        )

    async def fetch_events(self, external_id: str) -> ConnectorResult[list[RawEvent]]:
        del external_id
        return ConnectorResult(
            data=[],
            availability=DataAvailability.NOT_SUPPORTED,
            diagnostic=(
                "the audited Compras.gov.br OpenAPI exposes no structured session, "
                "disqualification, eligibility or appeal events"
            ),
        )

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    def _configured_modalities(self) -> list[str | int]:
        configured = setting(self.settings, "default_modalities", ["pregao_eletronico"])
        if isinstance(configured, str):
            return [value.strip() for value in configured.split(",") if value.strip()]
        return list(configured)

    @staticmethod
    def _modality_code(value: str | int) -> int:
        if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
            code = int(value)
            if code < 1:
                raise ValueError("Compras.gov.br modality code must be positive")
            return code
        key = _canonical_modality(str(value))
        try:
            return COMPRAS_MODALITY_CODES[key]
        except KeyError as exc:
            raise ValueError(f"unknown Compras.gov.br modality: {value}") from exc

    @staticmethod
    def _identifier_params(external_id: str) -> dict[str, str]:
        value = external_id.strip()
        if not value:
            raise ValueError("external_id cannot be empty")
        if _PNCP_CONTROL_NUMBER.fullmatch(value):
            return {"tipo": "numeroControlePNCPCompra", "codigo": value.upper()}
        return {"tipo": "idCompra", "codigo": value}

    def _matches_client_filters(self, row: dict[str, Any], filters: ProcurementFilters) -> bool:
        if filters.agency and not contains_text(
            [row.get("orgaoEntidadeRazaoSocial"), row.get("unidadeOrgaoNomeUnidade")],
            filters.agency,
        ):
            return False
        if filters.municipality and not contains_text(
            [row.get("unidadeOrgaoMunicipioNome")], filters.municipality
        ):
            return False
        return not filters.keyword or contains_text(
            [row.get("objetoCompra"), row.get("processo"), row.get("informacaoComplementar")],
            filters.keyword,
        )

    def _map_procurement(
        self,
        row: dict[str, Any],
        source_url: str,
        *,
        detail: bool = False,
    ) -> RawProcurement | RawProcurementDetail:
        external_id = str(row.get("idCompra") or row.get("numeroControlePNCP"))
        control = row.get("numeroControlePNCP")
        modality = row.get("modalidadeNome")
        purchase_number = row.get("numeroCompra")
        model_type = RawProcurementDetail if detail else RawProcurement
        values: dict[str, Any] = {
            "source": self.name,
            "source_url": source_url,
            "raw_payload": row,
            "external_id": external_id,
            "pncp_control_number": control,
            "uasg": (
                str(row.get("unidadeOrgaoCodigoUnidade"))
                if row.get("unidadeOrgaoCodigoUnidade") is not None
                else None
            ),
            "purchase_number": str(purchase_number) if purchase_number is not None else None,
            "purchase_year": to_int(row.get("anoCompraPncp")),
            "process_number": row.get("processo"),
            "modality": modality,
            "modality_code": row.get("codigoModalidade"),
            "title": " ".join(
                part for part in (str(modality or ""), str(purchase_number or "")) if part
            )
            or None,
            "object_description": row.get("objetoCompra"),
            "agency_name": row.get("orgaoEntidadeRazaoSocial")
            or row.get("unidadeOrgaoNomeUnidade"),
            "agency_cnpj": normalized_identifier(row.get("orgaoEntidadeCnpj")),
            "government_sphere": row.get("orgaoEntidadeEsferaId"),
            "government_branch": row.get("orgaoEntidadePoderId"),
            "uf": row.get("unidadeOrgaoUfSigla"),
            "municipality": row.get("unidadeOrgaoMunicipioNome"),
            "municipality_code": (
                str(row.get("unidadeOrgaoCodigoIbge"))
                if row.get("unidadeOrgaoCodigoIbge") is not None
                else None
            ),
            "estimated_value": to_decimal(row.get("valorTotalEstimado")),
            "homologated_value": to_decimal(row.get("valorTotalHomologado")),
            "proposal_start_at": to_datetime(row.get("dataAberturaPropostaPncp"), self.timezone),
            "proposal_end_at": to_datetime(row.get("dataEncerramentoPropostaPncp"), self.timezone),
            "publication_at": to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            "last_source_update_at": to_datetime(row.get("dataAtualizacaoPncp"), self.timezone),
            "status": row.get("situacaoCompraNomePncp"),
        }
        if detail:
            values["additional_data"] = {
                "information": row.get("informacaoComplementar"),
                "has_results": row.get("existeResultado"),
                "excluded": row.get("contratacaoExcluida"),
                "pncp_modality_code": row.get("modalidadeIdPncp"),
            }
        return model_type(**values)

    def _map_item(
        self, row: dict[str, Any], procurement_external_id: str, source_url: str
    ) -> RawProcurementItem:
        item_external_id = row.get("idCompraItem")
        item_number = row.get("numeroItemPncp", row.get("numeroItemCompra"))
        return RawProcurementItem(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=str(item_external_id or f"{procurement_external_id}:item:{item_number}"),
            procurement_external_id=procurement_external_id,
            item_number=str(item_number) if item_number is not None else None,
            description=row.get("descricaoResumida") or row.get("descricaodetalhada"),
            detailed_description=row.get("descricaodetalhada"),
            quantity=to_decimal(row.get("quantidade")),
            unit=row.get("unidadeMedida"),
            estimated_unit_value=to_decimal(row.get("valorUnitarioEstimado")),
            estimated_total_value=to_decimal(row.get("valorTotal")),
            result_status=row.get("situacaoCompraItemNome"),
            has_result=row.get("temResultado"),
        )

    def _map_result(
        self, row: dict[str, Any], procurement_external_id: str, source_url: str
    ) -> RawResult:
        item_number = row.get("numeroItemPncp")
        item_external_id = row.get("idCompraItem")
        sequence = row.get("sequencialResultado", 1)
        status = row.get("situacaoCompraItemResultadoNome")
        cancellation_reason = row.get("motivoCancelamento")
        is_cancelled = (
            bool(row.get("dataCancelamentoPncp"))
            or contains_text([status], "cancel")
            or contains_text([status], "exclu")
            or contains_text([status], "anulad")
        )
        return RawResult(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=f"{item_external_id or procurement_external_id}:result:{sequence}",
            procurement_external_id=procurement_external_id,
            item_external_id=(
                str(item_external_id)
                if item_external_id is not None
                else f"{procurement_external_id}:item:{item_number}"
            ),
            item_number=str(item_number) if item_number is not None else None,
            supplier_cnpj=normalized_identifier(row.get("niFornecedor")),
            supplier_name=row.get("nomeRazaoSocialFornecedor"),
            person_type=row.get("tipoPessoa") or row.get("tipoPessoaId"),
            role="awarded",
            quantity=to_decimal(row.get("quantidadeHomologada")),
            unit_value=to_decimal(row.get("valorUnitarioHomologado")),
            total_value=to_decimal(row.get("valorTotalHomologado")),
            rank=to_int(row.get("ordemClassificacaoSrp")),
            result_status=status,
            result_at=to_datetime(row.get("dataResultadoPncp"), self.timezone),
            is_cancelled=is_cancelled,
            cancellation_reason=cancellation_reason,
        )

    @staticmethod
    def _pagination(payload: Any, item_count: int) -> PaginationInfo:
        if not isinstance(payload, dict):
            return PaginationInfo(total_records=item_count, total_pages=1, fetched_pages=1)
        return PaginationInfo(
            page=1,
            total_records=to_int(payload.get("totalRegistros")) or item_count,
            total_pages=to_int(payload.get("totalPaginas")) or (1 if item_count else 0),
            pages_remaining=to_int(payload.get("paginasRestantes")) or 0,
            fetched_pages=1,
        )

    @staticmethod
    def _deduplicate(rows: list[RawProcurement]) -> list[RawProcurement]:
        unique: dict[tuple[Any, ...], RawProcurement] = {}
        for row in rows:
            if row.pncp_control_number:
                key: tuple[Any, ...] = ("pncp", row.pncp_control_number.upper())
            elif all(
                value is not None
                for value in (
                    row.agency_cnpj,
                    row.uasg,
                    row.purchase_number,
                    row.purchase_year,
                    row.modality,
                )
            ):
                key = (
                    "strong",
                    row.agency_cnpj,
                    row.uasg,
                    row.purchase_number,
                    row.purchase_year,
                    normalized_text(row.modality),
                )
            else:
                key = ("source", row.external_id)
            unique.setdefault(key, row)
        return list(unique.values())
