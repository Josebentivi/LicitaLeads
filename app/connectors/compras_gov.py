"""Connector for the current Compras.gov.br Dados Abertos API."""

from __future__ import annotations

import re
from typing import Any

import httpx

from app.connectors._common import (
    combine_discoveries,
    contains_text,
    failed_result,
    is_corporate_identifier,
    join_url,
    list_payload,
    nonnegative_decimal,
    normalized_identifier,
    normalized_text,
    observed_pages,
    positive_int,
    repeated_page,
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
    RawContract,
    RawContractItem,
    RawContractTerm,
    RawDocument,
    RawEvent,
    RawParticipant,
    RawPriceRegistry,
    RawPriceRegistryAdhesion,
    RawPriceRegistryCommitment,
    RawPriceRegistryItem,
    RawPriceRegistryUnit,
    RawProcurement,
    RawProcurementDetail,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.connectors.continuation import page_budget_exhausted
from app.connectors.discovery_budget import DiscoveryBudget, DiscoveryLimitReached
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    setting,
)
from app.connectors.page_window import PageWindow
from app.services.parallel import bounded_map

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
    ConnectorContractError,
    ConnectorDecodeError,
    httpx.TimeoutException,
    httpx.TransportError,
)
_MAX_DETAIL_PAGES = 500


def _canonical_modality(value: str) -> str:
    return normalized_text(value).replace("-", "_").replace(" ", "_")


class ComprasGovConnector:
    """Read-only connector for Lei 14.133 procurement modules 1, 2 and 3."""

    name = "compras_gov"
    adapter_version = 2

    PROCUREMENT_LIST_PATH = "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
    PROCUREMENT_ID_PATH = "modulo-contratacoes/1.1_consultarContratacoes_PNCP_14133_Id"
    ITEM_ID_PATH = "modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
    RESULT_ID_PATH = "modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
    ARP_LIST_PATH = "modulo-arp/1_consultarARP"
    ARP_ITEM_ID_PATH = "modulo-arp/2.1_consultarARPItem_Id"
    ARP_UNITS_PATH = "modulo-arp/3_consultarUnidadesItem"
    ARP_COMMITMENTS_PATH = "modulo-arp/4_consultarEmpenhosSaldoItem"
    ARP_ADHESIONS_PATH = "modulo-arp/5_consultarAdesoesItem"
    CONTRACT_LIST_PATH = "modulo-contratos/1_consultarContratos"
    CONTRACT_ITEM_ID_PATH = "modulo-contratos/2.1_consultarContratosItem_Id"

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
        self._arp_items: dict[str, tuple[list[str], bool]] = {}

    async def discover_procurements(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawProcurement]]:
        if _canonical_modality(filters.discovery_kind) not in {"publication", "publicacao"}:
            raise ValueError("Compras.gov.br discovery currently supports publication dates")
        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        modality_values = filters.modalities or self._configured_modalities()
        if len(modality_values) > 1:

            async def partition(modality: str | int) -> ConnectorResult[Any]:
                return await self.discover_procurements(
                    filters.model_copy(update={"modalities": [modality]})
                )

            stream = bounded_map(
                dict.fromkeys(modality_values),
                partition,
                capacity=min(4, int(setting(self.settings, "http_max_concurrency", 4))),
            )
            partitions = []
            try:
                async for _, partition_result in stream:
                    partitions.append(partition_result)
            finally:
                await stream.aclose()
            combined = combine_discoveries(partitions)
            combined.data = self._deduplicate(combined.data)
            return combined

        modalities = list(dict.fromkeys(self._modality_code(value) for value in modality_values))
        endpoint = join_url(self.base_url, self.PROCUREMENT_LIST_PATH)
        page_size = min(max(filters.page_size or 500, 10), 500)
        records: list[RawSourceRecord] = []
        procurements: list[RawProcurement] = []
        errors: list[Exception] = []
        fetched_pages = 0
        total_pages = 0
        total_records = 0
        truncated = False
        invalid_rows = 0

        for modality in modalities:
            page = 1
            collected_rows = 0
            observed_total_pages = 0
            async with PageWindow(
                concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
                max_pages=min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES),
            ) as window:
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
                        response = await window.request(
                            self._http,
                            endpoint,
                            params,
                            total_pages=observed_total_pages if page > 1 else 0,
                        )
                    except _HTTP_EXCEPTIONS as exc:
                        errors.append(exc)
                        break
                    record = source_record(self.name, response)
                    repeated = repeated_page(records, record)
                    records.append(record)
                    if repeated:
                        errors.append(
                            ConnectorContractError("official source repeated a discovery page")
                        )
                        break
                    payload = response.payload
                    try:
                        rows = list_payload(payload, "resultado", operation="Compras discovery")
                    except ConnectorContractError as exc:
                        errors.append(exc)
                        break
                    for row in rows:
                        if not self._has_procurement_identity(row):
                            invalid_rows += 1
                            continue
                        try:
                            if self._matches_client_filters(row, filters):
                                procurements.append(
                                    self._map_procurement(row, response.url).model_copy(
                                        update={"source_record_ref": record.record_key}
                                    )
                                )
                        except Exception:
                            invalid_rows += 1

                    fetched_pages += 1
                    try:
                        collected_rows += len(rows)
                        observed_total_pages = observed_pages(
                            payload,
                            page,
                            row_count=len(rows),
                            collected_rows=collected_rows,
                            page_size=page_size,
                        )
                    except ConnectorContractError as exc:
                        errors.append(exc)
                        break
                    total_pages += observed_total_pages if page == 1 else 0
                    if page == 1:
                        total_records += (
                            to_int(payload.get("totalRegistros"))
                            if isinstance(payload, dict)
                            else None
                        ) or len(rows)
                    has_more = page < observed_total_pages
                    if (
                        page_budget_exhausted(
                            self._http,
                            page,
                            min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES),
                            url=endpoint,
                            params=params,
                        )
                        and not window.has_pending
                        and has_more
                    ):
                        truncated = True
                        break
                    if not has_more:
                        break
                    page += 1
            records.extend(
                source_record(self.name, response) for response in window.unconsumed_responses()
            )

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
            partial = bool(errors or truncated or invalid_rows)
            return ConnectorResult(
                data=procurements,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    "partial result: one or more pages/rows were not usable" if partial else None
                ),
                partial=partial,
            )
        if errors:
            result: ConnectorResult[list[RawProcurement]] = failed_result([], errors[0])
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
        if invalid_rows:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.TEMPORARY_ERROR,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    "official response contained records without a usable procurement identity"
                ),
                partial=True,
            )
        return ConnectorResult(
            data=[],
            availability=(
                DataAvailability.TEMPORARY_ERROR
                if truncated or invalid_rows
                else DataAvailability.EMPTY
            ),
            partial=bool(truncated or invalid_rows),
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
        try:
            rows = list_payload(response.payload, "resultado")
            if any(not self._has_procurement_identity(row) for row in rows):
                raise ConnectorContractError("Compras detail without procurement identity")
        except ConnectorContractError as exc:
            failure: ConnectorResult[RawProcurementDetail | None] = failed_result(None, exc)
            failure.raw_records = records
            failure.source_urls = unique_urls(records)
            return failure
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
        rows, refs, records, pagination, error = await self._collect_result_pages(
            endpoint,
            self._identifier_params(external_id),
            operation="Compras items",
        )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawProcurementItem]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        items = [
            self._map_item(row, external_id, records_by_ref[ref].endpoint).model_copy(
                update={"source_record_ref": ref}
            )
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_item_identity(row)
        ]
        partial = error is not None or len(items) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=items,
            availability=(
                DataAvailability.AVAILABLE
                if items
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: item collection incomplete" if partial else None,
            partial=partial,
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
        rows, refs, records, pagination, error = await self._collect_result_pages(
            endpoint,
            self._identifier_params(external_id),
            operation="Compras results",
        )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawResult]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        results = [
            self._map_result(row, external_id, records_by_ref[ref].endpoint).model_copy(
                update={"source_record_ref": ref}
            )
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_result_identity(row)
        ]
        partial = error is not None or len(results) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=results,
            availability=(
                DataAvailability.AVAILABLE
                if results
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.NOT_PUBLISHED
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: result collection incomplete" if partial else None,
            partial=partial,
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

    async def discover_contracts(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawContract]]:
        """Collect federal contracts; the official module requires an agency code."""

        if not filters.agency_cnpj:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_SUPPORTED,
                diagnostic="Compras.gov.br contracts require an agency CNPJ",
            )
        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        endpoint = join_url(self.base_url, self.CONTRACT_LIST_PATH)
        page_size = min(max(filters.page_size or 500, 10), 500)
        records: list[RawSourceRecord] = []
        contracts: list[RawContract] = []
        errors: list[Exception] = []
        page = 1
        total_pages = 0
        total_records = 0
        fetched_pages = 0
        truncated = False
        invalid_rows = 0
        collected_rows = 0
        async with PageWindow(
            concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
            max_pages=min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES),
        ) as window:
            while True:
                params: dict[str, Any] = {
                    "pagina": page,
                    "tamanhoPagina": page_size,
                    "codigoOrgao": filters.agency_cnpj,
                    "dataVigenciaInicialMin": start.isoformat(),
                    "dataVigenciaInicialMax": end.isoformat(),
                }
                try:
                    response = await window.request(
                        self._http, endpoint, params, total_pages=total_pages or 0
                    )
                except _HTTP_EXCEPTIONS as exc:
                    errors.append(exc)
                    break
                record = source_record(self.name, response)
                repeated = repeated_page(records, record)
                records.append(record)
                if repeated:
                    errors.append(
                        ConnectorContractError("official source repeated a discovery page")
                    )
                    break
                payload = response.payload
                try:
                    rows = list_payload(
                        payload, "resultado", operation="Compras contract discovery"
                    )
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                invalid_rows += sum(not self._has_contract_identity(row) for row in rows)
                contracts.extend(
                    self._map_contract(row, response.url).model_copy(
                        update={"source_record_ref": record.record_key}
                    )
                    for row in rows
                    if self._has_contract_identity(row)
                )
                fetched_pages += 1
                try:
                    collected_rows += len(rows)
                    observed_total_pages = observed_pages(
                        payload,
                        page,
                        row_count=len(rows),
                        collected_rows=collected_rows,
                        page_size=page_size,
                    )
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                total_pages += observed_total_pages if page == 1 else 0
                if page == 1:
                    total_records += to_int(payload.get("totalRegistros")) or len(rows)
                has_more = page < observed_total_pages
                if (
                    page_budget_exhausted(
                        self._http,
                        page,
                        min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES),
                        url=endpoint,
                        params=params,
                    )
                    and not window.has_pending
                    and has_more
                ):
                    truncated = True
                    break
                if not has_more:
                    break
                page += 1
        records.extend(
            source_record(self.name, response) for response in window.unconsumed_responses()
        )
        pagination = PaginationInfo(
            page=1,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            pages_remaining=max(total_pages - fetched_pages, 0),
            fetched_pages=fetched_pages,
            truncated=truncated,
        )
        if contracts:
            return ConnectorResult(
                data=contracts,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    "partial result: one or more pages were not collected"
                    if errors or truncated or invalid_rows
                    else None
                ),
                partial=bool(errors or truncated or invalid_rows),
            )
        if errors:
            result: ConnectorResult[list[RawContract]] = failed_result([], errors[0])
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
        return ConnectorResult(
            data=[],
            availability=(
                DataAvailability.TEMPORARY_ERROR
                if truncated or invalid_rows
                else DataAvailability.EMPTY
            ),
            partial=bool(truncated or invalid_rows),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="no contracts matched the official response",
        )

    async def _fetch_entity_header(
        self, external_id: str, *, registry: bool
    ) -> ConnectorResult[Any]:
        path = (
            "modulo-arp/1.1_consultarARP_Id"
            if registry
            else "modulo-contratos/1.1_consultarContratos_Id"
        )
        params = (
            {"numeroControlePncpAta": external_id}
            if registry
            else {"tipo": "numeroControlePncpContrato", "codigo": external_id}
        )
        try:
            response = await self._http.request_json(
                "GET", join_url(self.base_url, path), params=params
            )
            records = [source_record(self.name, response)]
            rows = list_payload(response.payload, "resultado")
            if len(rows) > 1:
                raise ConnectorContractError("official identity query returned multiple headers")
            if not rows:
                return ConnectorResult(
                    data=None,
                    availability=DataAvailability.NOT_PUBLISHED,
                    raw_records=records,
                    source_urls=[response.url],
                )
            row = rows[0]
            valid = (
                self._has_registry_identity(row) if registry else self._has_contract_identity(row)
            )
            if not valid:
                raise ConnectorContractError("official header without stable identity")
            raw = (
                self._map_price_registry(row, response.url)
                if registry
                else self._map_contract(row, response.url)
            )
            if str(raw.pncp_control_number or raw.external_id).upper() != external_id.upper():
                raise ConnectorContractError(
                    "official header identity differs from the requested identity"
                )
            raw = raw.model_copy(update={"source_record_ref": records[0].record_key})
            return ConnectorResult(
                data=raw,
                availability=DataAvailability.AVAILABLE,
                raw_records=records,
                source_urls=[response.url],
            )
        except _HTTP_EXCEPTIONS as exc:
            return failed_result(None, exc)

    async def fetch_contract(self, external_id: str) -> ConnectorResult[Any]:
        return await self._fetch_entity_header(external_id, registry=False)

    async def fetch_price_registry(self, external_id: str) -> ConnectorResult[Any]:
        return await self._fetch_entity_header(external_id, registry=True)

    async def fetch_contract_items(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractItem]]:
        identifier = contract.pncp_control_number or contract.external_id
        if not identifier:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="contract has no PNCP control number",
            )
        endpoint = join_url(self.base_url, self.CONTRACT_ITEM_ID_PATH)
        rows, refs, records, pagination, error = await self._collect_result_pages(
            endpoint,
            {"tipo": "numeroControlePncpContrato", "codigo": identifier},
            operation="Compras contract items",
        )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawContractItem]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        items = [
            self._map_contract_item(row, contract, records_by_ref[ref].endpoint).model_copy(
                update={"source_record_ref": ref}
            )
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_contract_item_identity(row)
        ]
        partial = error is not None or len(items) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=items,
            availability=(
                DataAvailability.AVAILABLE
                if items
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: contract item collection incomplete" if partial else None,
            partial=partial,
        )

    async def fetch_contract_terms(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractTerm]]:
        del contract
        return ConnectorResult(
            data=[],
            availability=DataAvailability.NOT_SUPPORTED,
            diagnostic="Compras.gov.br contract API does not expose additive terms",
        )

    def _map_contract(self, row: dict[str, Any], source_url: str) -> RawContract:
        control = row.get("numeroControlePncpContrato")
        agency_cnpj = normalized_identifier(row.get("codigoOrgao"))
        year = to_int(str(row.get("dataVigenciaInicial") or "")[:4])
        contract_number = row.get("numeroContrato")
        external_id = str(
            control or f"{agency_cnpj}:{contract_number}:{row.get('dataVigenciaInicial')}"
        )
        return RawContract(
            source=self.name,
            source_updated_at=to_datetime(row.get("dataHoraAtualizacao"), self.timezone),
            source_update_field="dataHoraAtualizacao" if row.get("dataHoraAtualizacao") else None,
            source_url=source_url,
            raw_payload=row,
            external_id=external_id,
            pncp_control_number=str(control) if control else None,
            procurement_control_number=row.get("numeroControlePncpCompra"),
            contract_number=str(contract_number) if contract_number else None,
            year=year,
            process_number=row.get("processo"),
            agency_name=row.get("nomeOrgao"),
            agency_cnpj=agency_cnpj,
            uasg=row.get("codigoUnidadeGestora"),
            object_description=row.get("objeto"),
            status="excluido" if row.get("contratoExcluido") else None,
            valid_from=to_datetime(row.get("dataVigenciaInicial"), self.timezone),
            valid_until=to_datetime(row.get("dataVigenciaFinal"), self.timezone),
            global_value=nonnegative_decimal(row.get("valorGlobal")),
            accumulated_value=nonnegative_decimal(row.get("valorAcumulado")),
            supplier_cnpj=normalized_identifier(row.get("niFornecedor")),
            supplier_name=row.get("nomeRazaoSocialFornecedor"),
        )

    def _map_contract_item(
        self, row: dict[str, Any], contract: RawContract, source_url: str
    ) -> RawContractItem:
        number = str(row.get("numeroItem") or row.get("codigoItem") or "")
        return RawContractItem(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=f"{contract.external_id}:item:{number}",
            contract_external_id=contract.external_id,
            item_number=number or None,
            description=row.get("descricaoIitem") or row.get("descricaoItem"),
            unit=row.get("tipoItem"),
            quantity=to_decimal(row.get("quantidadeItem")),
            unit_value=to_decimal(row.get("valorUnitarioItem")),
            total_value=to_decimal(row.get("valorTotalItem")),
            supplier_cnpj=normalized_identifier(row.get("niFornecedor")),
            supplier_name=row.get("nomeRazaoSocialFornecedor"),
        )

    async def discover_price_registries(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawPriceRegistry]]:
        """Collect ARPs by their vigência window, as required by the official module."""

        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        endpoint = join_url(self.base_url, self.ARP_LIST_PATH)
        page_size = min(max(filters.page_size or 500, 10), 500)
        records: list[RawSourceRecord] = []
        registries: list[RawPriceRegistry] = []
        errors: list[Exception] = []
        fetched_pages = 0
        total_pages = 0
        total_records = 0
        truncated = False
        invalid_rows = 0
        page = 1
        collected_rows = 0
        max_pages = min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES)
        limit = int(setting(self.settings, "crawl_max_records_per_source", 0))
        budget = getattr(self._http, "arp_discovery_budget", None)
        if budget is None and limit:
            budget = DiscoveryBudget(limit)
        window = PageWindow(
            concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
            max_pages=max_pages,
            budget=budget,
        )
        async with window:
            while True:
                params: dict[str, Any] = {
                    "pagina": page,
                    "tamanhoPagina": page_size,
                    "codigoUnidadeGerenciadora": filters.unit_code,
                    "dataVigenciaInicialMin": start.isoformat(),
                    "dataVigenciaInicialMax": end.isoformat(),
                }
                try:
                    response = await window.request(
                        self._http, endpoint, params, total_pages=total_pages
                    )
                except DiscoveryLimitReached:
                    truncated = True
                    break
                except _HTTP_EXCEPTIONS as exc:
                    errors.append(exc)
                    break
                record = source_record(self.name, response)
                repeated = repeated_page(records, record)
                records.append(record)
                if repeated:
                    errors.append(
                        ConnectorContractError("official source repeated a discovery page")
                    )
                    break
                payload = response.payload
                try:
                    rows = list_payload(payload, "resultado", operation="Compras ARP discovery")
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                for row in rows:
                    if not self._has_registry_identity(row):
                        invalid_rows += 1
                        continue
                    registries.append(
                        self._map_price_registry(row, response.url).model_copy(
                            update={"source_record_ref": record.record_key}
                        )
                    )
                fetched_pages += 1
                try:
                    collected_rows += len(rows)
                    observed_total_pages = observed_pages(
                        payload,
                        page,
                        row_count=len(rows),
                        collected_rows=collected_rows,
                        page_size=page_size,
                    )
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                total_pages += observed_total_pages if page == 1 else 0
                if page == 1:
                    total_records += (
                        to_int(payload.get("totalRegistros")) if isinstance(payload, dict) else None
                    ) or len(rows)
                has_more = page < observed_total_pages
                if (
                    (
                        page_budget_exhausted(
                            self._http, page, max_pages, url=endpoint, params=params
                        )
                        or window.budget_exhausted
                    )
                    and has_more
                    and not window.has_pending
                ):
                    truncated = True
                    break
                if not has_more:
                    break
                page += 1

        records.extend(
            source_record(self.name, response) for response in window.unconsumed_responses()
        )
        pagination = PaginationInfo(
            page=1,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            pages_remaining=max(total_pages - fetched_pages, 0),
            fetched_pages=fetched_pages,
            truncated=truncated,
        )
        if registries:
            return ConnectorResult(
                data=registries,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    "partial result: one or more pages were not collected"
                    if errors or truncated or invalid_rows
                    else None
                ),
                partial=bool(errors or truncated or invalid_rows),
            )
        if errors:
            result: ConnectorResult[list[RawPriceRegistry]] = failed_result([], errors[0])
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
        return ConnectorResult(
            data=[],
            availability=(
                DataAvailability.TEMPORARY_ERROR
                if truncated or invalid_rows
                else DataAvailability.EMPTY
            ),
            partial=bool(truncated or invalid_rows),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="no price registries matched the official response",
        )

    async def fetch_price_registry_items(
        self, external_id: str
    ) -> ConnectorResult[list[RawPriceRegistryItem]]:
        self._arp_items.pop(external_id, None)
        endpoint = join_url(self.base_url, self.ARP_ITEM_ID_PATH)
        rows, refs, records, pagination, error = await self._collect_result_pages(
            endpoint,
            {"numeroControlePncpAta": external_id},
            operation="Compras ARP items",
        )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawPriceRegistryItem]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        items = [
            self._map_price_registry_item(
                row, external_id, records_by_ref[ref].endpoint
            ).model_copy(update={"source_record_ref": ref})
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_registry_item_identity(row)
        ]
        partial = error is not None or len(items) != len(rows) or pagination.truncated
        self._arp_items[external_id] = (
            list(dict.fromkeys(item.item_number for item in items if item.item_number)),
            not partial and all(item.item_number for item in items),
        )
        return ConnectorResult(
            data=items,
            availability=(
                DataAvailability.AVAILABLE
                if items
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.NOT_PUBLISHED
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: ARP item collection incomplete" if partial else None,
            partial=partial,
        )

    async def fetch_price_registry_units(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryUnit]]:
        return await self._fetch_arp_units(registry)

    async def fetch_price_registry_commitments(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryCommitment]]:
        return await self._fetch_arp_commitments(registry)

    async def fetch_price_registry_adhesions(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryAdhesion]]:
        return await self._fetch_arp_adhesions(registry)

    async def _fetch_arp_units(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryUnit]]:
        params = self._arp_lookup_params(registry)
        if params is None:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="ARP number/UASG not published; unit balances cannot be queried",
            )
        endpoint = join_url(self.base_url, self.ARP_UNITS_PATH)
        rows, refs, records, pagination, error = await self._collect_arp_item_pages(
            endpoint, params, registry, operation="Compras ARP units"
        )
        if self._arp_items.get(registry.external_id) == ([], True):
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="No published ARP items; item-dependent unit query not performed",
            )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawPriceRegistryUnit]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        data = [
            self._map_arp_unit(
                row,
                registry.external_id,
                records_by_ref[ref].endpoint,
                item_number=str(records_by_ref[ref].request_parameters["numeroItem"]),
            ).model_copy(update={"source_record_ref": ref})
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_registry_row_identity(row)
        ]
        partial = error is not None or len(data) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=data,
            availability=(
                DataAvailability.AVAILABLE
                if data
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: ARP units incomplete" if partial else None,
            partial=partial,
        )

    async def _fetch_arp_commitments(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryCommitment]]:
        params = self._arp_lookup_params(registry)
        if params is None:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="ARP number/UASG not published; commitment balances cannot be queried",
            )
        endpoint = join_url(self.base_url, self.ARP_COMMITMENTS_PATH)
        rows, refs, records, pagination, error = await self._collect_result_pages(
            endpoint, params, operation="Compras ARP commitments"
        )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawPriceRegistryCommitment]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        data = [
            self._map_arp_commitment(
                row, registry.external_id, records_by_ref[ref].endpoint
            ).model_copy(update={"source_record_ref": ref})
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and self._has_registry_row_identity(row)
        ]
        partial = error is not None or len(data) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=data,
            availability=(
                DataAvailability.AVAILABLE
                if data
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: ARP commitments incomplete" if partial else None,
            partial=partial,
        )

    async def _fetch_arp_adhesions(
        self, registry: RawPriceRegistry
    ) -> ConnectorResult[list[RawPriceRegistryAdhesion]]:
        params = self._arp_lookup_params(registry)
        if params is None:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="ARP number/UASG not published; adhesions cannot be queried",
            )
        endpoint = join_url(self.base_url, self.ARP_ADHESIONS_PATH)
        rows, refs, records, pagination, error = await self._collect_arp_item_pages(
            endpoint, params, registry, operation="Compras ARP adhesions"
        )
        if self._arp_items.get(registry.external_id) == ([], True):
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="No published ARP items; item-dependent adhesion query not performed",
            )
        if error is not None and not rows:
            failure: ConnectorResult[list[RawPriceRegistryAdhesion]] = failed_result([], error)
            return failure.model_copy(
                update={
                    "raw_records": records,
                    "pagination": pagination,
                    "partial": True,
                    "source_urls": unique_urls(records),
                }
            )
        records_by_ref = {record.record_key: record for record in records}
        data = [
            self._map_arp_adhesion(
                row,
                registry.external_id,
                records_by_ref[ref].endpoint,
                item_number=str(records_by_ref[ref].request_parameters["numeroItem"]),
            ).model_copy(update={"source_record_ref": ref})
            for row, ref in zip(rows, refs, strict=False)
            if ref in records_by_ref and bool(row.get("unidadeNaoParticipante"))
        ]
        partial = error is not None or len(data) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=data,
            availability=(
                DataAvailability.AVAILABLE
                if data
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: ARP adhesions incomplete" if partial else None,
            partial=partial,
        )

    async def _collect_arp_item_pages(
        self,
        endpoint: str,
        params: dict[str, str],
        registry: RawPriceRegistry,
        *,
        operation: str,
    ) -> tuple[
        list[dict[str, Any]], list[str], list[RawSourceRecord], PaginationInfo, Exception | None
    ]:
        if registry.external_id not in self._arp_items:
            item_result = await self.fetch_price_registry_items(registry.external_id)
            if registry.external_id not in self._arp_items:
                return (
                    [],
                    [],
                    item_result.raw_records,
                    item_result.pagination or PaginationInfo(),
                    ConnectorContractError(
                        "official item identifiers could not be collected for ARP details"
                    ),
                )
        numbers, complete = self._arp_items[registry.external_id]
        rows: list[dict[str, Any]] = []
        refs: list[str] = []
        records: list[RawSourceRecord] = []
        totals = PaginationInfo(fetched_pages=0, total_records=0, total_pages=0, pages_remaining=0)
        error: Exception | None = None
        if not complete:
            error = ConnectorContractError("official item identifiers incomplete for ARP details")
        replay = getattr(self._http, "collection_resume", None)
        known = (
            len(
                {
                    str(raw.request_parameters.get("numeroItem"))
                    for raw in replay.cache.values()
                    if raw.request_parameters.get("numeroItem") is not None
                    and all(
                        str(raw.request_parameters.get(key)) == str(value)
                        for key, value in params.items()
                    )
                }
            )
            if replay
            else 0
        )
        item_limit = _MAX_DETAIL_PAGES + known
        if len(numbers) > item_limit:
            numbers = numbers[:item_limit]
            error = ConnectorContractError("ARP item detail limit reached")
            totals.truncated = True
        for number in numbers:
            (
                page_rows,
                page_refs,
                page_records,
                pagination,
                page_error,
            ) = await self._collect_result_pages(
                endpoint,
                {**params, "numeroItem": number},
                operation=operation,
            )
            rows.extend(page_rows)
            refs.extend(page_refs)
            records.extend(page_records)
            totals.fetched_pages += pagination.fetched_pages
            totals.total_records = (totals.total_records or 0) + (pagination.total_records or 0)
            totals.total_pages = (totals.total_pages or 0) + (pagination.total_pages or 0)
            totals.pages_remaining = (totals.pages_remaining or 0) + (
                pagination.pages_remaining or 0
            )
            totals.truncated = totals.truncated or pagination.truncated
            error = error or page_error
        return rows, refs, records, totals, error

    @staticmethod
    def _arp_lookup_params(registry: RawPriceRegistry) -> dict[str, str] | None:
        if not registry.registry_number or not registry.uasg:
            return None
        return {
            "numeroAta": str(registry.registry_number),
            "unidadeGerenciadora": str(registry.uasg),
        }

    def _map_arp_unit(
        self,
        row: dict[str, Any],
        registry_external_id: str,
        source_url: str,
        *,
        item_number: str | None = None,
    ) -> RawPriceRegistryUnit:
        item = str(row.get("numeroItem") or item_number or "")
        unit = str(row.get("codigoUnidade") or "")
        supplier = str(row.get("fornecedor") or "")
        return RawPriceRegistryUnit(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=f"{registry_external_id}:unit:{item}:{unit}:{supplier}",
            price_registry_external_id=registry_external_id,
            item_number=row.get("numeroItem") or item_number,
            catalog_code=str(row["codigoPdm"]) if row.get("codigoPdm") is not None else None,
            description=row.get("descricaoItem"),
            supplier_name=row.get("fornecedor"),
            quantity_registered=nonnegative_decimal(row.get("quantidadeRegistrada")),
            adhesion_balance=nonnegative_decimal(row.get("saldoAdesoes")),
            remanejamento_balance=nonnegative_decimal(row.get("saldoRemanejamentoEmpenho")),
            adhesion_limit=nonnegative_decimal(row.get("qtdLimiteAdesao")),
            purchase_limit=nonnegative_decimal(row.get("qtdLimiteInformadoCompra")),
            accepts_adhesion=row.get("aceitaAdesao"),
            unit_code=row.get("codigoUnidade"),
            unit_name=row.get("nomeUnidade"),
            unit_type=row.get("tipoUnidade"),
            included_at=to_datetime(row.get("dataHoraInclusao"), self.timezone),
            updated_at=to_datetime(row.get("dataHoraAtualizacao"), self.timezone),
            excluded_at=to_datetime(row.get("dataHoraExclusao"), self.timezone),
        )

    def _map_arp_commitment(
        self, row: dict[str, Any], registry_external_id: str, source_url: str
    ) -> RawPriceRegistryCommitment:
        item = str(row.get("numeroItem") or "")
        unit = str(row.get("unidade") or "")
        kind = str(row.get("tipo") or "")
        return RawPriceRegistryCommitment(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=f"{registry_external_id}:commitment:{item}:{unit}:{kind}",
            price_registry_external_id=registry_external_id,
            item_number=row.get("numeroItem"),
            unit=row.get("unidade"),
            commitment_type=row.get("tipo"),
            quantity_registered=nonnegative_decimal(row.get("quantidadeRegistrada")),
            quantity_committed=nonnegative_decimal(row.get("quantidadeEmpenhada")),
            balance=nonnegative_decimal(row.get("saldoEmpenho")),
            included_at=to_datetime(row.get("dataHoraInclusao"), self.timezone),
            updated_at=to_datetime(row.get("dataHoraAtualizacao"), self.timezone),
        )

    def _map_arp_adhesion(
        self,
        row: dict[str, Any],
        registry_external_id: str,
        source_url: str,
        *,
        item_number: str | None = None,
    ) -> RawPriceRegistryAdhesion:
        item = str(row.get("numeroItem") or item_number or "")
        requesting = str(row.get("unidadeNaoParticipante") or "")
        approved = to_datetime(row.get("dataAprovacaoAnalise"), self.timezone)
        return RawPriceRegistryAdhesion(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=(
                f"{registry_external_id}:adhesion:{item}:{requesting}:"
                f"{approved.isoformat() if approved else ''}"
            ),
            price_registry_external_id=registry_external_id,
            item_number=row.get("numeroItem") or item_number,
            requesting_unit=row.get("unidadeNaoParticipante"),
            approved_at=approved,
            approved_quantity=nonnegative_decimal(row.get("quantidadeAprovadaAdesao")),
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
        external_id = row.get("idCompra") or row.get("numeroControlePNCP")
        if not external_id:
            # A fallback identity must contain every canonical discriminator.
            components = (
                normalized_identifier(row.get("orgaoEntidadeCnpj")),
                row.get("unidadeOrgaoCodigoUnidade"),
                row.get("numeroCompra"),
                row.get("anoCompraPncp"),
                row.get("modalidadeNome"),
            )
            if not all(value is not None and str(value).strip() for value in components):
                raise ConnectorContractError("Compras procurement has no complete source identity")
            external_id = "composite:" + ":".join(str(value) for value in components)
        control = row.get("numeroControlePNCP")
        modality = row.get("modalidadeNome")
        purchase_number = row.get("numeroCompra")
        model_type = RawProcurementDetail if detail else RawProcurement
        values: dict[str, Any] = {
            "source": self.name,
            "source_url": source_url,
            "raw_payload": row,
            "external_id": str(external_id),
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
            "estimated_value": nonnegative_decimal(row.get("valorTotalEstimado")),
            "homologated_value": nonnegative_decimal(row.get("valorTotalHomologado")),
            "is_srp": row.get("srp"),
            "legal_basis": (
                row.get("amparoLegalNome")
                or row.get("amparoLegalDescricao")
                or row.get("amparoLegalCodigoPncp")
            ),
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

    def _map_price_registry(self, row: dict[str, Any], source_url: str) -> RawPriceRegistry:
        control = row.get("numeroControlePncpAta")
        year = to_int(row.get("anoCompra"))
        registry_number = row.get("numeroAtaRegistroPreco")
        agency_name = row.get("nomeOrgao") or row.get("nomeUnidadeGerenciadora")
        external_id = str(control or f"{agency_name}:{year}:{registry_number}")
        excluded = bool(row.get("ataExcluido"))
        status = row.get("statusAta") or ("excluída" if excluded else None)
        return RawPriceRegistry(
            source=self.name,
            source_updated_at=to_datetime(row.get("dataHoraAtualizacao"), self.timezone),
            source_update_field="dataHoraAtualizacao" if row.get("dataHoraAtualizacao") else None,
            source_url=row.get("linkAtaPNCP") or source_url,
            raw_payload=row,
            external_id=external_id,
            pncp_control_number=str(control) if control else None,
            linked_pncp_control_number=row.get("numeroControlePncpCompra"),
            registry_number=str(registry_number) if registry_number is not None else None,
            year=year,
            agency_name=agency_name,
            uasg=(
                str(row.get("codigoUnidadeGerenciadora"))
                if row.get("codigoUnidadeGerenciadora") is not None
                else None
            ),
            object_description=row.get("objeto"),
            status=status,
            signed_at=to_datetime(row.get("dataAssinatura"), self.timezone),
            valid_from=to_datetime(row.get("dataVigenciaInicial"), self.timezone),
            valid_until=to_datetime(row.get("dataVigenciaFinal"), self.timezone),
            total_value=nonnegative_decimal(row.get("valorTotal")),
        )

    def _map_price_registry_item(
        self, row: dict[str, Any], registry_external_id: str, source_url: str
    ) -> RawPriceRegistryItem:
        item_number = row.get("numeroItem")
        supplier_cnpj = normalized_identifier(row.get("niFornecedor"))
        return RawPriceRegistryItem(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=(
                f"{registry_external_id}:item:{item_number}:{supplier_cnpj or 'sem-fornecedor'}"
            ),
            price_registry_external_id=registry_external_id,
            item_number=str(item_number) if item_number is not None else None,
            catalog_code=str(row["codigoPdm"]) if row.get("codigoPdm") is not None else None,
            item_type=row.get("tipoItem"),
            description=row.get("descricaoItem") or row.get("nomePdm"),
            quantity=nonnegative_decimal(row.get("quantidadeHomologadaItem")),
            unit_value=nonnegative_decimal(row.get("valorUnitario")),
            total_value=nonnegative_decimal(row.get("valorTotal")),
            max_adhesion_quantity=nonnegative_decimal(row.get("maximoAdesao")),
            supplier_cnpj=supplier_cnpj,
            supplier_name=row.get("nomeRazaoSocialFornecedor"),
        )

    def _map_item(
        self, row: dict[str, Any], procurement_external_id: str, source_url: str
    ) -> RawProcurementItem:
        item_external_id = row.get("idCompraItem")
        item_number = (
            row.get("numeroItemPncp") or row.get("numeroItemCompra") or row.get("numeroItem")
        )
        return RawProcurementItem(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=str(item_external_id or f"{procurement_external_id}:item:{item_number}"),
            procurement_external_id=procurement_external_id,
            item_number=str(item_number) if item_number is not None else None,
            description=row.get("descricaoResumida") or row.get("descricaodetalhada"),
            detailed_description=row.get("descricaodetalhada"),
            quantity=nonnegative_decimal(row.get("quantidade")),
            unit=row.get("unidadeMedida"),
            estimated_unit_value=nonnegative_decimal(row.get("valorUnitarioEstimado")),
            estimated_total_value=nonnegative_decimal(row.get("valorTotal")),
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
            external_id=f"{item_external_id or f'{procurement_external_id}:item:{item_number}'}:result:{sequence}",
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
            quantity=nonnegative_decimal(row.get("quantidadeHomologada")),
            unit_value=nonnegative_decimal(row.get("valorUnitarioHomologado")),
            total_value=nonnegative_decimal(row.get("valorTotalHomologado")),
            rank=positive_int(row.get("ordemClassificacaoSrp")),
            result_status=status,
            result_at=to_datetime(row.get("dataResultadoPncp"), self.timezone),
            is_cancelled=is_cancelled,
            cancellation_reason=cancellation_reason,
        )

    async def _collect_result_pages(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        operation: str,
        page_size: int = 500,
    ) -> tuple[
        list[dict[str, Any]],
        list[str],
        list[RawSourceRecord],
        PaginationInfo,
        Exception | None,
    ]:
        rows: list[dict[str, Any]] = []
        row_refs: list[str] = []
        records: list[RawSourceRecord] = []
        error: Exception | None = None
        total_pages: int | None = None
        total_records: int | None = None
        seen_hashes: set[str] = set()
        page = 1
        async with PageWindow(
            concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
            max_pages=_MAX_DETAIL_PAGES,
        ) as window:
            while True:
                try:
                    response = await window.request(
                        self._http,
                        endpoint,
                        {**params, "pagina": page, "tamanhoPagina": page_size},
                        total_pages=total_pages or 0,
                    )
                except _HTTP_EXCEPTIONS as exc:
                    error = exc
                    break
                record = source_record(self.name, response)
                if record.response_hash in seen_hashes:
                    error = ConnectorContractError(
                        f"{operation} repeated page {page}", url=response.url
                    )
                    break
                seen_hashes.add(record.response_hash)
                records.append(record)
                try:
                    page_rows = list_payload(response.payload, "resultado", operation=operation)
                except ConnectorContractError as exc:
                    error = exc
                    break
                rows.extend(page_rows)
                row_refs.extend(record.record_key for _ in page_rows)
                try:
                    total_pages = observed_pages(
                        response.payload,
                        page,
                        row_count=len(page_rows),
                        collected_rows=len(rows),
                        page_size=page_size,
                    )
                    if isinstance(response.payload, dict):
                        total_records = to_int(response.payload.get("totalRegistros"))
                except ConnectorContractError as exc:
                    error = exc
                    break
                has_more = page < total_pages
                if not has_more:
                    break
                if (
                    page_budget_exhausted(
                        self._http,
                        page,
                        _MAX_DETAIL_PAGES,
                        url=endpoint,
                        params={**params, "pagina": page, "tamanhoPagina": page_size},
                    )
                    and not window.has_pending
                ):
                    error = ConnectorContractError(
                        f"{operation} exceeded {_MAX_DETAIL_PAGES} pages", url=response.url
                    )
                    break
                page += 1
        records.extend(
            source_record(self.name, response) for response in window.unconsumed_responses()
        )
        pagination = PaginationInfo(
            page=1,
            page_size=page_size,
            total_records=total_records if total_records is not None else len(rows),
            total_pages=total_pages if total_pages is not None else len(records),
            pages_remaining=(
                max(total_pages - len(records), 0) if total_pages is not None else None
            ),
            fetched_pages=len(records),
            truncated=error is not None and bool(records),
        )
        return rows, row_refs, records, pagination, error

    @staticmethod
    def _has_contract_identity(row: dict[str, Any]) -> bool:
        return bool(
            row.get("numeroControlePncpContrato")
            or row.get("numeroContrato")
            or row.get("codigoOrgao")
        )

    @staticmethod
    def _has_registry_identity(row: dict[str, Any]) -> bool:
        return bool(
            row.get("numeroControlePncpAta")
            or row.get("numeroAtaRegistroPreco")
            or row.get("anoCompra")
        )

    @staticmethod
    def _has_procurement_identity(row: dict[str, Any]) -> bool:
        return bool(
            row.get("idCompra")
            or row.get("numeroControlePNCP")
            or (
                normalized_identifier(row.get("orgaoEntidadeCnpj"))
                and row.get("unidadeOrgaoCodigoUnidade")
                and row.get("numeroCompra")
                and row.get("anoCompraPncp")
                and row.get("modalidadeNome")
            )
        )

    @staticmethod
    def _has_item_identity(row: dict[str, Any]) -> bool:
        return bool(
            row.get("numeroItemPncp")
            or row.get("numeroItemCompra")
            or row.get("numeroItem")
            or row.get("idCompraItem")
        )

    @staticmethod
    def _has_result_identity(row: dict[str, Any]) -> bool:
        return bool(
            (row.get("numeroItemPncp") or row.get("idCompraItem"))
            and (
                normalized_identifier(row.get("niFornecedor"))
                or row.get("nomeRazaoSocialFornecedor")
            )
        )

    @staticmethod
    def _has_contract_item_identity(row: dict[str, Any]) -> bool:
        return bool(row.get("numeroItem") or row.get("codigoItem"))

    @staticmethod
    def _has_registry_item_identity(row: dict[str, Any]) -> bool:
        return bool(row.get("numeroItem") or row.get("codigoItem") or row.get("codigoPdm"))

    @staticmethod
    def _has_registry_row_identity(row: dict[str, Any]) -> bool:
        return bool(row.get("numeroItem") or row.get("codigoPdm") or row.get("unidade"))

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
