"""Connector for the public PNCP Consulta and Integração GET APIs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
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
    RawPriceRegistryItem,
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

PNCP_MODALITY_CODES: dict[str, int] = {
    "leilao_eletronico": 1,
    "dialogo_competitivo": 2,
    "concurso": 3,
    "concorrencia_eletronica": 4,
    "concorrencia_presencial": 5,
    "pregao_eletronico": 6,
    "pregao_presencial": 7,
    "dispensa": 8,
    "dispensa_eletronica": 8,
    "inexigibilidade": 9,
    "manifestacao_de_interesse": 10,
    "pre_qualificacao": 11,
    "credenciamento": 12,
    "leilao_presencial": 13,
}

_CONTROL_NUMBER = re.compile(
    r"^(?P<cnpj>[A-Z0-9]{14})-\d-(?P<sequence>\d{1,9})/(?P<year>\d{4})$",
    re.IGNORECASE,
)
_COMPOSITE_ID = re.compile(
    r"^(?P<cnpj>[A-Z0-9]{14})[:|](?P<year>\d{4})[:|](?P<sequence>\d{1,9})$",
    re.IGNORECASE,
)
_HTTP_EXCEPTIONS = (
    ConnectorHTTPError,
    ConnectorContractError,
    ConnectorDecodeError,
    httpx.TimeoutException,
    httpx.TransportError,
)
_MAX_DETAIL_PAGES = 1000


@dataclass(frozen=True, slots=True)
class PNCPReference:
    cnpj: str
    year: int
    sequence: int


def parse_pncp_external_id(external_id: str) -> PNCPReference:
    """Parse a PNCP control number or the explicit ``CNPJ:YEAR:SEQ`` form."""

    candidate = external_id.strip().upper()
    match = _CONTROL_NUMBER.fullmatch(candidate) or _COMPOSITE_ID.fullmatch(candidate)
    if match is None:
        raise ValueError("PNCP external_id must be a numeroControlePNCP or CNPJ:YEAR:SEQUENCE")
    return PNCPReference(
        cnpj=match.group("cnpj"),
        year=int(match.group("year")),
        sequence=int(match.group("sequence")),
    )


def _canonical_modality(value: str) -> str:
    return normalized_text(value).replace("-", "_").replace(" ", "_")


class PNCPConnector:
    """Read-only connector for officially documented, unauthenticated PNCP APIs."""

    name = "pncp"

    def __init__(
        self,
        settings: object | None = None,
        *,
        http_client: AsyncHTTPClient | None = None,
    ) -> None:
        self.settings = runtime_settings(settings)
        self.consulta_base_url = str(
            setting(self.settings, "pncp_base_url", "https://pncp.gov.br/api/consulta/v1")
        ).rstrip("/")
        self.integration_base_url = str(
            setting(
                self.settings,
                "pncp_integration_base_url",
                "https://pncp.gov.br/api/pncp/v1",
            )
        ).rstrip("/")
        self.timezone = str(setting(self.settings, "timezone", "America/Sao_Paulo"))
        self._http = http_client or AsyncHTTPClient(self.settings)
        self._owns_http = http_client is None
        self._item_cache: dict[
            str, tuple[list[dict[str, Any]], list[RawSourceRecord], PaginationInfo]
        ] = {}

    async def discover_procurements(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawProcurement]]:
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

        modalities = [self._modality_code(value) for value in modality_values]
        modalities = list(dict.fromkeys(modalities))
        kind = self._discovery_kind(filters.discovery_kind)
        endpoint = join_url(self.consulta_base_url, f"contratacoes/{kind}")
        page_size = min(max(filters.page_size or 50, 10), 50)
        records: list[RawSourceRecord] = []
        mapped: list[RawProcurement] = []
        errors: list[Exception] = []
        total_records = 0
        total_pages = 0
        fetched_pages = 0
        truncated = False
        invalid_rows = 0

        # Publication/update require a modality.  The proposals-open endpoint
        # makes it optional, but querying separately keeps source provenance and
        # behaviour consistent with the configured modality allow-list.
        for modality_code in modalities:
            page = 1
            collected_rows = 0
            observed_total_pages = 0
            async with PageWindow(
                concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
                max_pages=min(filters.max_pages or _MAX_DETAIL_PAGES, _MAX_DETAIL_PAGES),
            ) as window:
                while True:
                    params: dict[str, Any] = {
                        "dataFinal": end.strftime("%Y%m%d"),
                        "codigoModalidadeContratacao": modality_code,
                        "uf": filters.uf,
                        "codigoMunicipioIbge": filters.municipality_code,
                        "cnpj": filters.agency_cnpj,
                        "codigoUnidadeAdministrativa": filters.unit_code,
                        "pagina": page,
                        "tamanhoPagina": page_size,
                    }
                    if kind != "proposta":
                        params["dataInicial"] = start.strftime("%Y%m%d")
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
                        page_rows = list_payload(
                            payload, "data", operation="PNCP procurement discovery"
                        )
                    except ConnectorContractError as exc:
                        errors.append(exc)
                        break
                    for row in page_rows:
                        if not self._has_procurement_identity(row):
                            invalid_rows += 1
                            continue
                        try:
                            if self._matches_client_filters(row, filters):
                                mapped.append(
                                    self._map_procurement(row, response.url).model_copy(
                                        update={"source_record_ref": record.record_key}
                                    )
                                )
                        except Exception:
                            invalid_rows += 1

                    fetched_pages += 1
                    try:
                        collected_rows += len(page_rows)
                        observed_total_pages = observed_pages(
                            payload,
                            page,
                            row_count=len(page_rows),
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
                        ) or len(page_rows)
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

        mapped = self._deduplicate(mapped)
        pagination = PaginationInfo(
            page=1,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            pages_remaining=max(total_pages - fetched_pages, 0),
            fetched_pages=fetched_pages,
            truncated=truncated,
        )
        if mapped:
            diagnostic = None
            if errors or truncated or invalid_rows:
                diagnostic = "partial result: one or more pages/rows were not usable"
            return ConnectorResult(
                data=mapped,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=diagnostic,
                partial=bool(errors or truncated or invalid_rows),
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
        self, external_id: str, *, bypass_replay: bool = False
    ) -> ConnectorResult[RawProcurementDetail | None]:
        reference = parse_pncp_external_id(external_id)
        endpoint = join_url(
            self.consulta_base_url,
            f"orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}",
        )
        try:
            response = await self._http.request_json(
                "GET", endpoint, rate_scope="pncp.procurement_detail", bypass_replay=bypass_replay
            )
        except _HTTP_EXCEPTIONS as exc:
            return failed_result(None, exc)
        if not isinstance(response.payload, dict) or not self._has_procurement_identity(
            response.payload
        ):
            failure: ConnectorResult[RawProcurementDetail | None] = failed_result(
                None,
                ConnectorContractError(
                    "PNCP detail without procurement identity", url=response.url
                ),
            )
            failure.raw_records = [source_record(self.name, response)]
            failure.source_urls = [response.url]
            return failure
        procurement = self._map_procurement(response.payload, response.url, detail=True)
        return ConnectorResult(
            data=procurement,
            availability=DataAvailability.AVAILABLE,
            source_urls=[response.url],
            raw_records=[source_record(self.name, response)],
        )

    async def verify_procurement_version(self, external_id: str) -> ConnectorResult[Any]:
        return await self.fetch_procurement(external_id, bypass_replay=True)

    async def fetch_items(self, external_id: str) -> ConnectorResult[list[RawProcurementItem]]:
        reference = parse_pncp_external_id(external_id)
        path = f"orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}/itens"
        rows, row_refs, records, pagination, error = await self._collect_integration(path)
        if error is None:
            # Reuse the same audited item rows when the pipeline requests results.
            self._item_cache[external_id] = (rows, records, pagination)
        if error is not None and not records:
            return failed_result([], error)
        records_by_ref = self._records_by_ref(records)
        items = [
            self._map_item(
                row,
                external_id,
                records_by_ref[row_ref].endpoint,
                source_record_ref=row_ref,
            )
            for row, row_ref in zip(rows, row_refs, strict=False)
            if self._has_item_identity(row) and row_ref in records_by_ref
        ]
        if error is not None and not items:
            result: ConnectorResult[list[RawProcurementItem]] = failed_result([], error)
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
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
        reference = parse_pncp_external_id(external_id)
        path = f"orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}/arquivos"
        rows, row_refs, records, pagination, error = await self._collect_integration(path)
        if error is not None and not records:
            return failed_result([], error)
        records_by_ref = self._records_by_ref(records)
        documents = [
            self._map_document(
                row,
                external_id,
                records_by_ref[row_ref].endpoint,
                source_record_ref=row_ref,
            )
            for row, row_ref in zip(rows, row_refs, strict=False)
            if self._has_document_identity(row) and row_ref in records_by_ref
        ]
        if error is not None and not documents:
            result: ConnectorResult[list[RawDocument]] = failed_result([], error)
            result.source_urls = unique_urls(records)
            result.raw_records = records
            result.pagination = pagination
            return result
        partial = error is not None or len(documents) != len(rows) or pagination.truncated
        return ConnectorResult(
            data=documents,
            availability=(
                DataAvailability.AVAILABLE
                if documents
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.NOT_PUBLISHED
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            diagnostic="partial result: document collection incomplete" if partial else None,
            partial=partial,
        )

    async def fetch_results(self, external_id: str) -> ConnectorResult[list[RawResult]]:
        reference = parse_pncp_external_id(external_id)
        cached_items = self._item_cache.pop(external_id, None)
        if cached_items is not None:
            item_rows, _, _ = cached_items
            records: list[RawSourceRecord] = []
            item_error: Exception | None = None
        else:
            items_path = (
                f"orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}/itens"
            )
            item_rows, _, records, _, item_error = await self._collect_integration(items_path)
            if item_error is not None and not records:
                return failed_result([], item_error)

        results: list[RawResult] = []
        errors: list[Exception] = [item_error] if item_error is not None else []
        for item in item_rows:
            if item.get("temResultado") is not True:
                continue
            item_number = to_int(item.get("numeroItem"))
            if item_number is None:
                continue
            path = (
                f"orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}"
                f"/itens/{item_number}/resultados"
            )
            (
                result_rows,
                result_refs,
                result_records,
                pagination,
                result_error,
            ) = await self._collect_integration(path)
            records.extend(result_records)
            if result_error is not None:
                errors.append(result_error)
            if pagination.truncated and result_error is None:
                errors.append(ConnectorContractError("PNCP result collection truncated"))
            by_ref = self._records_by_ref(result_records)
            for row, row_ref in zip(result_rows, result_refs, strict=True):
                if not self._has_result_identity(row):
                    errors.append(
                        ConnectorContractError("PNCP result row without item/supplier identity")
                    )
                    continue
                results.append(
                    self._map_result(
                        row,
                        external_id,
                        item,
                        by_ref[row_ref].endpoint,
                        source_record_ref=row_ref,
                    )
                )

        if not results and errors:
            result: ConnectorResult[list[RawResult]] = failed_result([], errors[0])
            result.source_urls = unique_urls(records)
            result.raw_records = records
            return result
        partial = bool(errors)
        return ConnectorResult(
            data=results,
            availability=(
                DataAvailability.AVAILABLE if results else DataAvailability.NOT_PUBLISHED
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            diagnostic="partial result: one or more item results failed" if partial else None,
            partial=partial,
        )

    async def fetch_participants(self, external_id: str) -> ConnectorResult[list[RawParticipant]]:
        """Return only structured awarded suppliers, never inferred bidders."""

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
                "PNCP has no structured public feed for session/legal events; "
                "the historico endpoint is a technical maintenance log"
            ),
        )

    async def discover_price_registries(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawPriceRegistry]]:
        """Collect atas de registro de preços by the official validity-period query."""

        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        endpoint = join_url(
            self.consulta_base_url,
            "atas/atualizacao" if filters.discovery_kind == "update" else "atas",
        )
        page_size = min(max(filters.page_size or 50, 10), 50)
        records: list[RawSourceRecord] = []
        mapped: list[RawPriceRegistry] = []
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
                    "dataInicial": start.strftime("%Y%m%d"),
                    "dataFinal": end.strftime("%Y%m%d"),
                    "cnpj": filters.agency_cnpj,
                    "codigoUnidadeAdministrativa": filters.unit_code,
                    "pagina": page,
                    "tamanhoPagina": page_size,
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
                    registry_rows = list_payload(
                        payload, "data", operation="PNCP price registry discovery"
                    )
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                for row in registry_rows:
                    if not (row.get("numeroControlePNCPAta") or row.get("numeroAtaRegistroPreco")):
                        invalid_rows += 1
                        continue
                    mapped.append(
                        self._map_price_registry(row, response.url).model_copy(
                            update={"source_record_ref": record.record_key}
                        )
                    )
                fetched_pages += 1
                try:
                    collected_rows += len(registry_rows)
                    observed_total_pages = observed_pages(
                        payload,
                        page,
                        row_count=len(registry_rows),
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
                    ) or len(mapped)
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
        if mapped:
            partial = bool(errors or truncated or invalid_rows)
            return ConnectorResult(
                data=mapped,
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

    async def discover_contracts(
        self, filters: ProcurementFilters
    ) -> ConnectorResult[list[RawContract]]:
        """Collect contracts/commitments published by PNCP for a period/agency."""

        start, end = filters.resolved_period(
            lookback_days=int(setting(self.settings, "default_lookback_days", 30))
        )
        endpoint = join_url(
            self.consulta_base_url,
            "contratos/atualizacao" if filters.discovery_kind == "update" else "contratos",
        )
        page_size = min(max(filters.page_size or 50, 10), 50)
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
                    "dataInicial": start.strftime("%Y%m%d"),
                    "dataFinal": end.strftime("%Y%m%d"),
                    "cnpjOrgao": filters.agency_cnpj,
                    "codigoUnidadeAdministrativa": filters.unit_code,
                    "pagina": page,
                    "tamanhoPagina": page_size,
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
                    rows = list_payload(payload, "data", operation="PNCP contract discovery")
                except ConnectorContractError as exc:
                    errors.append(exc)
                    break
                invalid_rows += sum(
                    not bool(
                        row.get("numeroControlePNCP")
                        or (
                            (row.get("orgaoEntidade") or {}).get("cnpj")
                            and row.get("anoContrato")
                            and row.get("sequencialContrato")
                        )
                    )
                    for row in rows
                )
                contracts.extend(
                    self._map_contract(row, response.url).model_copy(
                        update={"source_record_ref": record.record_key}
                    )
                    for row in rows
                    if bool(
                        row.get("numeroControlePNCP")
                        or (
                            (row.get("orgaoEntidade") or {}).get("cnpj")
                            and row.get("anoContrato")
                            and row.get("sequencialContrato")
                        )
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
            partial = bool(errors or truncated or invalid_rows)
            return ConnectorResult(
                data=contracts,
                availability=DataAvailability.AVAILABLE,
                source_urls=unique_urls(records),
                raw_records=records,
                pagination=pagination,
                diagnostic=(
                    f"partial result: {invalid_rows} contract row(s) without official identity"
                    if invalid_rows
                    else "partial result: one or more pages were not collected"
                    if partial
                    else None
                ),
                partial=partial,
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
            diagnostic=(
                f"partial result: {invalid_rows} contract row(s) without official identity"
                if invalid_rows
                else "no contracts matched the official response"
            ),
        )

    async def fetch_contract_items(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractItem]]:
        """PNCP publishes contract headers; items come from Compras.gov.br."""

        del contract
        return ConnectorResult(
            data=[],
            availability=DataAvailability.NOT_SUPPORTED,
            diagnostic="PNCP publishes contract headers only; items come from Compras.gov.br",
        )

    async def fetch_contract_terms(
        self, contract: RawContract
    ) -> ConnectorResult[list[RawContractTerm]]:
        if not contract.agency_cnpj or contract.year is None or contract.sequence is None:
            return ConnectorResult(
                data=[],
                availability=DataAvailability.NOT_PUBLISHED,
                diagnostic="contract agency/year/sequence not published; terms cannot be queried",
            )
        path = f"orgaos/{contract.agency_cnpj}/contratos/{contract.year}/{contract.sequence}/termos"
        rows, refs, records, pagination, error = await self._collect_integration(path)
        by_ref = self._records_by_ref(records)
        valid = [
            (row, ref)
            for row, ref in zip(rows, refs, strict=True)
            if row.get("sequencialTermoContrato") is not None or row.get("numeroTermoContrato")
        ]
        partial = error is not None or len(valid) != len(rows) or pagination.truncated
        terms = [
            self._map_contract_term(row, contract, by_ref[ref].endpoint).model_copy(
                update={"source_record_ref": ref}
            )
            for row, ref in valid
        ]
        if not terms and error is not None:
            failure: ConnectorResult[list[RawContractTerm]] = failed_result([], error)
            failure.raw_records, failure.source_urls = records, unique_urls(records)
            failure.pagination, failure.partial = pagination, True
            return failure
        return ConnectorResult(
            data=terms,
            availability=(
                DataAvailability.AVAILABLE
                if terms
                else DataAvailability.TEMPORARY_ERROR
                if partial
                else DataAvailability.EMPTY
            ),
            source_urls=unique_urls(records),
            raw_records=records,
            pagination=pagination,
            partial=partial,
            diagnostic="partial result: contract term collection incomplete" if partial else None,
        )

    def _map_contract_term(
        self, row: dict[str, Any], contract: RawContract, source_url: str
    ) -> RawContractTerm:
        sequence = row.get("sequencialTermoContrato") or row.get("numeroTermoContrato")
        return RawContractTerm(
            source=self.name,
            source_url=source_url,
            raw_payload=row,
            external_id=f"{contract.external_id}:term:{sequence}",
            contract_external_id=contract.external_id,
            term_number=row.get("numeroTermoContrato")
            or (str(sequence) if sequence is not None else None),
            term_type=row.get("tipoTermoContratoNome"),
            signed_at=to_datetime(row.get("dataAssinatura"), self.timezone),
            published_at=to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            value_delta=to_decimal(row.get("valorAcrescido")),
            new_valid_until=to_datetime(row.get("dataVigenciaFim"), self.timezone),
            object_description=row.get("objetoTermoContrato"),
            status=row.get("informativoObservacao"),
        )

    def _map_contract(self, row: dict[str, Any], source_url: str) -> RawContract:
        agency = row.get("orgaoEntidade") or {}
        unit = row.get("unidadeOrgao") or {}
        control = row.get("numeroControlePNCP")
        agency_cnpj = normalized_identifier(agency.get("cnpj"))
        year = to_int(row.get("anoContrato"))
        sequence = row.get("sequencialContrato")
        external_id = str(control or f"{agency_cnpj}:{year}:{sequence}")
        return RawContract(
            source=self.name,
            source_updated_at=to_datetime(row.get("dataAtualizacao"), self.timezone),
            source_update_field="dataAtualizacao" if row.get("dataAtualizacao") else None,
            source_url=source_url,
            raw_payload=row,
            external_id=external_id,
            pncp_control_number=str(control) if control else None,
            procurement_control_number=row.get("numeroControlePncpCompra"),
            price_registry_control_number=row.get("numeroControlePncpAta"),
            contract_number=row.get("numeroContratoEmpenho"),
            year=year,
            sequence=to_int(sequence),
            process_number=row.get("processo"),
            agency_name=agency.get("razaoSocial"),
            agency_cnpj=agency_cnpj,
            uasg=unit.get("codigoUnidade") or unit.get("codigoUnidadeGestora"),
            object_description=row.get("objetoContrato"),
            signed_at=to_datetime(row.get("dataAssinatura"), self.timezone),
            published_at=to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            valid_from=to_datetime(row.get("dataVigenciaInicio"), self.timezone),
            valid_until=to_datetime(row.get("dataVigenciaFim"), self.timezone),
            initial_value=nonnegative_decimal(row.get("valorInicial")),
            global_value=nonnegative_decimal(row.get("valorGlobal")),
            accumulated_value=nonnegative_decimal(row.get("valorAcumulado")),
            is_adhesion=row.get("frutoAdesao"),
            supplier_cnpj=normalized_identifier(row.get("niFornecedor")),
            supplier_name=row.get("nomeRazaoSocialFornecedor"),
        )

    async def fetch_price_registry_items(
        self, external_id: str
    ) -> ConnectorResult[list[RawPriceRegistryItem]]:
        """PNCP publishes the ata header but no public item/supplier endpoint."""

        del external_id
        return ConnectorResult(
            data=[],
            availability=DataAvailability.NOT_SUPPORTED,
            diagnostic=(
                "PNCP publishes the ata header only; items/suppliers come from "
                "Compras.gov.br ARP or official documents"
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
                raise ValueError("PNCP modality code must be positive")
            return code
        key = _canonical_modality(str(value))
        try:
            return PNCP_MODALITY_CODES[key]
        except KeyError as exc:
            raise ValueError(f"unknown PNCP modality: {value}") from exc

    @staticmethod
    def _discovery_kind(value: str) -> str:
        canonical = _canonical_modality(value)
        aliases = {
            "publication": "publicacao",
            "publicacao": "publicacao",
            "update": "atualizacao",
            "atualizacao": "atualizacao",
            "open_proposals": "proposta",
            "proposals": "proposta",
            "proposta": "proposta",
        }
        try:
            return aliases[canonical]
        except KeyError as exc:
            raise ValueError(f"unsupported PNCP discovery kind: {value}") from exc

    def _matches_client_filters(self, row: dict[str, Any], filters: ProcurementFilters) -> bool:
        agency = row.get("orgaoEntidade") or {}
        unit = row.get("unidadeOrgao") or {}
        if filters.agency and not contains_text(
            [agency.get("razaoSocial"), unit.get("nomeUnidade")], filters.agency
        ):
            return False
        if filters.municipality and not contains_text(
            [unit.get("municipioNome")], filters.municipality
        ):
            return False
        return not filters.keyword or contains_text(
            [
                row.get("objetoCompra"),
                row.get("processo"),
                row.get("informacaoComplementar"),
            ],
            filters.keyword,
        )

    def _map_procurement(
        self,
        row: dict[str, Any],
        source_url: str,
        *,
        detail: bool = False,
    ) -> RawProcurement | RawProcurementDetail:
        agency = row.get("orgaoEntidade") or {}
        unit = row.get("unidadeOrgao") or {}
        control = row.get("numeroControlePNCP")
        agency_cnpj = normalized_identifier(agency.get("cnpj"))
        year = to_int(row.get("anoCompra"))
        sequence = to_int(row.get("sequencialCompra"))
        external_id = str(control or f"{agency_cnpj}:{year}:{sequence}")
        modality = row.get("modalidadeNome")
        purchase_number = row.get("numeroCompra")
        model_type = RawProcurementDetail if detail else RawProcurement
        common: dict[str, Any] = {
            "source": self.name,
            "source_url": source_url,
            "raw_payload": row,
            "external_id": external_id,
            "pncp_control_number": control,
            "uasg": unit.get("codigoUnidade"),
            "purchase_number": str(purchase_number) if purchase_number is not None else None,
            "purchase_year": year,
            "process_number": row.get("processo"),
            "modality": modality,
            "modality_code": row.get("modalidadeId"),
            "title": " ".join(
                part for part in (str(modality or ""), str(purchase_number or "")) if part
            )
            or None,
            "object_description": row.get("objetoCompra"),
            "agency_name": agency.get("razaoSocial") or unit.get("nomeUnidade"),
            "agency_cnpj": agency_cnpj,
            "government_sphere": agency.get("esferaId"),
            "government_branch": agency.get("poderId"),
            "uf": unit.get("ufSigla"),
            "municipality": unit.get("municipioNome"),
            "municipality_code": (
                str(unit.get("codigoIbge")) if unit.get("codigoIbge") is not None else None
            ),
            "estimated_value": nonnegative_decimal(row.get("valorTotalEstimado")),
            "homologated_value": nonnegative_decimal(row.get("valorTotalHomologado")),
            "is_srp": row.get("srp"),
            "legal_basis": self._legal_basis(row),
            "proposal_start_at": to_datetime(row.get("dataAberturaProposta"), self.timezone),
            "proposal_end_at": to_datetime(row.get("dataEncerramentoProposta"), self.timezone),
            "publication_at": to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            "last_source_update_at": to_datetime(
                row.get("dataAtualizacaoGlobal") or row.get("dataAtualizacao"), self.timezone
            ),
            "status": row.get("situacaoCompraNome"),
        }
        if detail:
            common["additional_data"] = {
                "information": row.get("informacaoComplementar"),
                "origin_system_url": row.get("linkSistemaOrigem"),
                "electronic_process_url": row.get("linkProcessoEletronico"),
                "has_results": row.get("existeResultado"),
            }
        return model_type(**common)

    @staticmethod
    def _legal_basis(row: dict[str, Any]) -> str | None:
        """Read the amparo legal in its documented object or flat form."""

        amparo = row.get("amparoLegal")
        if isinstance(amparo, dict):
            return amparo.get("nome") or amparo.get("descricao")
        if isinstance(amparo, str) and amparo.strip():
            return amparo
        return row.get("amparoLegalNome")

    def _map_price_registry(self, row: dict[str, Any], source_url: str) -> RawPriceRegistry:
        control = row.get("numeroControlePNCPAta")
        agency_cnpj = normalized_identifier(row.get("cnpjOrgao"))
        year = to_int(row.get("anoAta"))
        registry_number = row.get("numeroAtaRegistroPreco")
        external_id = str(control or f"{agency_cnpj}:{year}:{registry_number}")
        cancelled = bool(row.get("cancelado")) or bool(row.get("dataCancelamento"))
        return RawPriceRegistry(
            source=self.name,
            source_updated_at=to_datetime(row.get("dataAtualizacao"), self.timezone),
            source_update_field="dataAtualizacao" if row.get("dataAtualizacao") else None,
            source_url=source_url,
            raw_payload=row,
            external_id=external_id,
            pncp_control_number=str(control) if control else None,
            linked_pncp_control_number=row.get("numeroControlePNCPCompra"),
            registry_number=str(registry_number) if registry_number is not None else None,
            year=year,
            agency_name=row.get("nomeOrgao") or row.get("nomeUnidadeOrgao"),
            agency_cnpj=agency_cnpj,
            uasg=(
                str(row.get("codigoUnidadeOrgao"))
                if row.get("codigoUnidadeOrgao") is not None
                else None
            ),
            object_description=row.get("objetoContratacao"),
            status="cancelado" if cancelled else None,
            signed_at=to_datetime(row.get("dataAssinatura"), self.timezone),
            published_at=to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            valid_from=to_datetime(row.get("vigenciaInicio"), self.timezone),
            valid_until=to_datetime(row.get("vigenciaFim"), self.timezone),
            total_value=nonnegative_decimal(row.get("valorTotal")),
            allows_adhesion=row.get("possibilidadeAdesao"),
        )

    def _map_item(
        self,
        row: dict[str, Any],
        procurement_external_id: str,
        source_url: str,
        *,
        source_record_ref: str | None = None,
    ) -> RawProcurementItem:
        number = row.get("numeroItem")
        return RawProcurementItem(
            source=self.name,
            source_url=source_url,
            source_record_ref=source_record_ref,
            raw_payload=row,
            external_id=f"{procurement_external_id}:item:{number}",
            procurement_external_id=procurement_external_id,
            item_number=str(number) if number is not None else None,
            description=row.get("descricao"),
            detailed_description=row.get("informacaoComplementar"),
            quantity=nonnegative_decimal(row.get("quantidade")),
            unit=row.get("unidadeMedida"),
            estimated_unit_value=nonnegative_decimal(row.get("valorUnitarioEstimado")),
            estimated_total_value=nonnegative_decimal(row.get("valorTotal")),
            result_status=row.get("situacaoCompraItemNome"),
            has_result=row.get("temResultado"),
        )

    def _map_document(
        self,
        row: dict[str, Any],
        procurement_external_id: str,
        source_url: str,
        *,
        source_record_ref: str | None = None,
    ) -> RawDocument:
        sequence = row.get("sequencialDocumento")
        download_url = str(row.get("url") or row.get("uri"))
        external_key = sequence if sequence is not None else self._row_digest(row)[:16]
        return RawDocument(
            source=self.name,
            source_url=source_url,
            source_record_ref=source_record_ref,
            raw_payload=row,
            external_id=f"{procurement_external_id}:document:{external_key}",
            procurement_external_id=procurement_external_id,
            document_type=row.get("tipoDocumentoNome") or row.get("tipoDocumentoDescricao"),
            title=row.get("titulo"),
            download_url=download_url,
            published_at=to_datetime(row.get("dataPublicacaoPncp"), self.timezone),
            active=row.get("statusAtivo") is not False,
        )

    def _map_result(
        self,
        row: dict[str, Any],
        procurement_external_id: str,
        item: dict[str, Any],
        source_url: str,
        *,
        source_record_ref: str | None = None,
    ) -> RawResult:
        item_number = row.get("numeroItem", item.get("numeroItem"))
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
            source_record_ref=source_record_ref,
            raw_payload=row,
            external_id=f"{procurement_external_id}:item:{item_number}:result:{sequence}",
            procurement_external_id=procurement_external_id,
            item_external_id=f"{procurement_external_id}:item:{item_number}",
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
            result_at=to_datetime(
                row.get("dataResultadoPncp") or row.get("dataResultado"), self.timezone
            ),
            is_cancelled=is_cancelled,
            cancellation_reason=cancellation_reason,
        )

    async def _collect_integration(
        self, path: str
    ) -> tuple[
        list[dict[str, Any]],
        list[str],
        list[RawSourceRecord],
        PaginationInfo,
        Exception | None,
    ]:
        endpoint = join_url(self.integration_base_url, path)
        page_size = 50
        page = 1
        rows: list[dict[str, Any]] = []
        row_refs: list[str] = []
        records: list[RawSourceRecord] = []
        error: Exception | None = None
        total_pages: int | None = None
        total_records: int | None = None
        seen_hashes: set[str] = set()
        async with PageWindow(
            concurrency=int(setting(self.settings, "http_max_concurrency", 4)),
            max_pages=_MAX_DETAIL_PAGES,
        ) as window:
            while True:
                try:
                    response = await window.request(
                        self._http,
                        endpoint,
                        {"pagina": page, "tamanhoPagina": page_size},
                        total_pages=total_pages or 0,
                    )
                except _HTTP_EXCEPTIONS as exc:
                    error = exc
                    break
                record = source_record(self.name, response)
                if record.response_hash in seen_hashes:
                    error = ConnectorContractError(
                        f"PNCP detail endpoint repeated page {page}",
                        url=response.url,
                    )
                    break
                seen_hashes.add(record.response_hash)
                records.append(record)
                try:
                    page_rows = list_payload(
                        response.payload,
                        "data",
                        "itens",
                        "documentos",
                        "listaResultados",
                        "resultados",
                        "termos",
                        operation="PNCP integration detail",
                    )
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
                        params={"pagina": page, "tamanhoPagina": page_size},
                    )
                    and not window.has_pending
                ):
                    error = ConnectorContractError(
                        f"PNCP detail exceeded {_MAX_DETAIL_PAGES} pages",
                        url=response.url,
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
    def _records_by_ref(records: list[RawSourceRecord]) -> dict[str, RawSourceRecord]:
        return {record.record_key: record for record in records}

    @staticmethod
    def _row_digest(row: dict[str, Any]) -> str:
        encoded = json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _has_procurement_identity(row: dict[str, Any]) -> bool:
        if row.get("numeroControlePNCP"):
            return True
        agency = row.get("orgaoEntidade") or {}
        return bool(
            normalized_identifier(agency.get("cnpj"))
            and to_int(row.get("anoCompra")) is not None
            and to_int(row.get("sequencialCompra")) is not None
        )

    @staticmethod
    def _has_item_identity(row: dict[str, Any]) -> bool:
        return to_int(row.get("numeroItem")) is not None

    @staticmethod
    def _has_document_identity(row: dict[str, Any]) -> bool:
        return bool(row.get("url") or row.get("uri"))

    @staticmethod
    def _has_result_identity(row: dict[str, Any]) -> bool:
        return to_int(row.get("numeroItem")) is not None and bool(
            normalized_identifier(row.get("niFornecedor")) or row.get("nomeRazaoSocialFornecedor")
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
