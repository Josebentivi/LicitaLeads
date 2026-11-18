"""Read-only client for the TCE-MA public SINC-Contrata collections."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import httpx

from app.connectors._common import error_availability, error_diagnostic, source_record
from app.connectors.base import ConnectorResult, DataAvailability, PaginationInfo, RawSourceRecord
from app.connectors.http import (
    AsyncHTTPClient,
    ConnectorContractError,
    ConnectorDecodeError,
    ConnectorHTTPError,
    observation_time,
)

TCE_MA_BASE_URL = "https://tce-api.apps.tcema.tc.br/sinccontrata"


class SincContrataResource(StrEnum):
    PROCEDURES = "procedimentolicitatorio"
    BIDDERS = "licitante"
    RESULTS = "resultado"
    CONTRACTS = "contrato"


@dataclass(frozen=True, slots=True)
class SincContrataRow:
    payload: dict[str, Any]
    source_record_ref: str


def _page_metadata(
    payload: Any, requested_page: int, ente_id: int
) -> tuple[list[dict[str, Any]], int, int, int]:
    if not isinstance(payload, dict) or not isinstance(payload.get("content"), list):
        raise ConnectorContractError("envelope content do SINC-Contrata inválido")
    rows = payload["content"]
    page = payload.get("number")
    size = payload.get("size")
    pages = payload.get("totalPages")
    total = payload.get("totalElements")
    if not (type(page) is int and type(size) is int and type(pages) is int and type(total) is int):
        raise ConnectorContractError("metadados de paginação do SINC-Contrata ausentes")
    if (
        page != requested_page
        or size < 1
        or total < 0
        or pages != (total + size - 1) // size
        or (page >= pages and (page != 0 or total != 0))
        or len(rows) != min(size, max(0, total - page * size))
        or payload.get("last") is not (page + 1 >= pages)
    ):
        raise ConnectorContractError("paginação do SINC-Contrata inconsistente")
    if any(
        not isinstance(row, dict)
        or str(row.get("enteIdEnvio")) != str(ente_id)
        or not isinstance(row.get("id"), (str, int))
        or not str(row["id"]).strip()
        for row in rows
    ):
        raise ConnectorContractError("linha do SINC-Contrata sem identidade ou fora do ente")
    return rows, size, pages, total


class TCEMAConnector:
    """Read the municipal inventory, with coverage determined by the observed pages."""

    name = "tce_ma"

    def __init__(self, http_client: AsyncHTTPClient) -> None:
        self._http = http_client

    async def fetch(
        self,
        resource: SincContrataResource,
        *,
        ente_id: int,
        page_size: int = 500,
        max_pages: int = 25,
    ) -> ConnectorResult[list[SincContrataRow]]:
        if ente_id < 1 or not 1 <= page_size <= 500 or max_pages < 1:
            raise ValueError("ente, tamanho ou limite de páginas inválido para o TCE-MA")
        endpoint = f"{TCE_MA_BASE_URL}/{resource.value}"
        records: list[RawSourceRecord] = []
        rows: list[SincContrataRow] = []
        seen_ids: set[str] = set()
        metadata: tuple[int, int, int] | None = None
        error: Exception | None = None
        fetched = 0
        for page in range(max_pages):
            # The published ano/cnpj parameters did not scope these collections in production.
            params = {"enteId": ente_id, "page": page, "size": page_size}
            observed_at = observation_time()
            try:
                response = await self._http.request_json("GET", endpoint, params=params)
                record = source_record(self.name, response)
                records.append(record)
                content, size, pages, total = _page_metadata(response.payload, page, ente_id)
                if metadata is not None and metadata != (size, pages, total):
                    raise ConnectorContractError(
                        "total ou tamanho da página mudou durante a coleta"
                    )
                metadata = size, pages, total
                ids = {str(row["id"]) for row in content}
                if len(ids) != len(content) or seen_ids.intersection(ids):
                    raise ConnectorContractError("identificadores repetidos na paginação do TCE-MA")
            except ConnectorContractError as exc:
                error = exc
                break
            except (ConnectorHTTPError, ConnectorDecodeError, httpx.TransportError) as exc:
                error = exc
                http_status = (
                    exc.status_code
                    if isinstance(exc, (ConnectorHTTPError, ConnectorDecodeError))
                    else None
                )
                records.append(
                    RawSourceRecord.build(
                        source=self.name,
                        endpoint=endpoint,
                        request_parameters=params,
                        raw_payload=None,
                        http_status=http_status or 0,
                        collected_at=observed_at,
                    )
                )
                break
            seen_ids.update(ids)
            rows.extend(SincContrataRow(row, record.record_key) for row in content)
            fetched += 1
            if fetched >= pages:
                break

        observed_size, total_pages, total_records = metadata or (page_size, None, None)
        remaining = max(0, total_pages - fetched) if total_pages is not None else None
        truncated = error is None and bool(remaining)
        availability = (
            DataAvailability.AVAILABLE
            if rows
            else error_availability(error)
            if error
            else DataAvailability.EMPTY
        )
        diagnostic = None
        if isinstance(error, ConnectorHTTPError):
            diagnostic = f"TCE-MA retornou HTTP {error.status_code}"
        elif error is not None:
            diagnostic = error_diagnostic(error)
        elif truncated:
            diagnostic = "limite de páginas atingido no TCE-MA"
        return ConnectorResult(
            data=rows,
            raw_records=records,
            source_urls=list(dict.fromkeys(record.endpoint for record in records)),
            availability=availability,
            partial=error is not None or truncated,
            diagnostic=diagnostic,
            pagination=PaginationInfo(
                page=fetched,
                page_size=observed_size,
                total_records=total_records,
                total_pages=total_pages,
                pages_remaining=remaining,
                fetched_pages=fetched,
                truncated=truncated,
            ),
        )
