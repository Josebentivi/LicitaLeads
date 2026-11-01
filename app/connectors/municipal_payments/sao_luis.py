"""São Luís open-data CSV adapter with an explicit manual fallback."""

from __future__ import annotations

import asyncio
import base64
import csv
import io
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

import httpx

from app.config import Settings, get_settings
from app.connectors._common import observed_pages
from app.connectors.base import (
    ConnectorResult,
    DataAvailability,
    PaginationInfo,
    RawPayment,
    RawSourceRecord,
)
from app.connectors.bounded_http import SourceDownloadError, bounded_request
from app.connectors.http import ConnectorContractError
from app.connectors.municipal_payments.base import (
    MunicipalSource,
    clean_text,
    normalize_key,
    parse_brazilian_date,
    parse_brazilian_money,
    row_fingerprint,
    source_name,
)
from app.connectors.progress import ActivityCallback, ProgressEvent
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

_ALIASES: dict[str, tuple[str, ...]] = {
    "supplier_name": (
        "supplier_name",
        "fornecedor",
        "nome_fornecedor",
        "nome_do_fornecedor",
        "credor",
        "nome_credor",
        "nome_do_credor",
        "razao_social",
        "descricao",
    ),
    "supplier_cnpj": (
        "supplier_cnpj",
        "cnpj",
        "cnpj_fornecedor",
        "cnpj_do_fornecedor",
        "cpf_cnpj",
        "cpfcnpj",
        "cnpj_cpf",
        "cpf_cnpj_do_credor",
        "cpf_cnpj_fornecedor",
        "documento_fornecedor",
        "insmf",
    ),
    "agency_name": (
        "agency_name",
        "orgao",
        "nome_orgao",
        "nome_do_orgao",
        "unidade",
        "nome_unidade",
    ),
    "agency_cnpj": ("agency_cnpj", "cnpj_orgao"),
    "contract_number": (
        "contract_number",
        "contrato",
        "numero_contrato",
        "numero_do_contrato",
    ),
    "process_number": (
        "process_number",
        "processo",
        "numero_processo",
        "numero_do_processo",
    ),
    "payment_date": (
        "payment_date",
        "data",
        "data_pagamento",
        "data_do_pagamento",
        "data_movimento",
        "dt_pagamento",
    ),
    "amount": (
        "amount",
        "valor",
        "valor_pago",
        "valor_pagamento",
        "valor_do_pagamento",
        "valor_total",
        "vl_pagamento",
        "pago",
    ),
    "phase": ("phase", "fase", "fase_da_despesa", "etapa"),
    "document_number": (
        "document_number",
        "documento",
        "numero_documento",
        "numero_pagamento",
        "ordem_pagamento",
        "ob",
    ),
    "commitment_number": ("commitment_number", "empenho", "numero_empenho"),
    "status": ("status", "situacao"),
}
_HTML_LINK_RE = re.compile(
    r"""<a[^>]+href\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a>""",
    re.IGNORECASE | re.DOTALL,
)
_MAX_PAYLOAD_BYTES = 20 * 1024 * 1024


class SaoLuisCsvConnector:
    """Read the public CSV/HTML page published by São Luís.

    Cloudflare currently blocks this network with HTTP 403.  The connector
    reports that honestly and keeps a configurable direct URL so an operator
    can point it at a stable export without changing code.
    """

    def __init__(
        self,
        source: MunicipalSource,
        settings: Settings | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.source = source
        self.settings = settings or get_settings()
        self._owns_client = client is None
        self.checkpoint: Callable[[], Awaitable[None]] | None = None
        self.on_activity: ActivityCallback | None = None
        self._client = client or httpx.AsyncClient(
            trust_env=False,
            timeout=httpx.Timeout(self.settings.http_timeout_seconds),
            follow_redirects=True,
            max_redirects=3,
            headers={
                "Accept": "text/csv,application/json,text/html;q=0.9,*/*;q=0.8",
                "User-Agent": self.settings.http_user_agent,
            },
        )

    @property
    def name(self) -> str:
        return source_name(self.source)

    async def discover_payments(
        self,
        *,
        year: int,
        company_cnpj: str | None = None,
        include_details: bool = False,
    ) -> ConnectorResult[list[RawPayment]]:
        del include_details
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

        url = self.settings.sao_luis_payments_url.strip() or self.source.base_url
        try:
            if self.checkpoint is not None:
                await self.checkpoint()
            response = await self._download(url)
        except (httpx.TransportError, TimeoutError, SourceDownloadError) as exc:
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                f"{self.source.name}: falha de transporte ({type(exc).__name__})",
                http_status=None,
                url=url,
            )
        if response.status_code in {401, 403}:
            return self._error(
                DataAvailability.ACCESS_RESTRICTED,
                (
                    f"{self.source.name}: acesso automatizado bloqueado "
                    f"(HTTP {response.status_code}); use a exportacao manual"
                ),
                http_status=response.status_code,
                url=url,
            )
        if response.status_code in {400, 404, 405, 422}:
            return self._error(
                DataAvailability.NOT_SUPPORTED,
                f"{self.source.name}: recurso nao encontrado (HTTP {response.status_code})",
                http_status=response.status_code,
                url=url,
            )
        if response.is_error:
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                f"{self.source.name}: fonte indisponivel (HTTP {response.status_code})",
                http_status=response.status_code,
                url=url,
            )
        if len(response.content) > _MAX_PAYLOAD_BYTES:
            return self._error(
                DataAvailability.NOT_SUPPORTED,
                f"{self.source.name}: resposta maior que 20 MB",
                http_status=response.status_code,
                url=url,
            )

        url = str(response.url)
        content_type = response.headers.get("content-type", "").lower()
        if "json" in content_type:
            return await self._json_result(
                response.content,
                normalized_cnpj=normalized_cnpj,
                year=year,
                url=url,
                http_status=response.status_code,
                raw_payload={"excerpt": self._decode(response.content)[:5000]},
            )
        if "csv" in content_type or url.lower().endswith(".csv"):
            return await self._csv_result(
                response.content,
                normalized_cnpj=normalized_cnpj,
                year=year,
                url=url,
                http_status=response.status_code,
            )

        text = self._decode(response.content)
        lines = text.splitlines()
        first_line = lines[0].lstrip() if lines else ""
        if first_line.startswith(("{", "[")):
            return await self._json_result(
                response.content,
                normalized_cnpj=normalized_cnpj,
                year=year,
                url=url,
                http_status=response.status_code,
                raw_payload={"excerpt": text[:5000]},
            )
        if not text.lstrip().startswith(("<", "<!")) and any(
            separator in first_line for separator in (";", ",", "\t", "|")
        ):
            return await self._csv_result(
                response.content,
                normalized_cnpj=normalized_cnpj,
                year=year,
                url=url,
                http_status=response.status_code,
            )
        if text.lstrip().startswith(("<", "<!")):
            links = self._csv_links(text, url)
            if not links:
                return self._error(
                    DataAvailability.NOT_SUPPORTED,
                    (
                        f"{self.source.name}: pagina acessivel, mas nenhum CSV "
                        "direto localizado; configure SAO_LUIS_PAYMENTS_URL"
                    ),
                    http_status=response.status_code,
                    url=url,
                    raw_payload={"html_excerpt": text[:10000]},
                )
            selected_links = self._links_for_year(links, year)
            if not selected_links:
                return self._error(
                    DataAvailability.NOT_SUPPORTED,
                    (
                        f"{self.source.name}: nao foi possivel identificar os arquivos CSV "
                        f"do exercicio {year}; use a exportacao manual"
                    ),
                    http_status=response.status_code,
                    url=url,
                    raw_payload={"html_excerpt": text[:10000]},
                )
            payments: list[RawPayment] = []
            raw_records: list[RawSourceRecord] = []
            source_urls: list[str] = []
            notes: list[str] = []
            access_restricted = False
            for position, link in enumerate(selected_links, start=1):
                try:
                    if self.checkpoint is not None:
                        await self.checkpoint()
                    csv_response = await self._download(link)
                    if self.on_activity is not None:
                        await self.on_activity(
                            ProgressEvent(
                                operation="files",
                                page=position,
                                total_pages=len(selected_links),
                                message="Arquivo recebido do portal municipal",
                            )
                        )
                except (httpx.TransportError, TimeoutError, SourceDownloadError) as exc:
                    notes.append(f"{link}: falha de transporte ({type(exc).__name__})")
                    continue
                if csv_response.status_code in {401, 403}:
                    access_restricted = True
                    notes.append(f"{link}: HTTP {csv_response.status_code}")
                    continue
                if csv_response.is_error:
                    notes.append(f"{link}: HTTP {csv_response.status_code}")
                    continue
                if len(csv_response.content) > _MAX_PAYLOAD_BYTES:
                    notes.append(f"{link}: arquivo maior que 20 MB")
                    continue
                file_result = await self._csv_result(
                    csv_response.content,
                    normalized_cnpj=normalized_cnpj,
                    year=year,
                    url=str(csv_response.url),
                    http_status=csv_response.status_code,
                )
                payments.extend(file_result.data)
                raw_records.extend(file_result.raw_records)
                source_urls.extend(file_result.source_urls)
                if file_result.diagnostic:
                    notes.append(file_result.diagnostic)
            max_records = self.settings.municipal_payment_max_records
            if max_records and len(payments) > max_records:
                notes.append(
                    f"{self.source.name}: limitado a {max_records} pagamento(s) "
                    f"({len(payments)} encontrados)"
                )
                payments = payments[:max_records]
            if not payments and notes:
                availability = (
                    DataAvailability.ACCESS_RESTRICTED
                    if access_restricted
                    else DataAvailability.TEMPORARY_ERROR
                )
                result = self._error(
                    availability,
                    f"{self.source.name}: coleta dos CSVs falhou; {'; '.join(notes)}",
                    http_status=response.status_code,
                    url=url,
                    raw_payload={"html_excerpt": text[:10000], "links": selected_links},
                )
                result.raw_records = [*result.raw_records, *raw_records]
                result.source_urls = list(dict.fromkeys([*result.source_urls, *source_urls]))
                return result
            return ConnectorResult(
                data=payments,
                availability=DataAvailability.AVAILABLE if payments else DataAvailability.EMPTY,
                source_urls=list(dict.fromkeys(source_urls)),
                raw_records=raw_records,
                diagnostic="; ".join(notes) or None,
                partial=bool(notes),
            )

        return self._error(
            DataAvailability.NOT_SUPPORTED,
            f"{self.source.name}: formato nao reconhecido ({content_type or 'desconhecido'})",
            http_status=response.status_code,
            url=url,
            raw_payload={"excerpt": text[:5000]},
        )

    async def _download(self, url: str) -> httpx.Response:
        if self.on_activity is not None:
            await self.on_activity(ProgressEvent(message="Consultando portal municipal"))
        return await bounded_request(
            self._client,
            "GET",
            url,
            max_bytes=_MAX_PAYLOAD_BYTES,
            public_only=True,
            allow_http=self.settings.allow_insecure_http_sources,
        )

    async def _csv_result(
        self,
        content: bytes,
        *,
        normalized_cnpj: str | None,
        year: int,
        url: str,
        http_status: int,
    ) -> ConnectorResult[list[RawPayment]]:
        raw: dict[str, Any] = {
            "format": "csv",
            "content_base64": base64.b64encode(content).decode("ascii"),
        }
        try:
            rows = await asyncio.to_thread(self._csv_rows, content)
        except ConnectorContractError as exc:
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                str(exc),
                http_status=http_status,
                url=url,
                raw_payload=raw,
            )
        raw.update({"rows": rows, "row_count": len(rows)})
        return self._result_from_rows(
            rows,
            normalized_cnpj=normalized_cnpj,
            year=year,
            url=url,
            http_status=http_status,
            raw_payload=raw,
        )

    async def _json_result(
        self,
        content: bytes,
        *,
        normalized_cnpj: str | None,
        year: int,
        url: str,
        http_status: int,
        raw_payload: Any,
    ) -> ConnectorResult[list[RawPayment]]:
        try:
            rows = await asyncio.to_thread(self._json_rows_safe, content)
        except ConnectorContractError as exc:
            return self._error(
                DataAvailability.TEMPORARY_ERROR,
                str(exc),
                http_status=http_status,
                url=url,
                raw_payload={"content_base64": base64.b64encode(content).decode("ascii")},
            )
        original = json.loads(self._decode(content))
        result = self._result_from_rows(
            rows,
            normalized_cnpj=normalized_cnpj,
            year=year,
            url=url,
            http_status=http_status,
            raw_payload=original,
        )
        try:
            pages = observed_pages(
                original,
                1,
                row_count=len(rows),
                collected_rows=len(rows),
                page_size=max(len(rows) + 1, 50),
            )
        except ConnectorContractError as exc:
            result.partial = True
            result.diagnostic = str(exc)
            if not result.data:
                result.availability = DataAvailability.TEMPORARY_ERROR
            return result
        result.pagination = PaginationInfo(
            page=1,
            fetched_pages=1,
            total_pages=pages,
            pages_remaining=max(pages - 1, 0),
            total_records=original.get("totalRegistros")
            if isinstance(original, dict)
            else len(rows),
            truncated=pages > 1,
            continuation={"version": 1, "supported": False} if pages > 1 else None,
        )
        if pages > 1:
            result.partial = True
            result.diagnostic = (
                "resposta JSON paginada; coleção de pagamentos incompleta; "
                "contrato de continuação não suportado nesta fonte; importe a exportação completa"
            )
        return result

    def _result_from_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        normalized_cnpj: str | None,
        year: int,
        url: str,
        http_status: int,
        raw_payload: Any,
    ) -> ConnectorResult[list[RawPayment]]:
        raw = RawSourceRecord.build(
            source=self.name,
            endpoint=url,
            request_parameters={},
            raw_payload=raw_payload,
            http_status=http_status,
            collected_at=datetime.now(UTC),
        )
        payments: list[RawPayment] = []
        errors = 0
        out_of_period = 0
        undated = 0
        for row in rows:
            normalized = {normalize_key(str(key)): value for key, value in row.items()}
            if normalized_cnpj and self._row_cnpj(normalized) != normalized_cnpj:
                continue
            payment = self._map_row(
                normalized,
                url=url,
                source_record_ref=raw.record_key,
            )
            if payment is None:
                errors += 1
                continue
            if payment.payment_date is None:
                undated += 1
            elif payment.payment_date.year != year:
                out_of_period += 1
                continue
            payments.append(payment)

        max_records = self.settings.municipal_payment_max_records
        diagnostic = None
        truncated = False
        if max_records and len(payments) > max_records:
            diagnostic = (
                f"{self.source.name}: limitado a {max_records} pagamento(s) "
                f"({len(payments)} encontrados)"
            )
            payments = payments[:max_records]
            truncated = True
        notes: list[str] = []
        if errors:
            notes.append(f"{errors} linha(s) sem valor/fornecedor reconhecivel")
        if out_of_period:
            notes.append(f"{out_of_period} linha(s) fora do ano {year}")
        if undated:
            notes.append(f"{undated} linha(s) sem data; periodo nao confirmado")
        if notes:
            suffix = "; ".join(notes)
            diagnostic = f"{diagnostic}; {suffix}" if diagnostic else suffix
        availability = (
            DataAvailability.AVAILABLE
            if payments
            else DataAvailability.TEMPORARY_ERROR
            if errors
            else DataAvailability.EMPTY
        )
        return ConnectorResult(
            data=payments,
            availability=availability,
            source_urls=[url],
            raw_records=[raw],
            diagnostic=diagnostic,
            partial=bool(errors or truncated or undated),
        )

    def _map_row(
        self,
        row: dict[str, Any],
        *,
        url: str,
        source_record_ref: str,
    ) -> RawPayment | None:
        amount = parse_brazilian_money(self._pick(row, "amount"))
        supplier_name = clean_text(self._pick(row, "supplier_name"))
        supplier_cnpj = normalize_cnpj(self._pick(row, "supplier_cnpj"))
        if amount is None or amount <= 0 or (not supplier_name and not supplier_cnpj):
            return None
        if supplier_cnpj and not is_valid_cnpj(supplier_cnpj):
            supplier_cnpj = None
        agency_cnpj = normalize_cnpj(self._pick(row, "agency_cnpj"))
        if agency_cnpj and not is_valid_cnpj(agency_cnpj):
            agency_cnpj = None
        payment_date = parse_brazilian_date(self._pick(row, "payment_date"))
        return RawPayment(
            source=self.name,
            source_url=url,
            raw_payload=row,
            source_record_ref=source_record_ref,
            external_id=row_fingerprint(self.source.id, row),
            contract_number=clean_text(self._pick(row, "contract_number")),
            agency_name=clean_text(self._pick(row, "agency_name")),
            agency_cnpj=agency_cnpj,
            process_number=clean_text(self._pick(row, "process_number")),
            payment_date=payment_date,
            amount=amount,
            phase=clean_text(self._pick(row, "phase")) or "pago",
            document_number=clean_text(self._pick(row, "document_number")),
            commitment_number=clean_text(self._pick(row, "commitment_number")),
            status=(
                clean_text(self._pick(row, "status"))
                or ("periodo_nao_confirmado" if payment_date is None else "pagamento_municipal")
            ),
            supplier_cnpj=supplier_cnpj,
            supplier_name=supplier_name,
        )

    def _row_cnpj(self, row: dict[str, Any]) -> str | None:
        value = normalize_cnpj(self._pick(row, "supplier_cnpj"))
        return value if is_valid_cnpj(value) else None

    @staticmethod
    def _pick(row: dict[str, Any], field_name: str) -> str | None:
        for key in _ALIASES[field_name]:
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    @staticmethod
    def _decode(content: bytes) -> str:
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        return content.decode("utf-8", errors="replace")

    def _csv_rows(self, content: bytes) -> list[dict[str, Any]]:
        text = self._decode(content)
        if not text.strip():
            raise ConnectorContractError("CSV vazio não confirma estrutura da fonte")
        try:
            dialect = csv.Sniffer().sniff(text[:4096], delimiters=";,|\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        fields = {normalize_key(key) for key in (reader.fieldnames or [])}
        if not (
            fields.intersection(_ALIASES["amount"])
            and fields.intersection(_ALIASES["supplier_name"] + _ALIASES["supplier_cnpj"])
        ):
            raise ConnectorContractError("CSV sem colunas de valor e fornecedor reconhecidas")
        return [dict(row) for row in reader]

    def _json_rows_safe(self, content: bytes) -> list[dict[str, Any]]:
        try:
            return self._json_rows(content)
        except (ConnectorContractError, ValueError) as exc:
            raise ConnectorContractError(
                f"{self.source.name}: JSON response does not match the payment contract: {exc}"
            ) from exc

    def _json_rows(self, content: bytes) -> list[dict[str, Any]]:
        try:
            payload = json.loads(self._decode(content))
        except ValueError as exc:
            raise ConnectorContractError(f"{self.source.name}: invalid JSON response") from exc
        if isinstance(payload, dict):
            for key in ("payments", "pagamentos", "items", "data", "despesas"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    payload = candidate
                    break
        if not isinstance(payload, list):
            raise ConnectorContractError(f"{self.source.name}: JSON envelope has no payment list")
        if any(not isinstance(row, dict) for row in payload):
            raise ConnectorContractError(f"{self.source.name}: JSON contains a non-object payment")
        return payload

    @staticmethod
    def _links_for_year(links: list[str], year: int) -> list[str]:
        matching: list[str] = []
        dated = 0
        for link in links:
            years = {int(item) for item in re.findall(r"(?<!\d)(20\d{2})(?!\d)", link)}
            if years:
                dated += 1
            if year in years:
                matching.append(link)
        if matching:
            return matching
        if dated == 0 and len(links) == 1:
            return links
        return []

    @staticmethod
    def _csv_links(html: str, base_url: str) -> list[str]:
        scored: list[tuple[int, int, str]] = []
        for position, (href, label) in enumerate(_HTML_LINK_RE.findall(html)):
            href = href.strip()
            if not href or href.lower().startswith(("javascript:", "mailto:", "#")):
                continue
            absolute = urljoin(base_url, href)
            haystack = f"{absolute} {label}".lower()
            if not absolute.lower().endswith(".csv") and not (
                "csv" in haystack and ("despesa" in haystack or "pagamento" in haystack)
            ):
                continue
            score = 2 if "despesa" in haystack or "pagamento" in haystack else 1
            scored.append((score, position, absolute))
        result: list[str] = []
        for _score, _position, absolute in sorted(scored, key=lambda item: (-item[0], item[1])):
            if absolute not in result:
                result.append(absolute)
        return result

    def _error(
        self,
        availability: DataAvailability,
        diagnostic: str,
        *,
        http_status: int | None,
        url: str | None = None,
        raw_payload: Any | None = None,
    ) -> ConnectorResult[list[RawPayment]]:
        endpoint = url or self.settings.sao_luis_payments_url or self.source.base_url
        raw = RawSourceRecord.build(
            source=self.name,
            endpoint=endpoint,
            request_parameters={},
            raw_payload=raw_payload or {"error": diagnostic},
            http_status=http_status or 0,
            collected_at=datetime.now(UTC),
        )
        return ConnectorResult(
            data=[],
            availability=availability,
            source_urls=[endpoint],
            raw_records=[raw],
            diagnostic=diagnostic,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
