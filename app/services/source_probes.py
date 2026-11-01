"""Bounded, read-only probes using the actual source adapters and validators."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from app.config import Settings
from app.connectors._common import list_payload
from app.connectors.base import DataAvailability, ProcurementFilters
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient, ConnectorContractError, ConnectorHTTPError
from app.connectors.municipal_payments.fiorilli import FiorilliPaymentConnector
from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
from app.connectors.municipal_payments.sources import selected_municipal_sources
from app.connectors.pncp import PNCPConnector
from app.connectors.transparencia import TransparenciaPaymentsConnector


async def probe_connectivity(settings: Settings) -> list[tuple[str, DataAvailability, str]]:
    selected = settings.model_copy(update={"http_max_retries": 0, "http_timeout_seconds": 10})
    outcomes: list[tuple[str, DataAvailability, str]] = []
    for connector_type in (PNCPConnector, ComprasGovConnector):
        discovery_transport = AsyncHTTPClient(
            selected, public_only=True, allow_http=selected.allow_insecure_http_sources
        )
        connector = connector_type(selected, http_client=discovery_transport)
        try:
            result = await connector.discover_procurements(
                ProcurementFilters(
                    days=2,
                    uf=selected.default_uf,
                    modalities=["pregao_eletronico"],
                    page_size=10,
                    max_pages=1,
                )
            )
            outcomes.append(
                (
                    connector.name,
                    result.availability,
                    result.diagnostic or "contrato de resposta reconhecido",
                )
            )
        except Exception as exc:
            outcomes.append((connector.name, DataAvailability.TEMPORARY_ERROR, type(exc).__name__))
        finally:
            await connector.aclose()
            await discovery_transport.aclose()
    year = datetime.now(UTC).year
    async with httpx.AsyncClient(timeout=10, follow_redirects=True, trust_env=False) as client:
        transport = AsyncHTTPClient(
            selected,
            client=client,
            max_retries=0,
            public_only=True,
            allow_http=selected.allow_insecure_http_sources,
        )
        for source in selected_municipal_sources(selected):
            try:
                if source.base_url.startswith("http:") and not selected.allow_insecure_http_sources:
                    outcomes.append(
                        (
                            source.name,
                            DataAvailability.NOT_SUPPORTED,
                            "HTTP requer configuração explícita",
                        )
                    )
                    continue
                if source.is_fiorilli:
                    adapter = FiorilliPaymentConnector(source, selected, http_client=transport)
                    payment_result = await adapter.discover_payments(
                        year=year, include_details=False
                    )
                else:
                    adapter_csv = SaoLuisCsvConnector(source, selected, client=client)
                    payment_result = await adapter_csv.discover_payments(year=year)
                status = payment_result.availability
                if payment_result.partial or status is DataAvailability.NOT_SUPPORTED:
                    status = DataAvailability.TEMPORARY_ERROR
                outcomes.append(
                    (
                        source.name,
                        status,
                        payment_result.diagnostic or "estrutura de despesas reconhecida",
                    )
                )
            except (ConnectorHTTPError, httpx.HTTPStatusError) as exc:
                code = (
                    exc.status_code
                    if isinstance(exc, ConnectorHTTPError)
                    else exc.response.status_code
                )
                status = (
                    DataAvailability.ACCESS_RESTRICTED
                    if code in {401, 403, 429}
                    else DataAvailability.TEMPORARY_ERROR
                )
                outcomes.append((source.name, status, f"HTTP {code}"))
            except Exception as exc:
                outcomes.append((source.name, DataAvailability.TEMPORARY_ERROR, type(exc).__name__))
        cnpj = next(iter(selected.payment_poll_cnpjs), None)
        if selected.transparencia_api_key and cnpj:
            try:
                federal = TransparenciaPaymentsConnector(selected, http_client=transport)
                response = await transport.request_json(
                    "GET",
                    federal.base_url + "/api-de-dados/despesas/documentos-por-favorecido",
                    params={"codigoPessoa": cnpj, "ano": year, "fase": 3, "pagina": 1},
                    headers={"chave-api-dados": selected.transparencia_api_key},
                )
                rows = list_payload(response.payload, operation="Transparência")
                if any(not federal._has_payment_identity(row) for row in rows):
                    raise ConnectorContractError("pagamento sem identidade")
                outcomes.append(
                    (
                        "Transparência",
                        DataAvailability.AVAILABLE if rows else DataAvailability.EMPTY,
                        "estrutura reconhecida",
                    )
                )
            except Exception as exc:
                outcomes.append(
                    ("Transparência", DataAvailability.TEMPORARY_ERROR, type(exc).__name__)
                )
    return outcomes
