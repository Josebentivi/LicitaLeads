"""Bounded official-source validation; no domain persistence or production DB access."""

from __future__ import annotations

import argparse
import asyncio
import os
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


async def validate(output: Path) -> bool:
    import httpx

    from app.config import Settings
    from app.connectors import compras_gov, pncp
    from app.connectors._common import list_payload
    from app.connectors.base import DataAvailability, ProcurementFilters
    from app.connectors.http import AsyncHTTPClient
    from app.connectors.municipal_payments.sao_luis import SaoLuisCsvConnector
    from app.connectors.municipal_payments.sources import get_municipal_source
    from app.connectors.transparencia import TransparenciaPaymentsConnector
    from app.services.ingestion.audit import audit_sources

    settings = Settings(
        _env_file=".env",
        app_env="test",
        llm_enabled=False,
        contact_search_enabled=False,
        contact_domain_discovery_enabled=False,
        ocr_enabled=False,
        http_max_retries=0,
        http_timeout_seconds=15,
        http_min_request_interval_seconds=1,
    )
    pncp._MAX_DETAIL_PAGES = compras_gov._MAX_DETAIL_PAGES = 2
    rows: list[tuple[str, str, str]] = []
    failed = False

    def record(name: str, status: str, detail: str) -> None:
        nonlocal failed
        failed = failed or status in {"falha", "parcial"}
        rows.append((name, status, detail.replace("\n", " ").replace("|", "/")))
        print(f"{name}: {status}: {detail}", flush=True)

    for connector_type in (pncp.PNCPConnector, compras_gov.ComprasGovConnector):
        connector = connector_type(settings)
        try:
            async with asyncio.timeout(90):
                result = await connector.discover_procurements(
                    ProcurementFilters(
                        days=30,
                        uf="MA",
                        modalities=["pregao_eletronico"],
                        page_size=10,
                        max_pages=2,
                    )
                )
                if result.availability not in {DataAvailability.AVAILABLE, DataAvailability.EMPTY}:
                    record(connector.name, "falha", result.diagnostic or result.availability.value)
                    continue
                pagination = result.pagination
                record(
                    connector.name + " descoberta",
                    "validado",
                    f"{len(result.data)} registros; {pagination.fetched_pages if pagination else 0} páginas; amostra limitada a duas páginas",
                )
                if not result.data:
                    record(
                        connector.name + " detalhes",
                        "não verificado",
                        "janela de 30 dias sem amostra",
                    )
                    continue
                raw = result.data[0]
                detail = await connector.fetch_procurement(raw.external_id)
                items = await connector.fetch_items(raw.external_id)
                documents = await connector.fetch_documents(raw.external_id)
                for name, response in (
                    ("detalhe", detail),
                    ("itens", items),
                    ("documentos", documents),
                ):
                    acceptable = response.availability in {
                        DataAvailability.AVAILABLE,
                        DataAvailability.EMPTY,
                        DataAvailability.NOT_PUBLISHED,
                    }
                    if response.availability is DataAvailability.NOT_SUPPORTED:
                        record(
                            connector.name + " " + name,
                            "não suportado",
                            "fonte não expõe esta operação",
                        )
                    else:
                        record(
                            connector.name + " " + name,
                            "parcial"
                            if acceptable and response.partial
                            else "validado"
                            if acceptable
                            else "falha",
                            f"{response.availability.value}; respostas={len(response.raw_records)}; parcial={response.partial}; diagnóstico={response.diagnostic or 'nenhum'}",
                        )
                if connector.name == "pncp":
                    item = next(
                        (
                            item
                            for item in items.data
                            if item.raw_payload.get("temResultado") is True
                        ),
                        None,
                    )
                    if item is not None:
                        connector._item_cache[raw.external_id] = (
                            [item.raw_payload],
                            [],
                            items.pagination,
                        )
                        results = await connector.fetch_results(raw.external_id)
                        record(
                            "pncp resultado de um item",
                            "validado"
                            if results.availability
                            in {DataAvailability.AVAILABLE, DataAvailability.NOT_PUBLISHED}
                            else "falha",
                            f"{results.availability.value}; respostas={len(results.raw_records)}; parcial={results.partial}",
                        )
                    else:
                        record(
                            "pncp resultados",
                            "não verificado",
                            "amostra sem item com resultado publicado",
                        )
                else:
                    results = await connector.fetch_results(raw.external_id)
                    record(
                        "compras_gov resultados",
                        "validado"
                        if results.availability
                        in {DataAvailability.AVAILABLE, DataAvailability.NOT_PUBLISHED}
                        else "falha",
                        f"{results.availability.value}; respostas={len(results.raw_records)}; parcial={results.partial}",
                    )
        except Exception as exc:
            record(
                connector.name,
                "falha",
                f"{type(exc).__name__}"
                + (
                    f" HTTP {exc.response.status_code}"
                    if isinstance(exc, httpx.HTTPStatusError)
                    else ""
                ),
            )
        finally:
            await connector.aclose()

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        adapter = SaoLuisCsvConnector(get_municipal_source("sao_luis"), settings, client=client)
        try:
            landing = await client.get(settings.sao_luis_payments_url)
            landing.raise_for_status()
            links = adapter._links_for_year(
                adapter._csv_links(landing.text, str(landing.url)), datetime.now().year
            )
            if not links:
                record("São Luís CSVs", "falha", "página não expõe CSVs identificáveis para o ano")
            for link in links[:2]:
                response = await client.get(link)
                response.raise_for_status()
                if len(response.content) > 20 * 1024 * 1024:
                    raise ValueError("CSV excedeu limite")
                parsed = await asyncio.to_thread(adapter._csv_rows, response.content)
                record("São Luís CSV", "validado", f"{len(parsed)} linhas; {link}")
        except Exception as exc:
            record(
                "São Luís CSVs",
                "falha",
                f"{type(exc).__name__}"
                + (
                    f" HTTP {exc.response.status_code}"
                    if isinstance(exc, httpx.HTTPStatusError)
                    else ""
                ),
            )
        for source_id in ("imperatriz", "timon", "caxias"):
            source = get_municipal_source(source_id)
            https_url = (
                source.base_url.replace("http://", "https://", 1).rstrip("/")
                + "/VersaoJson/Despesas/"
            )
            try:
                from app.connectors.municipal_payments.fiorilli import FiorilliPaymentConnector

                adapter_json = FiorilliPaymentConnector(
                    source, settings, http_client=AsyncHTTPClient(settings, client=client)
                )
                response = await client.get(
                    https_url, params=adapter_json._list_params(datetime.now().year)
                )
                response.raise_for_status()
                parsed = list_payload(response.json(), operation=source_id)
                record(
                    source.name + " HTTPS",
                    "não verificado",
                    f"TLS e JSON disponíveis ({len(parsed)} linhas); equivalência de entidade ainda requer revisão; catálogo preservado",
                )
            except Exception as exc:
                record(
                    source.name + " HTTPS",
                    "falha",
                    f"{type(exc).__name__}; catálogo HTTP preservado com opt-in obrigatório",
                )
        cnpj = next(iter(settings.payment_poll_cnpjs), None)
        if settings.transparencia_api_key and cnpj:
            federal = TransparenciaPaymentsConnector(
                settings, http_client=AsyncHTTPClient(settings, client=client)
            )
            try:
                response = await client.get(
                    federal.base_url + "/api-de-dados/despesas/documentos-por-favorecido",
                    params={
                        "codigoPessoa": cnpj,
                        "ano": datetime.now().year,
                        "fase": 3,
                        "pagina": 1,
                    },
                    headers={"chave-api-dados": settings.transparencia_api_key},
                )
                response.raise_for_status()
                parsed = list_payload(response.json(), operation="Transparência")
                if any(not federal._has_payment_identity(row) for row in parsed):
                    raise ValueError("identidade de pagamento não reconhecida")
                record("Transparência", "validado", f"uma página, {len(parsed)} registros")
            except Exception as exc:
                record(
                    "Transparência",
                    "falha",
                    f"{type(exc).__name__}"
                    + (
                        f" HTTP {exc.response.status_code}"
                        if isinstance(exc, httpx.HTTPStatusError)
                        else ""
                    ),
                )
        else:
            record("Transparência", "não verificado", "chave ou CNPJ de consulta não configurado")
    with tempfile.TemporaryDirectory(prefix="licitaleads-openapi-") as directory:
        audit = await audit_sources(settings=settings, output_directory=Path(directory))
        openapi_text = audit.report_path.read_text(encoding="utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    date = datetime.now(ZoneInfo("America/Chicago")).date().isoformat()
    output.write_text(
        f"# Validação limitada das fontes — {date}\n\n"
        "Consultas somente leitura, sem persistência de dados de domínio. Falhas externas não são ignoradas.\n\n"
        "| Operação | Resultado | Evidência |\n|---|---|---|\n"
        + "\n".join(f"| {name} | {status} | {detail} |" for name, status, detail in rows)
        + "\n\n"
        + openapi_text,
        encoding="utf-8",
    )
    return not failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("docs/source_validation.md"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="licitaleads-live-validation-") as temporary:
        root = Path(temporary)
        os.environ.update(
            {
                "APP_ENV": "test",
                "DATABASE_URL": f"sqlite:///{(root / 'unused.db').as_posix()}",
                "RAW_DATA_STORAGE_PATH": str(root / "raw"),
                "DOCUMENT_STORAGE_PATH": str(root / "documents"),
                "LLM_ENABLED": "false",
                "CONTACT_SEARCH_ENABLED": "false",
                "OCR_ENABLED": "false",
            }
        )
        return 0 if asyncio.run(validate(args.output)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
