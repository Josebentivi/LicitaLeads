"""Opt-in live source-contract audit used by the CLI."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import httpx

from app.config import Settings, get_settings
from app.connectors.bounded_http import SourceDownloadError, bounded_request
from app.connectors.capabilities import EndpointDescription, get_source_capabilities


@dataclass(frozen=True, slots=True)
class SourceAuditResult:
    report_path: Path
    checked_sources: int
    missing_paths: int
    warnings: tuple[str, ...]
    validated_operations: int = 0
    unverified_operations: int = 0


async def _load_openapis(
    client: httpx.AsyncClient,
    candidates: tuple[str, ...],
) -> tuple[list[str], dict[str, tuple[dict[str, Any], dict[str, Any]]], list[str]]:
    errors: list[str] = []
    loaded: list[str] = []
    paths: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for url in candidates:
        try:
            response = await bounded_request(
                client, "GET", url, max_bytes=20 * 1024 * 1024, public_only=True
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("paths"), dict):
                loaded.append(str(response.url))
                paths.update(
                    {
                        path: (payload, node)
                        for path, node in payload["paths"].items()
                        if isinstance(node, dict)
                    }
                )
            else:
                errors.append(f"{url}: resposta não contém paths")
        except (httpx.HTTPError, ValueError, SourceDownloadError) as exc:
            errors.append(f"{url}: {type(exc).__name__}")
    if not loaded:
        raise RuntimeError("; ".join(errors))
    return loaded, paths, errors


def _canonical_path(source_id: str, path: str) -> str:
    normalized = "/" + path.lstrip("/")
    if source_id in {"pncp", "pncp_contracts"}:
        for prefix in ("/api/consulta", "/api/pncp", "/pncp-api"):
            if normalized.startswith(prefix + "/"):
                return normalized.removeprefix(prefix)
    return normalized


def _resolve(document: dict[str, Any], schema: Any) -> dict[str, Any]:
    for _ in range(12):
        if not isinstance(schema, dict):
            return {}
        reference = schema.get("$ref")
        if not reference:
            return schema
        if not isinstance(reference, str) or not reference.startswith("#/"):
            return {}
        schema = document
        for key in reference[2:].split("/"):
            schema = schema.get(key.replace("~1", "/").replace("~0", "~"), {})
    return {}


def _audit_operation(
    document: dict[str, Any], node: dict[str, Any], endpoint: EndpointDescription
) -> tuple[list[str], bool]:
    operation = node.get(endpoint.method.lower())
    if not isinstance(operation, dict):
        return ["método não documentado"], False
    parameters = [
        _resolve(document, parameter)
        for parameter in [*node.get("parameters", []), *operation.get("parameters", [])]
    ]
    names = {parameter.get("name") for parameter in parameters}
    issues = [
        f"parâmetro consumido ausente: {name}" for name in endpoint.parameters if name not in names
    ]
    consumed_parameters = set(endpoint.parameters) | set(re.findall(r"\{([^}]+)\}", endpoint.path))
    for parameter in parameters:
        if (
            parameter.get("required")
            and parameter.get("in") == "query"
            and parameter.get("name") not in consumed_parameters
        ):
            issues.append(f"novo parâmetro obrigatório não consumido: {parameter.get('name')}")
    for parameter in parameters:
        if parameter.get("name") in endpoint.parameters:
            schema = _resolve(document, parameter.get("schema", parameter))
            expected = (
                "integer"
                if parameter.get("name") in {"pagina", "tamanhoPagina", "ano", "sequencial"}
                else None
            )
            if expected and schema.get("type") not in {None, expected, "number"}:
                issues.append(f"tipo divergente: {parameter['name']}")
    responses = operation.get("responses", {})
    success = _resolve(document, responses.get("200", {}))
    content = success.get("content", {})
    json_content = (
        content.get("application/json")
        or content.get("*/*")
        or content.get("application/*+json")
        or {}
    )
    schema = _resolve(document, json_content.get("schema", success.get("schema")))
    if not schema:
        return issues + ["schema de resposta não documentado"], False
    shape, required = _response_contract(endpoint.path)
    if not required:
        return issues + ["operação sem contrato de campos específico"], False
    if shape == "collection":
        if schema.get("type") != "array":
            properties = schema.get("properties", {})
            envelopes = (
                ("resultado",)
                if "/modulo-" in endpoint.path
                else (
                    "data",
                    "documentos",
                    "itens",
                    "listaResultados",
                    "resultados",
                    "termos",
                    "resultado",
                )
            )
            collections = [properties[key] for key in envelopes if key in properties]
            if not collections:
                return issues + ["envelope de coleção consumido não documentado"], False
            schema = _resolve(document, collections[0])
        if schema.get("type") != "array":
            return issues + ["resposta consumida precisa ser uma coleção"], False
        schema = _resolve(document, schema.get("items"))
    if schema.get("type") != "object" or not schema.get("properties"):
        return issues + ["estrutura de resposta não confirma objetos consumíveis"], False

    def declares(path: str) -> bool:
        current = schema
        for name in path.split("."):
            properties = current.get("properties", {})
            if name not in properties:
                return False
            current = _resolve(document, properties[name])
        return True

    for alternatives in required:
        if not any(declares(field) for field in alternatives):
            issues.append("campo da operação ausente: " + "/".join(alternatives))
    return issues, not issues


def _response_contract(path: str) -> tuple[str, list[tuple[str, ...]]]:
    """Mirror each mapper's envelope and identity, without a global field union."""
    key = path.lower()
    if "documentos-por-favorecido" in key:
        return "collection", [("documento", "documentoResumido"), ("codigoFavorecido",)]
    if "termos" in key:
        return "collection", [("sequencialTermoContrato", "numeroTermoContrato")]
    if "arquivos" in key:
        return "collection", [("url", "uri")]
    if "resultado" in key:
        return "collection", [
            ("numeroItem", "numeroItemPncp", "idCompraItem"),
            ("niFornecedor", "nomeRazaoSocialFornecedor"),
        ]
    if "unidadesitem" in key:
        return "collection", [("numeroItem", "codigoPdm"), ("codigoUnidade",)]
    if "empenhossaldoitem" in key:
        return "collection", [("numeroItem", "codigoPdm"), ("unidade",)]
    if "adesoesitem" in key:
        return "collection", [("unidadeNaoParticipante",), ("dataAprovacaoAnalise",)]
    if "itens" in key or "item" in key:
        return "collection", [
            (
                "numeroItem",
                "numeroItemPncp",
                "numeroItemCompra",
                "idCompraItem",
                "codigoItem",
                "codigoPdm",
            )
        ]
    if "contratos" in key:
        return "collection", [
            (
                "numeroControlePNCP",
                "numeroControlePncpContrato",
                "numeroContrato",
                "numeroContratoEmpenho",
            )
        ]
    if "/atas" in key or "consultararp" in key:
        return "collection", [
            ("numeroControlePNCPAta", "numeroControlePncpAta", "numeroAtaRegistroPreco")
        ]
    if "contratacoes" in key:
        return "collection", [("numeroControlePNCP", "idCompra")]
    if "/compras/{" in key:
        return "object", [
            ("numeroControlePNCP", "orgaoEntidade.cnpj"),
            ("anoCompra",),
            ("sequencialCompra",),
        ]
    return "collection", []


async def audit_sources(
    *,
    settings: Settings | None = None,
    output_directory: Path = Path("docs"),
) -> SourceAuditResult:
    """Compare live OpenAPI documents with the versioned known-path registry."""

    settings = settings or get_settings()
    candidates = {
        "pncp": (
            "https://pncp.gov.br/api/consulta/v3/api-docs",
            "https://pncp.gov.br/pncp-api/v3/api-docs",
        ),
        "pncp_contracts": (
            "https://pncp.gov.br/api/consulta/v3/api-docs",
            "https://pncp.gov.br/pncp-api/v3/api-docs",
        ),
        "transparencia": ("https://api.portaldatransparencia.gov.br/v3/api-docs",),
        "compras_gov": (
            f"{settings.compras_gov_base_url.rstrip('/')}/v3/api-docs",
            f"{settings.compras_gov_base_url.rstrip('/')}/v3/api-docs/swagger-config",
        ),
    }
    warnings: list[str] = []
    sections: list[str] = []
    missing_count = 0
    validated_count = unverified_count = 0
    async with httpx.AsyncClient(
        trust_env=False,
        timeout=settings.http_timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": settings.http_user_agent},
    ) as client:
        for source in get_source_capabilities():
            known_paths = sorted(
                {
                    endpoint.path
                    for capability in source.capabilities
                    for endpoint in capability.endpoints
                }
            )
            try:
                openapi_urls, live_paths, source_warnings = await _load_openapis(
                    client, candidates[source.id]
                )
                warnings.extend(f"{source.name}: {item}" for item in source_warnings)
                canonical_live = {_canonical_path(source.id, item) for item in live_paths}
                missing = [
                    path
                    for path in known_paths
                    if _canonical_path(source.id, path) not in canonical_live
                ]
                missing_count += len(missing)
                operation_details: list[str] = []
                canonical_documents = {
                    _canonical_path(source.id, path): value for path, value in live_paths.items()
                }
                endpoints = {
                    (endpoint.method, endpoint.path): endpoint
                    for capability in source.capabilities
                    for endpoint in capability.endpoints
                }
                for endpoint in endpoints.values():
                    match = canonical_documents.get(_canonical_path(source.id, endpoint.path))
                    if match is None:
                        continue
                    issues, verified = _audit_operation(*match, endpoint)
                    validated_count += int(verified)
                    unverified_count += int(not verified)
                    label = "contrato mínimo validado" if verified else "operação não verificada"
                    operation_details.append(
                        f"- `{endpoint.method} {endpoint.path}`: {label}"
                        + ("; " + "; ".join(issues) if issues else "")
                    )
                    if issues:
                        warnings.append(f"{source.name}: {endpoint.path}: " + "; ".join(issues))
                status = (
                    "rotas encontradas; validação por operação abaixo"
                    if not missing
                    else "divergência requer revisão"
                )
                details = (
                    "\n".join(f"- `{item}`" for item in missing)
                    or "- Nenhuma rota conhecida ausente."
                )
                sections.append(
                    f"## {source.name}\n\n"
                    f"- OpenAPI consultado: {', '.join(openapi_urls)}\n"
                    f"- Rotas no contrato vivo: {len(live_paths)}\n"
                    f"- Resultado: **{status}**\n\n"
                    f"### Rotas conhecidas ausentes\n\n{details}\n\n"
                    f"### Métodos, parâmetros e respostas\n\n" + "\n".join(operation_details) + "\n"
                )
            except RuntimeError as exc:
                warning = f"{source.name}: OpenAPI indisponível ({exc})"
                warnings.append(warning)
                sections.append(
                    f"## {source.name}\n\n- Resultado: **não foi possível auditar**\n"
                    f"- Diagnóstico: {warning}\n"
                )
    output_directory.mkdir(parents=True, exist_ok=True)
    report = output_directory / f"source_audit_{date.today().isoformat()}.md"
    content = (
        f"# Auditoria de contratos das fontes — {date.today().isoformat()}\n\n"
        "Relatório gerado sob demanda. Ele não altera automaticamente a matriz versionada; "
        "divergências exigem revisão humana.\n\n" + "\n".join(sections)
    )
    report.write_text(content, encoding="utf-8")
    return SourceAuditResult(
        report.resolve(),
        len(candidates),
        missing_count,
        tuple(warnings),
        validated_count,
        unverified_count,
    )
