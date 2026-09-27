"""Opt-in live source-contract audit used by the CLI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import httpx

from app.config import Settings, get_settings
from app.connectors.capabilities import get_source_capabilities


@dataclass(frozen=True, slots=True)
class SourceAuditResult:
    report_path: Path
    checked_sources: int
    missing_paths: int
    warnings: tuple[str, ...]


async def _load_openapis(
    client: httpx.AsyncClient,
    candidates: tuple[str, ...],
) -> tuple[list[str], set[str], list[str]]:
    errors: list[str] = []
    loaded: list[str] = []
    paths: set[str] = set()
    for url in candidates:
        try:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("paths"), dict):
                loaded.append(str(response.url))
                paths.update(payload["paths"])
            else:
                errors.append(f"{url}: resposta não contém paths")
        except (httpx.HTTPError, ValueError) as exc:
            errors.append(f"{url}: {type(exc).__name__}")
    if not loaded:
        raise RuntimeError("; ".join(errors))
    return loaded, paths, errors


def _canonical_path(source_id: str, path: str) -> str:
    normalized = "/" + path.lstrip("/")
    if source_id == "pncp":
        for prefix in ("/api/consulta", "/api/pncp", "/pncp-api"):
            if normalized.startswith(prefix + "/"):
                return normalized.removeprefix(prefix)
    return normalized


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
        "compras_gov": (
            f"{settings.compras_gov_base_url.rstrip('/')}/v3/api-docs",
            f"{settings.compras_gov_base_url.rstrip('/')}/v3/api-docs/swagger-config",
        ),
    }
    warnings: list[str] = []
    sections: list[str] = []
    missing_count = 0
    async with httpx.AsyncClient(
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
                status = "compatível" if not missing else "divergência requer revisão"
                details = (
                    "\n".join(f"- `{item}`" for item in missing)
                    or "- Nenhuma rota conhecida ausente."
                )
                sections.append(
                    f"## {source.name}\n\n"
                    f"- OpenAPI consultado: {', '.join(openapi_urls)}\n"
                    f"- Rotas no contrato vivo: {len(live_paths)}\n"
                    f"- Resultado: **{status}**\n\n"
                    f"### Rotas conhecidas ausentes\n\n{details}\n"
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
    return SourceAuditResult(report.resolve(), len(candidates), missing_count, tuple(warnings))
