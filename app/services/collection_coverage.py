"""Read the recorded scope of an execution and the catalog responses for one purchase."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.pncp import parse_pncp_external_id
from app.models import CrawlRun, DataAvailability, Procurement, SourceRecord


async def latest_procurement_run(db: AsyncSession) -> CrawlRun | None:
    mode = CrawlRun.filters["mode"].as_string()
    return await db.scalar(
        select(CrawlRun)
        .where(
            CrawlRun.connector.in_(("all", "pncp", "compras_gov")),
            or_(
                mode == "procurements",
                mode.is_(None)
                & (
                    (CrawlRun.cursor["sources"]["pncp"]["mode"].as_string() == "procurements")
                    | (
                        CrawlRun.cursor["sources"]["compras_gov"]["mode"].as_string()
                        == "procurements"
                    )
                ),
            ),
        )
        .order_by(CrawlRun.created_at.desc(), CrawlRun.id.desc())
        .limit(1)
    )


def document_counters(run: CrawlRun) -> dict[str, int]:
    sources = (run.cursor or {}).get("sources", {})
    stage = sources.get("documents") if isinstance(sources, dict) else None
    if not isinstance(stage, dict):
        return {}
    metrics = {"documents_total": "Documentos selecionados"}
    if stage.get("status") in {"completed", "partial"}:
        # While running, processed measures attempts and the other counters are placeholders.
        metrics.update(
            documents_processed="Documentos processados",
            documents_failed="Falhas de processamento",
            events_created="Eventos criados",
            leads_created="Oportunidades criadas",
        )
    return {
        label: stage[key]
        for key, label in metrics.items()
        if type(stage.get(key)) is int and stage[key] >= 0
    }


@dataclass(frozen=True)
class CatalogCoverage:
    source: str
    label: str
    collected_at: datetime | None = None
    run_id: UUID | None = None
    record_id: UUID | None = None


_LABELS = {
    DataAvailability.EMPTY: "Consulta concluída sem registros",
    DataAvailability.NOT_PUBLISHED: "Sem publicação indicada pela fonte",
    DataAvailability.ACCESS_RESTRICTED: "Acesso restrito",
    DataAvailability.TEMPORARY_ERROR: "Erro na consulta",
    DataAvailability.NOT_SUPPORTED: "Consulta sem suporte na fonte",
    DataAvailability.AVAILABLE: "Registros retornados pela fonte",
}


async def procurement_catalogs(
    db: AsyncSession, procurement: Procurement, settings: Settings
) -> dict[str, list[CatalogCoverage]]:
    """A matching header or timestamp alone does not prove that its children were queried."""
    identities = {(procurement.source, procurement.external_id): procurement.pncp_control_number}
    identities.update(
        {
            (source.source, source.external_id): source.pncp_control_number
            or procurement.pncp_control_number
            for source in procurement.sources
        }
    )
    catalogs: dict[str, list[CatalogCoverage]] = {"items": [], "documents": []}
    for source, external_id in sorted(identities):
        for section in catalogs:
            catalogs[section].append(
                await _catalog_coverage(
                    db, source, external_id, section, settings, identities[source, external_id]
                )
            )
    return catalogs


async def _catalog_coverage(
    db: AsyncSession,
    source: str,
    external_id: str,
    section: str,
    settings: Settings,
    control: str | None,
) -> CatalogCoverage:
    empty = CatalogCoverage(source, "Sem consulta registrada para esta seção")
    if source == "compras_gov" and section == "documents":
        return CatalogCoverage(source, "A fonte não oferece consulta pública a documentos")
    statement = select(
        SourceRecord.id,
        SourceRecord.crawl_run_id,
        SourceRecord.collected_at,
        SourceRecord.availability,
        SourceRecord.collection_complete,
        SourceRecord.http_status,
    ).where(SourceRecord.source == source)
    if source == "pncp":
        for identifier in (external_id, control or ""):
            try:
                ref = parse_pncp_external_id(identifier)
                break
            except ValueError:
                continue
        else:
            return empty
        catalog = "itens" if section == "items" else "arquivos"
        endpoint = (
            f"{settings.pncp_integration_base_url.rstrip('/')}/orgaos/{ref.cnpj}"
            f"/compras/{ref.year}/{ref.sequence}/{catalog}"
        )
    elif source == "compras_gov":
        endpoint = f"{settings.compras_gov_base_url.rstrip('/')}/{ComprasGovConnector.ITEM_ID_PATH}"
        for key, value in ComprasGovConnector._identifier_params(external_id).items():
            statement = statement.where(SourceRecord.request_parameters[key].as_string() == value)
    else:
        return empty
    record = (
        await db.execute(
            statement.where(
                or_(
                    SourceRecord.endpoint == endpoint,
                    SourceRecord.endpoint.startswith(endpoint + "?", autoescape=True),
                )
            )
            .order_by(SourceRecord.collected_at.desc(), SourceRecord.id.desc())
            .limit(1)
        )
    ).first()
    # A network failure can leave no HTTP response. Only accept the exact diagnostic produced by
    # _note_detail_result for this source, external ID and operation at the beginning of a line.
    prefix = f"{source}/{external_id}: {section}: "
    failed = select(CrawlRun.id, CrawlRun.started_at).where(
        CrawlRun.connector.in_(("all", source)),
        or_(
            CrawlRun.diagnostic.startswith(prefix, autoescape=True),
            CrawlRun.diagnostic.contains("\n" + prefix, autoescape=True),
        ),
    )
    if record:
        failed = failed.where(
            func.coalesce(CrawlRun.started_at, CrawlRun.created_at) > record.collected_at
        )
    failure = (
        await db.execute(failed.order_by(CrawlRun.created_at.desc(), CrawlRun.id.desc()).limit(1))
    ).first()
    if failure:
        return CatalogCoverage(
            source, "Falha registrada na consulta", failure.started_at, failure.id
        )
    if not record:
        return empty
    label = _LABELS[record.availability]
    if record.availability in (DataAvailability.EMPTY, DataAvailability.AVAILABLE):
        if record.http_status is None or not 200 <= record.http_status <= 299:
            label = "Resposta sem sucesso HTTP comprovado"
        elif record.collection_complete is False:
            label = "Resposta parcial"
        elif record.collection_complete is None:
            label = "Resposta registrada com completude desconhecida"
    return CatalogCoverage(source, label, record.collected_at, record.crawl_run_id, record.id)
