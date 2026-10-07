"""Operational CLI for collection, processing, audit, and export."""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import async_session_factory
from app.models import Lead
from app.services.ingestion import (
    IngestionPipeline,
    PipelineRequest,
    backfill_procurement_fields,
)
from app.services.ingestion.audit import audit_sources as run_source_audit
from app.services.ingestion.contacts import ContactEnrichmentService
from app.services.ingestion.processor import DocumentProcessingService

app = typer.Typer(
    name="licita-lead",
    help="Coleta e processamento auditável de contratações públicas.",
    no_args_is_help=True,
)


def _source(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in {"pncp", "compras_gov"}:
        raise typer.BadParameter("use 'pncp' ou 'compras-gov'")
    return normalized


def _request(
    connector: str,
    uf: str,
    days: int,
    modalities: list[str] | None,
    *,
    process_documents: bool,
    document_batch_size: int | None = None,
) -> PipelineRequest:
    settings = get_settings()
    return PipelineRequest(
        connector=connector,
        uf=uf.upper(),
        days=days,
        modalities=tuple(modalities or settings.default_modalities),
        process_documents=process_documents,
        document_batch_size=document_batch_size,
    )


@app.command("audit-sources")
def audit_sources() -> None:
    """Compare live official OpenAPI documents with the known contract."""

    result = asyncio.run(run_source_audit())
    typer.echo(f"Relatório: {result.report_path}")
    typer.echo(f"Rotas ausentes: {result.missing_paths}; avisos: {len(result.warnings)}")


@app.command()
def crawl(
    source: Annotated[str, typer.Argument(help="pncp ou compras-gov")],
    uf: Annotated[str, typer.Option("--uf", min=2, max=2)] = "MA",
    days: Annotated[int, typer.Option("--days", min=1, max=365)] = 7,
    modality: Annotated[list[str] | None, typer.Option("--modality")] = None,
    max_pages: Annotated[int | None, typer.Option("--max-pages", min=1)] = None,
) -> None:
    """Collect structured records without downloading documents."""

    request = _request(_source(source), uf, days, modality, process_documents=False)
    if max_pages is not None:
        request = PipelineRequest(
            connector=request.connector,
            uf=request.uf,
            days=request.days,
            modalities=request.modalities,
            max_pages=max_pages,
            process_documents=False,
        )
    result = asyncio.run(IngestionPipeline().run(request))
    typer.echo(
        f"run={result.run_id} status={result.status.value} encontrados={result.records_found} "
        f"criados={result.records_created} atualizados={result.records_updated}"
    )
    if result.diagnostics:
        typer.echo("Diagnósticos: " + " | ".join(result.diagnostics), err=True)


@app.command("crawl-atas")
def crawl_atas(
    source: Annotated[str, typer.Argument(help="pncp, compras-gov ou all")] = "all",
    days: Annotated[int, typer.Option("--days", min=1, max=3650)] = 365,
    max_pages: Annotated[int | None, typer.Option("--max-pages", min=1)] = None,
) -> None:
    """Coleta atas de registro de preços (ARP) e itens publicados."""

    normalized = source.strip().lower().replace("-", "_")
    if normalized != "all":
        normalized = _source(normalized)
    request = PipelineRequest(
        connector=normalized,
        mode="price_registries",
        days=days,
        process_documents=False,
        max_pages=max_pages,
    )
    result = asyncio.run(IngestionPipeline().run(request))
    typer.echo(
        f"run={result.run_id} status={result.status.value} encontrados={result.records_found} "
        f"criados={result.records_created} atualizados={result.records_updated}"
    )
    if result.diagnostics:
        typer.echo("Diagnósticos: " + " | ".join(result.diagnostics), err=True)


@app.command("backfill-fields")
def backfill_fields(
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Máximo de respostas brutas a examinar."),
    ] = None,
) -> None:
    """Recupera SRP/amparo legal/rota de contratação dos payloads já coletados."""

    result = asyncio.run(backfill_procurement_fields(limit=limit))
    typer.echo(
        f"respostas={result.scanned_records} contratações_atualizadas="
        f"{result.updated_procurements} ignoradas={result.skipped}"
    )
    if result.diagnostics:
        typer.echo("Diagnósticos: " + " | ".join(result.diagnostics), err=True)


@app.command("process-documents")
def process_documents(
    limit: Annotated[int | None, typer.Option("--limit", min=1)] = None,
) -> None:
    """Download and extract pending official documents."""

    result = asyncio.run(DocumentProcessingService().process_pending(limit=limit))
    typer.echo(
        f"processados={result.documents_processed} falhas={result.documents_failed} "
        f"eventos={result.events_created} participantes={result.participants_created} "
        f"leads={result.leads_created}"
    )


@app.command("detect-events")
def detect_events(
    limit: Annotated[int | None, typer.Option("--limit", min=1)] = None,
) -> None:
    """Rerun deterministic detection on stored extracted text."""

    async def execute():
        service = DocumentProcessingService()
        try:
            return await service.detect_existing(limit=limit)
        finally:
            await service.aclose()

    result = asyncio.run(execute())
    typer.echo(
        f"documentos={result.documents_processed} eventos={result.events_created} "
        f"participantes={result.participants_created} leads={result.leads_created}"
    )


@app.command("calculate-deadlines")
def calculate_deadlines() -> None:
    """Recalculate deadline status and its explainable scoring effects."""

    async def execute():
        service = DocumentProcessingService()
        try:
            return await service.recalculate_deadlines_and_leads()
        finally:
            await service.aclose()

    result = asyncio.run(execute())
    typer.echo(f"leads novos={result.leads_created}; prazos existentes foram atualizados")


@app.command("enrich-contacts")
def enrich_contacts(
    force: Annotated[
        bool,
        typer.Option("--force", help="Executa mesmo com CONTACT_SEARCH_ENABLED=false."),
    ] = False,
) -> None:
    """Visit only pre-evidenced corporate domains and respect robots.txt."""

    result = asyncio.run(ContactEnrichmentService().run(force=force))
    typer.echo(
        f"empresas={result.companies_checked} criados={result.contacts_created} "
        f"atualizados={result.contacts_updated} erros={result.errors}"
    )


@app.command("create-leads")
def create_leads() -> None:
    """Idempotently create or refresh scored leads from confirmed events."""

    async def execute():
        service = DocumentProcessingService()
        try:
            return await service.recalculate_deadlines_and_leads()
        finally:
            await service.aclose()

    result = asyncio.run(execute())
    typer.echo(f"leads novos={result.leads_created}; demais candidatos atualizados")


@app.command("run-pipeline")
def run_pipeline(
    uf: Annotated[str, typer.Option("--uf", min=2, max=2)] = "MA",
    days: Annotated[int, typer.Option("--days", min=1, max=365)] = 7,
    connector: Annotated[str, typer.Option("--connector")] = "all",
    modality: Annotated[list[str] | None, typer.Option("--modality")] = None,
    max_pages: Annotated[int | None, typer.Option("--max-pages", min=1)] = None,
    document_batch_size: Annotated[int | None, typer.Option("--document-batch-size", min=1)] = None,
    skip_documents: Annotated[bool, typer.Option("--skip-documents")] = False,
) -> None:
    """Run collection through document/event/deadline/lead creation."""

    normalized = connector.strip().lower().replace("-", "_")
    if normalized != "all":
        normalized = _source(normalized)
    request = _request(
        normalized,
        uf,
        days,
        modality,
        process_documents=not skip_documents,
        document_batch_size=document_batch_size,
    )
    if max_pages is not None:
        request = PipelineRequest(
            connector=request.connector,
            uf=request.uf,
            days=request.days,
            modalities=request.modalities,
            max_pages=max_pages,
            process_documents=request.process_documents,
            document_batch_size=request.document_batch_size,
        )
    result = asyncio.run(IngestionPipeline().run(request))
    typer.echo(
        f"run={result.run_id} status={result.status.value} encontrados={result.records_found} "
        f"criados={result.records_created} atualizados={result.records_updated}"
    )
    if result.diagnostics:
        typer.echo("Diagnósticos: " + " | ".join(result.diagnostics), err=True)


@app.command("export-leads")
def export_leads(
    destination: Annotated[Path, typer.Argument(help="Arquivo CSV de destino")],
) -> None:
    """Export the auditable lead summary as UTF-8 CSV."""

    async def load_rows() -> list[Lead]:
        async with async_session_factory() as session:
            return list(
                (
                    await session.scalars(
                        select(Lead)
                        .options(
                            selectinload(Lead.company),
                            selectinload(Lead.procurement),
                            selectinload(Lead.triggering_event),
                            selectinload(Lead.deadline),
                        )
                        .order_by(Lead.score.desc(), Lead.created_at.desc())
                    )
                ).all()
            )

    rows = asyncio.run(load_rows())
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "lead_id",
                "empresa",
                "cnpj",
                "orgao",
                "processo",
                "evento",
                "motivo",
                "prazo_explicito",
                "prazo_estimado",
                "status_prazo",
                "score",
                "status_lead",
                "fonte",
            ],
        )
        writer.writeheader()
        for lead in rows:
            writer.writerow(
                {
                    "lead_id": lead.id,
                    "empresa": lead.company.legal_name or lead.company.normalized_name,
                    "cnpj": lead.company.cnpj,
                    "orgao": lead.procurement.agency_name,
                    "processo": lead.procurement.pncp_control_number
                    or lead.procurement.purchase_number,
                    "evento": lead.triggering_event.event_type.value,
                    "motivo": lead.reason_summary,
                    "prazo_explicito": (
                        lead.deadline.explicit_deadline_at if lead.deadline else None
                    ),
                    "prazo_estimado": (
                        lead.deadline.estimated_deadline_at if lead.deadline else None
                    ),
                    "status_prazo": lead.deadline.status.value if lead.deadline else "UNKNOWN",
                    "score": lead.score,
                    "status_lead": lead.lead_status.value,
                    "fonte": lead.triggering_event.source_url,
                }
            )
    typer.echo(f"{len(rows)} lead(s) exportados para {destination.resolve()}")
