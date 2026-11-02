"""Operational CLI for collection, processing, audit, and export."""

from __future__ import annotations

import asyncio
import csv
import json
from datetime import date
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
from app.services.ingestion.municipal_payments import run_selected_municipal_sources
from app.services.ingestion.payment_import import PaymentImportService
from app.services.ingestion.payments import PaymentIngestionService
from app.services.ingestion.processor import DocumentProcessingService

app = typer.Typer(
    name="licita-lead",
    help="Coleta e processamento auditável de contratações públicas.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)


def _source(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in {"pncp", "compras_gov"}:
        raise typer.BadParameter("use 'pncp' ou 'compras-gov'")
    return normalized


@app.command("prepare-test-browser")
def prepare_test_browser_command() -> None:
    """Prepara Edge/Chrome e driver local; não acessa o banco nem coleta fontes."""
    from app.services.test_browser import prepare_test_browser

    try:
        result = prepare_test_browser()
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"{result['browser']} {result['version']}: {result['detail']}")


@app.command("enqueue-llm")
def enqueue_llm_command(
    all_extracted: Annotated[bool, typer.Option("--all-extracted")] = False,
) -> None:
    """Enfileira IA para documentos já extraídos, sem baixar ou executar OCR."""
    if not all_extracted:
        raise typer.BadParameter("Informe --all-extracted")
    from app.services.llm.queue import LLMQueueService

    result = asyncio.run(LLMQueueService().backfill())
    typer.echo(json.dumps(result, ensure_ascii=False))


def _request(
    connector: str,
    uf: str,
    days: int,
    modalities: list[str] | None,
    *,
    process_documents: bool,
    document_batch_size: int | None = None,
    discovery_kind: str = "publication",
    force_refresh: bool = False,
) -> PipelineRequest:
    settings = get_settings()
    return PipelineRequest(
        connector=connector,
        uf=uf.upper(),
        days=days,
        modalities=tuple(modalities or settings.default_modalities),
        process_documents=process_documents,
        document_batch_size=document_batch_size,
        discovery_kind=discovery_kind,
        force_refresh=force_refresh,
    )


@app.command("reconcile-data")
def reconcile_data_command(
    apply: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="cria backup verificado antes de aplicar reparos determinísticos",
        ),
    ] = False,
    backup_dir: Annotated[
        Path | None, typer.Option("--backup-dir", help="diretório do backup obrigatório")
    ] = None,
    participation_integrity: Annotated[
        bool,
        typer.Option(
            "--participation-integrity",
            help="verifica participantes, valores e aliases documentais em todo o acervo",
        ),
    ] = False,
    resume: Annotated[
        str | None,
        typer.Option("--resume", help="retoma o ID de uma reconciliação de participantes"),
    ] = None,
    restore_suspended_state_from: Annotated[
        str | None,
        typer.Option(
            "--restore-suspended-state-from",
            help="recupera notas históricas e estado documental comprovados pelo reparo SQLite informado",
        ),
    ] = None,
) -> None:
    """Audita dados legados e aplica apenas reparos deriváveis do bruto."""

    from app.database import async_session_factory
    from app.services.backups import create_backup
    from app.services.data_reconciliation import reconcile_data
    from app.services.maintenance import (
        MaintenanceBlocked,
        exclusive_maintenance,
        run_blocking_maintenance,
    )

    async def run() -> dict[str, object]:
        if restore_suspended_state_from:
            from uuid import UUID

            from app.services.suspension_restoration import restore_suspended_state

            if participation_integrity or resume:
                raise typer.BadParameter("recuperação de notas não aceita outros modos de reparo")
            baseline_id = UUID(restore_suspended_state_from)
            if not apply:
                return await restore_suspended_state(
                    async_session_factory, get_settings(), baseline_id
                )
            async with exclusive_maintenance(async_session_factory) as owner:
                restoration_impact = await restore_suspended_state(
                    async_session_factory, get_settings(), baseline_id
                )
                if (
                    not restoration_impact["restorable"]
                    and not restoration_impact["documents_restorable"]
                    and not restoration_impact["pending_run_id"]
                ):
                    return {**restoration_impact, "applied": True}
                backup = await run_blocking_maintenance(
                    lambda: create_backup(
                        get_settings(),
                        backup_dir,
                        maintenance_owner=owner,
                        sqlite_timeout_seconds=300,
                    )
                )
                restoration_result = await restore_suspended_state(
                    async_session_factory,
                    get_settings(),
                    baseline_id,
                    apply=True,
                    backup=backup,
                    maintenance_owner=owner,
                )
                report_path = backup.path.with_suffix(".score-restoration.json")
                await asyncio.to_thread(
                    report_path.write_text,
                    json.dumps(
                        {"impact": restoration_impact, **restoration_result},
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                return {
                    **restoration_result,
                    "backup": backup.as_dict(),
                    "result_report": str(report_path),
                }
        if participation_integrity:
            from uuid import UUID

            from app.services.participation_reconciliation import reconcile_participation

            selected_resume = UUID(resume) if resume else None
            if not apply:
                if resume:
                    raise typer.BadParameter("--resume exige --apply")
                return await reconcile_participation(async_session_factory, get_settings())
            async with exclusive_maintenance(async_session_factory) as owner:
                integrity_impact = await reconcile_participation(
                    async_session_factory, get_settings()
                )
                backup = await run_blocking_maintenance(
                    lambda: create_backup(
                        get_settings(),
                        backup_dir,
                        maintenance_owner=owner,
                        sqlite_timeout_seconds=300,
                    )
                )
                impact_path = backup.path.with_suffix(".participation-impact.json")
                await asyncio.to_thread(
                    impact_path.write_text,
                    json.dumps(integrity_impact, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                integrity_result = await reconcile_participation(
                    async_session_factory,
                    get_settings(),
                    apply=True,
                    backup=backup,
                    maintenance_owner=owner,
                    resume_id=selected_resume,
                )
                result_path = backup.path.with_suffix(".participation-result.json")
                await asyncio.to_thread(
                    result_path.write_text,
                    json.dumps(integrity_result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                return {
                    **integrity_result,
                    "backup": backup.as_dict(),
                    "impact_report": str(impact_path),
                    "result_report": str(result_path),
                }
        if resume:
            raise typer.BadParameter("--resume exige --participation-integrity")
        if not apply:
            async with async_session_factory() as session:
                impact = await reconcile_data(session)
                return {"impact": impact.as_dict(), "applied": False}
        try:
            async with exclusive_maintenance(async_session_factory) as owner:
                async with async_session_factory() as session:
                    impact = await reconcile_data(session)
                    await session.rollback()
                    result: dict[str, object] = {"impact": impact.as_dict(), "applied": False}
                    backup = await run_blocking_maintenance(
                        lambda: create_backup(get_settings(), backup_dir, maintenance_owner=owner)
                    )
                    result["backup"] = backup.as_dict()
                    impact_path = backup.path.with_suffix(".impact.json")
                    await asyncio.to_thread(
                        impact_path.write_text,
                        json.dumps(impact.as_dict(), ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    result["impact_report"] = str(impact_path)
                    report = await reconcile_data(
                        session, apply=True, backup=backup, maintenance_owner=owner
                    )
                    await session.commit()
                    result.update({"applied": True, "result": report.as_dict()})
                    return result
        except MaintenanceBlocked as exc:
            raise typer.BadParameter("reconciliação bloqueada: " + str(exc)) from exc

    result = asyncio.run(run())
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


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
    force_refresh: Annotated[bool, typer.Option("--force-refresh")] = False,
    discovery_kind: Annotated[
        str, typer.Option("--discovery-kind", help="publication, update ou open_proposals")
    ] = "publication",
) -> None:
    """Collect structured records without downloading documents."""

    request = _request(
        _source(source),
        uf,
        days,
        modality,
        process_documents=False,
        discovery_kind=discovery_kind,
        force_refresh=force_refresh,
    )
    if max_pages is not None:
        request = PipelineRequest(
            connector=request.connector,
            uf=request.uf,
            days=request.days,
            modalities=request.modalities,
            max_pages=max_pages,
            process_documents=False,
            discovery_kind=request.discovery_kind,
            force_refresh=request.force_refresh,
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
    force_refresh: Annotated[bool, typer.Option("--force-refresh")] = False,
) -> None:
    """Coleta atas de registro de preços (ARP) e itens publicados."""

    normalized = source.strip().lower().replace("-", "_")
    if normalized != "all":
        normalized = _source(normalized)
    request = PipelineRequest(
        connector=normalized,
        mode="price_registries",
        force_refresh=force_refresh,
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


@app.command("crawl-contracts")
def crawl_contracts(
    source: Annotated[str, typer.Argument(help="pncp")] = "pncp",
    uf: Annotated[str, typer.Option("--uf", min=2, max=2)] = "MA",
    days: Annotated[int, typer.Option("--days", min=1, max=3650)] = 30,
    agency_cnpj: Annotated[
        str | None, typer.Option("--agency-cnpj", help="CNPJ do orgao (opcional)")
    ] = None,
    start_date: Annotated[
        str | None, typer.Option("--start-date", help="Data inicial AAAA-MM-DD")
    ] = None,
    end_date: Annotated[
        str | None, typer.Option("--end-date", help="Data final AAAA-MM-DD")
    ] = None,
    skip_details: Annotated[
        bool,
        typer.Option(
            "--skip-details",
            help="Coleta apenas o cabecalho do contrato (rapido; nao busca itens/termos)",
        ),
    ] = False,
    max_pages: Annotated[int | None, typer.Option("--max-pages", min=1)] = None,
    force_refresh: Annotated[bool, typer.Option("--force-refresh")] = False,
) -> None:
    """Coleta contratos publicados no PNCP."""

    try:
        parsed_start = date.fromisoformat(start_date) if start_date else None
        parsed_end = date.fromisoformat(end_date) if end_date else None
    except ValueError as exc:
        raise typer.BadParameter("use datas no formato AAAA-MM-DD") from exc
    request = PipelineRequest(
        connector=_source(source),
        mode="contracts",
        force_refresh=force_refresh,
        uf=uf,
        days=days,
        start_date=parsed_start,
        end_date=parsed_end,
        agency_cnpj=agency_cnpj,
        process_documents=False,
        contract_details=not skip_details,
        max_pages=max_pages,
    )
    result = asyncio.run(IngestionPipeline().run(request))
    typer.echo(
        f"run={result.run_id} status={result.status.value} encontrados={result.records_found} "
        f"criados={result.records_created} atualizados={result.records_updated}"
    )
    if result.diagnostics:
        typer.echo("Diagnosticos: " + " | ".join(result.diagnostics), err=True)


@app.command("import-payments")
def import_payments(
    company_cnpj: Annotated[str, typer.Option("--cnpj", help="CNPJ do fornecedor")],
    year: Annotated[int, typer.Option("--year", min=2000, max=2100)] = date.today().year,
    phase: Annotated[
        str | None,
        typer.Option("--phase", help="empenho, liquidacao ou pagamento (padrao: pagamento)"),
    ] = None,
) -> None:
    """Importa pagamentos federais do Portal da Transparencia."""

    typer.echo("Consultando o Portal da Transparencia...")
    result = asyncio.run(
        PaymentIngestionService().run(company_cnpj=company_cnpj, year=year, phase=phase)
    )
    typer.echo(
        f"disponibilidade={result.availability.value} criados={result.payments_created} "
        f"atualizados={result.payments_updated} conciliados={result.payments_linked} "
        f"nao_conciliados={result.payments_unlinked}"
    )
    if result.diagnostic:
        typer.echo(f"Diagnostico: {result.diagnostic}", err=True)
    if result.availability.value not in {"available", "empty"}:
        raise typer.Exit(code=1)


@app.command("import-payments-csv")
def import_payments_csv(
    arquivo: Annotated[Path, typer.Argument(help="CSV ou JSON exportado pelo portal")],
    fonte: Annotated[str, typer.Option("--fonte", help="Nome do portal de origem")] = "Portal",
) -> None:
    """Importa pagamentos de portais sem API usando CSV/JSON auditavel."""

    content = arquivo.read_bytes()
    service = PaymentImportService()
    try:
        result = asyncio.run(
            service.import_bytes(content, filename=arquivo.name, source_name=fonte)
        )
    finally:
        asyncio.run(service.aclose())
    typer.echo(
        f"criados={result.created} atualizados={result.updated} conciliados={result.linked} "
        f"nao_conciliados={result.unlinked} erros={len(result.errors)}"
    )
    for error in result.errors[:20]:
        typer.echo(f"linha {error.row}: {error.message}", err=True)


@app.command("crawl-payments-municipal")
def crawl_payments_municipal(
    source: Annotated[
        str,
        typer.Option(
            "--source",
            help="sao_luis, imperatriz, timon, caxias, bacabal ou all",
        ),
    ] = "all",
    year: Annotated[int, typer.Option("--year", min=2000, max=2100)] = date.today().year,
    include_details: Annotated[
        bool,
        typer.Option("--details", help="Busca pagamentos individuais por empenho (Fiorilli)."),
    ] = False,
    company_cnpj: Annotated[
        str | None,
        typer.Option("--cnpj", help="Filtra um fornecedor pelo CNPJ."),
    ] = None,
) -> None:
    """Coleta pagamentos dos portais municipais do Maranhao."""

    normalized = source.strip().lower().replace("-", "_")
    if normalized == "all":
        source_ids = None
    else:
        from app.connectors.municipal_payments.sources import get_municipal_source

        try:
            source_ids = [get_municipal_source(normalized).id]
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    results = asyncio.run(
        run_selected_municipal_sources(
            year=year,
            source_ids=source_ids,
            include_details=include_details,
            company_cnpj=company_cnpj,
        )
    )
    if not results:
        typer.echo(
            "Nenhuma fonte municipal habilitada. Configure MUNICIPAL_PAYMENT_SOURCES.",
            err=True,
        )
        return
    for result in results:
        typer.echo(
            f"{result.source_id}: disponibilidade={result.availability.value} "
            f"criados={result.payments_created} atualizados={result.payments_updated} "
            f"conciliados={result.payments_linked} nao_conciliados={result.payments_unlinked}"
        )
        if result.diagnostic:
            typer.echo(f"  {result.diagnostic}", err=True)


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
    force_refresh: Annotated[bool, typer.Option("--force-refresh")] = False,
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
        force_refresh=force_refresh,
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
            force_refresh=request.force_refresh,
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


@app.command("continue-crawl")
def continue_crawl_command(run_id: str) -> None:
    """Continua o trabalho pendente de uma coleta anterior."""
    from uuid import UUID

    from app.services.ingestion.continuation import create_continuation, execute_continuation

    async def run() -> None:
        async with async_session_factory() as session:
            child, filters = await create_continuation(session, UUID(run_id))
            await session.commit()
        await execute_continuation(child.id, filters)
        typer.echo(f"run={child.id} lote={child.batch_id}")

    try:
        asyncio.run(run())
    except (ValueError, LookupError) as exc:
        raise typer.BadParameter(str(exc)) from exc
