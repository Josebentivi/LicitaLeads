"""Server-rendered Portuguese web interface."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.routes.crawls import launch_crawl
from app.api.serialization import model_dict
from app.config import get_settings
from app.connectors.capabilities import get_source_capabilities
from app.dependencies import get_db
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    DeadlineStatus,
    Document,
    Lead,
    LeadStatus,
    Participant,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
)
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.processor import DocumentProcessingService

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory="app/templates")

_SOURCE_LABELS = {"pncp": "PNCP", "compras_gov": "Compras.gov.br"}
_CONNECTOR_LABELS = {**_SOURCE_LABELS, "all": "Todas as fontes"}
_STATUS_LABELS = {
    "pending": "Aguardando",
    "running": "Em andamento",
    "completed": "Concluída",
    "partial": "Parcial",
    "failed": "Falhou",
    "cancelled": "Cancelada",
    "temporary_error": "Indisponível",
}

_PROCUREMENT_TIMEZONE = ZoneInfo("America/Sao_Paulo")
_PROCUREMENT_MODALITY_LABELS = {
    "pregao_eletronico": "Pregão eletrônico",
    "pregao_presencial": "Pregão presencial",
    "concorrencia_eletronica": "Concorrência eletrônica",
    "concorrencia": "Concorrência",
    "dispensa": "Dispensa de licitação",
    "inexigibilidade": "Inexigibilidade",
    "leilao": "Leilão",
    "concurso": "Concurso",
    "dialogo_competitivo": "Diálogo competitivo",
}
_PROCUREMENT_STATUS_LABELS = {
    "em andamento": "Em andamento",
    "em julgamento": "Em julgamento",
    "aberta": "Aberta",
    "aberto": "Aberto",
    "publicada": "Publicada",
    "publicado": "Publicado",
    "divulgada no pncp": "Divulgada no PNCP",
    "homologada": "Homologada",
    "homologado": "Homologado",
    "adjudicada": "Adjudicada",
    "adjudicado": "Adjudicado",
    "cancelada": "Cancelada",
    "cancelado": "Cancelado",
    "suspensa": "Suspensa",
    "suspenso": "Suspenso",
    "deserta": "Deserta",
    "fracassada": "Fracassada",
    "revogada": "Revogada",
    "anulada": "Anulada",
}
_DOCUMENT_TYPE_LABELS = {
    "ata": "Ata",
    "edital": "Edital",
    "anexo": "Anexo",
    "aviso": "Aviso",
    "resultado": "Resultado",
    "termo_referencia": "Termo de referência",
}
_EXTRACTION_STATUS_LABELS = {
    "pending": "Aguardando extração",
    "extracted": "Texto extraído",
    "ocr_required": "OCR necessário",
    "failed": "Falha na extração",
    "unsupported": "Formato não suportado",
}
_PARTICIPANT_ROLE_LABELS = {
    "participant": "Participante",
    "winner": "Vencedora",
    "awarded": "Adjudicatária",
    "contractor": "Contratada",
    "unknown": "Não informado",
}
_EVENT_TYPE_LABELS = {
    "PROPOSAL_SUBMITTED": "Proposta apresentada",
    "PROPOSAL_ACCEPTED": "Proposta aceita",
    "PROPOSAL_REJECTED": "Proposta rejeitada",
    "DISQUALIFIED": "Desclassificação",
    "QUALIFIED": "Habilitação",
    "INELIGIBLE": "Inabilitação",
    "INTENT_TO_APPEAL": "Intenção de recurso",
    "APPEAL_SUBMITTED": "Recurso apresentado",
    "COUNTERARGUMENT_OPENED": "Contrarrazões abertas",
    "COUNTERARGUMENT_SUBMITTED": "Contrarrazões apresentadas",
    "APPEAL_DECIDED": "Recurso decidido",
    "WINNER_DECLARED": "Vencedora declarada",
    "ADJUDICATED": "Adjudicação",
    "HOMOLOGATED": "Homologação",
    "SESSION_SUSPENDED": "Sessão suspensa",
    "SESSION_REOPENED": "Sessão reaberta",
    "UNKNOWN": "Não informado",
}
_REASON_CATEGORY_LABELS = {
    "TECHNICAL_SPECIFICATION": "Especificação técnica",
    "MISSING_DOCUMENT": "Documento exigido não apresentado",
    "INVALID_DOCUMENT": "Documento inválido",
    "FISCAL_REGULARITY": "Regularidade fiscal",
    "LABOR_REGULARITY": "Regularidade trabalhista",
    "ECONOMIC_FINANCIAL": "Qualificação econômico-financeira",
    "TECHNICAL_QUALIFICATION": "Qualificação técnica",
    "PRICE_INEXEQUIBILITY": "Preço inexequível",
    "PRICE_ABOVE_ESTIMATE": "Preço acima do estimado",
    "LATE_SUBMISSION": "Envio fora do prazo",
    "PROPOSAL_FORMAT": "Formato da proposta",
    "SAMPLE_REJECTED": "Amostra rejeitada",
    "BRAND_OR_MODEL_NONCOMPLIANT": "Marca ou modelo não conforme",
    "FAILURE_TO_RESPOND": "Ausência de resposta",
    "OTHER": "Outro motivo",
    "UNKNOWN": "Não informado",
}
_TECHNICAL_SOURCE_LABELS = {
    **_SOURCE_LABELS,
    "document": "Documento oficial",
    "llm": "Análise por IA",
    "manual": "Registro manual",
}


def _local_datetime(value: datetime | None) -> str:
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    local = value.astimezone(ZoneInfo(get_settings().timezone))
    return local.strftime("%d/%m/%Y %H:%M:%S %Z")


def _display_text(value: object | None) -> str:
    """Render absent source values consistently in the operational interface."""
    if value is None:
        return "Não informado"
    if isinstance(value, str):
        return value.strip() or "Não informado"
    return str(value)


def _enum_key(value: object | None) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw)


def _label(value: object | None, labels: dict[str, str]) -> str:
    key = _enum_key(value)
    if key is None or not key.strip():
        return "Não informado"
    if key.casefold() in {"unknown", "not_available", "not_published", "none", "null"}:
        return "Não informado"
    return labels.get(key, labels.get(key.casefold(), key.replace("_", " ").capitalize()))


def _procurement_datetime(value: datetime | None) -> str:
    if value is None:
        return "Não informado"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(_PROCUREMENT_TIMEZONE).strftime("%d/%m/%Y %H:%M BRT")


def _brl(value: Decimal | None) -> str:
    if value is None:
        return "Não informado"
    rendered = f"{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {rendered}"


def _quantity_with_unit(quantity: Decimal | None, unit: str | None) -> str:
    if quantity is None:
        return "Não informado"
    rendered = format(quantity, "f").rstrip("0").rstrip(".")
    rendered = rendered or "0"
    return f"{rendered} {_display_text(unit)}" if unit else rendered


def _format_cnpj(value: str | None) -> str:
    raw = _display_text(value)
    if len(raw) == 14 and raw.isdigit():
        return f"{raw[:2]}.{raw[2:5]}.{raw[5:8]}/{raw[8:12]}-{raw[12:]}"
    return raw


def _confidence(value: Decimal | None) -> str:
    if value is None:
        return "Não informado"
    return f"{value * 100:.0f}%"


def _short_description(value: str | None, limit: int = 180) -> tuple[str, str, bool]:
    full = _display_text(value)
    if full == "Não informado":
        return full, full, False
    compact = " ".join(full.split())
    if len(compact) <= limit:
        return compact, compact, False
    return f"{compact[:limit].rstrip()}…", compact, True


def _technical_origin(
    *,
    source: str | None,
    official_url: str | None = None,
    source_record_id: UUID | None = None,
    external_id: str | None = None,
    evidence_id: UUID | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        source_label=_label(source, _TECHNICAL_SOURCE_LABELS),
        official_url=official_url,
        source_record_id=str(source_record_id) if source_record_id else None,
        external_id=_display_text(external_id) if external_id else None,
        evidence_url=f"/api/evidence/{evidence_id}" if evidence_id else None,
        evidence_id=str(evidence_id) if evidence_id else None,
    )


def _item_matches(item: ProcurementItem, query: str) -> bool:
    needle = query.casefold()
    return needle in item.item_number.casefold() or needle in (item.description or "").casefold()


def _item_view(item: ProcurementItem, procurement: Procurement) -> SimpleNamespace:
    summary, description, is_long = _short_description(item.description)
    return SimpleNamespace(
        item_number=_display_text(item.item_number),
        description_summary=summary,
        description=description,
        description_is_long=is_long,
        quantity=_quantity_with_unit(item.quantity, item.unit),
        unit_value=_brl(item.estimated_unit_value),
        total_value=_brl(item.estimated_total_value),
        status=_label(item.result_status, _PROCUREMENT_STATUS_LABELS),
        origin=_technical_origin(
            source=procurement.source,
            source_record_id=item.source_record_id,
            external_id=item.external_item_id,
        ),
    )


def _document_view(document: Document, procurement: Procurement) -> SimpleNamespace:
    return SimpleNamespace(
        title=_display_text(document.title),
        document_type=_label(document.document_type, _DOCUMENT_TYPE_LABELS),
        published_at=_procurement_datetime(document.published_at),
        extraction_status=_label(document.extraction_status, _EXTRACTION_STATUS_LABELS),
        extraction_state=_enum_key(document.extraction_status) or "pending",
        official_url=document.original_url,
        origin=_technical_origin(
            source=procurement.source,
            official_url=document.original_url,
            source_record_id=document.source_record_id,
        ),
    )


def _participant_view(participant: Participant) -> SimpleNamespace:
    evidence = participant.source_evidence
    company = participant.company
    return SimpleNamespace(
        company_name=_display_text(company.legal_name or company.normalized_name),
        cnpj=_format_cnpj(company.cnpj),
        item_number=_display_text(participant.item.item_number if participant.item else None),
        role=_label(participant.participation_role, _PARTICIPANT_ROLE_LABELS),
        proposal_value=_brl(participant.proposal_value),
        final_value=_brl(participant.final_value),
        status=_label(participant.status, _PROCUREMENT_STATUS_LABELS),
        confidence=_confidence(participant.confidence),
        evidence_url=f"/api/evidence/{evidence.id}" if evidence else None,
        origin=_technical_origin(
            source=participant.source,
            official_url=evidence.source_url if evidence else None,
            source_record_id=participant.source_record_id,
            evidence_id=evidence.id if evidence else None,
        ),
    )


def _event_view(event: ProcurementEvent) -> SimpleNamespace:
    evidence = event.evidence
    event_date = event.occurred_at or event.published_at
    reason = event.normalized_reason or _label(event.reason_category, _REASON_CATEGORY_LABELS)
    company_identified = event.company is not None
    company_name = _display_text(
        (event.company.legal_name or event.company.normalized_name) if event.company else None
    )
    return SimpleNamespace(
        id=event.id,
        procurement_id=event.procurement_id,
        event_type=_label(event.event_type, _EVENT_TYPE_LABELS),
        company_name="Empresa não identificada" if not company_identified else company_name,
        company_identified=company_identified,
        item_number=_display_text(event.item.item_number if event.item else None),
        reason=_display_text(reason),
        occurred_at=_procurement_datetime(event_date),
        confidence=_confidence(event.confidence),
        review_label="Revisão necessária"
        if event.requires_manual_review
        else "Sem revisão pendente",
        requires_manual_review=event.requires_manual_review,
        evidence_url=f"/api/evidence/{evidence.id}" if evidence else None,
        origin=_technical_origin(
            source=event.source,
            official_url=event.source_url or (evidence.source_url if evidence else None),
            source_record_id=event.source_record_id,
            evidence_id=evidence.id if evidence else None,
        ),
    )


def _procurement_view(procurement: Procurement) -> SimpleNamespace:
    process_number = _display_text(procurement.purchase_number)
    process = (
        f"{process_number}/{procurement.purchase_year}"
        if procurement.purchase_number and procurement.purchase_year
        else process_number
    )
    title = procurement.title or procurement.object_description or process
    dates: list[SimpleNamespace] = []
    for label, value in (
        ("Publicação", procurement.publication_at),
        ("Início das propostas", procurement.proposal_start_at),
        ("Fim das propostas", procurement.proposal_end_at),
        ("Início da sessão", procurement.session_start_at),
        ("Última atualização", procurement.last_source_update_at),
    ):
        if value is not None:
            dates.append(SimpleNamespace(label=label, value=_procurement_datetime(value)))

    location = "/".join(value for value in (procurement.municipality, procurement.uf) if value)
    sources = [
        _technical_origin(
            source=source.source,
            official_url=source.source_url,
            external_id=source.external_id,
        )
        for source in procurement.sources
    ]
    return SimpleNamespace(
        title=_display_text(title),
        object_description=_display_text(procurement.object_description),
        modality=_label(
            procurement.modality_key or procurement.modality, _PROCUREMENT_MODALITY_LABELS
        ),
        status=_label(procurement.status, _PROCUREMENT_STATUS_LABELS),
        agency_name=_display_text(procurement.agency_name),
        process=process,
        pncp_control_number=_display_text(procurement.pncp_control_number),
        location=_display_text(location),
        estimated_value=_brl(procurement.estimated_value),
        official_url=procurement.source_url,
        dates=dates,
        origin=_technical_origin(
            source=procurement.source,
            official_url=procurement.source_url,
            external_id=procurement.external_id,
        ),
        sources=sources,
    )


def _duration_label(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return "< 1 s"
    rounded = int(round(seconds))
    if rounded < 60:
        return f"{rounded} s"
    minutes, remaining = divmod(rounded, 60)
    return f"{minutes} min {remaining:02d} s"


def _diagnostic_messages(run: CrawlRun) -> list[str]:
    messages: list[str] = []
    for item in run.errors or []:
        if isinstance(item, dict) and item.get("message"):
            messages.append(str(item["message"]))
        elif item:
            messages.append(str(item))
    if not messages and run.diagnostic:
        messages.extend(part.strip() for part in run.diagnostic.splitlines() if part.strip())
    return messages


def _friendly_source_error(source_id: str, diagnostics: list[str]) -> str | None:
    if not diagnostics:
        return None
    source = _SOURCE_LABELS.get(source_id, source_id)
    joined = " ".join(diagnostics).casefold()
    if "readtimeout" in joined or "timeout" in joined:
        return f"{source} não respondeu dentro do tempo limite após as tentativas automáticas."
    if "non-json" in joined or "formato" in joined:
        return f"{source} respondeu em um formato inesperado. A coleta pode ser tentada novamente."
    if "http 5" in joined or "temporary" in joined:
        return (
            f"{source} está temporariamente indisponível. Os demais resultados foram preservados."
        )
    return f"Não foi possível concluir a coleta em {source}. Os detalhes técnicos estão disponíveis abaixo."


def _source_views(run: CrawlRun) -> list[SimpleNamespace]:
    requested = ["pncp", "compras_gov"] if run.connector == "all" else [run.connector]
    all_diagnostics = _diagnostic_messages(run)
    stored_sources = (run.cursor or {}).get("sources", {})
    if not isinstance(stored_sources, dict):
        stored_sources = {}

    failed_legacy: set[str] = set()
    for source_id in requested:
        if any(
            message.casefold().startswith(f"{source_id}:".casefold()) for message in all_diagnostics
        ):
            failed_legacy.add(source_id)

    views: list[SimpleNamespace] = []
    for source_id in requested:
        stored = stored_sources.get(source_id, {})
        if not isinstance(stored, dict):
            stored = {}
        diagnostics_value = stored.get("diagnostics", [])
        diagnostics = (
            [str(item) for item in diagnostics_value] if isinstance(diagnostics_value, list) else []
        )
        if not diagnostics:
            diagnostics = [
                message
                for message in all_diagnostics
                if message.casefold().startswith(f"{source_id}:".casefold())
            ]
        status = str(stored.get("status") or "")
        active = run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
        if active:
            # A source has no terminal result until the whole crawl finishes.
            status = "running" if run.status is CrawlRunStatus.RUNNING else "pending"
        elif not status:
            status = "temporary_error" if source_id in failed_legacy else "completed"
        found_value = stored.get("records_found")
        if found_value is None and not active:
            successful = [item for item in requested if item not in failed_legacy]
            found_value = (
                run.records_found if len(successful) == 1 and source_id in successful else None
            )
        duration_value = stored.get("duration_seconds")
        duration = float(duration_value) if isinstance(duration_value, (int, float)) else None
        attempts_value = stored.get("max_attempts_per_request")
        max_attempts = (
            int(attempts_value)
            if isinstance(attempts_value, (int, float))
            else get_settings().http_max_retries + 1
        )
        retryable = (
            not active and status in {"temporary_error", "failed", "partial"} and bool(diagnostics)
        )
        views.append(
            SimpleNamespace(
                id=source_id,
                label=_SOURCE_LABELS.get(source_id, source_id),
                status=status,
                status_label=_STATUS_LABELS.get(status, status.replace("_", " ").title()),
                records_found=found_value,
                duration=_duration_label(duration),
                max_attempts=max_attempts,
                friendly_error=None if active else _friendly_source_error(source_id, diagnostics),
                technical_detail="\n".join(diagnostics),
                retryable=retryable,
            )
        )
    return views


def _crawl_view(run: CrawlRun) -> SimpleNamespace:
    duration: float | None = None
    if run.started_at and run.finished_at:
        duration = max((run.finished_at - run.started_at).total_seconds(), 0)
    status = run.status.value
    general_diagnostics = [
        message
        for message in _diagnostic_messages(run)
        if not any(message.casefold().startswith(f"{source_id}:") for source_id in _SOURCE_LABELS)
    ]
    general_error: str | None = None
    if general_diagnostics:
        joined = " ".join(general_diagnostics).casefold()
        if "lease" in joined or "equivalent crawl" in joined:
            general_error = "Outra coleta equivalente já estava em andamento."
        elif "document" in joined:
            general_error = "A coleta terminou, mas houve falha no processamento de documentos."
        else:
            general_error = "A coleta terminou com uma pendência que requer atenção."
    documents_progress: str | None = None
    stored_sources = (run.cursor or {}).get("sources", {})
    if isinstance(stored_sources, dict):
        documents_result = stored_sources.get("documents")
        if isinstance(documents_result, dict):
            documents_progress = (
                f"{int(documents_result.get('documents_processed', 0))} documento(s) neste lote · "
                f"{int(documents_result.get('events_created', 0))} evento(s) · "
                f"{int(documents_result.get('leads_created', 0))} lead(s)"
            )
    return SimpleNamespace(
        id=run.id,
        connector_label=_CONNECTOR_LABELS.get(run.connector, run.connector),
        started_at=_local_datetime(run.started_at),
        finished_at=_local_datetime(run.finished_at),
        status=status,
        status_label=_STATUS_LABELS.get(status, status.title()),
        active=run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING},
        records_found=run.records_found,
        duration=_duration_label(duration),
        documents_progress=documents_progress,
        sources=_source_views(run),
        general_error=general_error,
        general_technical_detail="\n".join(general_diagnostics),
    )


def _filter_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value)
    return None


@router.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse("/dashboard", status_code=307)


@router.get("/dashboard")
async def dashboard(request: Request, db: AsyncSession = Depends(get_db)):
    """Render operational counts and recent opportunities."""
    counts = {
        "Contratações monitoradas": int((await db.scalar(select(func.count(Procurement.id)))) or 0),
        "Novos processos": int(
            (
                await db.scalar(
                    select(func.count(Procurement.id)).where(
                        Procurement.created_at >= datetime.now(UTC) - timedelta(hours=24)
                    )
                )
            )
            or 0
        ),
        "Eventos detectados": int((await db.scalar(select(func.count(ProcurementEvent.id)))) or 0),
        "Empresas identificadas": int((await db.scalar(select(func.count(Company.id)))) or 0),
        "Leads": int((await db.scalar(select(func.count(Lead.id)))) or 0),
        "Prazos em 24 horas": int(
            (
                await db.scalar(
                    select(func.count(Deadline.id)).where(
                        Deadline.status == DeadlineStatus.DUE_WITHIN_24H
                    )
                )
            )
            or 0
        ),
        "Prazos vencidos": int(
            (
                await db.scalar(
                    select(func.count(Deadline.id)).where(Deadline.status == DeadlineStatus.EXPIRED)
                )
            )
            or 0
        ),
        "Pendências de revisão": int(
            (
                await db.scalar(
                    select(func.count(Lead.id)).where(Lead.lead_status == LeadStatus.PENDING_REVIEW)
                )
            )
            or 0
        ),
        "Erros de coleta": int(
            (
                await db.scalar(
                    select(func.count(CrawlRun.id)).where(CrawlRun.status == CrawlRunStatus.FAILED)
                )
            )
            or 0
        ),
    }
    recent = (
        await db.scalars(
            select(Lead)
            .options(
                selectinload(Lead.company),
                selectinload(Lead.triggering_event),
                selectinload(Lead.deadline),
            )
            .order_by(Lead.created_at.desc())
            .limit(10)
        )
    ).all()
    leads = [
        SimpleNamespace(
            id=item.id,
            company_name=item.company.legal_name or item.company.normalized_name,
            event_type=item.triggering_event.event_type.value,
            score=item.score,
            deadline_status=item.deadline.status.value if item.deadline else "UNKNOWN",
            requires_manual_review=item.triggering_event.requires_manual_review,
        )
        for item in recent
    ]
    return templates.TemplateResponse(
        request, "dashboard.html", {"metrics": counts, "leads": leads}
    )


@router.get("/procurements")
async def procurements_page(
    request: Request,
    uf: str | None = Query(None, min_length=2, max_length=2),
    municipality: str | None = None,
    agency: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    statement = select(Procurement)
    if uf:
        statement = statement.where(Procurement.uf == uf.upper())
    if municipality:
        statement = statement.where(Procurement.municipality.ilike(f"%{municipality}%"))
    if agency:
        statement = statement.where(Procurement.agency_name.ilike(f"%{agency}%"))
    rows = (
        await db.scalars(statement.order_by(Procurement.publication_at.desc()).limit(200))
    ).all()
    return templates.TemplateResponse(
        request,
        "procurements.html",
        {
            "procurements": rows,
            "filters": SimpleNamespace(uf=uf, municipality=municipality, agency=agency),
        },
    )


@router.get("/procurements/{procurement_id}")
async def procurement_page(
    request: Request,
    procurement_id: UUID,
    item_query: str | None = Query(None, max_length=200),
    db: AsyncSession = Depends(get_db),
):
    procurement = await db.scalar(
        select(Procurement)
        .where(Procurement.id == procurement_id)
        .options(
            selectinload(Procurement.sources),
            selectinload(Procurement.items),
            selectinload(Procurement.documents),
            selectinload(Procurement.participants).selectinload(Participant.company),
            selectinload(Procurement.participants).selectinload(Participant.item),
            selectinload(Procurement.participants).selectinload(Participant.source_evidence),
            selectinload(Procurement.events).selectinload(ProcurementEvent.company),
            selectinload(Procurement.events).selectinload(ProcurementEvent.item),
            selectinload(Procurement.events).selectinload(ProcurementEvent.evidence),
        )
    )
    if procurement is None:
        raise HTTPException(status_code=404, detail="Contratação não encontrada")

    query = (item_query or "").strip()
    items = sorted(procurement.items, key=lambda item: item.item_number.casefold())
    if query:
        items = [item for item in items if _item_matches(item, query)]
    seen_companies: set[UUID] = set()
    candidate_companies: list[SimpleNamespace] = []
    for participant in procurement.participants:
        company = participant.company
        if company.id in seen_companies:
            continue
        seen_companies.add(company.id)
        candidate_companies.append(
            SimpleNamespace(
                id=company.id,
                name=company.legal_name or company.normalized_name,
                cnpj=_format_cnpj(company.cnpj),
            )
        )
    candidate_companies.sort(key=lambda company: company.name.casefold())
    return templates.TemplateResponse(
        request,
        "procurement_detail.html",
        {
            "procurement": _procurement_view(procurement),
            "items": [_item_view(item, procurement) for item in items],
            "item_query": query,
            "item_count": len(items),
            "documents": [
                _document_view(document, procurement) for document in procurement.documents
            ],
            "participants": [
                _participant_view(participant) for participant in procurement.participants
            ],
            "events": [_event_view(event) for event in procurement.events],
            "candidate_companies": candidate_companies,
            "link_feedback": request.query_params.get("link"),
        },
    )


@router.post("/procurements/{procurement_id}/events/{event_id}/link-company")
async def link_event_company(
    procurement_id: UUID,
    event_id: UUID,
    company_id: UUID = Form(...),
    reviewer: str = Form(""),
    note: str = Form(""),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Attach a human-confirmed company to an unattributed event and score its lead."""

    service = DocumentProcessingService()
    try:
        await service.link_event_company(
            event_id=event_id,
            company_id=company_id,
            reviewer=reviewer.strip() or None,
            note=note.strip() or None,
            expected_procurement_id=procurement_id,
            session=db,
        )
        await db.commit()
    except LookupError:
        raise HTTPException(status_code=404, detail="Evento ou empresa não encontrados") from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    finally:
        await service.aclose()
    return RedirectResponse(f"/procurements/{procurement_id}?link=ok", status_code=303)


@router.get("/leads")
async def leads_page(
    request: Request,
    min_score: int | None = Query(None, ge=0, le=100),
    event_type: str | None = None,
    requires_review: bool = False,
    db: AsyncSession = Depends(get_db),
):
    statement = select(Lead).options(
        selectinload(Lead.company), selectinload(Lead.triggering_event), selectinload(Lead.deadline)
    )
    if min_score is not None:
        statement = statement.where(Lead.score >= min_score)
    if event_type:
        statement = statement.join(Lead.triggering_event).where(
            ProcurementEvent.event_type == event_type
        )
    if requires_review:
        statement = statement.join(Lead.triggering_event).where(
            ProcurementEvent.requires_manual_review.is_(True)
        )
    rows = (
        (await db.scalars(statement.order_by(Lead.score.desc(), Lead.created_at.desc()).limit(200)))
        .unique()
        .all()
    )
    leads = [
        SimpleNamespace(
            id=item.id,
            company_name=item.company.legal_name or item.company.normalized_name,
            cnpj=item.company.cnpj,
            event_type=item.triggering_event.event_type.value,
            score=item.score,
            deadline_status=item.deadline.status.value if item.deadline else "UNKNOWN",
            lead_status=item.lead_status.value,
        )
        for item in rows
    ]
    filters = SimpleNamespace(
        min_score=min_score, event_type=event_type, requires_review=requires_review
    )
    return templates.TemplateResponse(request, "leads.html", {"leads": leads, "filters": filters})


@router.get("/leads/{lead_id}")
async def lead_page(request: Request, lead_id: UUID, db: AsyncSession = Depends(get_db)):
    lead = await db.scalar(
        select(Lead)
        .where(Lead.id == lead_id)
        .options(
            selectinload(Lead.company),
            selectinload(Lead.triggering_event).selectinload(ProcurementEvent.evidence),
            selectinload(Lead.deadline),
            selectinload(Lead.outreach_drafts),
        )
    )
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead não encontrado")
    view = SimpleNamespace(
        **model_dict(lead),
        company_name=lead.company.legal_name or lead.company.normalized_name,
        cnpj=lead.company.cnpj,
    )
    evidences = [lead.triggering_event.evidence] if lead.triggering_event.evidence else []
    draft = lead.outreach_drafts[-1] if lead.outreach_drafts else None
    return templates.TemplateResponse(
        request, "lead_detail.html", {"lead": view, "evidences": evidences, "draft": draft}
    )


@router.get("/crawls")
async def crawls_page(request: Request, db: AsyncSession = Depends(get_db)):
    views, has_active = await _crawl_views(db)
    return templates.TemplateResponse(
        request, "crawls.html", {"crawls": views, "has_active": has_active}
    )


@router.get("/crawls/table")
async def crawls_table(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX fragment repolled while a crawl is still running or pending."""

    views, has_active = await _crawl_views(db)
    return templates.TemplateResponse(
        request, "_crawl_table.html", {"crawls": views, "has_active": has_active}
    )


async def _crawl_views(db: AsyncSession) -> tuple[list[SimpleNamespace], bool]:
    rows = (
        await db.scalars(select(CrawlRun).order_by(CrawlRun.created_at.desc()).limit(200))
    ).all()
    views = [_crawl_view(run) for run in rows]
    return views, any(view.active for view in views)


@router.post("/crawls/run")
async def run_crawl_from_web(
    connector: str = Form("all"),
    uf: str = Form("MA"),
    days: int = Form(7),
    batch_size: int = Form(30),
) -> RedirectResponse:
    """Start a default crawl from the dashboard without exposing JSON details."""

    request = PipelineRequest(
        connector=connector,
        uf=uf,
        days=days,
        document_batch_size=max(1, batch_size),
    )
    run = await IngestionPipeline().create_run(request)
    launch_crawl(run.id, request)
    return RedirectResponse("/crawls", status_code=303)


@router.post("/crawls/{run_id}/retry")
async def retry_failed_crawl_source(
    run_id: UUID,
    connector: str = Form(...),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Retry only one failed source using the original crawl filters."""

    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Coleta não encontrada")
    failed_sources = {source.id for source in _source_views(run) if source.retryable}
    if connector not in failed_sources or connector not in _SOURCE_LABELS:
        raise HTTPException(status_code=409, detail="Essa fonte não possui falha repetível")

    filters = run.filters or {}
    modalities = filters.get("modalities") or get_settings().default_modalities
    retry_request = PipelineRequest(
        connector=connector,
        uf=str(filters.get("uf") or get_settings().default_uf),
        days=filters.get("days"),
        start_date=_filter_date(filters.get("start_date")),
        end_date=_filter_date(filters.get("end_date")),
        modalities=tuple(str(item) for item in modalities),
        municipality=filters.get("municipality"),
        agency=filters.get("agency"),
        keyword=filters.get("keyword"),
        max_pages=filters.get("max_pages"),
        process_documents=bool(filters.get("process_documents", True)),
        document_batch_size=filters.get("document_batch_size"),
    )
    retry_run = await IngestionPipeline().create_run(retry_request)
    launch_crawl(retry_run.id, retry_request)
    return RedirectResponse("/crawls", status_code=303)


@router.get("/settings")
async def settings_page(request: Request):
    return templates.TemplateResponse(
        request, "settings.html", {"settings": get_settings().public_view()}
    )


@router.get("/source-capabilities")
async def capabilities_page(request: Request):
    return templates.TemplateResponse(
        request, "source_capabilities.html", {"capabilities": get_source_capabilities()}
    )
