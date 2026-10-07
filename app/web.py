"""Server-rendered Portuguese web interface."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
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

from app.api.routes.crawls import cancel_crawl, launch_crawl
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
from app.repositories.price_registries import PriceRegistryRepository
from app.repositories.procurements import (
    CompanyRepository,
    ParticipantRepository,
    ProcurementEventRepository,
    ProcurementRepository,
)
from app.services.identifiers import canonical_modality, is_valid_cnpj, normalize_cnpj
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.processor import DocumentProcessingService
from app.services.maintenance import (
    COUNT_TABLE_LABELS,
    MaintenanceBlocked,
    clear_all_data,
    data_counts,
    maintenance_blocked_reason,
)

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
_BRAZILIAN_UFS = (
    ("AC", "Acre"),
    ("AL", "Alagoas"),
    ("AP", "Amapá"),
    ("AM", "Amazonas"),
    ("BA", "Bahia"),
    ("CE", "Ceará"),
    ("DF", "Distrito Federal"),
    ("ES", "Espírito Santo"),
    ("GO", "Goiás"),
    ("MA", "Maranhão"),
    ("MT", "Mato Grosso"),
    ("MS", "Mato Grosso do Sul"),
    ("MG", "Minas Gerais"),
    ("PA", "Pará"),
    ("PB", "Paraíba"),
    ("PR", "Paraná"),
    ("PE", "Pernambuco"),
    ("PI", "Piauí"),
    ("RJ", "Rio de Janeiro"),
    ("RN", "Rio Grande do Norte"),
    ("RS", "Rio Grande do Sul"),
    ("RO", "Rondônia"),
    ("RR", "Roraima"),
    ("SC", "Santa Catarina"),
    ("SP", "São Paulo"),
    ("SE", "Sergipe"),
    ("TO", "Tocantins"),
)
_PROCUREMENT_MODALITY_LABELS = {
    "pregao_eletronico": "Pregão eletrônico",
    "pregao_presencial": "Pregão presencial",
    "concorrencia_eletronica": "Concorrência eletrônica",
    "concorrencia_presencial": "Concorrência presencial",
    "concorrencia": "Concorrência",
    "dispensa": "Dispensa de licitação",
    "dispensa_eletronica": "Dispensa eletrônica",
    "inexigibilidade": "Inexigibilidade",
    "leilao": "Leilão",
    "leilao_eletronico": "Leilão eletrônico",
    "leilao_presencial": "Leilão presencial",
    "concurso": "Concurso",
    "dialogo_competitivo": "Diálogo competitivo",
    "credenciamento": "Credenciamento",
    "pre_qualificacao": "Pré-qualificação",
    "manifestacao_de_interesse": "Manifestação de interesse",
}
_PROFILTER_MODALITY_KEYS = (
    "pregao_eletronico",
    "pregao_presencial",
    "concorrencia_eletronica",
    "concorrencia_presencial",
    "concurso",
    "leilao_eletronico",
    "leilao_presencial",
    "dialogo_competitivo",
    "dispensa",
    "inexigibilidade",
    "credenciamento",
    "pre_qualificacao",
    "manifestacao_de_interesse",
)
_PROCUREMENT_TYPE_LABELS = {
    "licitacao": "Licitação",
    "contratacao_direta": "Contratação direta",
    "procedimento_auxiliar": "Procedimento auxiliar",
}
_PROCUREMENT_STATUS_CATEGORY_LABELS = {
    "aberta": "Proposta aberta",
    "encerrada": "Encerrada",
    "cancelada": "Cancelada",
    "suspensa": "Suspensa",
    "desconhecida": "Sem prazo conhecido",
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
_PARTICIPANT_STATUS_LABELS = {
    "winner": "Vencedora",
    "awarded": "Adjudicatária",
    "participant": "Participante",
    "disqualified": "Desclassificada",
    "ineligible": "Inabilitada",
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
_REVIEW_DECISION_LABELS = {
    "approved": "Aprovado",
    "rejected": "Rejeitado",
    "needs_changes": "Ajustes solicitados",
}


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    """Convert a local calendar day into an inclusive UTC range."""
    local_timezone = ZoneInfo(get_settings().timezone)
    start = datetime.combine(day, time.min, tzinfo=local_timezone).astimezone(UTC)
    end = datetime.combine(day, time.max, tzinfo=local_timezone).astimezone(UTC)
    return start, end


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


def _stats_datetime(stats: dict[str, object]) -> str:
    value = stats.get("last_participation_at")
    return _procurement_datetime(value if isinstance(value, datetime) else None)


def _brl(value: Decimal | None) -> str:
    if value is None:
        return "Não informado"
    rendered = f"{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {rendered}"


templates.env.filters["brl"] = _brl
templates.env.filters["display_text"] = _display_text
templates.env.filters["modality_label"] = lambda value: _label(value, _PROCUREMENT_MODALITY_LABELS)
templates.env.filters["procurement_type_label"] = lambda value: _label(
    value, _PROCUREMENT_TYPE_LABELS
)


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
        website=company.website,
        domain=company.domain,
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
        stored = stored_sources.get(source_id) or stored_sources.get(
            f"{source_id}:price_registries"
        )
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
        failed_value = stored.get("records_failed")
        records_failed = int(failed_value) if isinstance(failed_value, (int, float)) else 0
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
                records_failed=records_failed,
                duration=_duration_label(duration),
                max_attempts=max_attempts,
                friendly_error=None if active else _friendly_source_error(source_id, diagnostics),
                technical_detail="\n".join(diagnostics),
                retryable=retryable,
            )
        )
    return views


def _crawl_progress_label(stored_sources: dict[str, object]) -> str | None:
    """Return a human-readable in-flight progress label, when available."""

    for result in stored_sources.values():
        if not isinstance(result, dict):
            continue
        progress = result.get("progress")
        if not isinstance(progress, dict):
            continue
        processed = int(progress.get("processed", 0) or 0)
        total = int(progress.get("total", 0) or 0)
        if total and processed < total:
            return f"{processed}/{total} registro(s)"
    return None


def _crawl_view(run: CrawlRun) -> SimpleNamespace:
    duration: float | None = None
    if run.started_at and run.finished_at:
        duration = max((run.finished_at - run.started_at).total_seconds(), 0)
    status = run.status.value
    status_label = _STATUS_LABELS.get(status, status.title())
    if run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING} and run.cancel_requested:
        status_label = "Cancelando"
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
    progress_label: str | None = None
    stored_sources = (run.cursor or {}).get("sources", {})
    if isinstance(stored_sources, dict):
        documents_result = stored_sources.get("documents")
        if isinstance(documents_result, dict):
            processed_count = int(documents_result.get("documents_processed", 0))
            total_count = documents_result.get("documents_total")
            total_label = f"/{int(total_count)}" if isinstance(total_count, int) else ""
            documents_progress = (
                f"{processed_count}{total_label} documento(s) neste lote · "
                f"{int(documents_result.get('events_created', 0))} evento(s) · "
                f"{int(documents_result.get('leads_created', 0))} lead(s)"
            )
        progress_label = _crawl_progress_label(stored_sources)
    return SimpleNamespace(
        id=run.id,
        connector_label=_CONNECTOR_LABELS.get(run.connector, run.connector),
        mode_label=(
            "Atas (ARP)"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "price_registries"
            else None
        ),
        started_at=_local_datetime(run.started_at),
        finished_at=_local_datetime(run.finished_at),
        status=status,
        status_label=status_label,
        active=run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING},
        records_found=run.records_found,
        duration=_duration_label(duration),
        progress=progress_label,
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
            created_at=_local_datetime(item.created_at),
            deadline_status=item.deadline.status.value if item.deadline else "UNKNOWN",
            requires_manual_review=item.triggering_event.requires_manual_review,
        )
        for item in recent
    ]
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "metrics": counts,
            "leads": leads,
            "ufs": _BRAZILIAN_UFS,
            "selected_uf": get_settings().default_uf,
        },
    )


@router.get("/procurements")
async def procurements_page(
    request: Request,
    uf: str | None = Query(None, min_length=2, max_length=2),
    municipality: str | None = None,
    agency: str | None = None,
    agency_cnpj: str | None = None,
    company_cnpj: str | None = None,
    modality: list[str] | None = Query(None),
    procurement_type: str | None = None,
    status_category: str | None = None,
    is_srp: str | None = None,
    value_min: Decimal | None = Query(None, ge=0),
    value_max: Decimal | None = Query(None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    selected_modalities = [
        value for value in (modality or []) if canonical_modality(value) is not None
    ]
    selected_type = procurement_type if procurement_type in _PROCUREMENT_TYPE_LABELS else None
    selected_category = (
        status_category if status_category in _PROCUREMENT_STATUS_CATEGORY_LABELS else None
    )
    parsed_srp: bool | None = None
    if is_srp in {"true", "1", "sim"}:
        parsed_srp = True
    elif is_srp in {"false", "0", "nao", "não"}:
        parsed_srp = False
    selected_agency_cnpj = normalize_cnpj(agency_cnpj) if agency_cnpj else None
    if selected_agency_cnpj is not None and len(selected_agency_cnpj) != 14:
        selected_agency_cnpj = None
    selected_company_cnpj = normalize_cnpj(company_cnpj) if company_cnpj else None
    if selected_company_cnpj is not None and not is_valid_cnpj(selected_company_cnpj):
        selected_company_cnpj = None
    invalid_cnpj = bool(
        (agency_cnpj and selected_agency_cnpj is None)
        or (company_cnpj and selected_company_cnpj is None)
    )
    page = await ProcurementRepository(db).list_filtered(
        page=1,
        page_size=200,
        uf=uf,
        municipality=municipality,
        agency=agency,
        agency_cnpj=selected_agency_cnpj,
        company_cnpj=selected_company_cnpj,
        modalities=selected_modalities,
        procurement_type=selected_type,
        status_category=selected_category,
        is_srp=parsed_srp,
        value_min=value_min,
        value_max=value_max,
    )
    return templates.TemplateResponse(
        request,
        "procurements.html",
        {
            "procurements": page.items,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(
                uf=uf.upper() if uf else None,
                municipality=municipality,
                agency=agency,
                agency_cnpj=agency_cnpj,
                company_cnpj=company_cnpj,
                company_cnpj_valid=selected_company_cnpj,
                invalid_cnpj=invalid_cnpj,
                modalities=selected_modalities,
                procurement_type=selected_type,
                status_category=selected_category,
                is_srp=is_srp if parsed_srp is not None else None,
                value_min=value_min,
                value_max=value_max,
            ),
            "modality_options": [
                (key, _PROCUREMENT_MODALITY_LABELS[key]) for key in _PROFILTER_MODALITY_KEYS
            ],
            "type_options": list(_PROCUREMENT_TYPE_LABELS.items()),
            "status_category_options": list(_PROCUREMENT_STATUS_CATEGORY_LABELS.items()),
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


@router.get("/empresas")
async def companies_page(
    request: Request,
    search: str | None = Query(None, max_length=120),
    uf: str | None = Query(None, min_length=2, max_length=2),
    db: AsyncSession = Depends(get_db),
):
    """List companies with auditable participation counters."""

    result = await CompanyRepository(db).list_with_stats(
        page=1, page_size=200, search=search, uf=uf
    )
    companies = [
        SimpleNamespace(
            cnpj=company.cnpj,
            cnpj_label=_format_cnpj(company.cnpj),
            name=_display_text(company.legal_name or company.normalized_name),
            location="/".join(value for value in (company.municipality, company.uf) if value)
            or "—",
            participations=stats["participations"],
            awarded=stats["awarded"],
            disqualified=stats["disqualified"],
            ineligible=stats["ineligible"],
            last_participation_at=_stats_datetime(stats),
        )
        for company, stats in result.items
    ]
    return templates.TemplateResponse(
        request,
        "companies.html",
        {
            "companies": companies,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(uf=uf.upper() if uf else None, search=search),
        },
    )


@router.get("/empresas/{cnpj}")
async def company_page(
    request: Request,
    cnpj: str,
    status: str | None = Query(None),
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Show one company's participation and event history with provenance."""

    normalized = normalize_cnpj(cnpj)
    company = (
        await CompanyRepository(db).find_by_cnpj(normalized) if normalized is not None else None
    )
    if company is None:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")
    selected_status = status if status in _PARTICIPANT_STATUS_LABELS else None
    stats = await CompanyRepository(db).stats_for_company(company.id)
    participations = await ParticipantRepository(db).list_for_company(
        company.id,
        page=1,
        page_size=200,
        statuses=[selected_status] if selected_status else None,
        active_only=active_only,
    )
    events = await ProcurementEventRepository(db).list_for_company(
        company.id, page=1, page_size=200
    )
    return templates.TemplateResponse(
        request,
        "company_detail.html",
        {
            "company": SimpleNamespace(
                cnpj=company.cnpj,
                cnpj_label=_format_cnpj(company.cnpj),
                name=_display_text(company.legal_name or company.normalized_name),
                trade_name=_display_text(company.trade_name),
                location="/".join(value for value in (company.municipality, company.uf) if value)
                or "—",
                website=company.website,
                domain=company.domain,
            ),
            "stats": stats,
            "stats_last_participation_at": _stats_datetime(stats),
            "participations": [
                SimpleNamespace(
                    procurement_id=item.procurement_id,
                    process=item.procurement.purchase_number
                    or item.procurement.pncp_control_number
                    or item.procurement.external_id,
                    agency=_display_text(item.procurement.agency_name),
                    modality=_label(
                        item.procurement.modality_key or item.procurement.modality,
                        _PROCUREMENT_MODALITY_LABELS,
                    ),
                    type_label=_label(item.procurement.procurement_type, _PROCUREMENT_TYPE_LABELS),
                    is_srp=item.procurement.is_srp,
                    estimated_value=_brl(item.procurement.estimated_value),
                    status=_display_text(item.procurement.status),
                    role=_label(item.participation_role, _PARTICIPANT_ROLE_LABELS),
                    outcome=_label(item.status_code, _PARTICIPANT_STATUS_LABELS),
                    final_value=_brl(item.final_value),
                    confidence=_confidence(item.confidence),
                    origin=_technical_origin(
                        source=item.source,
                        official_url=item.procurement.source_url,
                        source_record_id=item.source_record_id,
                        evidence_id=item.source_evidence_id,
                    ),
                )
                for item in participations.items
            ],
            "events": [_event_view(event) for event in events.items],
            "status_options": list(_PARTICIPANT_STATUS_LABELS.items()),
            "filters": SimpleNamespace(status=selected_status, active_only=active_only),
        },
    )


@router.get("/atas")
async def price_registries_page(
    request: Request,
    search: str | None = Query(None, max_length=120),
    agency: str | None = None,
    registry_number: str | None = None,
    supplier_cnpj: str | None = None,
    valid_on: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """List atas de registro de preços with vigência and supplier filters."""

    normalized_supplier = normalize_cnpj(supplier_cnpj) if supplier_cnpj else None
    result = await PriceRegistryRepository(db).list_filtered(
        page=1,
        page_size=200,
        search=search,
        agency=agency,
        registry_number=registry_number,
        supplier_cnpj=normalized_supplier,
        valid_on=valid_on,
    )
    registries = [
        SimpleNamespace(
            id=item.id,
            registry_number=_display_text(item.registry_number),
            year=_display_text(item.year),
            agency_name=_display_text(item.agency_name),
            object_description=_display_text(item.object_description),
            status=_display_text(item.status),
            valid_from=_procurement_datetime(item.valid_from),
            valid_until=_procurement_datetime(item.valid_until),
            total_value=_brl(item.total_value),
            source=item.source,
        )
        for item in result.items
    ]
    return templates.TemplateResponse(
        request,
        "atas.html",
        {
            "registries": registries,
            "filters": SimpleNamespace(
                search=search,
                agency=agency,
                registry_number=registry_number,
                supplier_cnpj=supplier_cnpj,
                valid_on=valid_on.isoformat() if valid_on else "",
            ),
        },
    )


@router.get("/atas/{registry_id}")
async def price_registry_page(
    request: Request,
    registry_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Show one ata with its registered items/suppliers and provenance."""

    registry = await PriceRegistryRepository(db).get_detail(registry_id)
    if registry is None:
        raise HTTPException(status_code=404, detail="Ata não encontrada")
    return templates.TemplateResponse(
        request,
        "ata_detail.html",
        {
            "registry": SimpleNamespace(
                registry_number=_display_text(registry.registry_number),
                year=_display_text(registry.year),
                agency_name=_display_text(registry.agency_name),
                agency_cnpj=_format_cnpj(registry.agency_cnpj),
                uasg=_display_text(registry.uasg),
                object_description=_display_text(registry.object_description),
                status=_display_text(registry.status),
                signed_at=_procurement_datetime(registry.signed_at),
                published_at=_procurement_datetime(registry.published_at),
                valid_from=_procurement_datetime(registry.valid_from),
                valid_until=_procurement_datetime(registry.valid_until),
                total_value=_brl(registry.total_value),
                allows_adhesion=(
                    "Sim"
                    if registry.allows_adhesion
                    else ("Não" if registry.allows_adhesion is not None else "Não informado")
                ),
                pncp_control_number=_display_text(registry.pncp_control_number),
                linked_control_number=_display_text(registry.linked_pncp_control_number),
                procurement_id=registry.procurement_id,
                source_url=registry.source_url,
                source=registry.source,
            ),
            "items": [
                SimpleNamespace(
                    item_number=_display_text(item.item_number),
                    description=_display_text(item.description),
                    unit=_display_text(item.unit),
                    quantity=_quantity_with_unit(item.quantity, None),
                    unit_value=_brl(item.unit_value),
                    total_value=_brl(item.total_value),
                    max_adhesion=_quantity_with_unit(item.max_adhesion_quantity, None),
                    supplier_name=_display_text(item.supplier_name),
                    supplier_cnpj=item.supplier_cnpj,
                    supplier_cnpj_label=_format_cnpj(item.supplier_cnpj),
                    company_id=item.company_id,
                )
                for item in registry.items
            ],
        },
    )


@router.get("/leads")
async def leads_page(
    request: Request,
    min_score: int | None = Query(None, ge=0, le=100),
    event_type: str | None = None,
    requires_review: bool = False,
    created_from: date | None = Query(None),
    created_to: date | None = Query(None),
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
    if created_from is not None:
        start, _ = _day_bounds(created_from)
        statement = statement.where(Lead.created_at >= start)
    if created_to is not None:
        _, end = _day_bounds(created_to)
        statement = statement.where(Lead.created_at <= end)
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
            created_at=_local_datetime(item.created_at),
            deadline_status=item.deadline.status.value if item.deadline else "UNKNOWN",
            lead_status=item.lead_status.value,
        )
        for item in rows
    ]
    filters = SimpleNamespace(
        min_score=min_score,
        event_type=event_type,
        requires_review=requires_review,
        created_from=created_from.isoformat() if created_from else "",
        created_to=created_to.isoformat() if created_to else "",
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
            selectinload(Lead.reviews),
        )
    )
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead não encontrado")
    draft = lead.outreach_drafts[-1] if lead.outreach_drafts else None
    data = model_dict(lead)
    data.update(
        company_name=lead.company.legal_name or lead.company.normalized_name,
        cnpj=lead.company.cnpj,
        created_at=_local_datetime(lead.created_at),
        updated_at=_local_datetime(lead.updated_at),
        draft_created_at=_local_datetime(draft.created_at) if draft else None,
    )
    view = SimpleNamespace(**data)
    reviews = [
        SimpleNamespace(
            decision=_label(review.decision, _REVIEW_DECISION_LABELS),
            reviewer=review.reviewer,
            previous_status=review.previous_status,
            notes=review.notes,
            reviewed_at=_local_datetime(review.reviewed_at),
        )
        for review in lead.reviews
    ]
    evidences = [lead.triggering_event.evidence] if lead.triggering_event.evidence else []
    return templates.TemplateResponse(
        request,
        "lead_detail.html",
        {"lead": view, "evidences": evidences, "draft": draft, "reviews": reviews},
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
    process_documents: bool = Form(False),
) -> RedirectResponse:
    """Start a default crawl from the dashboard without exposing JSON details."""

    request = PipelineRequest(
        connector=connector,
        uf=uf,
        days=days,
        process_documents=process_documents,
        document_batch_size=max(1, batch_size),
    )
    run = await IngestionPipeline().create_run(request)
    launch_crawl(run.id, request)
    return RedirectResponse("/crawls", status_code=303)


@router.post("/crawls/{run_id}/cancel")
async def cancel_crawl_from_web(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Stop an active crawl started by the API or by the scheduler."""

    await cancel_crawl(run_id, db)
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
        mode=str(filters.get("mode") or "procurements"),
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
async def settings_page(request: Request, db: AsyncSession = Depends(get_db)):
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "settings": get_settings().public_view(),
            "counts": await data_counts(db),
            "count_labels": COUNT_TABLE_LABELS,
            "block_reason": await maintenance_blocked_reason(db),
            "reset_status": request.query_params.get("reset"),
        },
    )


@router.post("/settings/clear-data")
async def clear_data_from_web(
    confirmo: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Delete all collected data after an explicit confirmation checkbox."""

    if confirmo is None:
        return RedirectResponse("/settings?reset=sem-confirmacao", status_code=303)
    try:
        await clear_all_data(db)
    except MaintenanceBlocked:
        return RedirectResponse("/settings?reset=bloqueado", status_code=303)
    return RedirectResponse("/settings?reset=ok", status_code=303)


@router.get("/source-capabilities")
async def capabilities_page(request: Request):
    return templates.TemplateResponse(
        request, "source_capabilities.html", {"capabilities": get_source_capabilities()}
    )
