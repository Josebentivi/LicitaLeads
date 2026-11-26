"""Server-rendered Portuguese web interface."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, cast
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BeforeValidator
from sqlalchemy import false, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.api.routes.crawls import (
    cancel_crawl,
    continue_crawl_endpoint,
    launch_complete_collection,
    launch_crawl,
)
from app.api.routes.history import EntityType, history_page, relevant_changes
from app.api.routes.leads import generate_outreach
from app.api.serialization import model_dict
from app.config import Settings, get_settings
from app.connectors.capabilities import get_source_capabilities
from app.connectors.municipal_payments.sources import get_municipal_source, list_municipal_sources
from app.dependencies import get_db
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    Company,
    CompanyContact,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    Document,
    Evidence,
    FieldChange,
    FieldObservation,
    Lead,
    LeadReview,
    LeadStatus,
    Participant,
    Payment,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
    ReviewDecision,
    SourceRecord,
    TceMaVerifiedLink,
)
from app.models.participation import FactVerification
from app.navigation import (
    NAV_GROUPS,
    active_group_id,
    active_item_href,
    breadcrumbs,
    get_group,
)
from app.repositories.base import Page
from app.repositories.company_origins import ORIGIN_LABELS
from app.repositories.contracts import (
    ContractRepository,
    PaymentRepository,
    contract_value,
    published_term_dates,
)
from app.repositories.crawls import JobLeaseRepository
from app.repositories.current_evidence import evidence_current
from app.repositories.price_registries import PriceRegistryRepository
from app.repositories.procurements import (
    CompanyRepository,
    ProcurementRepository,
)
from app.schemas import OutreachRequest
from app.services.application_tests import ApplicationTestService, TestRunBusy
from app.services.collection_periods import (
    collection_today,
    earliest_collection_day,
    use_calendar_slices,
    validate_collection_period,
)
from app.services.complete_collection_views import attach_complete_stages
from app.services.crawl_monitor import (
    monitor_view,
    municipal_write_progress,
    nonnegative_int,
    timestamp,
)
from app.services.crawl_progress import safe_label
from app.services.deadlines.response import OFFICIAL_RESPONSE_DEADLINE_LABELS
from app.services.event_detection.detector import describe_event_scope
from app.services.event_detection.types import CompanyReference
from app.services.federal_payment_views import (
    FEDERAL_PAYMENT_LABELS,
    federal_batch_views,
    federal_day_label,
    federal_payment_category,
    federal_payment_filter,
    federal_payment_totals,
    latest_federal_batch,
)
from app.services.group_hubs import build_group_hub
from app.services.identifiers import (
    AUXILIARY_PROCEDURES,
    CONTRACTING_FORM_LABELS,
    LICITATION_MODALITIES,
    canonical_modality,
    contracting_form,
    is_valid_cnpj,
    normalize_cnpj,
)
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.complete_collection import (
    COMPLETE_COLLECTION_MODE,
    CollectionScope,
    CompleteCollection,
)
from app.services.ingestion.document_retry import (
    batch_document_ids,
    create_document_retry,
    document_processing_busy,
    documents_for_run,
    eligible,
)
from app.services.ingestion.federal_payments import (
    FEDERAL_BATCH_MODE,
    FEDERAL_CONNECTOR,
    FederalScope,
)
from app.services.ingestion.municipal_payments import (
    MunicipalPaymentIngestionService,
    discard_municipal_task,
    latest_municipal_runs,
    register_municipal_task,
)
from app.services.ingestion.payment_import import PaymentImportErrorList, PaymentImportService
from app.services.ingestion.processor import DocumentProcessingService
from app.services.ingestion.sao_luis_import import (
    MAX_CSV_BYTES,
    SaoLuisCsvImport,
    SaoLuisImportBusy,
)
from app.services.ingestion.tce_ma_observations import tce_observations_for
from app.services.maintenance import (
    COUNT_TABLE_LABELS,
    MaintenanceBlocked,
    active_leases,
    clear_all_data,
    data_counts,
    maintenance_blocked_reason,
)
from app.services.opportunity_workspace import (
    ACTIONABLE_STATUSES,
    OpportunityFilters,
    deadline_indication,
    deadline_usable,
    legacy_official_response_deadline_ids,
    loaded_opportunities,
    next_opportunity,
    opportunity_count,
    opportunity_query,
    resolve_queue_filters,
    safe_list_url,
)
from app.services.participation_integrity import deadline_current, event_verified, lead_current
from app.services.payment_tracking import (
    list_payment_tracking,
    set_payment_tracking,
    tracked_payment_cnpjs,
)
from app.services.price_registry_scopes import (
    known_price_registry_scopes,
    restore_price_registry_scopes,
)
from app.services.procurement_search import (
    COLLECTION_MODALITIES,
    GOVERNMENT_SPHERES,
    OBJECT_GROUPS,
    OBJECT_PRESETS,
    ObjectScope,
    normalized_search_text,
    published_sphere,
)
from app.services.public_source_links import origin_platform_url as _origin_platform_url
from app.services.public_source_links import procurement_source_page, source_link_label
from app.services.public_source_links import public_link as _public_link
from app.services.scheduler_control import scheduler_status, set_scheduler_paused
from app.services.tce_ma_review import (
    Excerpt,
    active_tce_links,
    add_tce_decision,
    confirm_tce_link,
    tce_decisions_for,
)
from app.services.write_admission import ApplicationSession, admit_before_row_lock
from app.shutdown_web import shutdown_available

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
templates.env.globals.update(
    nav_groups=NAV_GROUPS,
    nav_active_group=active_group_id,
    nav_active_item=active_item_href,
    nav_breadcrumbs=breadcrumbs,
    shutdown_available=shutdown_available,
)
_application_tests = ApplicationTestService()
_municipal_progress: dict[UUID, dict[str, Any]] = {}
COLLECTION_FORM_PATHS = {
    "/crawls/run": "/dashboard",
    "/crawls/collect": "/crawls#escopo",
    "/atas/collect": "/atas",
    "/contratos/collect": "/contratos",
}


async def collection_form_error(request: Request, message: str):
    """Keep only editable, nonsecret collection fields when validation fails."""
    form = await request.form()
    allowed = {
        "uf",
        "connector",
        "registry_scope",
        "agency_cnpj",
        "start_date",
        "end_date",
        "days",
        "batch_size",
        "process_documents",
        "force_refresh",
        "object_query",
        "object_group",
        "government_sphere",
        "procurements_only",
        "federal_scope",
    }
    values: dict[str, str | list[str]] = {
        key: str(value)[:250] for key, value in form.items() if key in allowed
    }
    for key in ("modalities", "object_preset"):
        values[key] = [str(value)[:80] for value in form.getlist(key)][:40]
    return templates.TemplateResponse(
        request,
        "collection_form_error.html",
        {
            "message": message,
            "values": values,
            "action": request.url.path,
            "back": COLLECTION_FORM_PATHS[request.url.path],
            "ufs": _BRAZILIAN_UFS,
            **_collection_object_options(get_settings()),
        },
        status_code=422,
    )


def _empty_query_to_none(value: object) -> object:
    """Treat blank HTML form fields as an omitted filter.

    Browsers always submit ``name=`` for empty inputs, while FastAPI parses
    typed query params strictly (``""`` is not ``None``), which would answer
    ``422`` on ordinary form submissions.
    """

    if isinstance(value, str) and not value.strip():
        return None
    return value


OptionalUfQuery = Annotated[str | None, BeforeValidator(_empty_query_to_none)]
OptionalDateQuery = Annotated[date | None, BeforeValidator(_empty_query_to_none)]
OptionalDateForm = Annotated[date | None, BeforeValidator(_empty_query_to_none)]
OptionalDecimalQuery = Annotated[Decimal | None, BeforeValidator(_empty_query_to_none)]
OptionalScoreQuery = Annotated[int | None, BeforeValidator(_empty_query_to_none)]


def _selected_uf(value: str | None) -> str | None:
    """Keep the strict two-letter rule without rejecting blank form fields."""

    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized if len(normalized) == 2 and normalized.isalpha() else None


_SOURCE_LABELS = {
    "pncp": "PNCP",
    "compras_gov": "Compras.gov.br",
    "transparencia": "Portal da Transparência",
    "manual": "Importação manual",
    **{
        f"municipal:{source.id}": f"{source.name} (municipal)"
        for source in list_municipal_sources()
    },
}
_CONNECTOR_LABELS = {
    **_SOURCE_LABELS,
    "all": "Todas as fontes",
    COMPLETE_COLLECTION_MODE: "Coleta completa",
    "payments:transparencia": "Portal da Transparência",
    FEDERAL_CONNECTOR: "Portal da Transparência",
}
_STATUS_LABELS = {
    "pending": "Aguardando",
    "running": "Em andamento",
    "completed": "Concluída",
    "partial": "Parcial",
    "failed": "Falhou",
    "cancelled": "Cancelada",
    "temporary_error": "Indisponível",
    "not_supported": "Sem cobertura nesta fonte",
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
    "pregao": "Pregão",
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
    "registro_cadastral": "Registro cadastral",
    "tomada_de_precos": "Tomada de preços",
}
_PROCUREMENT_TYPE_LABELS = {
    "licitacao": "Licitação",
    "contratacao_direta": "Contratação direta",
    "procedimento_auxiliar": "Procedimento auxiliar",
}
_PROCUREMENT_STATUS_CATEGORY_LABELS = {
    "prazo_resposta_aberto": "Prazo aberto para diligência ou recurso",
    "aberta": "Proposta aberta",
    "encerrada": "Encerrada",
    "cancelada": "Cancelada",
    "suspensa": "Suspensa",
    "desconhecida": "Sem prazo conhecido",
}
_LICITATION_MODALITY_LABELS = {key: label for key, (label, _) in LICITATION_MODALITIES.items()}
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
_LEAD_STATUS_LABELS = {
    "new": "Novo",
    "below_threshold": "Abaixo do limiar",
    "pending_review": "Revisão pendente",
    "approved": "Aprovado",
    "rejected": "Rejeitado",
    "needs_changes": "Ajustes solicitados",
    "archived": "Arquivado",
}
_DEADLINE_STATUS_LABELS = {
    "OPEN": "Aberto",
    "DUE_TODAY": "Vence hoje",
    "DUE_WITHIN_24H": "Vence em 24h",
    "DUE_WITHIN_72H": "Vence em 72h",
    "EXPIRED": "Vencido",
    "UNKNOWN": "Não informado",
    "REQUIRES_REVIEW": "Conferência necessária",
}
_DEADLINE_TYPE_LABELS = {
    "appeal": "Recurso",
    "intent_to_appeal": "Intenção de recurso",
    "counterargument": "Contrarrazões",
    "counterarguments": "Contrarrazões",
    "proposal": "Proposta",
    "documentation": "Documentação",
    "diligence": "Diligência",
    "event_response": "Resposta ao ato",
    **OFFICIAL_RESPONSE_DEADLINE_LABELS,
}

_EVENT_TYPE_LABELS = {
    "PROPOSAL_SUBMITTED": "Proposta apresentada",
    "PROPOSAL_ACCEPTED": "Proposta aceita",
    "PROPOSAL_REJECTED": "Proposta rejeitada",
    "DISQUALIFIED": "Desclassificação",
    "QUALIFIED": "Habilitação",
    "INELIGIBLE": "Inabilitação",
    "INTENT_TO_APPEAL": "Intenção de recurso",
    "APPEAL_WINDOW_OPENED": "Prazo para recurso aberto",
    "APPEAL_SUBMITTED": "Recurso apresentado",
    "COUNTERARGUMENT_OPENED": "Contrarrazões abertas",
    "COUNTERARGUMENT_SUBMITTED": "Contrarrazões apresentadas",
    "APPEAL_DECIDED": "Recurso decidido",
    "WINNER_DECLARED": "Vencedora declarada",
    "ADJUDICATED": "Adjudicação",
    "HOMOLOGATED": "Homologação",
    "SESSION_SUSPENDED": "Sessão suspensa",
    "SESSION_REOPENED": "Sessão reaberta",
    "SESSION_OPENED": "Sessão aberta",
    "BEST_OFFER_DECLARED": "Melhor oferta declarada",
    "NEGOTIATION_REQUESTED": "Negociação solicitada",
    "DILIGENCE_REQUESTED": "Diligência solicitada",
    "DOCUMENTATION_REQUESTED": "Documentação solicitada",
    "PARTICIPATION_DETECTED": "Participação em processo ativo",
    "UNKNOWN": "Não informado",
}
_PROCUREMENT_PHASE_LABELS = {
    "unknown": "Não informada",
    "proposals": "Propostas",
    "dispute": "Disputa",
    "diligence": "Diligência",
    "judgment": "Julgamento",
    "appeal": "Recurso",
    "counterargument": "Contrarrazões",
    "qualification": "Habilitação",
    "adjudication": "Adjudicação",
    "homologation": "Homologação",
    "closed": "Encerrada",
    "suspended": "Suspensa",
    "cancelled": "Cancelada",
    "failed": "Deserta/fracassada",
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
_CONTACT_TYPE_LABELS = {
    "address": "Endereço",
    "email": "E-mail",
    "phone": "Telefone",
    "whatsapp": "WhatsApp",
    "form": "Formulário",
    "website": "Site",
    "linkedin": "LinkedIn",
}
_CONTACT_STATUS_LABELS = {
    "discovered": "Descoberto",
    "verified": "Verificado",
    "invalid": "Inválido",
    "requires_review": "Revisão necessária",
}
_PAYMENT_PHASE_LABELS = {
    "empenhado": "Empenhado",
    "liquidado": "Liquidado",
    "pago": "Pago",
    "pago_agregado": "Pago agregado por empenho",
}


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    """Convert a local calendar day into an inclusive UTC range."""
    local_timezone = ZoneInfo(get_settings().timezone)
    start = datetime.combine(day, time.min, tzinfo=local_timezone).astimezone(UTC)
    end = datetime.combine(day, time.max, tzinfo=local_timezone).astimezone(UTC)
    return start, end


def _collection_period(days: int, *, today: date | None = None) -> SimpleNamespace:
    """Build the inclusive default window shown by the manual collection forms."""

    end = today or datetime.now(ZoneInfo(get_settings().timezone)).date()
    start = end - timedelta(days=max(days, 1) - 1)
    return SimpleNamespace(
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        label=f"{start:%d/%m/%Y} a {end:%d/%m/%Y}",
    )


def _collection_object_options(settings: Settings) -> dict[str, Any]:
    return {
        "object_presets": OBJECT_PRESETS,
        "object_groups": OBJECT_GROUPS,
        "government_spheres": GOVERNMENT_SPHERES,
        "collection_modalities": COLLECTION_MODALITIES,
        "min_collection_date": earliest_collection_day(
            collection_today(settings.timezone)
        ).isoformat(),
    }


def _new_procurement_collection_filters(
    settings: Settings,
    start_date: date | None,
    end_date: date | None,
    *,
    days: int | None,
    modalities: list[str] | None,
    object_query: str | None,
    object_preset: list[str] | None,
    object_group: str | None,
    government_sphere: str | None,
) -> tuple[tuple[str, ...], ObjectScope | None]:
    try:
        validate_collection_period(start_date, end_date, timezone=settings.timezone, days=days)
        selected = tuple(dict.fromkeys(modalities or ()))
        if set(selected).difference(dict(COLLECTION_MODALITIES)):
            raise ValueError("Selecione modalidades da lista de opções.")
        scope = ObjectScope.build(
            object_query, object_preset or (), object_group, government_sphere
        )
        return selected, scope
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


def _validate_collection_dates(
    start_date: date | None, end_date: date | None
) -> tuple[date | None, date | None]:
    """Require a coherent complete range when the forms use explicit dates."""

    if bool(start_date) != bool(end_date):
        raise HTTPException(status_code=422, detail="Informe as datas inicial e final do período")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422, detail="A data inicial não pode ser posterior à data final"
        )
    return start_date, end_date


def _deadline_view(
    deadline: Deadline | None, *, current: bool = True, revalidation_pending: bool = False
) -> SimpleNamespace:
    """A triggering fact is not a deadline; historical dates are never current urgency."""
    value = (deadline.explicit_deadline_at or deadline.estimated_deadline_at) if deadline else None
    kind = (
        "Prazo expresso na fonte"
        if deadline and deadline.explicit_deadline_at
        else "Prazo estimado — confirmar"
        if value
        else "Prazo não identificado"
    )
    if deadline and not current:
        kind = "Prazo histórico — não usar como atual"
    elif revalidation_pending:
        kind = "Prazo pendente de revalidação"
    elif deadline and value and deadline.calculation_method.value == "MANUAL":
        kind = "Prazo informado manualmente — conferir origem"
    relative = deadline_indication(
        value,
        explicit=bool(deadline and deadline.explicit_deadline_at),
        current=current,
        now=datetime.now(UTC),
        timezone=get_settings().timezone,
    )
    return SimpleNamespace(
        when=_local_datetime(value) if value else "Data-limite não identificada",
        kind=kind,
        recorded_status=_label(deadline.status, _DEADLINE_STATUS_LABELS)
        if deadline and current
        else "Não informado",
        status="Revalidação pendente" if revalidation_pending and current else relative,
        type=_label(deadline.deadline_type, _DEADLINE_TYPE_LABELS) if deadline else "Não informado",
        trigger=_local_datetime(deadline.trigger_at) if deadline else None,
        explanation=deadline.calculation_explanation if deadline else None,
        requires_review=revalidation_pending or bool(deadline and deadline.requires_manual_review),
        current=current,
    )


def _lead_view(
    lead: Lead,
    *,
    return_to: str = "/leads",
    current_deadline: bool = True,
    revalidation_pending: bool = False,
) -> SimpleNamespace:
    deadline = _deadline_view(
        lead.deadline, current=current_deadline, revalidation_pending=revalidation_pending
    )
    act = _event_act_view(lead.triggering_event, lead.company)
    return SimpleNamespace(
        id=lead.id,
        href=f"/leads/{lead.id}?{urlencode({'return_to': safe_list_url(return_to)})}",
        company_name=lead.company.legal_name or lead.company.normalized_name,
        cnpj=lead.company.cnpj,
        cnpj_label=_format_cnpj(lead.company.cnpj),
        event_type=act.label,
        event_scope=act.scope,
        reason=act.reason if act.award else lead.reason_summary,
        score=lead.score,
        created_at=_local_datetime(lead.created_at),
        deadline=deadline,
        deadline_status=deadline.status,
        lead_status=_label(lead.lead_status, _LEAD_STATUS_LABELS),
        requires_manual_review=lead.triggering_event.requires_manual_review,
    )


async def _deadline_display_ids(
    db: AsyncSession, deadline_ids: set[UUID]
) -> tuple[set[UUID], set[UUID]]:
    if not deadline_ids:
        return set(), set()
    deadlines = (
        await db.execute(
            select(Deadline.id, deadline_usable(datetime.now(UTC)).label("usable")).where(
                Deadline.id.in_(deadline_ids), deadline_current()
            )
        )
    ).all()
    usable_ids: set[UUID] = {identifier for identifier, usable in deadlines if usable}
    pending = await legacy_official_response_deadline_ids(
        db, {identifier for identifier, usable in deadlines if not usable}
    )
    return usable_ids | pending, pending


async def _lead_views(db: AsyncSession, rows, *, return_to: str = "/leads"):
    deadline_ids = {row.deadline_id for row in rows if row.deadline_id}
    current_ids, pending_revalidation = await _deadline_display_ids(db, deadline_ids)
    current_leads = (
        set(
            (
                await db.scalars(
                    select(Lead.id).where(Lead.id.in_([row.id for row in rows]), lead_current())
                )
            ).all()
        )
        if rows
        else set()
    )
    views = []
    for row in rows:
        view = _lead_view(
            row,
            return_to=return_to,
            current_deadline=row.deadline_id in current_ids and row.id in current_leads,
            revalidation_pending=row.deadline_id in pending_revalidation,
        )
        view.historical = row.id not in current_leads
        views.append(view)
    return views


def _list_context(request: Request, path: str, *, keys: set[str] | None = None) -> dict[str, Any]:
    params = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if keys is None or key in keys
    ]
    meaningful = [
        (key, value)
        for key, value in params
        if value
        and key not in {"page", "page_size", "view"}
        and not (
            value == "false"
            and key in {"requires_review", "include_history", "has_lead", "active_only"}
        )
    ]
    labels = {
        "q": "Busca",
        "search": "Busca",
        "uf": "UF",
        "lead_status": "Situação",
        "due": "Prazo",
        "created_from": "Adicionado de",
        "created_to": "Adicionado até",
        "published_from": "Publicação de",
        "published_to": "Publicação até",
        "agency_cnpj": "CNPJ do órgão",
        "company_cnpj": "CNPJ da empresa",
        "company_participation_only": "Apenas participações",
        "min_score": "Prioridade mínima",
        "event_type": "Evento",
        "municipality": "Município",
        "agency": "Órgão",
        "origin": "Origem da presença",
        "activity_location": "Local do órgão ou portal",
        "run": "Coleta",
        "status_category": "Situação",
        "supplier_cnpj": "CNPJ do fornecedor",
        "match_status": "Conciliação",
        "budget_function": "Função orçamentária",
        "federal_type": "Tipo federal",
        "fiscal_year": "Exercício",
        "source": "Fonte",
        "valid_on": "Vigente em",
        "registry_number": "Número da ata",
        "expiring_within_days": "A vencer em dias",
        "allows_adhesion": "Permite adesão",
        "modality": "Modalidade",
        "contracting_form": "Forma de contratação",
        "licitation_modality": "Modalidade de licitação",
        "auxiliary_procedure": "Procedimento auxiliar",
        "government_sphere": "Esfera do órgão",
        "object_query": "Objeto ou item",
        "object_preset": "Objeto sugerido",
        "object_group": "Grupo de objetos",
        "phase": "Fase",
        "procurement_type": "Tipo de contratação",
        "has_lead": "Com oportunidade",
        "active_only": "Proposta aberta",
        "requires_review": "Evidência exige conferência",
        "include_history": "Histórico",
    }
    values = {
        **ORIGIN_LABELS,
        **_LEAD_STATUS_LABELS,
        **_EVENT_TYPE_LABELS,
        **_PROCUREMENT_STATUS_CATEGORY_LABELS,
        **_PROCUREMENT_MODALITY_LABELS,
        **_PROCUREMENT_PHASE_LABELS,
        **_PROCUREMENT_TYPE_LABELS,
        **_LICITATION_MODALITY_LABELS,
        **GOVERNMENT_SPHERES,
        **FEDERAL_PAYMENT_LABELS,
        **OBJECT_GROUPS,
        **{preset.key: preset.label for preset in OBJECT_PRESETS},
        **_SOURCE_LABELS,
        **_PAYMENT_PHASE_LABELS,
        "today": "Hoje",
        "open": "Prazo aberto para diligência ou recurso",
        "actionable": "Para analisar",
        "24h": "Próximas 24 horas",
        "72h": "Próximas 72 horas",
        "true": "Sim",
    }
    chips = []
    for key, value in meaningful:
        remaining = [(k, v) for k, v in params if k != "page" and (k, v) != (key, value)]
        if path == "/leads" and not remaining:
            remaining = [("due", "")]
        chips.append(
            SimpleNamespace(
                label=labels.get(key, key.replace("_", " ").capitalize()),
                value="Selecionada" if key == "run" else values.get(value, value),
                remove_url=path + ("?" + urlencode(remaining) if remaining else ""),
            )
        )
    return {
        "list_url": path + ("?" + urlencode(params) if params else ""),
        "active_filter_count": str(len(meaningful)),
        "active_filters": chips,
    }


def _supplier_filter_error(value: str | None) -> str | None:
    if value and not is_valid_cnpj(normalize_cnpj(value) or ""):
        return "CNPJ do fornecedor inválido. Corrija o campo; os demais filtros foram mantidos."
    return None


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


def _brl(value: Decimal | None, decimal_places: int = 2) -> str:
    if value is None:
        return "Não informado"
    rendered = f"{value:,.{decimal_places}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {rendered}"


templates.env.filters["brl"] = _brl
templates.env.filters["local_datetime"] = _local_datetime
templates.env.filters["display_text"] = _display_text
templates.env.filters["modality_label"] = lambda value: _label(value, _PROCUREMENT_MODALITY_LABELS)
templates.env.filters["procurement_type_label"] = lambda value: _label(
    value, _PROCUREMENT_TYPE_LABELS
)
templates.env.filters["contracting_form"] = contracting_form
templates.env.filters["contracting_form_label"] = lambda value: CONTRACTING_FORM_LABELS.get(
    contracting_form(value) or "", "—"
)
templates.env.filters["government_sphere_label"] = lambda value: GOVERNMENT_SPHERES.get(
    published_sphere(value) or "", "Não informada"
)
templates.env.filters["procurement_phase_label"] = lambda value: _label(
    value, _PROCUREMENT_PHASE_LABELS
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
    source_url = _public_link(official_url)
    return SimpleNamespace(
        source_label=_label(source, _TECHNICAL_SOURCE_LABELS),
        official_url=source_url,
        link_label=source_link_label(source_url) if source_url else None,
        source_record_id=str(source_record_id) if source_record_id else None,
        external_id=_display_text(external_id) if external_id else None,
        evidence_url=f"/evidencias/{evidence_id}" if evidence_id else None,
        evidence_id=str(evidence_id) if evidence_id else None,
    )


def _item_matches(item: ProcurementItem, query: str) -> bool:
    needle = query.casefold()
    return any(
        needle in (text or "").casefold()
        for text in (item.item_number, item.description, item.summary_description)
    )


def _item_view(item: ProcurementItem, procurement: Procurement) -> SimpleNamespace:
    summary, description, is_long = _short_description(item.description)
    return SimpleNamespace(
        id=item.id,
        item_number=_display_text(item.item_number),
        description_summary=summary,
        description=description,
        description_is_long=is_long,
        quantity=_quantity_with_unit(item.quantity, item.unit),
        unit_value=_brl(item.estimated_unit_value, 4),
        total_value=_brl(item.estimated_total_value),
        status=_label(item.result_status, _PROCUREMENT_STATUS_LABELS),
        origin=_technical_origin(
            source=procurement.source,
            source_record_id=item.source_record_id,
            external_id=item.external_item_id,
        ),
    )


def _document_view(
    document: Document, *, local_file: bool, source: str | None, has_text: bool, current: bool
) -> SimpleNamespace:
    source_url = _public_link(document.original_url)
    return SimpleNamespace(
        title=_display_text(document.title),
        current=current,
        document_type=_label(document.document_type, _DOCUMENT_TYPE_LABELS),
        published_at=_procurement_datetime(document.published_at),
        extraction_status=_label(document.extraction_status, _EXTRACTION_STATUS_LABELS),
        extraction_state=_enum_key(document.extraction_status) or "pending",
        official_url=source_url,
        stored_url=f"/api/documents/{document.id}/view" if local_file or has_text else None,
        copy_label="Abrir texto extraído" if has_text else "Abrir cópia coletada",
        origin=_technical_origin(
            source=source,
            official_url=source_url,
            source_record_id=document.source_record_id,
        ),
    )


def _available_document_files(paths: list[tuple[UUID, str]], root: Path) -> set[UUID]:
    available: set[UUID] = set()
    root = root.resolve()
    for identifier, path in paths:
        try:
            candidate = Path(path).resolve()
            if candidate.is_relative_to(root) and candidate.is_file():
                available.add(identifier)
        except (OSError, RuntimeError, ValueError):
            continue
    return available


def _participant_view(
    participant: Participant, payload: dict[str, Any] | None = None
) -> SimpleNamespace:
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
        monetary_values=(payload or {}).get("monetary_values", []),
        verification_status=(payload or {}).get("verification_status", "legacy"),
        verification_reason=(payload or {}).get("verification_reason"),
        status=_label(participant.status, _PROCUREMENT_STATUS_LABELS),
        confidence=_confidence(participant.confidence),
        evidence_url=f"/evidencias/{evidence.id}" if evidence else None,
        origin=_technical_origin(
            source=participant.source,
            official_url=evidence.source_url if evidence else None,
            source_record_id=participant.source_record_id,
            evidence_id=evidence.id if evidence else None,
        ),
    )


def _event_act_view(event: ProcurementEvent, company: Company | None) -> SimpleNamespace:
    reference = (
        CompanyReference(
            str(company.id), company.legal_name or company.normalized_name, company.cnpj
        )
        if company
        else None
    )
    act = describe_event_scope(event, event.raw_description, company=reference)
    award = event.event_type.value in {
        "BEST_OFFER_DECLARED",
        "WINNER_DECLARED",
        "ADJUDICATED",
        "HOMOLOGATED",
    }
    parts = []
    if act.item_number:
        parts.append(f"Item {act.item_number}")
    if act.lot_number:
        parts.append(f"Lote {act.lot_number}")
    if act.stage:
        parts.append(_label(act.stage, _PROCUREMENT_PHASE_LABELS))
    elif award and event.event_type.value == "WINNER_DECLARED":
        parts.append("Fase não identificada")
    return SimpleNamespace(
        label=_label(event.event_type, _EVENT_TYPE_LABELS),
        scope=" · ".join(parts),
        reason=act.reason or "Trecho do ato requer revisão.",
        award=award,
    )


def _event_view(event: ProcurementEvent) -> SimpleNamespace:
    evidence = event.evidence
    event_date = event.occurred_at or event.published_at
    act = _event_act_view(event, event.company)
    reason = (
        act.reason
        if act.award
        else (event.normalized_reason or _label(event.reason_category, _REASON_CATEGORY_LABELS))
    )
    company_identified = event.company is not None
    company_name = _display_text(
        (event.company.legal_name or event.company.normalized_name) if event.company else None
    )
    return SimpleNamespace(
        id=event.id,
        procurement_id=event.procurement_id,
        event_type=act.label,
        event_scope=act.scope,
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
        evidence_url=f"/evidencias/{evidence.id}" if evidence else None,
        origin=_technical_origin(
            source=event.source,
            official_url=event.source_url or (evidence.source_url if evidence else None),
            source_record_id=event.source_record_id,
            evidence_id=evidence.id if evidence else None,
        ),
    )


def _contact_view(contact: CompanyContact) -> SimpleNamespace:
    return SimpleNamespace(
        contact_type=_label(contact.contact_type, _CONTACT_TYPE_LABELS),
        contact_type_key=_enum_key(contact.contact_type),
        value=contact.contact_value,
        status=_label(contact.status, _CONTACT_STATUS_LABELS),
        is_corporate=contact.is_corporate,
        source_url=_public_link(contact.source_url),
        verified_at=_local_datetime(contact.verified_at) if contact.verified_at else None,
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
        id=procurement.id,
        title=_display_text(title),
        object_description=_display_text(procurement.object_description),
        modality=_label(
            procurement.modality_key or procurement.modality, _PROCUREMENT_MODALITY_LABELS
        ),
        status=_label(procurement.status, _PROCUREMENT_STATUS_LABELS),
        phase=_label(procurement.current_phase, _PROCUREMENT_PHASE_LABELS),
        agency_name=_display_text(procurement.agency_name),
        process=process,
        year=procurement.purchase_year,
        pncp_control_number=_display_text(procurement.pncp_control_number),
        location=_display_text(location),
        estimated_value=_brl(procurement.estimated_value),
        official_url=procurement_source_page(procurement),
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
    if "database is locked" in joined or "database is busy" in joined:
        return "A gravação no banco local ficou bloqueada por outra transação."
    if "unique constraint failed" in joined:
        return "A gravação encontrou registros repetidos que precisam ser consolidados."
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
    if run.connector == COMPLETE_COLLECTION_MODE:
        return []
    document_mode = isinstance(run.filters, dict) and run.filters.get("mode") == "documents"
    requested = (
        ["documents"]
        if document_mode
        else (["pncp", "compras_gov"] if run.connector == "all" else [run.connector])
    )
    all_diagnostics = _diagnostic_messages(run)
    stored_sources = (run.cursor or {}).get("sources", {})
    if not isinstance(stored_sources, dict):
        stored_sources = {}

    failed_legacy: set[str] = set()
    for source_id in requested:
        if any(
            message.casefold().startswith((f"{source_id}:", f"{source_id}/"))
            for message in all_diagnostics
        ):
            failed_legacy.add(source_id)

    views: list[SimpleNamespace] = []
    for source_id in requested:
        is_municipal = source_id.startswith("municipal:")
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
                if message.casefold().startswith((f"{source_id}:", f"{source_id}/"))
            ]
        active = run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
        found_value: int | None = None
        if is_municipal:
            # Municipal runs persist one source per run and already use the
            # terminal crawl status; do not fall back to the procurement
            # "completed" default when the portal failed.
            status = run.status.value
            diagnostics = all_diagnostics
            found_value = run.records_found
        else:
            status = str(stored.get("status") or "")
            if stored.get("availability") == "not_supported":
                status = "not_supported"
            elif active:
                if not status:
                    progress = (run.cursor or {}).get("progress", {})
                    current_source = progress.get("source") if isinstance(progress, dict) else None
                    status = (
                        "running"
                        if run.status is CrawlRunStatus.RUNNING
                        and (current_source == source_id or len(requested) == 1)
                        else "pending"
                    )
            elif status in {"running", "pending"}:
                # A terminal run can never leave a source "in progress"; this covers
                # runs recovered from an interrupted process.
                status = "cancelled" if run.status is CrawlRunStatus.CANCELLED else "failed"
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
            not active
            and status in {"temporary_error", "failed", "partial"}
            and bool(diagnostics)
            and not source_id.startswith("municipal:")
            and source_id != "documents"
        )
        unsupported = stored.get("unsupported_modalities")
        coverage_notes = []
        if isinstance(unsupported, list) and unsupported:
            labels = ", ".join(_label(item, _PROCUREMENT_MODALITY_LABELS) for item in unsupported)
            coverage_notes.append(f"Modalidades sem cobertura nesta fonte: {labels}.")
        unknown_variants = nonnegative_int(stored.get("unclassified_modality_rows"))
        if unknown_variants:
            coverage_notes.append(
                f"{unknown_variants} registro(s) sem variante publicada ficaram fora do filtro."
            )
        views.append(
            SimpleNamespace(
                id=source_id,
                label="Documentos do lote"
                if source_id == "documents"
                else _SOURCE_LABELS.get(source_id, source_id),
                status=status,
                status_label=_STATUS_LABELS.get(status, status.replace("_", " ").title()),
                coverage_note=" ".join(coverage_notes),
                records_found=found_value,
                records_failed=records_failed,
                discovery_total=stored.get("discovery_total"),
                headers_received=stored.get("headers_received"),
                stop_reason=stored.get("stop_reason"),
                duration=_duration_label(duration),
                max_attempts=max_attempts,
                friendly_error=None
                if status in {"pending", "running"}
                else _friendly_source_error(source_id, diagnostics),
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
        processed = nonnegative_int(progress.get("processed"))
        total = nonnegative_int(progress.get("total"))
        if total and processed < total:
            return f"{processed}/{total} registro(s)"
    return None


def _crawl_view(run: CrawlRun) -> SimpleNamespace:
    duration: float | None = None
    if run.started_at and run.finished_at:
        duration = max((run.finished_at - run.started_at).total_seconds(), 0)
    elif run.started_at:
        started = (
            run.started_at.replace(tzinfo=UTC) if run.started_at.tzinfo is None else run.started_at
        )
        duration = max((datetime.now(UTC) - started).total_seconds(), 0)
    status = run.status.value
    status_label = _STATUS_LABELS.get(status, status.title())
    if run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING} and run.cancel_requested:
        status_label = "Cancelando"
    general_diagnostics = [
        message
        for message in _diagnostic_messages(run)
        if not any(
            message.casefold().startswith((f"{source_id}:", f"{source_id}/"))
            for source_id in _SOURCE_LABELS
        )
    ]
    general_error: str | None = None
    if general_diagnostics:
        joined = " ".join(general_diagnostics).casefold()
        if "database is locked" in joined or "database is busy" in joined:
            general_error = "A gravação no banco local ficou bloqueada por outra transação."
        elif "another equivalent crawl" in joined:
            general_error = "Outra coleta equivalente já estava em andamento."
        elif "lease" in joined:
            general_error = "A coleta perdeu a autorização de execução; verifique o diagnóstico."
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
            processed_count = nonnegative_int(documents_result.get("documents_processed"))
            total_count = documents_result.get("documents_total")
            total_label = f"/{int(total_count)}" if isinstance(total_count, int) else ""
            documents_progress = (
                f"{processed_count}{total_label} documento(s) neste lote · "
                f"{nonnegative_int(documents_result.get('events_created'))} evento(s) · "
                f"{nonnegative_int(documents_result.get('leads_created'))} lead(s)"
            )
        progress_label = _crawl_progress_label(stored_sources)
    period = (run.cursor or {}).get("collection") or run.filters or {}
    if not isinstance(period, dict):
        period = {}

    def calendar(key: str) -> str:
        try:
            return date.fromisoformat(str(period.get(key))).strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            return "Não registrado"

    mode = (run.filters or {}).get("mode", "procurements")
    complete_stages = (run.cursor or {}).get("complete_stages", [])
    procurement_runs: list[str] = next(
        (step.get("run_ids", []) for step in complete_stages if step.get("key") == "procurements"),
        [],
    )
    linked_run = procurement_runs[0] if procurement_runs else None
    return SimpleNamespace(
        id=run.id,
        connector_label=_CONNECTOR_LABELS.get(run.connector, run.connector),
        mode_label=(
            "Todas as etapas"
            if mode == COMPLETE_COLLECTION_MODE
            else "Reprocessamento de documentos"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "documents"
            else "Atas (ARP)"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "price_registries"
            else "Contratos"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "contracts"
            else "Pagamentos municipais"
            if run.connector.startswith("municipal:")
            else "Pagamentos federais"
            if mode == FEDERAL_BATCH_MODE
            else "Pagamentos"
            if mode in {"payments", "payment_import"}
            else "Licitações"
        ),
        started_at=_local_datetime(run.started_at),
        finished_at=_local_datetime(run.finished_at),
        status=status,
        status_label=status_label,
        active=run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING},
        records_found=run.records_found,
        records_created=run.records_created,
        records_changed=run.records_changed,
        records_unchanged=run.records_unchanged,
        records_stale_ignored=run.records_stale_ignored,
        records_conflicted=run.records_conflicted,
        counting_version=run.counting_version,
        can_continue=False,
        duration=_duration_label(duration),
        progress=progress_label,
        documents_progress=documents_progress,
        query_start=calendar("start_date"),
        query_end=calendar("end_date"),
        result_url="/atas"
        if mode == "price_registries"
        else "/contratos"
        if mode == "contracts"
        else "/pagamentos"
        if run.connector.startswith("municipal:")
        or mode in {"payments", "payment_import", FEDERAL_BATCH_MODE}
        else f"/leads?run={linked_run}"
        if linked_run
        else "/leads"
        if mode == COMPLETE_COLLECTION_MODE
        else f"/leads?run={run.id}",
        result_label="Ver acervo de atas"
        if mode == "price_registries"
        else "Ver acervo de contratos"
        if mode == "contracts"
        else "Ver pagamentos"
        if run.connector.startswith("municipal:")
        or mode in {"payments", "payment_import", FEDERAL_BATCH_MODE}
        else "Ver oportunidades"
        if mode == COMPLETE_COLLECTION_MODE and not linked_run
        else "Ver oportunidades vinculadas",
        records_url=f"/procurements?run={run.id}"
        if mode in {"procurements", "documents"} and not run.connector.startswith("municipal:")
        else None,
        complete_stages=complete_stages,
        complete=mode == COMPLETE_COLLECTION_MODE,
        federal=mode == FEDERAL_BATCH_MODE,
        federal_batch=None,
        has_linked_opportunities=bool(linked_run),
        has_document_batch=isinstance(stored_sources, dict)
        and isinstance(stored_sources.get("documents"), dict),
        retry_documents=run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
        and isinstance(stored_sources, dict)
        and isinstance(stored_sources.get("documents"), dict)
        and stored_sources["documents"].get("status") in {"partial", "failed"},
        sources=_source_views(run),
        general_error=general_error,
        general_technical_detail="\n".join(general_diagnostics),
        retry_all=(
            run.status
            in {
                CrawlRunStatus.FAILED,
                CrawlRunStatus.PARTIAL,
                CrawlRunStatus.CANCELLED,
            }
            and bool(run.filters)
            and not run.connector.startswith("municipal:")
            and mode
            not in {
                "documents",
                COMPLETE_COLLECTION_MODE,
                "payments",
                "payment_import",
                FEDERAL_BATCH_MODE,
            }
        ),
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
    from app.services.collection_coverage import document_counters, latest_procurement_run

    now = datetime.now(UTC)
    due_at = func.coalesce(Deadline.explicit_deadline_at, Deadline.estimated_deadline_at)
    usable_deadline = deadline_usable(now)
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
        "Eventos detectados": int(
            (await db.scalar(select(func.count(ProcurementEvent.id)).where(event_verified()))) or 0
        ),
        "Empresas identificadas": int((await db.scalar(select(func.count(Company.id)))) or 0),
        "Leads": int((await db.scalar(select(func.count(Lead.id)).where(lead_current()))) or 0),
        "Prazos em 24 horas": int(
            (
                await db.scalar(
                    select(func.count(Deadline.id)).where(
                        due_at >= now, due_at <= now + timedelta(hours=24), usable_deadline
                    )
                )
            )
            or 0
        ),
        "Prazos vencidos": int(
            (await db.scalar(select(func.count(Deadline.id)).where(due_at < now, usable_deadline)))
            or 0
        ),
        "Pendências de revisão": int(
            (
                await db.scalar(
                    select(func.count(Lead.id)).where(
                        Lead.lead_status == LeadStatus.PENDING_REVIEW, lead_current()
                    )
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
            loaded_opportunities(opportunity_query(OpportunityFilters(status="actionable"))).limit(
                10
            )
        )
    ).all()
    leads = await _lead_views(db, recent, return_to="/leads?lead_status=actionable")
    agenda = []
    for title, params, hint in [
        (
            "Prazo aberto para diligência ou recurso",
            {"due": "open"},
            "Oportunidades com prazo vigente na fonte oficial",
        ),
        ("Aguardando revisão", {"lead_status": "pending_review"}, "Decisões humanas pendentes"),
        (
            "Prazos próximos",
            {"due": "72h", "lead_status": "actionable"},
            "Oportunidades para revisar em até 72 horas",
        ),
        ("Novas oportunidades", {"lead_status": "new"}, "Ainda não analisadas"),
        (
            "Ajustes solicitados",
            {"lead_status": "needs_changes"},
            "Análises que precisam de complemento",
        ),
    ]:
        selected = OpportunityFilters.from_params(params, timezone=get_settings().timezone)
        agenda.append(
            SimpleNamespace(
                title=title,
                hint=hint,
                total=await opportunity_count(db, selected, now=now),
                href="/leads?" + urlencode(params),
            )
        )
    last_collection = await db.scalar(
        select(func.max(SourceRecord.collected_at)).where(
            SourceRecord.http_status.between(200, 299),
            SourceRecord.availability.in_(["available", "empty"]),
        )
    )
    pending_documents = int(
        await db.scalar(
            select(func.count(Document.id)).where(
                Document.is_active.is_(True),
                Document.extraction_status.in_(["pending", "ocr_required"]),
            )
        )
        or 0
    )
    last_run = await latest_procurement_run(db)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "metrics": counts,
            "leads": leads,
            "agenda": agenda,
            "scheduler": await scheduler_status(db, get_settings()),
            "last_collection": _local_datetime(last_collection),
            "pending_documents": pending_documents,
            "last_procurement_run": _crawl_view(last_run) if last_run else None,
            "document_counts": document_counters(last_run) if last_run else {},
            "selected_uf": get_settings().default_uf,
            "collection_days": get_settings().default_lookback_days,
        },
    )


@router.get("/grupos/{group_id}")
async def group_hub_page(
    request: Request,
    group_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Render a domain hub with real counters, alerts and primary actions."""

    group = get_group(group_id)
    if group is None or not group.hub:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")
    hub = await build_group_hub(db, group_id)
    if hub is None:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")
    return templates.TemplateResponse(request, "group_hub.html", {"hub": hub})


@router.get("/procurements")
async def procurements_page(
    request: Request,
    q: str = Query("", max_length=200),
    published_from: OptionalDateQuery = None,
    published_to: OptionalDateQuery = None,
    run: UUID | None = None,
    uf: OptionalUfQuery = None,
    municipality: str | None = None,
    agency: str | None = None,
    agency_cnpj: str | None = None,
    company_cnpj: str | None = None,
    company_participation_only: bool = False,
    modality: list[str] | None = Query(None),
    contracting_form: str | None = None,
    licitation_modality: str | None = None,
    auxiliary_procedure: str | None = None,
    object_query: str | None = Query(None, max_length=200),
    object_preset: list[str] | None = Query(None),
    object_group: str | None = None,
    government_sphere: str | None = None,
    procurement_type: str | None = None,
    status_category: str | None = None,
    phase: str | None = None,
    is_srp: str | None = None,
    value_min: OptionalDecimalQuery = None,
    value_max: OptionalDecimalQuery = None,
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    selected_modalities = [
        value for value in (modality or []) if canonical_modality(value) is not None
    ]
    selected_type = procurement_type if procurement_type in _PROCUREMENT_TYPE_LABELS else None
    selected_category = (
        status_category if status_category in _PROCUREMENT_STATUS_CATEGORY_LABELS else None
    )
    selected_phase = phase if phase in _PROCUREMENT_PHASE_LABELS else None
    parsed_srp: bool | None = None
    if is_srp in {"true", "1", "sim"}:
        parsed_srp = True
    elif is_srp in {"false", "0", "nao", "não"}:
        parsed_srp = False
    selected_agency_cnpj = normalize_cnpj(agency_cnpj) if agency_cnpj else None
    if selected_agency_cnpj is not None and not is_valid_cnpj(selected_agency_cnpj):
        selected_agency_cnpj = None
    selected_company_cnpj = normalize_cnpj(company_cnpj) if company_cnpj else None
    if selected_company_cnpj is not None and not is_valid_cnpj(selected_company_cnpj):
        selected_company_cnpj = None
    invalid_cnpj = bool(
        (agency_cnpj and selected_agency_cnpj is None)
        or (company_cnpj and selected_company_cnpj is None)
    )
    selected_uf = _selected_uf(uf)
    if value_min is not None and value_min < 0:
        value_min = None
    if value_max is not None and value_max < 0:
        value_max = None
    errors = []
    for value, options, label in (
        (contracting_form, CONTRACTING_FORM_LABELS, "forma de contratação"),
        (licitation_modality, _LICITATION_MODALITY_LABELS, "modalidade de licitação"),
        (auxiliary_procedure, AUXILIARY_PROCEDURES, "procedimento auxiliar"),
    ):
        if value and value not in options:
            errors.append(f"Selecione uma opção válida para {label}.")
    object_scope = None
    try:
        object_scope = ObjectScope.build(
            object_query, object_preset or (), object_group, government_sphere
        )
    except ValueError as exc:
        errors.append(str(exc))
    if invalid_cnpj:
        errors.append(
            "CNPJ inválido informado. Corrija o campo para aplicar a busca; nenhum filtro foi removido."
        )
    if published_from and published_to and published_from > published_to:
        errors.append("A data inicial de publicação não pode ser posterior à final.")
    if value_min is not None and value_max is not None and value_min > value_max:
        errors.append("O valor mínimo não pode ser maior que o máximo.")
    if run and await db.get(CrawlRun, run) is None:
        raise HTTPException(404, "Coleta não encontrada")
    page = (
        Page(items=[], page=page_number, page_size=50, total=0)
        if errors
        else await ProcurementRepository(db).list_filtered(
            page=max(page_number, 1),
            page_size=50,
            query=q,
            crawl_run_id=run,
            published_from=_day_bounds(published_from)[0] if published_from else None,
            published_to=_day_bounds(published_to)[1] if published_to else None,
            uf=selected_uf,
            municipality=municipality,
            agency=agency,
            agency_cnpj=selected_agency_cnpj,
            company_cnpj=selected_company_cnpj,
            company_participation_only=company_participation_only,
            modalities=selected_modalities,
            contracting_form=contracting_form,
            licitation_modality=licitation_modality,
            auxiliary_procedure=auxiliary_procedure,
            object_scope=object_scope,
            procurement_type=selected_type,
            status_category=selected_category,
            phase=selected_phase,
            is_srp=parsed_srp,
            value_min=value_min,
            value_max=value_max,
        )
    )
    return templates.TemplateResponse(
        request,
        "procurements.html",
        {
            "procurements": page.items,
            "pagination": page,
            **_list_context(request, "/procurements"),
            "filter_errors": errors,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(
                q=q,
                published_from=published_from.isoformat() if published_from else "",
                published_to=published_to.isoformat() if published_to else "",
                run=str(run) if run else "",
                uf=selected_uf,
                municipality=municipality,
                agency=agency,
                agency_cnpj=agency_cnpj,
                company_cnpj=company_cnpj,
                company_cnpj_valid=selected_company_cnpj,
                company_participation_only=company_participation_only,
                invalid_cnpj=invalid_cnpj,
                modalities=selected_modalities,
                contracting_form=contracting_form,
                licitation_modality=licitation_modality,
                auxiliary_procedure=auxiliary_procedure,
                object_query=object_query or "",
                object_preset=object_preset or (),
                object_group=object_group,
                government_sphere=object_scope.government_sphere
                if object_scope
                else government_sphere,
                procurement_type=selected_type,
                status_category=selected_category,
                phase=selected_phase,
                is_srp=is_srp if parsed_srp is not None else None,
                value_min=value_min,
                value_max=value_max,
            ),
            "contracting_forms": CONTRACTING_FORM_LABELS,
            "licitation_modalities": _LICITATION_MODALITY_LABELS,
            "auxiliary_procedures": AUXILIARY_PROCEDURES,
            "object_presets": OBJECT_PRESETS,
            "object_groups": OBJECT_GROUPS,
            "government_spheres": GOVERNMENT_SPHERES,
            "status_category_options": [
                (key, label)
                for key, label in _PROCUREMENT_STATUS_CATEGORY_LABELS.items()
                if key != "desconhecida" or selected_category == key
            ],
            "phase_options": list(_PROCUREMENT_PHASE_LABELS.items()),
        },
        status_code=422 if errors else 200,
    )


def _last_changes_review(request: Request) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(request.cookies.get("licita_changes_review", ""))
    except ValueError:
        return None
    return stamp if stamp.tzinfo and stamp <= datetime.now(UTC) else None


@router.get("/alteracoes")
async def archive_changes_page(
    request: Request,
    todas: bool = False,
    ate: datetime | None = None,
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    now = datetime.now(UTC)
    if ate is not None and (ate.tzinfo is None or ate > now):
        raise HTTPException(422, "Data de corte inválida.")
    cutoff = ate or now
    after = None if todas else _last_changes_review(request)
    history = await relevant_changes(db, after, cutoff, page_number)
    return templates.TemplateResponse(
        request,
        "archive_changes.html",
        {
            "history": history,
            "after": _local_datetime(after) if after else None,
            "cutoff": cutoff.isoformat(),
            "cutoff_label": _local_datetime(cutoff),
            "all_changes": todas,
        },
    )


@router.post("/alteracoes/revisar")
async def mark_changes_reviewed(request: Request, ate: datetime = Form(...)):
    if ate.tzinfo is None or ate > datetime.now(UTC):
        raise HTTPException(422, "Data de corte inválida.")
    previous = _last_changes_review(request)
    cutoff = max(ate, previous) if previous else ate
    response = RedirectResponse("/alteracoes", status_code=303)
    response.set_cookie(
        "licita_changes_review",
        cutoff.isoformat(),
        httponly=True,
        samesite="strict",
        path="/alteracoes",
        max_age=365 * 86400,
    )
    return response


@router.get("/itens")
async def procurement_items_page(
    request: Request,
    q: str = Query("", max_length=200),
    agency: str = Query("", max_length=160),
    unit: str = Query("", max_length=100),
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    statement = select(ProcurementItem, Procurement).join(ProcurementItem.procurement)
    if q.strip():
        statement = statement.where(
            or_(
                ProcurementItem.description.icontains(q.strip(), autoescape=True),
                ProcurementItem.summary_description.icontains(q.strip(), autoescape=True),
            )
        )
    if agency.strip():
        statement = statement.where(
            Procurement.agency_name.icontains(agency.strip(), autoescape=True)
        )
    if unit.strip():
        statement = statement.where(func.lower(ProcurementItem.unit) == func.lower(unit.strip()))
    total = await db.scalar(select(func.count()).select_from(statement.subquery()))
    rows = await db.execute(
        statement.order_by(
            Procurement.publication_at.desc().nullslast(),
            Procurement.id,
            ProcurementItem.item_number,
            ProcurementItem.id,
        )
        .offset((page_number - 1) * 50)
        .limit(50)
    )
    units = await db.scalars(
        select(ProcurementItem.unit)
        .distinct()
        .where(ProcurementItem.unit.is_not(None), ProcurementItem.unit != "")
        .order_by(ProcurementItem.unit)
    )
    return templates.TemplateResponse(
        request,
        "item_search.html",
        {
            **_list_context(request, "/itens"),
            "rows": rows.all(),
            "units": units.all(),
            "q": q,
            "agency": agency,
            "unit": unit,
            "page": page_number,
            "total": total or 0,
        },
    )


@router.get("/procurements/{procurement_id}")
async def procurement_page(
    request: Request,
    procurement_id: UUID,
    item_query: str | None = Query(None, max_length=200),
    include_history: bool = False,
    db: AsyncSession = Depends(get_db),
):
    from app.services.collection_coverage import procurement_catalogs

    procurement = await db.scalar(
        select(Procurement)
        .where(Procurement.id == procurement_id)
        .options(
            selectinload(Procurement.sources),
            selectinload(Procurement.items),
            selectinload(Procurement.documents).defer(Document.extracted_text),
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

    from app.services.documents.identity import noncanonical_document_ids
    from app.services.participation_integrity import (
        event_verified,
        participant_current,
        participant_payloads,
    )

    payloads = await participant_payloads(
        db, list(procurement.participants), historical=include_history
    )
    payload_by_id = {str(p["id"]): p for p in payloads}
    current_ids = set(
        (
            await db.scalars(
                select(Participant.id).where(
                    Participant.procurement_id == procurement_id, participant_current()
                )
            )
        ).all()
    )
    canonical_ids = set(
        (
            await db.scalars(
                select(Document.id).where(
                    Document.procurement_id == procurement_id,
                    Document.is_active.is_(True),
                    Document.id.not_in(noncanonical_document_ids()),
                )
            )
        ).all()
    )
    event_ids = set(
        (
            await db.scalars(
                select(ProcurementEvent.id).where(
                    ProcurementEvent.procurement_id == procurement_id, event_verified()
                )
            )
        ).all()
    )
    views = {
        p.id: _participant_view(p, payload_by_id.get(str(p.id))) for p in procurement.participants
    }
    review_rows = (
        await db.scalars(
            select(FieldChange)
            .where(
                FieldChange.parent_type == "procurement",
                FieldChange.parent_id == procurement_id,
                FieldChange.field_name == "monetary_review",
            )
            .order_by(FieldChange.recorded_at.desc(), FieldChange.id.desc())
        )
    ).all()
    monetary_reviews = []
    reviewed_documents = set()
    for review in review_rows:
        if review.entity_id in reviewed_documents:
            continue
        reviewed_documents.add(review.entity_id)
        if (
            (include_history or review.entity_id in canonical_ids)
            and isinstance(review.received_value, dict)
            and review.received_value.get("diagnostics")
        ):
            monetary_reviews.append(
                {
                    "document_id": str(review.entity_id),
                    "diagnostics": review.received_value["diagnostics"],
                }
            )

    query = (item_query or "").strip()
    items = sorted(procurement.items, key=lambda item: item.item_number.casefold())
    if query:
        items = [item for item in items if _item_matches(item, query)]
    seen_companies: set[UUID] = set()
    candidate_companies: list[SimpleNamespace] = []
    for participant in procurement.participants:
        if participant.id not in current_ids:
            continue
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
    shown_documents = [
        document
        for document in procurement.documents
        if include_history or document.id in canonical_ids
    ]
    file_paths = [(doc.id, doc.local_path) for doc in shown_documents if doc.local_path]
    local_files = (
        await asyncio.to_thread(
            _available_document_files, file_paths, get_settings().document_storage_path
        )
        if file_paths
        else set()
    )
    document_meta = {
        identifier: (source, bool(has_text))
        for identifier, source, has_text in await db.execute(
            select(
                Document.id,
                SourceRecord.source,
                Document.local_path.is_(None) & (Document.extracted_text != ""),
            )
            .outerjoin(SourceRecord, SourceRecord.id == Document.source_record_id)
            .where(Document.procurement_id == procurement.id)
        )
    }
    return templates.TemplateResponse(
        request,
        "procurement_detail.html",
        {
            "procurement": _procurement_view(procurement),
            "origin_platform_url": await _origin_platform_url(db, procurement),
            "return_to": safe_list_url(
                request.query_params.get("return_to"),
                path="/itens"
                if (request.query_params.get("return_to") or "").partition("?")[0] == "/itens"
                else "/procurements",
            ),
            "items": [_item_view(item, procurement) for item in items],
            "item_query": query,
            "item_count": len(items),
            "documents": [
                _document_view(
                    doc,
                    local_file=doc.id in local_files,
                    source=document_meta[doc.id][0],
                    has_text=document_meta[doc.id][1],
                    current=doc.id in canonical_ids,
                )
                for doc in shown_documents
                if doc.id in document_meta
            ],
            "catalogs": await procurement_catalogs(db, procurement, get_settings()),
            "source_labels": _SOURCE_LABELS,
            "format_collection_time": _local_datetime,
            "participants": [views[p.id] for p in procurement.participants if p.id in current_ids],
            "participant_candidates": [
                views[p.id]
                for p in procurement.participants
                if p.id not in current_ids and views[p.id].verification_status == "review"
            ],
            "historical_participants": [
                views[p.id]
                for p in procurement.participants
                if p.id not in current_ids and views[p.id].verification_status != "review"
            ]
            if include_history
            else [],
            "include_history": include_history,
            "monetary_reviews": monetary_reviews,
            "events": [
                _event_view(event)
                for event in procurement.events
                if include_history or event.id in event_ids
            ],
            "candidate_companies": candidate_companies,
            "tce_observations": await tce_observations_for(
                db, procurement.id, historical=include_history
            ),
            "tce_link": await db.get(TceMaVerifiedLink, procurement.id),
            "tce_link_active": await db.scalar(
                active_tce_links()
                .with_only_columns(TceMaVerifiedLink.confirmation_id)
                .where(TceMaVerifiedLink.procurement_id == procurement.id)
            ),
            "tce_decisions": await tce_decisions_for(db, procurement.id),
            "tce_documents": [
                doc
                for doc in shown_documents
                if doc.id in canonical_ids
                and doc.id in document_meta
                and document_meta[doc.id][0] in {"pncp", "compras_gov"}
            ],
            "link_feedback": request.query_params.get("link"),
        },
    )


@router.post("/procurements/{procurement_id}/tce-link")
async def review_tce_link(
    procurement_id: UUID,
    reviewer: str = Form(..., min_length=2, max_length=150),
    note: str = Form(..., min_length=5, max_length=1000),
    candidate_id: UUID | None = Form(None),
    document_id: UUID | None = Form(None),
    quote: str = Form("", max_length=4000),
    pncp_document_id: UUID | None = Form(None),
    pncp_quote: str = Form("", max_length=4000),
    tce_document_id: UUID | None = Form(None),
    tce_quote: str = Form("", max_length=4000),
    revoke_link_id: UUID | None = Form(None),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    reviewer, note = reviewer.strip(), note.strip()
    if len(reviewer) < 2 or len(note) < 5:
        raise HTTPException(422, "Informe o responsável e o motivo da decisão.")
    if db.get_bind().dialect.name == "sqlite":
        await db.execute(text("BEGIN IMMEDIATE"))
    else:
        await admit_before_row_lock(db)
    procurement = await db.get(Procurement, procurement_id, with_for_update=True)
    if procurement is None:
        raise HTTPException(404, "Contratação não encontrada")
    try:
        if revoke_link_id:
            link = await db.get(TceMaVerifiedLink, procurement_id, with_for_update=True)
            if not link or link.confirmation_id != revoke_link_id:
                raise ValueError("O vínculo foi alterado. Atualize a página antes de continuar.")
            confirmation = await db.get(FieldObservation, revoke_link_id)
            add_tce_decision(
                db,
                link,
                "revoked",
                reviewer=reviewer,
                reason=note,
                source_record_id=confirmation.source_record_id if confirmation else None,
            )
            await db.delete(link)
        elif candidate_id and document_id:
            await confirm_tce_link(
                db,
                procurement,
                candidate_id,
                Excerpt(document_id, quote),
                pncp_agency=Excerpt(pncp_document_id, pncp_quote) if pncp_document_id else None,
                tce_agency=Excerpt(tce_document_id, tce_quote) if tce_document_id else None,
                reviewer=reviewer,
                reason=note,
            )
        else:
            raise ValueError("Selecione o candidato do TCE-MA e o documento oficial.")
        await db.commit()
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(422, str(exc)) from None
    except IntegrityError:
        await db.rollback()
        raise HTTPException(409, "O procedimento já está vinculado. Atualize a página.") from None
    return RedirectResponse(f"/procurements/{procurement_id}#tce-title", status_code=303)


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
    uf: OptionalUfQuery = None,
    published_from: OptionalDateQuery = None,
    published_to: OptionalDateQuery = None,
    has_lead: bool = False,
    active_only: bool = False,
    origin: str = Query("", max_length=30),
    activity_location: str = Query("", max_length=160),
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List companies with auditable participation counters."""

    start = _day_bounds(published_from)[0] if published_from else None
    end = _day_bounds(published_to)[1] if published_to else None
    selected_uf = _selected_uf(uf)
    result = await CompanyRepository(db).list_with_stats(
        page=max(page_number, 1),
        page_size=50,
        search=search,
        uf=selected_uf,
        published_from=start,
        published_to=end,
        has_lead=has_lead,
        active_only=active_only,
        origin=origin,
        activity_location=activity_location.strip(),
    )
    filter_error = (
        "A data inicial de publicação não pode ser posterior à final."
        if published_from and published_to and published_from > published_to
        else None
    )
    if filter_error:
        result = Page(items=[], page=page_number, page_size=50, total=0)
    elif origin and origin not in ORIGIN_LABELS:
        filter_error = "Origem da presença inválida. Escolha uma opção da lista."
    companies = [
        SimpleNamespace(
            cnpj=company.cnpj,
            cnpj_label=_format_cnpj(company.cnpj),
            activity=stats["activity"],
            name=_display_text(company.legal_name or company.normalized_name),
            location="/".join(value for value in (company.municipality, company.uf) if value)
            or "—",
            participations=stats["participations"],
            awarded=stats["awarded"],
            disqualified=stats["disqualified"],
            ineligible=stats["ineligible"],
            documented_losses=stats["documented_losses"],
            failure_rate=(
                f"{cast(float, stats['failure_rate']) * 100:.1f}%"
                if stats["failure_rate"] is not None
                else "Sem base"
            ),
            last_participation_at=_stats_datetime(stats),
        )
        for company, stats in result.items
    ]
    return templates.TemplateResponse(
        request,
        "companies.html",
        {
            "companies": companies,
            "origins": ORIGIN_LABELS,
            **_list_context(request, "/empresas"),
            "filter_error": filter_error,
            "pagination": result,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(
                uf=selected_uf,
                search=search,
                origin=origin,
                activity_location=activity_location,
                published_from=published_from.isoformat() if published_from else "",
                published_to=published_to.isoformat() if published_to else "",
                has_lead=has_lead,
                active_only=active_only,
            ),
        },
        status_code=422 if filter_error else 200,
    )


@router.get("/empresas/{cnpj}")
async def company_page(
    request: Request,
    cnpj: str,
    status: str | None = Query(None),
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Show the bounded, consolidated dossier without changing domain data."""
    from app.company_web import company_page as consolidated_company_page

    return await consolidated_company_page(request, cnpj, status, active_only, db)


@router.get("/atas")
async def price_registries_page(
    request: Request,
    search: str | None = Query(None, max_length=120),
    agency: str | None = None,
    registry_number: str | None = None,
    supplier_cnpj: str | None = None,
    valid_on: OptionalDateQuery = None,
    expiring_within_days: OptionalScoreQuery = None,
    allows_adhesion: str | None = None,
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List atas de registro de precos with vigencia and supplier filters."""

    normalized_supplier = normalize_cnpj(supplier_cnpj) if supplier_cnpj else None
    parsed_adhesion: bool | None = None
    if allows_adhesion in {"true", "1", "sim"}:
        parsed_adhesion = True
    elif allows_adhesion in {"false", "0", "nao"}:
        parsed_adhesion = False
    result = await PriceRegistryRepository(db).list_filtered(
        page=max(page_number, 1),
        page_size=50,
        search=search,
        agency=agency,
        registry_number=registry_number,
        supplier_cnpj=normalized_supplier,
        valid_on=valid_on,
        expiring_within_days=expiring_within_days,
        allows_adhesion=parsed_adhesion,
    )
    filter_error = _supplier_filter_error(supplier_cnpj)
    if filter_error:
        result = Page(items=[], page=page_number, page_size=50, total=0)
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
            allows_adhesion=(
                "Sim"
                if item.allows_adhesion
                else ("Não" if item.allows_adhesion is not None else "Não informado")
            ),
            source=_label(item.source, _SOURCE_LABELS),
        )
        for item in result.items
    ]
    return templates.TemplateResponse(
        request,
        "atas.html",
        {
            "registries": registries,
            **_list_context(request, "/atas"),
            "filter_error": filter_error,
            "pagination": result,
            "filters": SimpleNamespace(
                search=search,
                agency=agency,
                registry_number=registry_number,
                supplier_cnpj=supplier_cnpj,
                valid_on=valid_on.isoformat() if valid_on else "",
                expiring_within_days=expiring_within_days,
                allows_adhesion=allows_adhesion,
            ),
        },
        status_code=422 if filter_error else 200,
    )


@router.post("/atas/collect")
async def collect_price_registries_from_web(
    uf: str = Form(..., min_length=2, max_length=2),
    registry_scope: str = Form(..., min_length=1, max_length=80),
    connector: str = Form("all"),
    days: int = Form(7, ge=1, le=3650),
    start_date: OptionalDateForm = Form(None),
    end_date: OptionalDateForm = Form(None),
    force_refresh: bool = Form(False),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Collect explicit known units, without turning a UF choice into a national query."""

    settings = get_settings()
    start_date, end_date = _validate_collection_dates(start_date, end_date)
    uf = uf.strip().upper()
    if uf not in dict(_BRAZILIAN_UFS):
        raise HTTPException(status_code=422, detail="UF inválida")
    scopes = await known_price_registry_scopes(db, uf)
    scope = next((item for item in scopes if item.key == registry_scope), None)
    selected = scopes if registry_scope == "all" else [scope] if scope is not None else []
    if not selected:
        raise HTTPException(
            status_code=422, detail="Selecione um órgão/unidade conhecido da UF informada"
        )
    if connector == "all":
        sources = {source for item in selected for source in item.sources}
        connector = "all" if len(sources) == 2 else next(iter(sources))
    elif not any(connector in item.sources for item in selected):
        raise HTTPException(
            status_code=422, detail="Fonte não identificada para este órgão/unidade"
        )
    request = PipelineRequest(
        connector=connector,
        mode="price_registries",
        force_refresh=force_refresh,
        uf=uf,
        days=None if start_date else days,
        start_date=start_date,
        end_date=end_date,
        agency=scope.agency_name if scope else None,
        agency_cnpj=scope.agency_cnpj if scope else None,
        unit_code=scope.unit_code if scope else None,
        price_registry_scopes=tuple(selected) if registry_scope == "all" else (),
        modalities=(),
        process_documents=False,
    )
    run = await IngestionPipeline(settings).create_run(request)
    launch_crawl(run.id, request)
    return RedirectResponse("/crawls", status_code=303)


@router.post("/contratos/collect")
async def collect_contracts_from_web(
    connector: str = Form("all"),
    agency_cnpj: str = Form(..., min_length=14, max_length=30),
    days: int = Form(30, ge=1, le=3650),
    start_date: OptionalDateForm = Form(None),
    end_date: OptionalDateForm = Form(None),
    force_refresh: bool = Form(False),
) -> RedirectResponse:
    start_date, end_date = _validate_collection_dates(start_date, end_date)
    cnpj = normalize_cnpj(agency_cnpj)
    if connector not in {"all", "pncp", "compras_gov"} or not cnpj or not is_valid_cnpj(cnpj):
        raise HTTPException(status_code=422, detail="Fonte ou CNPJ do órgão inválido")
    request = PipelineRequest(
        connector=connector,
        mode="contracts",
        agency_cnpj=cnpj,
        days=None if start_date else days,
        start_date=start_date,
        end_date=end_date,
        uf=get_settings().default_uf,
        modalities=(),
        process_documents=False,
        force_refresh=force_refresh,
    )
    run = await IngestionPipeline().create_run(request)
    launch_crawl(run.id, request)
    return RedirectResponse("/crawls", status_code=303)


@router.get("/atas/collection-scopes")
async def price_registry_collection_scopes(
    uf: str = Query(..., min_length=2, max_length=2), db: AsyncSession = Depends(get_db)
) -> dict[str, Any]:
    uf = uf.strip().upper()
    if uf not in dict(_BRAZILIAN_UFS):
        raise HTTPException(status_code=422, detail="UF inválida")
    scopes = await known_price_registry_scopes(db, uf)
    return {
        "uf": uf,
        "scopes": [
            {"key": item.key, "label": item.label, "sources": item.sources} for item in scopes
        ],
    }


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
    registry_source = _public_link(registry.source_url)
    return templates.TemplateResponse(
        request,
        "ata_detail.html",
        {
            "return_to": safe_list_url(request.query_params.get("return_to"), path="/atas"),
            "registry": SimpleNamespace(
                id=registry.id,
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
                source_url=registry_source,
                source_link_label=source_link_label(registry_source) if registry_source else None,
                source=_label(registry.source, _SOURCE_LABELS),
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
            "units": [
                SimpleNamespace(
                    item_number=_display_text(unit.item_number),
                    catalog_code=_display_text(unit.catalog_code),
                    description=_display_text(unit.description),
                    supplier_name=_display_text(unit.supplier_name),
                    quantity_registered=_quantity_with_unit(unit.quantity_registered, None),
                    adhesion_balance=_quantity_with_unit(unit.adhesion_balance, None),
                    remanejamento_balance=_quantity_with_unit(unit.remanejamento_balance, None),
                    adhesion_limit=_quantity_with_unit(unit.adhesion_limit, None),
                    purchase_limit=_quantity_with_unit(unit.purchase_limit, None),
                    accepts_adhesion=(
                        "Sim"
                        if unit.accepts_adhesion
                        else ("Não" if unit.accepts_adhesion is not None else "Não informado")
                    ),
                    unit=_display_text(unit.unit_name),
                    unit_code=_display_text(unit.unit_code),
                    unit_type=_display_text(unit.unit_type),
                    updated_at=_procurement_datetime(unit.updated_at_source),
                )
                for unit in registry.units
            ],
            "commitments": [
                SimpleNamespace(
                    item_number=_display_text(commitment.item_number),
                    unit=_display_text(commitment.unit),
                    commitment_type=_display_text(commitment.commitment_type),
                    quantity_registered=_quantity_with_unit(commitment.quantity_registered, None),
                    quantity_committed=_quantity_with_unit(commitment.quantity_committed, None),
                    balance=_quantity_with_unit(commitment.balance, None),
                    updated_at=_procurement_datetime(commitment.updated_at_source),
                )
                for commitment in registry.commitments
            ],
            "adhesions": [
                SimpleNamespace(
                    item_number=_display_text(adhesion.item_number),
                    requesting_unit=_display_text(adhesion.requesting_unit),
                    approved_at=_procurement_datetime(adhesion.approved_at),
                    approved_quantity=_quantity_with_unit(adhesion.approved_quantity, None),
                )
                for adhesion in registry.adhesions
            ],
        },
    )


@router.get("/contratos")
async def contracts_page(
    request: Request,
    search: str | None = Query(None, max_length=120),
    agency: str | None = None,
    supplier_cnpj: str | None = None,
    expiring_within_days: Annotated[
        int | None, Query(ge=1, le=365), BeforeValidator(_empty_query_to_none)
    ] = None,
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List contracts and their payment status."""

    normalized_supplier = normalize_cnpj(supplier_cnpj) if supplier_cnpj else None
    result = await ContractRepository(db).list_filtered(
        page=max(page_number, 1),
        page_size=50,
        search=search,
        agency=agency,
        supplier_cnpj=normalized_supplier,
        expiring_within_days=expiring_within_days,
    )
    filter_error = _supplier_filter_error(supplier_cnpj)
    if filter_error:
        result = Page(items=[], page=page_number, page_size=50, total=0)
    contracts = [
        SimpleNamespace(
            id=contract.id,
            number=_display_text(contract.contract_number),
            year=_display_text(contract.year),
            agency=_display_text(contract.agency_name),
            supplier=_display_text(contract.supplier_name),
            supplier_cnpj=contract.supplier_cnpj,
            object_description=_display_text(contract.object_description),
            signed_at=_procurement_datetime(contract.signed_at),
            valid_until=_procurement_datetime(contract.valid_until),
            term_dates=[
                SimpleNamespace(
                    number=_display_text(term.term_number),
                    date=_procurement_datetime(term.new_valid_until),
                )
                for term in published_term_dates(contract)
            ],
            value=_brl(contract_value(contract)),
            source=_label(contract.source, _SOURCE_LABELS),
        )
        for contract in result.items
    ]
    return templates.TemplateResponse(
        request,
        "contracts.html",
        {
            "contracts": contracts,
            **_list_context(request, "/contratos"),
            "filter_error": filter_error,
            "pagination": result,
            "filters": SimpleNamespace(
                search=search,
                agency=agency,
                supplier_cnpj=supplier_cnpj,
                expiring_within_days=expiring_within_days,
            ),
        },
        status_code=422 if filter_error else 200,
    )


@router.get("/contratos/{contract_id}")
async def contract_page(
    request: Request,
    contract_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Show one contract with payment documents and their provenance."""

    repository = ContractRepository(db)
    contract = await repository.get_detail(contract_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="Contrato nao encontrado")
    summary = await repository.payment_summary(
        contract, grace_days=get_settings().payment_grace_days
    )
    terms_observation = await repository.latest_terms_observation(contract.id)
    terms_evidence = terms_observation.source_record if terms_observation is not None else None
    return templates.TemplateResponse(
        request,
        "contract_detail.html",
        {
            "return_to": safe_list_url(request.query_params.get("return_to"), path="/contratos"),
            "contract": SimpleNamespace(
                id=contract.id,
                number=_display_text(contract.contract_number),
                year=_display_text(contract.year),
                agency=_display_text(contract.agency_name),
                agency_cnpj=_format_cnpj(contract.agency_cnpj),
                supplier=_display_text(contract.supplier_name),
                supplier_cnpj=contract.supplier_cnpj,
                supplier_cnpj_label=_format_cnpj(contract.supplier_cnpj),
                object_description=_display_text(contract.object_description),
                process_number=_display_text(contract.process_number),
                signed_at=_procurement_datetime(contract.signed_at),
                published_at=_procurement_datetime(contract.published_at),
                valid_from=_procurement_datetime(contract.valid_from),
                valid_until=_procurement_datetime(contract.valid_until),
                initial_value=_brl(contract.initial_value),
                global_value=_brl(contract.global_value),
                accumulated_value=_brl(contract.accumulated_value),
                is_adhesion=(
                    "Sim"
                    if contract.is_adhesion
                    else ("Nao" if contract.is_adhesion is not None else "Nao informado")
                ),
                source=_label(contract.source, _SOURCE_LABELS),
                source_url=_public_link(contract.source_url),
                procurement_id=contract.procurement_id,
                price_registry_id=contract.price_registry_id,
            ),
            "payment_count": summary["payment_count"],
            "paid_value": (
                _brl(Decimal(str(summary["paid_value"])))
                if summary["paid_value"] is not None
                else "Não apurado"
            ),
            "balance": (
                _brl(Decimal(str(summary["balance"])))
                if summary["balance"] is not None
                else "Não apurada"
            ),
            "payment_status": {
                "pago": "Os pagamentos vinculados atingem o valor de referência do contrato.",
                "parcial": "Os pagamentos vinculados cobrem parte do valor de referência.",
            }.get(str(summary["status"]), "Situação de pagamento não avaliada."),
            "terms_unavailable_source": (
                SimpleNamespace(
                    id=terms_evidence.id,
                    url=_public_link(terms_evidence.endpoint),
                )
                if terms_evidence is not None and terms_evidence.http_status == 410
                else None
            ),
            "items": [
                SimpleNamespace(
                    item_number=_display_text(item.item_number),
                    description=_display_text(item.description),
                    unit=_display_text(item.unit),
                    quantity=_quantity_with_unit(item.quantity, None),
                    unit_value=_brl(item.unit_value),
                    total_value=_brl(item.total_value),
                    supplier_name=_display_text(item.supplier_name),
                    supplier_cnpj=item.supplier_cnpj,
                )
                for item in contract.items
            ],
            "terms": [
                SimpleNamespace(
                    term_number=_display_text(term.term_number),
                    term_type=_display_text(term.term_type),
                    signed_at=_procurement_datetime(term.signed_at),
                    published_at=_procurement_datetime(term.published_at),
                    value_delta=_brl(term.value_delta),
                    new_valid_until=_procurement_datetime(term.new_valid_until),
                    object_description=_display_text(term.object_description),
                    status=_display_text(term.status),
                    source_url=_public_link(term.source_url),
                    source_record_id=term.source_record_id,
                )
                for term in contract.terms
            ],
            "payments": [
                SimpleNamespace(
                    id=payment.id,
                    date=_procurement_datetime(payment.payment_date),
                    amount=_brl(payment.amount),
                    phase=_display_text(payment.phase),
                    document_number=_display_text(payment.document_number),
                    agency=_display_text(payment.agency_name),
                    match_status=_display_text(payment.match_status),
                    source=_label(payment.source, _SOURCE_LABELS),
                    source_url=_public_link(payment.source_url),
                    federal_type=FEDERAL_PAYMENT_LABELS.get(
                        federal_payment_category(payment) or ""
                    ),
                    federal_conflict=payment.federal_identity_conflict,
                )
                for payment in contract.payments
            ],
        },
    )


_MUNICIPAL_STAGE_LABELS = {
    "queued": "Aguardando início",
    "discovery": "Consultando o portal",
    "preparing": "Preparando gravação",
    "persisting": "Gravando pagamentos",
    "finalizing": "Finalizando",
    "completed": "Concluída",
    "failed": "Falhou",
}


async def _municipal_source_views(
    db: AsyncSession,
    settings: Settings,
) -> tuple[list[SimpleNamespace], bool]:
    """Build payment cards with live progress from the current crawl runs."""

    enabled_municipal = set(getattr(settings, "municipal_payment_sources", ()))
    latest_runs = await latest_municipal_runs(db)
    now = datetime.now(UTC)
    views: list[SimpleNamespace] = []
    for municipal in list_municipal_sources(settings):
        latest = latest_runs.get(municipal.id)
        active = latest is not None and latest.status in {
            CrawlRunStatus.PENDING,
            CrawlRunStatus.RUNNING,
        }
        cursor = dict(latest.cursor or {}) if latest is not None and latest.cursor else {}
        stored_progress = cursor.get("progress")
        if not isinstance(stored_progress, dict) or not stored_progress:
            stored_progress = cursor
        progress = _municipal_progress.get(latest.id, {}) if active and latest is not None else {}
        stage = (
            str(progress.get("stage") or stored_progress.get("stage") or "queued") if active else ""
        )
        write_progress = municipal_write_progress(cursor)
        total: int | None
        if write_progress is not None:
            processed, total = write_progress
            written = processed
        else:
            processed = nonnegative_int(progress.get("processed", stored_progress.get("processed")))
            total = nonnegative_int(progress.get("total", stored_progress.get("total"))) or None
            written = 0
            if latest is not None:
                written = nonnegative_int(latest.records_created) + nonnegative_int(
                    latest.records_updated
                )
        percentage = min(round(processed * 100 / total), 100) if total else None
        started = latest.started_at if latest is not None else None
        elapsed_label = None
        if active and started is not None:
            elapsed_label = _duration_label(max((now - started).total_seconds(), 0))
        status = latest.status.value if latest is not None else None
        status_label = _STATUS_LABELS.get(status, status) if status else "Nunca coletado"
        if active and latest is not None and latest.cancel_requested:
            status_label = "Cancelando"
        message = str(
            progress.get("message")
            or stored_progress.get("message")
            or ("Coleta em andamento" if active else "")
        )
        views.append(
            SimpleNamespace(
                id=municipal.id,
                name=municipal.name,
                municipality=municipal.municipality,
                mode="API JSON" if municipal.mode == "json" else "CSV",
                configured=municipal.id in enabled_municipal,
                supports_details=municipal.supports_details,
                note=municipal.note,
                portal_url=municipal.portal_url,
                export_url=municipal.export_url,
                open_data_url=municipal.open_data_url,
                import_steps=tuple(
                    step.replace("{year}", str(now.year)) for step in municipal.import_steps
                ),
                active=active,
                run_id=str(latest.id) if latest is not None else None,
                last_status=status,
                last_status_label=status_label,
                last_run_at=_procurement_datetime(
                    (latest.finished_at or latest.started_at) if latest is not None else None
                ),
                last_found=latest.records_found if latest is not None else None,
                last_written=written,
                last_diagnostic=latest.diagnostic if latest is not None else None,
                municipal_write=write_progress,
                stage=stage,
                stage_label=_MUNICIPAL_STAGE_LABELS.get(stage, stage),
                progress_message=message,
                progress_processed=processed,
                progress_total=total,
                progress_percent=percentage,
                elapsed_label=elapsed_label,
            )
        )
    return views, any(view.active for view in views)


async def _active_municipal_run(db: AsyncSession, source_id: str) -> CrawlRun | None:
    return await db.scalar(
        select(CrawlRun)
        .where(
            CrawlRun.connector == f"municipal:{source_id}",
            CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
        )
        .order_by(CrawlRun.created_at.desc())
        .limit(1)
    )


async def _execute_municipal_collection(
    run_id: UUID,
    service: MunicipalPaymentIngestionService,
    source_id: str,
    year: int,
    include_details: bool,
) -> None:
    def report(progress: dict[str, Any]) -> None:
        _municipal_progress[run_id] = progress

    try:
        await service.run(
            source_id,
            year=year,
            include_details=include_details,
            run_id=run_id,
            progress_callback=report,
        )
    finally:
        await service.aclose()
        _municipal_progress.pop(run_id, None)


def _launch_municipal_collection(
    run_id: UUID,
    service: MunicipalPaymentIngestionService,
    source_id: str,
    year: int,
    include_details: bool,
) -> None:
    task = asyncio.create_task(
        _execute_municipal_collection(run_id, service, source_id, year, include_details),
        name=f"municipal-payments:{run_id}",
    )
    register_municipal_task(run_id, task)

    def _discard(finished: asyncio.Task[None]) -> None:
        del finished
        discard_municipal_task(run_id)

    task.add_done_callback(_discard)


@router.get("/pagamentos")
async def payments_page(
    request: Request,
    supplier_cnpj: str | None = None,
    phase: str | None = None,
    source: str | None = None,
    municipality: str | None = None,
    budget_function: str | None = None,
    fiscal_year: str | None = None,
    match_status: str | None = None,
    federal_type: str | None = None,
    view: str = "records",
    import_created: int | None = None,
    import_updated: int | None = None,
    import_errors: int | None = None,
    municipal_source: str | None = None,
    municipal_created: int | None = None,
    municipal_updated: int | None = None,
    municipal_linked: int | None = None,
    municipal_unlinked: int | None = None,
    municipal_diagnostic: str | None = None,
    municipal_started: bool = False,
    municipal_busy: bool = False,
    tracking: str | None = None,
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List payment facts; unlinked payments are not treated as unpaid."""

    settings = get_settings()
    current_year = datetime.now(ZoneInfo(get_settings().timezone)).year
    normalized_supplier = normalize_cnpj(supplier_cnpj) if supplier_cnpj else None
    repository = PaymentRepository(db)
    year = int(fiscal_year) if fiscal_year and re.fullmatch(r"20\d{2}|2100", fiscal_year) else None
    scope = repository.filtered_query(
        supplier_cnpj=normalized_supplier,
        phase=phase,
        source=source,
        municipality=municipality,
        budget_function=budget_function,
        fiscal_year=year,
        match_status=match_status,
    )
    municipal_sources, municipal_has_active = await _municipal_source_views(db, settings)
    filter_error = _supplier_filter_error(supplier_cnpj)
    if fiscal_year and year is None:
        filter_error = "Informe um exercício entre 2000 e 2100."
    if federal_type:
        if federal_type in FEDERAL_PAYMENT_LABELS:
            scope = scope.where(federal_payment_filter(federal_type))
        else:
            filter_error = "Selecione um tipo válido de pagamento federal."
    supplier_count = 0
    suppliers: list[dict[str, Any]] = []
    supplier_view = view == "suppliers"
    result: Page[Payment] = Page(items=[], page=page_number, page_size=50, total=0)
    if not filter_error:
        if supplier_view:
            result = Page(
                items=[], page=page_number, page_size=50, total=await repository.count(scope)
            )
        else:
            result = await repository.page(scope, page=page_number, page_size=50)
        supplier_count, suppliers = await repository.suppliers_in_scope(
            scope, page_number if supplier_view else None
        )
    entries = await repository.budget_entries(list(result.items))
    functions, years = await repository.budget_options()
    portals = {f"municipal:{item.id}": item for item in list_municipal_sources(settings)}
    receipt_ids = {
        p.source_record_id
        for p in result.items
        if (p.source in portals or p.federal_beneficiary_role) and p.source_record_id
    }
    receipts = (
        {
            key: (digest, _local_datetime(collected))
            for key, digest, collected in await db.execute(
                select(
                    SourceRecord.id, SourceRecord.response_hash, SourceRecord.collected_at
                ).where(SourceRecord.id.in_(receipt_ids))
            )
        }
        if receipt_ids
        else {}
    )
    match_labels = {
        "review": "Todos que exigem revisão",
        "unlinked": "Sem vínculo exato localizado",
        "linked": "Vínculo exato com contrato",
        "ambiguous": "Vínculo para revisão",
        "procurement_linked": "Vínculo exato com processo",
    }
    payments = [
        SimpleNamespace(
            id=payment.id,
            date=_procurement_datetime(payment.payment_date),
            has_payment_date=payment.payment_date is not None,
            supplier=_display_text(payment.supplier_name),
            supplier_cnpj=payment.supplier_cnpj if is_valid_cnpj(payment.supplier_cnpj) else None,
            agency=_display_text(payment.agency_name),
            amount=_brl(payment.amount),
            phase=_PAYMENT_PHASE_LABELS.get(payment.phase or "", _display_text(payment.phase)),
            document_number=_display_text(payment.document_number),
            match_status=match_labels.get(payment.match_status, "Conciliação não informada"),
            source=_label(payment.source, _SOURCE_LABELS),
            source_url=portals[payment.source].portal_url
            if payment.source in portals
            else _public_link(payment.source_url)
            if payment.federal_beneficiary_role
            else None,
            record_id=payment.source_record_id,
            receipt=receipts.get(payment.source_record_id) if payment.source_record_id else None,
            budget=entries.get(payment.id),
            aggregate=payment.status == "agregado_por_empenho",
            procurement_id=payment.procurement_id,
            contract_id=payment.contract_id,
            federal_type=FEDERAL_PAYMENT_LABELS.get(federal_payment_category(payment) or ""),
            federal_conflict=payment.federal_identity_conflict,
            federal_creditor_list=payment.federal_creditor_list,
        )
        for payment in result.items
    ]
    return templates.TemplateResponse(
        request,
        "payments.html",
        {
            "payments": payments,
            **_list_context(
                request,
                "/pagamentos",
                keys={
                    "supplier_cnpj",
                    "phase",
                    "source",
                    "page",
                    "municipality",
                    "budget_function",
                    "fiscal_year",
                    "match_status",
                    "federal_type",
                    "view",
                },
            ),
            "filter_error": filter_error,
            "source_options": list(_SOURCE_LABELS.items()),
            "phase_options": list(_PAYMENT_PHASE_LABELS.items()),
            "budget_functions": functions,
            "budget_years": years,
            "municipalities": list(portals.values()),
            "match_options": match_labels,
            "federal_payment_labels": FEDERAL_PAYMENT_LABELS,
            "supplier_count": supplier_count,
            "suppliers": suppliers,
            "supplier_view": supplier_view,
            "pagination": result,
            "filters": SimpleNamespace(
                supplier_cnpj=supplier_cnpj,
                phase=phase,
                source=source,
                municipality=municipality,
                budget_function=budget_function,
                fiscal_year=fiscal_year,
                match_status=match_status,
                federal_type=federal_type,
            ),
            "import_result": (
                {
                    "created": import_created,
                    "updated": import_updated,
                    "errors": import_errors,
                }
                if import_created is not None
                else None
            ),
            "municipal_sources": municipal_sources,
            "payment_tracking": await list_payment_tracking(db, settings.payment_poll_cnpjs),
            "federal_coverage": await latest_federal_batch(db, cnpj=normalized_supplier)
            if not filter_error
            else None,
            "federal_totals": await federal_payment_totals(db, scope) if not filter_error else {},
            "tracking_feedback": tracking,
            "federal_payment_source_enabled": "transparencia" in settings.payment_sources,
            "federal_payment_key_configured": bool(settings.transparencia_api_key.strip()),
            "municipal_has_active": municipal_has_active,
            "municipal_started": municipal_started,
            "municipal_busy": municipal_busy,
            "municipal_result": (
                {
                    "source": municipal_source,
                    "created": municipal_created,
                    "updated": municipal_updated,
                    "linked": municipal_linked,
                    "unlinked": municipal_unlinked,
                    "diagnostic": municipal_diagnostic,
                }
                if municipal_source is not None
                else None
            ),
            "current_year": current_year,
        },
        status_code=422 if filter_error or tracking == "invalid" else 200,
    )


@router.post("/pagamentos/acompanhamento")
async def update_payment_tracking_page(
    cnpj: str = Form(..., max_length=32),
    active: bool = Form(True),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    try:
        await set_payment_tracking(db, cnpj, active=active)
    except ValueError:
        await db.rollback()
        feedback = "invalid"
    else:
        await db.commit()
        feedback = "active" if active else "paused"
    return RedirectResponse(
        f"/pagamentos?tracking={feedback}#acompanhamento-federal", status_code=303
    )


@router.post("/pagamentos/importar")
async def import_payments_page(
    request: Request,
    file: UploadFile = File(...),
    source_name: str = Form("Portal"),
):
    """Import a portal export from the operator page."""

    if normalized_search_text(source_name) in {"sao luis", "prefeitura de sao luis"}:
        raise HTTPException(422, "Use Revisar CSV de São Luís na página de pagamentos.")
    content = await file.read(10 * 1024 * 1024 + 1)
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="arquivo maior que 10 MB")
    service = PaymentImportService()
    try:
        result = await service.import_bytes(
            content, filename=file.filename or "pagamentos.csv", source_name=source_name
        )
    except PaymentImportErrorList as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        await service.aclose()
    target = (
        f"/pagamentos?import_created={result.created}&import_updated={result.updated}"
        f"&import_errors={len(result.errors)}"
    )
    return RedirectResponse(target, status_code=303)


def _sao_luis_import(db: AsyncSession) -> SaoLuisCsvImport:
    return SaoLuisCsvImport(
        get_settings(),
        async_sessionmaker(
            db.bind, autoflush=False, expire_on_commit=False, sync_session_class=ApplicationSession
        ),
    )


@router.post("/pagamentos/sao-luis/previa")
async def preview_sao_luis_csv(
    request: Request, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)
):
    content = await file.read(MAX_CSV_BYTES + 1)
    if len(content) > MAX_CSV_BYTES:
        raise HTTPException(413, "O CSV deve ter até 10 MB.")
    try:
        preview = await _sao_luis_import(db).preview(content)
    except PaymentImportErrorList as exc:
        raise HTTPException(422, str(exc)) from exc
    return templates.TemplateResponse(request, "sao_luis_csv.html", {"preview": preview})


@router.post("/pagamentos/sao-luis/confirmar")
async def confirm_sao_luis_csv(
    request: Request, token: UUID = Form(...), db: AsyncSession = Depends(get_db)
):
    try:
        run_id, reused = await _sao_luis_import(db).confirm(token)
    except PaymentImportErrorList as exc:
        raise HTTPException(422, str(exc)) from exc
    except SaoLuisImportBusy as exc:
        return templates.TemplateResponse(
            request, "sao_luis_csv.html", {"error": str(exc), "token": token}, status_code=409
        )
    run = await db.get_one(CrawlRun, run_id)
    return templates.TemplateResponse(request, "sao_luis_csv.html", {"run": run, "reused": reused})


@router.post("/pagamentos/sao-luis/cancelar")
async def cancel_sao_luis_csv(token: UUID = Form(...), db: AsyncSession = Depends(get_db)):
    await _sao_luis_import(db).cancel(token)
    return RedirectResponse("/pagamentos#importar-sao-luis", status_code=303)


@router.post("/pagamentos/municipal")
async def collect_municipal_payments_page(
    source: str = Form(...),
    year: int = Form(...),
    include_details: bool = Form(False),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Start one municipal adapter in the background and return immediately."""

    try:
        municipal = get_municipal_source(source)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    active = await _active_municipal_run(db, municipal.id)
    if active is not None:
        return RedirectResponse("/pagamentos?municipal_busy=1#portais", status_code=303)

    service = MunicipalPaymentIngestionService(
        session_factory=async_sessionmaker(
            bind=db.bind,
            class_=AsyncSession,
            autoflush=False,
            expire_on_commit=False,
        )
        if db.bind is not None
        else None
    )
    try:
        run_id = await service.create_run(
            municipal.id,
            year,
            None,
            include_details,
        )
    except Exception:
        await service.aclose()
        raise
    _launch_municipal_collection(
        run_id,
        service,
        municipal.id,
        year,
        include_details,
    )
    return RedirectResponse("/pagamentos?municipal_started=1#portais", status_code=303)


@router.get("/pagamentos/portais")
async def municipal_payment_cards(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """HTMX partial with live progress for the five municipal sources."""

    settings = get_settings()
    municipal_sources, municipal_has_active = await _municipal_source_views(db, settings)
    return templates.TemplateResponse(
        request,
        "_municipal_payment_cards.html",
        {
            "municipal_sources": municipal_sources,
            "municipal_has_active": municipal_has_active,
            "current_year": datetime.now(ZoneInfo(get_settings().timezone)).year,
            "municipal_started": False,
            "municipal_busy": False,
        },
    )


@router.get("/leads/exportar.csv")
@router.get("/leads")
async def leads_page(
    request: Request,
    q: str = Query("", max_length=200),
    due: str = "",
    run: UUID | None = None,
    min_score: OptionalScoreQuery = None,
    event_type: str | None = None,
    requires_review: bool = False,
    lead_status: str | None = None,
    created_from: OptionalDateQuery = None,
    created_to: OptionalDateQuery = None,
    page_number: int = Query(1, alias="page", ge=1),
    include_history: bool = False,
    db: AsyncSession = Depends(get_db),
):
    export = request.url.path == "/leads/exportar.csv"
    implicit_queue = not export and not any(key != "page" for key in request.query_params)
    effective_due = "open" if implicit_queue else due
    selected = OpportunityFilters(
        query=q.strip(),
        status=lead_status or "",
        event_type=event_type or "",
        min_score=min_score,
        requires_review=requires_review,
        include_history=include_history,
        created_from=created_from,
        created_to=created_to,
        due=effective_due,
        run_id=run,
        timezone=get_settings().timezone,
    )
    errors = []
    if created_from and created_to and created_from > created_to:
        errors.append("A data inicial não pode ser posterior à data final.")
    if min_score is not None and not 0 <= min_score <= 100:
        errors.append("A prioridade mínima deve estar entre 0 e 100.")
    if run and await db.get(CrawlRun, run) is None:
        raise HTTPException(404, "Coleta não encontrada")
    selected = await resolve_queue_filters(db, selected)
    if export:
        from app.services.opportunity_export import opportunity_csv

        if errors:
            raise HTTPException(422, detail=" ".join(errors))
        cutoff = datetime.now(UTC)
        filename = f"oportunidades-{cutoff:%Y%m%d-%H%M%SZ}.csv"
        return StreamingResponse(
            opportunity_csv(db, selected, cutoff=cutoff),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    statement = opportunity_query(selected)
    if errors:
        statement = statement.where(false())
    total = int(
        await db.scalar(select(func.count()).select_from(statement.order_by(None).subquery())) or 0
    )
    rows = (
        (
            await db.scalars(
                loaded_opportunities(statement).offset((max(page_number, 1) - 1) * 50).limit(50)
            )
        )
        .unique()
        .all()
    )
    context = _list_context(request, "/leads")
    if implicit_queue:
        context["list_url"] = "/leads?" + urlencode({"due": "open", "page": page_number})
    export_url = request.url.remove_query_params(["page", "page_size"]).replace(
        path="/leads/exportar.csv"
    )
    if implicit_queue:
        export_url = export_url.include_query_params(due="open")
    leads = await _lead_views(db, rows, return_to=context["list_url"])
    filters = SimpleNamespace(
        min_score=min_score,
        include_history=include_history,
        event_type=event_type,
        requires_review=requires_review,
        lead_status=lead_status or "",
        created_from=created_from.isoformat() if created_from else "",
        created_to=created_to.isoformat() if created_to else "",
        q=q,
        due=effective_due,
        run=str(run) if run else "",
    )
    return templates.TemplateResponse(
        request,
        "leads.html",
        {
            "leads": leads,
            "filters": filters,
            "export_url": export_url,
            **context,
            "filter_errors": errors,
            "event_options": list(_EVENT_TYPE_LABELS.items()),
            "status_options": [("actionable", "Para analisar"), *_LEAD_STATUS_LABELS.items()],
            "empty_has_data": bool(await db.scalar(select(Lead.id).where(lead_current()).limit(1))),
            "pagination": SimpleNamespace(
                page=max(page_number, 1),
                page_size=50,
                total=total,
            ),
        },
        status_code=422 if errors else 200,
    )


@router.get("/leads/{lead_id}")
async def lead_page(request: Request, lead_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _render_lead_page(request, lead_id, db)


@router.get("/evidencias/{evidence_id}")
async def evidence_page(request: Request, evidence_id: UUID, db: AsyncSession = Depends(get_db)):
    evidence = await db.scalar(
        select(Evidence)
        .where(Evidence.id == evidence_id)
        .options(
            selectinload(Evidence.document),
            selectinload(Evidence.procurement),
            selectinload(Evidence.source_record),
        )
    )
    if evidence is None:
        raise HTTPException(404, "Evidência não encontrada")
    current = (
        await db.scalar(
            select(Evidence.id).where(Evidence.id == evidence_id, evidence_current(Evidence.id))
        )
        is not None
    )
    document = evidence.document
    stored = bool(document and (document.local_path or document.extracted_text))
    document_url = f"/api/documents/{document.id}/view" if document and stored else None
    if document_url and evidence.page_number:
        document_url += f"#page={evidence.page_number}"
    return templates.TemplateResponse(
        request,
        "evidence_detail.html",
        {
            "evidence": evidence,
            "title": document.title if document else "Registro oficial",
            "document_url": document_url,
            "official_url": _public_link(evidence.source_url)
            or _public_link(document.original_url if document else None),
            "collected_at": _local_datetime(evidence.collected_at),
            "current": current,
        },
    )


async def _render_lead_page(
    request: Request,
    lead_id: UUID,
    db: AsyncSession,
    *,
    draft_error: str | None = None,
    review_error: str | None = None,
    review_form: dict[str, str] | None = None,
):
    lead = await db.scalar(
        select(Lead)
        .where(Lead.id == lead_id)
        .options(
            selectinload(Lead.company).selectinload(Company.contacts),
            selectinload(Lead.triggering_event).selectinload(ProcurementEvent.evidence),
            selectinload(Lead.deadline),
            selectinload(Lead.outreach_drafts),
            selectinload(Lead.reviews),
            selectinload(Lead.procurement),
        )
    )
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead não encontrado")
    draft = (
        max(lead.outreach_drafts, key=lambda item: item.created_at)
        if lead.outreach_drafts
        else None
    )
    data = model_dict(lead)
    act = _event_act_view(lead.triggering_event, lead.company)
    data.update(
        company_name=lead.company.legal_name or lead.company.normalized_name,
        cnpj=lead.company.cnpj,
        cnpj_label=_format_cnpj(lead.company.cnpj),
        created_at=_local_datetime(lead.created_at),
        updated_at=_local_datetime(lead.updated_at),
        draft_created_at=_local_datetime(draft.created_at) if draft else None,
        event_label=act.label,
        event_scope=act.scope,
        reason_summary=act.reason if act.award else lead.reason_summary,
        event_at=_local_datetime(
            lead.triggering_event.occurred_at or lead.triggering_event.published_at
        ),
        lead_status=_label(lead.lead_status, _LEAD_STATUS_LABELS),
        process=lead.procurement.purchase_number or lead.procurement.title or "Contratação",
        agency=lead.procurement.agency_name or "Órgão não informado",
        official_url=procurement_source_page(lead.procurement),
        technical_url=_public_link(lead.procurement.source_url),
        origin_platform_url=await _origin_platform_url(db, lead.procurement),
    )
    view = SimpleNamespace(**data)
    reviews = [
        SimpleNamespace(
            decision=_label(review.decision, _REVIEW_DECISION_LABELS),
            reviewer=review.reviewer,
            previous_status=_label(review.previous_status, _LEAD_STATUS_LABELS),
            notes=review.notes,
            reviewed_at=_local_datetime(review.reviewed_at),
        )
        for review in lead.reviews
    ]
    evidences = [lead.triggering_event.evidence] if lead.triggering_event.evidence else []
    contacts = [_contact_view(contact) for contact in lead.company.contacts if contact.is_corporate]
    verification = await db.scalar(
        select(FactVerification).where(
            FactVerification.entity_type == "lead", FactVerification.entity_id == lead.id
        )
    )
    current = await db.scalar(select(Lead.id).where(Lead.id == lead.id, lead_current())) is not None
    notice = (
        verification.reason
        if verification and verification.status != "confirmed"
        else (
            "Evidência histórica ou desativada: novas operações bloqueadas."
            if not current
            else None
        )
    )
    current_deadline_ids, pending_revalidation = await _deadline_display_ids(
        db, {lead.deadline_id} if lead.deadline_id else set()
    )
    return_to = safe_list_url(request.query_params.get("return_to"))
    queue_filters = OpportunityFilters.from_url(return_to, timezone=get_settings().timezone)
    next_item = (
        await next_opportunity(db, lead, queue_filters)
        if current or queue_filters.include_history
        else None
    )
    return templates.TemplateResponse(
        request,
        "lead_detail.html",
        {
            "lead": view,
            "evidences": evidences,
            "draft": draft,
            "draft_error": draft_error,
            "integrity_notice": notice,
            "reviews": reviews,
            "contacts": contacts,
            "deadline": _deadline_view(
                lead.deadline,
                current=current and lead.deadline_id in current_deadline_ids,
                revalidation_pending=lead.deadline_id in pending_revalidation,
            ),
            "review_feedback": request.query_params.get("review"),
            "review_error": review_error,
            "review_form": review_form or {},
            "return_to": return_to,
            "next_href": f"/leads/{next_item.id}?{urlencode({'return_to': return_to})}"
            if next_item
            else None,
            "continue_available": bool(
                next_item
                and not queue_filters.include_history
                and next_item.lead_status in ACTIONABLE_STATUSES
            ),
        },
        status_code=422 if review_error else 409 if draft_error else 200,
    )


@router.post("/leads/{lead_id}/draft")
async def draft_lead_from_web(request: Request, lead_id: UUID, db: AsyncSession = Depends(get_db)):
    """Generate an idempotent draft from a regular browser form."""

    try:
        await generate_outreach(lead_id, OutreachRequest(channel="email"), db)
    except HTTPException as exc:
        if exc.status_code != 409:
            raise
        return await _render_lead_page(
            request,
            lead_id,
            db,
            draft_error=exc.detail
            if isinstance(exc.detail, str) and exc.detail.startswith("Lead histórico")
            else (
                "Não foi possível gerar o rascunho: este lead não possui uma fonte pública "
                "rastreável. Confira as evidências e a origem da contratação."
            ),
        )
    context = safe_list_url(request.query_params.get("return_to"))
    suffix = "?" + urlencode({"return_to": context}) if context != "/leads" else ""
    return RedirectResponse(f"/leads/{lead_id}{suffix}#rascunho", status_code=303)


@router.post("/leads/{lead_id}/review")
async def review_lead_from_web(
    request: Request,
    lead_id: UUID,
    decision: str = Form(""),
    reviewer: str = Form(""),
    notes: str = Form(""),
    return_to: str = Form("/leads"),
    continue_review: bool = Form(False),
    db: AsyncSession = Depends(get_db),
):
    """Record a human review decision from the operator page."""

    reviewer_name = reviewer.strip()
    error = None
    if not reviewer_name or len(reviewer_name) > 255:
        error = "Informe o nome do revisor (até 255 caracteres)."
    try:
        selected_decision = ReviewDecision(decision)
    except ValueError:
        selected_decision = None
        error = "Selecione uma decisão de revisão."
    if len(notes) > 4000:
        error = "As observações devem ter até 4.000 caracteres."
    if error or selected_decision is None:
        return await _render_lead_page(
            request,
            lead_id,
            db,
            review_error=error or "Selecione uma decisão.",
            review_form={"decision": decision, "reviewer": reviewer, "notes": notes},
        )
    lead = await db.get(Lead, lead_id)
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead não encontrado")
    if await db.scalar(select(Lead.id).where(Lead.id == lead_id, lead_current())) is None:
        raise HTTPException(
            409, "Oportunidade histórica ou invalidada: consulte a evidência atual."
        )
    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    return_to = safe_list_url(return_to)
    next_item = (
        await next_opportunity(
            db,
            lead,
            OpportunityFilters.from_url(return_to, timezone=get_settings().timezone),
            for_review=True,
        )
        if continue_review
        else None
    )
    previous = lead.lead_status.value
    lead.lead_status = LeadStatus(selected_decision.value)
    db.add(
        LeadReview(
            lead_id=lead.id,
            decision=selected_decision,
            notes=notes.strip() or None,
            reviewer=reviewer_name,
            previous_status=previous,
            reviewed_at=datetime.now(UTC),
        )
    )
    await db.commit()
    target = next_item.id if next_item else lead_id
    query = {"review": "previous" if next_item else "ok"}
    if return_to != "/leads":
        query["return_to"] = return_to
    return RedirectResponse(f"/leads/{target}?{urlencode(query)}", status_code=303)


@router.get("/crawls")
async def crawls_page(
    request: Request, run: UUID | None = None, db: AsyncSession = Depends(get_db)
):
    settings = get_settings()
    views, has_active = await _crawl_views(db, run)
    progress_views, llm_status = await _crawl_monitor_views(db)
    return templates.TemplateResponse(
        request,
        "crawls.html",
        {
            "crawls": views,
            "focused_run": run,
            "has_active": has_active,
            "monitored_crawls": progress_views,
            "monitor_has_active": any(item.active for item in progress_views),
            "resumable_crawls": await _resumable_crawl_views(db),
            "llm_status": llm_status,
            "ufs": _BRAZILIAN_UFS,
            "selected_uf": settings.default_uf,
            "period": _collection_period(settings.default_lookback_days),
            **_collection_object_options(settings),
        },
    )


@router.get("/crawls/progress")
async def crawls_progress(request: Request, db: AsyncSession = Depends(get_db)):
    views, llm_status = await _crawl_monitor_views(db)
    response = templates.TemplateResponse(
        request,
        "_crawl_progress.html",
        {
            "monitored_crawls": views,
            "monitor_has_active": any(item.active for item in views),
            "resumable_crawls": await _resumable_crawl_views(db),
            "llm_status": llm_status,
        },
    )
    response.headers["Cache-Control"] = "no-store"
    return response


async def _crawl_monitor_views(
    db: AsyncSession,
) -> tuple[list[SimpleNamespace], dict[str, Any] | None]:
    now = datetime.now(UTC)
    active = list(
        (
            await db.scalars(
                select(CrawlRun)
                .where(CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]))
                .order_by(CrawlRun.created_at.desc())
            )
        ).all()
    )
    finished = list(
        (
            await db.scalars(
                select(CrawlRun)
                .where(
                    CrawlRun.status.not_in([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
                    CrawlRun.finished_at >= now - timedelta(minutes=2),
                )
                .order_by(CrawlRun.finished_at.desc(), CrawlRun.id)
                .limit(3)
            )
        ).all()
    )
    from app.services.llm.queue import queue_statuses

    runs = [*active, *finished]
    latest_complete = await db.scalar(
        select(CrawlRun)
        .where(CrawlRun.connector == COMPLETE_COLLECTION_MODE)
        .order_by(CrawlRun.created_at.desc())
        .limit(1)
    )
    if latest_complete and all(run.id != latest_complete.id for run in runs):
        runs.insert(0, latest_complete)
    summaries = await queue_statuses(
        db, get_settings(), run_ids=[run.id for run in runs], include_global=True
    )
    base_views = [_crawl_view(run) for run in runs]
    await _attach_collection_details(db, runs, base_views)
    views = []
    for run, base in zip(runs, base_views, strict=True):
        view = monitor_view(run, base, now=now)
        view.llm = summaries[run.id]
        views.append(view)
    return views, summaries[None]


async def _resumable_crawl_views(db: AsyncSession) -> list[SimpleNamespace]:
    saved_records = (
        select(CollectionWorkItem.id)
        .where(
            CollectionWorkItem.batch_id == CollectionBatch.id,
            or_(
                (CollectionWorkItem.kind == "record")
                & CollectionWorkItem.status.in_(("pending", "failed", "details_pending")),
                (CollectionBatch.filters["mode"].as_string() == FEDERAL_BATCH_MODE)
                & CollectionWorkItem.kind.in_(("archive", "beneficiary"))
                & CollectionWorkItem.status.in_(("pending", "failed", "waiting")),
            ),
        )
        .exists()
    )
    runs = (
        await db.scalars(
            select(CrawlRun)
            .join(CollectionBatch, CrawlRun.batch_id == CollectionBatch.id)
            .where(
                CollectionBatch.status.in_(("limited", "failed", "suspended")),
                CollectionBatch.last_run_id == CrawlRun.id,
                CrawlRun.status.not_in((CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING)),
                saved_records,
            )
            .order_by(CrawlRun.created_at.desc(), CrawlRun.id)
            .limit(8)
        )
    ).all()
    municipal_names = {
        f"municipal:{source.id}": source.name for source in list_municipal_sources(get_settings())
    }
    views = []
    for run in runs:
        view = _crawl_view(run)
        view.connector_label = municipal_names.get(run.connector, view.connector_label)
        view.year = (run.filters or {}).get("year")
        views.append(view)
    return views


@router.post("/crawls/{run_id}/retry-llm")
async def retry_llm_from_web(run_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.api.routes.crawls import retry_llm

    await retry_llm(run_id, db)
    return RedirectResponse("/crawls", status_code=303)


@router.post("/crawls/llm/{batch_id}/cancel")
async def cancel_llm_batch_from_web(batch_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models import LLMBatch
    from app.repositories.crawls import JobLeaseRepository
    from app.services.llm.queue import cancel_batch

    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    batch = await db.get(LLMBatch, batch_id)
    if batch is None:
        raise HTTPException(404, "Lote de IA não encontrado")
    await cancel_batch(db, batch)
    await db.commit()
    return RedirectResponse("/crawls", status_code=303)


@router.post("/crawls/llm/{batch_id}/retry")
async def retry_llm_batch_from_web(batch_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.services.llm.queue import retry_batch

    try:
        await retry_batch(db, batch_id, get_settings())
        await db.commit()
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    return RedirectResponse("/crawls", status_code=303)


@router.get("/crawls/table")
async def crawls_table(
    request: Request, run: UUID | None = None, db: AsyncSession = Depends(get_db)
):
    """HTMX fragment repolled while a crawl is still running or pending."""

    views, has_active = await _crawl_views(db, run)
    return templates.TemplateResponse(
        request,
        "_crawl_table.html",
        {"crawls": views, "has_active": has_active, "focused_run": run},
    )


@router.get("/crawls/{run_id}/documents")
async def crawl_documents(request: Request, run_id: UUID, db: AsyncSession = Depends(get_db)):
    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Coleta não encontrada")
    settings, now = get_settings(), datetime.now(UTC)
    rows = []
    for document in await documents_for_run(db, run):
        error = safe_label(document.extraction_error or "")
        rows.append(
            SimpleNamespace(
                title=document.title or "Documento sem título",
                status=document.extraction_status.value,
                error=error,
                attempts=document.extraction_attempts,
                next_attempt=_local_datetime(document.next_attempt_at),
                active=document.is_active,
                eligible=eligible(document, now=now, max_attempts=settings.document_max_attempts),
            )
        )
    busy = run.status in {
        CrawlRunStatus.RUNNING,
        CrawlRunStatus.PENDING,
    } or await document_processing_busy(db, now=now)
    return templates.TemplateResponse(
        request,
        "crawl_documents.html",
        {
            "run": _crawl_view(run),
            "documents": rows,
            "legacy_scope": batch_document_ids(run) is None,
            "eligible_count": sum(row.eligible for row in rows),
            "processing_busy": busy,
            "max_attempts": settings.document_max_attempts,
        },
    )


@router.get("/crawls/{run_id}/federal")
async def federal_crawl_details(
    request: Request,
    run_id: UUID,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(CrawlRun, run_id)
    if run is None or run.batch_id is None:
        raise HTTPException(404, "Lote federal não encontrado")
    batch = (await federal_batch_views(db, {run.batch_id})).get(run.batch_id)
    if batch is None:
        raise HTTPException(404, "Lote federal não encontrado")
    scope = select(CollectionWorkItem).where(
        CollectionWorkItem.batch_id == batch.id,
        CollectionWorkItem.kind.in_(("archive", "beneficiary")),
    )
    total = await db.scalar(select(func.count()).select_from(scope.subquery())) or 0
    page_size = 50
    items = await db.scalars(
        scope.order_by(
            CollectionWorkItem.status.in_(("failed", "waiting", "pending")).desc(),
            CollectionWorkItem.position,
            CollectionWorkItem.id,
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return templates.TemplateResponse(
        request,
        "crawl_federal.html",
        {
            "federal_batch": batch,
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": [
                SimpleNamespace(
                    kind=item.kind,
                    status=item.status,
                    day=federal_day_label(item.payload.get("day")),
                    cnpj=item.payload.get("cnpj"),
                    year=item.payload.get("year"),
                    records=item.payload.get("records"),
                    diagnostic=safe_label(str(item.payload.get("diagnostic") or "")),
                    next_attempt=timestamp(item.payload.get("next_attempt_at")),
                )
                for item in items
            ],
        },
    )


@router.post("/crawls/{run_id}/retry-documents")
async def retry_documents_from_web(run_id: UUID, db: AsyncSession = Depends(get_db)):
    try:
        run, pipeline_request = await create_document_retry(db, run_id, get_settings())
        await db.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    launch_crawl(run.id, pipeline_request)
    return RedirectResponse("/crawls", status_code=303)


async def _crawl_views(
    db: AsyncSession, focused_run: UUID | None = None
) -> tuple[list[SimpleNamespace], bool]:
    statement = select(CrawlRun)
    if focused_run:
        statement = statement.where(CrawlRun.id == focused_run)
    rows = (await db.scalars(statement.order_by(CrawlRun.created_at.desc()).limit(200))).all()
    if focused_run and not rows:
        raise HTTPException(status_code=404, detail="Execução de coleta não encontrada")
    views = [_crawl_view(run) for run in rows]
    await _attach_collection_details(db, rows, views)
    batch_ids = {run.batch_id for run in rows if run.batch_id}
    batches = (
        {
            batch.id: batch
            for batch in (
                await db.scalars(select(CollectionBatch).where(CollectionBatch.id.in_(batch_ids)))
            ).all()
        }
        if batch_ids
        else {}
    )
    for run, view in zip(rows, views, strict=True):
        batch = batches.get(run.batch_id) if run.batch_id else None
        if batch and batch.filters.get("start_date") and batch.filters.get("end_date"):
            view.query_start = date.fromisoformat(batch.filters["start_date"]).strftime("%d/%m/%Y")
            view.query_end = date.fromisoformat(batch.filters["end_date"]).strftime("%d/%m/%Y")
        view.can_continue = bool(
            batch
            and batch.status in {"limited", "failed", "suspended"}
            and batch.last_run_id == run.id
            and not view.active
        )
    return views, any(view.active for view in views)


async def _attach_collection_details(
    db: AsyncSession, runs: Sequence[CrawlRun], views: list[SimpleNamespace]
) -> None:
    batch_ids = {}
    for run, view in zip(runs, views, strict=True):
        if view.federal and run.batch_id is not None:
            batch_ids[run.id] = run.batch_id
        elif view.complete:
            identifier = next(
                (
                    step.get("batch_id")
                    for step in view.complete_stages
                    if step.get("key") == "federal"
                ),
                None,
            )
            if isinstance(identifier, str):
                try:
                    batch_ids[run.id] = UUID(identifier)
                except ValueError:
                    continue
    batches = await federal_batch_views(db, set(batch_ids.values()))
    for view in views:
        if (batch_id := batch_ids.get(view.id)) is not None:
            view.federal_batch = batches.get(batch_id)
    await attach_complete_stages(db, views, get_settings(), _TECHNICAL_SOURCE_LABELS)


@router.post("/crawls/collect")
async def collect_all_from_web(
    uf: str | None = Form(None),
    start_date: OptionalDateForm = Form(None),
    end_date: OptionalDateForm = Form(None),
    force_refresh: bool = Form(False),
    modalities: list[str] | None = Form(None),
    object_query: str | None = Form(None),
    object_preset: list[str] | None = Form(None),
    object_group: str | None = Form(None),
    government_sphere: str | None = Form(None),
    procurements_only: bool = Form(False),
    federal_scope: FederalScope = Form("acervo"),
) -> RedirectResponse:
    """Start one background run across the available sources and record categories."""
    settings = get_settings()
    region = (uf or settings.default_uf).strip().upper()
    if region not in dict(_BRAZILIAN_UFS):
        raise HTTPException(422, "Selecione uma UF válida")
    selected, object_scope = _new_procurement_collection_filters(
        settings,
        start_date,
        end_date,
        days=settings.default_lookback_days,
        modalities=modalities,
        object_query=object_query,
        object_preset=object_preset,
        object_group=object_group,
        government_sphere=government_sphere,
    )
    if start_date is None or end_date is None:
        period = _collection_period(settings.default_lookback_days)
        start_date, end_date = (
            date.fromisoformat(period.start_date),
            date.fromisoformat(period.end_date),
        )
    scope = CollectionScope(
        region,
        start_date,
        end_date,
        force_refresh=force_refresh,
        modalities=selected,
        object_scope=object_scope,
        procurements_only=procurements_only,
        federal_scope="pular" if procurements_only else federal_scope,
    )
    service = CompleteCollection(settings)
    try:
        ticket = await service.enqueue(scope)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    launch_complete_collection(service, ticket, scope)
    return RedirectResponse(f"/crawls#coleta-{ticket.run_id}", status_code=303)


@router.post("/crawls/run")
async def run_crawl_from_web(
    connector: str = Form("all"),
    uf: str = Form("MA"),
    days: int = Form(7, ge=1, le=732),
    start_date: OptionalDateForm = Form(None),
    end_date: OptionalDateForm = Form(None),
    batch_size: int = Form(30, ge=1, le=500),
    process_documents: bool = Form(False),
    force_refresh: bool = Form(False),
    modalities: list[str] | None = Form(None),
    object_query: str | None = Form(None),
    object_preset: list[str] | None = Form(None),
    object_group: str | None = Form(None),
    government_sphere: str | None = Form(None),
) -> RedirectResponse:
    """Start a default crawl from the dashboard without exposing JSON details."""

    settings = get_settings()
    selected, object_scope = _new_procurement_collection_filters(
        settings,
        start_date,
        end_date,
        days=days,
        modalities=modalities,
        object_query=object_query,
        object_preset=object_preset,
        object_group=object_group,
        government_sphere=government_sphere,
    )
    uf = uf.strip().upper()
    if connector not in {"all", "pncp", "compras_gov"} or uf not in dict(_BRAZILIAN_UFS):
        raise HTTPException(422, "Selecione uma fonte e uma UF válidas.")
    request = PipelineRequest(
        connector=connector,
        uf=uf,
        days=None if start_date else days,
        start_date=start_date,
        end_date=end_date,
        modalities=selected or tuple(settings.default_modalities),
        object_scope=object_scope,
        calendar_months=use_calendar_slices(start_date, end_date, days),
        process_documents=process_documents,
        document_batch_size=max(1, batch_size),
        force_refresh=force_refresh,
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


def _retry_request(
    filters: dict[str, Any], *, connector: str, force_refresh: bool = False
) -> PipelineRequest:
    """Rebuild a crawl request from the filters persisted on a previous run."""

    if filters.get("mode") == "documents":
        raise HTTPException(
            status_code=409,
            detail="Use Reprocessar documentos com falha para selecionar somente os arquivos ainda elegíveis.",
        )
    if filters.get("mode") == FEDERAL_BATCH_MODE:
        raise HTTPException(409, "Use Continuar coleta federal para aproveitar o lote salvo.")
    modalities = filters.get("modalities") or get_settings().default_modalities
    return PipelineRequest(
        connector=connector,
        mode=str(filters.get("mode") or "procurements"),
        uf=str(filters.get("uf") or get_settings().default_uf),
        days=filters.get("days"),
        start_date=_filter_date(filters.get("start_date")),
        end_date=_filter_date(filters.get("end_date")),
        modalities=tuple(str(item) for item in modalities),
        municipality=filters.get("municipality"),
        agency=filters.get("agency"),
        agency_cnpj=filters.get("agency_cnpj"),
        unit_code=filters.get("unit_code"),
        price_registry_scopes=restore_price_registry_scopes(filters),
        keyword=filters.get("keyword"),
        object_scope=ObjectScope.from_mapping(filters["object_scope"])
        if filters.get("object_scope")
        else None,
        calendar_months=bool(filters.get("calendar_months")),
        max_pages=filters.get("max_pages"),
        process_documents=bool(filters.get("process_documents", True)),
        document_batch_size=filters.get("document_batch_size"),
        contract_details=bool(filters.get("contract_details", True)),
        discovery_kind=str(filters.get("discovery_kind") or "publication"),
        external_ids=tuple(str(value) for value in filters.get("external_ids") or []),
        force_refresh=force_refresh,
    )


@router.post("/crawls/{run_id}/retry")
async def retry_failed_crawl_source(
    run_id: UUID,
    connector: str = Form(...),
    force_refresh: bool = Form(False),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Retry only one failed source using the original crawl filters."""

    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Coleta não encontrada")
    failed_sources = {source.id for source in _source_views(run) if source.retryable}
    if (
        connector not in failed_sources
        or connector not in _SOURCE_LABELS
        or connector.startswith("municipal:")
    ):
        raise HTTPException(status_code=409, detail="Essa fonte não possui falha repetível")

    retry_request = _retry_request(
        run.filters or {}, connector=connector, force_refresh=force_refresh
    )
    retry_run = await IngestionPipeline().create_run(retry_request)
    launch_crawl(retry_run.id, retry_request)
    return RedirectResponse("/crawls", status_code=303)


@router.post("/crawls/{run_id}/retry-all")
async def retry_terminal_crawl(
    run_id: UUID,
    force_refresh: bool = Form(False),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Recreate a terminal crawl (failed/partial/cancelled) from its original filters."""

    run = await db.get(CrawlRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Coleta não encontrada")
    if run.status not in {
        CrawlRunStatus.FAILED,
        CrawlRunStatus.PARTIAL,
        CrawlRunStatus.CANCELLED,
    }:
        raise HTTPException(status_code=409, detail="A coleta não está em estado repetível")
    filters = run.filters or {}
    connector = str(filters.get("connector") or run.connector)
    if connector.startswith("municipal:"):
        raise HTTPException(
            status_code=409,
            detail="Coleta municipal deve ser repetida na pagina de Pagamentos",
        )
    active = int(
        (
            await db.scalar(
                select(func.count())
                .select_from(CrawlRun)
                .where(
                    CrawlRun.connector == connector,
                    CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING]),
                )
            )
        )
        or 0
    )
    if active:
        raise HTTPException(status_code=409, detail="Já existe coleta em andamento para esta fonte")

    retry_request = _retry_request(filters, connector=connector, force_refresh=force_refresh)
    retry_run = await IngestionPipeline().create_run(retry_request)
    launch_crawl(retry_run.id, retry_request)
    return RedirectResponse("/crawls", status_code=303)


@router.get("/ajuda")
async def help_page(request: Request):
    """Render the lawyer-facing usage manual in plain Portuguese."""

    return templates.TemplateResponse(request, "ajuda.html", {})


@router.get("/testes")
async def tests_page(request: Request):
    """Render the operator-facing diagnostics and full test-suite runner."""

    if hasattr(_application_tests, "refresh_identity"):
        await _application_tests.refresh_identity()
    state = _application_tests.current_state
    running = state is not None and state.status == "running"
    report = None if running else _application_tests.last_report()
    return templates.TemplateResponse(
        request,
        "tests.html",
        {
            "state": state,
            "running": running,
            "report": report,
            "report_load_error": getattr(_application_tests, "report_load_error", None),
            "pytest_available": _application_tests.pytest_available,
            "report_path": str(_application_tests.report_path),
            "reports_by_mode": {
                mode: _application_tests.report_for_mode(mode) for mode in ("quick", "full")
            }
            if hasattr(_application_tests, "report_for_mode")
            else {},
        },
    )


@router.post("/testes/executar")
async def run_application_tests(
    request: Request,
    mode: str = Form("quick"),
    include_live: bool = Form(False),
) -> RedirectResponse:
    """Start a background diagnostic or pytest run."""

    try:
        _application_tests.start(mode, request.app, include_live=include_live)
    except TestRunBusy:
        return RedirectResponse("/testes?status=busy", status_code=303)
    except ValueError:
        return RedirectResponse("/testes?status=invalid", status_code=303)
    return RedirectResponse("/testes", status_code=303)


@router.post("/testes/cancelar")
async def cancel_application_tests() -> RedirectResponse:
    """Cancel the active diagnostic/suite run."""

    if not _application_tests.cancel():
        raise HTTPException(status_code=409, detail="Nenhuma execução está em andamento")
    return RedirectResponse("/testes", status_code=303)


@router.get("/testes/status")
async def application_tests_status(request: Request):
    """HTMX partial used while a test run is in progress."""

    state = _application_tests.current_state
    running = state is not None and state.status == "running"
    if not running and hasattr(_application_tests, "refresh_identity"):
        await _application_tests.refresh_identity()
    report = None if running else _application_tests.last_report()
    return templates.TemplateResponse(
        request,
        "_test_status.html",
        {
            "test_status_fragment": True,
            "state": state,
            "running": running,
            "report": report,
            "report_load_error": getattr(_application_tests, "report_load_error", None),
            "pytest_available": _application_tests.pytest_available,
        },
    )


@router.get("/settings")
async def settings_page(request: Request, db: AsyncSession = Depends(get_db)):
    leases = await active_leases(db)
    scheduler = await scheduler_status(db)
    last_activity = scheduler.get("last_activity_at")
    settings = get_settings()
    public_settings = settings.public_view()
    public_settings["payment_poll_count"] = len(
        await tracked_payment_cnpjs(db, settings.payment_poll_cnpjs)
    )
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "settings": public_settings,
            "counts": await data_counts(db),
            "count_labels": COUNT_TABLE_LABELS,
            "block_reason": await maintenance_blocked_reason(db),
            "leases": [
                SimpleNamespace(name=name, expires_at=_local_datetime(expires))
                for name, expires in leases
            ],
            "scheduler": SimpleNamespace(
                paused=bool(scheduler.get("paused")),
                enabled=bool(scheduler.get("enabled")),
                alive=bool(scheduler.get("alive")),
                heartbeat_age_seconds=scheduler.get("heartbeat_age_seconds"),
                last_activity_at=(
                    _local_datetime(last_activity) if isinstance(last_activity, datetime) else None
                ),
            ),
            "scheduler_feedback": request.query_params.get("scheduler"),
            "reset_status": request.query_params.get("reset"),
        },
    )


@router.post("/settings/pause-scheduler")
async def pause_scheduler(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    """Pause the automatic collection jobs without stopping the process."""

    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    await asyncio.to_thread(set_scheduler_paused, True)
    await db.commit()
    return RedirectResponse("/settings?scheduler=pausado", status_code=303)


@router.post("/settings/resume-scheduler")
async def resume_scheduler(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    """Resume the automatic collection jobs."""

    await JobLeaseRepository(db).assert_writes_allowed(now=datetime.now(UTC))
    await asyncio.to_thread(set_scheduler_paused, False)
    await db.commit()
    return RedirectResponse("/settings?scheduler=ativo", status_code=303)


@router.post("/settings/release-stale-leases")
async def release_stale_leases(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    """Delete expired leases and orphan crawl leases, then return to settings."""

    await JobLeaseRepository(db).purge_stale(now=datetime.now(UTC))
    await db.commit()
    return RedirectResponse("/settings?reset=livre", status_code=303)


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


@router.get("/history/{entity_type}/{entity_id}")
async def field_history_fragment(
    request: Request,
    entity_type: EntityType,
    entity_id: UUID,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    history = await history_page(db, entity_type, entity_id, page)
    return templates.TemplateResponse(
        request,
        "_field_history.html",
        {"entity_type": entity_type, "entity_id": entity_id, "history": history},
    )


@router.post("/crawls/{run_id}/continue")
async def continue_crawl_from_web(
    run_id: UUID, db: AsyncSession = Depends(get_db)
) -> RedirectResponse:
    await continue_crawl_endpoint(run_id, db)
    return RedirectResponse("/crawls", status_code=303)
