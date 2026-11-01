"""Server-rendered Portuguese web interface."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BeforeValidator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.api.routes.crawls import cancel_crawl, continue_crawl_endpoint, launch_crawl
from app.api.routes.history import EntityType, history_page
from app.api.routes.leads import generate_outreach
from app.api.serialization import model_dict
from app.config import Settings, get_settings
from app.connectors.capabilities import get_source_capabilities
from app.connectors.municipal_payments.sources import get_municipal_source, list_municipal_sources
from app.dependencies import get_db
from app.models import (
    CollectionBatch,
    Company,
    CompanyContact,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    DeadlineStatus,
    Document,
    FieldChange,
    Lead,
    LeadReview,
    LeadStatus,
    Participant,
    Procurement,
    ProcurementEvent,
    ProcurementItem,
    ReviewDecision,
)
from app.models.participation import FactVerification
from app.navigation import (
    NAV_GROUPS,
    active_group_id,
    active_item_href,
    breadcrumbs,
    get_group,
)
from app.repositories.contracts import ContractRepository, PaymentRepository, contract_value
from app.repositories.crawls import JobLeaseRepository
from app.repositories.price_registries import PriceRegistryRepository
from app.repositories.procurements import (
    CompanyRepository,
    ProcurementRepository,
)
from app.schemas import OutreachRequest
from app.services.application_tests import ApplicationTestService, TestRunBusy
from app.services.crawl_monitor import monitor_view, nonnegative_int
from app.services.crawl_progress import safe_label
from app.services.group_hubs import build_group_hub
from app.services.identifiers import canonical_modality, is_valid_cnpj, normalize_cnpj
from app.services.ingestion import IngestionPipeline, PipelineRequest
from app.services.ingestion.document_retry import (
    batch_document_ids,
    create_document_retry,
    document_processing_busy,
    documents_for_run,
    eligible,
)
from app.services.ingestion.municipal_payments import (
    MunicipalPaymentIngestionService,
    discard_municipal_task,
    latest_municipal_runs,
    register_municipal_task,
)
from app.services.ingestion.payment_import import PaymentImportErrorList, PaymentImportService
from app.services.ingestion.processor import DocumentProcessingService
from app.services.maintenance import (
    COUNT_TABLE_LABELS,
    MaintenanceBlocked,
    active_leases,
    clear_all_data,
    data_counts,
    maintenance_blocked_reason,
)
from app.services.participation_integrity import deadline_current, event_verified, lead_current
from app.services.price_registry_scopes import (
    known_price_registry_scopes,
    restore_price_registry_scopes,
)
from app.services.scheduler_control import scheduler_status, set_scheduler_paused

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
templates.env.globals.update(
    nav_groups=NAV_GROUPS,
    nav_active_group=active_group_id,
    nav_active_item=active_item_href,
    nav_breadcrumbs=breadcrumbs,
)
_application_tests = ApplicationTestService()
_municipal_progress: dict[UUID, dict[str, Any]] = {}


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
    "transparencia": "Portal da Transparencia",
    "manual": "Importacao manual",
    **{
        f"municipal:{source.id}": f"{source.name} (municipal)"
        for source in list_municipal_sources()
    },
}
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
    "SESSION_OPENED": "Sessao aberta",
    "BEST_OFFER_DECLARED": "Melhor oferta declarada",
    "NEGOTIATION_REQUESTED": "Negociacao solicitada",
    "DILIGENCE_REQUESTED": "Diligencia solicitada",
    "DOCUMENTATION_REQUESTED": "Documentacao solicitada",
    "PARTICIPATION_DETECTED": "Participação em processo ativo",
    "UNKNOWN": "Não informado",
}
_PROCUREMENT_PHASE_LABELS = {
    "unknown": "Nao informada",
    "proposals": "Propostas",
    "dispute": "Disputa",
    "diligence": "Diligencia",
    "judgment": "Julgamento",
    "appeal": "Recurso",
    "counterargument": "Contrarrazoes",
    "qualification": "Habilitacao",
    "adjudication": "Adjudicacao",
    "homologation": "Homologacao",
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


def _contact_view(contact: CompanyContact) -> SimpleNamespace:
    return SimpleNamespace(
        contact_type=_label(contact.contact_type, _CONTACT_TYPE_LABELS),
        contact_type_key=_enum_key(contact.contact_type),
        value=contact.contact_value,
        status=_label(contact.status, _CONTACT_STATUS_LABELS),
        is_corporate=contact.is_corporate,
        source_url=contact.source_url,
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
            if active:
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
        views.append(
            SimpleNamespace(
                id=source_id,
                label="Documentos do lote"
                if source_id == "documents"
                else _SOURCE_LABELS.get(source_id, source_id),
                status=status,
                status_label=_STATUS_LABELS.get(status, status.replace("_", " ").title()),
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
    return SimpleNamespace(
        id=run.id,
        connector_label=_CONNECTOR_LABELS.get(run.connector, run.connector),
        mode_label=(
            "Reprocessamento de documentos"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "documents"
            else "Atas (ARP)"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "price_registries"
            else "Contratos"
            if isinstance(run.filters, dict) and run.filters.get("mode") == "contracts"
            else "Pagamentos municipais"
            if run.connector.startswith("municipal:")
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
            and run.filters.get("mode") != "documents"
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
                        Deadline.status == DeadlineStatus.DUE_WITHIN_24H, deadline_current()
                    )
                )
            )
            or 0
        ),
        "Prazos vencidos": int(
            (
                await db.scalar(
                    select(func.count(Deadline.id)).where(
                        Deadline.status == DeadlineStatus.EXPIRED, deadline_current()
                    )
                )
            )
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
            select(Lead)
            .where(lead_current())
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
            event_type=_label(item.triggering_event.event_type, _EVENT_TYPE_LABELS),
            score=item.score,
            created_at=_local_datetime(item.created_at),
            deadline_status=(
                _label(item.deadline.status, _DEADLINE_STATUS_LABELS)
                if item.deadline
                else "Nao informado"
            ),
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
    uf: OptionalUfQuery = None,
    municipality: str | None = None,
    agency: str | None = None,
    agency_cnpj: str | None = None,
    company_cnpj: str | None = None,
    modality: list[str] | None = Query(None),
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
    if selected_agency_cnpj is not None and len(selected_agency_cnpj) != 14:
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
    page = await ProcurementRepository(db).list_filtered(
        page=max(page_number, 1),
        page_size=50,
        uf=selected_uf,
        municipality=municipality,
        agency=agency,
        agency_cnpj=selected_agency_cnpj,
        company_cnpj=selected_company_cnpj,
        modalities=selected_modalities,
        procurement_type=selected_type,
        status_category=selected_category,
        phase=selected_phase,
        is_srp=parsed_srp,
        value_min=value_min,
        value_max=value_max,
    )
    return templates.TemplateResponse(
        request,
        "procurements.html",
        {
            "procurements": page.items,
            "pagination": page,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(
                uf=selected_uf,
                municipality=municipality,
                agency=agency,
                agency_cnpj=agency_cnpj,
                company_cnpj=company_cnpj,
                company_cnpj_valid=selected_company_cnpj,
                invalid_cnpj=invalid_cnpj,
                modalities=selected_modalities,
                procurement_type=selected_type,
                status_category=selected_category,
                phase=selected_phase,
                is_srp=is_srp if parsed_srp is not None else None,
                value_min=value_min,
                value_max=value_max,
            ),
            "modality_options": [
                (key, _PROCUREMENT_MODALITY_LABELS[key]) for key in _PROFILTER_MODALITY_KEYS
            ],
            "type_options": list(_PROCUREMENT_TYPE_LABELS.items()),
            "status_category_options": list(_PROCUREMENT_STATUS_CATEGORY_LABELS.items()),
            "phase_options": list(_PROCUREMENT_PHASE_LABELS.items()),
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
    return templates.TemplateResponse(
        request,
        "procurement_detail.html",
        {
            "procurement": _procurement_view(procurement),
            "items": [_item_view(item, procurement) for item in items],
            "item_query": query,
            "item_count": len(items),
            "documents": [
                _document_view(document, procurement)
                for document in procurement.documents
                if include_history or document.id in canonical_ids
            ],
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
    uf: OptionalUfQuery = None,
    published_from: OptionalDateQuery = None,
    published_to: OptionalDateQuery = None,
    has_lead: bool = False,
    active_only: bool = False,
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
            "pagination": result,
            "ufs": _BRAZILIAN_UFS,
            "filters": SimpleNamespace(
                uf=selected_uf,
                search=search,
                published_from=published_from.isoformat() if published_from else "",
                published_to=published_to.isoformat() if published_to else "",
                has_lead=has_lead,
                active_only=active_only,
            ),
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

    settings = get_settings()
    collection_scopes = await known_price_registry_scopes(db, settings.default_uf)
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
            source=item.source,
        )
        for item in result.items
    ]
    return templates.TemplateResponse(
        request,
        "atas.html",
        {
            "collection": SimpleNamespace(
                interval_hours=settings.scheduler_price_registry_hours,
                lookback_days=settings.effective_price_registry_lookback_days,
                default_days=min(7, settings.effective_price_registry_lookback_days),
                uf=settings.default_uf,
                scopes=collection_scopes,
            ),
            "ufs": _BRAZILIAN_UFS,
            "registries": registries,
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
    )


@router.post("/atas/collect")
async def collect_price_registries_from_web(
    uf: str = Form(..., min_length=2, max_length=2),
    registry_scope: str = Form(..., min_length=1, max_length=80),
    connector: str = Form("all"),
    days: int = Form(7, ge=1, le=3650),
    force_refresh: bool = Form(False),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Collect explicit known units, without turning a UF choice into a national query."""

    settings = get_settings()
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
        days=days,
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
    force_refresh: bool = Form(False),
) -> RedirectResponse:
    cnpj = normalize_cnpj(agency_cnpj)
    if connector not in {"all", "pncp", "compras_gov"} or not cnpj or not is_valid_cnpj(cnpj):
        raise HTTPException(status_code=422, detail="Fonte ou CNPJ do órgão inválido")
    request = PipelineRequest(
        connector=connector,
        mode="contracts",
        agency_cnpj=cnpj,
        days=days,
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
    return templates.TemplateResponse(
        request,
        "ata_detail.html",
        {
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
    )
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
            "pagination": result,
            "filters": SimpleNamespace(
                search=search,
                agency=agency,
                supplier_cnpj=supplier_cnpj,
            ),
        },
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
    return templates.TemplateResponse(
        request,
        "contract_detail.html",
        {
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
                global_value=_brl(contract_value(contract)),
                accumulated_value=_brl(contract.accumulated_value),
                is_adhesion=(
                    "Sim"
                    if contract.is_adhesion
                    else ("Nao" if contract.is_adhesion is not None else "Nao informado")
                ),
                source=_label(contract.source, _SOURCE_LABELS),
                source_url=contract.source_url,
                procurement_id=contract.procurement_id,
                price_registry_id=contract.price_registry_id,
            ),
            "paid_value": _brl(Decimal(str(summary["paid_value"]))),
            "balance": (
                _brl(Decimal(str(summary["balance"])))
                if summary["balance"] is not None
                else "Nao informado"
            ),
            "payment_status": {
                "pago": "Pago",
                "parcial": "Pagamento parcial localizado",
                "sem_pagamento_localizado": "Pagamento nao localizado na fonte",
                "nao_avaliado": "Pagamento nao avaliado (contrato recente ou sem valor)",
            }.get(str(summary["status"]), "Pagamento nao avaliado"),
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
                    source_url=term.source_url,
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
                    source_url=payment.source_url,
                )
                for payment in contract.payments
            ],
        },
    )


_MUNICIPAL_STAGE_LABELS = {
    "queued": "Aguardando início",
    "discovery": "Consultando o portal",
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
        progress = _municipal_progress.get(latest.id, {}) if active and latest is not None else {}
        stage = str(progress.get("stage") or cursor.get("stage") or "queued") if active else ""
        processed_value = progress.get("processed", cursor.get("processed", 0))
        total_value = progress.get("total", cursor.get("total"))
        try:
            processed = max(int(processed_value or 0), 0)
        except (TypeError, ValueError):
            processed = 0
        try:
            total = max(int(total_value), 0) if total_value is not None else None
        except (TypeError, ValueError):
            total = None
        if total == 0:
            total = None
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
            or cursor.get("message")
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
                last_created=latest.records_created if latest is not None else None,
                last_diagnostic=latest.diagnostic if latest is not None else None,
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
    page_number: int = Query(1, alias="page", ge=1),
    db: AsyncSession = Depends(get_db),
):
    """List payment facts; unlinked payments are not treated as unpaid."""

    settings = get_settings()
    current_year = datetime.now(ZoneInfo(get_settings().timezone)).year
    normalized_supplier = normalize_cnpj(supplier_cnpj) if supplier_cnpj else None
    result = await PaymentRepository(db).list_filtered(
        page=max(page_number, 1),
        page_size=50,
        supplier_cnpj=normalized_supplier,
        phase=phase,
        source=source,
    )
    municipal_sources, municipal_has_active = await _municipal_source_views(db, settings)
    payments = [
        SimpleNamespace(
            id=payment.id,
            date=_procurement_datetime(payment.payment_date),
            supplier=_display_text(payment.supplier_name),
            supplier_cnpj=payment.supplier_cnpj,
            agency=_display_text(payment.agency_name),
            amount=_brl(payment.amount),
            phase=_display_text(payment.phase),
            document_number=_display_text(payment.document_number),
            match_status=_display_text(payment.match_status),
            source=_label(payment.source, _SOURCE_LABELS),
            source_url=payment.source_url,
            contract_id=payment.contract_id,
        )
        for payment in result.items
    ]
    return templates.TemplateResponse(
        request,
        "payments.html",
        {
            "payments": payments,
            "pagination": result,
            "filters": SimpleNamespace(supplier_cnpj=supplier_cnpj, phase=phase, source=source),
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
    )


@router.post("/pagamentos/importar")
async def import_payments_page(
    request: Request,
    file: UploadFile = File(...),
    source_name: str = Form("Portal"),
):
    """Import a portal export from the operator page."""

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


@router.get("/leads")
async def leads_page(
    request: Request,
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
    if min_score is not None and not 0 <= min_score <= 100:
        min_score = None
    statement = select(Lead).options(
        selectinload(Lead.company), selectinload(Lead.triggering_event), selectinload(Lead.deadline)
    )
    if not include_history:
        statement = statement.where(lead_current())
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
    if lead_status:
        try:
            selected_lead_status = LeadStatus(lead_status)
        except ValueError:
            selected_lead_status = None
        if selected_lead_status is not None:
            statement = statement.where(Lead.lead_status == selected_lead_status)
    if created_from is not None:
        start, _ = _day_bounds(created_from)
        statement = statement.where(Lead.created_at >= start)
    if created_to is not None:
        _, end = _day_bounds(created_to)
        statement = statement.where(Lead.created_at <= end)
    total = int(
        await db.scalar(select(func.count()).select_from(statement.order_by(None).subquery())) or 0
    )
    rows = (
        (
            await db.scalars(
                statement.order_by(Lead.score.desc(), Lead.created_at.desc())
                .offset((max(page_number, 1) - 1) * 50)
                .limit(50)
            )
        )
        .unique()
        .all()
    )
    leads = [
        SimpleNamespace(
            id=item.id,
            company_name=item.company.legal_name or item.company.normalized_name,
            cnpj=item.company.cnpj,
            event_type=_label(item.triggering_event.event_type, _EVENT_TYPE_LABELS),
            score=item.score,
            created_at=_local_datetime(item.created_at),
            deadline_status=(
                _label(item.deadline.status, _DEADLINE_STATUS_LABELS)
                if item.deadline
                else "Nao informado"
            ),
            lead_status=_label(item.lead_status, _LEAD_STATUS_LABELS),
        )
        for item in rows
    ]
    filters = SimpleNamespace(
        min_score=min_score,
        include_history=include_history,
        event_type=event_type,
        requires_review=requires_review,
        lead_status=lead_status or "",
        created_from=created_from.isoformat() if created_from else "",
        created_to=created_to.isoformat() if created_to else "",
    )
    return templates.TemplateResponse(
        request,
        "leads.html",
        {
            "leads": leads,
            "filters": filters,
            "pagination": SimpleNamespace(
                page=max(page_number, 1),
                page_size=50,
                total=total,
            ),
        },
    )


@router.get("/leads/{lead_id}")
async def lead_page(request: Request, lead_id: UUID, db: AsyncSession = Depends(get_db)):
    return await _render_lead_page(request, lead_id, db)


async def _render_lead_page(
    request: Request, lead_id: UUID, db: AsyncSession, *, draft_error: str | None = None
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
    contacts = [_contact_view(contact) for contact in lead.company.contacts if contact.is_corporate]
    verification = await db.scalar(
        select(FactVerification).where(
            FactVerification.entity_type == "lead", FactVerification.entity_id == lead.id
        )
    )
    return templates.TemplateResponse(
        request,
        "lead_detail.html",
        {
            "lead": view,
            "evidences": evidences,
            "draft": draft,
            "draft_error": draft_error,
            "integrity_notice": verification.reason
            if verification and verification.status != "confirmed"
            else (
                "Evidência histórica ou desativada: novas operações bloqueadas."
                if await db.scalar(select(Lead.id).where(Lead.id == lead.id, lead_current()))
                is None
                else None
            ),
            "reviews": reviews,
            "contacts": contacts,
            "deadline": (
                SimpleNamespace(
                    when=_procurement_datetime(
                        lead.deadline.explicit_deadline_at
                        or lead.deadline.estimated_deadline_at
                        or lead.deadline.trigger_at
                    ),
                    status=lead.deadline.status.value,
                    type=lead.deadline.deadline_type,
                )
                if lead.deadline is not None
                else None
            ),
            "review_feedback": request.query_params.get("review"),
        },
        status_code=409 if draft_error else 200,
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
    return RedirectResponse(f"/leads/{lead_id}#rascunho", status_code=303)


@router.post("/leads/{lead_id}/review")
async def review_lead_from_web(
    lead_id: UUID,
    decision: str = Form(...),
    reviewer: str = Form(...),
    notes: str = Form(""),
    db: AsyncSession = Depends(get_db),
) -> RedirectResponse:
    """Record a human review decision from the operator page."""

    reviewer_name = reviewer.strip()
    if not reviewer_name:
        raise HTTPException(status_code=422, detail="Informe o revisor")
    try:
        selected_decision = ReviewDecision(decision)
    except ValueError:
        raise HTTPException(status_code=422, detail="Decisão de revisão inválida") from None
    lead = await db.get(Lead, lead_id)
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead não encontrado")
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
    return RedirectResponse(f"/leads/{lead_id}?review=ok", status_code=303)


@router.get("/crawls")
async def crawls_page(request: Request, db: AsyncSession = Depends(get_db)):
    views, has_active = await _crawl_views(db)
    progress_views, llm_status = await _crawl_monitor_views(db)
    return templates.TemplateResponse(
        request,
        "crawls.html",
        {
            "crawls": views,
            "has_active": has_active,
            "monitored_crawls": progress_views,
            "monitor_has_active": any(item.active for item in progress_views),
            "llm_status": llm_status,
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
    summaries = await queue_statuses(
        db, get_settings(), run_ids=[run.id for run in runs], include_global=True
    )
    views = []
    for run in runs:
        view = monitor_view(run, _crawl_view(run), now=now)
        view.llm = summaries[run.id]
        views.append(view)
    return views, summaries[None]


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
async def crawls_table(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX fragment repolled while a crawl is still running or pending."""

    views, has_active = await _crawl_views(db)
    return templates.TemplateResponse(
        request, "_crawl_table.html", {"crawls": views, "has_active": has_active}
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


async def _crawl_views(db: AsyncSession) -> tuple[list[SimpleNamespace], bool]:
    rows = (
        await db.scalars(select(CrawlRun).order_by(CrawlRun.created_at.desc()).limit(200))
    ).all()
    views = [_crawl_view(run) for run in rows]
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
        view.can_continue = bool(
            batch
            and batch.status in {"limited", "failed", "suspended"}
            and batch.last_run_id == run.id
            and not view.active
        )
    return views, any(view.active for view in views)


@router.post("/crawls/run")
async def run_crawl_from_web(
    connector: str = Form("all"),
    uf: str = Form("MA"),
    days: int = Form(7),
    batch_size: int = Form(30),
    process_documents: bool = Form(False),
    force_refresh: bool = Form(False),
) -> RedirectResponse:
    """Start a default crawl from the dashboard without exposing JSON details."""

    request = PipelineRequest(
        connector=connector,
        uf=uf,
        days=days,
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
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "settings": get_settings().public_view(),
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

    set_scheduler_paused(True)
    return RedirectResponse("/settings?scheduler=pausado", status_code=303)


@router.post("/settings/resume-scheduler")
async def resume_scheduler(db: AsyncSession = Depends(get_db)) -> RedirectResponse:
    """Resume the automatic collection jobs."""

    set_scheduler_paused(False)
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
