"""HTML dossier fragments. All membership decisions live in the read service."""

from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes.company_overview import RescoreRequest, rescore
from app.api.routes.leads import generate_outreach, review_lead
from app.dependencies import get_db
from app.schemas import LeadReviewRequest, OutreachRequest
from app.services.company_overview import (
    SECTION_LABELS,
    CompanyOverviewService,
    Section,
    find_company,
)

router = APIRouter(include_in_schema=False)

LABELS = {
    "verification_status": "Verificação da atribuição",
    "verification_reason": "Motivo da verificação",
    "event_type": "Evento",
    "reason_category": "Categoria do motivo",
    "raw_description": "Descrição oficial",
    "normalized_reason": "Motivo",
    "reason_summary": "Motivo",
    "recommended_action": "Ação recomendada",
    "lead_status": "Situação do lead",
    "score": "Pontuação",
    "scoring_version": "Versão da pontuação",
    "score_breakdown": "Explicação da pontuação",
    "urgency_score": "Urgência",
    "evidence_score": "Evidência",
    "legal_relevance_score": "Relevância do evento",
    "contact_score": "Contato",
    "economic_value_score": "Valor econômico",
    "current_phase": "Fase estimada (auxílio operacional)",
    "procurement_type": "Tipo de contratação",
    "modality": "Modalidade",
    "title": "Descrição",
    "participation_role": "Papel documentado",
    "confidence": "Confiança",
    "requires_manual_review": "Exige revisão humana",
    "explicit_deadline_at": "Prazo publicado",
    "estimated_deadline_at": "Prazo estimado, confirmar",
    "deadline_type": "Tipo de prazo",
    "calculation_explanation": "Explicação do prazo",
    "legal_basis": "Base informada",
    "phase": "Fase financeira",
    "match_status": "Conciliação",
    "match_reason": "Motivo da conciliação",
    "contact_type": "Tipo de contato",
    "contact_value": "Contato corporativo",
    "verified_at": "Verificado em",
    "occurred_at": "Data do evento",
    "created_at": "Registrado em",
    "updated_at": "Atualizado no sistema",
    "source": "Fonte",
    "source_url": "Origem",
    "collected_at": "Consultado em",
    "endpoint": "Operação consultada",
    "extraction_status": "Extração",
    "extraction_error": "Diagnóstico da extração",
    "text_excerpt": "Trecho da evidência",
    "page_number": "Página",
    "document_type": "Tipo documental",
    "original_url": "Endereço recebido",
    "resolved_url": "Endereço público validado",
    "is_active": "Documento ativo",
    "deactivated_at": "Desativado em",
    "decision": "Decisão humana",
    "reviewer": "Revisor",
    "reviewed_at": "Revisado em",
    "notes": "Observações",
    "previous_status": "Situação anterior",
    "subject": "Assunto",
    "message": "Rascunho",
    "approved": "Aprovado",
    "sent": "Enviado",
    "previous_score": "Pontuação anterior",
    "previous_version": "Versão anterior",
    "calculated_at": "Calculado em",
    "components": "Componentes da pontuação",
    "origin": "Origem da avaliação",
    "publication_at": "Publicação",
    "proposal_start_at": "Início das propostas",
    "proposal_end_at": "Fim das propostas",
    "session_start_at": "Início da sessão",
    "purchase_number": "Número da contratação",
    "purchase_year": "Ano",
    "uf": "UF",
    "municipality": "Município",
    "is_current": "Representação financeira atual",
    "awarded": "Adjudicada",
    "winner": "Vencedora",
    "disqualified": "Desclassificada",
    "ineligible": "Inabilitada",
    "participant": "Participante",
    "new": "Novo",
    "pending_review": "Pendente de revisão",
    "below_threshold": "Abaixo do limite",
    "rejected": "Rejeitado",
    "needs_changes": "Solicita ajustes",
    "archived": "Arquivado",
    "verified": "Verificado",
    "discovered": "Localizado",
    "requires_review": "Exige revisão",
    "invalid": "Inválido",
    "urgency": "Urgência",
    "evidence": "Evidência",
    "legal_relevance": "Relevância do evento",
    "contact": "Contato",
    "economic_value": "Valor econômico",
    "penalties": "Reduções",
    "status": "Situação publicada",
}


def label(value):
    from app.api.routes.history import FIELD_LABELS
    from app.web import _EVENT_TYPE_LABELS, _PARTICIPANT_STATUS_LABELS, _REASON_CATEGORY_LABELS

    for mapping in (_EVENT_TYPE_LABELS, _REASON_CATEGORY_LABELS, _PARTICIPANT_STATUS_LABELS):
        if str(value) in mapping:
            return mapping[str(value)]
    return LABELS.get(str(value), FIELD_LABELS.get(str(value), str(value).replace("_", " ")))


def visible(key):
    # Operational internals remain in the API or the explicit source references.
    return key not in {
        "id",
        "section",
        "kind",
        "title",
        "groups",
        "origins",
        "historical_notice",
        "detail_page",
        "inputs",
        "references",
        "request_id",
        "fingerprint",
        "source_record_id",
        "evidence_id",
        "is_personal",
        "is_corporate",
        "fit_score",
        "remaining_seconds",
        "file_hash",
        "response_hash",
        "local_path",
        "extraction_attempts",
        "activity_revision",
        "mime_type",
        "file_size",
        "next_attempt_at",
        "last_checked_at",
        "downloaded_at",
        "extracted_text",
    } and not key.endswith("_id")


def safe_url(value):
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(str(value))
        return (
            parsed.scheme.lower() in {"https", "http"}
            and bool(parsed.hostname)
            and not parsed.username
            and not parsed.password
        )
    except ValueError:
        return False


def display_value(value, key):
    from app.web import _brl

    if key == "verification_status":
        return {
            "confirmed": "Comprovada",
            "review": "Revisão necessária",
            "invalidated": "Invalidada",
            "legacy": "Legado não revalidado",
        }.get(str(value), value)

    if key.endswith("cnpj"):
        from app.web import _format_cnpj

        return _format_cnpj(str(value))
    from app.web import (
        _CONTACT_TYPE_LABELS,
        _DEADLINE_STATUS_LABELS,
        _EXTRACTION_STATUS_LABELS,
        _PROCUREMENT_PHASE_LABELS,
    )

    contextual = {
        "contact_type": _CONTACT_TYPE_LABELS,
        "extraction_status": _EXTRACTION_STATUS_LABELS,
        "current_phase": _PROCUREMENT_PHASE_LABELS,
    }
    if key in contextual:
        return contextual[key].get(str(value), value)
    if key == "match_status":
        return {
            "unlinked": "Sem vínculo exato localizado",
            "linked": "Vínculo exato localizado",
            "ambiguous": "Vínculo ambíguo — revisar",
        }.get(str(value), value)
    if key == "status" and str(value) in _DEADLINE_STATUS_LABELS:
        return _DEADLINE_STATUS_LABELS[str(value)]

    if key in {
        "amount",
        "unit_value",
        "total_value",
        "estimated_value",
        "initial_value",
        "global_value",
        "accumulated_value",
        "value_delta",
        "final_value",
        "proposal_value",
    }:
        from decimal import Decimal

        return _brl(Decimal(str(value)))
    if key in {
        "event_type",
        "reason_category",
        "status",
        "lead_status",
        "decision",
        "previous_status",
        "participation_role",
        "status_code",
    }:
        return label(value)
    if key.endswith("_at") or key in {"valid_from", "valid_until", "new_valid_until"}:
        from datetime import datetime

        from app.web import _local_datetime

        try:
            return _local_datetime(datetime.fromisoformat(str(value)))
        except ValueError:
            return value
    return value


def render(request, template, context):
    from app.web import templates

    templates.env.filters.update(
        company_label=label,
        company_visible=visible,
        company_safe_url=safe_url,
        company_value=display_value,
    )
    return templates.TemplateResponse(request, template, context)


async def company_page(
    request: Request, cnpj: str, status: str | None, active_only: bool, db: AsyncSession
):
    company = await find_company(db, cnpj)
    service = CompanyOverviewService(db, company)
    overview = await service.overview()
    initial = await service.page("leads")
    return render(
        request,
        "company_detail.html",
        {
            "overview": overview,
            "company": company,
            "section_labels": SECTION_LABELS,
            "initial": initial,
            "status": status,
            "active_only": active_only,
            "request_id": str(uuid4()),
        },
    )


@router.get("/empresas/{cnpj}/summary")
async def company_summary(request: Request, cnpj: str, db: AsyncSession = Depends(get_db)):
    company = await find_company(db, cnpj)
    overview = await CompanyOverviewService(db, company).overview()
    return render(
        request,
        "_company_summary.html",
        {"company": company, "overview": overview, "section_labels": SECTION_LABELS},
    )


@router.get("/empresas/{cnpj}/sections/{section}")
async def company_section(
    request: Request,
    cnpj: str,
    section: Section,
    page: int = Query(1, ge=1),
    historical: bool = False,
    status: str | None = None,
    active_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    company = await find_company(db, cnpj)
    data = await CompanyOverviewService(db, company).page(
        section, page=page, historical=historical, status=status, active_only=active_only
    )
    return render(
        request,
        "_company_section.html"
        if request.headers.get("HX-Request")
        else "company_fragment_page.html",
        {
            "company": company,
            "data": data,
            "fragment": "_company_section.html",
            "request_id": str(uuid4()),
        },
    )


@router.get("/empresas/{cnpj}/sections/{section}/{identifier}")
async def company_detail(
    request: Request,
    cnpj: str,
    section: Section,
    identifier: UUID,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db),
):
    company = await find_company(db, cnpj)
    data = await CompanyOverviewService(db, company).detail(section, identifier, page=page)
    return render(
        request,
        "_company_detail.html"
        if request.headers.get("HX-Request")
        else "company_fragment_page.html",
        {
            "company": company,
            "item": data,
            "fragment": "_company_detail.html",
            "request_id": str(uuid4()),
        },
    )


@router.post("/empresas/{cnpj}/rescore")
async def company_rescore(
    request: Request,
    cnpj: str,
    request_id: UUID = Form(...),
    lead_ids: list[UUID] = Form(...),
    page: int = Form(1, ge=1),
    status: str = Form(""),
    historical: bool = Form(False),
    detail_lead: UUID | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    result = await rescore(cnpj, RescoreRequest(request_id=request_id, lead_ids=lead_ids), db)
    if not request.headers.get("HX-Request"):
        return RedirectResponse(f"/empresas/{cnpj}#company-leads", status_code=303)
    if detail_lead is not None:
        response = await company_detail(request, cnpj, "leads", detail_lead, 1, db)
        response.headers["HX-Trigger"] = "company-updated"
        return response
    company = await find_company(db, cnpj)
    data = await CompanyOverviewService(db, company).page(
        "leads", page=page, status=status or None, historical=historical
    )
    response = render(
        request,
        "_company_section.html",
        {"company": company, "data": data, "result": result, "request_id": str(uuid4())},
    )
    response.headers["HX-Trigger"] = "company-updated"
    return response


@router.post("/empresas/{cnpj}/leads/{identifier}/{action}")
async def company_lead_action(
    request: Request, cnpj: str, identifier: UUID, action: str, db: AsyncSession = Depends(get_db)
):
    company = await find_company(db, cnpj)
    await CompanyOverviewService(db, company).detail("leads", identifier)
    form = await request.form()
    from fastapi import HTTPException
    from pydantic import ValidationError

    try:
        if action == "review":
            await review_lead(identifier, LeadReviewRequest.model_validate(dict(form)), db)
        elif action == "draft":
            await generate_outreach(
                identifier, OutreachRequest(channel="email", force_regenerate=False), db
            )
        else:
            raise HTTPException(404, "Ação não encontrada")
    except ValidationError as exc:
        raise HTTPException(422, "Confira os campos da revisão") from exc
    if not request.headers.get("HX-Request"):
        return RedirectResponse(f"/empresas/{cnpj}#company-leads", status_code=303)
    response = await company_detail(request, cnpj, "leads", identifier, 1, db)
    response.headers["HX-Trigger"] = "company-updated"
    return response
