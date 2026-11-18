"""HTML AI console, backed by the same read/action services as the API."""

from typing import Literal
from urllib.parse import urlencode, urlsplit
from uuid import UUID

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes import llm
from app.company_web import label as event_label
from app.config import get_settings
from app.dependencies import get_db
from app.services.llm import central

router = APIRouter(include_in_schema=False)
TABS = {
    "summary": "Resumo",
    "documents": "Documentos e análises",
    "suggestions": "Sugestões para revisão",
    "cost": "Consumo",
    "configuration": "Configuração",
}
STATUS_LABELS = {
    "pending": "Pendente",
    "running": "Em andamento",
    "succeeded": "Concluída e validada",
    "failed": "Falha",
    "cancelled": "Cancelada",
    "obsolete": "Obsoleta",
    "confirmed": "Confirmada",
    "rejected": "Rejeitada",
    "extracted": "Extraído",
    "unsupported": "Formato não suportado",
    "ocr_required": "OCR necessário",
    "blocked": "Bloqueada",
    "reused": "Reutilizada",
}


@router.get("/ia")
async def home(
    request: Request,
    tab: Literal["summary", "documents", "suggestions", "cost", "configuration"] = "summary",
    page: int = Query(default=1, ge=1),
    params: dict[str, str] = Depends(llm.filters),
    db: AsyncSession = Depends(get_db),
):
    from app.web import templates

    summary = await central.overview(db, get_settings())
    data = (
        await central.documents(db, params, page)
        if tab == "documents"
        else await central.suggestions(db, params, page)
        if tab == "suggestions"
        else None
    )
    query = {"tab": tab, **{k: v for k, v in params.items() if v}}
    url = "/ia?" + urlencode({**query, "page": page})
    queue = summary["queue"] or {}
    active = bool(
        queue.get("running")
        or (
            summary["configuration"]["enabled"]
            and queue.get("pending", 0) > queue.get("configuration_changed", 0)
            and not summary["controls"]["llm"]
            and not queue.get("blocked")
        )
    )
    context = {
        "summary": summary,
        "event_label": event_label,
        "data": data,
        "tab": tab,
        "tabs": TABS,
        "tab_urls": {
            key: "/ia?"
            + urlencode({**{k: v for k, v in params.items() if v and k != "status"}, "tab": key})
            for key in TABS
        },
        "filters": params,
        "url": url,
        "active": active,
        "previous_url": "/ia?" + urlencode({**query, "page": max(1, page - 1)}),
        "next_url": "/ia?" + urlencode({**query, "page": page + 1}),
        "message": request.query_params.get("message"),
    }
    return templates.TemplateResponse(
        request,
        "_ai_content.html" if request.headers.get("HX-Request") == "true" else "ai.html",
        context,
    )


@router.get("/ia/{kind}/{identifier}")
async def detail(
    request: Request,
    kind: Literal["documents", "jobs", "suggestions"],
    identifier: UUID,
    page: int = Query(default=1, ge=1),
    return_to: str = Query(default="", max_length=2000),
    db: AsyncSession = Depends(get_db),
):
    from app.web import templates

    try:
        data = (
            await central.document_detail(db, identifier, page)
            if kind == "documents"
            else await central.job_detail(db, identifier)
            if kind == "jobs"
            else await central.suggestion_detail(db, identifier)
        )
    except LookupError as exc:
        raise llm.action_error(exc) from None
    return templates.TemplateResponse(
        request,
        "ai_detail.html",
        {
            "data": data,
            "labels": STATUS_LABELS,
            "event_label": event_label,
            "kind": kind,
            "identifier": identifier,
            "page": page,
            "return_to": return_to
            if return_to and _redirect(return_to).headers["location"] == return_to
            else "/ia?"
            + urlencode({"tab": "suggestions" if kind == "suggestions" else "documents"}),
        },
    )


@router.post("/ia/controls/{component}")
async def control(
    component: Literal["collections", "llm"],
    paused: bool = Form(),
    return_to: str = Form(default="/ia"),
    db: AsyncSession = Depends(get_db),
):
    await llm.control(component, llm.ControlRequest(paused=paused), db)
    return _redirect(return_to)


def _redirect(destination: str) -> RedirectResponse:
    target = urlsplit(destination)
    return RedirectResponse(
        destination
        if not target.netloc
        and not target.scheme
        and target.path == "/ia"
        and (destination == "/ia" or destination.startswith("/ia?"))
        else "/ia",
        status_code=303,
    )


@router.post("/ia/{kind}/{identifier}/{action}")
async def action(
    kind: Literal["documents", "batches", "suggestions"],
    identifier: UUID,
    action: str,
    return_to: str = Form(default="/ia"),
    company_id: UUID | None = Form(default=None),
    reviewer: str | None = Form(default=None, max_length=200),
    note: str | None = Form(default=None, max_length=1000),
    db: AsyncSession = Depends(get_db),
):
    try:
        if kind == "documents" and action == "enqueue":
            await central.enqueue(db, identifier, get_settings())
        elif kind == "batches" and action in {"retry", "cancel"}:
            await central.batch_action(db, identifier, action, get_settings())
        elif kind == "suggestions" and action in {"confirm", "reject"}:
            await central.review_suggestion(
                db,
                identifier,
                confirm=action == "confirm",
                company_id=company_id,
                reviewer=reviewer,
                note=note,
                settings=get_settings(),
            )
        else:
            raise ValueError("Ação desconhecida")
        await db.commit()
    except (LookupError, ValueError, llm.LLMQueueBusy) as exc:
        raise llm.action_error(exc) from None
    return _redirect(return_to)
