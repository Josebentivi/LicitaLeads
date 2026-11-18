"""Local-only, instance-bound shutdown forms and status endpoints."""

from __future__ import annotations

import ipaddress
import secrets

from fastapi import APIRouter, Form, HTTPException, Request
from pydantic import BaseModel

from app.runtime_control import RuntimeChannel
from app.security import _is_loopback_host, _origin_matches_host

router = APIRouter(tags=["application"])


def shutdown_available() -> bool:
    channel = RuntimeChannel.from_environment()
    return bool(channel and channel.available)


def _local(request: Request) -> None:
    try:
        local = bool(request.client and ipaddress.ip_address(request.client.host).is_loopback)
    except ValueError:
        local = False
    if not local:
        raise HTTPException(403, "O encerramento está disponível somente nesta máquina")


def _channel(request: Request) -> RuntimeChannel:
    _local(request)
    channel = RuntimeChannel.from_environment()
    if channel is None or not channel.available:
        raise HTTPException(
            409, "Abra a aplicação pelo iniciar.bat para usar o encerramento completo"
        )
    return channel


def _authorize(request: Request, token: str) -> RuntimeChannel:
    channel = _channel(request)
    origin = request.headers.get("origin")
    host = request.headers.get("host", "")
    # Referrer-Policy: no-referrer can make same-origin browser forms opaque.
    # Match the existing form guard, still requiring loopback + instance token.
    opaque_local = (
        origin == "null"
        and request.headers.get("sec-fetch-site") == "same-origin"
        and _is_loopback_host(host)
    )
    if not opaque_local and (
        not origin or not _origin_matches_host(origin, host, scheme=request.url.scheme)
    ):
        raise HTTPException(403, "Origem de encerramento inválida")
    expected = channel.read("manifest").get("token")
    if not isinstance(expected, str) or not expected or not secrets.compare_digest(token, expected):
        raise HTTPException(403, "Confirmação de encerramento inválida; recarregue a página")
    return channel


class ShutdownRequest(BaseModel):
    token: str


@router.get("/api/application/shutdown")
async def shutdown_status(request: Request):
    _local(request)
    channel = RuntimeChannel.from_environment()
    return (
        channel.public_status()
        if channel
        else {"available": False, "phase": "unmanaged", "id": None}
    )


@router.post("/api/application/shutdown", status_code=202)
async def shutdown_api(request: Request, payload: ShutdownRequest):
    _authorize(request, payload.token)
    service = getattr(request.app.state, "shutdown", None)
    if service is None:
        raise HTTPException(409, "O controle de encerramento ainda não está pronto")
    return await service.request()


@router.get("/application/shutdown", include_in_schema=False)
async def shutdown_page(request: Request):
    from app.web import templates

    _local(request)
    channel = RuntimeChannel.from_environment()
    available = bool(channel and channel.available)
    return templates.TemplateResponse(
        request,
        "shutdown.html",
        {
            "available": available,
            "token": channel.read("manifest").get("token", "") if available and channel else "",
            "state": channel.public_status() if channel else {"phase": "unmanaged"},
        },
    )


@router.post("/application/shutdown", include_in_schema=False, status_code=202)
async def shutdown_form(request: Request, token: str = Form(...)):
    from app.web import templates

    _authorize(request, token)
    service = getattr(request.app.state, "shutdown", None)
    if service is None:
        raise HTTPException(409, "O controle de encerramento ainda não está pronto")
    state = await service.request()
    return templates.TemplateResponse(
        request,
        "shutdown.html",
        {
            "available": True,
            "state": state,
            "token": "",
        },
        status_code=202,
    )
