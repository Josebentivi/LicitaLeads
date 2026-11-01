"""OpenAI-compatible chat provider for evidence-bound event analysis.

The provider only transports text to the model and parses its JSON answer.  It
never trusts model output: :class:`EvidenceBoundLLMEventAnalyzer` still rejects
any quote, CNPJ or company name that is absent from the source document.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from app.config import Settings
from app.connectors.http import AsyncHTTPClient, ConnectorDecodeError, ConnectorHTTPError
from app.services.event_detection.types import EventType, ReasonCategory

logger = logging.getLogger(__name__)
PROMPT_VERSION = "evidence-bound-2"


@dataclass(frozen=True)
class LLMReply:
    analyses: list[dict[str, Any]] = field(default_factory=list)
    input_tokens: int | None = None
    output_tokens: int | None = None
    model: str | None = None
    diagnostic: dict[str, Any] = field(default_factory=dict)
    transient: bool = False
    systemic: bool = False
    retry_seconds: float = 0


def _safe_identifier(value: Any, secret: str) -> str | None:
    if not isinstance(value, str) or (secret and secret in value):
        return None
    return value if re.fullmatch(r"[A-Za-z0-9_.:/\[\]-]{1,120}", value) else None


def _tokens(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


_EVENT_TYPES = ", ".join(
    member.value for member in EventType if member is not EventType.PARTICIPATION_DETECTED
)
_REASON_CATEGORIES = ", ".join(member.value for member in ReasonCategory)

_SYSTEM_PROMPT = f"""Você é um extrator de fatos de documentos oficiais de licitação pública.
Analise o TEXTO fornecido e devolva SOMENTE um objeto JSON válido no formato:
{{"analyses": [{{"event_type": "...", "company_name": null, "company_cnpj": null,
"item_number": null, "reason_summary": null, "reason_category": "UNKNOWN",
"event_date": null, "evidence_quotes": [{{"text": "...", "page": null}}],
"confidence": 0.0, "requires_manual_review": true}}]}}

Regras obrigatórias:
- Extraia apenas fatos explicitamente presentes no texto. Nunca invente nada.
- event_type deve ser um destes valores: {_EVENT_TYPES}.
- reason_category deve ser um destes valores: {_REASON_CATEGORIES}.
- evidence_quotes.text deve ser um trecho LITERAL copiado do texto (sem reescrever).
- event_date, quando existir no texto, deve ser ISO 8601 com fuso (ex.: 2026-09-15T14:30:00-03:00).
- confidence entre 0.0 e 1.0; se for menor que 0.75, requires_manual_review deve ser true.
- Se não houver fato relevante, devolva {{"analyses": []}}."""


def _user_prompt(text: str, page: int | None) -> str:
    location = f"página {page}" if page is not None else "trecho sem paginação"
    return f"Texto ({location}):\n\n{text}"


class OpenAIChatLLMProvider:
    """Call an OpenAI-compatible ``/chat/completions`` endpoint with JSON output."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: AsyncHTTPClient | None = None,
    ) -> None:
        self.settings = settings
        self._endpoint = f"{settings.llm_base_url.rstrip('/')}/chat/completions"
        self._client = client or AsyncHTTPClient(
            {
                "http_timeout_seconds": settings.llm_timeout_seconds,
                "http_max_retries": 0,  # Durable queue owns all retries and accounting.
                "http_max_concurrency": 1,
                "http_min_request_interval_seconds": 0,
                "http_user_agent": settings.http_user_agent,
            }
        )
        self._owns_client = client is None

    async def __call__(
        self,
        text: str,
        pages: dict[int, str] | None = None,
    ) -> list[dict[str, Any]]:
        reply = await self.request(text, pages)
        if reply.diagnostic.get("error"):
            raise ConnectorDecodeError(
                "LLM request rejected: " + str(reply.diagnostic.get("code", "response"))
            )
        return reply.analyses

    async def request(self, text: str, pages: dict[int, str] | None = None) -> LLMReply:
        page = next(iter(pages)) if pages else None
        body: dict[str, Any] = {
            "model": self.settings.llm_model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _user_prompt(text, page)},
            ],
            "max_completion_tokens": self.settings.llm_max_output_tokens,
            "response_format": {"type": "json_object"},
        }
        try:
            response = await self._client.request_json(
                "POST",
                self._endpoint,
                json_body=body,
                headers={"Authorization": f"Bearer {self.settings.llm_api_key}"},
            )
        except ConnectorHTTPError as exc:
            try:
                parsed_error = json.loads(exc.body)
                error = parsed_error.get("error", {}) if isinstance(parsed_error, dict) else {}
            except ValueError:
                error = {}
            if not isinstance(error, dict):
                error = {}
            code = _safe_identifier(error.get("code"), self.settings.llm_api_key)
            diagnostic = {
                "error": True,
                "status_http": exc.status_code,
                "code": code,
                "type": _safe_identifier(error.get("type"), self.settings.llm_api_key),
                "param": _safe_identifier(error.get("param"), self.settings.llm_api_key),
                "request_id": _safe_identifier(
                    exc.headers.get("x-request-id"), self.settings.llm_api_key
                ),
            }
            input_error = code in {
                "context_length_exceeded",
                "string_above_max_length",
                "invalid_image",
            }
            systemic = (
                exc.status_code in {400, 401, 403, 404, 422} and not input_error
            ) or code in {"insufficient_quota", "billing_hard_limit_reached", "billing_not_active"}
            delay = AsyncHTTPClient._parse_retry_after(exc.headers.get("retry-after", "")) or 0
            return LLMReply(
                diagnostic=diagnostic,
                systemic=systemic,
                transient=not systemic and exc.status_code in {408, 429, 500, 502, 503, 504},
                retry_seconds=delay,
            )
        except (httpx.InvalidURL, httpx.UnsupportedProtocol):
            return LLMReply(diagnostic={"error": True, "code": "invalid_endpoint"}, systemic=True)
        except Exception:
            # Never serialize exceptions: URLs, credentials or submitted text
            # can be echoed by a transport/provider exception.
            return LLMReply(diagnostic={"error": True, "code": "transport_error"}, transient=True)
        payload = response.payload if isinstance(response.payload, dict) else {}
        usage = payload.get("usage")
        usage = usage if isinstance(usage, dict) else {}
        metadata: dict[str, Any] = dict(
            input_tokens=_tokens(usage.get("prompt_tokens")),
            output_tokens=_tokens(usage.get("completion_tokens")),
            model=_safe_identifier(payload.get("model"), self.settings.llm_api_key),
        )
        diagnostic = {
            "status_http": response.status_code,
            "request_id": _safe_identifier(
                response.headers.get("x-request-id"), self.settings.llm_api_key
            ),
        }
        try:
            choices = payload.get("choices")
            choice = choices[0] if isinstance(choices, list) and choices else {}
            if not isinstance(choice, dict) or choice.get("finish_reason") not in {"stop"}:
                reason = choice.get("finish_reason") if isinstance(choice, dict) else None
                return LLMReply(
                    **metadata,
                    diagnostic={
                        **diagnostic,
                        "error": True,
                        "code": "output_truncated" if reason == "length" else "incomplete_response",
                        "finish_reason": _safe_identifier(reason, self.settings.llm_api_key),
                    },
                )
            content = self._content(payload)
            parsed = json.loads(content)
            analyses = parsed.get("analyses") if isinstance(parsed, dict) else None
            if not isinstance(analyses, list) or any(
                not isinstance(item, dict) for item in analyses
            ):
                raise ConnectorDecodeError("invalid_schema")
        except (ValueError, ConnectorDecodeError):
            return LLMReply(
                **metadata,
                diagnostic={**diagnostic, "error": True, "code": "invalid_or_incomplete_response"},
            )
        return LLMReply(analyses=analyses, **metadata, diagnostic=diagnostic)

    @staticmethod
    def _content(payload: Any) -> str:
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if not isinstance(choices, list) or not choices:
            raise ConnectorDecodeError("LLM response has no choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise ConnectorDecodeError("LLM response has no text content")
        return content

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
