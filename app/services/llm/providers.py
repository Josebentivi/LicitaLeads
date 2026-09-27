"""OpenAI-compatible chat provider for evidence-bound event analysis.

The provider only transports text to the model and parses its JSON answer.  It
never trusts model output: :class:`EvidenceBoundLLMEventAnalyzer` still rejects
any quote, CNPJ or company name that is absent from the source document.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.config import Settings
from app.connectors.http import AsyncHTTPClient, ConnectorDecodeError
from app.services.event_detection.types import EventType, ReasonCategory

logger = logging.getLogger(__name__)

_EVENT_TYPES = ", ".join(member.value for member in EventType)
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
                "http_max_retries": settings.llm_max_retries,
                "http_max_concurrency": 1,
                "http_user_agent": settings.http_user_agent,
            }
        )
        self._owns_client = client is None

    async def __call__(
        self,
        text: str,
        pages: dict[int, str] | None = None,
    ) -> list[dict[str, Any]]:
        page = next(iter(pages)) if pages else None
        body: dict[str, Any] = {
            "model": self.settings.llm_model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _user_prompt(text, page)},
            ],
            "temperature": 0,
            "max_tokens": self.settings.llm_max_output_tokens,
            "response_format": {"type": "json_object"},
        }
        response = await self._client.request_json(
            "POST",
            self._endpoint,
            json_body=body,
            headers={"Authorization": f"Bearer {self.settings.llm_api_key}"},
        )
        content = self._content(response.payload)
        try:
            parsed = json.loads(content)
        except ValueError as exc:
            raise ConnectorDecodeError("LLM response is not valid JSON") from exc
        analyses = parsed.get("analyses") if isinstance(parsed, dict) else None
        if not isinstance(analyses, list):
            raise ConnectorDecodeError("LLM response does not contain an analyses list")
        return [item for item in analyses if isinstance(item, dict)]

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
