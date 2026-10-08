# ruff: noqa: E501

"""OpenAI-compatible provider parsing and analyzer selection."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.config import Settings
from app.connectors.http import ConnectorDecodeError
from app.services.llm import (
    DisabledLLMEventAnalyzer,
    EvidenceBoundLLMEventAnalyzer,
    OpenAIChatLLMProvider,
    build_event_analyzer,
)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "test",
        "llm_enabled": True,
        "llm_provider": "openai",
        "llm_api_key": "sk-test",
        "llm_model": "gpt-5.6-luna",
    }
    values.update(overrides)
    return Settings(**values)


@respx.mock
@pytest.mark.asyncio
async def test_provider_sends_bearer_and_parses_analyses() -> None:
    route = respx.post("https://api.openai.com/v1/chat/completions").mock(
        return_value=httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({"analyses": [{"event_type": "INELIGIBLE"}]})
                        }
                    }
                ]
            },
        )
    )
    provider = OpenAIChatLLMProvider(_settings())
    try:
        result = await provider("texto", {1: "texto"})
    finally:
        await provider.aclose()

    assert result == [{"event_type": "INELIGIBLE"}]
    assert route.called
    request = route.calls[0].request
    assert request.headers["authorization"] == "Bearer sk-test"
    assert json.loads(request.content)["model"] == "gpt-5.6-luna"


@respx.mock
@pytest.mark.asyncio
async def test_provider_rejects_non_json_content() -> None:
    respx.post("https://api.openai.com/v1/chat/completions").mock(
        return_value=httpx.Response(200, json={"choices": [{"message": {"content": "not-json"}}]})
    )
    provider = OpenAIChatLLMProvider(_settings())
    try:
        with pytest.raises(ConnectorDecodeError):
            await provider("texto")
    finally:
        await provider.aclose()


def test_factory_selects_analyzer_by_configuration() -> None:
    assert isinstance(build_event_analyzer(_settings(llm_enabled=False)), DisabledLLMEventAnalyzer)
    assert isinstance(build_event_analyzer(_settings()), EvidenceBoundLLMEventAnalyzer)


def test_llm_prompt_excludes_programmatic_participation_event() -> None:
    """The open-process trigger owns PARTICIPATION_DETECTED; the model must not."""

    from app.services.llm import providers

    assert "PARTICIPATION_DETECTED" not in providers._EVENT_TYPES
    assert "INELIGIBLE" in providers._EVENT_TYPES


@pytest.mark.asyncio
async def test_analyzer_drops_programmatic_participation_event() -> None:
    quote = "Empresa Exemplo Ltda. participou do processo."

    async def provider(text: str, pages: dict[int, str] | None):
        del text, pages
        return [
            {
                "event_type": "PARTICIPATION_DETECTED",
                "evidence_quotes": [{"text": quote}],
                "confidence": 0.9,
                "requires_manual_review": False,
            },
            {
                "event_type": "INELIGIBLE",
                "evidence_quotes": [{"text": quote}],
                "confidence": 0.9,
                "requires_manual_review": False,
            },
        ]

    analyzer = EvidenceBoundLLMEventAnalyzer(provider)
    analyses = await analyzer.analyze(quote)

    assert [analysis.event_type.value for analysis in analyses] == ["INELIGIBLE"]
