"""Provider receipts retain usage while exposing only safe diagnostics."""

import json

import httpx
import pytest
import respx

from app.config import Settings
from app.services.llm.providers import OpenAIChatLLMProvider


def settings():
    return Settings(
        llm_enabled=True,
        llm_provider="openai",
        llm_api_key="sk-private-test",
        llm_model="gpt-6-luna",
        llm_max_output_tokens=2000,
    )


@respx.mock
@pytest.mark.parametrize(
    "status,code,systemic,transient",
    [
        (400, "unsupported_parameter", True, False),
        (401, "invalid_api_key", True, False),
        (403, None, True, False),
        (404, "model_not_found", True, False),
        (429, "insufficient_quota", True, False),
        (429, "rate_limit_exceeded", False, True),
        (503, None, False, True),
        (400, "context_length_exceeded", False, False),
    ],
)
async def test_classifies_failures_without_transport_retries(status, code, systemic, transient):
    route = respx.post("https://api.openai.com/v1/chat/completions").mock(
        return_value=httpx.Response(
            status,
            headers={"retry-after": "60", "x-request-id": "req_safe"},
            json={
                "error": {
                    "code": code,
                    "type": "invalid_request_error",
                    "param": "max_tokens",
                    "message": "sk-private-test submitted secret text",
                }
            },
        )
    )
    provider = OpenAIChatLLMProvider(settings())
    try:
        receipt = await provider.request("documento oficial")
    finally:
        await provider.aclose()
    assert receipt.systemic is systemic and receipt.transient is transient
    assert route.call_count == 1 and receipt.retry_seconds >= 59
    assert receipt.diagnostic["request_id"] == "req_safe"
    assert receipt.diagnostic["param"] == "max_tokens"
    assert "private" not in json.dumps(receipt.diagnostic) and "secret text" not in json.dumps(
        receipt.diagnostic
    )


@respx.mock
@pytest.mark.parametrize(
    "finish,content,valid",
    [
        ("stop", '{"analyses":[]}', True),
        ("length", '{"analyses":[]}', False),
        ("stop", "bad json", False),
        ("stop", '{"analyses":[0]}', False),
    ],
)
async def test_usage_survives_bad_output_without_double_counting_reasoning(finish, content, valid):
    respx.post("https://api.openai.com/v1/chat/completions").mock(
        return_value=httpx.Response(
            200,
            json={
                "model": "gpt-6-luna",
                "choices": [{"finish_reason": finish, "message": {"content": content}}],
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "completion_tokens_details": {"reasoning_tokens": 40},
                },
            },
        )
    )
    provider = OpenAIChatLLMProvider(settings())
    try:
        reply = await provider.request("documento")
    finally:
        await provider.aclose()
    assert reply.input_tokens == 100 and reply.output_tokens == 50
    assert bool(reply.diagnostic.get("error")) is not valid


@respx.mock
async def test_missing_usage_is_unknown_and_secret_is_redacted_in_all_fields():
    respx.post("https://api.openai.com/v1/chat/completions").mock(
        return_value=httpx.Response(
            400,
            headers={"x-request-id": "sk-private-test"},
            json={
                "error": {
                    "code": "sk-private-test",
                    "type": "sk-private-test",
                    "param": "sk-private-test",
                }
            },
        )
    )
    provider = OpenAIChatLLMProvider(settings())
    try:
        reply = await provider.request("documento")
    finally:
        await provider.aclose()
    assert reply.input_tokens is None and reply.output_tokens is None
    assert "sk-private-test" not in json.dumps(reply.diagnostic)
