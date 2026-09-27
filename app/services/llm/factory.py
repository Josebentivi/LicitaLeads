"""Select the event analyzer implementation according to configuration."""

from __future__ import annotations

from app.config import Settings
from app.services.llm.analyzer import (
    DisabledLLMEventAnalyzer,
    EvidenceBoundLLMEventAnalyzer,
    LLMEventAnalyzer,
)
from app.services.llm.providers import OpenAIChatLLMProvider


def build_event_analyzer(settings: Settings) -> LLMEventAnalyzer:
    """Return the evidence-bound LLM analyzer, or the no-op one when disabled."""

    if not settings.llm_enabled or settings.llm_provider == "none":
        return DisabledLLMEventAnalyzer()
    if settings.llm_provider == "openai":
        return EvidenceBoundLLMEventAnalyzer(OpenAIChatLLMProvider(settings))
    raise ValueError(f"unsupported LLM provider: {settings.llm_provider}")
