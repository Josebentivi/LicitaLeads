# ruff: noqa: E501

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.services.event_detection.types import EventType, ReasonCategory
from app.services.identifiers import (
    is_valid_cnpj,
    normalize_cnpj,
    normalize_company_name,
)

_CNPJ_TOKEN = re.compile(
    r"(?<![A-Z0-9])[A-Z0-9]{2}\.?[A-Z0-9]{3}\.?[A-Z0-9]{3}/?[A-Z0-9]{4}-?[0-9]{2}(?![A-Z0-9])",
    re.IGNORECASE | re.ASCII,
)


class LLMEvidenceError(ValueError):
    pass


class LLMEvidenceQuote(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    text: str = Field(min_length=8, max_length=2000)
    page: int | None = Field(default=None, ge=1)


class LLMEventAnalysis(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_type: EventType
    company_name: str | None = Field(default=None, max_length=300)
    company_cnpj: str | None = None
    item_number: str | None = Field(default=None, max_length=100)
    reason_summary: str | None = Field(default=None, max_length=1000)
    reason_category: ReasonCategory = ReasonCategory.UNKNOWN
    event_date: datetime | None = None
    evidence_quotes: list[LLMEvidenceQuote] = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)
    requires_manual_review: bool

    @field_validator("company_cnpj")
    @classmethod
    def validate_cnpj(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = normalize_cnpj(value)
        if not is_valid_cnpj(normalized):
            raise ValueError("invalid company CNPJ")
        return normalized

    @field_validator("event_date")
    @classmethod
    def require_event_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("event_date must include a timezone")
        return value

    @model_validator(mode="after")
    def cautious_confidence(self) -> LLMEventAnalysis:
        if self.confidence < 0.75 and not self.requires_manual_review:
            raise ValueError("low-confidence LLM findings require manual review")
        return self


@runtime_checkable
class LLMEventAnalyzer(Protocol):
    async def analyze(
        self, text: str, *, pages: dict[int, str] | None = None
    ) -> list[LLMEventAnalysis]: ...


class DisabledLLMEventAnalyzer:
    async def analyze(
        self, text: str, *, pages: dict[int, str] | None = None
    ) -> list[LLMEventAnalysis]:
        del text, pages
        return []


LLMCallable = Callable[[str, dict[int, str] | None], Awaitable[list[dict[str, Any]]]]


def _quote_key(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().casefold()


class EvidenceBoundLLMEventAnalyzer:
    """Validate provider output and reject every quote absent from the source."""

    def __init__(self, provider: LLMCallable) -> None:
        self.provider = provider

    async def analyze(
        self, text: str, *, pages: dict[int, str] | None = None
    ) -> list[LLMEventAnalysis]:
        raw_items = await self.provider(text, pages)
        return self.validate(raw_items, text, pages=pages)

    @staticmethod
    def validate(
        raw_items: list[dict[str, Any]], text: str, *, pages: dict[int, str] | None = None
    ) -> list[LLMEventAnalysis]:
        analyses = [LLMEventAnalysis.model_validate(item) for item in raw_items]
        # The programmatic open-process trigger owns this event type; a model
        # must never fabricate it (it has no active-process guard).
        analyses = [
            analysis
            for analysis in analyses
            if analysis.event_type is not EventType.PARTICIPATION_DETECTED
        ]
        full_text = _quote_key(text)
        source_names = normalize_company_name(text, strip_legal_suffix=False) or ""
        # Normalize complete tokens only, never join unrelated text fragments.
        source_cnpjs = {
            candidate
            for match in _CNPJ_TOKEN.finditer(text)
            if (candidate := normalize_cnpj(match.group())) and is_valid_cnpj(candidate)
        }
        page_text = {page: _quote_key(value) for page, value in (pages or {}).items()}
        checked: list[LLMEventAnalysis] = []
        for analysis in analyses:
            quotes: list[str] = []
            for quote in analysis.evidence_quotes:
                needle = _quote_key(quote.text)
                if quote.page is not None and pages is not None and quote.page not in pages:
                    raise LLMEvidenceError("LLM evidence page is absent from source")
                haystack = (
                    page_text.get(quote.page, "") if quote.page is not None and pages else full_text
                )
                if needle not in haystack:
                    raise LLMEvidenceError(
                        "LLM evidence quote is absent from source"
                        + (f" page {quote.page}" if quote.page else "")
                    )
                quotes.append(quote.text)
            quote_text = " ".join(quotes)
            if analysis.company_cnpj and analysis.company_cnpj not in source_cnpjs:
                raise LLMEvidenceError("LLM company CNPJ is absent from source")
            if analysis.company_name:
                normalized_name = normalize_company_name(analysis.company_name)
                if normalized_name and normalized_name not in source_names:
                    raise LLMEvidenceError("LLM company name is absent from source")
            update: dict[str, Any] = {}
            if analysis.reason_summary:
                normalized_reason = _quote_key(analysis.reason_summary)
                if normalized_reason and normalized_reason not in _quote_key(quote_text):
                    update["reason_summary"] = None
            if analysis.item_number and analysis.item_number not in quote_text:
                update["item_number"] = None
            if analysis.event_date is not None:
                candidates = {
                    analysis.event_date.strftime("%d/%m/%Y"),
                    analysis.event_date.strftime("%Y-%m-%d"),
                }
                if not any(value in quote_text for value in candidates):
                    update["event_date"] = None
            checked.append(analysis.model_copy(update=update) if update else analysis)
        return checked
