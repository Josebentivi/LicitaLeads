# ruff: noqa: E501

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.lead_scoring import LeadScoreInput, LeadScorer
from app.services.llm import EvidenceBoundLLMEventAnalyzer, LLMEvidenceError
from app.services.outreach import OutreachContext, OutreachDraftService


def test_lead_score_is_explainable_and_applies_penalties() -> None:
    scorer = LeadScorer()
    high = scorer.score(
        LeadScoreInput(
            event_type="INELIGIBLE",
            deadline_status="DUE_WITHIN_24H",
            evidence_confidence=0.95,
            has_direct_evidence=True,
            contact_types=("licitacao_email",),
            estimated_value=Decimal("180000"),
        )
    )
    assert high.score == 95
    assert high.lead_status(50) == "new"
    assert high.breakdown["urgency"].startswith("30/30")

    weak = scorer.score(
        LeadScoreInput(
            event_type="UNKNOWN",
            evidence_confidence=0.2,
            company_confirmed=False,
            cnpj_present=False,
            reason_known=False,
            deadline_estimated=True,
            ocr_pending=True,
            deadline_expired=True,
        )
    )
    assert weak.score == 0
    assert weak.lead_status(50) == "below_threshold"
    assert "OCR pendente" in weak.breakdown["penalties"]


def _outreach_context(*, estimated: bool = True) -> OutreachContext:
    return OutreachContext(
        company_name="ABC Equipamentos Ltda.",
        procurement="Pregão Eletrônico 14/2026",
        agency="Prefeitura Municipal de Exemplo",
        event="inabilitação",
        reason="Ausência da certidão do item 8.4.",
        deadline="prazo estimado em 18/09/2026 às 23:59",
        source_label="Ata da sessão — página 17",
        source_url="https://example.gov.br/ata.pdf",
        sender_name="Nome",
        law_firm="Escritório",
        sender_contact="contato@example.com",
        deadline_estimated=estimated,
    )


def test_outreach_is_draft_only_idempotent_and_warns_on_estimate() -> None:
    service = OutreachDraftService()
    first = service.generate(_outreach_context())
    second = service.generate(_outreach_context())
    assert first.idempotency_key == second.idempotency_key
    assert "deverá ser confirmado" in first.message
    assert not first.approved and not first.sent
    with pytest.raises(ValueError, match="draft_only"):
        OutreachDraftService(mode="send")


@pytest.mark.asyncio
async def test_llm_analysis_requires_literal_evidence() -> None:
    async def provider(text: str, pages: dict[int, str] | None) -> list[dict]:
        del text, pages
        return [
            {
                "event_type": "INELIGIBLE",
                "company_name": "ABC Ltda.",
                "company_cnpj": "04.252.011/0001-10",
                "item_number": "4",
                "reason_summary": "Certidão ausente.",
                "reason_category": "MISSING_DOCUMENT",
                "event_date": "2026-09-15T14:30:00-03:00",
                "evidence_quotes": [
                    {"text": "foi inabilitada por não apresentar a certidão", "page": 12}
                ],
                "confidence": 0.91,
                "requires_manual_review": False,
            }
        ]

    analyzer = EvidenceBoundLLMEventAnalyzer(provider)
    source = (
        "A licitante ABC Ltda., CNPJ 04.252.011/0001-10, "
        "foi inabilitada por não apresentar a certidão exigida."
    )
    result = await analyzer.analyze(source, pages={12: source})
    assert result[0].company_cnpj == "04252011000110"


@pytest.mark.asyncio
async def test_llm_hallucinated_quote_is_rejected() -> None:
    async def provider(text: str, pages: dict[int, str] | None) -> list[dict]:
        del text, pages
        return [
            {
                "event_type": "DISQUALIFIED",
                "reason_category": "UNKNOWN",
                "evidence_quotes": [{"text": "trecho que não existe no documento"}],
                "confidence": 0.5,
                "requires_manual_review": True,
            }
        ]

    with pytest.raises(LLMEvidenceError):
        await EvidenceBoundLLMEventAnalyzer(provider).analyze("texto oficial")
