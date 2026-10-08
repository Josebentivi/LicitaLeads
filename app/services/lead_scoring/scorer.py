# ruff: noqa: E501

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class LeadScoreInput:
    event_type: str
    deadline_status: str = "UNKNOWN"
    hours_remaining: float | None = None
    evidence_confidence: float = 0.0
    has_direct_evidence: bool = False
    contact_types: tuple[str, ...] = ()
    estimated_value: Decimal | None = None
    company_confirmed: bool = True
    cnpj_present: bool = True
    reason_known: bool = True
    deadline_estimated: bool = False
    ocr_pending: bool = False
    process_closed: bool = False
    deadline_expired: bool = False


@dataclass(frozen=True, slots=True)
class LeadScoreResult:
    score: int
    urgency_score: int
    evidence_score: int
    legal_relevance_score: int
    contact_score: int
    economic_value_score: int
    penalties: int
    breakdown: Mapping[str, str]
    scoring_version: str = "1.0"

    def lead_status(self, minimum_score: int) -> str:
        return "new" if self.score >= minimum_score else "below_threshold"


_HIGH_RELEVANCE = {
    "INELIGIBLE",
    "DISQUALIFIED",
    "INTENT_TO_APPEAL",
    "APPEAL_SUBMITTED",
    "COUNTERARGUMENT_OPENED",
}


class LeadScorer:
    VERSION = "1.0"

    def score(self, value: LeadScoreInput) -> LeadScoreResult:
        urgency, urgency_reason = self._urgency(value)
        evidence = min(
            25,
            round(max(0.0, min(1.0, value.evidence_confidence)) * 20)
            + (5 if value.has_direct_evidence else 0),
        )
        legal = self._legal_relevance(value.event_type)
        contact, contact_reason = self._contact(value.contact_types)
        economic = self._economic_value(value.estimated_value)

        penalty_items: list[tuple[int, str]] = []
        if not value.company_confirmed:
            penalty_items.append((15, "empresa não confirmada"))
        if not value.cnpj_present:
            penalty_items.append((8, "CNPJ ausente"))
        if value.evidence_confidence < 0.5:
            penalty_items.append((10, "evidência fraca"))
        if not value.reason_known:
            penalty_items.append((6, "motivo desconhecido"))
        if value.deadline_estimated:
            penalty_items.append((5, "prazo estimado"))
        if value.ocr_pending:
            penalty_items.append((12, "OCR pendente"))
        if value.process_closed:
            penalty_items.append((15, "processo encerrado"))
        if value.deadline_expired or value.deadline_status.upper() == "EXPIRED":
            penalty_items.append((20, "prazo vencido"))
        penalties = sum(amount for amount, _ in penalty_items)
        total = max(0, min(100, urgency + evidence + legal + contact + economic - penalties))
        breakdown = MappingProxyType(
            {
                "urgency": f"{urgency}/30 — {urgency_reason}",
                "evidence": f"{evidence}/25 — confiança {value.evidence_confidence:.0%}; evidência direta: {'sim' if value.has_direct_evidence else 'não'}",
                "legal_relevance": f"{legal}/20 — evento {value.event_type.upper()}",
                "contact": f"{contact}/15 — {contact_reason}",
                "economic_value": f"{economic}/10 — valor {'não publicado' if value.estimated_value is None else str(value.estimated_value)}",
                "penalties": f"-{penalties} — "
                + (", ".join(reason for _, reason in penalty_items) or "nenhuma redução"),
            }
        )
        return LeadScoreResult(
            score=total,
            urgency_score=urgency,
            evidence_score=evidence,
            legal_relevance_score=legal,
            contact_score=contact,
            economic_value_score=economic,
            penalties=penalties,
            breakdown=breakdown,
            scoring_version=self.VERSION,
        )

    @staticmethod
    def _urgency(value: LeadScoreInput) -> tuple[int, str]:
        status = value.deadline_status.upper()
        if status == "EXPIRED" or value.deadline_expired:
            return 0, "prazo vencido"
        if status == "DUE_TODAY":
            return 30, "vence hoje"
        if status == "DUE_WITHIN_24H" or (
            value.hours_remaining is not None and 0 <= value.hours_remaining <= 24
        ):
            return 30, "vence em até 24 horas"
        if value.hours_remaining is not None and 0 <= value.hours_remaining <= 72:
            return 24, "vence em até 72 horas"
        if value.hours_remaining is not None and 0 <= value.hours_remaining <= 168:
            return 18, "vence em até 7 dias"
        if status == "OPEN":
            return 12, "prazo aberto"
        return 3, "prazo desconhecido ou exige revisão"

    @staticmethod
    def _legal_relevance(event_type: str) -> int:
        event = event_type.upper()
        if event in _HIGH_RELEVANCE:
            return 20
        if event in {"PROPOSAL_REJECTED", "COUNTERARGUMENT_SUBMITTED", "APPEAL_DECIDED"}:
            return 15
        if event in {
            "SESSION_SUSPENDED",
            "QUALIFIED",
            "PROPOSAL_ACCEPTED",
            "PARTICIPATION_DETECTED",
        }:
            return 8
        if event in {"WINNER_DECLARED", "ADJUDICATED", "HOMOLOGATED"}:
            return 4
        return 2

    @staticmethod
    def _contact(contact_types: tuple[str, ...]) -> tuple[int, str]:
        types = {value.lower() for value in contact_types}
        if types & {"legal_email", "licitacao_email", "juridico_email"}:
            return 15, "e-mail jurídico/licitações confirmado"
        if "email" in types or "whatsapp" in types:
            return 13, "canal corporativo direto"
        if types & {"phone", "form"}:
            return 10, "telefone ou formulário corporativo"
        if types & {"website", "linkedin"}:
            return 4, "somente presença web corporativa"
        return 0, "nenhum contato corporativo"

    @staticmethod
    def _economic_value(value: Decimal | None) -> int:
        if value is None or value < 0:
            return 2
        if value >= Decimal("1000000"):
            return 10
        if value >= Decimal("200000"):
            return 8
        if value >= Decimal("50000"):
            return 6
        return 3
