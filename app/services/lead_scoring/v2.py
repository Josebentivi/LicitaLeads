"""Version 2 prioritizes time, documentary proof and adverse procedural acts."""

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from types import MappingProxyType

from .scorer import LeadScoreInput as LegacyInput
from .scorer import LeadScorer as LegacyScorer
from .scorer import LeadScoreResult


@dataclass(frozen=True, slots=True)
class LeadScoreInput(LegacyInput):
    verified_contact_types: tuple[str, ...] = ()
    pending_contact_types: tuple[str, ...] = ()


class LeadScorer:
    VERSION = "2.0"

    def score(self, value: LeadScoreInput) -> LeadScoreResult:
        legacy = LegacyScorer().score(value)
        hours = value.hours_remaining
        expired = value.deadline_expired or value.deadline_status.upper() == "EXPIRED"
        urgency = 0
        if not expired:
            if hours is not None and hours >= 0:
                urgency = 30 if hours <= 24 else 24 if hours <= 72 else 18 if hours <= 168 else 12
            elif value.deadline_status.upper() in {"DUE_TODAY", "DUE_WITHIN_24H"}:
                urgency = 30
            elif value.deadline_status.upper() == "OPEN":
                urgency = 12
        if value.deadline_estimated:
            urgency = min(12, urgency)
        confidence = max(Decimal(0), min(Decimal(1), Decimal(str(value.evidence_confidence))))
        evidence = min(
            30,
            int((confidence * 25).quantize(Decimal(1), rounding=ROUND_HALF_UP))
            + (5 if value.has_direct_evidence else 0),
        )
        legal = self._legal_relevance(value.event_type)
        verified = set(value.verified_contact_types)
        contact = (
            5
            if verified & {"email", "whatsapp"}
            else 3
            if verified & {"phone", "form"}
            else 1
            if verified & {"website", "linkedin"} or value.pending_contact_types
            else 0
        )
        economic = self._economic_value(value.estimated_value)
        total = max(0, min(100, urgency + evidence + legal + contact + economic - legacy.penalties))
        breakdown = MappingProxyType(
            {
                "urgency": f"{urgency}/30 — faixa de urgência do prazo {'estimado' if value.deadline_estimated else 'publicado ou não disponível'}",
                "evidence": f"{evidence}/30 — confiança {confidence:.0%}; evidência direta ativa: {'sim' if value.has_direct_evidence else 'não'}",
                "legal_relevance": f"{legal}/25 — relevância operacional do evento {value.event_type.upper()}",
                "contact": f"{contact}/5 — melhor canal corporativo verificado; pendentes limitados a 1 ponto",
                "economic_value": f"{economic}/10 — maior valor individual comprovado da empresa neste caso: {value.estimated_value if value.estimated_value is not None else 'não disponível'}",
                "penalties": legacy.breakdown["penalties"],
            }
        )
        return LeadScoreResult(
            total,
            urgency,
            evidence,
            legal,
            contact,
            economic,
            legacy.penalties,
            breakdown,
            self.VERSION,
        )

    @staticmethod
    def _economic_value(value: Decimal | None) -> int:
        if value is None or value <= 0:
            return 0
        return 10 if value >= 1_000_000 else 8 if value >= 200_000 else 6 if value >= 50_000 else 3

    @staticmethod
    def _legal_relevance(event_type: str) -> int:
        event = event_type.upper()
        groups = {
            25: {
                "INELIGIBLE",
                "DISQUALIFIED",
                "PROPOSAL_REJECTED",
                "INTENT_TO_APPEAL",
                "APPEAL_SUBMITTED",
                "COUNTERARGUMENT_OPENED",
            },
            18: {
                "DILIGENCE_REQUESTED",
                "DOCUMENTATION_REQUESTED",
                "COUNTERARGUMENT_SUBMITTED",
                "APPEAL_DECIDED",
            },
            8: {"NEGOTIATION_REQUESTED", "SESSION_OPENED", "SESSION_SUSPENDED", "SESSION_REOPENED"},
            4: {
                "PARTICIPATION_DETECTED",
                "PROPOSAL_SUBMITTED",
                "PROPOSAL_ACCEPTED",
                "QUALIFIED",
                "BEST_OFFER_DECLARED",
            },
            2: {"WINNER_DECLARED", "ADJUDICATED", "HOMOLOGATED"},
        }
        return next((points for points, events in groups.items() if event in events), 0)
