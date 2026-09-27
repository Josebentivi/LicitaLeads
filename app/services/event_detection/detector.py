# ruff: noqa: E501

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import replace

from app.services.identifiers import is_valid_cnpj, normalize_cnpj, normalize_company_name

from .types import (
    CompanyReference,
    DetectedEvent,
    DetectedParticipant,
    DocumentEvidence,
    EventType,
    ParticipationRole,
    ReasonCategory,
)


def _fold(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value).lower()
        if not unicodedata.combining(char)
    )


_CNPJ_TOKEN = re.compile(
    r"(?<![A-Z0-9])(?:[A-Z0-9]{2}\.?[A-Z0-9]{3}\.?[A-Z0-9]{3}/?[A-Z0-9]{4}-?\d{2})(?![A-Z0-9])",
    re.I,
)
_ITEM = re.compile(r"\b(?:item|lote)\s*(?:n[ºo°.]?\s*)?([A-Z0-9.-]+)", re.I)
_NAME_BEFORE_CNPJ = re.compile(
    r"(?P<name>[A-ZÀ-Ý][A-ZÀ-Ý0-9 &'().,/\-]{2,120}?)\s*(?:[,;|\-]\s*)?(?:CNPJ(?:/MF)?\s*(?:n[ºo°.]?\s*)?(?:sob\s+o\s+n[ºo°.]?\s*)?[:\-]?)\s*$",
    re.I,
)

_EVENT_PATTERNS: tuple[tuple[EventType, re.Pattern[str]], ...] = (
    (
        EventType.INTENT_TO_APPEAL,
        re.compile(
            r"\bmanifest(?:ou|a)\s+inten[cç][aã]o\s+de\s+(?:interpor\s+)?recorr?er|\binten[cç][aã]o\s+de\s+recurso\b",
            re.I,
        ),
    ),
    (
        EventType.COUNTERARGUMENT_OPENED,
        re.compile(
            r"\bprazo\s+(?:para|de)\s+contrarraz(?:a|õ|o)es\b|\bcontrarraz(?:a|õ|o)es\s+(?:abertas|aberto)\b",
            re.I,
        ),
    ),
    (
        EventType.COUNTERARGUMENT_SUBMITTED,
        re.compile(r"\b(?:apresentou|protocolou|interp[oô]s)\s+contrarraz(?:a|õ|o)es\b", re.I),
    ),
    (
        EventType.APPEAL_SUBMITTED,
        re.compile(
            r"\b(?:apresentou|protocolou|interp[oô]s)\s+(?:o\s+)?recurso\b|\braz[oõ]es\s+recursais\s+(?:apresentadas|protocoladas)\b",
            re.I,
        ),
    ),
    (
        EventType.APPEAL_DECIDED,
        re.compile(
            r"\brecurso\s+(?:foi\s+)?(?:deferido|indeferido|provido|improvido|julgado)\b", re.I
        ),
    ),
    (
        EventType.INELIGIBLE,
        re.compile(
            r"\b(?:foi|restou|licitante\s+foi|empresa\s+foi)?\s*inabilitad[ao]\b|\blicitante\s+inabilitad[ao]\b",
            re.I,
        ),
    ),
    (
        EventType.DISQUALIFIED,
        re.compile(
            r"\b(?:foi|restou)?\s*desclassificad[ao]\b|\bproposta\s+desclassificada\b", re.I
        ),
    ),
    (EventType.PROPOSAL_REJECTED, re.compile(r"\bproposta\s+(?:foi\s+)?recusada\b", re.I)),
    (EventType.PROPOSAL_ACCEPTED, re.compile(r"\bproposta\s+(?:foi\s+)?aceita\b", re.I)),
    (EventType.QUALIFIED, re.compile(r"\b(?:foi|restou)?\s*habilitad[ao]\b", re.I)),
    (
        EventType.WINNER_DECLARED,
        re.compile(r"\b(?:declarad[ao]|sagrou-se)\s+vencedor(?:a)?\b", re.I),
    ),
    (EventType.ADJUDICATED, re.compile(r"\badjudicad[ao]\b", re.I)),
    (EventType.HOMOLOGATED, re.compile(r"\bhomologad[ao]\b", re.I)),
    (EventType.SESSION_SUSPENDED, re.compile(r"\bsess[aã]o\s+(?:foi\s+)?suspensa\b", re.I)),
    (EventType.SESSION_REOPENED, re.compile(r"\bsess[aã]o\s+(?:foi\s+)?reaberta\b", re.I)),
)

_REASON_PATTERNS: tuple[tuple[ReasonCategory, re.Pattern[str]], ...] = (
    (
        ReasonCategory.MISSING_DOCUMENT,
        re.compile(
            r"\bn[aã]o\s+(?:apresentou|apresentar|encaminhou|encaminhar|anexou|anexar)|\baus[eê]ncia\s+(?:da|de|do)\s+(?:certid[aã]o|document)",
            re.I,
        ),
    ),
    (
        ReasonCategory.INVALID_DOCUMENT,
        re.compile(r"\b(?:documento|certid[aã]o)\s+(?:inv[aá]lid[ao]|vencid[ao])\b", re.I),
    ),
    (
        ReasonCategory.FISCAL_REGULARITY,
        re.compile(r"\bregularidade\s+fiscal\b|\bd[eé]bito\s+(?:fiscal|tribut[aá]rio)", re.I),
    ),
    (ReasonCategory.LABOR_REGULARITY, re.compile(r"\bregularidade\s+trabalhista\b|\bCNDT\b", re.I)),
    (
        ReasonCategory.ECONOMIC_FINANCIAL,
        re.compile(
            r"\b(?:qualifica[cç][aã]o|capacidade)\s+econ[oô]mico.?financeira\b|\bbalan[cç]o\s+patrimonial\b",
            re.I,
        ),
    ),
    (
        ReasonCategory.TECHNICAL_QUALIFICATION,
        re.compile(
            r"\bqualifica[cç][aã]o\s+t[eé]cnica\b|\batestado\s+de\s+capacidade\s+t[eé]cnica\b", re.I
        ),
    ),
    (
        ReasonCategory.PRICE_INEXEQUIBILITY,
        re.compile(r"\b(?:pre[cç]o|proposta)\s+inexequ[ií]vel\b|\binexequibilidade\b", re.I),
    ),
    (
        ReasonCategory.PRICE_ABOVE_ESTIMATE,
        re.compile(
            r"\b(?:pre[cç]o|valor)\s+(?:acima|superior)\s+(?:ao|do)\s+(?:estimado|or[cç]amento)",
            re.I,
        ),
    ),
    (
        ReasonCategory.LATE_SUBMISSION,
        re.compile(r"\b(?:fora|ap[oó]s)\s+do\s+prazo\b|\bintempestiv[ao]\b", re.I),
    ),
    (
        ReasonCategory.PROPOSAL_FORMAT,
        re.compile(
            r"\bformato\s+(?:da\s+)?proposta\b|\bplanilha\s+(?:incorreta|incompat[ií]vel)", re.I
        ),
    ),
    (
        ReasonCategory.SAMPLE_REJECTED,
        re.compile(r"\bamostra\s+(?:foi\s+)?(?:reprovada|rejeitada)\b", re.I),
    ),
    (
        ReasonCategory.BRAND_OR_MODEL_NONCOMPLIANT,
        re.compile(r"\b(?:marca|modelo)\s+(?:n[aã]o\s+atende|incompat[ií]vel|divergente)", re.I),
    ),
    (
        ReasonCategory.FAILURE_TO_RESPOND,
        re.compile(
            r"\bn[aã]o\s+(?:respondeu|se\s+manifestou)|\baus[eê]ncia\s+de\s+resposta\b", re.I
        ),
    ),
    (
        ReasonCategory.TECHNICAL_SPECIFICATION,
        re.compile(
            r"\bn[aã]o\s+atende\s+(?:ao\s+)?edital|\bdescumpriu\s+(?:o\s+)?item|\bespecifica[cç][aã]o\s+t[eé]cnica\b",
            re.I,
        ),
    ),
)

_RULE_LANGUAGE = re.compile(
    r"\b(?:ser[aá]|ser[aã]o|poder[aá]|dever[aá])\s+(?:ser\s+)?(?:inabilitad|desclassificad)|\bensejar[aá]\s+(?:a\s+)?(?:inabilita[cç][aã]o|desclassifica[cç][aã]o)",
    re.I,
)
_NEGATED_EVENT = re.compile(
    r"\bn[aã]o\s+(?:(?:foi|ser[aá]|restou)\s+)?$|"
    r"\bsem\s+(?:inabilita[cç][aã]o|desclassifica[cç][aã]o)\b",
    re.I,
)
_DOCUMENT_PRIORITIES = {
    "ata": 0.94,
    "ata_sessao": 0.96,
    "termo_julgamento": 0.95,
    "julgamento": 0.94,
    "decisao": 0.94,
    "recurso": 0.90,
    "resultado": 0.88,
    "habilitacao": 0.88,
    "outro": 0.72,
}


def _context(text: str, start: int, end: int, radius: int = 280) -> tuple[str, int]:
    left = max(0, text.rfind("\n", 0, max(0, start - radius)))
    if left == -1:
        left = max(0, start - radius)
    right_break = text.find("\n", min(len(text), end + radius))
    right = len(text) if right_break == -1 else right_break
    return text[left:right].strip(), left


def _document_base_confidence(document_type: str | None) -> float:
    value = (document_type or "outro").lower().replace(" ", "_")
    if value in {"edital", "termo_de_referencia", "aviso"}:
        return 0.0
    return _DOCUMENT_PRIORITIES.get(value, _DOCUMENT_PRIORITIES["outro"])


def _extract_item(context: str) -> str | None:
    match = _ITEM.search(context)
    return match.group(1).rstrip(".-") if match else None


def _cnpjs(text: str) -> list[str]:
    values: list[str] = []
    for match in _CNPJ_TOKEN.finditer(text):
        candidate = normalize_cnpj(match.group())
        if candidate and is_valid_cnpj(candidate) and candidate not in values:
            values.append(candidate)
    return values


def _clean_extracted_name(value: str) -> str:
    value = re.sub(
        r"^.*\b(?:licitante|participante|empresa|fornecedor)\s+",
        "",
        value,
        flags=re.I,
    )
    value = re.sub(r"^(?:item|lote)\s*[A-Z0-9.-]+\s*[-:|]?\s*", "", value, flags=re.I)
    return value.strip(" ,;-|")


def associate_company(
    context: str,
    companies: Iterable[CompanyReference],
) -> tuple[CompanyReference | None, str, bool]:
    """Associate only by exact CNPJ or an unambiguous normalized full name."""

    company_list = list(companies)
    context_cnpjs = set(_cnpjs(context))
    cnpj_matches = [
        company for company in company_list if normalize_cnpj(company.cnpj) in context_cnpjs
    ]
    if len(cnpj_matches) == 1:
        return cnpj_matches[0], "exact_cnpj", False
    if len(cnpj_matches) > 1:
        return None, "ambiguous_cnpj", True

    folded_context = normalize_company_name(context, strip_legal_suffix=False) or ""
    name_matches: list[CompanyReference] = []
    for company in company_list:
        normalized = normalize_company_name(company.legal_name)
        if (
            normalized
            and len(normalized) >= 5
            and re.search(rf"(?:^| )({re.escape(normalized)})(?: |$)", folded_context)
        ):
            name_matches.append(company)
    unique_ids = {
        company.external_id or company.cnpj or company.legal_name for company in name_matches
    }
    if len(unique_ids) == 1 and name_matches:
        return name_matches[0], "exact_normalized_name", not bool(name_matches[0].cnpj)
    if len(unique_ids) > 1:
        return None, "ambiguous_name", True
    return None, "not_found", True


class ParticipantDetector:
    """Find document-backed participants; never synthesise missing competitors."""

    def detect(
        self,
        evidence: DocumentEvidence,
        *,
        known_companies: Iterable[CompanyReference] = (),
    ) -> list[DetectedParticipant]:
        base = _document_base_confidence(evidence.document_type)
        if base == 0:
            return []
        companies = list(known_companies)
        candidates: list[DetectedParticipant] = []
        seen: set[tuple[str | None, str, str | None]] = set()
        for match in _CNPJ_TOKEN.finditer(evidence.text):
            cnpj = normalize_cnpj(match.group())
            if not cnpj or not is_valid_cnpj(cnpj):
                continue
            context, context_start = _context(evidence.text, match.start(), match.end(), 180)
            folded = _fold(context)
            if not re.search(
                r"\b(licitante|participante|empresa|fornecedor|vencedor|adjudicad|inabilitad|desclassificad)\b",
                folded,
            ):
                continue
            known = next(
                (company for company in companies if normalize_cnpj(company.cnpj) == cnpj), None
            )
            if known:
                name = known.legal_name
                external_id = known.external_id
            else:
                prefix = evidence.text[max(context_start, match.start() - 150) : match.start()]
                name_match = _NAME_BEFORE_CNPJ.search(prefix)
                name = (
                    _clean_extracted_name(name_match.group("name"))
                    if name_match
                    else "Empresa não identificada"
                )
                external_id = None
            role = ParticipationRole.PARTICIPANT
            if "vencedor" in folded:
                role = ParticipationRole.WINNER
            elif "adjudicad" in folded:
                role = ParticipationRole.AWARDED
            status = None
            if "inabilitad" in folded:
                status = "ineligible"
            elif "desclassificad" in folded:
                status = "disqualified"
            item = _extract_item(context)
            key = (cnpj, role.value, item)
            if key in seen:
                continue
            seen.add(key)
            local_evidence = replace(
                evidence,
                text=context,
                start_offset=context_start,
                end_offset=context_start + len(context),
            )
            candidates.append(
                DetectedParticipant(
                    company_name=name,
                    company_cnpj=cnpj,
                    role=role,
                    status=status,
                    item_number=item,
                    evidence=local_evidence,
                    confidence=min(1.0, base + 0.04),
                    requires_manual_review=False,
                    company_external_id=external_id,
                )
            )
        return candidates

    @staticmethod
    def from_structured_winner(
        company: CompanyReference,
        evidence: DocumentEvidence,
        *,
        item_number: str | None = None,
        awarded: bool = False,
    ) -> DetectedParticipant:
        """Represent exactly the winner returned by an API, not other bidders."""

        return DetectedParticipant(
            company_name=company.legal_name,
            company_cnpj=normalize_cnpj(company.cnpj),
            role=ParticipationRole.AWARDED if awarded else ParticipationRole.WINNER,
            status="awarded" if awarded else "winner",
            item_number=item_number,
            evidence=evidence,
            confidence=1.0,
            requires_manual_review=not bool(company.cnpj),
            company_external_id=company.external_id,
        )


class EventDetector:
    def detect(
        self,
        evidence: DocumentEvidence,
        *,
        known_companies: Iterable[CompanyReference] = (),
    ) -> list[DetectedEvent]:
        base = _document_base_confidence(evidence.document_type)
        if base == 0:
            return []
        companies = list(known_companies)
        detected: list[DetectedEvent] = []
        seen: set[tuple[EventType, int, str | None]] = set()
        for event_type, pattern in _EVENT_PATTERNS:
            for match in pattern.finditer(evidence.text):
                prefix = evidence.text[max(0, match.start() - 80) : match.start()]
                phrase = evidence.text[
                    max(0, match.start() - 60) : min(len(evidence.text), match.end() + 60)
                ]
                if _RULE_LANGUAGE.search(phrase) or _NEGATED_EVENT.search(prefix):
                    continue
                context, start = _context(evidence.text, match.start(), match.end())
                company, method, ambiguous = associate_company(context, companies)
                category = self.classify_reason(context)
                confidence = base
                if method == "exact_cnpj":
                    confidence = min(1.0, confidence + 0.05)
                elif method == "exact_normalized_name":
                    confidence -= 0.04
                elif companies:
                    confidence -= 0.15
                if category is ReasonCategory.UNKNOWN and event_type in {
                    EventType.INELIGIBLE,
                    EventType.DISQUALIFIED,
                    EventType.PROPOSAL_REJECTED,
                }:
                    confidence -= 0.08
                confidence = max(0.0, round(confidence, 2))
                requires_review = ambiguous or confidence < 0.75 or company is None
                local_evidence = replace(
                    evidence,
                    text=context,
                    start_offset=start,
                    end_offset=start + len(context),
                )
                key = (event_type, start, company.external_id if company else None)
                if key in seen:
                    continue
                seen.add(key)
                detected.append(
                    DetectedEvent(
                        event_type=event_type,
                        raw_description=context,
                        normalized_reason=self.extract_reason(context, category),
                        reason_category=category,
                        evidence=local_evidence,
                        confidence=confidence,
                        requires_manual_review=requires_review,
                        company_name=company.legal_name if company else None,
                        company_cnpj=normalize_cnpj(company.cnpj) if company else None,
                        company_external_id=company.external_id if company else None,
                        item_number=_extract_item(context),
                        occurred_at=evidence.published_at,
                    )
                )
        return sorted(
            detected, key=lambda event: (event.evidence.start_offset or 0, event.event_type.value)
        )

    @staticmethod
    def classify_reason(context: str) -> ReasonCategory:
        for category, pattern in _REASON_PATTERNS:
            if pattern.search(context):
                return category
        return ReasonCategory.UNKNOWN

    @staticmethod
    def extract_reason(context: str, category: ReasonCategory) -> str | None:
        if category is ReasonCategory.UNKNOWN:
            return None
        # Evidence remains literal; the normalized reason is a compact sentence
        # selected deterministically rather than an invented paraphrase.
        sentences = re.split(r"(?<=[.!?;])\s+|\n+", context)
        pattern = next(pattern for candidate, pattern in _REASON_PATTERNS if candidate is category)
        sentence = next(
            (sentence.strip() for sentence in sentences if pattern.search(sentence)),
            context.strip(),
        )
        return sentence[:500]
