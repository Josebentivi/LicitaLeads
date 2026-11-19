# ruff: noqa: E501

from __future__ import annotations

import re
import unicodedata
from bisect import bisect_left
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from app.models.enums import ProcurementEventType, ProcurementPhase
from app.services.event_detection.authority import assertion_diagnostic
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

if TYPE_CHECKING:
    from app.models import ProcurementEvent


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
_SCOPE = re.compile(r"\b(item|lote)\s+(?:n[ºo°.]?\s*)?(\d+[A-Z]?(?:[.-]\d+[A-Z]?)*)\b", re.I)
_SCOPE_HEADER = re.compile(
    r"\s*(?:item|lote)\s+(?:n[ºo°.]?\s*)?\d+[A-Z]?(?:[.-]\d+[A-Z]?)*\s*[.:|—–-]?\s*", re.I
)
_ACT_BREAK = re.compile(
    r"\r?\n(?:[ \t]*\r?\n)*|(?<=[!?;])[ \t]+|(?<=\.)[ \t]+(?=[A-ZÀ-Ý])|,(?=\s+(?:enquanto|quanto\s+[aà]|ao\s+passo|j[áa]\s+a)\b)",
    re.I,
)
_ABBREVIATION = re.compile(r"\b(?:ltda|s\.a|art|inc|sr|sra|dr|dra|n)\.$", re.I)
_NEW_ROW = re.compile(
    r"^\s*(?:(?:item|lote)\s+(?:n[ºo°.]?\s*)?\d+\b|[•▪*—-]\s+"
    r"|(?:\[?\d{1,2}[/.-]\d{1,2}[/.-]20\d{2}[ ,]+(?:às\s+)?)?"
    r"\d{1,2}:\d{2}(?::\d{2})?\s*[]–—|:\-])",
    re.I,
)
_NEW_PARTY_ROW = re.compile(
    r"^\s*(?:(?:a|o|outr[ao])\s+)?(?:empresa|licitante|fornecedor[ae]?)\b", re.I
)
_CONTINUATION_WORD = re.compile(r"\b(?:para|pela|pelo|perante|d[aeo]|a|à|ao|que)\s*[:,]?\s*$", re.I)
_PARTY_REFERENCE = re.compile(
    r"\b(?:a|o|outr[ao]|demais)\s+(?:empresa|licitante|fornecedor[ae]?)\b", re.I
)
_RECIPIENT_OPENINGS = {
    EventType.APPEAL_WINDOW_OPENED,
    EventType.COUNTERARGUMENT_OPENED,
    EventType.DILIGENCE_REQUESTED,
    EventType.DOCUMENTATION_REQUESTED,
}
_ADDRESSEE = re.compile(
    r"\b(?:destinat[aá]ri[oa]\s*:\s*|para\s+(?:a|o)\s+|[àa]o\s+)"
    r"|(?<!\w)à\s+|\b(?:convoca(?:-se|mos)?|solicita(?:-se|mos)?|intima(?:-se|mos)?)\s+(?:a|o)\s+",
    re.I,
)
_PASSIVE_SUMMONS = re.compile(
    r"\b(?:foi|fica|est[aá]|restou)\s+(?:convocad[ao]|intimad[ao]|notificad[ao])\b", re.I
)
_APPELLANT_CLAUSE = re.compile(
    r"\brecurso\s+(?:(?:administrativo|hier[aá]rquico)\s+)?"
    r"(?:interposto|apresentado|protocolado|manejado)\b",
    re.I,
)
_RECIPIENT_ACT = re.compile(
    r"\b(?:(?:abert[oa]|concedid[oa]|iniciad[oa]|abriu-se|concedeu-se)\s+(?:o\s+)?prazo"
    r"|convoca(?:-se|mos)?|intima(?:-se|mos)?|notifica(?:-se|mos)?)\b",
    re.I,
)
_NON_RECIPIENT_TARGET = re.compile(r"^\s*(?:recurso|edital|certame|julgamento)\b", re.I)
_PARTY_PREFIX = re.compile(r"^(?:[ao]\s+)?(?:(?:empresa|licitante|fornecedor[ae]?)\s+)?", re.I)
_ADDRESSEE_END = re.compile(
    r"[;|]|\b(?:at[eé]|a\s+partir|para\s+(?:apresentar|responder|interpor|manifestar)|"
    r"ao\s+recurso|em\s+face|no\s+(?:item|lote)|foi\s+(?:intimad[ao]|convocad[ao]|notificad[ao]))\b",
    re.I,
)
_SUBJECT_BOUNDARY = re.compile(
    r"(?:[,;|]|\s+[—–-]\s+)(?!\s*(?:CNPJ\b|" + _CNPJ_TOKEN.pattern + r"))", re.I
)
_SUBJECT_HEADER = re.compile(
    r"^\s*(?:(?:item|lote)\s+(?:n[ºo°.]?\s*)?\d+[A-Z]?(?:[.-]\d+[A-Z]?)*\s*[.:|—–-]\s*"
    r"|\[?\d{1,2}[/.-]\d{1,2}[/.-]20\d{2}\s+\d{1,2}:\d{2}(?::\d{2})?\s*[\]—–|:-]\s*)",
    re.I,
)
_DEADLINE_CONTINUATION = re.compile(
    r"\s*prazo(?:\s+final)?\s+at[eé]\s+\d{1,2}[/.-]\d{1,2}[/.-]20\d{2}"
    r"(?:\s+(?:[àa]s?\s+)?\d{1,2}(?::\d{2}|h\d{0,2}))?\s*\.?\s*",
    re.I,
)
_RECURSAL_PURPOSE = (
    r"(?:recursal\b|(?:para|de)\s+(?:(?:a\s+)?(?:manifestar\s+|manifesta[cç][aã]o\s+d[ea]\s+|"
    r"(?:apresenta[cç][aã]o|interposi[cç][aã]o)\s+d[eo]\s+|(?:apresentar|interpor)\s+)(?:o\s+)?)?"
    r"(?:inten[cç][aã]o\s+de\s+(?:recurso|recorrer)|recursos?\b|raz[oõ]es\s+recursais))"
)
_NAME_BEFORE_CNPJ = re.compile(
    r"(?P<name>[A-ZÀ-Ý][A-ZÀ-Ý0-9 &'().,/\-]{2,120}?)\s*(?:[,;|\-]\s*)?(?:CNPJ(?:/MF)?\s*(?:n[ºo°.]?\s*)?(?:sob\s+o\s+n[ºo°.]?\s*)?[:\-]?)\s*$",
    re.I,
)

_EVENT_PATTERNS: tuple[tuple[EventType, re.Pattern[str]], ...] = (
    (
        EventType.APPEAL_WINDOW_OPENED,
        re.compile(
            r"\b(?:(?:(?:fica|foi|est[aá]|restou)\s+)?(?:aberto|concedido|iniciado|prorrogado|reaberto)|(?:abriu-se|concedeu-se))"
            r"\s+(?:o\s+)?prazo\s+"
            + _RECURSAL_PURPOSE
            + r"|\bprazo\s+"
            + _RECURSAL_PURPOSE
            + r"\s*[:–-]?\s+(?:foi\s+|est[aá]\s+)?(?:aberto|concedido|iniciado|prorrogado|reaberto)\b",
            re.I,
        ),
    ),
    (
        EventType.INTENT_TO_APPEAL,
        re.compile(
            r"\b(?:manifestou|regist(?:rou|rada)|apresentou|protocolou)\s+(?:a\s+)?inten[cç][aã]o\s+de\s+(?:recorrer|recurso)\b"
            r"|\binten[cç][aã]o\s+de\s+(?:recurso|recorrer)\s+(?:manifestada|registrada|apresentada|protocolada)\b",
            re.I,
        ),
    ),
    (
        EventType.COUNTERARGUMENT_OPENED,
        re.compile(
            r"\bprazo\s+(?:para|de)\s+(?:(?:a\s+)?apresenta[cç][aã]o\s+de\s+|apresentar\s+)?contrarraz(?:a|õ|o)es\b"
            r"|\bcontrarraz(?:a|õ|o)es\s+(?:abertas|aberto)\b",
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
    (
        EventType.SESSION_SUSPENDED,
        re.compile(
            r"\bsess[aã]o\s+(?:do\s+certame\s+)?(?:foi\s+)?suspensa\b|\bcertame\s+(?:foi\s+)?suspenso\b",
            re.I,
        ),
    ),
    (EventType.SESSION_REOPENED, re.compile(r"\bsess[aã]o\s+(?:foi\s+)?reaberta\b", re.I)),
    (
        EventType.SESSION_OPENED,
        re.compile(
            r"\bsess[a\u00e3]o\s+p[u\u00fa]blica\b[^.\n]{0,80}\b(?:aberta|iniciada)\b"
            r"|\babertura\s+da\s+sess[a\u00e3]o\b",
            re.I,
        ),
    ),
    (
        EventType.BEST_OFFER_DECLARED,
        re.compile(
            r"\bdetentor(?:a)?\s+d[ao]\s+(?:melhor\s+(?:oferta|lance)|menor\s+(?:lance|pre[cç]o))\b"
            r"|\b(?:melhor\s+(?:oferta|lance)|menor\s+lance)\s+(?:[e\u00e9]|foi|pertence)\b"
            r"|\bclassificad[ao]\s+em\s+(?:primeiro|1[\u00ba\u00b0o])\s+lugar\b",
            re.I,
        ),
    ),
    (
        EventType.NEGOTIATION_REQUESTED,
        re.compile(
            r"\bnegocia[c\u00e7][a\u00e3]o\b[^.\n]{0,120}\b(?:prazo|convoc|solicit|abert)\b"
            r"|\b(?:convocad[ao]|solicitad[ao])[^.\n]{0,100}\bnegocia",
            re.I,
        ),
    ),
    (
        EventType.DILIGENCE_REQUESTED,
        re.compile(
            r"\b(?:convocad[ao]|solicitad[ao]|abert[ao]|prazo)\b[^.\n]{0,140}"
            r"\bdilig[e\u00ea]ncia\b"
            r"|\bdilig[e\u00ea]ncia\b[^.\n]{0,140}"
            r"\b(?:convocad[ao]|solicitad[ao]|prazo|comprova)\b",
            re.I,
        ),
    ),
    (
        EventType.DOCUMENTATION_REQUESTED,
        re.compile(
            r"\b(?:convocad[ao]|solicitad[ao]|prazo)\b[^.\n]{0,140}"
            r"\b(?:apresenta[c\u00e7][a\u00e3]o|apresentar|envio)\b[^.\n]{0,80}"
            r"\bdocumentos?\b"
            r"|\bprazo\s+para\s+apresenta[c\u00e7][a\u00e3]o\s+de\s+documentos?\b",
            re.I,
        ),
    ),
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
    r"\b(?:ser[aá]|ser[aã]o|poder[aá]|dever[aá])\s+(?:ser\s+)?(?:inabilitad|desclassificad|(?:re?abert|prorrogad|concedid)[ao]\s+(?:o\s+)?prazo)|\bensejar[aá]\s+(?:a\s+)?(?:inabilita[cç][aã]o|desclassifica[cç][aã]o)",
    re.I,
)
_NEGATED_EVENT = re.compile(
    r"\bn[aã]o\s+(?:(?:foi|ser[aá]|restou)\s+)?$|"
    r"\bsem\s+(?:inabilita[cç][aã]o|desclassifica[cç][aã]o)\b",
    re.I,
)
_NEGATED_PARTICIPANT_ROLE = re.compile(
    r"\bnao\s+(?:(?:foi|sera|restou|e)\s+)?(?:declarad[ao]\s+)?"
    r"(?:vencedor[a]?|adjudicad[ao]?|inabilitad[ao]?|desclassificad[ao]?)\b"
)
_DOCUMENT_PRIORITIES = {
    "ata": 0.94,
    "ata_sessao": 0.96,
    "ata_de_sessao": 0.96,
    "relatorio_de_sessao": 0.95,
    "termo_julgamento": 0.95,
    "termo_de_julgamento": 0.95,
    "julgamento": 0.94,
    "decisao": 0.94,
    "decisao_recurso": 0.92,
    "decisao_de_recurso": 0.92,
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
    value = re.sub(r"[^a-z0-9]+", "_", _fold(document_type or "outro")).strip("_")
    if value in {"edital", "termo_de_referencia", "aviso"}:
        return 0.0
    return _DOCUMENT_PRIORITIES.get(value, _DOCUMENT_PRIORITIES["outro"])


def _extract_item(context: str) -> str | None:
    return _scope_number(context, "item")


def _scope_number(text: str, kind: str) -> str | None:
    numbers = scope_numbers(text, kind)
    return next(iter(numbers)) if len(numbers) == 1 else None


def scope_numbers(text: str, kind: str) -> set[str]:
    return {
        match[2]
        for match in _SCOPE.finditer(text)
        if match[1].casefold() == kind
        and not re.match(r"\s+d(?:o|este|aquele)\s+edital\b", text[match.end() :], re.I)
    }


def _semantic_breaks(text: str, locator: str | None) -> Iterable[re.Match[str]]:
    for boundary in _ACT_BREAK.finditer(text):
        before = text[max(0, boundary.start() - 100) : boundary.start()].rstrip()
        abbreviation = bool(_ABBREVIATION.search(before))
        separator = boundary.group()
        if "\n" not in separator:
            if not abbreviation:
                yield boundary
            continue
        after = text[boundary.end() :].split("\n", 1)[0]
        prior_row = before.rsplit("\n", 1)[-1]
        if (
            separator.count("\n") > 1
            or (not abbreviation and prior_row.endswith((".", "!", "?", ";")))
            or _NEW_ROW.match(after)
            or (_NEW_PARTY_ROW.match(after) and not _CONTINUATION_WORD.search(prior_row))
            or (locator and locator.startswith("sheet:"))
            or (" | " in prior_row and " | " in after)
        ):
            yield boundary


def _act_bounds(
    text: str, start: int, end: int, *, include_deadline: bool = False, locator: str | None = None
) -> tuple[int, int]:
    """Keep neighboring acts out of the attribution and retain an adjacent item heading."""
    left = previous = 0
    right = len(text)
    boundaries = iter(_semantic_breaks(text, locator))
    for boundary in boundaries:
        if boundary.end() <= start:
            previous, left = left, boundary.end()
        elif boundary.start() >= end:
            right = boundary.start()
            if include_deadline:
                next_boundary = next(boundaries, None)
                continuation_end = next_boundary.start() if next_boundary else len(text)
                if _DEADLINE_CONTINUATION.fullmatch(text[boundary.end() : continuation_end]):
                    right = continuation_end
            break
    heading = text[previous:left]
    if left > previous and _SCOPE_HEADER.fullmatch(heading):
        left = previous
    span = text[left:right]
    return left + len(span) - len(span.lstrip()), left + len(span.rstrip())


def _act_span(
    text: str, start: int, end: int, *, include_deadline: bool = False, locator: str | None = None
) -> str:
    left, right = _act_bounds(text, start, end, include_deadline=include_deadline, locator=locator)
    return text[left:right]


# A supplied citation can establish a proposal registration without promoting
# every mention of a registration in a full document to a new rules event.
_LITERAL_PROOF_PATTERNS = (
    (
        EventType.PROPOSAL_SUBMITTED,
        re.compile(
            r"\b(?:recebeu|registrou|protocolou|apresentou|enviou)\s+(?:a\s+)?proposta\b"
            r"|\bproposta\s+(?:foi\s+)?(?:recebida|registrada|protocolada|apresentada|enviada)\b",
            re.I,
        ),
    ),
)


def _credible_act(act: str, matched: str, title: str | None) -> bool:
    prefix = act[: act.find(matched)].rstrip()
    return not (
        _RULE_LANGUAGE.search(act)
        or _NEGATED_EVENT.search(prefix + " ")
        or assertion_diagnostic(act, title)
    )


@dataclass(frozen=True, slots=True)
class EventLiteralAct:
    text: str
    start: int
    end: int


def event_literal_act(
    event_type: str,
    text: str,
    *,
    company: CompanyReference | None = None,
    title: str | None = None,
    document_type: str | None = None,
    include_deadline: bool = False,
    locator: str | None = None,
) -> EventLiteralAct | None:
    """Recover a unique literal act from retained evidence, without borrowing another act."""
    if (
        _document_base_confidence(document_type) == 0
        or assertion_diagnostic(text, title) == "status_legend"
    ):
        return None
    acts = set()
    for kind, pattern in (*_EVENT_PATTERNS, *_LITERAL_PROOF_PATTERNS):
        if kind.value != event_type:
            continue
        for match in pattern.finditer(text):
            start, end = _act_bounds(
                text, match.start(), match.end(), include_deadline=include_deadline, locator=locator
            )
            act = text[start:end]
            if _credible_act(act, match.group(), title) and (
                company is None or associate_act_company(kind, act, (company,))[0] == company
            ):
                acts.add(EventLiteralAct(act, start, end))
    return next(iter(acts)) if len(acts) == 1 else None


def event_act_excerpt(
    event_type: str,
    text: str,
    *,
    company: CompanyReference | None = None,
    title: str | None = None,
    document_type: str | None = None,
    include_deadline: bool = False,
    locator: str | None = None,
) -> str | None:
    act = event_literal_act(
        event_type,
        text,
        company=company,
        title=title,
        document_type=document_type,
        include_deadline=include_deadline,
        locator=locator,
    )
    return act.text if act else None


@dataclass(frozen=True, slots=True)
class EventScope:
    stage: ProcurementPhase | None = None
    item_number: str | None = None
    lot_number: str | None = None
    reason: str | None = None


_DOCUMENTED_STAGES = (
    (ProcurementPhase.DISPUTE, r"disputa(?: de lances)?|lances"),
    (ProcurementPhase.JUDGMENT, r"julgamento(?: das propostas)?"),
    (ProcurementPhase.DILIGENCE, r"diligencia"),
    (ProcurementPhase.QUALIFICATION, r"habilitacao"),
    (ProcurementPhase.APPEAL, r"recurso|recursal"),
)


def describe_event_scope(
    event: ProcurementEvent | ProcurementEventType,
    evidence_text: str | None = None,
    *,
    company: CompanyReference | None = None,
) -> EventScope:
    kind = event.value if isinstance(event, ProcurementEventType) else event.event_type.value
    act = event_act_excerpt(kind, evidence_text or "", company=company)
    stage = {
        EventType.ADJUDICATED.value: ProcurementPhase.ADJUDICATION,
        EventType.HOMOLOGATED.value: ProcurementPhase.HOMOLOGATION,
    }.get(kind)
    if act is None:
        return EventScope()
    if stage is None:
        stages = [
            phase
            for phase, terms in _DOCUMENTED_STAGES
            if re.search(
                rf"\b(?:na|nesta|da|desta|durante\s+a)\s+(?:fase|etapa)\s+(?:d[aeo]s?\s+)?(?:{terms})\b",
                _fold(act),
            )
        ]
        if len(stages) == 1:
            stage = stages[0]
    category = EventDetector.classify_reason(act)
    reason = EventDetector.extract_reason(act, category) or " ".join(act.split())[:500]
    return EventScope(stage, _scope_number(act, "item"), _scope_number(act, "lote"), reason)


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
    if len(context_cnpjs) > 1 or len(_PARTY_REFERENCE.findall(context)) > 1:
        return None, "ambiguous_cnpj", True
    cnpj_matches = [
        company for company in company_list if normalize_cnpj(company.cnpj) in context_cnpjs
    ]
    if len(cnpj_matches) == 1:
        return cnpj_matches[0], "exact_cnpj", False
    if len(cnpj_matches) > 1:
        return None, "ambiguous_cnpj", True
    if context_cnpjs:
        return None, "not_found", True

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


def _recipient_match(candidate: str, company: CompanyReference) -> str | None:
    candidate = candidate.strip()
    if _APPELLANT_CLAUSE.search(candidate) or _NON_RECIPIENT_TARGET.match(candidate):
        return None
    name = normalize_company_name(company.legal_name)
    cnpj = normalize_cnpj(company.cnpj)
    identifiers = set(_cnpjs(candidate))
    if identifiers:
        if identifiers != {cnpj}:
            return None
        match = next(
            match for match in _CNPJ_TOKEN.finditer(candidate) if normalize_cnpj(match[0]) == cnpj
        )
        return "exact_cnpj" if not candidate[match.end() :].strip(" \t\n\r,.)") else None
    normalized = {
        normalize_company_name(value)
        for value in (candidate, _PARTY_PREFIX.sub("", candidate, count=1))
    }
    return "exact_normalized_name" if name and name in normalized else None


def _recipient_spans(text: str) -> Iterable[str]:
    for anchor in _ADDRESSEE.finditer(text):
        appellant = _APPELLANT_CLAUSE.search(text, endpos=anchor.start())
        if appellant and not _RECIPIENT_ACT.search(text, appellant.end(), anchor.start()):
            continue
        yield _ADDRESSEE_END.split(text[anchor.end() :], maxsplit=1)[0]
    for summons in _PASSIVE_SUMMONS.finditer(text):
        prefix = text[: summons.start()].rstrip(" \t\n\r,")
        yield _SUBJECT_HEADER.sub("", prefix)
        for boundary in _SUBJECT_BOUNDARY.finditer(prefix):
            yield _SUBJECT_HEADER.sub("", prefix[boundary.end() :])


def associate_act_company(
    event_type: EventType, text: str, companies: Iterable[CompanyReference]
) -> tuple[CompanyReference | None, str, bool]:
    if event_type not in _RECIPIENT_OPENINGS:
        return associate_company(text, companies)
    spans = tuple(_recipient_spans(text))
    matches = [
        (company, methods)
        for company in companies
        if (methods := {method for span in spans if (method := _recipient_match(span, company))})
    ]
    if len(matches) == 1:
        company, methods = matches[0]
        method = "exact_cnpj" if "exact_cnpj" in methods else "exact_normalized_name"
        return company, method, not bool(company.cnpj)
    return None, "recipient_not_proven", True


class ParticipantDetector:
    """Find document-backed participants; never synthesise missing competitors."""

    def detect(
        self,
        evidence: DocumentEvidence,
        *,
        known_companies: Iterable[CompanyReference] = (),
    ) -> list[DetectedParticipant]:
        from app.services.event_detection.participation_values import reference_status

        if assertion_diagnostic("", evidence.document_title):
            return []
        base = _document_base_confidence(evidence.document_type)
        if base == 0:
            return []
        companies = list(known_companies)
        candidates: list[DetectedParticipant] = []
        seen: set[tuple[str | None, str, str | None]] = set()
        reference_cache: dict[str, str] = {}
        for match in _CNPJ_TOKEN.finditer(evidence.text):
            cnpj = normalize_cnpj(match.group())
            if not cnpj or not is_valid_cnpj(cnpj):
                continue
            if cnpj not in reference_cache:
                reference_cache[cnpj] = reference_status(
                    evidence.text, cnpj, evidence.document_type
                )
            reference = reference_cache[cnpj]
            if reference == "invalidated":
                continue
            context, context_start = _context(evidence.text, match.start(), match.end(), 180)
            act = _act_span(evidence.text, match.start(), match.end(), locator=evidence.locator)
            folded = _fold(act)
            if reference != "confirmed" and not re.search(
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
            ambiguous = bool(
                reference != "confirmed"
                or _RULE_LANGUAGE.search(folded)
                or _NEGATED_PARTICIPANT_ROLE.search(folded)
                or len(_cnpjs(act)) > 1
                or len(_PARTY_REFERENCE.findall(act)) > 1
                or assertion_diagnostic(act, evidence.document_title)
                or assertion_diagnostic(context, evidence.document_title) == "status_legend"
            )
            role = ParticipationRole.PARTICIPANT
            status = None
            if not ambiguous:
                if "vencedor" in folded:
                    role = ParticipationRole.WINNER
                elif "adjudicad" in folded:
                    role = ParticipationRole.AWARDED
                if "inabilitad" in folded:
                    status = "ineligible"
                elif "desclassificad" in folded:
                    status = "disqualified"
            item = _extract_item(act)
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
                    requires_manual_review=ambiguous,
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
        if assertion_diagnostic("", evidence.document_title):
            return []
        base = _document_base_confidence(evidence.document_type)
        if base == 0:
            return []
        companies = list(known_companies)
        detected: list[DetectedEvent] = []
        seen: set[tuple[EventType, int, int]] = set()
        for event_type, pattern in _EVENT_PATTERNS:
            acts = [
                (
                    match,
                    _act_bounds(
                        evidence.text,
                        match.start(),
                        match.end(),
                        include_deadline=event_type in _RECIPIENT_OPENINGS,
                        locator=evidence.locator,
                    ),
                )
                for match in pattern.finditer(evidence.text)
            ]
            match_positions = [match.start() for match, _ in acts]
            for match, (act_start, act_end) in acts:
                act = evidence.text[act_start:act_end]
                context, start = _context(evidence.text, match.start(), match.end())
                if (
                    not _credible_act(act, match.group(), evidence.document_title)
                    or assertion_diagnostic(context, evidence.document_title) == "status_legend"
                ):
                    continue
                key = (event_type, act_start, act_end)
                if key in seen:
                    continue
                seen.add(key)
                begin = bisect_left(match_positions, start)
                end = bisect_left(match_positions, start + len(context))
                neighbors = {bounds for _, bounds in acts[begin:end]}
                if len(neighbors) > 1:
                    context, start = act, act_start
                company, method, ambiguous = associate_act_company(event_type, act, companies)
                category = self.classify_reason(act)
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
                detected.append(
                    DetectedEvent(
                        event_type=event_type,
                        raw_description=context,
                        normalized_reason=self.extract_reason(act, category),
                        reason_category=category,
                        evidence=local_evidence,
                        confidence=confidence,
                        requires_manual_review=requires_review,
                        company_name=company.legal_name if company else None,
                        company_cnpj=normalize_cnpj(company.cnpj) if company else None,
                        company_external_id=company.external_id if company else None,
                        item_number=_extract_item(act),
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
