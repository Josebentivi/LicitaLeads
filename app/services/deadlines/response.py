"""Deadlines that can establish an open response window from an official act."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo
from datetime import timezone as fixed_timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from app.models.enums import ProcurementEventType
from app.services.event_detection.authority import fold
from app.services.event_detection.detector import associate_act_company, scope_numbers
from app.services.event_detection.types import CompanyReference, EventType
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.models import Company, Document, ProcurementEvent

OFFICIAL_RESPONSE_DEADLINE_LABELS = {
    "diligence_response": "Resposta à diligência",
    "appeal_intent": "Intenção de recurso",
    "appeal_submission": "Apresentação de recurso",
    "counterarguments_submission": "Apresentação de contrarrazões",
}
OFFICIAL_RESPONSE_DEADLINE_TYPES = tuple(OFFICIAL_RESPONSE_DEADLINE_LABELS)
RESPONSE_SCOPE_OBSERVATION = "official_response_scope"
RESPONSE_ACT_OBSERVATION = "official_response_act"
RESPONSE_PROOF_VERSION = 3
OFFICIAL_RESPONSE_EVENT_TYPES = {
    "diligence_response": (
        ProcurementEventType.DILIGENCE_REQUESTED,
        ProcurementEventType.DOCUMENTATION_REQUESTED,
    ),
    "appeal_intent": (ProcurementEventType.APPEAL_WINDOW_OPENED,),
    "appeal_submission": (ProcurementEventType.APPEAL_WINDOW_OPENED,),
    "counterarguments_submission": (ProcurementEventType.COUNTERARGUMENT_OPENED,),
}
RESPONSE_CLOSING_EVENT_TYPES = {
    "diligence_response": (
        ProcurementEventType.QUALIFIED,
        ProcurementEventType.INELIGIBLE,
        ProcurementEventType.DISQUALIFIED,
    ),
    "appeal_intent": (ProcurementEventType.INTENT_TO_APPEAL,),
    "appeal_submission": (ProcurementEventType.APPEAL_SUBMITTED,),
    "counterarguments_submission": (ProcurementEventType.COUNTERARGUMENT_SUBMITTED,),
}
RESPONSE_INTERRUPTION_EVENT_TYPES = {
    ProcurementEventType.SESSION_SUSPENDED,
    ProcurementEventType.HOMOLOGATED,
    ProcurementEventType.APPEAL_DECIDED,
    *(event for events in RESPONSE_CLOSING_EVENT_TYPES.values() for event in events),
}
RESPONSE_OPENING_EVENT_TYPES = {
    event for events in OFFICIAL_RESPONSE_EVENT_TYPES.values() for event in events
}
_PROCESS_SCOPE = re.compile(
    r"\b(?:toda\s+a\s+sessao|todo\s+o\s+certame|todos\s+os\s+(?:itens|lotes)|"
    r"certame\s+inteiro|sessao\s+inteira|contratacao\s+inteira)\b"
)
_SCOPE_ID = r"\d{1,12}[a-z]?(?:[.-]\d{1,12}[a-z]?)*"
_SCOPE_GROUP = re.compile(
    rf"\b(?P<kind>item|itens|lotes?)\s+(?:n(?:[oº°.])?\s*)?(?P<identifiers>{_SCOPE_ID}"
    rf"(?:\s*(?:,|(?:e|ou|ate|a)\b)\s*(?!\d{{1,2}}[/.-]\d{{1,2}}[/.-]\d{{2,4}}\b){_SCOPE_ID})*)\b"
)
_SCOPE_EXCEPT = re.compile(r"\b(?:exceto|salvo|ressalvad[oa]s?|com\s+excecao\s+d[eo]s?)\b")
_SCOPE_RESTRICTION = re.compile(
    r"\b(?:apenas|somente|exclusivamente|restrit[ao]s?|parcial(?:mente)?)\b"
)

_DATE_AND_TIME = (
    r"(?P<day>\d{1,2})[/.-](?P<month>\d{1,2})[/.-](?P<year>20\d{2})"
    r"[\s,]+(?:as\s+)?(?P<hour>\d{1,2})(?::(?P<minute>\d{2})(?::(?P<second>\d{2}))?"
    r"|h(?P<h_minute>\d{2})?)?(?=$|z\b|[^\w:])"
)
_END_WORDING = r"\b(?:ate|encerra(?:-se)?\s+em|vence\s+em)\s+(?:(?:o\s+)?dia\s+)?"
_END_MARKER = re.compile(_END_WORDING + r"(?=\d{1,2}[/.-]\d)")
_END = re.compile(_END_WORDING + _DATE_AND_TIME)
_START_WORDING = (
    r"\b(?:a\s+partir\s+d[aeo]|inicio\s+(?:em|as)|(?:abert[oa]|iniciad[oa]|convocad[oa])\s+em"
    r"|contad[oa]s?\s+(?:de|do|da|desde)|a\s+contar\s+d[aeo])\s+"
)
_START_MARKER = re.compile(_START_WORDING)
_START = re.compile(_START_WORDING + r"(?:(?:o\s+)?dia\s+)?" + _DATE_AND_TIME)
_DEFERRED = re.compile(
    r"\b(?:apos|mediante)\s+(?:a\s+|o\s+)?(?:recebimento|ciencia|intimacao|notificacao|publicacao)\b"
    r"|\b(?:quando|assim\s+que|caso|se)\s+(?:for|houver|receber|ocorrer|tiver)\b"
)
_STAMP = re.compile(r"^\s*\[?" + _DATE_AND_TIME)
_STAMP_MARKER = re.compile(r"^\s*\[?\d{1,2}[/.-]\d{1,2}[/.-]20\d{2}[\s,]+(?:as\s+)?\d")
_DURATION_PREFIX = r"\b(?:prazo\s+de|dentro\s+de|em\s+ate|por)\s+"
_DURATION_MARKER = re.compile(
    _DURATION_PREFIX
    + r"(?=\d+(?:[.,]\d+)?\s*(?:[hm](?=\d|\b)|horas?|minutos?|segundos?|dias?|semanas?))"
)
_DURATION = re.compile(
    _DURATION_PREFIX
    + r"(?:(?P<hours>\d{1,3})\s+horas?(?:\s+(?:e\s+)?(?P<extra>\d{1,3})\s+minutos?)?"
    r"|(?P<minutes>\d{1,3})\s+minutos?)\b"
    r"(?!\s*(?:uteis|de\s+expediente|\d|e\s+(?:\d|meia\b|meio\b|um[ a]|dois\b|duas\b)))"
)
_OPENING = re.compile(r"\b(?:abert[oa]|concedid[oa]|iniciad[oa]|abriu-se|concedeu-se)\b")
_EXTENSION = re.compile(r"\b(?:prorrogad[oa]|reabert[oa])\b")
_REQUEST = re.compile(r"\b(?:convocad[oa]|convoca(?:-se)?|solicitad[oa]|solicita(?:-se)?)\b")
_NOT_OPEN = re.compile(
    r"\b(?:nao\s+(?:(?:foi|esta|fica)\s+)?|(?:sera|podera\s+ser|devera\s+ser)\s+)"
    r"(?:abert[oa]|concedid[oa]|iniciad[oa]|convocad[oa]|solicitad[oa]|prorrogad[oa]|reabert[oa])\b"
    r"|\bprazo\s+(?:(?:foi|esta)\s+)?(?:encerrado|expirado|suspenso|cancelado)\b"
)
_OTHER_TIMEZONE = re.compile(
    r"\b(?:brt|brst|est|edt|pst|pdt|cest|cet|bst)\b"
    r"|\bhorario\s+(?:local|d[eo]\s+(?!brasilia\b))"
)
_UTC_MENTION = re.compile(r"\b(?:utc|gmt)\b")
_EXPLICIT_ZONE = re.compile(
    r"[ \t]*(?:(?P<utc>utc|gmt)\b(?:[ \t]*(?P<sign>[+-])(?P<h>\d{1,2})(?::(?P<m>\d{2}))?)?"
    r"|z\b|(?P<numeric_sign>[+-])(?P<numeric_h>\d{2}):(?P<numeric_m>\d{2}))(?![\w:]|[ \t]*[+-]\d)"
)
_POSSIBLE_TIME_OFFSET = re.compile(r"[ \t]*[+-]\d")
_COLLECTIVE_RECIPIENTS = re.compile(
    r"\b(?:demais|outr[oa]s|tod[oa]s|quaisquer)\s+(?:as\s+|os\s+)?(?:licitantes|empresas|fornecedor[ae]s)\b"
)


@dataclass(frozen=True, slots=True)
class OfficialResponseDeadline:
    kind: str
    starts_at: datetime
    ends_at: datetime
    extension: bool = False


def _scope_ids(text: str) -> dict[str, set[str]] | None:
    result: dict[str, set[str]] = {"item": set(), "lote": set()}
    for match in _SCOPE_GROUP.finditer(text):
        if re.match(r"\s+d(?:o|este|aquele)\s+edital\b", text[match.end() :]):
            continue
        identifiers = match["identifiers"]
        kind = "item" if match["kind"].startswith("it") else "lote"
        for part in re.split(r"\s*(?:,|\be\b)\s*", identifiers):
            if re.fullmatch(_SCOPE_ID, part):
                result[kind].add(part.lstrip("0") or part)
                continue
            interval = re.fullmatch(r"(\d+)\s+(?:a|ate)\s+(\d+)", part)
            if interval is None:
                return None
            first, last = map(int, interval.groups())
            if not 0 < first <= last <= first + 250:
                return None
            result[kind].update(str(value) for value in range(first, last + 1))
    return result


def response_scope(act: str) -> dict[str, str]:
    text = fold(act)
    parts = _SCOPE_EXCEPT.split(text)
    included = _scope_ids(parts[0])
    scope = {"scope": "unknown", "item": "", "lote": ""}
    if included is None:
        return scope
    items, lots = included["item"], included["lote"]
    whole = bool(_PROCESS_SCOPE.search(parts[0])) and not _SCOPE_RESTRICTION.search(parts[0])
    if len(parts) > 1:
        if len(parts) != 2 or not whole or items or lots:
            return scope
        excluded = _scope_ids(parts[1])
        if excluded is not None and any(excluded.values()):
            return {
                **scope,
                "scope": "procurement_except",
                "excluded_items": _packed_ids(excluded["item"]),
                "excluded_lots": _packed_ids(excluded["lote"]),
            }
        # "Todos os lotes, exceto 2 e 3" carries its scope into the exception.
        collective = re.search(r"\btodos\s+os\s+(itens|lotes)\b", parts[0])
        implicit_ids = _scope_ids(collective[1] + " " + parts[1].strip()) if collective else None
        if implicit_ids is not None and any(implicit_ids.values()):
            return {
                **scope,
                "scope": "procurement_except",
                "excluded_items": _packed_ids(implicit_ids["item"]),
                "excluded_lots": _packed_ids(implicit_ids["lote"]),
            }
        return scope
    if len(lots) > 1:
        return {**scope, "scope": "lot_list", "lots": _packed_ids(lots)} if not items else scope
    if len(items) > 1:
        return {
            **scope,
            "scope": "item_list",
            "items": _packed_ids(items),
            "lote": next(iter(lots), ""),
        }
    return {
        "scope": "item" if items else "lot" if lots else "procurement" if whole else "unknown",
        "item": next(iter(items), ""),
        "lote": next(iter(lots), ""),
    }


def _packed_ids(identifiers: set[str]) -> str:
    return "|" + "|".join(sorted(identifiers)) + "|" if identifiers else ""


def _read_times(
    matches: list[re.Match[str]], default_zone: tzinfo, act: str, zones: set[int]
) -> set[datetime] | None:
    instants = set()
    for match in matches:
        try:
            zone = default_zone
            if suffix := _EXPLICIT_ZONE.match(act, match.end()):
                offset = 0
                sign = suffix["sign"] or suffix["numeric_sign"]
                if sign:
                    hour = int(suffix["h"] or suffix["numeric_h"])
                    minute = int(suffix["m"] or suffix["numeric_m"] or 0)
                    if hour > 23 or minute > 59:
                        return None
                    offset = (hour * 60 + minute) * (-1 if sign == "-" else 1)
                zone = fixed_timezone(timedelta(minutes=offset))
                if suffix["utc"]:
                    zones.add(suffix.start("utc"))
            elif _POSSIBLE_TIME_OFFSET.match(act, match.end()):
                return None
            instants.add(
                datetime(
                    int(match["year"]),
                    int(match["month"]),
                    int(match["day"]),
                    int(match["hour"]),
                    int(match["minute"] or match["h_minute"] or 0),
                    int(match["second"] or 0),
                    tzinfo=zone,
                ).astimezone(UTC)
            )
        except ValueError:
            return None
    return instants


def parse_official_response_deadline(
    event_type: str, act_text: str, *, published_at: datetime | None, timezone: str
) -> OfficialResponseDeadline | None:
    """Use a performed summons/opening and a single evidenced time range."""
    act = fold(act_text)
    if (
        _NOT_OPEN.search(act)
        or _OTHER_TIMEZONE.search(act)
        or _COLLECTIVE_RECIPIENTS.search(act)
        or _DEFERRED.search(act)
    ):
        return None
    extension = bool(_EXTENSION.search(act))
    if event_type in {"DILIGENCE_REQUESTED", "DOCUMENTATION_REQUESTED"} and (
        _REQUEST.search(act) or extension
    ):
        kind = "diligence_response"
    elif event_type == "APPEAL_WINDOW_OPENED" and (_OPENING.search(act) or extension):
        kind = "appeal_intent" if re.search(r"\bintencao\s+de\b", act) else "appeal_submission"
        if kind == "appeal_intent" and re.search(r"\brazoes\s+recursais\b", act):
            return None
    elif event_type == "COUNTERARGUMENT_OPENED" and (_OPENING.search(act) or extension):
        kind = "counterarguments_submission"
    else:
        return None

    def matches_all(
        pattern: re.Pattern[str], markers: re.Pattern[str]
    ) -> list[re.Match[str]] | None:
        matches = list(pattern.finditer(act))
        return (
            matches
            if {match.start() for match in matches}
            == {marker.start() for marker in markers.finditer(act)}
            else None
        )

    stated_starts = matches_all(_START, _START_MARKER)
    log_starts = matches_all(_STAMP, _STAMP_MARKER)
    stated_ends = matches_all(_END, _END_MARKER)
    stated_durations = matches_all(_DURATION, _DURATION_MARKER)
    if any(
        matches is None for matches in (stated_starts, log_starts, stated_ends, stated_durations)
    ):
        return None

    zone: tzinfo = ZoneInfo(
        "America/Sao_Paulo" if re.search(r"\bhorario\s+d[eo]\s+brasilia\b", act) else timezone
    )
    zones: set[int] = set()
    # An explicitly scheduled start supersedes the log entry's timestamp.
    stated = _read_times(stated_starts or [], zone, act, zones)
    logged = _read_times(log_starts or [], zone, act, zones)
    ends = _read_times(stated_ends or [], zone, act, zones)
    if (
        stated is None
        or logged is None
        or {match.start() for match in _UTC_MENTION.finditer(act)} != zones
    ):
        return None
    starts = stated or logged
    durations = {
        timedelta(
            hours=int(match["hours"] or 0),
            minutes=int(match["minutes"] or match["extra"] or 0),
        )
        for match in stated_durations or []
    }
    if starts is None or len(starts) > 1 or ends is None or len(ends) > 1 or len(durations) > 1:
        return None
    if starts:
        starts_at = next(iter(starts))
    elif ends and not durations and published_at is not None and published_at.tzinfo is not None:
        # Publication of this explicit opening can establish that it has taken effect.
        starts_at = published_at
    else:
        return None
    if durations:
        duration_end = starts_at + next(iter(durations))
        if ends and duration_end not in ends:
            return None
        ends = {duration_end}
    if not ends:
        return None
    ends_at = next(iter(ends))
    if ends_at <= starts_at:
        return None
    return OfficialResponseDeadline(kind, starts_at, ends_at, extension)


async def verified_event_response_deadline(
    session: AsyncSession,
    event: ProcurementEvent,
    company: Company,
    document: Document | None,
    act: str | None,
    *,
    timezone: str,
) -> OfficialResponseDeadline | None:
    from app.models import Procurement, ProcurementItem

    if (
        not act
        or not document
        or not document.is_active
        or document.procurement_id != event.procurement_id
        or company.id != event.company_id
        or not is_valid_cnpj(company.cnpj)
        or event.requires_manual_review
    ):
        return None
    target = CompanyReference(
        str(company.id), company.legal_name or company.normalized_name, company.cnpj
    )
    if associate_act_company(EventType(event.event_type.value), act, (target,))[0] is None:
        return None
    scope = response_scope(act)
    if scope["scope"] in {"item_list", "lot_list", "procurement_except"} or re.search(
        r"\b(?:itens|lotes)\s+\d", act, re.I
    ):
        return None
    items = scope_numbers(act, "item")
    if len(items) > 1 or len(scope_numbers(act, "lote")) > 1:
        return None
    if event.item_id:
        item = await session.get(ProcurementItem, event.item_id)
        if not item or item.procurement_id != event.procurement_id or item.item_number not in items:
            return None
    window = parse_official_response_deadline(
        event.event_type.value, act, published_at=document.published_at, timezone=timezone
    )
    if window is None:
        return None
    procurement = await session.get(Procurement, event.procurement_id)
    if not procurement or normalize_cnpj(procurement.agency_cnpj) == normalize_cnpj(company.cnpj):
        return None
    return window
