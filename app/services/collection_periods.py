"""Calendar bounds for newly requested procurement collections."""

from collections.abc import Iterator
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from app.services.procurement_search import normalized_search_text

_DISCOVERY_KINDS = {
    "publication": "publication",
    "publicacao": "publication",
    "update": "update",
    "atualizacao": "update",
    "open proposals": "open_proposals",
    "proposals": "open_proposals",
    "proposta": "open_proposals",
}


def normalize_discovery_kind(value: str) -> str:
    key = normalized_search_text(value)
    try:
        return _DISCOVERY_KINDS[key]
    except KeyError as exc:
        raise ValueError("Informe publicação, atualização ou propostas abertas.") from exc


def earliest_collection_day(today: date) -> date:
    day = 28 if (today.month, today.day) == (2, 29) else today.day
    return today.replace(year=today.year - 2, day=day)


def collection_today(timezone: str) -> date:
    return datetime.now(ZoneInfo(timezone)).date()


def validate_collection_period(
    start_date: date | None,
    end_date: date | None,
    *,
    timezone: str,
    days: int | None = None,
    today: date | None = None,
) -> tuple[date | None, date | None]:
    """Validate explicit dates or the fallback for a fresh request only."""
    if (start_date is None) != (end_date is None):
        raise ValueError("Informe as duas datas do período de coleta.")
    current_day = today or collection_today(timezone)
    earliest = earliest_collection_day(current_day)
    if start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ValueError("A data inicial não pode ser posterior à data final.")
        if start_date < earliest or end_date > current_day:
            raise ValueError("Selecione datas dentro dos últimos dois anos, até hoje.")
    elif days is not None and not 1 <= days <= (current_day - earliest).days + 1:
        raise ValueError("Selecione um período dentro dos últimos dois anos.")
    return start_date, end_date


def calendar_month_slices(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Partition an inclusive period from the most recent month backwards."""
    while end >= start:
        first = max(start, end.replace(day=1))
        yield first, end
        if first == start:
            break
        end = first - timedelta(days=1)


def use_calendar_slices(start: date | None, end: date | None, days: int | None = None) -> bool:
    if start is not None and end is not None:
        return (end - start).days >= 31
    return days is not None and days > 31
