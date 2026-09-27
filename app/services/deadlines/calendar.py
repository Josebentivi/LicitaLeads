# ruff: noqa: E501

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TextIO


@dataclass(frozen=True, slots=True)
class Holiday:
    date: date
    name: str
    scope: str = "national"
    uf: str | None = None
    municipality_ibge: str | None = None


def national_holidays(year: int) -> tuple[Holiday, ...]:
    """Brazilian nationwide civil holidays.

    State/municipal religious holidays are intentionally not guessed.  They can
    be supplied through the documented CSV calendar.
    """

    fixed = (
        (1, 1, "Confraternização Universal"),
        (4, 21, "Tiradentes"),
        (5, 1, "Dia Mundial do Trabalho"),
        (9, 7, "Independência do Brasil"),
        (10, 12, "Nossa Senhora Aparecida"),
        (11, 2, "Finados"),
        (11, 15, "Proclamação da República"),
        (11, 20, "Dia Nacional de Zumbi e da Consciência Negra"),
        (12, 25, "Natal"),
    )
    return tuple(Holiday(date(year, month, day), name) for month, day, name in fixed)


class BrazilBusinessCalendar:
    def __init__(self, holidays: tuple[Holiday, ...] | list[Holiday] = ()) -> None:
        self.holidays = tuple(holidays)

    @classmethod
    def from_csv(cls, source: str | Path | TextIO) -> BrazilBusinessCalendar:
        stream: TextIO
        if isinstance(source, Path):
            stream = source.open("r", encoding="utf-8-sig", newline="")
            close = True
        elif isinstance(source, str):
            path = Path(source)
            if "\n" not in source and path.exists():
                stream = path.open("r", encoding="utf-8-sig", newline="")
            else:
                stream = io.StringIO(source)
            close = True
        else:
            stream = source
            close = False
        try:
            reader = csv.DictReader(stream)
            expected = {"date", "name", "scope", "uf", "municipality_ibge"}
            if not reader.fieldnames or not expected.issubset(reader.fieldnames):
                raise ValueError(f"holiday CSV must contain columns: {', '.join(sorted(expected))}")
            holidays: list[Holiday] = []
            for line_number, row in enumerate(reader, start=2):
                try:
                    holiday_date = date.fromisoformat((row["date"] or "").strip())
                except ValueError as exc:
                    raise ValueError(f"invalid holiday date on line {line_number}") from exc
                scope = (row["scope"] or "").strip().lower()
                if scope not in {"national", "state", "municipal"}:
                    raise ValueError(f"invalid holiday scope on line {line_number}")
                uf = (row.get("uf") or "").strip().upper() or None
                municipality = (row.get("municipality_ibge") or "").strip() or None
                if scope == "state" and not uf:
                    raise ValueError(f"state holiday without UF on line {line_number}")
                if scope == "municipal" and not municipality:
                    raise ValueError(
                        f"municipal holiday without municipality_ibge on line {line_number}"
                    )
                holidays.append(
                    Holiday(
                        date=holiday_date,
                        name=(row["name"] or "").strip() or "Feriado sem nome",
                        scope=scope,
                        uf=uf,
                        municipality_ibge=municipality,
                    )
                )
            return cls(holidays)
        finally:
            if close:
                stream.close()

    def holiday_on(
        self, day: date, *, uf: str | None = None, municipality_ibge: str | None = None
    ) -> Holiday | None:
        candidates = [*national_holidays(day.year), *self.holidays]
        normalized_uf = uf.upper() if uf else None
        for holiday in candidates:
            if holiday.date != day:
                continue
            if holiday.scope == "national":
                return holiday
            if holiday.scope == "state" and holiday.uf == normalized_uf:
                return holiday
            if holiday.scope == "municipal" and holiday.municipality_ibge == municipality_ibge:
                return holiday
        return None

    def is_business_day(
        self, day: date, *, uf: str | None = None, municipality_ibge: str | None = None
    ) -> bool:
        return (
            day.weekday() < 5
            and self.holiday_on(day, uf=uf, municipality_ibge=municipality_ibge) is None
        )

    def add_business_days(
        self,
        start: date,
        days: int,
        *,
        uf: str | None = None,
        municipality_ibge: str | None = None,
    ) -> tuple[date, tuple[tuple[date, str], ...]]:
        """Exclude the start date and include the Nth following business day."""

        if days < 0:
            raise ValueError("business days must be non-negative")
        current = start
        counted = 0
        excluded: list[tuple[date, str]] = []
        while counted < days:
            current += timedelta(days=1)
            holiday = self.holiday_on(current, uf=uf, municipality_ibge=municipality_ibge)
            if current.weekday() >= 5:
                excluded.append((current, "fim de semana"))
            elif holiday:
                excluded.append((current, holiday.name))
            else:
                counted += 1
        return current, tuple(excluded)
