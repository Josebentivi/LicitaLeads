# ruff: noqa: E501

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .calendar import BrazilBusinessCalendar
from .types import DeadlineMethod, DeadlineRequest, DeadlineResult, DeadlineStatus

_LEGAL_ESTIMATE_EVENTS = {
    "INELIGIBLE",
    "DISQUALIFIED",
    "PROPOSAL_REJECTED",
    "INTENT_TO_APPEAL",
    "COUNTERARGUMENT_OPENED",
}


def _aware(value: datetime, timezone: ZoneInfo) -> tuple[datetime, bool]:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone), True
    return value.astimezone(timezone), False


def _pt_datetime(value: datetime, timezone: ZoneInfo) -> str:
    return value.astimezone(timezone).strftime("%d/%m/%Y às %H:%M")


class DeadlineEngine:
    def __init__(self, calendar: BrazilBusinessCalendar | None = None) -> None:
        self.calendar = calendar or BrazilBusinessCalendar()

    def calculate(self, request: DeadlineRequest) -> DeadlineResult:
        try:
            timezone = ZoneInfo(request.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {request.timezone}") from exc
        now = request.now or datetime.now(UTC)
        now_local, now_was_naive = _aware(now, timezone)
        trigger_local: datetime | None = None
        trigger_was_naive = False
        if request.trigger_at:
            trigger_local, trigger_was_naive = _aware(request.trigger_at, timezone)

        method = DeadlineMethod.UNKNOWN
        deadline_local: datetime | None = None
        explicit_local: datetime | None = None
        estimated_local: datetime | None = None
        confidence = 0.0
        excluded: tuple[tuple[date, str], ...] = ()
        business_days: int | None = None

        if request.explicit_deadline_at:
            deadline_local, deadline_naive = _aware(request.explicit_deadline_at, timezone)
            explicit_local = deadline_local
            method = DeadlineMethod.EXPLICIT
            confidence = 0.98
            trigger_was_naive = trigger_was_naive or deadline_naive
        elif request.document_deadline_at:
            deadline_local, deadline_naive = _aware(request.document_deadline_at, timezone)
            explicit_local = deadline_local
            method = DeadlineMethod.DOCUMENT_EXTRACTED
            confidence = 0.90
            trigger_was_naive = trigger_was_naive or deadline_naive
        elif request.edital_business_days is not None and trigger_local:
            business_days = request.edital_business_days
            due_date, excluded = self.calendar.add_business_days(
                trigger_local.date(),
                business_days,
                uf=request.uf,
                municipality_ibge=request.municipality_ibge,
            )
            deadline_local = datetime.combine(due_date, time(23, 59, 59), timezone)
            estimated_local = deadline_local
            method = DeadlineMethod.EDITAL_RULE
            confidence = 0.80
        elif self._legal_estimate_is_safe(request, trigger_local):
            assert trigger_local is not None
            business_days = request.legal_business_days
            due_date, excluded = self.calendar.add_business_days(
                trigger_local.date(),
                business_days,
                uf=request.uf,
                municipality_ibge=request.municipality_ibge,
            )
            deadline_local = datetime.combine(due_date, time(23, 59, 59), timezone)
            estimated_local = deadline_local
            method = DeadlineMethod.LEGAL_ESTIMATE
            confidence = 0.65

        review_reasons: list[str] = []
        if method is DeadlineMethod.LEGAL_ESTIMATE:
            review_reasons.append("prazo é estimativa jurídica")
        if (
            method in {DeadlineMethod.EDITAL_RULE, DeadlineMethod.LEGAL_ESTIMATE}
            and not request.local_holiday_calendar_complete
        ):
            review_reasons.append("feriados estaduais/municipais não confirmados")
            confidence = min(confidence, 0.65)
        if request.timezone_inferred or trigger_was_naive or now_was_naive:
            review_reasons.append("timezone inferido")
            confidence = max(0.0, confidence - 0.05)
        if request.session_suspended and request.reopened_at is None:
            review_reasons.append("sessão suspensa sem data segura de reabertura")
            confidence = min(confidence, 0.40)

        if deadline_local is None:
            status = DeadlineStatus.UNKNOWN
            review_reasons.append("não há marco e regra suficientes para calcular")
        elif request.session_suspended and request.reopened_at is None:
            status = DeadlineStatus.REQUIRES_REVIEW
        else:
            status = self._status(deadline_local, now_local)
        requires_review = bool(review_reasons)
        remaining = int((deadline_local - now_local).total_seconds()) if deadline_local else None
        explanation = self._explain(
            request,
            method,
            trigger_local,
            deadline_local,
            business_days,
            excluded,
            review_reasons,
            confidence,
            timezone,
        )
        return DeadlineResult(
            method=method,
            trigger_at=trigger_local.astimezone(UTC) if trigger_local else None,
            deadline_at=deadline_local.astimezone(UTC) if deadline_local else None,
            explicit_deadline_at=explicit_local.astimezone(UTC) if explicit_local else None,
            estimated_deadline_at=estimated_local.astimezone(UTC) if estimated_local else None,
            remaining_seconds=remaining,
            status=status,
            confidence=round(confidence, 2),
            requires_manual_review=requires_review,
            legal_basis=request.legal_basis,
            calculation_explanation=explanation,
            excluded_dates=tuple(f"{day.isoformat()}: {reason}" for day, reason in excluded),
        )

    @staticmethod
    def _legal_estimate_is_safe(request: DeadlineRequest, trigger: datetime | None) -> bool:
        if trigger is None or request.legal_business_days <= 0:
            return False
        applicable = request.legal_estimate_applicable
        if applicable is False:
            return False
        return request.event_type.upper() in _LEGAL_ESTIMATE_EVENTS

    @staticmethod
    def _status(deadline: datetime, now: datetime) -> DeadlineStatus:
        if deadline < now:
            return DeadlineStatus.EXPIRED
        if deadline.date() == now.date():
            return DeadlineStatus.DUE_TODAY
        if (deadline - now).total_seconds() <= 24 * 60 * 60:
            return DeadlineStatus.DUE_WITHIN_24H
        return DeadlineStatus.OPEN

    @staticmethod
    def _explain(
        request: DeadlineRequest,
        method: DeadlineMethod,
        trigger: datetime | None,
        deadline: datetime | None,
        business_days: int | None,
        excluded: tuple[tuple[date, str], ...],
        review_reasons: list[str],
        confidence: float,
        timezone: ZoneInfo,
    ) -> str:
        lines: list[str] = []
        if trigger:
            lines.append(f"Marco inicial: {_pt_datetime(trigger, timezone)}.")
            if request.trigger_source:
                lines.append(f"Origem: {request.trigger_source}.")
        if method is DeadlineMethod.EXPLICIT:
            lines.append("Regra utilizada: prazo explicitamente publicado.")
        elif method is DeadlineMethod.DOCUMENT_EXTRACTED:
            lines.append("Regra utilizada: prazo extraído de documento oficial.")
        elif method is DeadlineMethod.EDITAL_RULE:
            lines.append(f"Regra utilizada: {business_days} dias úteis previstos no edital.")
        elif method is DeadlineMethod.LEGAL_ESTIMATE:
            lines.append(f"Regra utilizada: estimativa configurada de {business_days} dias úteis.")
        else:
            lines.append("Regra utilizada: nenhuma; prazo mantido como desconhecido.")
        if excluded:
            descriptions = ", ".join(
                f"{day.strftime('%d/%m')} ({reason})" for day, reason in excluded
            )
            lines.append(f"Dias excluídos: {descriptions}.")
        if deadline:
            label = (
                "Prazo estimado"
                if method in {DeadlineMethod.EDITAL_RULE, DeadlineMethod.LEGAL_ESTIMATE}
                else "Prazo"
            )
            lines.append(f"{label}: {_pt_datetime(deadline, timezone)}.")
        if review_reasons:
            lines.append("Revisão humana recomendada: " + "; ".join(review_reasons) + ".")
        lines.append(f"Confiança: {confidence:.0%}.")
        return "\n".join(lines)
