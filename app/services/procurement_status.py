"""Operational procurement status classification shared across services.

The tokens mirror what the sources publish as free text (``situacaoCompraNome``
and ``situacaoCompraNomePncp``) and what the filters use to derive an
open/closed view.  Keeping them here avoids drift between ingestion (lead
trigger) and repositories (SQL filters).
"""

from __future__ import annotations

from datetime import UTC, datetime

CANCELLED_TOKENS = ("cancel", "revog", "anulad", "exclu")
SUSPENDED_TOKENS = ("suspens",)
CLOSED_TOKENS = ("encerr", "homolog", "adjudic", "desert", "fracass")
OPEN_TOKENS = ("andamento", "aberta", "aberto", "publicad", "divulgad", "recebendo")


def procurement_is_active(
    status: str | None,
    proposal_end_at: datetime | None,
    *,
    now: datetime | None = None,
) -> bool:
    """Return True only when the process can still receive proposals.

    Absence of both a proposal window and open status tokens is treated as
    unknown, never as open.
    """

    reference = now or datetime.now(UTC)
    text = (status or "").casefold()
    if any(token in text for token in CANCELLED_TOKENS + SUSPENDED_TOKENS + CLOSED_TOKENS):
        return False
    if proposal_end_at is not None:
        return proposal_end_at >= reference
    return any(token in text for token in OPEN_TOKENS)
