"""Reserve header capacity before concurrent ARP page requests are sent."""

import asyncio
from typing import Any

from app.connectors.http import ConnectorContractError


class DiscoveryLimitReached(ConnectorContractError):
    """A deliberate collection limit, rather than a source failure."""


class DiscoveryBudget:
    def __init__(self, limit: int) -> None:
        self.remaining = limit
        self.pending: dict[str, int] = {}
        self.changed = asyncio.Event()

    def reserve(self, key: str, page_size: int) -> bool:
        if key in self.pending:
            return True
        if self.remaining <= 0:
            return False
        reserved = min(self.remaining, page_size)
        self.remaining -= reserved
        self.pending[key] = reserved
        return True

    def settle(self, key: str, payload: Any = None) -> None:
        reserved = self.pending.pop(key, None)
        if reserved is None:
            return
        rows = (
            payload
            if isinstance(payload, list)
            else (
                payload.get("data", payload.get("resultado", []))
                if isinstance(payload, dict)
                else []
            )
        )
        received = len(rows) if isinstance(rows, list) else 0
        # The last page may exceed its remaining credits. Keep the entire
        # response durable and let the domain consumer queue those extra rows.
        self.remaining += reserved - received
        self.changed.set()
        self.changed = asyncio.Event()
