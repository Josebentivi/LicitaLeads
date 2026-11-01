"""Optional operational signals, independent of source payloads and persistence."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    stage: str | None = None
    source: str | None = None
    operation: str | None = None
    subject: str | None = None
    message: str | None = None
    processed: int | None = None
    total: int | None = None
    page: int | None = None
    total_pages: int | None = None
    attempt: int | None = None
    max_attempts: int | None = None
    retry_at: str | None = None
    http_active: int | None = None
    http_limit: int | None = None
    http_peak: int | None = None
    http_waiting: int | None = None
    http_waiting_rate: int | None = None
    http_waiting_capacity: int | None = None
    http_waiting_checkpoint: int | None = None
    http_waiting_retry: int | None = None
    http_rate_interval_seconds: float | None = None
    http_cooldown_seconds: float | None = None
    download_active: int | None = None
    download_limit: int | None = None
    process_active: int | None = None
    process_limit: int | None = None
    queue_ready: int | None = None


ActivityCallback = Callable[[ProgressEvent], Awaitable[None]]
