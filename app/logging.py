"""Small structured-JSON logging setup with secret redaction."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

_REDACTED_KEYS = {"authorization", "api_key", "token", "password", "secret"}


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***" if any(part in key.lower() for part in _REDACTED_KEYS) else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    """Serialize standard log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(_redact(context))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger exactly once."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
