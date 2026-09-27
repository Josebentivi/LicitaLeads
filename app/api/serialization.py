"""Serialization helpers that keep ORM objects out of public contracts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import inspect


def json_value(value: Any) -> Any:
    """Convert domain values into JSON-compatible values."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    return value


def model_dict(instance: Any, *, include: set[str] | None = None) -> dict[str, Any]:
    """Serialize mapped columns only, avoiding implicit relationship queries."""
    mapper = inspect(instance).mapper
    values: dict[str, Any] = {}
    for column in mapper.column_attrs:
        key = column.key
        if include is not None and key not in include:
            continue
        values[key] = json_value(getattr(instance, key))
    return values
