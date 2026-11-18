"""Index only published SCPI budget dimensions, preserving the exact response and commitment."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import batched
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.connectors.municipal_payments.sources import MUNICIPAL_SOURCES
from app.models import SourceRecord
from app.models.entities import MunicipalBudgetEntry

_SCPI = {f"municipal:{item.id}": item for item in MUNICIPAL_SOURCES if item.is_fiorilli}
_LIST = "DespesasGerais"
_DETAIL = "EmpenhosDespesas_Pago_PorNumeroEmpenho"


@dataclass(frozen=True)
class BudgetResponse:
    id: UUID
    source: str
    params: dict[str, Any]
    payload: Any
    http_status: int | None


def _text(value: Any) -> str:
    return str(value).strip() if type(value) in (int, str) else ""


def _portal_reference(value: Any) -> str | None:
    # Only structured administrative references. Free text, CPF and contact data stay private.
    ref = _text(value)
    if not re.fullmatch(r"(?=[A-Za-z0-9./ -]*\d)[A-Za-z]{0,8}[ -]?[\d./-]{1,60}", ref):
        return None
    if len(re.sub(r"\D", "", ref)) == 11 or re.search(
        r"(?<!\d)\d{3}\.?\d{3}\.?\d{3}-?\d{2}(?!\d)", ref
    ):
        return None
    return ref if any(char in "123456789" for char in ref) else None


def budget_function_label(value: Any) -> str | None:
    name = " ".join(_text(value).split())
    return (
        name
        if name and len(name) <= 120 and all(c.isalpha() or c in " -(),/" for c in name)
        else None
    )


def public_budget_values(entry: MunicipalBudgetEntry) -> dict[str, Any]:
    """Apply the same allowlist when reading indexes created by an earlier version."""
    return {
        "source_record_id": entry.source_record_id,
        "classification_record_id": entry.classification_record_id,
        "municipality": entry.municipality,
        "fiscal_year": entry.fiscal_year,
        "commitment_identity": entry.commitment_identity,
        "function_name": budget_function_label(entry.function_name),
        "function_code": entry.function_code,
        "portal_process": _portal_reference(entry.portal_process),
        "portal_bid": _portal_reference(entry.portal_bid),
    }


def projection_rows(responses: Iterable[BudgetResponse]) -> Iterator[dict[str, Any]]:
    """One run at a time. Detail classification requires the matching parent in this run."""
    parents: dict[str, dict[str, Any] | None] = {}
    details: list[tuple[BudgetResponse, str]] = []
    for response in responses:
        source = _SCPI.get(response.source)
        params = response.params
        if (
            not source
            or not isinstance(params, dict)
            or not isinstance(response.payload, list)
            or not (200 <= (response.http_status or 0) < 300)
        ):
            continue
        year = _text(params.get("Exercicio"))
        entity = _text(params.get("Empresa"))
        if entity != source.entity_code or not re.fullmatch(r"20\d{2}|2100", year):
            continue
        if any(_text(params[key]) != year for key in ("Ano", "ConectarExercicio") if key in params):
            continue
        prefix = f"{source.id}:{year}:" if entity == "1" else f"{source.id}:e{entity}:{year}:"
        if params.get("Listagem") == _DETAIL:
            details.append((response, prefix))
            continue
        if params.get("Listagem") != _LIST:
            continue
        rows = [
            row
            for row in response.payload
            if isinstance(row, dict)
            and (
                _text(row.get("EMPRESA")) == entity
                or (not source.filter_client_entity and row.get("EMPRESA") is None)
            )
        ]
        keys = Counter(_text(row.get("PKEMP")) for row in rows)
        for row in rows:
            key = _text(row.get("PKEMP"))
            if not re.fullmatch(r"\d{1,30}", key):
                continue
            identity = prefix + key
            if keys[key] != 1:
                parents[identity] = None
                continue
            code = _text(row.get("FUNCAO"))
            values = dict(
                source_record_id=response.id,
                classification_record_id=response.id,
                municipality=source.id,
                fiscal_year=int(year),
                commitment_identity=identity,
                function_name=budget_function_label(row.get("FUNCAONOME")),
                function_code=code if re.fullmatch(r"\d{1,3}", code) else None,
                portal_process=_portal_reference(row.get("PROC")),
                portal_bid=_portal_reference(row.get("NUMLICIT")),
            )
            yield values
            if identity in parents:
                previous = parents[identity]
                if previous is None or any(
                    values[k] != previous[k]
                    for k in ("function_name", "function_code", "portal_process", "portal_bid")
                ):
                    parents[identity] = None
            else:
                parents[identity] = values
    for response, prefix in details:
        detail_keys = {_text(row.get("PKEMP")) for row in response.payload if isinstance(row, dict)}
        for key in detail_keys:
            parent = parents.get(prefix + key)
            if parent is not None:
                yield {**parent, "source_record_id": response.id}


def store_projection(connection: Connection, rows: Iterable[dict[str, Any]]) -> None:
    insert = pg_insert if connection.dialect.name == "postgresql" else sqlite_insert
    statement = insert(MunicipalBudgetEntry).on_conflict_do_nothing(
        index_elements=["source_record_id", "commitment_identity"]
    )
    for block in batched(rows, 400):
        connection.execute(statement, list(block))


def backfill_budget_projection(connection: Connection) -> None:
    """Populate the derived index from archived successful responses, without changing facts."""
    src = SourceRecord.__table__.c
    scopes = connection.execute(
        select(src.crawl_run_id, src.source).where(src.source.in_(_SCPI)).distinct()
    ).all()
    for run_id, source in scopes:
        records = connection.execute(
            select(src.id, src.source, src.request_parameters, src.raw_payload, src.http_status)
            .where(
                src.crawl_run_id == run_id, src.source == source, src.http_status.between(200, 299)
            )
            .order_by(src.id)
        )
        responses = (
            BudgetResponse(r.id, r.source, r.request_parameters, r.raw_payload, r.http_status)
            for r in records
        )
        if run_id is None:
            # Orphaned responses can identify their own rows, never each other's.
            for response in responses:
                store_projection(connection, projection_rows([response]))
        else:
            store_projection(connection, projection_rows(responses))
