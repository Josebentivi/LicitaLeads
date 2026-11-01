"""Durable, fenced snapshots and work queues for bounded collections."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.connectors.base import ConnectorResult, DataAvailability, RawSourceRecord
from app.connectors.continuation import ResponseReplay, request_key
from app.connectors.http import response_validators
from app.database import write_session
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    SourceRecord,
    SourceResponseValidator,
)

logger = logging.getLogger(__name__)


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def stream_key(filters: dict[str, Any]) -> str:
    return digest(
        {
            key: value
            for key, value in filters.items()
            if key
            not in {
                "continuation_batch_id",
                "resume_automatically",
                "retry_of_run_id",
            }
            and not (key == "force_refresh" and value is False)
        }
    )


def row_identity(raw: Any) -> str:
    data = raw.model_dump(mode="json")
    for name in (
        "observed_at",
        "source_record_ref",
        "source_record_id",
        "source_url",
        "collection_work_id",
        "collection_stage",
    ):
        data.pop(name, None)
    return digest(data)


class CollectionCoordinator:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        run_id: UUID,
        fence: Callable[[AsyncSession], Awaitable[None]],
    ) -> None:
        self.sessions, self.run_id, self.fence = sessions, run_id, fence
        self.batch_id: UUID | None = None
        self.replays: dict[str, ResponseReplay] = {}
        self.items: dict[str, UUID] = {}
        self.current_item_id: UUID | None = None

    async def start(
        self, original: dict[str, Any], fixed: dict[str, Any], batch_id: UUID | None = None
    ) -> dict[str, Any]:
        async with write_session(self.sessions) as session:
            await self.fence(session)
            run = await session.get_one(CrawlRun, self.run_id)
            batch = await session.get(CollectionBatch, batch_id) if batch_id else None
            if batch_id and batch is None:
                raise ValueError("lote de coleta não encontrado")
            if batch is None:
                batch = CollectionBatch(stream_key=stream_key(original), filters=fixed, cursors={})
                session.add(batch)
                await session.flush()
            batch.last_run_id = self.run_id
            batch.status = "running"
            run.batch_id = batch.id
            run.status = CrawlRunStatus.RUNNING
            run.started_at = run.started_at or datetime.now(UTC)
            run.heartbeat_at = datetime.now(UTC)
            self.batch_id = batch.id
            from app.services.ingestion.scheduled_collections import bind_sync_batch

            await bind_sync_batch(session, original, batch)
            if batch.filters.get("start_date") and batch.filters.get("end_date"):
                context = original.get("sync_context") or {}
                run.cursor = {
                    **(run.cursor or {}),
                    "collection": {
                        "strategy": context.get(
                            "strategy",
                            "manual_complete"
                            if original.get("force_refresh")
                            else "manual_versions",
                        ),
                        "phase": context.get("phase"),
                        "start_date": batch.filters["start_date"],
                        "end_date": batch.filters["end_date"],
                    },
                }
            return dict(batch.filters)

    async def attach(self, connector: Any, *, force_refresh: bool = False) -> None:
        transport = getattr(connector, "_http", None)
        if transport is None or not hasattr(transport, "request_json"):
            return
        source = connector.name
        from app.services.ingestion.verified_versions import adapter_key

        client_headers = getattr(getattr(transport, "_client", None), "headers", {})
        validator_base = digest(
            [
                "http-validator-v1",
                adapter_key(connector),
                client_headers.get("User-Agent"),
                client_headers.get("Accept"),
            ]
        )

        async def checkpoint() -> None:
            async with write_session(self.sessions) as session:
                await self.fence(session)
                run = await session.get_one(CrawlRun, self.run_id)
                run.heartbeat_at = datetime.now(UTC)

        transport.checkpoint = checkpoint
        async with self.sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(CollectionWorkItem).where(
                            CollectionWorkItem.batch_id == self.batch_id,
                            CollectionWorkItem.source == source,
                            CollectionWorkItem.kind == "response",
                        )
                    )
                ).all()
            )
            records = [RawSourceRecord.model_validate(row.payload) for row in rows]

        async def save(raw: RawSourceRecord) -> None:
            snapshot_started = time.perf_counter()
            async with write_session(self.sessions) as session:
                await self.fence(session)
                run = await session.get_one(CrawlRun, self.run_id)
                now = datetime.now(UTC)
                run.heartbeat_at = now
                # Telemetry is owned by CrawlProgressRecorder. A response from
                # another concurrent operation must not reset its stage.
                fingerprint = digest(
                    [str(self.run_id), raw.record_key, raw.collected_at.isoformat()]
                )
                record = await session.scalar(
                    select(SourceRecord).where(SourceRecord.fingerprint == fingerprint)
                )
                if record is None:
                    record = SourceRecord(
                        crawl_run_id=self.run_id,
                        source=raw.source,
                        endpoint=raw.endpoint,
                        request_parameters=raw.request_parameters,
                        response_hash=raw.response_hash,
                        raw_payload=raw.raw_payload,
                        http_status=raw.http_status,
                        availability=DataAvailability.AVAILABLE,
                        collected_at=raw.collected_at,
                        fingerprint=fingerprint,
                    )
                    session.add(record)
                    await session.flush()
                identity = request_key(raw.endpoint, raw.request_parameters)
                work = await session.scalar(
                    select(CollectionWorkItem).where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                        CollectionWorkItem.kind == "response",
                        CollectionWorkItem.identity == identity,
                    )
                )
                if work is None:
                    work = CollectionWorkItem(
                        batch_id=self.batch_id,
                        source=source,
                        kind="response",
                        identity=identity,
                        payload={},
                    )
                    session.add(work)
                work.payload = raw.model_dump(mode="json")
                work.source_record_id = record.id
                if source in {"pncp", "compras_gov"}:
                    validator_key = digest([validator_base, identity])
                    validator = await session.scalar(
                        select(SourceResponseValidator).where(
                            SourceResponseValidator.validator_key == validator_key
                        )
                    )
                    etag, modified = (
                        raw.response_headers.get("etag"),
                        raw.response_headers.get("last-modified"),
                    )
                    if etag or modified:
                        if validator is None:
                            validator = SourceResponseValidator(
                                validator_key=validator_key, source=source
                            )
                            session.add(validator)
                        if (
                            validator.validated_at is None
                            or raw.collected_at >= validator.validated_at
                        ):
                            validator.source_record_id = record.id
                            validator.etag, validator.last_modified = etag, modified
                            validator.validated_at = raw.collected_at
                    elif validator is not None and raw.collected_at >= validator.validated_at:
                        await session.delete(validator)
            logger.info(
                "crawl_snapshot_timing",
                extra={
                    "context": {
                        "source": source,
                        "crawl_run_id": str(self.run_id),
                        "page": raw.request_parameters.get(
                            "pagina", raw.request_parameters.get("page")
                        ),
                        "snapshot_seconds": round(time.perf_counter() - snapshot_started, 6),
                    }
                },
            )

        async def invalidate() -> None:
            async with write_session(self.sessions) as session:
                await self.fence(session)
                await session.execute(
                    delete(CollectionWorkItem).where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                        CollectionWorkItem.kind == "response",
                    )
                )
                batch = await session.get_one(CollectionBatch, self.batch_id)
                batch.cursors = {
                    **batch.cursors,
                    source: {"resumable": True, "discovery_complete": False},
                }

        async def invalidate_partition(operation: str) -> None:
            async with write_session(self.sessions) as session:
                await self.fence(session)
                responses = (
                    await session.scalars(
                        select(CollectionWorkItem).where(
                            CollectionWorkItem.batch_id == self.batch_id,
                            CollectionWorkItem.source == source,
                            CollectionWorkItem.kind == "response",
                        )
                    )
                ).all()
                refs: set[str] = set()
                source_ids: set[UUID] = set()
                for work in responses:
                    raw = RawSourceRecord.model_validate(work.payload)
                    if (
                        request_key(raw.endpoint, raw.request_parameters, operation=True)
                        == operation
                    ):
                        refs.add(raw.record_key)
                        if work.source_record_id:
                            source_ids.add(work.source_record_id)
                        await session.delete(work)
                rows = (
                    await session.scalars(
                        select(CollectionWorkItem).where(
                            CollectionWorkItem.batch_id == self.batch_id,
                            CollectionWorkItem.source == source,
                            CollectionWorkItem.kind == "record",
                        )
                    )
                ).all()
                for work in rows:
                    if (
                        work.source_record_id in source_ids
                        or work.payload.get("source_record_ref") in refs
                    ):
                        await session.delete(work)
                batch = await session.get_one(CollectionBatch, self.batch_id)
                batch.cursors = {
                    **batch.cursors,
                    source: {
                        **batch.cursors.get(source, {}),
                        "resumable": True,
                        "discovery_complete": False,
                    },
                }

        async def conditional_lookup(url: str, params: dict[str, Any]) -> Any:
            if force_refresh or source not in {"pncp", "compras_gov"}:
                return None
            key = digest([validator_base, request_key(url, params)])
            async with self.sessions() as session:
                validator = await session.scalar(
                    select(SourceResponseValidator).where(
                        SourceResponseValidator.validator_key == key
                    )
                )
                if validator is None:
                    return None
                stored = (
                    await session.get(SourceRecord, validator.source_record_id)
                    if validator and validator.source_record_id
                    else None
                )
                if (
                    stored is None
                    or stored.raw_payload is None
                    or not 200 <= (stored.http_status or 0) < 300
                ):
                    return None
                headers = (
                    {"If-None-Match": validator.etag}
                    if validator.etag
                    else {"If-Modified-Since": validator.last_modified}
                    if validator.last_modified
                    else {}
                )
                if not headers:
                    return None
                raw = RawSourceRecord(
                    source=stored.source,
                    endpoint=stored.endpoint,
                    request_parameters=stored.request_parameters,
                    raw_payload=stored.raw_payload,
                    response_hash=stored.response_hash,
                    http_status=stored.http_status,
                    collected_at=stored.collected_at,
                )
                return raw, headers, stored.id

        async def on_revalidated(raw: RawSourceRecord, response: Any, source_id: UUID) -> None:
            identity = request_key(raw.endpoint, raw.request_parameters)
            async with write_session(self.sessions) as session:
                await self.fence(session)
                if await session.get(SourceRecord, source_id) is None:
                    raise ValueError("evidência da resposta HTTP 304 foi removida")
                validator = await session.scalar(
                    select(SourceResponseValidator).where(
                        SourceResponseValidator.validator_key == digest([validator_base, identity])
                    )
                )
                if (
                    validator is not None
                    and validator.source_record_id == source_id
                    and response.collected_at >= validator.validated_at
                ):
                    headers = response_validators(
                        {
                            **({"etag": validator.etag} if validator.etag else {}),
                            **(
                                {"last-modified": validator.last_modified}
                                if validator.last_modified
                                else {}
                            ),
                            **dict(response.headers),
                        }
                    )
                    if headers:
                        validator.etag = headers.get("etag")
                        validator.last_modified = headers.get("last-modified")
                        validator.validated_at = response.collected_at
                    else:
                        await session.delete(validator)
                work = await session.scalar(
                    select(CollectionWorkItem).where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                        CollectionWorkItem.kind == "response",
                        CollectionWorkItem.identity == identity,
                    )
                )
                if work is None:
                    work = CollectionWorkItem(
                        batch_id=self.batch_id,
                        source=source,
                        kind="response",
                        identity=identity,
                        payload={},
                    )
                    session.add(work)
                work.payload, work.source_record_id = raw.model_dump(mode="json"), source_id
                session.add(
                    CollectionWorkItem(
                        batch_id=self.batch_id,
                        source=source,
                        kind="validation",
                        status="succeeded",
                        identity=digest([identity, response.collected_at.isoformat()]),
                        source_record_id=source_id,
                        payload={
                            "http_status": 304,
                            "crawl_run_id": str(self.run_id),
                            "validated_at": response.collected_at.isoformat(),
                            "source_record_id": str(source_id),
                            "request_key": identity,
                        },
                    )
                )

        self.replays[source] = ResponseReplay(
            transport,
            records,
            save,
            invalidate,
            source=source,
            invalidate_partition=invalidate_partition,
            conditional_lookup=conditional_lookup,
            on_revalidated=on_revalidated,
        )

    async def pending_result(
        self, source: str, model: Any, *, limit: int = 0, exclude: set[UUID] | None = None
    ) -> ConnectorResult[Any] | None:
        async with self.sessions() as session:
            query = (
                select(CollectionWorkItem)
                .where(
                    CollectionWorkItem.batch_id == self.batch_id,
                    CollectionWorkItem.source == source,
                    CollectionWorkItem.kind == "record",
                    CollectionWorkItem.status.in_(["pending", "details_pending"]),
                )
                .order_by(
                    CollectionWorkItem.status.desc(),
                    CollectionWorkItem.position,
                    CollectionWorkItem.id,
                )
            )
            if limit:
                query = query.limit(limit)
            if exclude:
                query = query.where(CollectionWorkItem.id.not_in(exclude))
            work = list((await session.scalars(query)).all())
            if not work:
                batch = await session.get_one(CollectionBatch, self.batch_id)
                if batch.cursors.get(source, {}).get("discovery_complete"):
                    return ConnectorResult(data=[], availability=DataAvailability.EMPTY)
                return None
            records = []
            data = []
            seen = set()
            for item in work:
                raw = model.model_validate(item.payload)
                data.append(raw)
                self.items[row_identity(raw)] = item.id
                if item.source_record_id and item.source_record_id not in seen:
                    stored = await session.get(SourceRecord, item.source_record_id)
                    if stored is not None:
                        records.append(
                            RawSourceRecord(
                                source=stored.source,
                                endpoint=stored.endpoint,
                                request_parameters=stored.request_parameters,
                                response_hash=stored.response_hash,
                                raw_payload=stored.raw_payload,
                                http_status=stored.http_status or 200,
                                collected_at=stored.collected_at,
                            )
                        )
                        seen.add(item.source_record_id)
            return ConnectorResult(
                data=data, availability=DataAvailability.AVAILABLE, raw_records=records
            )

    async def stage(
        self, source: str, result: ConnectorResult[Any], mapping: dict[str, UUID]
    ) -> None:
        async with write_session(self.sessions) as session:
            await self.fence(session)
            batch = await session.get_one(CollectionBatch, self.batch_id)
            # A pending snapshot replay has no pagination and must not erase a remote cursor.
            if result.pagination is not None or source not in batch.cursors:
                supported = not (
                    result.pagination
                    and result.pagination.continuation
                    and result.pagination.continuation.get("supported") is False
                )
                resumable = bool(result.pagination and result.pagination.truncated and supported)
                batch.cursors = {
                    **batch.cursors,
                    source: {
                        "discovery_complete": result.is_complete,
                        "total_records": result.pagination.total_records
                        if result.pagination
                        else None,
                        "resumable": resumable,
                        "failed": not result.is_complete and not resumable and supported,
                        "continuation": result.pagination.continuation
                        if result.pagination
                        else None,
                    },
                }
            position = int(
                await session.scalar(
                    select(func.max(CollectionWorkItem.position)).where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                    )
                )
                or 0
            )
            for raw in result.data:
                identity = row_identity(raw)
                existing = await session.scalar(
                    select(CollectionWorkItem).where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                        CollectionWorkItem.kind == "record",
                        CollectionWorkItem.identity == identity,
                    )
                )
                if existing is not None:
                    self.items[identity] = existing.id
                    continue
                source_id = getattr(raw, "source_record_id", None) or (
                    mapping.get(raw.source_record_ref) if raw.source_record_ref else None
                )
                if source_id is None and len(mapping) == 1:
                    source_id = next(iter(mapping.values()))
                payload = raw.model_dump(mode="json")
                if not payload.get("source_record_ref") and len(result.raw_records) == 1:
                    payload["source_record_ref"] = result.raw_records[0].record_key
                position += 1
                item = CollectionWorkItem(
                    batch_id=batch.id,
                    source=source,
                    kind="record",
                    identity=identity,
                    payload=payload,
                    source_record_id=source_id,
                    position=position,
                )
                session.add(item)
                await session.flush()
                self.items[identity] = item.id

    async def discovery_complete(self, source: str) -> bool:
        async with self.sessions() as session:
            batch = await session.get_one(CollectionBatch, self.batch_id)
            return bool(batch.cursors.get(source, {}).get("discovery_complete"))

    async def discovery_total(self, source: str) -> int | None:
        async with self.sessions() as session:
            batch = await session.get_one(CollectionBatch, self.batch_id)
            return batch.cursors.get(source, {}).get("total_records")

    async def has_pending(self, source: str, *, exclude: set[UUID]) -> bool:
        async with self.sessions() as session:
            return (
                await session.scalar(
                    select(CollectionWorkItem.id)
                    .where(
                        CollectionWorkItem.batch_id == self.batch_id,
                        CollectionWorkItem.source == source,
                        CollectionWorkItem.kind == "record",
                        CollectionWorkItem.status.in_(["pending", "details_pending"]),
                        CollectionWorkItem.id.not_in(exclude),
                    )
                    .limit(1)
                )
                is not None
            )

    async def select_records(
        self, source: str, model: Any, *, limit: int = 0, exclude: set[UUID] | None = None
    ) -> list[Any]:
        result = await self.pending_result(source, model, limit=limit, exclude=exclude)
        return result.data if result is not None else []

    async def confirm(
        self,
        session: AsyncSession,
        raw: Any,
        *,
        details_pending: bool = False,
        details_failed: bool = False,
        work_id: UUID | None = None,
    ) -> None:
        identifier = work_id or self.current_item_id or self.items.get(row_identity(raw))
        if identifier is None:
            # Detail endpoints can enrich the discovery payload; match its official ID.
            for item_id in self.items.values():
                item = await session.get(CollectionWorkItem, item_id)
                if (
                    item is not None
                    and item.source == raw.source
                    and item.payload.get("external_id") == raw.external_id
                ):
                    identifier = item.id
                    break
        if identifier is not None:
            item = await session.get_one(CollectionWorkItem, identifier)
            item.status = (
                "failed"
                if details_failed
                else "staged"
                if getattr(raw, "collection_stage", False)
                else "details_pending"
                if details_pending
                else "succeeded"
            )
            item.attempts += 1

    async def failed_record(self, work_id: UUID | None = None) -> None:
        identifier = work_id or self.current_item_id
        if identifier is None:
            return
        async with write_session(self.sessions) as session:
            await self.fence(session)
            item = await session.get_one(CollectionWorkItem, identifier)
            item.status = "failed"
            item.attempts += 1

    async def failed_source(self, source: str) -> None:
        async with write_session(self.sessions) as session:
            await self.fence(session)
            batch = await session.get_one(CollectionBatch, self.batch_id)
            entry = {
                **batch.cursors.get(source, {}),
                "failed": True,
                "resumable": False,
                "discovery_complete": False,
            }
            batch.cursors = {**batch.cursors, source: entry}

    async def payment_slice(self, source: str, records: list[Any], limit: int) -> list[Any]:
        """New rows consume the budget; complete a staged commitment atomically."""
        from app.connectors.base import RawPayment

        selected = records[:limit] if limit else list(records)
        remainder = records[len(selected) :]
        cut_groups = {
            row.commitment_external_id
            for row in remainder
            if row.status == "detalhe_empenho" and row.commitment_external_id
        }
        ready_groups = {
            row.commitment_external_id
            for row in selected
            if row.status == "detalhe_empenho"
            and row.commitment_external_id
            and row.commitment_external_id not in cut_groups
        }
        prepared = []
        for raw in selected:
            updates: dict[str, Any] = {"collection_work_id": str(self.items[row_identity(raw)])}
            if raw.status == "detalhe_empenho" and raw.commitment_external_id in cut_groups:
                updates.update(
                    collection_stage=True, commitment_details_complete=False, is_current=False
                )
            prepared.append(raw.model_copy(update=updates))
        if ready_groups:
            async with self.sessions() as session:
                rows = (
                    await session.scalars(
                        select(CollectionWorkItem).where(
                            CollectionWorkItem.batch_id == self.batch_id,
                            CollectionWorkItem.source == source,
                            CollectionWorkItem.kind == "record",
                            CollectionWorkItem.status == "staged",
                        )
                    )
                ).all()
                for item in rows:
                    if item.payload.get("commitment_external_id") in ready_groups:
                        raw = RawPayment.model_validate(item.payload)
                        prepared.append(raw.model_copy(update={"collection_work_id": str(item.id)}))
        return prepared

    async def finish(self, status: CrawlRunStatus) -> None:
        async with write_session(self.sessions) as session:
            await self.fence(session)
            await self.finish_in_session(session, status)

    async def finish_in_session(self, session: AsyncSession, status: CrawlRunStatus) -> None:
        batch = await session.get_one(CollectionBatch, self.batch_id)
        remaining = await session.scalar(
            select(CollectionWorkItem.id)
            .where(
                CollectionWorkItem.batch_id == batch.id,
                CollectionWorkItem.kind == "record",
                CollectionWorkItem.status.in_(["pending", "details_pending"]),
            )
            .limit(1)
        )
        cursors = dict(batch.cursors)
        for source, replay in self.replays.items():
            entry = dict(cursors.get(source, {}))
            entry["resumable"] = bool(entry.get("resumable") or replay.invalidated)
            if replay.invalidated:
                entry["discovery_complete"] = False
            cursors[source] = entry
        batch.cursors = cursors
        limited = bool(remaining or any(item.get("resumable") for item in cursors.values()))
        failed = await session.scalar(
            select(CollectionWorkItem.id)
            .where(
                CollectionWorkItem.batch_id == batch.id,
                CollectionWorkItem.kind == "record",
                CollectionWorkItem.status.in_(["failed"] if limited else ["failed", "staged"]),
            )
            .limit(1)
        )
        batch.status = (
            "suspended"
            if status is CrawlRunStatus.CANCELLED
            else "failed"
            if status is CrawlRunStatus.FAILED
            else "failed"
            if failed
            or (
                batch.filters.get("mode") != "payment_import"
                and any(item.get("failed") for item in cursors.values())
            )
            else "limited"
            if limited
            else "complete"
        )


async def latest_limited_batch(
    sessions: async_sessionmaker[AsyncSession], filters: dict[str, Any]
) -> UUID | None:
    async with sessions() as session:
        return await session.scalar(
            select(CollectionBatch.id)
            .where(
                CollectionBatch.stream_key == stream_key(filters),
                CollectionBatch.status == "limited",
            )
            .order_by(CollectionBatch.created_at)
            .limit(1)
        )
