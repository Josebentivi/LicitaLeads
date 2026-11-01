"""Source-to-database ingestion with conservative canonical matching."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, cast
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import (
    ConnectorResult,
    ContractConnector,
    PaginationInfo,
    PriceRegistryConnector,
    ProcurementFilters,
    ProcurementSourceConnector,
    RawContract,
    RawContractItem,
    RawContractTerm,
    RawDocument,
    RawPriceRegistry,
    RawPriceRegistryAdhesion,
    RawPriceRegistryCommitment,
    RawPriceRegistryItem,
    RawPriceRegistryUnit,
    RawProcurement,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.connectors.base import (
    DataAvailability as ConnectorAvailability,
)
from app.connectors.discovery_budget import DiscoveryBudget
from app.connectors.http import AsyncHTTPClient
from app.connectors.progress import ProgressEvent
from app.database import async_session_factory, write_session
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    Company,
    Contract,
    ContractItem,
    ContractTerm,
    CrawlRun,
    CrawlRunStatus,
    DataAvailability,
    Document,
    ExtractionStatus,
    FieldObservation,
    FieldValueStatus,
    JobLease,
    Participant,
    ParticipantRole,
    ParticipantStatus,
    PriceRegistry,
    PriceRegistryAdhesion,
    PriceRegistryCommitment,
    PriceRegistryItem,
    PriceRegistryUnit,
    Procurement,
    ProcurementItem,
    ProcurementSource,
    SourceRecord,
)
from app.repositories.crawls import JobLeaseRepository
from app.repositories.procurements import ProcurementRepository
from app.services.crawl_progress import STAGE_LABELS, CrawlProgressRecorder
from app.services.identifiers import (
    canonical_modality,
    is_valid_cnpj,
    modality_category,
    normalize_cnpj,
    normalize_company_name,
    participant_status_code,
)
from app.services.ingestion.collection_batches import (
    CollectionCoordinator,
    latest_limited_batch,
    row_identity,
)
from app.services.ingestion.document_retry import DOCUMENT_LEASE
from app.services.ingestion.field_updates import RecordOutcome, install_audit
from app.services.ingestion.verified_versions import (
    find_reusable,
    official_global_time,
    record_verified,
)
from app.services.parallel import SerialWriter, bounded_map
from app.services.price_registry_scopes import PriceRegistryScope
from app.services.procurement_phase import refresh_procurement_phase


def stable_fingerprint(*parts: object) -> str:
    """Hash a typed identity deterministically across Python processes."""

    encoded = json.dumps(
        parts,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class PipelineCancelled(RuntimeError):
    """Raised cooperatively when an operator cancels a crawl run."""


class PipelineBusy(RuntimeError):
    """A scheduled source was not admitted; no crawl or attempt was started."""


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _detail_pending(result: ConnectorResult[Any]) -> bool:
    return bool(
        (result.pagination and result.pagination.truncated)
        or "pagination totals changed" in (result.diagnostic or "")
    )


def _detail_failed(result: ConnectorResult[Any]) -> bool:
    return not _detail_pending(result) and (
        result.availability
        in {ConnectorAvailability.TEMPORARY_ERROR, ConnectorAvailability.ACCESS_RESTRICTED}
        or result.partial
    )


@dataclass(frozen=True, slots=True)
class PipelineRequest:
    """Source-independent crawl input shared by REST, CLI, and scheduler."""

    connector: str = "all"
    uf: str = "MA"
    days: int | None = 7
    start_date: date | None = None
    end_date: date | None = None
    modalities: tuple[str, ...] = ("pregao_eletronico",)
    municipality: str | None = None
    agency: str | None = None
    agency_cnpj: str | None = None
    unit_code: str | None = None
    keyword: str | None = None
    max_pages: int | None = None
    process_documents: bool = True
    document_batch_size: int | None = None
    contract_details: bool = True
    mode: str = "procurements"
    discovery_kind: str = "publication"
    external_ids: tuple[str, ...] = ()
    document_ids: tuple[str, ...] = ()
    retry_of_run_id: str | None = None
    continuation_batch_id: UUID | None = None
    resume_automatically: bool = False
    force_refresh: bool = False
    sync_context: dict[str, Any] | None = None
    recheck_key: str | None = None
    price_registry_scopes: tuple[PriceRegistryScope, ...] = ()

    def filters(self) -> ProcurementFilters:
        return ProcurementFilters(
            start_date=self.start_date,
            end_date=self.end_date,
            days=self.days,
            uf=self.uf.upper(),
            municipality=self.municipality,
            agency=self.agency,
            agency_cnpj=self.agency_cnpj,
            unit_code=self.unit_code,
            keyword=self.keyword,
            modalities=list(self.modalities),
            max_pages=self.max_pages,
            discovery_kind=self.discovery_kind,
        )

    def as_json(self) -> dict[str, Any]:
        return _json_value(
            {
                "connector": self.connector,
                "mode": self.mode,
                "uf": self.uf.upper(),
                "days": self.days,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "modalities": list(self.modalities),
                "municipality": self.municipality,
                "agency": self.agency,
                "agency_cnpj": self.agency_cnpj,
                **({"unit_code": self.unit_code} if self.unit_code is not None else {}),
                "keyword": self.keyword,
                "max_pages": self.max_pages,
                "process_documents": self.process_documents,
                "document_batch_size": self.document_batch_size,
                "contract_details": self.contract_details,
                "discovery_kind": self.discovery_kind,
                "external_ids": list(self.external_ids),
                "document_ids": list(self.document_ids),
                "retry_of_run_id": self.retry_of_run_id,
                "continuation_batch_id": self.continuation_batch_id,
                "resume_automatically": self.resume_automatically,
                "force_refresh": self.force_refresh,
                **({"sync_context": self.sync_context} if self.sync_context else {}),
                **({"recheck_key": self.recheck_key} if self.recheck_key else {}),
                **(
                    {"price_registry_scopes": [asdict(item) for item in self.price_registry_scopes]}
                    if self.price_registry_scopes
                    else {}
                ),
            }
        )


@dataclass(slots=True)
class PipelineSummary:
    """Terminal counters and diagnostics for one crawl run."""

    run_id: UUID
    status: CrawlRunStatus = CrawlRunStatus.COMPLETED
    records_found: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_failed: int = 0
    records_changed: int = 0
    records_unchanged: int = 0
    records_stale_ignored: int = 0
    records_conflicted: int = 0
    diagnostics: list[str] = field(default_factory=list)
    source_results: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(slots=True)
class _PipelineExecution:
    run_id: UUID | None = None
    generations: dict[str, int] = field(default_factory=dict)
    worker: asyncio.Task[Any] | None = None
    lease_lost: bool = False
    progress: CrawlProgressRecorder | None = None
    writer: SerialWriter = field(default_factory=SerialWriter)
    summaries: dict[str, PipelineSummary] = field(default_factory=dict)
    root_summary: PipelineSummary | None = None
    coordinator: CollectionCoordinator | None = None


@dataclass(slots=True)
class _RecordWork:
    work_id: UUID | None = None
    details_pending: bool = False
    details_failed: bool = False
    outcome: RecordOutcome = field(default_factory=RecordOutcome)
    verification: dict[str, Any] | None = None


class IngestionPipeline:
    """Collect official records while retaining every normalization input."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        connectors: dict[str, ProcurementSourceConnector] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.session_factory = session_factory or async_session_factory
        self._connectors = connectors
        self._execution_context: ContextVar[_PipelineExecution | None] = ContextVar(
            "pipeline_execution", default=None
        )

        self._record_context: ContextVar[_RecordWork | None] = ContextVar(
            "pipeline_record", default=None
        )

    def _record(self) -> _RecordWork:
        return self._record_context.get() or _RecordWork()

    def _execution(self) -> _PipelineExecution:
        return self._execution_context.get() or _PipelineExecution()

    async def create_run(self, request: PipelineRequest) -> CrawlRun:
        """Persist a pending run before background execution starts."""

        created: list[UUID] = []
        try:
            return await self._create_run_record(request, created)
        except asyncio.CancelledError:
            if created:
                cleanup = asyncio.create_task(
                    self._abort_admission(created[0], [], f"pipeline:{created[0]}", cancelled=True)
                )
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await cleanup
            raise

    async def _create_run_record(
        self, request: PipelineRequest, created: list[UUID], *, identifier: UUID | None = None
    ) -> CrawlRun:

        connector = request.connector.lower()
        if connector not in {"pncp", "compras_gov", "all"}:
            raise ValueError("connector must be pncp, compras_gov, or all")
        if request.mode not in {"procurements", "price_registries", "contracts", "documents"}:
            raise ValueError("unsupported collection mode")
        if request.mode == "documents" and not request.document_ids:
            raise ValueError("document retry requires explicit document IDs")
        async with write_session(self.session_factory) as session:
            await JobLeaseRepository(session).assert_writes_allowed(now=datetime.now(UTC))
            run = CrawlRun(
                id=identifier or uuid4(),
                connector=connector,
                status=CrawlRunStatus.PENDING,
                filters=request.as_json(),
                batch_id=request.continuation_batch_id,
                counting_version=2,
                records_changed=0,
                records_unchanged=0,
                records_stale_ignored=0,
                records_conflicted=0,
                cursor={
                    "progress": {"stage": "queued", "updated_at": datetime.now(UTC).isoformat()}
                },
            )
            session.add(run)
            await session.flush()
            run_id = run.id
            created.append(run_id)
        async with self.session_factory() as session:
            return await session.get_one(CrawlRun, run_id)

    async def run(self, request: PipelineRequest, *, run_id: UUID | None = None) -> PipelineSummary:
        token = self._execution_context.set(_PipelineExecution())
        try:
            return await self._run(request, run_id=run_id)
        finally:
            self._execution_context.reset(token)

    async def _run(
        self, request: PipelineRequest, *, run_id: UUID | None = None
    ) -> PipelineSummary:
        """Execute a crawl under a cooperative database lease."""

        scheduled_admission = run_id is None and request.resume_automatically
        if run_id is None:
            if request.resume_automatically and request.continuation_batch_id is None:
                batch_id = await latest_limited_batch(self.session_factory, request.as_json())
                if batch_id is not None:
                    request = replace(request, continuation_batch_id=batch_id)
            run_id = uuid4() if scheduled_admission else (await self.create_run(request)).id
        self._execution().run_id = run_id
        self._execution().generations = {}
        self._execution().worker = asyncio.current_task()
        owner = f"pipeline:{run_id}"
        lease_names = self._lease_names(request)
        self._execution().lease_lost = False
        acquired_names: list[str] = []
        try:
            for lease_name in lease_names:
                if not await self._acquire_lease(lease_name, owner):
                    for held_name in acquired_names:
                        await self._release_lease(held_name, owner)
                    if scheduled_admission:
                        raise PipelineBusy("fonte ocupada; coleta automática adiada")
                    await self._fail_run(
                        run_id, "another equivalent crawl currently holds the lease"
                    )
                    return PipelineSummary(
                        run_id=run_id,
                        status=CrawlRunStatus.FAILED,
                        diagnostics=["another equivalent crawl currently holds the lease"],
                    )
                acquired_names.append(lease_name)
            if scheduled_admission:
                await self._create_run_record(request, [], identifier=run_id)
        except (asyncio.CancelledError, Exception) as exc:
            cleanup = asyncio.create_task(
                self._abort_admission(
                    run_id, lease_names, owner, cancelled=isinstance(exc, asyncio.CancelledError)
                )
            )
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
            raise

        coordinator: CollectionCoordinator | None = None
        summary = PipelineSummary(run_id=run_id)
        self._execution().progress = CrawlProgressRecorder()
        connectors: list[ProcurementSourceConnector] = []
        heartbeat = asyncio.create_task(self._heartbeat_leases(lease_names, owner))
        try:
            await self._mark_running(run_id)
            if request.mode != "documents":
                coordinator = CollectionCoordinator(
                    self.session_factory, run_id, self._assert_fence
                )
                end = request.end_date or datetime.now(ZoneInfo(self.settings.timezone)).date()
                start = request.start_date or end - timedelta(
                    days=(request.days or self.settings.default_lookback_days) - 1
                )
                fixed = {
                    **request.as_json(),
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                }
                filters = await coordinator.start(
                    request.as_json(), fixed, request.continuation_batch_id
                )
                request = replace(
                    request,
                    start_date=date.fromisoformat(filters["start_date"]),
                    end_date=date.fromisoformat(filters["end_date"]),
                )
                self._execution().coordinator = coordinator
            if await self._cancellation_requested(run_id):
                raise PipelineCancelled
            connectors = (
                [] if request.mode == "documents" else self._selected_connectors(request.connector)
            )
            for connector in connectors:
                if coordinator := self._execution().coordinator:
                    await coordinator.attach(connector, force_refresh=request.force_refresh)
                transport = getattr(connector, "_http", None)
                if transport is not None:
                    transport.checkpoint = self._checkpoint

                    async def source_activity(
                        event: ProgressEvent, source: str = connector.name
                    ) -> None:
                        await self._report_activity(replace(event, source=source))

                    transport.on_activity = source_activity
            runner = (
                self._run_price_registry_connector
                if request.mode == "price_registries"
                else (
                    self._run_contract_connector
                    if request.mode == "contracts"
                    else self._run_connector
                )
            )
            execution = self._execution()
            execution.root_summary = summary

            async def collect_source(connector: ProcurementSourceConnector) -> None:
                local = PipelineSummary(run_id=run_id)
                execution.summaries[connector.name] = local
                local.source_results[connector.name] = {"status": "running"}
                started = time.perf_counter()
                status = "completed"
                try:
                    await self._report_activity(
                        ProgressEvent(
                            stage="discovery",
                            source=connector.name,
                            message="Buscando registros na fonte oficial",
                        )
                    )
                    await runner(connector, request, local)
                except (asyncio.CancelledError, PipelineCancelled):
                    raise
                except Exception as exc:
                    status = "failed"
                    if coordinator := execution.coordinator:
                        await coordinator.failed_source(connector.name)
                    local.diagnostics.append(f"{connector.name}: {type(exc).__name__}: {exc}")
                if status != "failed" and local.diagnostics:
                    status = (
                        "temporary_error"
                        if any("temporary" in item.casefold() for item in local.diagnostics)
                        else "partial"
                    )
                local.source_results[connector.name].update(
                    status=status,
                    records_found=local.records_found,
                    records_created=local.records_created,
                    records_updated=local.records_updated,
                    records_failed=local.records_failed,
                    duration_seconds=round(time.perf_counter() - started, 3),
                    max_attempts_per_request=self.settings.http_max_retries + 1,
                    diagnostics=list(local.diagnostics),
                )
                await self._update_progress(local)

            async with execution.writer.running():
                tasks = [asyncio.create_task(collect_source(connector)) for connector in connectors]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        if not task.cancelling():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self._aggregate_sources()
                    execution.summaries.clear()
            if request.process_documents and request.mode in {"procurements", "documents"}:
                from app.services.ingestion.processor import DocumentProcessingService

                batch_size = request.document_batch_size or self.settings.document_batch_size
                documents_entry: dict[str, Any] = {
                    "status": "running",
                    "documents_processed": 0,
                    "documents_total": None,
                    "events_created": 0,
                    "participants_created": 0,
                    "leads_created": 0,
                    "batch_size": batch_size,
                }
                summary.source_results["documents"] = documents_entry
                await self._report_activity(
                    ProgressEvent(
                        stage="documents",
                        source="documents",
                        operation="documents",
                        message="Preparando documentos pendentes do lote",
                    )
                )
                await self._update_progress(summary)

                async def selected_documents(identifiers: list[UUID]) -> None:
                    documents_entry["document_ids"] = [
                        str(identifier) for identifier in identifiers
                    ]
                    documents_entry["documents_total"] = len(identifiers)
                    if request.mode == "documents":
                        summary.records_found = len(identifiers)
                    await self._update_progress(summary)

                async def report_documents(processed_count: int, total: int) -> None:
                    if await self._cancellation_requested(summary.run_id):
                        raise PipelineCancelled
                    documents_entry["documents_processed"] = processed_count
                    documents_entry["documents_total"] = total
                    recorder = self._execution().progress
                    if recorder is not None:
                        recorder.record(
                            ProgressEvent(
                                stage="documents",
                                processed=processed_count,
                                total=total,
                                message="Documento do lote examinado",
                            )
                        )
                    await self._update_progress(summary)

                processed = await DocumentProcessingService(
                    self.settings,
                    session_factory=self.session_factory,
                    crawl_run_id=self._execution().run_id,
                    http_budgets={
                        connector.name: transport
                        for connector in connectors
                        if isinstance(
                            transport := getattr(connector, "_http", None), AsyncHTTPClient
                        )
                    },
                ).process_pending(
                    limit=batch_size,
                    on_progress=report_documents,
                    checkpoint=self._assert_fence,
                    on_activity=self._report_activity,
                    document_ids=tuple(UUID(value) for value in request.document_ids)
                    if request.mode == "documents"
                    else None,
                    failed_only=request.mode == "documents",
                    on_selected=selected_documents,
                )
                if processed.documents_failed:
                    summary.diagnostics.append(
                        f"documentos: {processed.documents_failed} falha(s) de processamento"
                    )
                documents_entry["status"] = (
                    "completed" if not processed.documents_failed else "partial"
                )
                documents_entry["documents_processed"] = processed.documents_processed
                documents_entry["events_created"] = processed.events_created
                documents_entry["participants_created"] = processed.participants_created
                documents_entry["leads_created"] = processed.leads_created
                documents_entry["documents_failed"] = processed.documents_failed
                documents_entry["documents_skipped"] = processed.documents_skipped
                if processed.documents_skipped:
                    summary.diagnostics.append(
                        f"documentos: {processed.documents_skipped} ignorado(s) por mudança de situação; verifique os arquivos"
                    )
                if request.mode == "documents":
                    summary.records_updated = processed.documents_processed
                    summary.records_failed = processed.documents_failed
                await self._update_progress(summary)
            if summary.diagnostics:
                summary.status = CrawlRunStatus.PARTIAL
            await self._report_activity(
                ProgressEvent(stage="finalizing", message="Finalizando coleta")
            )
        except (asyncio.CancelledError, PipelineCancelled) as exc:
            summary.status = CrawlRunStatus.CANCELLED
            summary.diagnostics.append("coleta encerrada pelo usuário")
            if isinstance(exc, asyncio.CancelledError):
                raise
        except Exception as exc:
            summary.status = CrawlRunStatus.FAILED
            summary.diagnostics.append(f"{type(exc).__name__}: {exc}")
        finally:
            recorder = self._execution().progress
            if recorder is not None:
                await recorder.aclose()
            heartbeat.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await heartbeat
            for connector in connectors:
                with suppress(Exception):
                    await connector.aclose()
            try:
                await self._finish_run(summary)
            finally:
                for lease_name in reversed(lease_names):
                    with suppress(Exception):
                        await self._release_lease(lease_name, owner)
        return summary

    def _selected_connectors(self, requested: str) -> list[ProcurementSourceConnector]:
        names = ["pncp", "compras_gov"] if requested.lower() == "all" else [requested.lower()]
        if self._connectors is not None:
            missing = [name for name in names if name not in self._connectors]
            if missing:
                raise ValueError(f"connector implementations not supplied: {', '.join(missing)}")
            return [self._connectors[name] for name in names]

        from app.connectors.compras_gov import ComprasGovConnector
        from app.connectors.pncp import PNCPConnector

        connectors: list[ProcurementSourceConnector] = []
        for name in names:
            if name == "pncp":
                connectors.append(PNCPConnector(self.settings))
            elif name == "compras_gov":
                connectors.append(ComprasGovConnector(self.settings))
            else:
                raise ValueError(f"unsupported connector: {name}")
        return connectors

    async def _discover_by_ids(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
    ) -> tuple[ConnectorResult[Any], dict[str, ConnectorResult[Any]]]:
        """Refresh known official identities independently of publication windows."""
        rows: list[Any] = []
        raw_records: list[RawSourceRecord] = []
        cached: dict[str, ConnectorResult[Any]] = {}
        failures: list[str] = []

        async def fetch(external_id: str) -> ConnectorResult[Any] | Exception:
            await self._checkpoint()
            try:
                method = (
                    connector.fetch_procurement
                    if request.mode == "procurements"
                    else getattr(
                        connector,
                        "fetch_price_registry"
                        if request.mode == "price_registries"
                        else "fetch_contract",
                        None,
                    )
                )
                if method is None:
                    return ConnectorResult(
                        data=None, availability=ConnectorAvailability.NOT_SUPPORTED
                    )
                return await method(external_id)
            except (asyncio.CancelledError, PipelineCancelled):
                raise
            except Exception as exc:
                return exc

        stream = bounded_map(
            dict.fromkeys(request.external_ids),
            fetch,
            capacity=2 * self.settings.http_max_concurrency,
        )
        try:
            async for external_id, detail in stream:
                if isinstance(detail, Exception):
                    summary.records_failed += 1
                    failures.append(f"{external_id}: {type(detail).__name__}: {detail}")
                    continue
                raw_records.extend(detail.raw_records)
                if not detail.is_available or detail.data is None:
                    summary.records_failed += 1
                    failures.append(
                        f"{external_id}: {detail.diagnostic or detail.availability.value}"
                    )
                    continue
                raw = detail.data
                if not raw.source_record_ref and len(detail.raw_records) == 1:
                    raw = raw.model_copy(
                        update={"source_record_ref": detail.raw_records[0].record_key}
                    )
                    detail = detail.model_copy(update={"data": raw})
                rows.append(raw)
                cached[raw.external_id] = detail
        finally:
            await stream.aclose()
        return ConnectorResult(
            data=rows,
            raw_records=raw_records,
            source_urls=list(dict.fromkeys(record.endpoint for record in raw_records)),
            availability=(
                ConnectorAvailability.AVAILABLE if rows else ConnectorAvailability.TEMPORARY_ERROR
            ),
            partial=bool(failures),
            diagnostic="atualização por identidade: " + "; ".join(failures) if failures else None,
        ), cached

    async def _run_connector(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
    ) -> None:
        filters = request.filters()
        if filters.max_pages is None and self.settings.crawl_max_pages:
            filters.max_pages = self.settings.crawl_max_pages
        details_by_id: dict[str, ConnectorResult[Any]] = {}
        if request.external_ids:
            discovery, details_by_id = await self._discover_by_ids(connector, request, summary)
        else:
            coordinator = self._execution().coordinator
            pending = (
                await coordinator.pending_result(connector.name, RawProcurement)
                if coordinator
                else None
            )
            discovery = (
                pending if pending is not None else await connector.discover_procurements(filters)
            )
        discovery_record_ids = await self._persist_source_records(summary.run_id, discovery)
        self._note_partial_discovery(summary, connector.name, discovery)
        if discovery.availability not in {
            ConnectorAvailability.AVAILABLE,
            ConnectorAvailability.EMPTY,
        } and (coordinator := self._execution().coordinator):
            await coordinator.failed_source(connector.name)
        if discovery.availability is ConnectorAvailability.TEMPORARY_ERROR:
            summary.diagnostics.append(
                f"{connector.name}: {discovery.diagnostic or 'temporary source error'}"
            )
            return
        if discovery.availability not in {
            ConnectorAvailability.AVAILABLE,
            ConnectorAvailability.EMPTY,
        }:
            summary.diagnostics.append(f"{connector.name}: {discovery.availability.value}")
            return

        records = discovery.data
        if coordinator := self._execution().coordinator:
            await coordinator.stage(
                connector.name,
                discovery,
                self._source_id_map(discovery.raw_records, discovery_record_ids),
            )
            records = await coordinator.select_records(connector.name, RawProcurement)
        max_records = self.settings.crawl_max_records_per_source
        if max_records and len(records) > max_records:
            summary.diagnostics.append(
                f"{connector.name}: limitado a {max_records} registro(s) por fonte "
                f"({len(records)} encontrados)"
            )
            records = records[:max_records]
        await self._collect_records(
            connector,
            request,
            summary,
            records,
            discovery,
            discovery_record_ids,
            details_by_id,
        )

    async def _discover_price_registry_scopes(
        self,
        connector: PriceRegistryConnector,
        request: PipelineRequest,
        filters: ProcurementFilters,
    ) -> ConnectorResult[list[RawPriceRegistry]]:
        """Query frozen, exact units under one crawl and its existing replay/fence."""
        if not request.price_registry_scopes:
            return await connector.discover_price_registries(filters)
        results: list[ConnectorResult[list[RawPriceRegistry]]] = []
        failures: list[str] = []
        failed = False
        scopes = [
            scope for scope in request.price_registry_scopes if connector.name in scope.sources
        ]

        async def query(
            scope: PriceRegistryScope,
        ) -> ConnectorResult[list[RawPriceRegistry]] | Exception:
            await self._checkpoint()
            await self._report_activity(
                ProgressEvent(
                    stage="discovery",
                    source=connector.name,
                    subject=scope.label,
                    message=f"Consultando {scope.agency_name} · unidade {scope.unit_code}",
                )
            )
            scoped_filters = filters.model_copy(
                update={
                    "agency": scope.agency_name,
                    "agency_cnpj": scope.agency_cnpj,
                    "unit_code": scope.unit_code,
                }
            )
            try:
                return await connector.discover_price_registries(scoped_filters)
            except (asyncio.CancelledError, PipelineCancelled):
                raise
            except Exception as exc:
                return exc

        stream = bounded_map(scopes, query, capacity=min(4, self.settings.http_max_concurrency))
        try:
            async for scope, result in stream:
                if isinstance(result, Exception):
                    failed = True
                    failures.append(f"{scope.key}: {type(result).__name__}: {result}")
                    continue
                results.append(result)
                if not result.is_complete:
                    failures.append(
                        f"{scope.key}: {result.diagnostic or result.availability.value}"
                    )
                    failed = failed or not (result.pagination and result.pagination.truncated)
        finally:
            await stream.aclose()
        records = {row_identity(row): row for result in results for row in result.data}
        pages = [result.pagination for result in results if result.pagination is not None]
        # Network/access failures require explicit continuation, even if another unit was limited.
        limited = any(page.truncated for page in pages) and not failed
        if not results and not failures:
            return ConnectorResult(data=[], availability=ConnectorAvailability.NOT_SUPPORTED)
        return ConnectorResult(
            data=list(records.values()),
            availability=(
                ConnectorAvailability.AVAILABLE
                if records
                else ConnectorAvailability.TEMPORARY_ERROR
                if failed
                else ConnectorAvailability.EMPTY
            ),
            raw_records=[raw for result in results for raw in result.raw_records],
            source_urls=list(
                dict.fromkeys(url for result in results for url in result.source_urls)
            ),
            pagination=PaginationInfo(
                total_records=sum(page.total_records or 0 for page in pages),
                total_pages=sum(page.total_pages or 0 for page in pages),
                fetched_pages=sum(page.fetched_pages for page in pages),
                pages_remaining=sum(page.pages_remaining or 0 for page in pages),
                truncated=limited,
            ),
            partial=bool(failures),
            diagnostic="; ".join(failures) or None,
        )

    async def _run_price_registry_connector(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
    ) -> None:
        """Drain saved rows first, then reserve remaining capacity before discovery."""
        price_connector = cast(PriceRegistryConnector, connector)
        filters = request.filters()
        if filters.max_pages is None and self.settings.crawl_max_pages:
            filters.max_pages = self.settings.crawl_max_pages
        coordinator = self._execution().coordinator
        limit = self.settings.crawl_max_records_per_source
        consumed = 0
        attempted: set[UUID] = set()
        pending = (
            await coordinator.pending_result(connector.name, RawPriceRegistry, limit=limit)
            if coordinator and not request.external_ids
            else None
        )
        for phase in range(2 if pending is not None and pending.data else 1):
            discovery_started = datetime.now(UTC)
            from_saved = phase == 0 and pending is not None
            if from_saved:
                discovery = pending
                assert discovery is not None
            elif request.external_ids:
                discovery, _ = await self._discover_by_ids(connector, request, summary)
            else:
                transport = getattr(connector, "_http", None)
                if transport is not None:
                    transport.arp_discovery_budget = (
                        DiscoveryBudget(limit - consumed) if limit else None
                    )
                discovery = await self._discover_price_registry_scopes(
                    price_connector, request, filters
                )
            discovery_ids = await self._persist_source_records(summary.run_id, discovery)
            self._note_partial_discovery(summary, connector.name, discovery)
            if discovery.availability not in {
                ConnectorAvailability.AVAILABLE,
                ConnectorAvailability.EMPTY,
            }:
                if coordinator:
                    await coordinator.failed_source(connector.name)
                summary.diagnostics.append(
                    f"{connector.name}: {discovery.diagnostic or discovery.availability.value}"
                )
                return
            remaining = limit - consumed if limit else 0
            if coordinator:
                await coordinator.stage(
                    connector.name,
                    discovery,
                    self._source_id_map(discovery.raw_records, discovery_ids),
                )
                records = await coordinator.select_records(
                    connector.name,
                    RawPriceRegistry,
                    limit=remaining,
                    exclude=attempted,
                )
                attempted.update(coordinator.items[row_identity(raw)] for raw in records)
            else:
                records = discovery.data[:remaining] if remaining else discovery.data
            entry = summary.source_results[connector.name]
            if not from_saved:
                entry["discovery_total"] = (
                    discovery.pagination.total_records
                    if discovery.pagination
                    else len(discovery.data)
                )
                fresh_refs = {
                    raw.record_key
                    for raw in discovery.raw_records
                    if raw.collected_at >= discovery_started
                }
                entry["headers_received"] = entry.get("headers_received", 0) + sum(
                    raw.source_record_ref in fresh_refs for raw in discovery.data
                )
            else:
                entry["discovery_total"] = (
                    await coordinator.discovery_total(connector.name) if coordinator else None
                )
                entry.setdefault("headers_received", 0)
            await self._collect_records(
                connector,
                request,
                summary,
                records,
                discovery,
                discovery_ids,
                {},
            )
            consumed += len(records)
            entry = summary.source_results[connector.name]
            if discovery.pagination and discovery.pagination.truncated:
                entry["stop_reason"] = "Limite de descoberta/páginas; há trabalho para continuar"
            if limit and consumed >= limit:
                incomplete = coordinator and not await coordinator.discovery_complete(
                    connector.name
                )
                queued = coordinator and await coordinator.has_pending(
                    connector.name, exclude=attempted
                )
                if (
                    incomplete
                    or queued
                    or (discovery.pagination and discovery.pagination.truncated)
                ):
                    entry["stop_reason"] = f"Limite de {limit} atas por fonte nesta execução"
                    summary.diagnostics.append(
                        f"{connector.name}: limitado a {limit} ata(s) por fonte"
                    )
                return
            if (
                not from_saved
                or not coordinator
                or await coordinator.discovery_complete(connector.name)
                or summary.records_failed
            ):
                return

    async def _run_contract_connector(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
    ) -> None:
        """Collect contract headers and link them to known processes/companies."""

        contract_connector = cast(ContractConnector, connector)
        filters = request.filters()
        if filters.max_pages is None and self.settings.crawl_max_pages:
            filters.max_pages = self.settings.crawl_max_pages
        coordinator = self._execution().coordinator
        pending = (
            await coordinator.pending_result(connector.name, RawContract) if coordinator else None
        )
        if request.external_ids:
            discovery, _ = await self._discover_by_ids(connector, request, summary)
        else:
            discovery = (
                pending
                if pending is not None
                else await contract_connector.discover_contracts(filters)
            )
        discovery_record_ids = await self._persist_source_records(summary.run_id, discovery)
        self._note_partial_discovery(summary, connector.name, discovery)
        if discovery.availability not in {
            ConnectorAvailability.AVAILABLE,
            ConnectorAvailability.EMPTY,
        } and (coordinator := self._execution().coordinator):
            await coordinator.failed_source(connector.name)
        if discovery.availability is ConnectorAvailability.TEMPORARY_ERROR:
            summary.diagnostics.append(
                f"{connector.name}: {discovery.diagnostic or 'temporary source error'}"
            )
            return
        if discovery.availability not in {
            ConnectorAvailability.AVAILABLE,
            ConnectorAvailability.EMPTY,
        }:
            summary.diagnostics.append(f"{connector.name}: {discovery.availability.value}")
            return
        records = discovery.data
        if coordinator := self._execution().coordinator:
            await coordinator.stage(
                connector.name,
                discovery,
                self._source_id_map(discovery.raw_records, discovery_record_ids),
            )
            records = await coordinator.select_records(connector.name, RawContract)
        max_records = self.settings.crawl_max_records_per_source
        if max_records and len(records) > max_records:
            summary.diagnostics.append(
                f"{connector.name}: limitado a {max_records} contrato(s) por fonte "
                f"({len(records)} encontrados)"
            )
            records = records[:max_records]
        await self._collect_records(
            connector,
            request,
            summary,
            records,
            discovery,
            discovery_record_ids,
            {},
        )

    async def _collect_records(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
        records: list[Any],
        discovery: ConnectorResult[Any],
        discovery_record_ids: list[UUID],
        details_by_id: dict[str, ConnectorResult[Any]],
    ) -> None:
        summary.records_found += len(records)
        previous = summary.source_results.get(connector.name, {})
        previous_progress = previous.get("progress", {})
        progress: dict[str, Any] = {
            "source": connector.name,
            "processed": previous_progress.get("processed", 0),
            "total": previous_progress.get("total", 0) + len(records),
            "ready": 0,
        }
        summary.source_results[connector.name] = {
            **previous,
            "status": "running",
            "mode": request.mode,
            "records_found": previous.get("records_found", 0) + len(records),
            "records_failed": previous.get("records_failed", 0),
            "progress": progress,
            "records_reused": previous.get("records_reused", 0),
            "operations_reused": previous.get("operations_reused", 0),
        }
        await self._update_progress(summary)

        async def prepare(
            raw: Any,
        ) -> tuple[dict[str, ConnectorResult[Any]], Exception | None, UUID | None]:
            if await self._cancellation_requested(summary.run_id):
                raise PipelineCancelled
            await self._checkpoint()
            try:
                if not request.force_refresh:
                    async with self.session_factory() as session:
                        proof = await find_reusable(
                            session, connector, request.mode, raw, timezone=self.settings.timezone
                        )
                    if proof is not None:
                        progress["ready"] += 1
                        return {}, None, proof.id
                await self._report_activity(
                    ProgressEvent(
                        stage="details",
                        source=connector.name,
                        subject=raw.external_id,
                        message="Consultando detalhes na fonte oficial",
                    )
                )
                values = await self._fetch_bundle(connector, request, raw, details_by_id)
                progress["ready"] += 1
                return values, None, None
            except (asyncio.CancelledError, PipelineCancelled):
                raise
            except Exception as exc:
                return {}, exc, None

        stream = bounded_map(records, prepare, capacity=2 * self.settings.http_max_concurrency)
        try:
            async for raw, (values, error, reused_id) in stream:
                if await self._cancellation_requested(summary.run_id):
                    raise PipelineCancelled
                await self._checkpoint()
                coordinator = self._execution().coordinator
                state = _RecordWork(
                    work_id=coordinator.items.get(row_identity(raw)) if coordinator else None,
                    details_pending=any(_detail_pending(value) for value in values.values()),
                    details_failed=any(_detail_failed(value) for value in values.values()),
                )
                try:
                    if error is not None:
                        raise error
                    progress["ready"] -= 1
                    persisted: dict[str, list[UUID]] = {}
                    saved_results: dict[int, list[UUID]] = {}
                    for key, result in values.items():
                        # One final HTTP response can both enrich the header
                        # and confirm the version. It remains one observation.
                        identity = id(result)
                        if identity not in saved_results:
                            saved_results[identity] = await self._persist_source_records(
                                summary.run_id, result
                            )
                            self._note_detail_result(
                                summary, connector.name, raw.external_id, key, result
                            )
                        persisted[key] = saved_results[identity]
                    raw_source_id = self._source_id_for(raw, discovery, discovery_record_ids)

                    async def commit(
                        state: _RecordWork = state,
                        raw: Any = raw,
                        values: dict[str, ConnectorResult[Any]] = values,
                        persisted: dict[str, list[UUID]] = persisted,
                        raw_source_id: UUID | None = raw_source_id,
                        reused_id: UUID | None = reused_id,
                    ) -> bool:
                        token = self._record_context.set(state)
                        try:
                            if reused_id is not None:
                                await self._commit_reused(
                                    connector, request, summary, raw, reused_id
                                )
                                return False
                            state.verification = dict(
                                connector=connector,
                                mode=request.mode,
                                raw=raw,
                                values=values,
                                references=persisted,
                                discovery_id=raw_source_id,
                                run_id=summary.run_id,
                                timezone=self.settings.timezone,
                            )
                            return await self._commit_bundle(
                                request.mode,
                                summary.run_id,
                                raw,
                                values,
                                persisted,
                                raw_source_id,
                                discovery_record_ids,
                            )
                        finally:
                            self._record_context.reset(token)

                    created = await self._execution().writer.submit(commit)
                    if reused_id is not None:
                        entry = summary.source_results[connector.name]
                        entry["records_reused"] += 1
                        entry["operations_reused"] += 4
                    state.outcome.count(summary, created)
                    if created:
                        summary.records_created += 1
                    else:
                        summary.records_updated += 1
                except (asyncio.CancelledError, PipelineCancelled):
                    raise
                except Exception as exc:
                    if coordinator:
                        await coordinator.failed_record(state.work_id)
                    summary.records_failed += 1
                    summary.source_results[connector.name]["records_failed"] = (
                        summary.records_failed
                    )
                    summary.diagnostics.append(
                        f"{connector.name}/{raw.external_id}: {type(exc).__name__}: {exc}"
                    )
                progress["processed"] += 1
                await self._report_activity(
                    ProgressEvent(
                        stage="details",
                        source=connector.name,
                        processed=progress["processed"],
                        total=progress["total"],
                        message="Registro examinado",
                    )
                )
                await self._update_progress(summary)
        finally:
            await stream.aclose()

    async def _fetch_bundle(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        raw: Any,
        cached: dict[str, ConnectorResult[Any]],
    ) -> dict[str, ConnectorResult[Any]]:
        async def procurement_items_results() -> dict[str, ConnectorResult[Any]]:
            # PNCP results reuse the audited items cached by fetch_items.
            items = await connector.fetch_items(raw.external_id)
            results = await connector.fetch_results(raw.external_id)
            return {"items": items, "results": results}

        async def procurement_detail() -> ConnectorResult[Any]:
            return cached.get(raw.external_id) or await connector.fetch_procurement(raw.external_id)

        verify = getattr(connector, "verify_procurement_version", None)
        global_version = (
            connector.name == "pncp"
            and request.mode == "procurements"
            and official_global_time(raw, self.settings.timezone) is not None
        )
        final_detail = global_version and callable(verify)
        calls: dict[str, Any]
        initial: dict[str, ConnectorResult[Any]] = {}
        if request.mode == "price_registries":
            price = cast(PriceRegistryConnector, connector)
            # Compras unit/adhesion queries require the official item numbers.
            # Finish this dependency once before fanning out the other details.
            initial["items"] = await price.fetch_price_registry_items(raw.external_id)
            calls = {}
            calls.update(
                {
                    key: self._price_registry_detail(price, method, raw)
                    for key, method in (
                        ("units", "fetch_price_registry_units"),
                        ("commitments", "fetch_price_registry_commitments"),
                        ("adhesions", "fetch_price_registry_adhesions"),
                    )
                }
            )
        elif request.mode == "contracts":
            if not request.contract_details:
                return {
                    key: ConnectorResult(data=[], availability=ConnectorAvailability.NOT_SUPPORTED)
                    for key in ("items", "terms")
                }
            contract = cast(ContractConnector, connector)
            calls = {
                key: self._contract_detail(contract, method, raw)
                for key, method in (
                    ("items", "fetch_contract_items"),
                    ("terms", "fetch_contract_terms"),
                )
            }
        else:
            calls = {
                "documents": connector.fetch_documents(raw.external_id),
                "items_results": procurement_items_results(),
            }
            if not final_detail:
                calls["detail"] = procurement_detail()
        tasks = {key: asyncio.create_task(call) for key, call in calls.items()}
        try:
            await asyncio.gather(*tasks.values())
            result = {key: task.result() for key, task in tasks.items()}
            if "items_results" in result:
                result.update(result.pop("items_results"))
            if final_detail:
                # Query the complete detail once, after every child response.
                # This fresh response also checks the discovery global clock;
                # a mismatched clock still prevents certification/reuse.
                assert callable(verify)
                detail = await verify(raw.external_id)
                result["detail"] = result["version_check"] = detail
            elif global_version:
                result["version_check"] = ConnectorResult(
                    data=None, availability=ConnectorAvailability.NOT_SUPPORTED
                )
            return {**initial, **result}
        finally:
            for task in tasks.values():
                if not task.cancelling():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    async def _commit_reused(
        self,
        connector: Any,
        request: PipelineRequest,
        summary: PipelineSummary,
        raw: Any,
        proof_id: UUID,
    ) -> None:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            proof = await find_reusable(
                session, connector, request.mode, raw, timezone=self.settings.timezone
            )
            if proof is None or proof.id != proof_id:
                raise ValueError("comprovação da versão mudou antes da confirmação")
            if coordinator := self._execution().coordinator:
                await coordinator.confirm(session, raw, work_id=self._record().work_id)
                work = await session.get_one(CollectionWorkItem, self._record().work_id)
                work.payload = {**work.payload, "verified_version_id": str(proof_id)}
            else:
                entry = summary.source_results[connector.name]
                entry.setdefault("verified_versions", []).append(str(proof_id))

    async def _save_verification(self, session: AsyncSession) -> None:
        state = self._record()
        if state.verification and not (state.outcome.stale or state.outcome.conflicted):
            await record_verified(session, **state.verification)

    async def _commit_bundle(
        self,
        mode: str,
        run_id: UUID,
        raw: Any,
        values: dict[str, ConnectorResult[Any]],
        stored: dict[str, list[UUID]],
        raw_source_id: UUID | None,
        discovery_record_ids: list[UUID],
    ) -> bool:
        if mode == "price_registries":
            items, units, commitments, adhesions = (
                values[key] for key in ("items", "units", "commitments", "adhesions")
            )
            persisted_items = stored["items"]
            persisted_units = stored["units"]
            persisted_commitments = stored["commitments"]
            persisted_adhesions = stored["adhesions"]
            created = await self._persist_price_registry_bundle(
                raw,
                items.data if items.is_available else [],
                source_record_id=raw_source_id or (self._single_source_id(discovery_record_ids)),
                item_source_record_ids=self._source_id_map(items.raw_records, persisted_items),
                units=units.data if units.is_available else [],
                unit_source_record_ids=self._source_id_map(units.raw_records, persisted_units),
                commitments=commitments.data if commitments.is_available else [],
                commitment_source_record_ids=self._source_id_map(
                    commitments.raw_records, persisted_commitments
                ),
                adhesions=adhesions.data if adhesions.is_available else [],
                adhesion_source_record_ids=self._source_id_map(
                    adhesions.raw_records, persisted_adhesions
                ),
                fallback_item_source_record_id=self._single_source_id(persisted_items),
                fallback_unit_source_record_id=self._single_source_id(persisted_units),
                fallback_commitment_source_record_id=(
                    self._single_source_id(persisted_commitments)
                ),
                fallback_adhesion_source_record_id=(self._single_source_id(persisted_adhesions)),
            )
        elif mode == "contracts":
            items, terms = values["items"], values["terms"]
            persisted_items, persisted_terms = stored["items"], stored["terms"]
            created = await self._persist_contract_bundle(
                raw,
                source_record_id=raw_source_id or (self._single_source_id(discovery_record_ids)),
                items=items.data if items.is_available else [],
                item_source_record_ids=self._source_id_map(items.raw_records, persisted_items),
                terms=terms.data if terms.is_available else [],
                term_source_record_ids=self._source_id_map(terms.raw_records, persisted_terms),
                fallback_item_source_record_id=self._single_source_id(persisted_items),
                fallback_term_source_record_id=self._single_source_id(persisted_terms),
            )
        else:  # procurement
            detail, items, documents, results = (
                values[key] for key in ("detail", "items", "documents", "results")
            )
            enriched = detail.data if detail.is_available and detail.data is not None else raw
            persisted = [stored[key] for key in ("detail", "items", "documents", "results")]
            created = await self._persist_procurement_bundle(
                run_id,
                enriched,
                items.data if items.is_available else [],
                [row for row in documents.data if documents.is_complete or row.active is not False]
                if documents.is_available
                else [],
                [row for row in results.data if results.is_complete or not row.is_cancelled]
                if results.is_available
                else [],
                source_record_id=(
                    self._source_id_for(enriched, detail, persisted[0])
                    if detail.is_available
                    else None
                )
                or raw_source_id,
                item_source_record_ids=self._source_id_map(items.raw_records, persisted[1]),
                document_source_record_ids=self._source_id_map(documents.raw_records, persisted[2]),
                result_source_record_ids=self._source_id_map(results.raw_records, persisted[3]),
                fallback_item_source_record_id=self._single_source_id(persisted[1]),
                fallback_document_source_record_id=self._single_source_id(persisted[2]),
                fallback_result_source_record_id=self._single_source_id(persisted[3]),
            )
        return created

    async def _contract_detail(
        self,
        connector: ContractConnector,
        method_name: str,
        raw: RawContract,
    ) -> ConnectorResult[list[Any]]:
        method = getattr(connector, method_name, None)
        if method is None:
            return ConnectorResult(
                data=[],
                availability=ConnectorAvailability.NOT_SUPPORTED,
                diagnostic=f"{connector.name} does not publish {method_name}",
            )
        return await method(raw)

    async def _persist_contract_bundle(
        self,
        raw: RawContract,
        *,
        source_record_id: UUID | None,
        items: list[RawContractItem] | None = None,
        item_source_record_ids: dict[str, UUID] | None = None,
        terms: list[RawContractTerm] | None = None,
        term_source_record_ids: dict[str, UUID] | None = None,
        fallback_item_source_record_id: UUID | None = None,
        fallback_term_source_record_id: UUID | None = None,
    ) -> bool:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            source_record = (
                await session.get(SourceRecord, source_record_id) if source_record_id else None
            )
            tracker = install_audit(
                session,
                raw,
                run_id=self._execution().run_id,
                source_record_id=source_record_id,
                observed_at=source_record.collected_at if source_record else None,
            )
            contract = await session.scalar(
                select(Contract).where(
                    Contract.source == raw.source,
                    Contract.external_id == raw.external_id,
                )
            )
            if contract is None and raw.pncp_control_number:
                contract = await session.scalar(
                    select(Contract).where(Contract.pncp_control_number == raw.pncp_control_number)
                )
            created = contract is None
            if contract is None:
                contract = Contract(
                    source=raw.source,
                    external_id=raw.external_id,
                    fingerprint=stable_fingerprint("contract", raw.source, raw.external_id),
                )
                session.add(contract)
                await session.flush()
            raw_supplier = normalize_cnpj(raw.supplier_cnpj)
            supplier_cnpj = raw_supplier if is_valid_cnpj(raw_supplier) else None
            for name, value in {
                "pncp_control_number": raw.pncp_control_number,
                "procurement_control_number": raw.procurement_control_number,
                "price_registry_control_number": raw.price_registry_control_number,
                "source_record_id": source_record_id,
                "contract_number": raw.contract_number,
                "year": raw.year,
                "process_number": raw.process_number,
                "agency_name": raw.agency_name,
                "agency_cnpj": normalize_cnpj(raw.agency_cnpj),
                "uasg": raw.uasg,
                "object_description": raw.object_description,
                "status": raw.status,
                "signed_at": raw.signed_at,
                "published_at": raw.published_at,
                "valid_from": raw.valid_from,
                "valid_until": raw.valid_until,
                "initial_value": raw.initial_value,
                "global_value": raw.global_value,
                "accumulated_value": raw.accumulated_value,
                "is_adhesion": raw.is_adhesion,
                "supplier_cnpj": supplier_cnpj,
                "supplier_name": raw.supplier_name,
                "source_url": raw.source_url,
            }.items():
                if value is not None:
                    setattr(contract, name, value)
            if raw.procurement_control_number:
                procurement = await session.scalar(
                    select(Procurement).where(
                        Procurement.pncp_control_number == raw.procurement_control_number
                    )
                )
                if procurement is not None:
                    contract.procurement_id = procurement.id
            if raw.price_registry_control_number:
                registry = await session.scalar(
                    select(PriceRegistry).where(
                        PriceRegistry.pncp_control_number == raw.price_registry_control_number
                    )
                )
                if registry is not None:
                    contract.price_registry_id = registry.id
            cnpj = normalize_cnpj(raw.supplier_cnpj)
            if cnpj and is_valid_cnpj(cnpj):
                company = await self._get_or_create_company(session, cnpj, raw.supplier_name)
                contract.company_id = company.id
            for raw_item in items or []:
                number = raw_item.item_number or raw_item.external_id or "1"
                fingerprint = stable_fingerprint("contract_item", contract.id, number)
                item_entity = await session.scalar(
                    select(ContractItem).where(ContractItem.fingerprint == fingerprint)
                )
                if item_entity is None:
                    item_entity = ContractItem(
                        contract_id=contract.id,
                        item_number=number,
                        fingerprint=fingerprint,
                    )
                    session.add(item_entity)
                tracker.register(item_entity, raw_item)
                item_entity.source_record_id = self._reference_id(
                    item_source_record_ids or {},
                    raw_item.source_record_ref,
                    fallback_item_source_record_id,
                )
                item_entity.description = raw_item.description or item_entity.description
                item_entity.unit = raw_item.unit or item_entity.unit
                for name in ("quantity", "unit_value", "total_value"):
                    value = getattr(raw_item, name)
                    if value is not None:
                        setattr(item_entity, name, value)
                item_cnpj = normalize_cnpj(raw_item.supplier_cnpj)
                item_entity.supplier_cnpj = (
                    item_cnpj if is_valid_cnpj(item_cnpj) else item_entity.supplier_cnpj
                )
                item_entity.supplier_name = raw_item.supplier_name or item_entity.supplier_name
            for raw_term in terms or []:
                term_number = raw_term.term_number or raw_term.external_id or "1"
                fingerprint = stable_fingerprint(
                    "contract_term", contract.id, term_number, raw_term.term_type
                )
                term_entity = await session.scalar(
                    select(ContractTerm).where(ContractTerm.fingerprint == fingerprint)
                )
                if term_entity is None:
                    term_entity = ContractTerm(
                        contract_id=contract.id,
                        term_number=term_number,
                        fingerprint=fingerprint,
                    )
                    session.add(term_entity)
                tracker.register(term_entity, raw_term)
                term_entity.source_record_id = self._reference_id(
                    term_source_record_ids or {},
                    raw_term.source_record_ref,
                    fallback_term_source_record_id,
                )
                term_entity.term_type = raw_term.term_type or term_entity.term_type
                term_entity.signed_at = raw_term.signed_at or term_entity.signed_at
                term_entity.published_at = raw_term.published_at or term_entity.published_at
                if raw_term.value_delta is not None:
                    term_entity.value_delta = raw_term.value_delta
                term_entity.new_valid_until = (
                    raw_term.new_valid_until or term_entity.new_valid_until
                )
                term_entity.object_description = (
                    raw_term.object_description or term_entity.object_description
                )
                term_entity.status = raw_term.status or term_entity.status
                term_entity.source_url = raw_term.source_url or term_entity.source_url
            await session.flush()
            self._record().outcome = tracker.outcome
            await self._save_verification(session)
            if coordinator := self._execution().coordinator:
                await coordinator.confirm(
                    session,
                    raw,
                    work_id=self._record().work_id,
                    details_pending=self._record().details_pending,
                    details_failed=self._record().details_failed,
                )
            return created

    async def _price_registry_detail(
        self,
        connector: PriceRegistryConnector,
        method_name: str,
        raw: RawPriceRegistry,
    ) -> ConnectorResult[list[Any]]:
        method = getattr(connector, method_name, None)
        if method is None:
            return ConnectorResult(
                data=[],
                availability=ConnectorAvailability.NOT_SUPPORTED,
                diagnostic=f"{connector.name} does not publish {method_name}",
            )
        return await method(raw)

    async def _persist_price_registry_bundle(
        self,
        raw: RawPriceRegistry,
        items: list[RawPriceRegistryItem],
        *,
        source_record_id: UUID | None,
        item_source_record_ids: dict[str, UUID] | None = None,
        units: list[RawPriceRegistryUnit] | None = None,
        unit_source_record_ids: dict[str, UUID] | None = None,
        commitments: list[RawPriceRegistryCommitment] | None = None,
        commitment_source_record_ids: dict[str, UUID] | None = None,
        adhesions: list[RawPriceRegistryAdhesion] | None = None,
        adhesion_source_record_ids: dict[str, UUID] | None = None,
        fallback_item_source_record_id: UUID | None = None,
        fallback_unit_source_record_id: UUID | None = None,
        fallback_commitment_source_record_id: UUID | None = None,
        fallback_adhesion_source_record_id: UUID | None = None,
    ) -> bool:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            source_record = (
                await session.get(SourceRecord, source_record_id) if source_record_id else None
            )
            tracker = install_audit(
                session,
                raw,
                run_id=self._execution().run_id,
                source_record_id=source_record_id,
                observed_at=source_record.collected_at if source_record else None,
            )
            registry = await session.scalar(
                select(PriceRegistry).where(
                    PriceRegistry.source == raw.source,
                    PriceRegistry.external_id == raw.external_id,
                )
            )
            if registry is None and raw.pncp_control_number:
                registry = await session.scalar(
                    select(PriceRegistry).where(
                        PriceRegistry.pncp_control_number == raw.pncp_control_number
                    )
                )
            created = registry is None
            if registry is None:
                registry = PriceRegistry(
                    source=raw.source,
                    external_id=raw.external_id,
                    fingerprint=stable_fingerprint("price_registry", raw.source, raw.external_id),
                )
                session.add(registry)
                await session.flush()
            values = {
                "pncp_control_number": raw.pncp_control_number,
                "linked_pncp_control_number": raw.linked_pncp_control_number,
                "source_record_id": source_record_id,
                "registry_number": raw.registry_number,
                "year": raw.year,
                "agency_name": raw.agency_name,
                "agency_cnpj": normalize_cnpj(raw.agency_cnpj),
                "uasg": raw.uasg,
                "object_description": raw.object_description,
                "status": raw.status,
                "signed_at": raw.signed_at,
                "published_at": raw.published_at,
                "valid_from": raw.valid_from,
                "valid_until": raw.valid_until,
                "total_value": raw.total_value,
                "allows_adhesion": raw.allows_adhesion,
                "source_url": raw.source_url,
            }
            for name, value in values.items():
                if value is not None:
                    setattr(registry, name, value)
            if raw.linked_pncp_control_number:
                procurement = await session.scalar(
                    select(Procurement).where(
                        Procurement.pncp_control_number == raw.linked_pncp_control_number
                    )
                )
                if procurement is not None:
                    registry.procurement_id = procurement.id
            await session.flush()
            for raw_item in items:
                number = raw_item.item_number or raw_item.external_id or "1"
                cnpj = normalize_cnpj(raw_item.supplier_cnpj)
                entity = await session.scalar(
                    select(PriceRegistryItem).where(
                        PriceRegistryItem.price_registry_id == registry.id,
                        PriceRegistryItem.item_number == number,
                        PriceRegistryItem.supplier_cnpj == cnpj,
                    )
                )
                company_id: UUID | None = None
                if cnpj and is_valid_cnpj(cnpj):
                    company = await self._get_or_create_company(
                        session, cnpj, raw_item.supplier_name
                    )
                    company_id = company.id
                if entity is None:
                    entity = PriceRegistryItem(
                        price_registry_id=registry.id,
                        item_number=number,
                        fingerprint=stable_fingerprint(
                            "price_registry_item", registry.id, number, cnpj
                        ),
                    )
                    session.add(entity)
                tracker.register(entity, raw_item)
                entity.company_id = company_id or entity.company_id
                entity.source_record_id = self._reference_id(
                    item_source_record_ids or {},
                    raw_item.source_record_ref,
                    fallback_item_source_record_id,
                )
                entity.description = raw_item.description or entity.description
                entity.unit = raw_item.unit or entity.unit
                if raw_item.quantity is not None:
                    entity.quantity = raw_item.quantity
                if raw_item.unit_value is not None:
                    entity.unit_value = raw_item.unit_value
                if raw_item.total_value is not None:
                    entity.total_value = raw_item.total_value
                if raw_item.max_adhesion_quantity is not None:
                    entity.max_adhesion_quantity = raw_item.max_adhesion_quantity
                entity.catalog_code = raw_item.catalog_code or entity.catalog_code
                entity.item_type = raw_item.item_type or entity.item_type
                entity.supplier_cnpj = cnpj or entity.supplier_cnpj
                entity.supplier_name = raw_item.supplier_name or entity.supplier_name
            for raw_unit in units or []:
                fingerprint = stable_fingerprint(
                    "price_registry_unit",
                    registry.id,
                    raw_unit.item_number,
                    raw_unit.unit_code,
                    raw_unit.supplier_name,
                )
                unit_entity = await session.scalar(
                    select(PriceRegistryUnit).where(PriceRegistryUnit.fingerprint == fingerprint)
                )
                if unit_entity is None:
                    unit_entity = PriceRegistryUnit(
                        price_registry_id=registry.id,
                        fingerprint=fingerprint,
                    )
                    session.add(unit_entity)
                tracker.register(unit_entity, raw_unit)
                unit_entity.source_record_id = self._reference_id(
                    unit_source_record_ids or {},
                    raw_unit.source_record_ref,
                    fallback_unit_source_record_id,
                )
                unit_entity.item_number = raw_unit.item_number or unit_entity.item_number
                unit_entity.catalog_code = raw_unit.catalog_code or unit_entity.catalog_code
                unit_entity.description = raw_unit.description or unit_entity.description
                unit_entity.supplier_name = raw_unit.supplier_name or unit_entity.supplier_name
                for name in (
                    "quantity_registered",
                    "adhesion_balance",
                    "remanejamento_balance",
                    "adhesion_limit",
                    "purchase_limit",
                ):
                    value = getattr(raw_unit, name)
                    if value is not None:
                        setattr(unit_entity, name, value)
                if raw_unit.accepts_adhesion is not None:
                    unit_entity.accepts_adhesion = raw_unit.accepts_adhesion
                unit_entity.unit_code = raw_unit.unit_code or unit_entity.unit_code
                unit_entity.unit_name = raw_unit.unit_name or unit_entity.unit_name
                unit_entity.unit_type = raw_unit.unit_type or unit_entity.unit_type
                unit_entity.included_at = raw_unit.included_at or unit_entity.included_at
                unit_entity.updated_at_source = raw_unit.updated_at or unit_entity.updated_at_source
                unit_entity.excluded_at = raw_unit.excluded_at or unit_entity.excluded_at
            for raw_commitment in commitments or []:
                fingerprint = stable_fingerprint(
                    "price_registry_commitment",
                    registry.id,
                    raw_commitment.item_number,
                    raw_commitment.unit,
                    raw_commitment.commitment_type,
                )
                commitment_entity = await session.scalar(
                    select(PriceRegistryCommitment).where(
                        PriceRegistryCommitment.fingerprint == fingerprint
                    )
                )
                if commitment_entity is None:
                    commitment_entity = PriceRegistryCommitment(
                        price_registry_id=registry.id,
                        fingerprint=fingerprint,
                    )
                    session.add(commitment_entity)
                tracker.register(commitment_entity, raw_commitment)
                commitment_entity.source_record_id = self._reference_id(
                    commitment_source_record_ids or {},
                    raw_commitment.source_record_ref,
                    fallback_commitment_source_record_id,
                )
                commitment_entity.item_number = (
                    raw_commitment.item_number or commitment_entity.item_number
                )
                commitment_entity.unit = raw_commitment.unit or commitment_entity.unit
                commitment_entity.commitment_type = (
                    raw_commitment.commitment_type or commitment_entity.commitment_type
                )
                for name in ("quantity_registered", "quantity_committed", "balance"):
                    value = getattr(raw_commitment, name)
                    if value is not None:
                        setattr(commitment_entity, name, value)
                commitment_entity.included_at = (
                    raw_commitment.included_at or commitment_entity.included_at
                )
                commitment_entity.updated_at_source = (
                    raw_commitment.updated_at or commitment_entity.updated_at_source
                )
            pending_adhesions: dict[str, PriceRegistryAdhesion] = {}
            for raw_adhesion in adhesions or []:
                fingerprint = stable_fingerprint(
                    "price_registry_adhesion",
                    registry.id,
                    raw_adhesion.item_number,
                    raw_adhesion.requesting_unit,
                    raw_adhesion.approved_at,
                )
                if fingerprint in pending_adhesions:
                    # Preserve differing observations of this identity before
                    # the next row changes it; all flushes remain in this transaction.
                    await session.flush()
                adhesion_entity = pending_adhesions.get(fingerprint) or await session.scalar(
                    select(PriceRegistryAdhesion).where(
                        PriceRegistryAdhesion.fingerprint == fingerprint
                    )
                )
                if adhesion_entity is None:
                    adhesion_entity = PriceRegistryAdhesion(
                        price_registry_id=registry.id,
                        fingerprint=fingerprint,
                    )
                    session.add(adhesion_entity)
                pending_adhesions[fingerprint] = adhesion_entity
                tracker.register(adhesion_entity, raw_adhesion)
                adhesion_entity.source_record_id = self._reference_id(
                    adhesion_source_record_ids or {},
                    raw_adhesion.source_record_ref,
                    fallback_adhesion_source_record_id,
                )
                adhesion_entity.item_number = (
                    raw_adhesion.item_number or adhesion_entity.item_number
                )
                adhesion_entity.requesting_unit = (
                    raw_adhesion.requesting_unit or adhesion_entity.requesting_unit
                )
                if raw_adhesion.approved_at is not None:
                    adhesion_entity.approved_at = raw_adhesion.approved_at
                if raw_adhesion.approved_quantity is not None:
                    adhesion_entity.approved_quantity = raw_adhesion.approved_quantity
            await session.flush()
            self._record().outcome = tracker.outcome
            await self._save_verification(session)
            if coordinator := self._execution().coordinator:
                await coordinator.confirm(
                    session,
                    raw,
                    work_id=self._record().work_id,
                    details_pending=self._record().details_pending,
                    details_failed=self._record().details_failed,
                )
            return created

    @staticmethod
    async def _get_or_create_company(
        session: AsyncSession,
        cnpj: str,
        name: str | None,
    ) -> Company:
        """Resolve a company by validated CNPJ, never by textual similarity."""

        company = await session.scalar(select(Company).where(Company.cnpj == cnpj))
        if company is None:
            company = Company(
                cnpj=cnpj,
                legal_name=name,
                normalized_name=normalize_company_name(name) or cnpj,
                fingerprint=stable_fingerprint("company", cnpj),
            )
            session.add(company)
            await session.flush()
        elif name and not company.legal_name:
            company.legal_name = name
            company.normalized_name = normalize_company_name(name) or cnpj
        return company

    @staticmethod
    def _note_partial_discovery(
        summary: PipelineSummary,
        source: str,
        result: ConnectorResult[Any],
    ) -> None:
        pagination = result.pagination
        pagination_incomplete = bool(
            pagination
            and (
                pagination.truncated
                or (pagination.pages_remaining or 0) > 0
                or (pagination.total_pages or 0) > (pagination.fetched_pages or 0)
            )
        )
        if pagination_incomplete:
            summary.diagnostics.append(
                f"{source}: paginacao incompleta; resultados parciais preservados"
            )
        if result.diagnostic and result.availability is ConnectorAvailability.AVAILABLE:
            summary.diagnostics.append(f"{source}: {result.diagnostic}")

    @staticmethod
    def _note_detail_result(
        summary: PipelineSummary,
        source: str,
        external_id: str,
        kind: str,
        result: ConnectorResult[Any],
        *,
        optional: bool = False,
    ) -> None:
        del optional
        status = result.availability
        pagination = result.pagination
        incomplete = bool(
            result.partial
            or (
                pagination
                and (
                    pagination.truncated
                    or (pagination.pages_remaining or 0) > 0
                    or (pagination.total_pages or 0) > (pagination.fetched_pages or 0)
                )
            )
        )
        if incomplete:
            summary.diagnostics.append(
                f"{source}/{external_id}: {kind} incompleto; dados parciais preservados"
            )
        if status in {
            ConnectorAvailability.TEMPORARY_ERROR,
            ConnectorAvailability.ACCESS_RESTRICTED,
        }:
            summary.diagnostics.append(
                f"{source}/{external_id}: {kind}: {result.diagnostic or status.value}"
            )

    @staticmethod
    def _single_source_id(identifiers: list[UUID]) -> UUID | None:
        return identifiers[0] if len(identifiers) == 1 else None

    @staticmethod
    def _source_id_map(
        records: list[RawSourceRecord],
        persisted_ids: list[UUID],
    ) -> dict[str, UUID]:
        return {
            record.record_key: source_id
            for record, source_id in zip(records, persisted_ids, strict=True)
        }

    @staticmethod
    def _reference_id(
        mapping: dict[str, UUID], reference: str | None, fallback: UUID | None
    ) -> UUID | None:
        if reference:
            if reference not in mapping:
                raise ValueError("source_record_ref sem resposta correspondente")
            return mapping[reference]
        if mapping and fallback is None:
            raise ValueError("proveniência ambígua: mais de uma resposta possível")
        return fallback

    @staticmethod
    def _source_id_for(
        raw: Any,
        result: ConnectorResult[Any],
        persisted_ids: list[UUID],
    ) -> UUID | None:
        reference = getattr(raw, "source_record_ref", None)
        if reference:
            for raw_record, source_id in zip(result.raw_records, persisted_ids, strict=False):
                if raw_record.record_key == reference:
                    return source_id
            raise ValueError("source_record_ref sem resposta correspondente")
        if len(persisted_ids) > 1:
            raise ValueError("proveniência ambígua na descoberta")
        return persisted_ids[0] if persisted_ids else None

    async def _persist_source_records(
        self,
        run_id: UUID,
        result: ConnectorResult[Any],
    ) -> list[UUID]:
        identifiers: list[UUID] = []
        for raw in result.raw_records:
            async with write_session(self.session_factory) as session:
                entity = await self._source_record(
                    session,
                    run_id,
                    raw,
                    result.availability,
                    result.diagnostic,
                    collection_complete=result.is_complete,
                    pagination_metadata=result.pagination.model_dump()
                    if result.pagination
                    else None,
                )
                identifiers.append(entity.id)
        return identifiers

    async def _source_record(
        self,
        session: AsyncSession,
        run_id: UUID,
        raw: RawSourceRecord,
        availability: ConnectorAvailability,
        diagnostic: str | None,
        *,
        collection_complete: bool | None = None,
        pagination_metadata: dict[str, Any] | None = None,
    ) -> SourceRecord:
        fingerprint = stable_fingerprint(
            "source_record",
            run_id,
            raw.source,
            raw.endpoint,
            _json_value(raw.request_parameters),
            raw.response_hash,
            collection_complete,
            _json_value(pagination_metadata),
        )
        await self._assert_fence(session)
        entity = await session.scalar(
            select(SourceRecord).where(SourceRecord.fingerprint == fingerprint)
        )
        if entity is None:
            entity = SourceRecord(
                crawl_run_id=run_id,
                source=raw.source,
                endpoint=raw.endpoint,
                request_parameters=_json_value(raw.request_parameters),
                response_hash=raw.response_hash,
                raw_payload=_json_value(raw.raw_payload),
                http_status=raw.http_status,
                availability=DataAvailability(availability.value),
                collected_at=raw.collected_at,
                diagnostic=diagnostic,
                collection_complete=collection_complete,
                pagination_metadata=pagination_metadata,
                fingerprint=fingerprint,
            )
            session.add(entity)
            await session.flush()
        return entity

    async def _persist_procurement_bundle(
        self,
        run_id: UUID,
        raw: RawProcurement,
        items: list[RawProcurementItem],
        documents: list[RawDocument],
        results: list[RawResult],
        *,
        source_record_id: UUID | None,
        item_source_record_ids: dict[str, UUID] | None = None,
        document_source_record_ids: dict[str, UUID] | None = None,
        result_source_record_ids: dict[str, UUID] | None = None,
        fallback_item_source_record_id: UUID | None = None,
        fallback_document_source_record_id: UUID | None = None,
        fallback_result_source_record_id: UUID | None = None,
    ) -> bool:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            source_record = (
                await session.get(SourceRecord, source_record_id) if source_record_id else None
            )
            tracker = install_audit(
                session,
                raw,
                run_id=self._execution().run_id,
                source_record_id=source_record_id,
                observed_at=source_record.collected_at if source_record else None,
            )
            repository = ProcurementRepository(session)
            procurement = await repository.resolve_canonical(
                source=raw.source,
                external_id=raw.external_id,
                pncp_control_number=raw.pncp_control_number,
                agency_cnpj=normalize_cnpj(raw.agency_cnpj),
                uasg=raw.uasg,
                purchase_number=raw.purchase_number,
                purchase_year=raw.purchase_year,
                modality=raw.modality,
            )
            created = procurement is None
            if procurement is None:
                procurement = Procurement(
                    source=raw.source,
                    external_id=raw.external_id,
                    fingerprint=stable_fingerprint("procurement", raw.source, raw.external_id),
                )
                session.add(procurement)
                await session.flush()
            self._apply_procurement(procurement, raw)
            # Derive the phase from accepted fields, after rejecting older source values.
            await session.flush()
            await refresh_procurement_phase(session, procurement)
            source = await session.scalar(
                select(ProcurementSource).where(
                    ProcurementSource.source == raw.source,
                    ProcurementSource.external_id == raw.external_id,
                )
            )
            if source is None:
                source = ProcurementSource(
                    procurement_id=procurement.id,
                    source_record_id=source_record_id,
                    source=raw.source,
                    external_id=raw.external_id,
                    pncp_control_number=raw.pncp_control_number,
                    endpoint=raw.source_url,
                    source_url=raw.source_url,
                    collected_at=datetime.now(UTC),
                    is_primary=created or procurement.source == raw.source,
                    fingerprint=stable_fingerprint(
                        "procurement_source", raw.source, raw.external_id
                    ),
                )
                session.add(source)
            else:
                source.source_record_id = source_record_id
                source.source_url = raw.source_url or source.source_url
                source.collected_at = datetime.now(UTC)
            await session.flush()
            await self._observe_fields(session, procurement, raw, source_record_id)
            item_map = await self._upsert_items(
                session,
                procurement,
                items,
                item_source_record_ids or {},
                fallback_item_source_record_id,
            )
            await self._upsert_documents(
                session,
                procurement,
                documents,
                document_source_record_ids or {},
                fallback_document_source_record_id,
            )
            await self._upsert_results(
                session,
                procurement,
                results,
                item_map,
                result_source_record_ids or {},
                fallback_result_source_record_id,
            )
            await session.flush()
            await refresh_procurement_phase(session, procurement)
            await session.flush()
            self._record().outcome = tracker.outcome
            await self._save_verification(session)
            if coordinator := self._execution().coordinator:
                await coordinator.confirm(
                    session,
                    raw,
                    work_id=self._record().work_id,
                    details_pending=self._record().details_pending,
                    details_failed=self._record().details_failed,
                )
            return created

    @staticmethod
    def _apply_procurement(entity: Procurement, raw: RawProcurement) -> None:
        values = {
            "pncp_control_number": raw.pncp_control_number,
            "uasg": raw.uasg,
            "purchase_number": raw.purchase_number,
            "purchase_year": raw.purchase_year,
            "modality": raw.modality,
            "modality_key": canonical_modality(raw.modality),
            "procurement_type": modality_category(raw.modality),
            "is_srp": raw.is_srp,
            "legal_basis": str(raw.legal_basis)[:255] if raw.legal_basis else None,
            "title": raw.title,
            "object_description": raw.object_description,
            "agency_name": raw.agency_name,
            "agency_cnpj": normalize_cnpj(raw.agency_cnpj),
            "government_sphere": raw.government_sphere,
            "government_branch": raw.government_branch,
            "uf": raw.uf.upper() if raw.uf else None,
            "municipality": raw.municipality,
            "estimated_value": raw.estimated_value,
            "proposal_start_at": raw.proposal_start_at,
            "proposal_end_at": raw.proposal_end_at,
            "session_start_at": raw.session_start_at,
            "publication_at": raw.publication_at,
            "last_source_update_at": raw.last_source_update_at,
            "status": raw.status,
            "source_url": raw.source_url,
        }
        for name, value in values.items():
            if value is not None:
                setattr(entity, name, value)

    async def _observe_fields(
        self,
        session: AsyncSession,
        procurement: Procurement,
        raw: RawProcurement,
        source_record_id: UUID | None,
    ) -> None:
        observed = {
            "pncp_control_number": raw.pncp_control_number,
            "uasg": raw.uasg,
            "purchase_number": raw.purchase_number,
            "purchase_year": raw.purchase_year,
            "modality": raw.modality,
            "modality_key": canonical_modality(raw.modality),
            "procurement_type": modality_category(raw.modality),
            "is_srp": raw.is_srp,
            "legal_basis": raw.legal_basis,
            "object_description": raw.object_description,
            "agency_name": raw.agency_name,
            "agency_cnpj": normalize_cnpj(raw.agency_cnpj),
            "estimated_value": raw.estimated_value,
            "proposal_end_at": raw.proposal_end_at,
            "status": raw.status,
        }
        for field_name, value in observed.items():
            status = FieldValueStatus.OBSERVED if value is not None else FieldValueStatus.UNKNOWN
            normalized = _json_value(value)
            fingerprint = stable_fingerprint(
                "field_observation",
                procurement.id,
                raw.source,
                field_name,
                normalized,
                status.value,
            )
            exists = await session.scalar(
                select(FieldObservation.id).where(FieldObservation.fingerprint == fingerprint)
            )
            if exists is None:
                session.add(
                    FieldObservation(
                        entity_type="procurement",
                        entity_id=procurement.id,
                        field_name=field_name,
                        value=normalized,
                        value_status=status,
                        source=raw.source,
                        source_record_id=source_record_id,
                        confidence=Decimal("1") if value is not None else None,
                        collected_at=datetime.now(UTC),
                        fingerprint=fingerprint,
                    )
                )

    async def _upsert_items(
        self,
        session: AsyncSession,
        procurement: Procurement,
        items: list[RawProcurementItem],
        source_record_ids: dict[str, UUID],
        fallback_source_record_id: UUID | None = None,
    ) -> dict[str, ProcurementItem]:
        item_map: dict[str, ProcurementItem] = {}
        by_number = {
            item.item_number: item
            for item in (
                await session.scalars(
                    select(ProcurementItem).where(ProcurementItem.procurement_id == procurement.id)
                )
            ).all()
        }
        observed: set[str] = set()
        for position, raw in enumerate(items, start=1):
            number = raw.item_number or raw.external_id or str(position)
            source_record_id = self._reference_id(
                source_record_ids, raw.source_record_ref, fallback_source_record_id
            )
            entity = by_number.get(number)
            if number in observed:
                # Record each received version in the transactional audit before
                # applying another observation of this same official item.
                await session.flush()
            if entity is None:
                entity = ProcurementItem(
                    procurement_id=procurement.id,
                    source_record_id=source_record_id,
                    item_number=number,
                    fingerprint=stable_fingerprint("item", procurement.id, number),
                )
                session.add(entity)
                by_number[number] = entity
            if tracker := session.info.get("field_update_tracker"):
                tracker.register(entity, raw)
            entity.external_item_id = raw.external_id
            entity.source_record_id = source_record_id
            entity.description = raw.detailed_description or raw.description
            entity.quantity = raw.quantity
            entity.unit = raw.unit
            entity.estimated_unit_value = raw.estimated_unit_value
            entity.estimated_total_value = raw.estimated_total_value
            entity.result_status = raw.result_status
            item_map[raw.external_id] = entity
            item_map[number] = entity
            observed.add(number)
        await session.flush()
        return item_map

    async def _upsert_documents(
        self,
        session: AsyncSession,
        procurement: Procurement,
        documents: list[RawDocument],
        source_record_ids: dict[str, UUID],
        fallback_source_record_id: UUID | None = None,
    ) -> None:
        from app.services.documents.identity import existing_document, register_alias

        for raw in documents:
            fingerprint = stable_fingerprint("document", procurement.id, raw.download_url)
            source_record_id = self._reference_id(
                source_record_ids, raw.source_record_ref, fallback_source_record_id
            )
            source_record = (
                await session.get(SourceRecord, source_record_id) if source_record_id else None
            )
            entity = await existing_document(
                session, procurement.id, raw.download_url, source_record
            )
            if entity is None:
                entity = await session.scalar(
                    select(Document).where(Document.fingerprint == fingerprint)
                )
            if entity is not None:
                old_record = (
                    await session.get(SourceRecord, entity.source_record_id)
                    if entity.source_record_id
                    else None
                )
                await register_alias(
                    session,
                    entity,
                    url=entity.original_url,
                    record=old_record,
                    allow_schedule=False,
                )
            active = raw.active is not False
            if entity is None:
                entity = Document(
                    procurement_id=procurement.id,
                    source_record_id=source_record_id,
                    document_type=raw.document_type,
                    title=raw.title or raw.external_id,
                    original_url=raw.download_url,
                    mime_type=raw.mime_type,
                    extraction_status=(
                        ExtractionStatus.PENDING if active else ExtractionStatus.UNSUPPORTED
                    ),
                    is_active=active,
                    deactivated_at=None if active else datetime.now(UTC),
                    published_at=raw.published_at,
                    fingerprint=fingerprint,
                )
                session.add(entity)
                if tracker := session.info.get("field_update_tracker"):
                    tracker.register(entity, raw)
                await session.flush()
                await register_alias(session, entity, url=raw.download_url, record=source_record)
            else:
                was_active = entity.is_active
                if tracker := session.info.get("field_update_tracker"):
                    tracker.register(entity, raw)
                entity.source_record_id = source_record_id
                entity.document_type = raw.document_type or entity.document_type
                entity.title = raw.title or entity.title
                entity.mime_type = raw.mime_type or entity.mime_type
                entity.is_active = active
                entity.published_at = raw.published_at or entity.published_at
                # Reject stale source values before changing extraction or derived facts.
                await session.flush()
                await register_alias(session, entity, url=raw.download_url, record=source_record)
                if entity.is_active == was_active:
                    continue
                active = entity.is_active
                entity.deactivated_at = None if active else datetime.now(UTC)
                if not active and entity.extraction_status is ExtractionStatus.PENDING:
                    entity.extraction_status = ExtractionStatus.UNSUPPORTED
                elif (
                    active
                    and not was_active
                    and entity.extraction_status is ExtractionStatus.UNSUPPORTED
                ):
                    entity.extraction_status = ExtractionStatus.PENDING
                    entity.extraction_error = None
                    entity.next_attempt_at = None
                if (
                    active
                    and not was_active
                    and await session.scalar(
                        select(Document.id).where(Document.parent_document_id == entity.id).limit(1)
                    )
                ):
                    # Withdrawn archive members stay inactive until the current
                    # archive is downloaded and its membership is verified again.
                    entity.extraction_status = ExtractionStatus.PENDING
                if not active:
                    from app.services.ingestion.processor import suspend_document_derivatives

                    await suspend_document_derivatives(session, entity.id)

    @staticmethod
    async def _result_item(
        session: AsyncSession,
        procurement: Procurement,
        raw: RawResult,
        item_map: dict[str, ProcurementItem],
    ) -> ProcurementItem | None:
        item = item_map.get(raw.item_external_id or "") or item_map.get(raw.item_number or "")
        if item is not None:
            return item
        identities = []
        if raw.item_external_id:
            identities.append(ProcurementItem.external_item_id == raw.item_external_id)
        if raw.item_number:
            identities.append(ProcurementItem.item_number == raw.item_number)
        if not identities:
            return None
        stored = list(
            (
                await session.scalars(
                    select(ProcurementItem)
                    .where(ProcurementItem.procurement_id == procurement.id, or_(*identities))
                    .limit(2)
                )
            ).all()
        )
        return stored[0] if len(stored) == 1 else None

    async def _upsert_results(
        self,
        session: AsyncSession,
        procurement: Procurement,
        results: list[RawResult],
        item_map: dict[str, ProcurementItem],
        source_record_ids: dict[str, UUID],
        fallback_source_record_id: UUID | None = None,
    ) -> None:
        for raw in results:
            source_record_id = self._reference_id(
                source_record_ids, raw.source_record_ref, fallback_source_record_id
            )
            # Cancelled outcomes and natural persons are retained in SourceRecord,
            # but never represented as an active awarded company.
            if raw.is_cancelled:
                cnpj = normalize_cnpj(raw.supplier_cnpj)
                company = (
                    await session.scalar(select(Company).where(Company.cnpj == cnpj))
                    if cnpj and is_valid_cnpj(cnpj)
                    else None
                )
                if company is not None:
                    item = await self._result_item(session, procurement, raw, item_map)
                    fingerprint = stable_fingerprint(
                        "participant",
                        procurement.id,
                        company.id,
                        item.id if item else None,
                        (
                            ParticipantRole.AWARDED
                            if raw.role.lower() == "awarded"
                            else ParticipantRole.WINNER
                        ).value,
                        raw.external_id,
                    )
                    participant = await session.scalar(
                        select(Participant).where(Participant.fingerprint == fingerprint)
                    )
                    if participant is not None:
                        participant.source_record_id = source_record_id
                        participant.status = raw.result_status or "cancelado"
                        participant.status_code = ParticipantStatus.CANCELLED
                        await session.flush()
                        from app.services.ingestion.processor import (
                            review_cancelled_award_derivatives,
                        )

                        await review_cancelled_award_derivatives(session, participant)
                continue
            if raw.person_type and raw.person_type.upper() in {"F", "PF", "CPF"}:
                continue
            cnpj = normalize_cnpj(raw.supplier_cnpj)
            if not cnpj or not is_valid_cnpj(cnpj):
                continue
            company = await self._get_or_create_company(session, cnpj, raw.supplier_name)

            item = await self._result_item(session, procurement, raw, item_map)
            role = (
                ParticipantRole.AWARDED if raw.role.lower() == "awarded" else ParticipantRole.WINNER
            )
            fingerprint = stable_fingerprint(
                "participant",
                procurement.id,
                company.id,
                item.id if item else None,
                role.value,
                raw.external_id,
            )
            participant = await session.scalar(
                select(Participant).where(Participant.fingerprint == fingerprint)
            )
            if participant is None:
                session.add(
                    Participant(
                        procurement_id=procurement.id,
                        company_id=company.id,
                        item_id=item.id if item else None,
                        participation_role=role,
                        final_value=raw.total_value,
                        rank=raw.rank,
                        status=raw.result_status,
                        status_code=participant_status_code(role.value, raw.result_status),
                        source=raw.source,
                        source_record_id=source_record_id,
                        confidence=Decimal("1"),
                        fingerprint=fingerprint,
                    )
                )
            else:
                participant.source_record_id = source_record_id
                participant.final_value = (
                    raw.total_value if raw.total_value is not None else participant.final_value
                )
                participant.rank = raw.rank if raw.rank is not None else participant.rank
                participant.status = raw.result_status or participant.status
                new_code = participant_status_code(
                    participant.participation_role.value, participant.status
                )
                if new_code is not ParticipantStatus.UNKNOWN or participant.status_code is None:
                    participant.status_code = new_code

    async def _cancellation_requested(self, run_id: UUID) -> bool:
        """Return True when an operator asked to stop this run."""

        async with self.session_factory() as session:
            return bool(
                await session.scalar(select(CrawlRun.cancel_requested).where(CrawlRun.id == run_id))
            )

    @staticmethod
    def _lease_names(request: PipelineRequest) -> list[str]:
        if request.mode == "documents":
            return [DOCUMENT_LEASE]
        connector = request.connector.lower()
        names = ["pncp", "compras_gov"] if connector == "all" else [connector]
        uf = request.uf.upper()
        leases = [f"crawl:{name}:{uf}" for name in names]
        if request.process_documents and request.mode == "procurements":
            leases.append(DOCUMENT_LEASE)
        return leases

    async def _fence_owned(self, session: AsyncSession) -> bool:
        if self._execution().run_id is None:
            return True
        for name, generation in self._execution().generations.items():
            if not await JobLeaseRepository(session).fence(
                name=name,
                owner_id=f"pipeline:{self._execution().run_id}",
                generation=generation,
                now=datetime.now(UTC),
            ):
                return False
        return bool(self._execution().generations)

    async def _assert_fence(self, session: AsyncSession) -> None:
        if self._execution().run_id is None:
            return
        if not await self._fence_owned(session):
            raise PipelineCancelled("lease perdida")
        run = await session.get(CrawlRun, self._execution().run_id)
        if (
            run is None
            or run.cancel_requested
            or run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}
        ):
            raise PipelineCancelled("coleta encerrada")

    async def _checkpoint(self) -> None:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)

    async def _heartbeat_leases(self, names: list[str], owner: str) -> None:
        while True:
            await asyncio.sleep(60)
            for name in names:
                if not await self._renew_lease(name, owner):
                    self._execution().lease_lost = True
                    worker = self._execution().worker
                    if worker is not None:
                        worker.cancel()
                    return

    async def _renew_lease(self, name: str, owner: str) -> bool:
        now = datetime.now(UTC)
        async with write_session(self.session_factory) as session:
            renewed = await JobLeaseRepository(session).renew(
                name=name,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(hours=2),
                generation=self._execution().generations.get(name),
            )
            if renewed and self._execution().run_id is not None:
                run = await session.get(CrawlRun, self._execution().run_id)
                if run is not None and run.status in {
                    CrawlRunStatus.RUNNING,
                    CrawlRunStatus.PENDING,
                }:
                    run.heartbeat_at = now
            return renewed

    async def _acquire_lease(self, name: str, owner: str) -> bool:
        now = datetime.now(UTC)
        async with write_session(self.session_factory) as session:
            repository = JobLeaseRepository(session)
            acquired = await repository.acquire(
                name=name,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(hours=2),
            )
            if acquired:
                self._execution().generations[name] = int(
                    await session.scalar(select(JobLease.version).where(JobLease.name == name)) or 1
                )
            return acquired

    @staticmethod
    async def _owns_run_lease(session: AsyncSession, run: CrawlRun) -> bool:
        filters = run.filters if isinstance(run.filters, dict) else {}
        connector = str(filters.get("connector") or run.connector).lower()
        uf = str(filters.get("uf") or "").upper()
        if not uf:
            return True
        if filters.get("mode") == "documents":
            lease_names = [DOCUMENT_LEASE]
        else:
            names = ["pncp", "compras_gov"] if connector == "all" else [connector]
            lease_names = [f"crawl:{name}:{uf}" for name in names]
            if (
                filters.get("process_documents")
                and filters.get("mode", "procurements") == "procurements"
            ):
                lease_names.append(DOCUMENT_LEASE)
        expected_owner = f"pipeline:{run.id}"
        for name in lease_names:
            owner = await session.scalar(
                select(JobLease.owner_id).where(
                    JobLease.name == name,
                )
            )
            if owner is None:
                return False
            if owner != expected_owner:
                return False
        return True

    async def _release_lease(self, name: str, owner: str) -> None:
        async with write_session(self.session_factory) as session:
            repository = JobLeaseRepository(session)
            await repository.release(
                name=name, owner_id=owner, generation=self._execution().generations.get(name)
            )

    async def _abort_admission(
        self, run_id: UUID, names: list[str], owner: str, *, cancelled: bool
    ) -> None:
        async with write_session(self.session_factory) as session:
            run = await session.get(CrawlRun, run_id)
            if run is not None and run.status is CrawlRunStatus.PENDING:
                run.status = CrawlRunStatus.CANCELLED if cancelled else CrawlRunStatus.FAILED
                run.cancel_requested = cancelled
                run.finished_at = datetime.now(UTC)
                run.diagnostic = (
                    "coleta cancelada durante a admissão"
                    if cancelled
                    else "falha durante a admissão da coleta"
                )
                run.errors = [{"message": run.diagnostic}]
            for name in reversed(names):
                await JobLeaseRepository(session).release(
                    name=name, owner_id=owner, generation=self._execution().generations.get(name)
                )

    async def _mark_running(self, run_id: UUID) -> None:
        async with write_session(self.session_factory) as session:
            run = await session.get(CrawlRun, run_id)
            if run is None:
                raise LookupError(f"crawl run {run_id} not found")
            await self._assert_fence(session)
            run.status = CrawlRunStatus.RUNNING
            run.started_at = datetime.now(UTC)
            run.heartbeat_at = datetime.now(UTC)

    async def _fail_run(self, run_id: UUID, diagnostic: str) -> None:
        async with write_session(self.session_factory) as session:
            run = await session.get(CrawlRun, run_id)
            if run is not None and run.status in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                run.status = CrawlRunStatus.FAILED
                run.started_at = run.started_at or datetime.now(UTC)
                run.finished_at = datetime.now(UTC)
                run.diagnostic = diagnostic
                run.errors = [{"message": diagnostic}]

    async def _report_activity(self, event: ProgressEvent) -> None:
        recorder = self._execution().progress
        if recorder is not None:
            if event.http_active is not None or event.download_active is not None:
                recorder.enqueue(event, self._persist_activity)
            else:
                await recorder.observe(event, self._persist_activity)

    async def _persist_activity(self) -> None:
        async with write_session(self.session_factory) as session:
            await self._assert_fence(session)
            run = await session.get(CrawlRun, self._execution().run_id)
            recorder = self._execution().progress
            if run is not None and recorder is not None:
                recorder.apply(run)
                run.heartbeat_at = datetime.now(UTC)

    def _aggregate_sources(self) -> PipelineSummary | None:
        execution = self._execution()
        root = execution.root_summary
        if root is not None and execution.summaries:
            for name in (
                "records_found",
                "records_created",
                "records_updated",
                "records_failed",
                "records_changed",
                "records_unchanged",
                "records_stale_ignored",
                "records_conflicted",
            ):
                setattr(
                    root, name, sum(getattr(local, name) for local in execution.summaries.values())
                )
            root.diagnostics = [
                item for local in execution.summaries.values() for item in local.diagnostics
            ]
            root.source_results = {
                name: entry
                for local in execution.summaries.values()
                for name, entry in local.source_results.items()
            }
        return root

    async def _update_progress(self, summary: PipelineSummary) -> None:
        """Persist partial counters so a long crawl reports honest progress."""

        async with write_session(self.session_factory) as session:
            summary = self._aggregate_sources() or summary
            run = await session.get(CrawlRun, summary.run_id)
            if run is None:
                return
            if run.status not in {CrawlRunStatus.RUNNING, CrawlRunStatus.PENDING}:
                return
            await self._assert_fence(session)
            run.records_found = summary.records_found
            run.records_created = summary.records_created
            run.records_updated = summary.records_updated
            run.counting_version = 2
            run.records_changed = summary.records_changed
            run.records_unchanged = summary.records_unchanged
            run.records_stale_ignored = summary.records_stale_ignored
            run.records_conflicted = summary.records_conflicted
            run.cursor = {**(run.cursor or {}), "sources": summary.source_results}
            recorder = self._execution().progress
            if recorder is not None:
                recorder.apply(run)
            run.heartbeat_at = datetime.now(UTC)
            run.diagnostic = "\n".join(summary.diagnostics) or None
            run.errors = [{"message": item} for item in summary.diagnostics]

    async def _finish_run(self, summary: PipelineSummary) -> None:
        async with write_session(self.session_factory) as session:
            run = await session.get(CrawlRun, summary.run_id)
            if run is None:
                return
            if run.status not in {CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING}:
                return
            if not await self._owns_run_lease(session, run):
                return
            if not await self._fence_owned(session):
                return
            if coordinator := self._execution().coordinator:
                await coordinator.finish_in_session(session, summary.status)
                from app.services.ingestion.scheduled_collections import finish_sync

                batch = await session.get_one(CollectionBatch, coordinator.batch_id)
                await finish_sync(session, run, batch, summary.status)
                validations = (
                    await session.execute(
                        select(CollectionWorkItem.source, func.count())
                        .where(
                            CollectionWorkItem.batch_id == coordinator.batch_id,
                            CollectionWorkItem.kind == "validation",
                            CollectionWorkItem.payload["crawl_run_id"].as_string() == str(run.id),
                        )
                        .group_by(CollectionWorkItem.source)
                    )
                ).all()
                for source, count in validations:
                    if source in summary.source_results:
                        entry = summary.source_results[source]
                        entry["operations_reused"] = entry.get("operations_reused", 0) + count
            run.status = summary.status
            if summary.status is CrawlRunStatus.CANCELLED:
                from app.services.llm.queue import cancel_for_run

                await cancel_for_run(session, run.id)
            run.finished_at = datetime.now(UTC)
            run.records_found = summary.records_found
            run.records_created = summary.records_created
            run.records_updated = summary.records_updated
            run.counting_version = 2
            run.records_changed = summary.records_changed
            run.records_unchanged = summary.records_unchanged
            run.records_stale_ignored = summary.records_stale_ignored
            run.records_conflicted = summary.records_conflicted
            run.cursor = {**(run.cursor or {}), "sources": summary.source_results}
            recorder = self._execution().progress
            if recorder is not None:
                recorder.record(
                    ProgressEvent(
                        stage=summary.status.value,
                        message=STAGE_LABELS.get(summary.status.value),
                    )
                )
                recorder.apply(run)
            run.diagnostic = "\n".join(summary.diagnostics) or None
            run.errors = [{"message": item} for item in summary.diagnostics]
            # Publish the terminal state and relinquish ownership atomically. A
            # restart after this commit must not strand a completed run's lease.
            leases = JobLeaseRepository(session)
            for name, generation in self._execution().generations.items():
                await leases.release(
                    name=name, owner_id=f"pipeline:{run.id}", generation=generation
                )
