"""Source-to-database ingestion with conservative canonical matching."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.connectors.base import (
    ConnectorResult,
    ProcurementFilters,
    ProcurementSourceConnector,
    RawDocument,
    RawProcurement,
    RawProcurementItem,
    RawResult,
    RawSourceRecord,
)
from app.connectors.base import (
    DataAvailability as ConnectorAvailability,
)
from app.database import async_session_factory
from app.models import (
    Company,
    CrawlRun,
    CrawlRunStatus,
    DataAvailability,
    Document,
    ExtractionStatus,
    FieldObservation,
    FieldValueStatus,
    Participant,
    ParticipantRole,
    Procurement,
    ProcurementItem,
    ProcurementSource,
    SourceRecord,
)
from app.repositories.crawls import JobLeaseRepository
from app.repositories.procurements import ProcurementRepository
from app.services.identifiers import (
    canonical_modality,
    is_valid_cnpj,
    normalize_cnpj,
    normalize_company_name,
)


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
    keyword: str | None = None
    max_pages: int | None = None
    process_documents: bool = True
    document_batch_size: int | None = None

    def filters(self) -> ProcurementFilters:
        return ProcurementFilters(
            start_date=self.start_date,
            end_date=self.end_date,
            days=self.days,
            uf=self.uf.upper(),
            municipality=self.municipality,
            agency=self.agency,
            keyword=self.keyword,
            modalities=list(self.modalities),
            max_pages=self.max_pages,
        )

    def as_json(self) -> dict[str, Any]:
        return _json_value(
            {
                "connector": self.connector,
                "uf": self.uf.upper(),
                "days": self.days,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "modalities": list(self.modalities),
                "municipality": self.municipality,
                "agency": self.agency,
                "keyword": self.keyword,
                "max_pages": self.max_pages,
                "process_documents": self.process_documents,
                "document_batch_size": self.document_batch_size,
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
    diagnostics: list[str] = field(default_factory=list)
    source_results: dict[str, dict[str, Any]] = field(default_factory=dict)


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

    async def create_run(self, request: PipelineRequest) -> CrawlRun:
        """Persist a pending run before background execution starts."""

        connector = request.connector.lower()
        if connector not in {"pncp", "compras_gov", "all"}:
            raise ValueError("connector must be pncp, compras_gov, or all")
        async with self.session_factory() as session, session.begin():
            run = CrawlRun(
                connector=connector,
                status=CrawlRunStatus.PENDING,
                filters=request.as_json(),
            )
            session.add(run)
            await session.flush()
            run_id = run.id
        async with self.session_factory() as session:
            return await session.get_one(CrawlRun, run_id)

    async def run(self, request: PipelineRequest, *, run_id: UUID | None = None) -> PipelineSummary:
        """Execute a crawl under a cooperative database lease."""

        if run_id is None:
            run_id = (await self.create_run(request)).id
        owner = f"pipeline:{uuid4()}"
        lease_name = f"crawl:{request.connector.lower()}:{request.uf.upper()}"
        acquired = await self._acquire_lease(lease_name, owner)
        if not acquired:
            await self._fail_run(run_id, "another equivalent crawl currently holds the lease")
            return PipelineSummary(
                run_id=run_id,
                status=CrawlRunStatus.FAILED,
                diagnostics=["another equivalent crawl currently holds the lease"],
            )

        summary = PipelineSummary(run_id=run_id)
        connectors: list[ProcurementSourceConnector] = []
        try:
            await self._mark_running(run_id)
            connectors = self._selected_connectors(request.connector)
            for connector in connectors:
                source_started = time.perf_counter()
                found_before = summary.records_found
                created_before = summary.records_created
                updated_before = summary.records_updated
                diagnostics_before = len(summary.diagnostics)
                source_status = "completed"
                try:
                    await self._run_connector(connector, request, summary)
                except Exception as exc:
                    source_status = "failed"
                    summary.diagnostics.append(f"{connector.name}: {type(exc).__name__}: {exc}")
                source_diagnostics = summary.diagnostics[diagnostics_before:]
                if source_status != "failed" and source_diagnostics:
                    source_status = (
                        "temporary_error"
                        if any("temporary" in item.casefold() for item in source_diagnostics)
                        else "partial"
                    )
                summary.source_results[connector.name] = {
                    "status": source_status,
                    "records_found": summary.records_found - found_before,
                    "records_created": summary.records_created - created_before,
                    "records_updated": summary.records_updated - updated_before,
                    "duration_seconds": round(time.perf_counter() - source_started, 3),
                    "max_attempts_per_request": self.settings.http_max_retries + 1,
                    "diagnostics": source_diagnostics,
                }
                await self._update_progress(summary)
            if request.process_documents:
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
                await self._update_progress(summary)

                async def report_documents(processed_count: int, total: int) -> None:
                    documents_entry["documents_processed"] = processed_count
                    documents_entry["documents_total"] = total
                    await self._update_progress(summary)

                processed = await DocumentProcessingService(
                    self.settings,
                    session_factory=self.session_factory,
                ).process_pending(limit=batch_size, on_progress=report_documents)
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
                await self._update_progress(summary)
            if summary.diagnostics:
                summary.status = CrawlRunStatus.PARTIAL
        except Exception as exc:
            summary.status = CrawlRunStatus.FAILED
            summary.diagnostics.append(f"{type(exc).__name__}: {exc}")
        finally:
            for connector in connectors:
                await connector.aclose()
            await self._finish_run(summary)
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

    async def _run_connector(
        self,
        connector: ProcurementSourceConnector,
        request: PipelineRequest,
        summary: PipelineSummary,
    ) -> None:
        filters = request.filters()
        if filters.max_pages is None and self.settings.crawl_max_pages:
            filters.max_pages = self.settings.crawl_max_pages
        discovery = await connector.discover_procurements(filters)
        discovery_record_ids = await self._persist_source_records(summary.run_id, discovery)
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
        max_records = self.settings.crawl_max_records_per_source
        if max_records and len(records) > max_records:
            summary.diagnostics.append(
                f"{connector.name}: limitado a {max_records} registro(s) por fonte "
                f"({len(records)} encontrados)"
            )
            records = records[:max_records]
        summary.records_found += len(records)
        progress: dict[str, Any] = {
            "source": connector.name,
            "processed": 0,
            "total": len(records),
        }
        summary.source_results[connector.name] = {
            "status": "running",
            "records_found": len(records),
            "progress": progress,
        }
        await self._update_progress(summary)
        for position, raw in enumerate(records, start=1):
            detail = await connector.fetch_procurement(raw.external_id)
            items = await connector.fetch_items(raw.external_id)
            documents = await connector.fetch_documents(raw.external_id)
            results = await connector.fetch_results(raw.external_id)
            persisted: list[list[UUID]] = []
            for result in (detail, items, documents, results):
                persisted.append(await self._persist_source_records(summary.run_id, result))
                if result.availability is ConnectorAvailability.TEMPORARY_ERROR:
                    summary.diagnostics.append(
                        f"{connector.name}/{raw.external_id}: "
                        f"{result.diagnostic or 'temporary source error'}"
                    )
            enriched = detail.data if detail.is_available and detail.data is not None else raw
            procurement_record_ids = persisted[0] or discovery_record_ids
            created = await self._persist_procurement_bundle(
                summary.run_id,
                enriched,
                items.data if items.is_available else [],
                documents.data if documents.is_available else [],
                results.data if results.is_available else [],
                source_record_id=procurement_record_ids[0] if procurement_record_ids else None,
                item_source_record_id=persisted[1][0] if persisted[1] else None,
                document_source_record_id=persisted[2][0] if persisted[2] else None,
                result_source_record_id=persisted[3][0] if persisted[3] else None,
            )
            if created:
                summary.records_created += 1
            else:
                summary.records_updated += 1
            progress["processed"] = position
            await self._update_progress(summary)

    async def _persist_source_records(
        self,
        run_id: UUID,
        result: ConnectorResult[Any],
    ) -> list[UUID]:
        identifiers: list[UUID] = []
        for raw in result.raw_records:
            async with self.session_factory() as session, session.begin():
                entity = await self._source_record(
                    session,
                    run_id,
                    raw,
                    result.availability,
                    result.diagnostic,
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
    ) -> SourceRecord:
        fingerprint = stable_fingerprint(
            "source_record",
            run_id,
            raw.source,
            raw.endpoint,
            _json_value(raw.request_parameters),
            raw.response_hash,
        )
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
        item_source_record_id: UUID | None,
        document_source_record_id: UUID | None,
        result_source_record_id: UUID | None,
    ) -> bool:
        del run_id  # source-record links remain independently navigable and auditable
        async with self.session_factory() as session, session.begin():
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
            item_map = await self._upsert_items(session, procurement, items, item_source_record_id)
            await self._upsert_documents(session, procurement, documents, document_source_record_id)
            await self._upsert_results(
                session,
                procurement,
                results,
                item_map,
                result_source_record_id,
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
        source_record_id: UUID | None,
    ) -> dict[str, ProcurementItem]:
        item_map: dict[str, ProcurementItem] = {}
        for position, raw in enumerate(items, start=1):
            number = raw.item_number or raw.external_id or str(position)
            entity = await session.scalar(
                select(ProcurementItem).where(
                    ProcurementItem.procurement_id == procurement.id,
                    ProcurementItem.item_number == number,
                )
            )
            if entity is None:
                entity = ProcurementItem(
                    procurement_id=procurement.id,
                    source_record_id=source_record_id,
                    item_number=number,
                    fingerprint=stable_fingerprint("item", procurement.id, number),
                )
                session.add(entity)
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
        await session.flush()
        return item_map

    async def _upsert_documents(
        self,
        session: AsyncSession,
        procurement: Procurement,
        documents: list[RawDocument],
        source_record_id: UUID | None,
    ) -> None:
        for raw in documents:
            fingerprint = stable_fingerprint("document", procurement.id, raw.download_url)
            entity = await session.scalar(
                select(Document).where(Document.fingerprint == fingerprint)
            )
            if entity is None:
                session.add(
                    Document(
                        procurement_id=procurement.id,
                        source_record_id=source_record_id,
                        document_type=raw.document_type,
                        title=raw.title or raw.external_id,
                        original_url=raw.download_url,
                        mime_type=raw.mime_type,
                        extraction_status=ExtractionStatus.PENDING,
                        published_at=raw.published_at,
                        fingerprint=fingerprint,
                    )
                )
            else:
                entity.source_record_id = source_record_id
                entity.document_type = raw.document_type or entity.document_type
                entity.title = raw.title or entity.title
                entity.mime_type = raw.mime_type or entity.mime_type
                entity.published_at = raw.published_at or entity.published_at

    async def _upsert_results(
        self,
        session: AsyncSession,
        procurement: Procurement,
        results: list[RawResult],
        item_map: dict[str, ProcurementItem],
        source_record_id: UUID | None,
    ) -> None:
        for raw in results:
            # Cancelled outcomes and natural persons are retained in SourceRecord,
            # but never represented as an awarded company.
            if raw.is_cancelled or (
                raw.person_type and raw.person_type.upper() in {"F", "PF", "CPF"}
            ):
                continue
            cnpj = normalize_cnpj(raw.supplier_cnpj)
            if not cnpj or not is_valid_cnpj(cnpj):
                continue
            company = await session.scalar(select(Company).where(Company.cnpj == cnpj))
            if company is None:
                company = Company(
                    cnpj=cnpj,
                    legal_name=raw.supplier_name,
                    normalized_name=normalize_company_name(raw.supplier_name) or cnpj,
                    fingerprint=stable_fingerprint("company", cnpj),
                )
                session.add(company)
                await session.flush()
            elif raw.supplier_name and not company.legal_name:
                company.legal_name = raw.supplier_name
                company.normalized_name = normalize_company_name(raw.supplier_name) or cnpj

            item = item_map.get(raw.item_external_id or "") or item_map.get(raw.item_number or "")
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
                        source=raw.source,
                        source_record_id=source_record_id,
                        confidence=Decimal("1"),
                        fingerprint=fingerprint,
                    )
                )
            else:
                participant.source_record_id = source_record_id
                participant.final_value = raw.total_value or participant.final_value
                participant.rank = raw.rank or participant.rank
                participant.status = raw.result_status or participant.status

    async def _acquire_lease(self, name: str, owner: str) -> bool:
        now = datetime.now(UTC)
        async with self.session_factory() as session, session.begin():
            repository = JobLeaseRepository(session)
            return await repository.acquire(
                name=name,
                owner_id=owner,
                now=now,
                expires_at=now + timedelta(hours=2),
            )

    async def _release_lease(self, name: str, owner: str) -> None:
        async with self.session_factory() as session, session.begin():
            repository = JobLeaseRepository(session)
            await repository.release(name=name, owner_id=owner)

    async def _mark_running(self, run_id: UUID) -> None:
        async with self.session_factory() as session, session.begin():
            run = await session.get(CrawlRun, run_id)
            if run is None:
                raise LookupError(f"crawl run {run_id} not found")
            run.status = CrawlRunStatus.RUNNING
            run.started_at = datetime.now(UTC)

    async def _fail_run(self, run_id: UUID, diagnostic: str) -> None:
        async with self.session_factory() as session, session.begin():
            run = await session.get(CrawlRun, run_id)
            if run is not None:
                run.status = CrawlRunStatus.FAILED
                run.started_at = run.started_at or datetime.now(UTC)
                run.finished_at = datetime.now(UTC)
                run.diagnostic = diagnostic
                run.errors = [{"message": diagnostic}]

    async def _update_progress(self, summary: PipelineSummary) -> None:
        """Persist partial counters so a long crawl reports honest progress."""

        async with self.session_factory() as session, session.begin():
            run = await session.get(CrawlRun, summary.run_id)
            if run is None:
                return
            run.records_found = summary.records_found
            run.records_created = summary.records_created
            run.records_updated = summary.records_updated
            run.cursor = {"sources": summary.source_results}
            run.diagnostic = "\n".join(summary.diagnostics) or None
            run.errors = [{"message": item} for item in summary.diagnostics]

    async def _finish_run(self, summary: PipelineSummary) -> None:
        async with self.session_factory() as session, session.begin():
            run = await session.get(CrawlRun, summary.run_id)
            if run is None:
                return
            run.status = summary.status
            run.finished_at = datetime.now(UTC)
            run.records_found = summary.records_found
            run.records_created = summary.records_created
            run.records_updated = summary.records_updated
            run.cursor = {"sources": summary.source_results}
            run.diagnostic = "\n".join(summary.diagnostics) or None
            run.errors = [{"message": item} for item in summary.diagnostics]
