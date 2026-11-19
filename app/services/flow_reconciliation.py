"""Stored-only flow integrity repair, bounded, fenced and restartable."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select

from app.database import write_session
from app.models import (
    Company,
    Contract,
    Deadline,
    DeadlineStatus,
    Document,
    Evidence,
    FieldChange,
    FieldObservation,
    FieldState,
    Participant,
    Payment,
    PriceRegistry,
    Procurement,
    ProcurementEvent,
    ProcurementEventType,
    ProcurementItem,
    ReconciliationRun,
    SourceRecord,
)
from app.services.deadlines.response import (
    OFFICIAL_RESPONSE_DEADLINE_TYPES,
    RESPONSE_INTERRUPTION_EVENT_TYPES,
    RESPONSE_OPENING_EVENT_TYPES,
)
from app.services.event_detection.authority import assertion_diagnostic, nonaward_supplier
from app.services.event_detection.detector import event_act_excerpt
from app.services.event_detection.types import CompanyReference, DocumentEvidence
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.ingestion.field_updates import json_value
from app.services.maintenance import fence_maintenance, run_blocking_maintenance
from app.services.participation_integrity import (
    _verify_event_dependents,
    invalidate_participation_derivatives,
    verify_fact,
)
from app.services.participation_reconciliation import IntegrityReport, retained_pages

VERSION = "flow-integrity-3"

_AWARD_EVENTS = frozenset(
    {
        ProcurementEventType.BEST_OFFER_DECLARED,
        ProcurementEventType.WINNER_DECLARED,
        ProcurementEventType.ADJUDICATED,
        ProcurementEventType.HOMOLOGATED,
    }
)


async def _proof(db, kind, entity, field):
    state = await db.scalar(
        select(FieldState).where(
            FieldState.entity_type == kind,
            FieldState.entity_id == entity.id,
            FieldState.field_name == field,
        )
    )
    if not state or state.value != json_value(getattr(entity, field)) or not state.source_record_id:
        return None
    return state if await db.get(SourceRecord, state.source_record_id) else None


async def _link(db, entity, kind, field, value, proof, apply, report):
    previous = getattr(entity, field)
    if previous == value:
        return
    manual = await db.scalar(
        select(FieldObservation.id)
        .where(
            FieldObservation.entity_type == kind,
            FieldObservation.entity_id == entity.id,
            FieldObservation.field_name == field,
            FieldObservation.source.in_(["manual", "manual_operator"]),
        )
        .limit(1)
    )
    if manual:
        report.add("manual_links_requiring_review")
        return
    report.add("links_requiring_repair")
    if not apply:
        return
    state = await db.scalar(
        select(FieldState).where(
            FieldState.entity_type == kind,
            FieldState.entity_id == entity.id,
            FieldState.field_name == field,
        )
    )
    db.add(
        FieldChange(
            entity_type=kind,
            entity_id=entity.id,
            parent_type=kind,
            parent_id=entity.id,
            field_name=field,
            previous_value=json_value(previous),
            received_value=json_value(value),
            decision="accepted_identity_repair",
            applied=True,
            source=VERSION,
            source_record_id=proof.source_record_id,
            observed_at=proof.observed_at,
            source_updated_at=proof.source_updated_at,
            timestamp_origin=proof.timestamp_origin,
        )
    )
    if state is None:
        state = FieldState(entity_type=kind, entity_id=entity.id, field_name=field)
        db.add(state)
    state.value, state.source, state.source_record_id = (
        json_value(value),
        VERSION,
        proof.source_record_id,
    )
    state.observed_at, state.source_updated_at, state.timestamp_origin = (
        proof.observed_at,
        proof.source_updated_at,
        proof.timestamp_origin,
    )
    state.uncertain = False
    setattr(entity, field, value)


async def inspect_links(db, entity, kind, *, apply, report, settings):
    pairs: list[tuple[str, str, Any]] = []
    if kind == "contract":
        pairs = [
            ("procurement_control_number", "procurement_id", Procurement),
            ("price_registry_control_number", "price_registry_id", PriceRegistry),
        ]
    elif kind == "price_registry":
        pairs = [("linked_pncp_control_number", "procurement_id", Procurement)]
    elif kind == "payment":
        pairs = [("procurement_control_number", "procurement_id", Procurement)]
    for identity, target, model in pairs:
        value = getattr(entity, identity)
        if not value:
            continue
        proof = await _proof(db, kind, entity, identity)
        if proof is None:
            report.add("links_without_retained_proof")
            continue
        linked = await db.scalar(select(model).where(model.pncp_control_number == value))
        if (
            linked
            and entity.agency_cnpj
            and normalize_cnpj(linked.agency_cnpj) != normalize_cnpj(entity.agency_cnpj)
        ):
            report.add("identity_conflicts")
            continue
        await _link(db, entity, kind, target, linked.id if linked else None, proof, apply, report)
    if kind not in {"contract", "payment"}:
        return
    cnpj = normalize_cnpj(entity.supplier_cnpj)
    proof = await _proof(db, kind, entity, "supplier_cnpj")
    if proof and is_valid_cnpj(cnpj) and cnpj != normalize_cnpj(entity.agency_cnpj):
        company = await db.scalar(select(Company).where(Company.cnpj == cnpj))
        if company:
            await _link(db, entity, kind, "company_id", company.id, proof, apply, report)
        else:
            report.add("supplier_without_company")
    if kind == "payment" and entity.contract_control_number:
        proof = await _proof(db, kind, entity, "contract_control_number")
        # No legacy match is repaired from an unproven date or supplier.
        if (
            not proof
            or not await _proof(db, kind, entity, "supplier_cnpj")
            or not await _proof(db, kind, entity, "payment_date")
        ):
            report.add("links_without_retained_proof")
            return
        from app.connectors.base import RawPayment
        from app.services.ingestion.payments import PaymentIngestionService

        raw = RawPayment(
            source=entity.source,
            external_id=entity.external_id,
            contract_control_number=entity.contract_control_number,
            agency_cnpj=entity.agency_cnpj,
            supplier_cnpj=entity.supplier_cnpj,
            payment_date=entity.payment_date,
        )
        service = PaymentIngestionService(settings)
        try:
            contract, _, _, _ = await service._match_contract(db, raw, cnpj)
        finally:
            await service.aclose()
        await _link(
            db, entity, kind, "contract_id", contract.id if contract else None, proof, apply, report
        )


async def _inspect_response_deadline(
    db, event, company, document, proof, act, *, settings, apply, report, material
):
    from app.services.deadlines.response import verified_event_response_deadline
    from app.services.ingestion.pipeline import stable_fingerprint
    from app.services.opportunity_workspace import legacy_official_response_deadline_ids

    deadline = await db.scalar(
        select(Deadline).where(
            Deadline.fingerprint == stable_fingerprint("deadline", event.id, "event_response"),
            Deadline.deadline_type.in_(("event_response", *OFFICIAL_RESPONSE_DEADLINE_TYPES)),
        )
    )
    if not deadline:
        return
    typed = deadline.deadline_type in OFFICIAL_RESPONSE_DEADLINE_TYPES
    if typed and not await legacy_official_response_deadline_ids(db, [deadline.id]):
        return
    verified = await verified_event_response_deadline(
        db, event, company, document, act, timezone=settings.timezone
    )
    if not verified and not typed:
        return
    report.add(
        "legacy_response_deadlines_requiring_validation"
        if verified
        else "legacy_response_deadlines_requiring_review"
    )
    if not material or not any(proof.text_excerpt in page.text for page in material):
        report.add("events_without_retained_material")
        return
    if not apply:
        return

    from app.services.ingestion.processor import DocumentProcessingService, event_document_is_active

    procurement = await db.get(Procurement, event.procurement_id)
    if not procurement or not await event_document_is_active(db, event):
        return
    fingerprint = stable_fingerprint(VERSION, "legacy_response_deadline", deadline.id)
    if not await db.scalar(
        select(FieldObservation.id).where(FieldObservation.fingerprint == fingerprint)
    ):
        db.add(
            FieldObservation(
                entity_type="deadline",
                entity_id=deadline.id,
                field_name="legacy_response_deadline",
                value={
                    field: json_value(getattr(deadline, field))
                    for field in (
                        "deadline_type",
                        "trigger_at",
                        "explicit_deadline_at",
                        "estimated_deadline_at",
                        "calculation_method",
                        "status",
                        "calculation_explanation",
                        "requires_manual_review",
                        "remaining_seconds",
                    )
                },
                source=VERSION,
                evidence_id=proof.id,
                fingerprint=fingerprint,
            )
        )
    if not verified:
        deadline.status = DeadlineStatus.REQUIRES_REVIEW
        deadline.requires_manual_review = True
        deadline.remaining_seconds = None
        return
    service = DocumentProcessingService(settings)
    try:
        await service._deadline_and_lead(db, procurement, event, revalidate_legacy_response=True)
    finally:
        await service.aclose()


async def inspect_event(db, event, *, settings, apply, report, material):
    proof = await db.get(Evidence, event.evidence_id) if event.evidence_id else None
    doc = await db.get(Document, proof.document_id) if proof and proof.document_id else None
    if not doc or not proof:
        report.add("events_without_documentary_proof")
        return
    reason = assertion_diagnostic(proof.text_excerpt, doc.title)
    scoped_review = False
    if (
        reason is None
        and event.company_id
        and event.event_type in _AWARD_EVENTS | RESPONSE_OPENING_EVENT_TYPES
    ):
        company = await db.get(Company, event.company_id)
        if company:
            act = event_act_excerpt(
                event.event_type.value,
                proof.text_excerpt,
                company=CompanyReference(
                    str(company.id), company.legal_name or company.normalized_name, company.cnpj
                ),
                title=doc.title,
                document_type=doc.document_type,
                include_deadline=True,
                locator=proof.locator,
            )
            if event.event_type in _AWARD_EVENTS and not act:
                reason, scoped_review = "party_not_proven_in_act", True
            elif event.event_type in RESPONSE_OPENING_EVENT_TYPES:
                await _inspect_response_deadline(
                    db,
                    event,
                    company,
                    doc,
                    proof,
                    act,
                    settings=settings,
                    apply=apply,
                    report=report,
                    material=material,
                )
    if reason is None and event.source == "llm" and event.company_id:
        from app.services.llm.analyzer import LLMEventAnalysis, attribution_status

        company = await db.get(Company, event.company_id)
        if company:
            analysis = LLMEventAnalysis(
                event_type=event.event_type.value,
                company_cnpj=company.cnpj,
                company_name=company.legal_name,
                evidence_quotes=[{"text": proof.text_excerpt}],
                confidence=0.6,
                requires_manual_review=True,
            )
            if attribution_status(analysis) in {"review", "contradictory"}:
                reason = "party_not_proven"
    if not reason:
        if (
            event.event_type in RESPONSE_INTERRUPTION_EVENT_TYPES
            and material
            and any(proof.text_excerpt in page.text for page in material)
        ):
            from app.services.ingestion.processor import (
                DocumentProcessingService,
                event_document_is_active,
            )

            company = await db.get(Company, event.company_id) if event.company_id else None
            if await event_document_is_active(
                db, event
            ) and await DocumentProcessingService.record_response_act_scope(
                db,
                event,
                doc,
                proof,
                company,
                apply=apply,
            ):
                report.add("legacy_response_act_scopes_requiring_validation")
        return
    report.add("events_requiring_invalidation")
    if len(report.diagnostics) < 100:
        report.diagnostics.append(f"evento {event.id}: {reason}")
    if not material or not any(proof.text_excerpt in page.text for page in material):
        report.add("events_without_retained_material")
        return
    if apply:
        manual = await db.scalar(
            select(FieldObservation.id)
            .where(
                FieldObservation.entity_type == "procurement_event",
                FieldObservation.entity_id == event.id,
                FieldObservation.source.in_(["manual", "manual_operator"]),
            )
            .limit(1)
        )
        await verify_fact(
            db,
            "procurement_event",
            event.id,
            event.procurement_id,
            "review" if manual or scoped_review else "invalidated",
            reason,
            evidence_id=proof.id,
        )
        event.requires_manual_review = True
        await _verify_event_dependents(
            db,
            event,
            "review" if manual or scoped_review else "invalidated",
            reason,
            preserve_noncurrent=True,
        )
        from app.services.procurement_phase import refresh_procurement_phase

        procurement = await db.get(Procurement, event.procurement_id)
        if procurement:
            await refresh_procurement_phase(db, procurement)


async def reconcile_flow(
    sessions, settings, *, apply=False, backup=None, maintenance_owner=None, resume_id=None
) -> dict[str, Any]:
    if apply:
        if backup is None or maintenance_owner is None:
            raise ValueError("reparo exige backup verificado e manutenção exclusiva")
        await run_blocking_maintenance(
            lambda: backup.verify(settings.database_url, maintenance_owner)
        )
    report = IntegrityReport()
    stage, checkpoint, run_id = 0, None, None
    stages: list[tuple[Any, str]] = [
        (ProcurementEvent, "event"),
        (Participant, "participant"),
        (Contract, "contract"),
        (PriceRegistry, "price_registry"),
        (Payment, "payment"),
        (Procurement, "procurement_search"),
        (ProcurementItem, "item_search"),
    ]

    async def fence(db):
        db.info["maintenance_owner"] = maintenance_owner
        await fence_maintenance(db, maintenance_owner)

    if apply:
        async with write_session(sessions) as db:
            await fence(db)
            run = await db.get(ReconciliationRun, resume_id) if resume_id else None
            if resume_id and (run is None or run.version != VERSION):
                raise ValueError("reconciliação ausente ou de outra versão")
            if run:
                if run.status == "completed":
                    return {
                        "run_id": str(run.id),
                        "already_completed": True,
                        "applied": True,
                        "result": run.totals,
                    }
                stage, checkpoint = run.totals.get("_stage", 0), run.checkpoint
                report.totals = {
                    k: v for k, v in run.totals.items() if isinstance(v, int) and k != "_stage"
                }
                report.diagnostics = run.totals.get("diagnostics", [])
                run.backup = {"receipts": [*run.backup.get("receipts", []), backup.as_dict()]}
            else:
                run = ReconciliationRun(version=VERSION, backup={"receipts": [backup.as_dict()]})
                db.add(run)
                await db.flush()
            run_id = run.id
    for index in range(stage, len(stages)):
        model, kind = stages[index]
        while True:
            async with sessions() as db:
                query = select(model.id).order_by(model.id).limit(20)
                if kind == "procurement_search":
                    query = query.where(Procurement.object_complement.is_(None))
                elif kind == "item_search":
                    query = query.where(ProcurementItem.summary_description.is_(None))
                if checkpoint:
                    query = query.where(model.id > UUID(checkpoint))
                ids = list((await db.scalars(query)).all())
            if not ids:
                break
            for identifier in ids:
                material: list[DocumentEvidence] = []
                if kind == "event":
                    async with sessions() as reader:
                        event = await reader.get(ProcurementEvent, identifier)
                        proof = (
                            await reader.get(Evidence, event.evidence_id)
                            if event.evidence_id
                            else None
                        )
                        doc = (
                            await reader.get(Document, proof.document_id)
                            if proof and proof.document_id
                            else None
                        )
                        if (
                            doc
                            and proof
                            and (
                                event.source == "llm"
                                or (
                                    event.company_id
                                    and event.event_type
                                    in _AWARD_EVENTS | RESPONSE_OPENING_EVENT_TYPES
                                )
                                or event.event_type in RESPONSE_INTERRUPTION_EVENT_TYPES
                                or assertion_diagnostic(proof.text_excerpt, doc.title)
                            )
                        ):
                            material, _ = await retained_pages(reader, doc, settings, {})
                async with write_session(sessions) if apply else sessions.begin() as db:
                    if apply:
                        await fence(db)
                    entity = await db.get(model, identifier)
                    if entity is None:
                        continue
                    if kind == "event":
                        await inspect_event(
                            db,
                            entity,
                            settings=settings,
                            apply=apply,
                            report=report,
                            material=material,
                        )
                    elif kind in {"procurement_search", "item_search"}:
                        from app.services.procurement_search_reconciliation import (
                            inspect_legacy_search_text,
                        )

                        await inspect_legacy_search_text(db, entity, apply=apply, report=report)
                    elif kind == "participant":
                        source = (
                            await db.get(SourceRecord, entity.source_record_id)
                            if entity.source_record_id
                            else None
                        )
                        from app.services.data_reconciliation import _rows_from_payload

                        rows = _rows_from_payload(source.raw_payload) if source else []
                        company = await db.get(Company, entity.company_id)
                        bad = company and any(
                            nonaward_supplier(r.get("nomeRazaoSocialFornecedor"))
                            and normalize_cnpj(r.get("niFornecedor")) == company.cnpj
                            for r in rows
                        )
                        if bad:
                            assert source is not None
                            report.add("nonaward_participants")
                            if apply:
                                await verify_fact(
                                    db,
                                    "participant",
                                    entity.id,
                                    entity.procurement_id,
                                    "invalidated",
                                    "PNCP publicou marcador sem homologação",
                                    source_record_id=source.id,
                                )
                                await invalidate_participation_derivatives(db, entity)
                    else:
                        await inspect_links(
                            db, entity, kind, apply=apply, report=report, settings=settings
                        )
                    report.add("inspected_" + kind)
                    checkpoint = str(identifier)
                    if apply:
                        await fence(db)
                        run = await db.get(ReconciliationRun, run_id)
                        assert run is not None
                        run.checkpoint, run.totals = (
                            checkpoint,
                            {**report.as_dict(), "_stage": index},
                        )
        checkpoint = None
    if apply:
        async with write_session(sessions) as db:
            await fence(db)
            run = await db.get(ReconciliationRun, run_id)
            assert run is not None
            run.status, run.finished_at = "completed", datetime.now(UTC)
            run.totals = report.as_dict()
    return {"run_id": str(run_id) if run_id else None, "applied": apply, "result": report.as_dict()}
