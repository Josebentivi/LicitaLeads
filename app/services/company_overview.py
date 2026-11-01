"""Read-only, bounded company dossier shared by the API and HTML fragments.

Supplier identity is exact. Looking at a dossier never repairs legacy links.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import and_, exists, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, load_only

from app.api.serialization import model_dict
from app.models import (
    Company,
    CompanyContact,
    Contract,
    ContractItem,
    ContractTerm,
    Deadline,
    Document,
    Evidence,
    FieldObservation,
    Lead,
    LeadReview,
    OutreachDraft,
    Participant,
    Payment,
    PriceRegistry,
    PriceRegistryItem,
    Procurement,
    ProcurementEvent,
    SourceRecord,
)
from app.repositories.current_evidence import evidence_current as evidence_current
from app.repositories.current_evidence import inactive_document_ids as inactive_document_ids
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.participation_integrity import (
    event_verified,
    lead_current,
    participant_current,
)

Section = Literal[
    "leads",
    "procurements",
    "price_registries",
    "contracts",
    "payments",
    "events",
    "deadlines",
    "documents",
    "contacts",
]
SECTION_LABELS = {
    "leads": "Leads",
    "procurements": "Licitações e participações",
    "price_registries": "Atas",
    "contracts": "Contratos",
    "payments": "Pagamentos",
    "events": "Eventos",
    "deadlines": "Prazos",
    "documents": "Documentos e evidências",
    "contacts": "Contatos corporativos",
}
MODELS: dict[str, Any] = {
    "leads": Lead,
    "procurements": Procurement,
    "price_registries": PriceRegistry,
    "contracts": Contract,
    "payments": Payment,
    "events": ProcurementEvent,
    "deadlines": Deadline,
    "documents": Document,
    "contacts": CompanyContact,
}


def event_current():
    """An attribution inferred from a withdrawn award is historical context only."""
    inferred = exists(
        select(FieldObservation.id).where(
            FieldObservation.entity_type == "procurement_event",
            FieldObservation.entity_id == ProcurementEvent.id,
            FieldObservation.field_name == "company_id",
            FieldObservation.source == "inferred_single_winner",
        )
    )
    winner = exists(
        select(Participant.id).where(
            Participant.company_id == ProcurementEvent.company_id,
            Participant.procurement_id == ProcurementEvent.procurement_id,
            or_(
                Participant.status_code.in_(["winner", "awarded"]),
                and_(
                    Participant.status_code.is_(None),
                    Participant.participation_role.in_(["winner", "awarded"]),
                ),
            ),
            participant_current(),
        )
    )
    return and_(
        event_verified(), evidence_current(ProcurementEvent.evidence_id), or_(~inferred, winner)
    )


def supplier_identity(model, company: Company):
    """A contradictory supplier CNPJ or company FK cannot grant membership."""
    return or_(
        and_(
            model.company_id == company.id,
            or_(
                model.supplier_cnpj.is_(None),
                func.trim(model.supplier_cnpj) == "",
                model.supplier_cnpj == company.cnpj,
            ),
        ),
        and_(model.company_id.is_(None), model.supplier_cnpj == company.cnpj),
    )


async def find_company(db: AsyncSession, cnpj: str) -> Company:
    normalized = normalize_cnpj(cnpj)
    if not normalized or not is_valid_cnpj(normalized):
        raise HTTPException(404, "Empresa não encontrada")
    company = await db.scalar(select(Company).where(Company.cnpj == normalized))
    if company is None:
        raise HTTPException(404, "Empresa não encontrada")
    return company


def registry_procurement_link():
    return official_procurement_link(
        PriceRegistry.procurement_id, PriceRegistry.linked_pncp_control_number
    )


def contract_procurement_link():
    return official_procurement_link(Contract.procurement_id, Contract.procurement_control_number)


def official_procurement_link(foreign_key, control_number):
    return or_(
        and_(
            foreign_key == Procurement.id,
            or_(
                control_number.is_(None),
                func.trim(control_number) == "",
                Procurement.pncp_control_number.is_(None),
                func.trim(Procurement.pncp_control_number) == "",
                control_number == Procurement.pncp_control_number,
            ),
        ),
        and_(
            foreign_key.is_(None),
            control_number.is_not(None),
            func.trim(control_number) != "",
            func.trim(Procurement.pncp_control_number) != "",
            control_number == Procurement.pncp_control_number,
        ),
    )


def company_procurement_condition(company: Company, *, historical: bool = False):
    participation = [
        Participant.company_id == company.id,
        Participant.procurement_id == Procurement.id,
    ]
    if not historical:
        participation.append(participant_current())
    return or_(
        exists(select(Participant.id).where(*participation)),
        exists(
            select(PriceRegistryItem.id)
            .join(PriceRegistry)
            .where(supplier_identity(PriceRegistryItem, company), registry_procurement_link())
        ),
        exists(
            select(Contract.id).where(
                supplier_identity(Contract, company), contract_procurement_link()
            )
        ),
        exists(
            select(Payment.id).where(
                supplier_identity(Payment, company),
                official_procurement_link(
                    Payment.procurement_id, Payment.procurement_control_number
                ),
                or_(Payment.is_current.is_(True), literal(historical)),
            )
        ),
        exists(
            select(ProcurementEvent.id).where(
                ProcurementEvent.company_id == company.id,
                ProcurementEvent.procurement_id == Procurement.id,
                or_(event_current(), literal(historical)),
            )
        ),
    )


class CompanyOverviewService:
    def __init__(self, db: AsyncSession, company: Company):
        self.db, self.company = db, company

    def statement(
        self,
        section: str,
        *,
        historical: bool = False,
        status: str | None = None,
        active_only: bool = False,
    ):
        if section not in MODELS:
            raise HTTPException(422, "Seção inválida")
        company = self.company
        model = MODELS[section]
        if section == "leads":
            condition = Lead.company_id == company.id
            if not historical:
                condition = and_(condition, lead_current())
            if status:
                from app.models import LeadStatus

                if status not in {item.value for item in LeadStatus}:
                    raise HTTPException(422, "Situação do lead inválida")
                condition = and_(condition, Lead.lead_status == status)
            return (
                select(Lead)
                .where(condition)
                .order_by(Lead.score.desc(), Lead.created_at.desc(), Lead.id)
            )
        if section == "procurements":
            condition = company_procurement_condition(company, historical=historical)
            if status:
                from app.models import ParticipantStatus

                if status not in {item.value for item in ParticipantStatus}:
                    raise HTTPException(422, "Situação do participante inválida")
                condition = and_(
                    condition,
                    exists(
                        select(Participant.id).where(
                            Participant.company_id == company.id,
                            Participant.procurement_id == Procurement.id,
                            Participant.status_code == status,
                            participant_current(),
                        )
                    ),
                )
            if active_only:
                from app.repositories.procurements import _status_category_condition

                condition = and_(
                    condition, *_status_category_condition("aberta", now=datetime.now(UTC))
                )
            return (
                select(Procurement)
                .where(condition)
                .order_by(Procurement.publication_at.desc().nullslast(), Procurement.id)
            )
        if section == "price_registries":
            condition = exists(
                select(PriceRegistryItem.id).where(
                    PriceRegistryItem.price_registry_id == PriceRegistry.id,
                    supplier_identity(PriceRegistryItem, company),
                )
            )
        elif section in {"contracts", "payments"}:
            condition = supplier_identity(model, company)
            if section == "payments" and not historical:
                condition = and_(condition, Payment.is_current.is_(True))
        elif section == "events":
            condition = ProcurementEvent.company_id == company.id
            if not historical:
                condition = and_(condition, event_current())
        elif section == "deadlines":
            condition = and_(
                or_(
                    Deadline.company_id == company.id,
                    and_(
                        Deadline.company_id.is_(None),
                        exists(
                            select(ProcurementEvent.id).where(
                                ProcurementEvent.id == Deadline.event_id,
                                ProcurementEvent.company_id == company.id,
                            )
                        ),
                    ),
                ),
                or_(
                    literal(historical),
                    Deadline.event_id.is_(None),
                    exists(
                        select(ProcurementEvent.id).where(
                            ProcurementEvent.id == Deadline.event_id,
                            event_current(),
                        )
                    ),
                ),
                or_(
                    Deadline.event_id.is_(None),
                    exists(
                        select(ProcurementEvent.id).where(
                            ProcurementEvent.id == Deadline.event_id,
                            ProcurementEvent.company_id == company.id,
                            ProcurementEvent.procurement_id == Deadline.procurement_id,
                        )
                    ),
                ),
            )
        elif section == "documents":
            condition = exists(
                select(Procurement.id).where(
                    Procurement.id == Document.procurement_id,
                    company_procurement_condition(company, historical=historical),
                )
            )
            if not historical:
                from app.services.documents.identity import noncanonical_document_ids

                tree = inactive_document_ids()
                condition = and_(
                    condition,
                    ~Document.id.in_(select(tree.c.id)),
                    Document.id.not_in(noncanonical_document_ids()),
                )
        else:
            condition = and_(
                CompanyContact.company_id == company.id, CompanyContact.is_corporate.is_(True)
            )
        ordering = {
            "price_registries": PriceRegistry.published_at,
            "contracts": Contract.published_at,
            "payments": Payment.payment_date,
            "events": ProcurementEvent.occurred_at,
            "deadlines": Deadline.explicit_deadline_at,
            "documents": Document.created_at,
            "contacts": CompanyContact.created_at,
        }
        return (
            select(model).where(condition).order_by(ordering[section].desc().nullslast(), model.id)
        )

    async def overview(self) -> dict[str, Any]:
        # Scalar subqueries keep the number of round trips independent of the dossier size.
        counts = {}
        for section in MODELS:
            query = self.statement(section).order_by(None).subquery()
            counts[section] = select(func.count()).select_from(query).scalar_subquery()
        counts["pending_review"] = (
            select(func.count())
            .select_from(Lead)
            .where(
                Lead.company_id == self.company.id,
                Lead.lead_status.in_(["pending_review", "needs_changes"]),
            )
            .scalar_subquery()
        )
        row = (await self.db.execute(select(*(v.label(k) for k, v in counts.items())))).one()
        priority = await self.db.scalars(
            self.statement("leads")
            .where(
                ~Lead.lead_status.in_(["rejected", "archived"]),
                exists(
                    select(ProcurementEvent.id).where(
                        ProcurementEvent.id == Lead.triggering_event_id,
                        ProcurementEvent.company_id == Lead.company_id,
                        ProcurementEvent.procurement_id == Lead.procurement_id,
                        event_current(),
                    )
                ),
            )
            .limit(3)
        )
        upcoming = await self.db.scalars(
            self.statement("deadlines")
            .where(
                func.coalesce(Deadline.explicit_deadline_at, Deadline.estimated_deadline_at)
                >= datetime.now(UTC)
            )
            .order_by(None)
            .order_by(
                func.coalesce(Deadline.explicit_deadline_at, Deadline.estimated_deadline_at),
                Deadline.id,
            )
            .limit(3)
        )
        contacts = await self.db.scalars(self.statement("contacts").limit(3))
        from app.repositories.procurements import CompanyRepository

        repository = CompanyRepository(self.db)
        return {
            "company": model_dict(self.company),
            "counts": dict(row._mapping),
            "participation_stats": await repository.stats_for_company(self.company.id),
            "failure_breakdown": await repository.failure_breakdown(self.company.id),
            "priority_leads": [self.row("leads", x) for x in priority],
            "upcoming_deadlines": [self.row("deadlines", x) for x in upcoming],
            "contacts": [self.row("contacts", x) for x in contacts],
            "notice": "Acervo coletado; ausência de registros não comprova ausência de atividade.",
        }

    @staticmethod
    def row(section: str, entity: Any) -> dict[str, Any]:
        # Do not expose filesystem paths, complete extraction text or raw source payloads.
        excluded = {"local_path", "extracted_text", "fingerprint"}
        result = model_dict(
            entity, include={c.key for c in entity.__mapper__.column_attrs} - excluded
        )
        title_fields = {
            "leads": ("reason_summary",),
            "procurements": ("purchase_number", "title", "external_id"),
            "price_registries": ("registry_number", "external_id"),
            "contracts": ("contract_number", "external_id"),
            "payments": ("document_number", "external_id"),
            "events": ("event_type",),
            "deadlines": ("deadline_type",),
            "documents": ("title", "document_type"),
            "contacts": ("contact_value",),
        }
        result["title"] = next(
            (str(result[k]) for k in title_fields[section] if result.get(k)), "Não informado"
        )
        result["section"] = section
        return result

    async def page(
        self,
        section: str,
        *,
        page: int = 1,
        page_size: int = 20,
        historical: bool = False,
        status: str | None = None,
        active_only: bool = False,
    ) -> dict[str, Any]:
        statement = self.statement(
            section, historical=historical, status=status, active_only=active_only
        )
        if section == "documents":
            statement = statement.options(
                defer(Document.extracted_text), defer(Document.local_path)
            )
        total = int(
            await self.db.scalar(
                select(func.count()).select_from(statement.order_by(None).subquery())
            )
            or 0
        )
        rows = await self.db.scalars(statement.offset((page - 1) * page_size).limit(page_size))
        return {
            "items": [self.row(section, x) for x in rows],
            "page": page,
            "page_size": page_size,
            "total": total,
            "section": section,
            "label": SECTION_LABELS[section],
            "historical": historical,
            "status": status,
            "active_only": active_only,
        }

    async def detail(self, section: str, identifier: UUID, *, page: int = 1) -> dict[str, Any]:
        statement = self.statement(section, historical=True).where(MODELS[section].id == identifier)
        if section == "documents":
            statement = statement.options(
                defer(Document.extracted_text), defer(Document.local_path)
            )
        entity = await self.db.scalar(statement)
        if entity is None:
            raise HTTPException(404, "Registro não encontrado para esta empresa")
        result = self.row(section, entity)
        children: dict[str, Any] = {}
        company = self.company
        if section == "leads":
            from app.models import LeadScoreRevision

            children = {
                "Evento": select(ProcurementEvent).where(
                    ProcurementEvent.id == entity.triggering_event_id,
                    ProcurementEvent.company_id == company.id,
                    ProcurementEvent.procurement_id == entity.procurement_id,
                ),
                "Prazo": self.statement("deadlines", historical=True).where(
                    Deadline.id == entity.deadline_id
                ),
                "Revisões humanas": select(LeadReview)
                .where(LeadReview.lead_id == entity.id)
                .order_by(LeadReview.reviewed_at.desc()),
                "Rascunhos": select(OutreachDraft)
                .where(OutreachDraft.lead_id == entity.id)
                .order_by(OutreachDraft.created_at.desc()),
                "Histórico da pontuação": select(LeadScoreRevision)
                .where(LeadScoreRevision.lead_id == entity.id)
                .order_by(LeadScoreRevision.calculated_at.desc()),
                "Evidências": select(Evidence)
                .join(ProcurementEvent, ProcurementEvent.evidence_id == Evidence.id)
                .where(
                    ProcurementEvent.id == entity.triggering_event_id,
                    ProcurementEvent.company_id == company.id,
                    ProcurementEvent.procurement_id == entity.procurement_id,
                ),
            }
        elif section == "procurements":
            from app.models import ProcurementItem

            children = {
                "Participações da empresa": select(Participant).where(
                    Participant.procurement_id == entity.id, Participant.company_id == company.id
                ),
                "Itens da contratação (contexto geral)": select(ProcurementItem).where(
                    ProcurementItem.procurement_id == entity.id
                ),
                "Documentos da contratação (contexto geral)": select(Document).where(
                    Document.procurement_id == entity.id
                ),
            }
        elif section == "price_registries":
            from app.models import PriceRegistryAdhesion, PriceRegistryCommitment, PriceRegistryUnit

            children = {
                "Itens da empresa": select(PriceRegistryItem).where(
                    PriceRegistryItem.price_registry_id == entity.id,
                    supplier_identity(PriceRegistryItem, company),
                ),
                "Saldos/unidades (contexto geral da ata)": select(PriceRegistryUnit).where(
                    PriceRegistryUnit.price_registry_id == entity.id
                ),
                "Empenhos (contexto geral da ata)": select(PriceRegistryCommitment).where(
                    PriceRegistryCommitment.price_registry_id == entity.id
                ),
                "Adesões (contexto geral da ata)": select(PriceRegistryAdhesion).where(
                    PriceRegistryAdhesion.price_registry_id == entity.id
                ),
            }
        elif section == "contracts":
            children = {
                "Itens da empresa": select(ContractItem).where(
                    ContractItem.contract_id == entity.id,
                    or_(
                        ContractItem.supplier_cnpj == company.cnpj,
                        ContractItem.supplier_cnpj.is_(None),
                    ),
                ),
                "Termos e aditivos": select(ContractTerm).where(
                    ContractTerm.contract_id == entity.id
                ),
                "Execução financeira": select(Payment).where(
                    Payment.contract_id == entity.id, supplier_identity(Payment, company)
                ),
            }
        elif section == "documents":
            children = {
                "Evidências atribuídas à empresa": select(Evidence).where(
                    Evidence.document_id == entity.id,
                    or_(
                        exists(
                            select(ProcurementEvent.id).where(
                                ProcurementEvent.evidence_id == Evidence.id,
                                ProcurementEvent.company_id == company.id,
                            )
                        ),
                        exists(
                            select(Participant.id).where(
                                Participant.source_evidence_id == Evidence.id,
                                Participant.company_id == company.id,
                            )
                        ),
                    ),
                )
            }
            result["text_excerpt"] = (
                await self.db.scalar(
                    select(func.substr(Document.extracted_text, 1, 6000)).where(
                        Document.id == entity.id
                    )
                )
                or ""
            )
        elif section == "events":
            children = {"Evidências": select(Evidence).where(Evidence.id == entity.evidence_id)}
        result["groups"] = []
        for label, query in children.items():
            model = query.column_descriptions[0]["entity"]
            if model is Document:
                query = query.options(defer(Document.extracted_text), defer(Document.local_path))
            total = int(
                await self.db.scalar(
                    select(func.count()).select_from(query.order_by(None).subquery())
                )
                or 0
            )
            query = query.order_by(model.id)
            rows = await self.db.scalars(query.offset((page - 1) * 20).limit(20))
            row_list = list(rows)
            participant_data = {}
            if model is Participant:
                from app.services.participation_integrity import participant_payloads

                participant_data = {
                    str(x["id"]): x
                    for x in await participant_payloads(self.db, row_list, historical=True)
                }
            values = []
            for row in row_list:
                data = model_dict(
                    row,
                    include={c.key for c in row.__mapper__.column_attrs}
                    - {"local_path", "extracted_text", "fingerprint"},
                )
                data["kind"] = model.__tablename__
                if str(row.id) in participant_data:
                    data.update(participant_data[str(row.id)])
                    data["kind"] = model.__tablename__
                values.append(data)
            result["groups"].append({"label": label, "items": values, "total": total})
        if section in {
            "leads",
            "events",
            "deadlines",
            "price_registries",
            "contracts",
            "payments",
            "documents",
        }:
            related = await self.db.scalar(
                self.statement("procurements", historical=True).where(
                    Procurement.id == getattr(entity, "procurement_id", None)
                )
            )
            if related is None and section in {"price_registries", "contracts", "payments"}:
                control = getattr(entity, "linked_pncp_control_number", None) or getattr(
                    entity, "procurement_control_number", None
                )
                if control:
                    related = await self.db.scalar(
                        self.statement("procurements", historical=True).where(
                            Procurement.pncp_control_number == control
                        )
                    )
            if related is not None:
                result["groups"].append(
                    {
                        "label": "Contratação vinculada (contexto geral)",
                        "total": 1,
                        "items": [self.row("procurements", related)],
                    }
                )
        result["detail_page"] = page
        source_ids = {getattr(entity, "source_record_id", None)} - {None}
        for group in result["groups"]:
            for child in group["items"]:
                if child.get("source_record_id"):
                    source_ids.add(UUID(str(child["source_record_id"])))
        result["origins"] = [
            model_dict(x, include={"id", "source", "collected_at", "endpoint", "availability"})
            for x in (
                await self.db.scalars(
                    select(SourceRecord)
                    .where(SourceRecord.id.in_(source_ids))
                    .options(
                        load_only(
                            SourceRecord.id,
                            SourceRecord.source,
                            SourceRecord.collected_at,
                            SourceRecord.endpoint,
                            SourceRecord.availability,
                        )
                    )
                )
            )
        ]
        result["historical_notice"] = (
            "Verifique situação, versão e atividade das evidências. Histórico não comprova vigência atual."
        )
        if section in {"leads", "events"}:
            from app.services.ingestion.processor import (
                event_award_inference_is_current,
                event_document_is_active,
            )

            current_event = (
                entity
                if section == "events"
                else await self.db.get(ProcurementEvent, entity.triggering_event_id)
            )
            result["is_evidence_current"] = bool(
                current_event
                and current_event.company_id == company.id
                and current_event.procurement_id == entity.procurement_id
                and await event_document_is_active(self.db, current_event)
                and await event_award_inference_is_current(self.db, current_event)
            )
            if not result["is_evidence_current"]:
                result["historical_notice"] = (
                    "Evidência retirada ou vínculo oficial sem vigência atual. Este registro é histórico e exige nova conferência."
                )
        return result
