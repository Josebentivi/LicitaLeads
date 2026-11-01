"""Historical rows remain auditable without inflating the current views."""

from decimal import Decimal

from sqlalchemy import func, select

from app.models import (
    Company,
    Contract,
    Document,
    Evidence,
    Participant,
    Payment,
    Procurement,
    ProcurementEvent,
)
from app.models.enums import ParticipantStatus, ProcurementEventType
from app.repositories.contracts import ContractRepository, PaymentRepository
from app.repositories.procurements import (
    CompanyRepository,
    ParticipantRepository,
    ProcurementRepository,
)


async def test_superseded_payments_are_excluded_from_lists_and_totals(database):
    async with database.sessions() as session, session.begin():
        contract = Contract(source="pncp", external_id="contract", fingerprint="a" * 64)
        session.add(contract)
        await session.flush()
        for index, (amount, current) in enumerate((("100", False), ("30", True), ("70", True))):
            session.add(
                Payment(
                    source="municipal:test",
                    external_id=str(index),
                    contract_id=contract.id,
                    amount=Decimal(amount),
                    phase="pagamento",
                    is_current=current,
                    fingerprint=str(index) * 64,
                )
            )
    async with database.sessions() as session:
        assert (await PaymentRepository(session).list_filtered()).total == 2
        assert await ContractRepository(session).payment_total(contract.id) == Decimal("100")
        assert len((await ContractRepository(session).get_detail(contract.id)).payments) == 2
        assert await session.scalar(select(func.count(Payment.id))) == 3


async def test_inactive_document_participants_leave_current_views_but_keep_history(database):
    async with database.sessions() as session, session.begin():
        procurement = Procurement(source="pncp", external_id="process", fingerprint="a" * 64)
        company = Company(cnpj="12345678000190", normalized_name="empresa", fingerprint="b" * 64)
        session.add_all([procurement, company])
        await session.flush()
        document = Document(
            procurement_id=procurement.id,
            original_url="https://official.test/ata",
            is_active=False,
            fingerprint="c" * 64,
        )
        session.add(document)
        await session.flush()
        evidence = Evidence(
            procurement_id=procurement.id,
            document_id=document.id,
            text_excerpt="Participante",
            fingerprint="d" * 64,
        )
        session.add(evidence)
        await session.flush()
        session.add(
            Participant(
                procurement_id=procurement.id,
                company_id=company.id,
                source="document",
                source_evidence_id=evidence.id,
                status_code=ParticipantStatus.DISQUALIFIED,
                fingerprint="e" * 64,
            )
        )
        session.add(
            ProcurementEvent(
                procurement_id=procurement.id,
                company_id=company.id,
                evidence_id=evidence.id,
                event_type=ProcurementEventType.DISQUALIFIED,
                source="document",
                raw_description="Desclassificada",
                confidence=Decimal("1"),
                fingerprint="f" * 64,
            )
        )
    async with database.sessions() as session:
        assert (await ParticipantRepository(session).list_for_company(company.id)).total == 0
        detail = await ProcurementRepository(session).get_detail(procurement.id)
        assert detail.participants == []
        assert (
            await ProcurementRepository(session).list_filtered(company_cnpj=company.cnpj)
        ).total == 0
        rows = await CompanyRepository(session).list_with_stats()
        assert rows.items[0][1]["participations"] == 0
        breakdown = await CompanyRepository(session).failure_breakdown(company.id)
        assert breakdown == {"documented_losses": 0, "by_event": {}, "by_reason": {}}
        assert await session.scalar(select(func.count(Participant.id))) == 1
        assert await session.scalar(select(func.count(ProcurementEvent.id))) == 1
