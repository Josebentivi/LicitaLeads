"""Federal files and API converge on the same verified document without mixing totals."""

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select

from app.connectors.base import RawPayment
from app.models import DataAvailability, FieldObservation, Payment, SourceRecord
from app.services.ingestion.payments import PaymentIngestionService

D = "153173152532025OB008929"
CNPJ = "00000000000191"


def payment(source_id, role="issued", **extra):
    return RawPayment(
        source="transparencia",
        external_id=source_id,
        document_number=D,
        phase="Pagamento",
        supplier_cnpj=CNPJ,
        payment_date=datetime(2025, 10, 17, tzinfo=UTC),
        amount=Decimal("200" if role == "issued" else "24"),
        federal_beneficiary_role=role,
        federal_creditor_list="LISTA1" if role == "final" else None,
        **extra,
    )


async def test_federal_legacy_issued_and_final_records_keep_independent_totals(database):
    service = PaymentIngestionService(session_factory=database.sessions)
    try:
        async with database.sessions() as session, session.begin():
            legacy = Payment(
                source="transparencia",
                external_id="legacy-issued",
                fingerprint="legacy-issued",
                document_number=D,
                supplier_cnpj=CNPJ,
                amount=Decimal("200"),
                phase="Pagamento",
            )
            session.add(legacy)
            await session.flush()
            prior_id = legacy.id

        archive = await service.persist_raw_payments(
            [
                payment("archive-issued", federal_intermediary=True),
                payment("archive-final", "final"),
            ]
        )
        api = await service.persist_raw_payments(
            [payment("api-issued", federal_intermediary=False), payment("api-final", "final")]
        )
        assert (archive.payments_created, archive.payments_updated) == (1, 1)
        assert (api.payments_created, api.payments_updated) == (0, 2)
        async with database.sessions() as session:
            payments = {
                record.federal_beneficiary_role: record
                for record in (await session.scalars(select(Payment))).all()
            }
            assert set(payments) == {"issued", "final"}
            assert payments["issued"].id == prior_id
            assert payments["issued"].external_id == "legacy-issued"
            assert payments["issued"].federal_intermediary is True
            assert payments["issued"].amount == Decimal("200")
            assert payments["final"].federal_creditor_list == "LISTA1"
            assert payments["final"].amount == Decimal("24")
    finally:
        await service.aclose()


async def test_conflicting_legacy_rows_are_preserved_with_official_review_evidence(database):
    service = PaymentIngestionService(session_factory=database.sessions)
    try:
        async with database.sessions() as session, session.begin():
            session.add_all(
                [
                    Payment(
                        source="transparencia",
                        external_id=f"legacy-{amount}",
                        fingerprint=f"legacy-{amount}",
                        document_number=D,
                        supplier_cnpj=CNPJ,
                        amount=Decimal(amount),
                        phase="Pagamento",
                    )
                    for amount in (123, 200)
                ]
            )
            authority = SourceRecord(
                source="transparencia",
                endpoint="https://portaldatransparencia.gov.br/download-de-dados/despesas/20251017",
                request_parameters={},
                response_hash="test-hash",
                raw_payload={"Código Pagamento": D, "Código Favorecido": CNPJ},
                http_status=200,
                availability=DataAvailability.AVAILABLE,
                fingerprint="test-federal-archive",
            )
            session.add(authority)
            await session.flush()
            source_id = authority.id

        summary = await service.persist_raw_payments([payment("new")], source_record_id=source_id)
        assert summary.payments_conflicted == 1
        assert summary.payments_created == 0
        async with database.sessions() as session:
            old = (await session.scalars(select(Payment))).all()
            assert len(old) == 2
            assert all(
                row.federal_identity_conflict and row.federal_identity is None for row in old
            )
            observations = (await session.scalars(select(FieldObservation))).all()
            assert len(observations) == 2
            assert {row.source_record_id for row in observations} == {source_id}
            assert {row.entity_id for row in observations} == {row.id for row in old}
    finally:
        await service.aclose()


async def test_replaying_a_pre_migration_api_row_keeps_both_official_evidence_links(database):
    service = PaymentIngestionService(session_factory=database.sessions)
    try:
        async with database.sessions() as session, session.begin():
            sources = [
                SourceRecord(
                    source="transparencia",
                    endpoint=endpoint,
                    request_parameters=parameters,
                    response_hash=f"sha-{index}",
                    raw_payload={"documento": D, "codigoFavorecido": CNPJ},
                    http_status=200,
                    availability=DataAvailability.AVAILABLE,
                    fingerprint=f"new-origin-{index}",
                )
                for index, (endpoint, parameters) in enumerate(
                    (
                        (
                            "https://portaldatransparencia.gov.br/download-de-dados/despesas/20251017",
                            {},
                        ),
                        (
                            "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos-por-favorecido",
                            {"fase": 3},
                        ),
                    )
                )
            ]
            session.add_all(sources)
            await session.flush()
            archive_id, api_id = [source.id for source in sources]
        await service.persist_raw_payments([payment("arquivo")], source_record_id=archive_id)
        replay = payment("legacy-api").model_copy(
            update={"federal_beneficiary_role": None, "phase": None}
        )
        summary = await service.persist_raw_payments([replay], source_record_id=api_id)
        assert summary.payments_created == 0
        async with database.sessions() as session:
            row = (await session.scalars(select(Payment))).one()
            assert row.federal_beneficiary_role == "issued"
            observations = (await session.scalars(select(FieldObservation))).all()
            assert {observation.entity_id for observation in observations} == {row.id}
            assert {observation.source_record_id for observation in observations} == {
                archive_id,
                api_id,
            }
    finally:
        await service.aclose()
