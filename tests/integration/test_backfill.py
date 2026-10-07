"""Backfill tests for fields recovered from already-collected raw payloads."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.models import FieldObservation, Procurement, SourceRecord
from app.services.ingestion import backfill_procurement_fields

from .conftest import DatabaseContext


def _procurement(source: str, external_id: str, control: str | None) -> Procurement:
    return Procurement(
        source=source,
        external_id=external_id,
        pncp_control_number=control,
        title="Processo legado",
        agency_name="Órgão de Exemplo",
        modality="Pregão - Eletrônico",
        status="Divulgada no PNCP",
        fingerprint=external_id.ljust(64, "0")[:64],
    )


@pytest.mark.asyncio
async def test_backfill_recovers_srp_and_legal_basis_from_raw_payloads(
    database: DatabaseContext,
) -> None:
    """Only literal source keys are applied, and the operation is idempotent."""

    async with database.sessions() as session, session.begin():
        pncp = _procurement("pncp", "00000000000191-1-000001/2026", "00000000000191-1-000001/2026")
        compras = _procurement("compras_gov", "98092105900012026", "00000000000191-1-000002/2026")
        session.add_all([pncp, compras])
        session.add_all(
            [
                SourceRecord(
                    source="pncp",
                    endpoint="https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
                    request_parameters={"pagina": 1},
                    response_hash="a" * 64,
                    raw_payload={
                        "data": [
                            {
                                "numeroControlePNCP": "00000000000191-1-000001/2026",
                                "srp": True,
                                "amparoLegal": {
                                    "codigo": 1,
                                    "nome": "Lei 14.133/2021, art. 75, inciso I",
                                },
                            }
                        ]
                    },
                    http_status=200,
                    collected_at=datetime.now(UTC),
                    fingerprint="f" * 64,
                ),
                SourceRecord(
                    source="compras_gov",
                    endpoint=(
                        "https://dadosabertos.compras.gov.br/"
                        "modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
                    ),
                    request_parameters={"pagina": 1},
                    response_hash="b" * 64,
                    raw_payload={
                        "resultado": [
                            {
                                "idCompra": "98092105900012026",
                                "numeroControlePNCP": "00000000000191-1-000002/2026",
                                "srp": False,
                                "amparoLegalNome": "Lei 14.133/2021, art. 74, inciso III",
                            }
                        ]
                    },
                    http_status=200,
                    collected_at=datetime.now(UTC),
                    fingerprint="g" * 64,
                ),
            ]
        )

    first = await backfill_procurement_fields(session_factory=database.sessions)
    second = await backfill_procurement_fields(session_factory=database.sessions)

    assert first.updated_procurements == 2
    assert second.updated_procurements == 0
    async with database.sessions() as session:
        stored = {
            item.external_id: item for item in (await session.scalars(select(Procurement))).all()
        }
        observations = list((await session.scalars(select(FieldObservation))).all())

    pncp = stored["00000000000191-1-000001/2026"]
    compras = stored["98092105900012026"]
    assert pncp.is_srp is True
    assert pncp.legal_basis == "Lei 14.133/2021, art. 75, inciso I"
    assert pncp.procurement_type == "licitacao"
    assert compras.is_srp is False
    assert compras.legal_basis == "Lei 14.133/2021, art. 74, inciso III"
    assert compras.procurement_type == "licitacao"
    assert {item.field_name for item in observations} >= {
        "is_srp",
        "legal_basis",
        "procurement_type",
    }
    assert all(item.source_record_id is not None for item in observations)


@pytest.mark.asyncio
async def test_backfill_processes_in_batches_and_honors_limit(
    database: DatabaseContext,
) -> None:
    """Keyset batching walks every stored response and respects --limit."""

    base = datetime.now(UTC) - timedelta(days=3)
    async with database.sessions() as session, session.begin():
        for index in range(3):
            control = f"00000000000191-1-00000{index}/2026"
            session.add(_procurement("pncp", f"batch-{index}", control))
            session.add(
                SourceRecord(
                    source="pncp",
                    endpoint="https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
                    request_parameters={},
                    response_hash=str(index) * 64,
                    raw_payload={"data": [{"numeroControlePNCP": control, "srp": True}]},
                    http_status=200,
                    collected_at=base + timedelta(minutes=index),
                    fingerprint=str(index + 1) * 64,
                )
            )

    first = await backfill_procurement_fields(session_factory=database.sessions, batch_size=2)
    limited = await backfill_procurement_fields(
        session_factory=database.sessions, limit=2, batch_size=1
    )

    assert first.scanned_records == 3
    assert first.updated_procurements == 3
    assert limited.scanned_records == 2
    assert limited.updated_procurements == 0
