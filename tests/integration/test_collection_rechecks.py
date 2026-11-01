"""Old records rotate by official identity and never widen the source's scope."""

from datetime import UTC, datetime, timedelta

import pytest

from app.config import Settings
from app.models import (
    Contract,
    CrawlRun,
    CrawlRunStatus,
    PriceRegistry,
    Procurement,
    ProcurementSource,
    SourceRecord,
    VerifiedCollectionVersion,
)
from app.services.ingestion.collection_rechecks import recheck_compras
from app.services.ingestion.pipeline import PipelineRequest


@pytest.mark.parametrize(
    "mode,model", [("price_registries", PriceRegistry), ("contracts", Contract)]
)
async def test_at_most_one_hundred_known_source_ids_rotate_without_repeating(mode, model, database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(
            connector="compras_gov", status=CrawlRunStatus.COMPLETED, filters={"uf": "MA"}
        )
        session.add(run)
        await session.flush()
        record = SourceRecord(
            source="compras_gov",
            crawl_run_id=run.id,
            endpoint="https://official.test",
            response_hash="a" * 64,
            raw_payload={},
            request_parameters={},
            http_status=200,
            collected_at=now,
            fingerprint="b" * 64,
        )
        session.add(record)
        await session.flush()
        for i in range(105):
            session.add(
                model(
                    source="compras_gov",
                    external_id=f"id-{i}",
                    pncp_control_number=f"00000000000191-2-{i:06}/2026",
                    source_record_id=record.id,
                    agency_cnpj="00000000000191",
                    fingerprint=f"{i:064x}",
                )
            )
    visits = []

    class Pipeline:
        async def run(self, request):
            visits.append(request)

    cfg = Settings(
        _env_file=None, compras_recheck_batch_size=100, crawl_max_records_per_source=2000
    )
    request = PipelineRequest(connector="compras_gov", mode=mode, agency_cnpj="00000000000191")
    await recheck_compras(Pipeline(), request, cfg, database.sessions)
    await recheck_compras(Pipeline(), request, cfg, database.sessions)
    assert len(visits[0].external_ids) == len(visits[1].external_ids) == 100
    assert len(set(visits[0].external_ids) | set(visits[1].external_ids)) == 105
    assert visits[0].recheck_key and visits[0].sync_context is None
    assert visits[0].process_documents is False


async def test_compras_rotation_includes_closed_records_and_respects_scope(database):
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        for i, (uf, agency, modality) in enumerate(
            [
                ("MA", "00000000000191", "Pregão Eletrônico"),
                ("PI", "00000000000191", "Pregão Eletrônico"),
                ("MA", "11111111000191", "Pregão Eletrônico"),
                ("MA", "00000000000191", "Concorrência"),
            ]
        ):
            item = Procurement(
                source="compras_gov",
                external_id=f"closed-{i}",
                uf=uf,
                agency_cnpj=agency,
                modality=modality,
                status="Homologada",
                fingerprint=f"{i:064x}",
            )
            session.add(item)
            await session.flush()
            session.add(
                ProcurementSource(
                    source="compras_gov",
                    procurement_id=item.id,
                    external_id=item.external_id,
                    collected_at=now - timedelta(days=300),
                    fingerprint=f"{i + 10:064x}",
                )
            )
    requests = []

    class Pipeline:
        async def run(self, request):
            requests.append(request)

    await recheck_compras(
        Pipeline(),
        PipelineRequest(connector="compras_gov", agency_cnpj="00000000000191"),
        Settings(_env_file=None),
        database.sessions,
    )
    assert requests[0].external_ids == ("closed-0",)


@pytest.mark.parametrize(
    "mode,model", [("price_registries", PriceRegistry), ("contracts", Contract)]
)
async def test_verified_source_identity_survives_later_header_from_other_source(
    database, mode, model
):
    identity = "00000000000191-2-000001/2026"
    now = datetime.now(UTC)
    async with database.sessions() as session, session.begin():
        run = CrawlRun(connector="pncp", filters={"uf": "MA"}, status=CrawlRunStatus.COMPLETED)
        session.add(run)
        await session.flush()
        latest = SourceRecord(
            source="pncp",
            crawl_run_id=run.id,
            endpoint="https://official.test",
            raw_payload={},
            response_hash="a" * 64,
            fingerprint="b" * 64,
        )
        session.add(latest)
        await session.flush()
        session.add(
            model(
                source="pncp",
                external_id=identity,
                pncp_control_number=identity,
                source_record_id=latest.id,
                fingerprint="c" * 64,
            )
        )
        session.add(
            VerifiedCollectionVersion(
                source="compras_gov",
                mode=mode,
                external_id=identity,
                adapter_key="d" * 64,
                header_hash="e" * 64,
                source_updated_at=now,
                timestamp_origin="dataHoraAtualizacao",
                operations=["header"],
                source_records={},
                verified_at=now,
            )
        )
    requests = []

    class Pipeline:
        async def run(self, request):
            requests.append(request)

    await recheck_compras(
        Pipeline(),
        PipelineRequest(connector="compras_gov", mode=mode),
        Settings(_env_file=None),
        database.sessions,
    )
    assert requests[0].external_ids == (identity,)
