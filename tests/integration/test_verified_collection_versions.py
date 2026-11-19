"""Real adapters and transactions: reuse must never certify a partial observation."""

import asyncio
import copy
from urllib.parse import urlsplit

import httpx
import pytest
from sqlalchemy import delete, func, select

from app.config import Settings
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector
from app.models import (
    CollectionWorkItem,
    CrawlRun,
    CrawlRunStatus,
    FieldChange,
    FieldObservation,
    FieldState,
    Procurement,
    ProcurementItem,
    SourceRecord,
    SourceResponseValidator,
    VerifiedCollectionVersion,
)
from app.repositories.procurements import ProcurementRepository
from app.services.ingestion.pipeline import IngestionPipeline, PipelineRequest
from app.services.participation_reconciliation import IntegrityReport
from app.services.procurement_search import ObjectScope
from app.services.procurement_search_reconciliation import inspect_legacy_search_text


def row():
    return {
        "numeroControlePNCP": "00000000000191-1-000001/2026",
        "numeroCompra": "1",
        "anoCompra": 2026,
        "sequencialCompra": 1,
        "modalidadeId": 6,
        "modalidadeNome": "Pregão Eletrônico",
        "objetoCompra": "Equipamentos",
        "orgaoEntidade": {"cnpj": "00000000000191", "razaoSocial": "Órgão"},
        "unidadeOrgao": {"codigoUnidade": "980921", "ufSigla": "MA"},
        "dataAtualizacaoGlobal": "2026-10-28T10:00:00",
        "dataAtualizacao": "2026-10-28T10:00:00",
        "situacaoCompraNome": "Divulgada no PNCP",
    }


def setup(database, *, etags=False):
    state = {
        "row": row(),
        "item": "Equipamento",
        "requests": [],
        "partial": False,
        "change_during": False,
    }

    def handler(request):
        state["requests"].append(request)
        if etags and request.headers.get("If-None-Match") == '"version-1"':
            return httpx.Response(304, headers={"ETag": '"version-1"'})
        path = request.url.path
        if "/contratacoes/" in path:
            payload = {
                "data": [copy.deepcopy(state["row"])],
                "totalPaginas": 1,
                "totalRegistros": 1,
            }
        elif path.endswith("/itens"):
            if state["partial"]:
                return httpx.Response(503)
            payload = [{"numeroItem": 1, "descricao": state["item"], "temResultado": False}]
            if state.get("unknown_result"):
                payload[0].pop("temResultado")
            if state["change_during"]:
                state["row"]["dataAtualizacaoGlobal"] = "2026-10-28T11:00:00"
        elif path.endswith("/arquivos"):
            payload = []
        else:
            payload = copy.deepcopy(state["row"])
            if state.get("detail_omits_scope"):
                payload.pop("informacaoComplementar", None)
                payload["orgaoEntidade"].pop("esferaId", None)
        return httpx.Response(200, json=payload, headers={"ETag": '"version-1"'} if etags else {})

    raw = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(_env_file=None, http_min_request_interval_seconds=0, http_max_retries=0)
    connector = PNCPConnector(settings, http_client=AsyncHTTPClient(settings, client=raw))
    pipeline = IngestionPipeline(
        settings, session_factory=database.sessions, connectors={"pncp": connector}
    )
    return state, raw, pipeline


async def test_same_official_version_avoids_details_and_force_refresh_fetches_again(database):
    state, raw, pipeline = setup(database)
    request = PipelineRequest(connector="pncp", process_documents=False)
    try:
        first = await pipeline.run(request)
        async with database.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(VerifiedCollectionVersion))
                == 1
            )
            old_changes = await session.scalar(select(func.count()).select_from(FieldChange))
        assert first.records_created == 1
        state["requests"].clear()
        second = await pipeline.run(request)
        assert len(state["requests"]) == 1 and "/contratacoes/" in state["requests"][0].url.path
        assert second.records_updated == second.records_unchanged == 1
        assert second.source_results["pncp"]["records_reused"] == 1
        async with database.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(FieldChange)) == old_changes
            )
            works = (
                await session.scalars(
                    select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
                )
            ).all()
            assert any(w.payload.get("verified_version_id") for w in works)
        state["requests"].clear()
        forced = await pipeline.run(
            PipelineRequest(connector="pncp", process_documents=False, force_refresh=True)
        )
        assert len(state["requests"]) > 1 and forced.source_results["pncp"]["records_reused"] == 0
    finally:
        await raw.aclose()


@pytest.mark.parametrize("legacy_action", ["recollect", "reconcile"])
async def test_discovery_scope_projection_stays_auditable_and_recoverable(database, legacy_action):
    state, client, pipeline = setup(database)
    state["row"]["informacaoComplementar"] = "Fornecimento de CÂMERA"
    state["row"]["orgaoEntidade"]["esferaId"] = "M"
    state["detail_omits_scope"] = True
    scope = ObjectScope.build("camera", government_sphere="M")
    request = PipelineRequest(connector="pncp", process_documents=False, object_scope=scope)
    try:
        assert (await pipeline.run(request)).records_created == 1
        async with database.sessions() as session, session.begin():
            procurement = (
                await ProcurementRepository(session).list_filtered(object_scope=scope)
            ).items[0]
            assert procurement.object_complement == "Fornecimento de CÂMERA"
            assert procurement.government_sphere == "M"
            for model in (FieldState, FieldObservation):
                tracked = await session.execute(
                    select(model.field_name, model.value, SourceRecord.endpoint)
                    .join(SourceRecord, SourceRecord.id == model.source_record_id)
                    .where(
                        model.entity_id == procurement.id,
                        model.field_name.in_(("object_complement", "government_sphere")),
                    )
                )
                observations = tracked.all()
                assert {(field, value) for field, value, _ in observations} == {
                    ("object_complement", "Fornecimento de CÂMERA"),
                    ("government_sphere", "M"),
                }
                assert all(
                    urlsplit(endpoint).path.endswith("/contratacoes/publicacao")
                    for _, _, endpoint in observations
                )

            assert await session.scalar(select(VerifiedCollectionVersion))
            # Simulate a version certified before the header projection existed.
            procurement.object_complement = None
            await session.execute(
                delete(FieldState).where(FieldState.field_name == "object_complement")
            )
        if legacy_action == "reconcile":
            async with database.sessions() as session, session.begin():
                procurement = (await session.scalars(select(Procurement))).one()
                report = IntegrityReport()
                await inspect_legacy_search_text(session, procurement, apply=False, report=report)
                assert report.totals == {"search_text_requiring_repair": 1}
                assert procurement.object_complement is None
                await inspect_legacy_search_text(session, procurement, apply=True, report=report)
        else:
            state["requests"].clear()
            summary = await pipeline.run(request)
            assert summary.source_results["pncp"]["records_reused"] == 0
            assert len(state["requests"]) > 1

        async with database.sessions() as session:
            assert (
                await ProcurementRepository(session).list_filtered(object_scope=scope)
            ).total == 1
            evidence_endpoint = await session.scalar(
                select(SourceRecord.endpoint)
                .join(FieldState, FieldState.source_record_id == SourceRecord.id)
                .where(FieldState.field_name == "object_complement")
            )
            assert evidence_endpoint and urlsplit(evidence_endpoint).path.endswith(
                "/contratacoes/publicacao"
            )
    finally:
        await client.aclose()


async def test_direct_pncp_refresh_only_expands_when_the_official_version_changes(database):
    state, client, pipeline = setup(database)
    request = PipelineRequest(
        connector="pncp",
        external_ids=(state["row"]["numeroControlePNCP"],),
        process_documents=False,
    )
    try:
        first = await pipeline.run(request)
        assert first.status is CrawlRunStatus.COMPLETED

        state["requests"].clear()
        unchanged = await pipeline.run(request)
        paths = [call.url.path for call in state["requests"]]
        assert len(paths) == 1 and paths[0].endswith("/compras/2026/1")
        assert unchanged.source_results["pncp"]["records_reused"] == 1

        state["requests"].clear()
        state["row"]["dataAtualizacaoGlobal"] = "2026-10-28T11:00:00"
        state["item"] = "Equipamento atualizado"
        changed = await pipeline.run(request)
        assert changed.status is CrawlRunStatus.COMPLETED
        assert changed.source_results["pncp"]["records_reused"] == 0
        paths = [call.url.path for call in state["requests"]]
        assert any(path.endswith("/itens") for path in paths)
        assert any(path.endswith("/arquivos") for path in paths)
        async with database.sessions() as session:
            assert await session.scalar(select(ProcurementItem.description)) == state["item"]
    finally:
        await client.aclose()


async def test_one_final_detail_request_both_enriches_and_verifies_the_bundle(database):
    state, raw, pipeline = setup(database)
    try:
        summary = await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
        paths = [request.url.path for request in state["requests"]]
        details = [
            i
            for i, path in enumerate(paths)
            if "/orgaos/" in path and not path.endswith(("/itens", "/arquivos"))
        ]
        assert len(details) == 1
        assert details[0] > paths.index(next(path for path in paths if path.endswith("/itens")))
        assert details[0] > paths.index(next(path for path in paths if path.endswith("/arquivos")))
        assert summary.records_created == 1 and summary.records_failed == 0
        async with database.sessions() as session:
            proof = await session.scalar(select(VerifiedCollectionVersion))
            assert proof and {"detail", "version_check"} <= set(proof.operations)
            responses = (
                await session.scalars(
                    select(SourceRecord).where(SourceRecord.endpoint.endswith("/compras/2026/1"))
                )
            ).all()
            # Snapshot and domain references may share the response, but there
            # was one query time, not an invented second HTTP observation.
            assert len({response.collected_at for response in responses}) == 1
    finally:
        await raw.aclose()


async def test_cancelled_final_detail_cannot_confirm_or_certify_the_record(database, monkeypatch):
    state, raw, pipeline = setup(database)
    entered = asyncio.Event()
    release = asyncio.Event()
    connector = pipeline._connectors["pncp"]
    original = connector.verify_procurement_version

    async def paused(identifier):
        entered.set()
        await release.wait()
        return await original(identifier)

    monkeypatch.setattr(connector, "verify_procurement_version", paused)
    worker = asyncio.create_task(
        pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        async with database.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(VerifiedCollectionVersion))
                == 0
            )
            assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
            run = await session.scalar(select(CrawlRun))
            assert run.status is CrawlRunStatus.CANCELLED
            records = (
                await session.scalars(
                    select(CollectionWorkItem).where(CollectionWorkItem.kind == "record")
                )
            ).all()
            assert records and all(item.status != "succeeded" for item in records)
    finally:
        release.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await raw.aclose()


@pytest.mark.parametrize(
    "change", ["child", "equal_divergent", "missing_clock", "older_clock", "missing_evidence"]
)
async def test_changed_uncertain_or_unproven_versions_must_be_fetched(database, change):
    state, raw, pipeline = setup(database)
    request = PipelineRequest(connector="pncp", process_documents=False)
    try:
        await pipeline.run(request)
        if change == "child":
            state["row"]["dataAtualizacaoGlobal"] = "2026-10-28T11:00:00"
            state["item"] = "Equipamento atualizado"
        elif change == "equal_divergent":
            state["row"]["objetoCompra"] = "Objeto divergente"
        elif change == "missing_clock":
            state["row"].pop("dataAtualizacaoGlobal")
        elif change == "older_clock":
            state["row"]["dataAtualizacaoGlobal"] = "2026-10-27T10:00:00"
        else:
            async with database.sessions() as session, session.begin():
                proof = await session.scalar(select(VerifiedCollectionVersion))
                identifier = next(iter(proof.source_records))
                from uuid import UUID

                await session.execute(
                    delete(SourceRecord).where(SourceRecord.id == UUID(identifier))
                )
        state["requests"].clear()
        result = await pipeline.run(request)
        assert len(state["requests"]) > 1 and result.source_results["pncp"]["records_reused"] == 0
        if change == "older_clock":
            state["requests"].clear()
            repeated = await pipeline.run(request)
            assert len(state["requests"]) > 1
            assert repeated.source_results["pncp"]["records_reused"] == 0
        if change == "child":
            async with database.sessions() as session:
                assert (
                    await session.scalar(select(ProcurementItem))
                ).description == "Equipamento atualizado"
            assert result.records_changed == 1
    finally:
        await raw.aclose()


@pytest.mark.parametrize("failure", ["partial", "change_during", "unknown_result"])
async def test_incomplete_or_changing_bundle_is_never_certified(database, failure):
    state, raw, pipeline = setup(database)
    state[failure] = True
    try:
        await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
        async with database.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(VerifiedCollectionVersion))
                == 0
            )
            assert await session.scalar(select(func.count()).select_from(Procurement)) == 1
    finally:
        await raw.aclose()


async def test_verification_and_business_write_roll_back_together(database, monkeypatch):
    state, raw, pipeline = setup(database)

    async def fail(*args, **kwargs):
        raise RuntimeError("verification insert failed")

    monkeypatch.setattr("app.services.ingestion.pipeline.record_verified", fail)
    try:
        result = await pipeline.run(PipelineRequest(connector="pncp", process_documents=False))
        assert result.records_failed == 1
        async with database.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Procurement)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(VerifiedCollectionVersion))
                == 0
            )
    finally:
        await raw.aclose()


async def test_304_reuses_original_snapshot_dates_and_force_refresh_bypasses_validators(database):
    state, raw, pipeline = setup(database, etags=True)
    state["row"].pop("dataAtualizacaoGlobal")  # cannot skip the complete bundle by its header clock
    request = PipelineRequest(connector="pncp", process_documents=False)
    try:
        await pipeline.run(request)
        async with database.sessions() as session:
            original_dates = set((await session.scalars(select(SourceRecord.collected_at))).all())
            assert (
                await session.scalar(select(func.count()).select_from(SourceResponseValidator)) > 0
            )
        state["requests"].clear()
        second = await pipeline.run(request)
        assert state["requests"] and all(r.headers.get("If-None-Match") for r in state["requests"])
        assert second.source_results["pncp"]["operations_reused"] == len(state["requests"])
        async with database.sessions() as session:
            validations = (
                await session.scalars(
                    select(CollectionWorkItem).where(CollectionWorkItem.kind == "validation")
                )
            ).all()
            assert validations and all(w.payload["http_status"] == 304 for w in validations)
            dates = set(
                (
                    await session.scalars(
                        select(SourceRecord.collected_at).where(
                            SourceRecord.crawl_run_id == second.run_id
                        )
                    )
                ).all()
            )
            assert dates <= original_dates
        state["requests"].clear()
        await pipeline.run(
            PipelineRequest(connector="pncp", process_documents=False, force_refresh=True)
        )
        assert all(not r.headers.get("If-None-Match") for r in state["requests"])
    finally:
        await raw.aclose()
