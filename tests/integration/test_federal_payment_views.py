"""Federal collection controls and evidence-backed financial summaries."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from bs4 import BeautifulSoup

from app import web
from app.models import (
    CollectionBatch,
    CollectionWorkItem,
    Company,
    Contract,
    CrawlRun,
    CrawlRunStatus,
    Payment,
)
from app.services.ingestion.complete_collection import COMPLETE_COLLECTION_MODE, CompleteCollection
from app.services.ingestion.federal_payments import FEDERAL_BATCH_MODE, FEDERAL_CONNECTOR

COMPANY_CNPJ = "12345678000195"
OTHER_CNPJ = "00000000000191"


@pytest.mark.parametrize(
    ("form", "expected"),
    [
        ({}, "acervo"),
        ({"federal_scope": "acompanhadas"}, "acompanhadas"),
        ({"federal_scope": "pular"}, "pular"),
        ({"federal_scope": "acervo", "procurements_only": "true"}, "pular"),
    ],
)
async def test_general_collection_post_freezes_federal_scope(
    api_client, database, monkeypatch, form, expected
):
    monkeypatch.setattr(
        web,
        "CompleteCollection",
        lambda settings: CompleteCollection(settings, sessions=database.sessions),
    )
    monkeypatch.setattr(web, "launch_complete_collection", lambda *args: None)
    result = await api_client.post("/crawls/collect", data=form)
    assert result.status_code == 303
    run_id = UUID(result.headers["location"].split("#coleta-")[1])
    async with database.sessions() as db:
        run = await db.get(CrawlRun, run_id)
        assert run is not None and run.filters["federal_scope"] == expected


async def test_federal_form_preserves_scope_on_period_error(api_client):
    response = await api_client.get("/crawls")
    default = BeautifulSoup(response.text, "html.parser")
    assert default.select_one('select[name="federal_scope"] option[selected]')["value"] == "acervo"

    invalid = await api_client.post(
        "/crawls/collect",
        data={
            "start_date": "2026-11-02",
            "end_date": "2026-11-01",
            "federal_scope": "acompanhadas",
        },
    )
    assert invalid.status_code == 422
    recovered = BeautifulSoup(invalid.text, "html.parser")
    assert (
        recovered.select_one('select[name="federal_scope"] option[selected]')["value"]
        == "acompanhadas"
    )


async def _seed_federal_batch(database):
    parent_id, batch_id, original_run_id, last_run_id = (uuid4() for _ in range(4))
    now = datetime.now(UTC)
    filters = {
        "mode": FEDERAL_BATCH_MODE,
        "connector": FEDERAL_CONNECTOR,
        "parent_run_id": str(parent_id),
        "start_date": "2026-10-01",
        "end_date": "2026-10-04",
        "cnpjs": [COMPANY_CNPJ, OTHER_CNPJ],
        "tracked_cnpjs": [COMPANY_CNPJ],
    }
    items = [
        ("archive", "2026-10-01", "succeeded", {"day": "2026-10-01", "found_cnpjs": [OTHER_CNPJ]}),
        ("archive", "2026-10-02", "empty", {"day": "2026-10-02", "found_cnpjs": []}),
        ("archive", "2026-10-03", "waiting", {"day": "2026-10-03"}),
        ("archive", "2026-10-04", "failed", {"day": "2026-10-04", "diagnostic": "ZIP inválido"}),
        ("beneficiary", f"{COMPANY_CNPJ}:2026", "empty", {"cnpj": COMPANY_CNPJ, "year": 2026}),
    ]
    async with database.sessions.begin() as db:
        db.add(
            CollectionBatch(
                id=batch_id,
                stream_key="federal-views-test",
                filters=filters,
                status="limited",
                last_run_id=last_run_id,
                cursors={
                    "federal": {
                        "archives": {
                            "total": 4,
                            "completed": 2,
                            "empty": 1,
                            "waiting": 1,
                            "failed": 1,
                        },
                        "api": {"total": 1, "completed": 1, "empty": 1},
                        "records_created": 10,
                    }
                },
            )
        )
        await db.flush()
        db.add(
            CrawlRun(
                id=parent_id,
                connector=COMPLETE_COLLECTION_MODE,
                status=CrawlRunStatus.PARTIAL,
                filters={"mode": COMPLETE_COLLECTION_MODE},
                finished_at=now,
                cursor={
                    "complete_stages": [
                        {
                            "key": "federal",
                            "label": "Pagamentos federais",
                            "status": "partial",
                            "run_ids": [str(original_run_id)],
                            "batch_id": str(batch_id),
                        }
                    ]
                },
            )
        )
        for identifier in (original_run_id, last_run_id):
            db.add(
                CrawlRun(
                    id=identifier,
                    batch_id=batch_id,
                    connector=FEDERAL_CONNECTOR,
                    filters=filters,
                    status=CrawlRunStatus.PARTIAL,
                    finished_at=now,
                )
            )
        for position, (kind, identity, status, payload) in enumerate(items):
            db.add(
                CollectionWorkItem(
                    batch_id=batch_id,
                    source="transparencia",
                    kind=kind,
                    identity=identity,
                    status=status,
                    position=position,
                    payload=payload,
                )
            )
    return parent_id, original_run_id, last_run_id


async def test_progress_uses_latest_federal_pass_and_shows_diagnostics(api_client, database):
    parent_id, original_id, latest_id = await _seed_federal_batch(database)
    progress = await api_client.get("/crawls/progress")
    page = BeautifulSoup(progress.text, "html.parser")
    stage_list = page.select_one(f"#coleta-{parent_id} .complete-stage-list")
    assert f"/crawls/{latest_id}/continue" in str(stage_list)
    assert f"/crawls/{original_id}/continue" not in str(stage_list)
    assert "2 de 4 concluídos" in stage_list.get_text()
    assert "1 aguardando nova tentativa" in stage_list.get_text()

    response = await api_client.get(f"/crawls/{original_id}/federal")
    assert response.status_code == 200
    assert "04/10/2026" in response.text
    assert "ZIP inválido" in response.text
    assert "Concluída sem registros" in response.text
    assert f"/crawls/{latest_id}/continue" in response.text


async def test_payment_and_dossier_keep_federal_totals_separate(api_client, database):
    await _seed_federal_batch(database)
    categories = [
        ("issued", False, False, "101.25"),
        ("issued", True, False, "400.00"),
        ("final", False, False, "81.30"),
        ("final", False, True, "104.00"),
    ]
    async with database.sessions.begin() as db:
        company = Company(
            cnpj=COMPANY_CNPJ,
            legal_name="Fornecedor federal",
            normalized_name="FORNECEDOR FEDERAL",
            fingerprint="federal-company-views",
        )
        contract = Contract(
            source="pncp",
            external_id="federal-contract-views",
            fingerprint="federal-contract-views",
        )
        db.add_all([company, contract])
        await db.flush()
        contract_id = contract.id
        for index, (role, intermediary, conflict, amount) in enumerate(categories):
            reference = f"federal-views-row-{index}"
            db.add(
                Payment(
                    source="transparencia",
                    external_id=reference,
                    fingerprint=reference,
                    source_url="https://portaldatransparencia.gov.br/despesas/pagamento/112",
                    document_number=reference,
                    company_id=company.id,
                    supplier_cnpj=company.cnpj,
                    federal_beneficiary_role=role,
                    federal_intermediary=intermediary,
                    federal_identity_conflict=conflict,
                    contract_id=contract_id,
                    payment_date=datetime.now(UTC),
                    phase="pagamento",
                    amount=Decimal(amount),
                )
            )

    for url in (
        f"/pagamentos?supplier_cnpj={COMPANY_CNPJ}",
        f"/empresas/{COMPANY_CNPJ}?section=payments",
        f"/empresas/{COMPANY_CNPJ}/sections/payments",
    ):
        response = await api_client.get(url)
        assert response.status_code == 200
        panel = BeautifulSoup(response.text, "html.parser").select_one("#cobertura-federal")
        text = panel.get_text()
        assert "101,25" in text and "400,00" in text and "81,30" in text
        assert "104,00" not in text and "185,30" not in text
        assert "2 de 4 concluídos · 2 sem registros" in text
        assert "1 de 1 concluídas · 1 sem registros" in text

    filtered = await api_client.get("/pagamentos?federal_type=intermediary")
    assert "federal-views-row-1" in filtered.text
    assert "federal-views-row-0" not in filtered.text

    detail = await api_client.get(f"/api/contracts/{contract_id}")
    assert detail.json()["payment_summary"]["paid_value"] == 101.25
