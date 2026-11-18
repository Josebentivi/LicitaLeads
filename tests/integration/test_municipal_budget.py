"""Budget queries use current financial facts and exactly identified, archived commitments."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete, select, update

from app.connectors.base import RawPayment, RawSourceRecord
from app.models import Company, Payment
from app.models.entities import MunicipalBudgetEntry
from app.services.ingestion.municipal_payments import MunicipalPaymentIngestionService
from app.services.municipal_budget import (
    BudgetResponse,
    backfill_budget_projection,
    projection_rows,
)

from .test_municipal_payments import MultiRecordMunicipalConnector, _settings


def test_ambiguous_missing_wrong_entity_or_wrong_year_cannot_classify():
    scope = {"Listagem": "DespesasGerais", "Empresa": "18", "Exercicio": "2026"}
    row = {"PKEMP": "11", "EMPRESA": "18", "FUNCAONOME": "Cultura"}

    def listing(params, rows):
        return BudgetResponse(uuid4(), "municipal:timon", params, rows, 200)

    assert (
        list(
            projection_rows(
                [
                    listing(
                        scope, [row, row, {**row, "PKEMP": "2", "EMPRESA": "1"}, {"PKEMP": "4"}]
                    ),
                    listing({**scope, "Ano": "2025"}, [row]),
                    listing({**scope, "Listagem": "EmpenhosDespesas_Pago_PorNumeroEmpenho"}, [row]),
                ]
            )
        )
        == []
    )


async def test_municipal_budget_current_and_archived_evidence(database, api_client):
    source = "municipal:imperatriz"
    scope = {"Empresa": "2", "Exercicio": "2026", "Ano": "2026"}

    def response(listing, rows):
        return RawSourceRecord.build(
            source=source,
            endpoint="https://portal.test/despesas",
            request_parameters={**scope, "Listagem": listing},
            raw_payload=rows,
            http_status=200,
            collected_at=datetime.now(UTC),
        )

    listing = response(
        "DespesasGerais",
        [
            {
                "PKEMP": "1",
                "FUNCAO": "13",
                "FUNCAONOME": "Cultura",
                "PROC": "10/2026",
                "EMPRESA": "2",
                "HISTORICO": "DADO PRIVADO",
                "NUMLICIT": "123.456.789-01/2026",
            },
            {"PKEMP": "2", "FUNCAO": "12", "FUNCAONOME": "Educação", "EMPRESA": "2"},
        ],
    )
    detail = response("EmpenhosDespesas_Pago_PorNumeroEmpenho", [{"PKEMP": "2"}])
    records = [
        RawPayment(
            source=source,
            source_record_ref=record.record_key,
            external_id=external_id,
            status=status,
            commitment_external_id=identity,
            commitment_number="1",
            amount=Decimal("42.00"),
            phase=phase,
            supplier_name="Pessoa privada",
            commitment_details_complete=True,
        )
        for record, external_id, status, identity, phase in [
            (
                listing,
                "imperatriz:e2:2026:1:agregado",
                "agregado_por_empenho",
                "imperatriz:e2:2026:1",
                "pago_agregado",
            ),
            (
                detail,
                "imperatriz:e2:2026:2:1:1:1:detalhe",
                "detalhe_empenho",
                "imperatriz:e2:2026:2",
                "pago",
            ),
        ]
    ]
    service = MunicipalPaymentIngestionService(
        _settings(),
        session_factory=database.sessions,
        connector=MultiRecordMunicipalConnector(records, [listing, detail]),
    )
    try:
        summary = await service.run("imperatriz", year=2026)
        assert summary.payments_created == 2
    finally:
        await service.aclose()
    filters = dict(
        municipality="imperatriz",
        budget_function="Cultura",
        fiscal_year="2026",
        match_status="review",
    )

    async def assertions():
        result = (await api_client.get("/api/payments", params=filters)).json()
        assert result["total"] == 1
        fact = result["items"][0]
        assert fact["supplier_cnpj"] is None and fact["supplier_name"] == "Pessoa privada"
        history = await api_client.get(f"/api/history/payment/{fact['id']}")
        assert history.status_code == 200 and "Pessoa privada" in history.text
        assert (
            fact["payment_date"] is None
            and fact["match_status"] == "unlinked"
            and fact["amount"] == 42
        )
        assert fact["municipal_budget"]["function_code"] == "13"
        assert fact["source_record_id"] == fact["municipal_budget"]["classification_record_id"]
        assert fact["municipal_budget"]["portal_bid"] is None
        assert fact["municipal_budget"]["portal_process"] == "10/2026"
        html = (await api_client.get("/pagamentos", params=filters)).text
        assert "Sem data individual" in html and "imperatriz:e2:2026:1" in html
        assert "Hash da resposta" in html and listing.response_hash in html
        assert (
            "10/2026" in html and "DADO PRIVADO" not in html and "<td>Pessoa privada</td>" in html
        )
        grouped = await api_client.get("/pagamentos", params={**filters, "view": "suppliers"})
        assert grouped.status_code == 200 and "Pessoa privada" not in grouped.text
        assert "0 fornecedor(es) com CNPJ confirmado" in grouped.text
        result = (
            await api_client.get("/api/payments", params={**filters, "budget_function": "Educação"})
        ).json()
        assert result["total"] == 1
        fact = result["items"][0]
        assert fact["source_record_id"] != fact["municipal_budget"]["classification_record_id"]
        for key, value in [
            ("municipality", "caxias"),
            ("fiscal_year", "2025"),
            ("match_status", "linked"),
        ]:
            assert (await api_client.get("/api/payments", params={**filters, key: value})).json()[
                "total"
            ] == 0

    await assertions()
    async with database.engine.begin() as connection:
        await connection.execute(delete(MunicipalBudgetEntry))
        await connection.run_sync(backfill_budget_projection)
        await connection.run_sync(backfill_budget_projection)
    await assertions()
    async with database.engine.begin() as connection:
        await connection.execute(
            update(MunicipalBudgetEntry)
            .where(MunicipalBudgetEntry.function_code == "13")
            .values(portal_bid="123.456.789-01/2026")
        )
    await assertions()
    async with database.sessions() as db, db.begin():
        company = Company(
            cnpj="52246769000198", legal_name="Empresa do recorte", normalized_name="empresa"
        )
        db.add(company)
        await db.flush()
        for row in await db.scalars(select(Payment)):
            row.company_id, row.supplier_cnpj = company.id, company.cnpj
    grouped = await api_client.get(
        "/pagamentos", params={"municipality": "imperatriz", "view": "suppliers"}
    )
    assert grouped.status_code == 200
    assert (
        "1 fornecedor(es) com CNPJ confirmado" in grouped.text and "2 registro(s)" in grouped.text
    )
    from bs4 import BeautifulSoup

    suppliers_page = BeautifulSoup(grouped.text, "html.parser")
    link = suppliers_page.find("a", string="Empresa do recorte")
    assert "section=payments" in link["href"]
    records_link = suppliers_page.find("a", string="2 registro(s)")
    assert (
        "municipality=imperatriz" in records_link["href"]
        and "supplier_cnpj=52246769000198" in records_link["href"]
    )
    dossier = await api_client.get(link["href"])
    dossier_page = BeautifulSoup(dossier.text, "html.parser")
    assert dossier_page.select_one("details#company-payments[open]")
    assert "Pagamentos · voltar aos filtros" in dossier.text
    back = dossier_page.find("a", string=lambda v: v and "voltar aos filtros" in v)
    assert "municipality=imperatriz" in back["href"] and "view=suppliers" in back["href"]
    async with database.sessions() as db, db.begin():
        aggregate = await db.scalar(select(Payment).where(Payment.status == "agregado_por_empenho"))
        aggregate.is_current = False
    assert (await api_client.get("/api/payments", params=filters)).json()["total"] == 0
