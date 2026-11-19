from datetime import UTC, date, datetime

from bs4 import BeautifulSoup
from sqlalchemy import event, func, select

from app.models import (
    Company,
    CompanyContact,
    FieldObservation,
    Participant,
    PriceRegistry,
    Procurement,
    ProcurementSource,
    SourceRecord,
)
from app.services.company_overview import CompanyOverviewService


async def test_company_corporate_profile_keeps_manual_provenance(api_client, database):
    cnpj = "00000000000191"
    url = f"/empresas/{cnpj}"
    async with database.sessions() as session, session.begin():
        session.add(Company(cnpj=cnpj, normalized_name=cnpj))
    initial = await api_client.get(url)
    assert initial.status_code == 200
    assert "4 de 5 campos" in initial.text

    name_data = {
        "field": "legal_name",
        "value": "Empresa Confirmada Ltda.",
        "source_url": "https://empresa.example/registro",
        "corporate": "true",
    }
    assert (await api_client.post(f"{url}/contatos", data=name_data)).status_code == 303
    name_data["value"] = "Nome diferente"
    assert (await api_client.post(f"{url}/contatos", data=name_data)).status_code == 422
    csv = (
        "cnpj,contact_type,contact_value,source_url,is_corporate\n"
        f"{cnpj},address,Rua Exemplo 10,https://empresa.example/contato,true\n"
    )
    for outcome in ("created", "updated"):
        response = await api_client.post(
            "/api/contacts/import", files={"file": ("contatos.csv", csv, "text/csv")}
        )
        assert response.status_code == 200
        assert response.json()[outcome] == 1
    bad_contact = await api_client.post(
        f"{url}/contatos",
        data={
            "field": "phone",
            "value": "98987654321",
            "source_url": "https://empresa.example/contato",
        },
    )
    assert bad_contact.status_code == 422

    after = await api_client.get(url)
    profile = BeautifulSoup(after.text, "html.parser").select_one("#dados-corporativos")
    assert profile is not None
    assert "Empresa Confirmada Ltda." in profile.text
    assert "Rua Exemplo 10" in profile.text
    assert "2 de 5 campos" in profile.text
    assert profile.select_one('a[href="https://empresa.example/registro"]')
    assert profile.select_one('a[href="https://empresa.example/contato"]')
    async with database.sessions() as session:
        assert await session.scalar(select(func.count(SourceRecord.id))) == 2
        contact = (await session.scalars(select(CompanyContact))).one()
        assert contact.source_record_id is not None
        source = await session.get_one(SourceRecord, contact.source_record_id)
        assert source.raw_payload["contact_value"] == "Rua Exemplo 10"
        observed = (await session.scalars(select(FieldObservation))).one()
        assert observed.field_name == "legal_name" and observed.source_record_id


async def test_manual_form_rejects_malformed_contact_before_storing_source(api_client, database):
    cnpj = "00000000000191"
    async with database.sessions() as session, session.begin():
        session.add(Company(cnpj=cnpj, normalized_name=cnpj))
    for contact_type, value in (
        ("email", "sem e-mail"),
        ("email", "financeiro@empresa..example"),
        ("phone", "123"),
        ("phone", "(98) 98765-4321 / 99999-4321"),
    ):
        response = await api_client.post(
            f"/empresas/{cnpj}/contatos",
            data={
                "field": contact_type,
                "value": value,
                "source_url": "https://empresa.example/contato",
                "corporate": "true",
            },
        )
        assert response.status_code == 422
    async with database.sessions() as session:
        assert await session.scalar(select(func.count(CompanyContact.id))) == 0
        assert await session.scalar(select(func.count(SourceRecord.id))) == 0


async def test_csv_locks_exact_companies_in_order_without_reordering_input(api_client, database):
    first, second = "00000000000191", "11222333000181"
    async with database.sessions() as session, session.begin():
        session.add_all(
            [
                Company(cnpj=first, normalized_name=first),
                Company(cnpj=second, normalized_name=second),
            ]
        )

    statements = []

    def observe_company_query(connection, cursor, statement, parameters, context, executemany):
        if "FROM companies" in statement:
            statements.append((statement, parameters))

    engine = database.engine.sync_engine
    event.listen(engine, "before_cursor_execute", observe_company_query)
    try:
        source = "https://empresa.example/contato"
        data = (
            "cnpj,contact_type,contact_value,source_url,is_corporate\n"
            f"{second},phone,+55 (98) 3333-4444 ramal 12,{source},true\n"
            f"{first},email,sem e-mail,{source},true\n"
            f"{first},phone,0800 123 4567,{source},true\n"
            f"11111111111111,email,setor+licita@empresa.example,{source},true\n"
            f"{first},phone,0800 123 4567,{source},true\n"
            f"{second},whatsapp,123,{source},true\n"
            f"{second},email,setor+licita@empresa.example,{source},true\n"
            f"{second},whatsapp,(98) 98765-4321,{source},true\n"
        )
        result = await api_client.post(
            "/api/contacts/import", files={"file": ("contatos.csv", data, "text/csv")}
        )
    finally:
        event.remove(engine, "before_cursor_execute", observe_company_query)
    assert result.status_code == 200
    payload = result.json()
    assert (payload["created"], payload["updated"], payload["skipped"]) == (4, 1, 3)
    assert [entry["row"] for entry in payload["errors"]] == [3, 5, 7]
    ((query, keys),) = statements
    assert "ORDER BY companies.cnpj" in query
    assert keys == (first, second)
    async with database.sessions() as session:
        contacts = list(await session.scalars(select(CompanyContact.contact_value)))
        assert set(contacts) == {
            "+55 (98) 3333-4444 ramal 12",
            "0800 123 4567",
            "setor+licita@empresa.example",
            "(98) 98765-4321",
        }
        assert await session.scalar(select(func.count(SourceRecord.id))) == 4


async def test_csv_rejects_overlong_website_domain_without_rolling_back_other_rows(
    api_client, database
):
    cnpj = "00000000000191"
    async with database.sessions.begin() as db:
        db.add(Company(cnpj=cnpj, normalized_name=cnpj))
    host = ".".join(["a" * 63] * 4 + ["example"])
    origin = "https://empresa.example/contato"
    csv = (
        "cnpj,contact_type,contact_value,source_url,is_corporate\n"
        f"{cnpj},website,https://{host},{origin},true\n"
        f"{cnpj},email,licitacoes@empresa.example,{origin},true\n"
    )
    response = await api_client.post(
        "/api/contacts/import", files={"file": ("contatos.csv", csv, "text/csv")}
    )
    assert response.status_code == 200
    result = response.json()
    assert (result["created"], result["skipped"]) == (1, 1)
    assert result["errors"][0]["row"] == 2
    async with database.sessions() as db:
        assert (await db.scalars(select(Company))).one().domain is None
        assert (await db.scalars(select(CompanyContact))).one().contact_value == (
            "licitacoes@empresa.example"
        )
        assert await db.scalar(select(func.count(SourceRecord.id))) == 1


async def test_company_history_and_official_browser_links(api_client, database):
    cnpj = "00000000000191"
    control = f"{cnpj}-1-000007/2025"
    technical = "https://pncp.gov.br/api/consulta/v1/orgaos/empresa/compras/2025/7"
    published = "https://compras.example/compra/2025-7"
    async with database.sessions() as session, session.begin():
        company = Company(cnpj=cnpj, normalized_name="EMPRESA")
        procurement = Procurement(
            source="pncp",
            external_id=control,
            pncp_control_number=control,
            source_url=technical,
            publication_at=datetime(2025, 7, 1, 2, 59, tzinfo=UTC),
        )
        source = SourceRecord(
            source="pncp",
            endpoint=technical,
            response_hash="1" * 64,
            raw_payload={
                "data": [
                    {
                        "numeroControlePNCP": f"{cnpj}-1-000008/2025",
                        "linkSistemaOrigem": "https://compras.example/outro",
                    },
                    {"numeroControlePNCP": control, "linkSistemaOrigem": published},
                ]
            },
        )
        session.add_all([company, procurement, source])
        await session.flush()
        procurement_id = procurement.id
        session.add_all(
            [
                ProcurementSource(
                    procurement_id=procurement.id,
                    source_record_id=source.id,
                    source="pncp",
                    external_id=control,
                    source_url=technical,
                ),
                Participant(company_id=company.id, procurement_id=procurement.id, source="pncp"),
                Participant(
                    company_id=company.id, procurement_id=procurement.id, source="document"
                ),
            ]
        )
        await session.flush()
        service = CompanyOverviewService(session, company)
        history = await service.participation_history(
            "semestre", timezone="America/Sao_Paulo", today=date(2026, 10, 31)
        )
        nonempty = [period for period in history if period["total"]]
        assert len(nonempty) == 1
        assert nonempty[0]["title"] == "1º semestre de 2025"
        assert nonempty[0]["total"] == 1
        assert "company_participation_only=true" in nonempty[0]["href"]

    detail = await api_client.get(
        f"/empresas/{cnpj}/sections/procurements/{procurement_id}", headers={"HX-Request": "true"}
    )
    assert detail.status_code == 200
    assert "https://pncp.gov.br/app/editais/00000000000191/2025/7" in detail.text
    assert published in detail.text
    assert "https://compras.example/outro" not in detail.text
    assert technical in detail.text and "Consultar dados técnicos da fonte" in detail.text


async def test_capabilities_localize_the_page_without_changing_api_codes(api_client):
    response = await api_client.get("/source-capabilities")
    assert response.status_code == 200
    assert ">Sem cobertura nesta integração<" in response.text
    assert ">Somente em documentos<" in response.text
    assert ">not_supported<" not in response.text
    api = await api_client.get("/api/source-capabilities")
    assert any(
        row["availability"] == "not_supported"
        for source in api.json()
        for row in source["capabilities"]
    )


async def test_price_registry_explains_absent_values_without_inventing_rows(api_client, database):
    async with database.sessions() as session, session.begin():
        registry = PriceRegistry(source="pncp", external_id="sem-detalhes", registry_number="7")
        session.add(registry)
        await session.flush()
        registry_id = registry.id

    response = await api_client.get(f"/atas/{registry_id}")
    assert response.status_code == 200
    assert "<dd>PNCP</dd>" in response.text
    assert "Não informado" in response.text
    assert "Nenhum item no acervo desta ata" in response.text
    assert "/ajuda#atas" in response.text
