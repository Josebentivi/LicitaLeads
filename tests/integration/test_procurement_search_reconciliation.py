"""Legacy text recovery keeps the exact published row and its audit link."""

from sqlalchemy import select

from app.api.serialization import model_dict
from app.models import FieldChange, Procurement, ProcurementSource, SourceRecord
from app.models.entities import ProcurementItem
from app.repositories.procurements import ProcurementRepository
from app.services.participation_reconciliation import IntegrityReport
from app.services.procurement_search import ObjectScope
from app.services.procurement_search_reconciliation import inspect_legacy_search_text

from .test_api_leads import _full_graph


async def test_search_text_recovery_only_uses_linked_procurement_and_exact_item(database):
    ids = await _full_graph(database)
    async with database.sessions() as db, db.begin():
        procurement = await db.get_one(Procurement, ids.procurement)
        item = await db.get_one(ProcurementItem, ids.item)
        item.description = "Frasco de 1 litro"
        source = await db.scalar(
            select(ProcurementSource).where(ProcurementSource.procurement_id == procurement.id)
        )
        source.pncp_control_number = procurement.pncp_control_number
        header = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
            response_hash="1" * 64,
            raw_payload={
                "data": [
                    {
                        "numeroControlePNCP": "00000000000191-1-000088/2026",
                        "informacaoComplementar": "Complemento de outro processo",
                    },
                    {
                        "numeroControlePNCP": procurement.pncp_control_number,
                        "informacaoComplementar": "Entrega refrigerada",
                    },
                ]
            },
        )
        items = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/api/pncp/v1/orgaos/00000000000191/compras/2026/88/itens",
            response_hash="2" * 64,
            raw_payload={
                "data": [
                    {"numeroItem": 1, "descricao": "Item vizinho"},
                    {
                        "numeroItem": 3,
                        "descricao": "Detergente",
                        "informacaoComplementar": item.description,
                    },
                ]
            },
        )
        db.add_all([header, items])
        await db.flush()
        source.source_record_id = header.id
        item.source_record_id = items.id
        await db.flush()

        invalid_link = IntegrityReport()
        await inspect_legacy_search_text(db, item, apply=True, report=invalid_link)
        assert item.summary_description is None
        items.endpoint = items.endpoint.replace("/88/", "/99/")

        dry_run = IntegrityReport()
        for record in (procurement, item):
            await inspect_legacy_search_text(db, record, apply=False, report=dry_run)
        assert dry_run.totals == {"search_text_requiring_repair": 2}
        assert procurement.object_complement is None and item.summary_description is None

        applied = IntegrityReport()
        for record in (procurement, item):
            await inspect_legacy_search_text(db, record, apply=True, report=applied)
        await db.flush()
        assert procurement.object_complement == "Entrega refrigerada"
        assert item.summary_description == "Detergente"
        assert item.description == "Frasco de 1 litro"
        audit = await db.scalars(select(FieldChange))
        assert {change.field_name: change.source_record_id for change in audit} == {
            "object_complement": header.id,
            "summary_description": items.id,
        }

        repository = ProcurementRepository(db)
        for term in ("detergente", "frasco", "entrega refrigerada"):
            result = await repository.list_filtered(object_scope=ObjectScope.build(term))
            assert [row.id for row in result.items] == [procurement.id]
        assert (
            await repository.list_filtered(object_scope=ObjectScope.build("item vizinho"))
        ).total == 0
        assert (await repository.list_filtered(object_scope=ObjectScope.build("pregão"))).total == 0
        assert (await repository.list_filtered(query="Pregão")).total == 1


async def test_search_and_collection_agree_on_unicode_and_field_boundaries(database):
    scope = ObjectScope.build("camera")
    descriptions = ("camera", "CÂMERA", "Ca\u0302mera", "précamera", "ca-me-ra")
    async with database.sessions() as db, db.begin():
        for number, description in enumerate(descriptions):
            db.add(
                Procurement(source="pncp", external_id=str(number), object_description=description)
            )
        split = Procurement(source="pncp", external_id="split", object_description="CÂMERA")
        split.object_complement = "DIGITAL"
        db.add(split)
        await db.flush()
        db.add(
            ProcurementItem(
                procurement_id=split.id,
                item_number="1",
                description="Imagem",
                summary_description="Ótica",
            )
        )
        await db.flush()
        repository = ProcurementRepository(db)
        rows = await repository.list_filtered(object_scope=scope)
        expected = {
            str(i) for i, description in enumerate(descriptions) if scope.matches_text(description)
        }
        assert {row.external_id for row in rows.items} == expected | {"split"}
        for phrase in ("camera digital", "imagem otica"):
            assert (
                await repository.list_filtered(object_scope=ObjectScope.build(phrase))
            ).total == 0
        assert (await repository.list_filtered(object_scope=ObjectScope.build("otica"))).total == 1
        assert not any(name.endswith("_search") for name in model_dict(split))
