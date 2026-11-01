"""Read-only, paginated changes for an explicit set of public domain entities."""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.serialization import model_dict
from app.dependencies import get_db
from app.models import Contract, FieldChange, Payment, PriceRegistry, Procurement, SourceRecord

router = APIRouter(prefix="/history", tags=["history"])
EntityType = Literal["procurement", "price_registry", "contract", "payment"]
MODELS = {
    "procurement": Procurement,
    "price_registry": PriceRegistry,
    "contract": Contract,
    "payment": Payment,
}
DECISIONS = {
    "review_required": "Extração documental pendente de revisão",
    "review_resolved": "Pendência documental resolvida",
    "integrity_repair": "Verificação documental auditada",
    "created": "Informação registrada",
    "updated": "Atualização da fonte",
    "conflict_applied": "Última consulta aplicada — divergência para revisão",
    "stale_ignored": "Versão antiga ignorada",
    "observation_ignored": "Consulta anterior ignorada",
}
FIELD_LABELS = {
    "monetary_review": "Diagnóstico dos valores documentais",
    "verification_status": "Verificação da atribuição",
    "proposal_value": "Valor da proposta",
    "item_id": "Vínculo com item oficial",
    "status": "Situação",
    "current_phase": "Fase estimada",
    "estimated_value": "Valor estimado",
    "global_value": "Valor global",
    "initial_value": "Valor inicial",
    "accumulated_value": "Valor acumulado",
    "total_value": "Valor total",
    "unit_value": "Valor unitário",
    "amount": "Valor",
    "quantity": "Quantidade",
    "unit": "Unidade",
    "description": "Descrição",
    "object_description": "Objeto",
    "title": "Título",
    "is_srp": "Registro de preços",
    "allows_adhesion": "Permite adesão",
    "is_adhesion": "Originado de adesão",
    "agency_name": "Órgão",
    "agency_cnpj": "CNPJ do órgão",
    "supplier_name": "Fornecedor",
    "supplier_cnpj": "CNPJ do fornecedor",
    "payment_date": "Data do pagamento",
    "valid_from": "Início da vigência",
    "valid_until": "Fim da vigência",
    "signed_at": "Assinatura",
    "published_at": "Publicação",
    "publication_at": "Publicação",
    "proposal_end_at": "Fim das propostas",
    "proposal_start_at": "Início das propostas",
    "session_start_at": "Início da sessão",
    "registry_number": "Número da ata",
    "contract_number": "Número do contrato",
    "process_number": "Processo",
    "document_number": "Documento",
    "commitment_number": "Empenho",
    "is_current": "Representação atual",
    "item_number": "Número do item",
    "participation_role": "Papel documentado",
    "final_value": "Valor final",
    "status_code": "Situação documentada",
    "is_active": "Documento ativo",
    "value_delta": "Valor do termo",
    "balance": "Saldo",
    "quantity_registered": "Quantidade registrada",
    "quantity_committed": "Quantidade comprometida",
}


async def history_page(
    db: AsyncSession,
    entity_type: str,
    entity_id: UUID,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    model = MODELS.get(entity_type)
    if model is None:
        raise HTTPException(422, "Tipo de histórico inválido")
    if await db.get(model, entity_id) is None:
        raise HTTPException(404, "Registro não encontrado")
    conditions = (FieldChange.parent_type == entity_type, FieldChange.parent_id == entity_id)
    total = int(
        await db.scalar(select(func.count()).select_from(FieldChange).where(*conditions)) or 0
    )
    rows = list(
        (
            await db.scalars(
                select(FieldChange)
                .where(*conditions)
                .order_by(
                    FieldChange.observed_at.desc(),
                    FieldChange.id.desc(),
                )
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        ).all()
    )
    source_ids = {row.source_record_id for row in rows if row.source_record_id}
    sources = (
        {
            row.id: row
            for row in (
                await db.scalars(select(SourceRecord).where(SourceRecord.id.in_(source_ids)))
            ).all()
        }
        if source_ids
        else {}
    )
    items = []
    for row in rows:
        item = model_dict(row)
        item["field_label"] = FIELD_LABELS.get(row.field_name, row.field_name.replace("_", " "))
        item["previous_display"] = (
            "Sim"
            if row.previous_value is True
            else "Não"
            if row.previous_value is False
            else row.previous_value
        )
        item["received_display"] = (
            "Sim"
            if row.received_value is True
            else "Não"
            if row.received_value is False
            else row.received_value
        )
        item["decision_label"] = DECISIONS.get(row.decision, row.decision)
        source = sources.get(row.source_record_id) if row.source_record_id else None
        parts = urlsplit(source.endpoint) if source else None
        item["source_url"] = (
            urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
            if parts
            and parts.scheme in {"http", "https"}
            and not parts.username
            and not parts.password
            else None
        )
        items.append(item)
    return {"items": items, "page": page, "page_size": page_size, "total": total}


@router.get("/{entity_type}/{entity_id}")
async def get_history(
    entity_type: EntityType,
    entity_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
) -> dict[str, object]:
    return await history_page(db, entity_type, entity_id, page, page_size)
