"""Export the same evidence-bound selection used in the analyst workspace."""

import csv
from collections.abc import AsyncIterator
from datetime import datetime
from io import StringIO

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Company, Deadline, Evidence, Lead, Procurement, ProcurementEvent
from app.services.opportunity_workspace import (
    OpportunityFilters,
    deadline_indication,
    opportunity_query,
)
from app.services.participation_integrity import deadline_current, lead_current


async def opportunity_csv(
    session: AsyncSession, filters: OpportunityFilters, *, cutoff: datetime
) -> AsyncIterator[bytes]:
    """Use one cursor so validity, source and deadline come from the same statement."""
    statement = (
        opportunity_query(filters, now=cutoff)
        .join(Lead.company)
        .join(Lead.procurement)
        .join(Lead.triggering_event)
        .outerjoin(Lead.deadline)
        .outerjoin(ProcurementEvent.evidence)
        .with_only_columns(
            Lead.id.label("lead_id"),
            func.coalesce(Company.legal_name, Company.normalized_name).label("empresa"),
            Company.cnpj.label("cnpj"),
            Procurement.agency_name.label("orgao"),
            func.coalesce(Procurement.pncp_control_number, Procurement.purchase_number).label(
                "processo"
            ),
            ProcurementEvent.event_type.label("evento"),
            Lead.reason_summary.label("motivo"),
            Deadline.explicit_deadline_at.label("prazo_explicito"),
            Deadline.estimated_deadline_at.label("prazo_estimado"),
            Deadline.status.label("status_prazo_gravado"),
            Lead.score,
            Lead.lead_status.label("status_lead"),
            func.coalesce(Evidence.source_url, ProcurementEvent.source_url).label("fonte"),
            Evidence.id.label("evidencia_id"),
            Evidence.document_id.label("documento_id"),
            Evidence.text_excerpt.label("trecho_evidencia"),
            Evidence.page_number.label("pagina"),
            Evidence.locator.label("localizador"),
            func.coalesce(Evidence.source_record_id, ProcurementEvent.source_record_id).label(
                "registro_origem_id"
            ),
            lead_current().label("vigente"),
            Lead.deadline.has(deadline_current()).label("prazo_vigente"),
        )
        .execution_options(yield_per=250)
    )
    result = await session.stream(statement)
    buffer = StringIO(newline="")
    fields = [key for key in result.keys() if key not in ("vigente", "prazo_vigente")]
    fields.extend(("validade", "status_prazo", "evidencia_local", "emissao_utc"))
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    try:
        yield buffer.getvalue().encode("utf-8-sig")
        async for values in result.mappings():
            row = dict(values)
            current = row.pop("vigente")
            current_deadline = row.pop("prazo_vigente")
            row["validade"] = "Atual" if current else "Histórica ou invalidada"
            row["status_prazo"] = deadline_indication(
                row["prazo_explicito"] or row["prazo_estimado"],
                explicit=bool(row["prazo_explicito"]),
                current=bool(current and current_deadline),
                now=cutoff,
                timezone=filters.timezone,
            )
            row["evidencia_local"] = (
                f"/evidencias/{row['evidencia_id']}" if row["evidencia_id"] else None
            )
            row["emissao_utc"] = cutoff.isoformat()
            for key, value in row.items():
                if isinstance(value, str) and (
                    value.lstrip().startswith(("=", "+", "-", "@"))
                    or value.startswith(("\t", "\r", "\n"))
                ):
                    row[key] = "'" + value
            buffer.seek(0)
            buffer.truncate()
            writer.writerow(row)
            yield buffer.getvalue().encode("utf-8")
    finally:
        await result.close()
