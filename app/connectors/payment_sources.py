"""Configured payment sources and their honest availability."""

from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings, get_settings


@dataclass(frozen=True, slots=True)
class PaymentSourceInfo:
    id: str
    name: str
    scope: str
    configured: bool
    mode: str
    note: str


def list_payment_sources(settings: Settings | None = None) -> list[PaymentSourceInfo]:
    """Return configured sources plus explicit placeholders for unknown adapters."""

    selected = settings or get_settings()
    configured = {item.strip().lower() for item in selected.payment_sources if item.strip()}
    sources: list[PaymentSourceInfo] = []
    if "transparencia" in configured:
        sources.append(
            PaymentSourceInfo(
                id="transparencia",
                name="Portal da Transparencia",
                scope="federal",
                configured=bool(selected.transparencia_api_key.strip()),
                mode="api",
                note=(
                    "Poder Executivo federal; requer TRANSPARENCIA_API_KEY."
                    if not selected.transparencia_api_key.strip()
                    else "Poder Executivo federal; token configurado."
                ),
            )
        )
    if "manual" in configured:
        sources.append(
            PaymentSourceInfo(
                id="manual",
                name="Importacao manual CSV/JSON",
                scope="estadual/municipal",
                configured=True,
                mode="file",
                note="Planilha/exportacao do portal com proveniencia por linha.",
            )
        )
    if "municipal" in configured:
        from app.connectors.municipal_payments.sources import (
            selected_municipal_sources,
            unknown_municipal_source_ids,
        )

        selected_municipal = selected_municipal_sources(selected)
        unknown_municipal = unknown_municipal_source_ids(selected)
        municipal_note = (
            f"{len(selected_municipal)} fonte(s) municipal(is) habilitada(s): "
            + ", ".join(item.name for item in selected_municipal)
            if selected_municipal
            else "Nenhuma fonte municipal habilitada em MUNICIPAL_PAYMENT_SOURCES."
        )
        if unknown_municipal:
            municipal_note += "; ignoradas: " + ", ".join(unknown_municipal)
        sources.append(
            PaymentSourceInfo(
                id="municipal",
                name="Portais municipais do Maranhao",
                scope="municipal",
                configured=bool(selected_municipal),
                mode="adapter",
                note=municipal_note,
            )
        )
    for source_id in sorted(configured - {"transparencia", "manual", "municipal"}):
        sources.append(
            PaymentSourceInfo(
                id=source_id,
                name=source_id,
                scope="estadual/municipal",
                configured=False,
                mode="adapter",
                note="Adaptador de portal nao implementado ou nao configurado.",
            )
        )
    return sources
