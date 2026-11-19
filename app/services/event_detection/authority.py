"""Conservative assertion checks shared by rules, AI and stored-data repair."""

from __future__ import annotations

import re
import unicodedata


def fold(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text.casefold()) if not unicodedata.combining(c)
    )


def assertion_diagnostic(text: str, title: str | None = None) -> str | None:
    """Return why a passage cannot establish a performed procurement act.

    Contextual patterns deliberately avoid banning isolated words such as
    'minuta', which may occur in a genuine decision about that document.
    """
    value = fold(text)
    heading = fold(title or "").replace("_", "-")
    if re.search(r"\bminuta[-\s]+(?:de[-\s]+)?(?:ata|contrato)", heading):
        return "template_document"
    if re.search(
        r"\b(?:constantes?|condicoes|conforme|nos termos).{0,100}\b(?:proposta adjudicada|licitacao homologada)\b",
        value,
    ):
        return "contractual_reference"
    if re.search(r"\b(?:em outro|no outro|de outro)\s+(?:processo|pregao|certame)\b", value):
        return "other_proceeding"
    statuses = sum(
        word in value
        for word in ("arrematante", "desclassificado", "inabilitado", "classificado", "rejeitado")
    )
    if (statuses >= 4 and (";" in value or "legenda" in value)) or re.search(
        r"\blegenda\s*[:\-]", value
    ):
        return "status_legend"
    if re.search(
        r"\b(?:sera|serao|podera|poderao|devera|deverao)\s+(?:ser\s+)?"
        r"(?:inabilitad|desclassificad|classificad|adjudicad|homologad|rejeitad|declarad[ao]s?\s+vencedor)",
        value,
    ):
        return "future_rule"
    if re.search(
        r"\b(?:caso|se)\s+(?:o |a )?(?:licitante|empresa|proposta).{0,90}\b(?:inabilitad|desclassificad|declarad[ao]\s+vencedor)",
        value,
    ) or re.search(
        r"\b(?:se\s+(?:for|vier\s+a\s+ser)|caso\s+(?:seja|venha\s+a\s+ser))\s+"
        r"(?:inabilitad|desclassificad|classificad|adjudicad|homologad|declarad[ao]s?\s+vencedor)",
        value,
    ):
        return "conditional_rule"
    if re.search(
        r"\b(?:alega|alegou|sustenta|sustentou|requer|requereu)\b.{0,100}\b(?:inabilitacao|desclassificacao|declarad[ao]\s+vencedor[ao]?)\b",
        value,
    ) and not re.search(r"\b(?:decido|decidiu|indefiro|defiro|decisao)\b", value):
        return "unconfirmed_allegation"
    return None


def nonaward_supplier(name: str | None) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", fold(name or "")).strip()
    return normalized in {
        "nao homologado",
        "nao homologada",
        "nao adjudicado",
        "nao adjudicada",
        "sem vencedor",
        "nao informado",
        "deserto",
        "fracassado",
    }
