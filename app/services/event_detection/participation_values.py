"""Deterministic, document-scoped attribution of bidders and monetary totals.

Numbers in a cost quotation are not bids. Local lot/item numbers are retained
as published and are deliberately never interpreted as PNCP item identities.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

from app.services.event_detection.detector import (
    _CNPJ_TOKEN,
    _NEGATED_PARTICIPANT_ROLE,
    _RULE_LANGUAGE,
    _cnpjs,
    _document_base_confidence,
    _fold,
)
from app.services.event_detection.types import DocumentEvidence
from app.services.identifiers import is_valid_cnpj, normalize_cnpj

VERSION = "participation-values-1"
_LOT = re.compile(r"\blote\s*(?:n[ºo°.]?\s*)?[:#-]?\s*(\d+)\b", re.I)
_LOCAL_ITEM = re.compile(r"\bitem\s*(?:n[ºo°.]?\s*)?[:#-]?\s*(\d+)(?!\d|\.\d)\b", re.I)
_MONEY = re.compile(r"R\$\s*(-?\s*(?:\d{1,3}(?:[. \t]\d{3})+|\d+),\d{2,4})(?!\d)", re.I)
_SUBJECT = re.compile(
    r"(?:\b(?:licitantes?|participantes?|proponentes?|arrematantes?)\b\s*[:\-]?\s*|"
    r"\b(?:fornecedor|empresa)\s+(?:vencedor[ao]?|adjudicad[oa])\s*[:\-]?\s*|"
    r"\bpropostas?\s+(?:apresentadas?\s+)?(?:da|pela)\s+empresa\s+|"
    r"\bempresa\s+licitante\s+)[^:;\n]{0,180}$",
    re.I,
)
_ANCILLARY = re.compile(
    r"\b(?:fornecedor(?:a)?\s+de\s+(?:custo|origem)|"
    r"fabricante|seguradora|cliente|nota\s+fiscal|cotacao|documento\s+de\s+custo)\b"
)
_CUES = re.compile(
    r"\b(?:propost[oa](?:\s+(?:inicial|readequad[ao]|final))?|"
    r"adjudicad[oa]|homologad[oa]|adjudico|homologo|adjudicou|homologou|valor\s+final|estimad[oa]|or[cç]ad[oa]|custo|contrato|empenho|pagamento|"
    r"garantia|seguro|faturamento|limite|orcamento|unitari[oa]|multa|nota\s+fiscal|"
    r"planilha|preco\s+(?:de\s+compra|minimo|de\s+custo))\b"
)
_COLUMN = re.compile(
    r"^[^\S\n]*(?:(?:(?:valor|pre[cç]o)[^\S\n]+)?unit[aá]ri[oa](?:[^\S\n]+(?:adjudicad[oa]|or[cç]ad[oa]|homologad[oa]))?|item|lote)[.:]?[^\S\n]*$",
    re.I | re.M,
)
_TOTAL_NEAR = re.compile(
    r"\b(?:valor\s+(?:total|global)(?:\s+d[ao]\s+(?:proposta|fornecedor|empresa))?(?:\s+(?:adjudicad[oa]|homologad[oa]|propost[oa]))?|propost[ao](?:\s+(?:inicial|readequad[ao]|final))?\s+(?:total|global)|total\s+(?:geral|d[ao]\s+proposta|adjudicad[oa]|homologad[oa]))\s*(?:(?:e|de|no\s+valor\s+de)\s*)?[:=\-]?\s*$"
)
_GRAND_TOTAL_NEAR = re.compile(
    r"\b(?:total\s+geral|valor\s+global\s+d[ao]\s+(?:proposta|fornecedor|empresa))\s*(?:(?:e|de)\s*)?[:=\-]?\s*$"
)
_AWARD_OWNER = re.compile(
    r"\b(?:homologo|adjudico|homologou|adjudicou|homologad[oa]|adjudicad[oa])\b[\s\S]{0,500}\bem\s+favor\s+(?:d[aeo]\s+(?:empresa|fornecedor)|de\s*:\s*fornecedor)\b"
)
_APPEAL_OWNER = re.compile(
    r"\b(?:recursos?|contrarrazoes|intenc(?:ao|oes)\s+de\s+recurso)\b[\s\S]{0,100}\b(?:interpost[oa]s?|apresentad[oa]s?)\b[\s\S]{0,250}\bpelas?\s+empresas?\b"
)
_BIDDER_TABLE = re.compile(
    r"(?:validade\s+das\s+propostas\s+fornecedor\s+(?:cpf\s*/\s*)?cnpj\s+validade|intencoes\s+de\s+recurso\s+cnpj\s+data\s+de\s+envio\s+intencao(?:\s+julgamento)?)"
)


def reference_status(text: str, cnpj: str, document_type: str | None = None) -> str:
    """Classify the *reference*, not the company's activity outside this excerpt."""
    matches = [m for m in _CNPJ_TOKEN.finditer(text) if normalize_cnpj(m.group()) == cnpj]
    if not matches:
        return "review"
    statuses = []
    for match in matches:
        extended = _fold(text[max(0, match.start() - 1500) : match.start()])
        prefix = text[max(0, match.start() - 300) : match.start()]
        prior_identifiers = list(_CNPJ_TOKEN.finditer(prefix))
        if prior_identifiers:
            prefix = prefix[prior_identifiers[-1].end() :]
        folded = _fold(prefix)
        if re.search(r"\borgao\s+licitante\s*[:\-]?\s*[^:;]{0,180}$", folded):
            statuses.append("review")
            continue
        # A labelled bidder takes precedence over ancillary words in a prior paragraph.
        labelled = re.search(
            r"\b(?:(?:licitante|participante|proponente|arrematante)(?:\s+vencedor[ao]?)?|(?:fornecedor|empresa)\s+(?:vencedor[ao]?|adjudicad[oa]))\s*[:\-]",
            folded,
        )
        ancillary = list(_ANCILLARY.finditer(folded))
        emitted_cost = re.search(r"\bemitid[ao]\s+pel[ao]\b", folded) and re.search(
            r"\bcusto\b", folded
        )
        if labelled and (not ancillary or labelled.start() > ancillary[-1].start()):
            statuses.append("confirmed")
        elif ancillary or emitted_cost:
            statuses.append("invalidated")
        elif _RULE_LANGUAGE.search(
            _fold(text[max(0, match.start() - 180) : match.end() + 180])
        ) or _NEGATED_PARTICIPANT_ROLE.search(
            _fold(text[max(0, match.start() - 180) : match.end() + 180])
        ):
            statuses.append("review")
        elif _SUBJECT.search(prefix.replace("\n", " ")):
            statuses.append("confirmed")
        elif _AWARD_OWNER.search(folded) or _APPEAL_OWNER.search(folded):
            statuses.append("confirmed")
        elif (
            _APPEAL_OWNER.search(extended)
            and re.search(r"\bpelas\s+empresas\b", extended)
            and re.match(r"^\s*[)\]]?\s*(?:e|,)\s+", folded)
            and not re.search(r"\b(?:municipio\s+de|prefeitura|contratante)\b", folded)
        ):
            statuses.append("confirmed")
        elif _BIDDER_TABLE.search(extended) and not re.search(
            r"\b(?:pesquisa\s+de\s+precos|estudo\s+tecnico|levantamento\s+de\s+mercado)\b", extended
        ):
            statuses.append("confirmed")
        else:
            after = _fold(text[match.end() : match.end() + 150])
            next_identifier = _CNPJ_TOKEN.search(after)
            if next_identifier:
                after = after[: next_identifier.start()]
            if re.match(
                r"^[\s,;:()\[\].-]*(?:apresentou\s+proposta|(?:foi|declarad[oa])\s+(?:inabilitad[oa]|desclassificad[oa]|habilitad[oa])|vencedor[ao]?|participou|participante|licitante|proponente|ofertou|classificad[oa]|desclassificad[oa]|inabilitad[oa]|habilitad[oa]|arrematante|rejeitad[oa])\b(?!\s*[:\-])",
                after,
            ) or (
                re.search(r"\b(?:fornecedor|licitante|empresa)\s*(?:cnpj\s*)?[:\-]?\s*$", folded)
                and re.match(
                    r"^\s*[-–]\s+[^:;]{1,140}?\b(?:foi|restou)\s+(?:inabilitad[oa]|desclassificad[oa]|habilitad[oa])\b",
                    after,
                )
            ):
                statuses.append("confirmed")
            else:
                statuses.append("review")
    return (
        ("confirmed" if _document_base_confidence(document_type) > 0 else "review")
        if "confirmed" in statuses
        else "invalidated"
        if all(s == "invalidated" for s in statuses)
        else "review"
    )


@dataclass(frozen=True)
class MonetaryFinding:
    company_cnpj: str
    amount: Decimal
    kind: str
    scope: str
    lot_number: str | None
    local_item_number: str | None
    evidence: DocumentEvidence
    subject_evidence: DocumentEvidence


@dataclass(frozen=True)
class ValueExtraction:
    values: tuple[MonetaryFinding, ...]
    diagnostics: tuple[str, ...]


def published_scope(text: str) -> tuple[set[str], bool]:
    """Retain published lot scope; local item references are not official IDs."""
    lots = {str(int(m.group(1))) for m in _LOT.finditer(text)}
    for match in re.finditer(r"\blotes\s+(\d+(?:\s*(?:,|e|a|até|-)\s*\d+)*)", text, re.I):
        group = match.group(1)
        numbers = [int(v) for v in re.findall(r"\d+", group)]
        if (
            re.search(r"\d+\s*(?:a|até|-)\s*\d+", group, re.I)
            and len(numbers) == 2
            and 0 <= numbers[1] - numbers[0] <= 1000
        ):
            numbers = list(range(numbers[0], numbers[1] + 1))
        lots.update(str(n) for n in numbers)
    local_item = False
    for match in _LOCAL_ITEM.finditer(text):
        after = _fold(text[match.end() : match.end() + 35])
        if not re.match(r"\s*(?:do|da)\s+(?:edital|lei|decreto)", after):
            local_item = True
    return lots, local_item


def _excerpt(page: DocumentEvidence, start: int, end: int) -> DocumentEvidence:
    from dataclasses import replace

    left, right = max(0, start - 240), min(len(page.text), end + 160)
    return replace(
        page,
        text=page.text[left:right],
        start_offset=(page.start_offset or 0) + left,
        end_offset=(page.start_offset or 0) + right,
    )


def extract_values(pages: list[DocumentEvidence]) -> ValueExtraction:
    """Extract explicit totals, using only an unequivocal bidder and published scope."""
    subjects: dict[str, DocumentEvidence] = {}
    all_lots: set[str] = set()
    for page in pages:
        if _document_base_confidence(page.document_type) == 0:
            continue
        all_lots.update(published_scope(page.text)[0])
        reference_cache: dict[str, str] = {}
        for match in _CNPJ_TOKEN.finditer(page.text):
            cnpj = normalize_cnpj(match.group())
            if not cnpj or cnpj in subjects or not is_valid_cnpj(cnpj):
                continue
            if cnpj not in reference_cache:
                reference_cache[cnpj] = reference_status(page.text, cnpj, page.document_type)
            if reference_cache[cnpj] == "confirmed":
                subjects.setdefault(cnpj, _excerpt(page, match.start(), match.end()))
    findings: list[MonetaryFinding] = []
    diagnostics: list[str] = []
    seen: set[tuple] = set()
    for page in pages:
        if _document_base_confidence(page.document_type) == 0:
            continue
        columns = list(_COLUMN.finditer(page.text))
        for match in _MONEY.finditer(page.text):
            prefix = page.text[max(0, match.start() - 240) : match.start()]
            suffix = page.text[match.end() : match.end() + 65]
            folded = _fold(prefix)
            if any(header.start() < match.start() for header in columns) and not _TOTAL_NEAR.search(
                folded
            ):
                diagnostics.append(
                    f"página {page.page_number}: tabela sem coluna e escopo monetário inequívocos"
                )
            cues = list(_CUES.finditer(folded))
            if not cues and not _TOTAL_NEAR.search(folded):
                continue
            cue = cues[-1].group() if cues else "official_total"
            cue_start, cue_end = (cues[-1].start(), cues[-1].end()) if cues else (0, 0)
            if not (
                cue.startswith("propost")
                or cue
                in {
                    "adjudicado",
                    "adjudicada",
                    "homologado",
                    "homologada",
                    "homologo",
                    "adjudico",
                    "homologou",
                    "adjudicou",
                    "valor final",
                    "official_total",
                }
            ):
                continue
            if re.search(
                r"\b(?:custo|garantia|estimativa|orcamento)\s+(?:da|de|para\s+a)\s+proposta\b[^.;]{0,50}$",
                folded,
            ):
                continue
            clause = re.split(r"(?<=[.!?;])\s+(?=[A-ZÀ-Ü])", prefix)[-1]
            clause_folded = _fold(clause)
            if re.search(r"\bitens\s+\d+", clause_folded):
                diagnostics.append(
                    f"página {page.page_number}: total de múltiplos itens sem valor individual publicado"
                )
                continue
            if re.search(
                r"\b(?:exemplo|exemplificativ\w*|hipotetic\w*|simulad\w*)\b", clause_folded
            ):
                continue
            if re.search(r"\blotes\s+\d+", _fold(clause + suffix)) and len(all_lots) > 1:
                diagnostics.append(f"página {page.page_number}: valor agregado de vários lotes")
                continue
            if re.match(
                r"\s*(?:(?:por|cada|a|o|ao)\s+(?:unidade|caixa|ampola|frasco|comprimido|kg|litro|peca|item)\b|/)",
                _fold(suffix),
            ):
                continue
            # Quoted third-party offers remain costs, even when labelled "Proposta".
            nearby = set(_cnpjs(prefix + match.group() + suffix))
            if nearby - set(subjects):
                preceding = list(_CNPJ_TOKEN.finditer(prefix))
                if preceding:
                    nearest = normalize_cnpj(preceding[-1].group())
                    local = prefix[preceding[-2].end() if len(preceding) > 1 else 0 :]
                    if nearest in subjects and reference_status(local, nearest) == "confirmed":
                        nearby = {nearest} | set(_cnpjs(suffix))
            candidates = nearby.intersection(subjects)
            if nearby - set(subjects):
                continue
            if len(candidates) == 1:
                cnpj = next(iter(candidates))
            elif not nearby and len(subjects) == 1:
                cnpj = next(iter(subjects))
            else:
                diagnostics.append(f"página {page.page_number}: valor sem licitante inequívoca")
                continue
            award_subject = subjects[cnpj].text in page.text and _AWARD_OWNER.search(
                _fold(subjects[cnpj].text)
            )
            if cue == "official_total":
                if not award_subject:
                    continue
                cue = "homologo"
            amount = Decimal(re.sub(r"\s", "", match.group(1)).replace(".", "").replace(",", "."))
            if amount < 0:
                continue
            if amount >= Decimal("1000000000000000"):
                diagnostics.append(f"página {page.page_number}: valor fora da precisão suportada")
                continue
            lot_matches = list(_LOT.finditer(clause))
            lot = (
                str(int(lot_matches[-1].group(1)))
                if lot_matches
                else next(iter(all_lots))
                if len(all_lots) == 1
                else None
            )
            if lot_matches and re.match(
                r"\s*(?:,|e|a|ao|até)\s*(?:lote\s*)?\d+\b", clause[lot_matches[-1].end() :], re.I
            ):
                diagnostics.append(f"página {page.page_number}: valor com múltiplos lotes")
                continue
            local_matches = list(_LOCAL_ITEM.finditer(clause))
            local_item = None
            if local_matches:
                tail = clause[local_matches[-1].end() :]
                if len(tail) < 80 and not re.search(r"\b(?:edital|lei|decreto)\b", _fold(tail)):
                    local_item = str(int(local_matches[-1].group(1)))
            if lot is None and all_lots:
                diagnostics.append(f"página {page.page_number}: valor sem lote inequívoco")
                continue
            kind = "proposal"
            if cue in {
                "adjudicado",
                "adjudicada",
                "homologado",
                "homologada",
                "homologo",
                "adjudico",
                "homologou",
                "adjudicou",
                "valor final",
            }:
                # A "final proposal" by itself is not an awarded result.
                if not (
                    award_subject
                    or re.search(
                        r"\b(?:adjudicad|homologad|homologo|adjudico|resultado\s+final)", folded
                    )
                ):
                    continue
                kind = "final_result"
            elif "readequad" in folded[max(0, cue_start - 65) :]:
                kind = "revised_proposal"
            elif "inicial" in cue:
                kind = "initial_proposal"
            scope = (
                "lot_item"
                if lot and local_item
                else "lot"
                if lot
                else "item"
                if local_item
                else "procurement"
            )
            table_before = any(header.start() < match.start() for header in columns)
            total_near = bool(_TOTAL_NEAR.search(folded))
            last_line = prefix.rsplit("\n", 1)[-1].strip()
            bare_row = bool(re.fullmatch(r"[\d\s|;,.:/-]+", last_line)) and any(
                c.isdigit() for c in last_line
            )
            if (table_before and not total_near) or (bare_row and "\n" in prefix[cue_end:]):
                diagnostics.append(
                    f"página {page.page_number}: tabela sem coluna e escopo monetário inequívocos"
                )
                continue
            if scope in {"procurement", "item", "lot_item"} and not total_near:
                diagnostics.append(
                    f"página {page.page_number}: valor total e escopo não confirmados"
                )
                continue
            if scope == "procurement" and table_before and not _GRAND_TOTAL_NEAR.search(folded):
                diagnostics.append(
                    f"página {page.page_number}: total de linha não comprova total da empresa"
                )
                continue
            key = (cnpj, amount, kind, scope, lot, local_item, page.page_number, match.start())
            if key in seen:
                continue
            seen.add(key)
            findings.append(
                MonetaryFinding(
                    cnpj,
                    amount,
                    kind,
                    scope,
                    lot,
                    local_item,
                    _excerpt(page, match.start(), match.end()),
                    subjects[cnpj],
                )
            )
    return ValueExtraction(tuple(findings), tuple(dict.fromkeys(diagnostics)))
