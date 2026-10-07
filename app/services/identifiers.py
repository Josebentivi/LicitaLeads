"""Brazilian company identifier and business-name normalization helpers."""

from __future__ import annotations

import re
import unicodedata

from app.models.enums import ParticipantStatus

_CNPJ_PATTERN = re.compile(r"^[A-Z0-9]{12}[0-9]{2}$")
_NON_ALNUM = re.compile(r"[^A-Z0-9]+")
_NAME_NON_ALNUM = re.compile(r"[^A-Z0-9]+")
_LEGAL_SUFFIXES = (
    ("SOCIEDADE", "ANONIMA"),
    ("EMPRESA", "INDIVIDUAL", "DE", "RESPONSABILIDADE", "LIMITADA"),
    ("SOCIEDADE", "LIMITADA", "UNIPESSOAL"),
    ("LIMITADA", "UNIPESSOAL"),
    ("EIRELI",),
    ("LTDA",),
    ("LIMITADA",),
    ("S", "A"),
    ("SA",),
    ("ME",),
    ("EPP",),
)


def normalize_cnpj(value: str | None) -> str | None:
    """Return the canonical 14-character CNPJ or ``None`` for an empty value.

    The 2026 alphanumeric CNPJ keeps two numeric check digits.  Punctuation is
    accepted for backwards compatibility, while ambiguous/non-ASCII characters
    are rejected by :func:`is_valid_cnpj`.
    """

    if value is None:
        return None
    normalized = _NON_ALNUM.sub("", value.strip().upper())
    return normalized or None


def _character_value(character: str) -> int:
    # Receita Federal's alphanumeric specification maps a character to its
    # ASCII code minus 48 (digits therefore retain their usual numeric value).
    return ord(character) - 48


def _check_digit(base: str, weights: tuple[int, ...]) -> str:
    remainder = (
        sum(_character_value(char) * weight for char, weight in zip(base, weights, strict=True))
        % 11
    )
    return "0" if remainder < 2 else str(11 - remainder)


def is_valid_cnpj(value: str | None) -> bool:
    """Validate legacy numeric and Receita alphanumeric CNPJ values."""

    normalized = normalize_cnpj(value)
    if normalized is None or not _CNPJ_PATTERN.fullmatch(normalized):
        return False
    if len(set(normalized[:12])) == 1 and normalized[:12].isdigit():
        return False
    first = _check_digit(normalized[:12], (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    second = _check_digit(normalized[:12] + first, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    return normalized[-2:] == first + second


def format_cnpj(value: str) -> str:
    """Format a validated CNPJ without assuming its root is numeric."""

    normalized = normalize_cnpj(value)
    if normalized is None or len(normalized) != 14:
        raise ValueError("CNPJ must contain 14 characters")
    return (
        f"{normalized[:2]}.{normalized[2:5]}.{normalized[5:8]}/{normalized[8:12]}-{normalized[12:]}"
    )


def _strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def normalize_company_name(value: str | None, *, strip_legal_suffix: bool = True) -> str | None:
    """Produce a stable comparison key for a Brazilian business name.

    This helper is intentionally conservative.  It removes punctuation,
    diacritics and trailing legal forms, but does not perform fuzzy matching.
    """

    if value is None or not value.strip():
        return None
    normalized = _strip_accents(value).upper().replace("&", " E ")
    tokens = _NAME_NON_ALNUM.sub(" ", normalized).split()
    if strip_legal_suffix:
        changed = True
        while changed and tokens:
            changed = False
            for suffix in _LEGAL_SUFFIXES:
                if len(tokens) >= len(suffix) and tuple(tokens[-len(suffix) :]) == suffix:
                    del tokens[-len(suffix) :]
                    changed = True
                    break
    return " ".join(tokens) or None


PROCUREMENT_TYPE_LICITACAO = "licitacao"
PROCUREMENT_TYPE_CONTRATACAO_DIRETA = "contratacao_direta"
PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR = "procedimento_auxiliar"

_MODALITY_ALIASES: dict[str, str] = {
    # Labels with explanatory suffixes are reduced to the shared filter key.
    "dispensa_de_licitacao": "dispensa",
    "dispensa_de_licitacao_eletronica": "dispensa_eletronica",
    "inexigibilidade_de_licitacao": "inexigibilidade",
}

_MODALITY_CATEGORIES: dict[str, str] = {
    # Modalidades do art. 28 da Lei 14.133/2021, incluindo as variações
    # eletrônica/presencial publicadas pelas fontes.
    "pregao_eletronico": PROCUREMENT_TYPE_LICITACAO,
    "pregao_presencial": PROCUREMENT_TYPE_LICITACAO,
    "pregao": PROCUREMENT_TYPE_LICITACAO,
    "concorrencia": PROCUREMENT_TYPE_LICITACAO,
    "concorrencia_eletronica": PROCUREMENT_TYPE_LICITACAO,
    "concorrencia_presencial": PROCUREMENT_TYPE_LICITACAO,
    "concurso": PROCUREMENT_TYPE_LICITACAO,
    "leilao": PROCUREMENT_TYPE_LICITACAO,
    "leilao_eletronico": PROCUREMENT_TYPE_LICITACAO,
    "leilao_presencial": PROCUREMENT_TYPE_LICITACAO,
    "dialogo_competitivo": PROCUREMENT_TYPE_LICITACAO,
    "convite": PROCUREMENT_TYPE_LICITACAO,
    "tomada_de_precos": PROCUREMENT_TYPE_LICITACAO,
    # Contratação direta (arts. 74 e 75).
    "dispensa": PROCUREMENT_TYPE_CONTRATACAO_DIRETA,
    "dispensa_eletronica": PROCUREMENT_TYPE_CONTRATACAO_DIRETA,
    "inexigibilidade": PROCUREMENT_TYPE_CONTRATACAO_DIRETA,
    # Procedimentos auxiliares (art. 78).
    "credenciamento": PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR,
    "pre_qualificacao": PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR,
    "manifestacao_de_interesse": PROCUREMENT_TYPE_PROCEDIMENTO_AUXILIAR,
}


def modality_category(value: str | None) -> str | None:
    """Map a raw modality label to the Lei 14.133/2021 contracting category.

    Returns ``None`` for labels that cannot be classified deterministically;
    callers must preserve that as unknown instead of guessing.
    """

    key = canonical_modality(value)
    if key is None:
        return None
    return _MODALITY_CATEGORIES.get(key)


def canonical_modality(value: str | None) -> str | None:
    """Normalize a source modality label into the shared filter key.

    PNCP and Compras.gov.br publish human labels such as ``"Pregão -
    Eletrônico"`` while API filters use keys such as ``pregao_eletronico``.
    Both are reduced to the same deterministic key so filtering and
    cross-source deduplication compare the same value. The raw label is kept
    separately for display.
    """

    if value is None:
        return None
    normalized = _strip_accents(str(value)).lower().replace("&", " e ")
    tokens = [token for token in re.sub(r"[^a-z0-9]+", " ", normalized).split() if token]
    if not tokens:
        return None
    key = "_".join(tokens)
    return _MODALITY_ALIASES.get(key, key)


def cnpj_fingerprint(value: str) -> str:
    """Return a validated CNPJ suitable for unique keys."""

    normalized = normalize_cnpj(value)
    if not is_valid_cnpj(normalized):
        raise ValueError("invalid CNPJ")
    assert normalized is not None
    return normalized


def participant_status_code(role: str | None, status: str | None) -> ParticipantStatus:
    """Normalize a participation outcome for auditable filtering.

    The raw source/detection text is preserved in ``Participant.status``; this
    helper only maps it to the canonical enum used by filters.
    """

    normalized = _strip_accents(status or "").lower()
    if "inabilit" in normalized or "ineligible" in normalized:
        return ParticipantStatus.INELIGIBLE
    if "desclassific" in normalized or "disqualified" in normalized:
        return ParticipantStatus.DISQUALIFIED
    role_key = (role or "").strip().lower()
    if role_key == "winner":
        return ParticipantStatus.WINNER
    if role_key == "awarded":
        return ParticipantStatus.AWARDED
    if role_key == "participant":
        return ParticipantStatus.PARTICIPANT
    if "vencedor" in normalized or "homologad" in normalized or "adjudicad" in normalized:
        return ParticipantStatus.AWARDED
    return ParticipantStatus.UNKNOWN
