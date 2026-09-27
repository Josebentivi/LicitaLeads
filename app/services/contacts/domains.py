"""Deterministic derivation of corporate domains from evidence-bound e-mails.

A domain is only accepted when the normalized company name is embedded in it;
a generic inbox prefix alone is not enough. Free mail providers and official
government domains are always rejected. Nothing here touches the network.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass

EMAIL_PATTERN = re.compile(
    r"(?<![\w.+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w.-])",
    re.I,
)

GENERIC_EMAIL_PREFIXES = {
    "administrativo",
    "atendimento",
    "comercial",
    "contato",
    "financeiro",
    "juridico",
    "licitacao",
    "licitacoes",
    "sac",
    "vendas",
}

FREE_EMAIL_DOMAINS = {
    "aol.com",
    "bol.com.br",
    "gmail.com",
    "globo.com",
    "globomail.com",
    "gmx.com",
    "gmx.net",
    "googlemail.com",
    "hotmail.com",
    "hotmail.com.br",
    "icloud.com",
    "ig.com.br",
    "live.com",
    "mac.com",
    "mail.com",
    "me.com",
    "msn.com",
    "oi.com.br",
    "outlook.com",
    "outlook.com.br",
    "proton.me",
    "protonmail.com",
    "r7.com",
    "terra.com.br",
    "uol.com.br",
    "yahoo.com",
    "yahoo.com.br",
    "ymail.com",
    "zipmail.com.br",
    "zoho.com",
}

BLOCKED_EMAIL_SUFFIXES = ("gov.br", "jus.br", "leg.br", "mp.br", "mil.br")
_DOMAIN_LABEL = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")
_MIN_NAME_CHARACTERS = 4


@dataclass(frozen=True, slots=True)
class EmailCandidate:
    email: str
    local_part: str
    domain: str
    start: int
    end: int

    @property
    def is_generic(self) -> bool:
        return self.local_part.split("+", 1)[0] in GENERIC_EMAIL_PREFIXES


def extract_email_candidates(text: str) -> list[EmailCandidate]:
    """Return unique e-mail candidates with their positions in the source text."""

    seen: set[str] = set()
    candidates: list[EmailCandidate] = []
    for match in EMAIL_PATTERN.finditer(text):
        email = match.group(1).lower().rstrip(".")
        if email in seen:
            continue
        seen.add(email)
        local_part, _, domain = email.partition("@")
        candidates.append(
            EmailCandidate(
                email=email,
                local_part=local_part,
                domain=domain.rstrip("."),
                start=match.start(1),
                end=match.end(1),
            )
        )
    return candidates


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _valid_domain(domain: str) -> bool:
    if not domain or len(domain) > 253 or "." not in domain:
        return False
    try:
        ipaddress.ip_address(domain)
        return False
    except ValueError:
        pass
    try:
        ascii_domain = domain.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    labels = ascii_domain.split(".")
    if len(labels) < 2:
        return False
    return all(_DOMAIN_LABEL.fullmatch(label) for label in labels)


def _is_blocked(domain: str) -> bool:
    if domain in FREE_EMAIL_DOMAINS:
        return True
    return any(
        domain == suffix or domain.endswith(f".{suffix}") for suffix in BLOCKED_EMAIL_SUFFIXES
    )


def derive_company_domain(
    candidate: EmailCandidate,
    normalized_company_name: str | None,
) -> str | None:
    """Return the evidenced domain only when the company name is inside it."""

    domain = candidate.domain
    if not _valid_domain(domain) or _is_blocked(domain):
        return None
    compact_name = _compact(normalized_company_name or "")
    if len(compact_name) < _MIN_NAME_CHARACTERS:
        return None
    if compact_name not in _compact(domain):
        return None
    return domain


def context_window(text: str, start: int, end: int, radius: int = 180) -> tuple[str, int]:
    """Return a bounded slice around a match; used to bind the e-mail to a company."""

    left = max(0, start - radius)
    right = min(len(text), end + radius)
    return text[left:right].strip(), left
