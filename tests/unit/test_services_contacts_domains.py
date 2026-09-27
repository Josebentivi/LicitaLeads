# ruff: noqa: E501

"""Deterministic domain derivation from evidenced e-mails."""

from __future__ import annotations

from app.services.contacts.domains import (
    EmailCandidate,
    context_window,
    derive_company_domain,
    extract_email_candidates,
)

DOMAIN = "empresaexemplo.com.br"
EMAIL = "contato@" + DOMAIN


def _candidate(text: str) -> EmailCandidate:
    candidates = extract_email_candidates(text)
    assert len(candidates) == 1
    return candidates[0]


def test_extract_email_candidates_deduplicates_and_locates() -> None:
    text = f"Fale com {EMAIL} ou {EMAIL.upper()}."
    candidates = extract_email_candidates(text)

    assert len(candidates) == 1
    assert candidates[0].email == EMAIL
    assert candidates[0].domain == DOMAIN
    assert candidates[0].is_generic is True
    assert text[candidates[0].start : candidates[0].end] == EMAIL


def test_derive_company_domain_requires_company_name_in_domain() -> None:
    candidate = _candidate(EMAIL)

    assert derive_company_domain(candidate, "EMPRESA EXEMPLO") == DOMAIN
    assert derive_company_domain(candidate, "Outra Empresa") is None
    assert derive_company_domain(candidate, None) is None
    assert derive_company_domain(candidate, "ABC") is None


def test_derive_company_domain_rejects_free_and_official_domains() -> None:
    assert derive_company_domain(_candidate("contato@" + "gmail.com"), "EMPRESA EXEMPLO") is None
    assert (
        derive_company_domain(_candidate("contato@empresaexemplo.gov.br"), "EMPRESA EXEMPLO")
        is None
    )


def test_derive_company_domain_rejects_malformed_domains() -> None:
    malformed = EmailCandidate(
        email="x@empresaexemplo",
        local_part="x",
        domain="empresaexemplo",
        start=0,
        end=1,
    )

    assert derive_company_domain(malformed, "EMPRESA EXEMPLO") is None


def test_context_window_is_bounded() -> None:
    text = "a" * 400 + " " + EMAIL + " " + "b" * 400
    start = text.index(EMAIL)

    window, left = context_window(text, start, start + len(EMAIL), radius=50)

    assert EMAIL in window
    assert left == start - 50
