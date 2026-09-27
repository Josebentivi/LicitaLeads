# ruff: noqa: E501

from __future__ import annotations

import pytest

from app.services.contacts.providers import (
    CompanyContactTarget,
    ContactType,
    FetchedPage,
    ManualContactProvider,
    PublicWebsiteContactProvider,
    validate_public_url,
)


def test_manual_contact_csv_reports_errors_and_deduplicates() -> None:
    content = """cnpj,contact_type,contact_value,source_url,is_corporate
04.252.011/0001-10,email,licitacoes@example.com,https://example.com/contato,sim
04.252.011/0001-10,email,licitacoes@example.com,https://example.com/contato,true
invalid,email,x@example.com,https://example.com,sim
"""
    result = ManualContactProvider().import_csv(content)
    assert len(result.contacts) == 1
    assert result.contacts[0].is_corporate
    assert len(result.errors) == 1
    assert result.errors[0].line == 4


@pytest.mark.asyncio
async def test_public_website_provider_collects_only_corporate_contacts() -> None:
    async def fetcher(url: str) -> FetchedPage:
        if url.endswith("robots.txt"):
            return FetchedPage(url, 200, "text/plain", "User-agent: *\nAllow: /")
        return FetchedPage(
            url,
            200,
            "text/html; charset=utf-8",
            """<html><body>
            <p>Contato: licitacoes@example.com</p>
            <p>Diretora: maria@example.com</p>
            <p>WhatsApp: (11) 99999-1234</p>
            <form action="/enviar"></form>
            <a href="https://www.linkedin.com/company/example">LinkedIn</a>
            </body></html>""",
        )

    provider = PublicWebsiteContactProvider(fetcher=fetcher, validate_dns=False)
    contacts = await provider.find_contacts(
        CompanyContactTarget("1", "Example Ltda.", "04.252.011/0001-10", domain="example.com")
    )
    values = {contact.contact_value for contact in contacts}
    assert "licitacoes@example.com" in values
    assert "maria@example.com" not in values
    assert any(contact.contact_type is ContactType.WHATSAPP for contact in contacts)
    assert any(contact.contact_type is ContactType.FORM for contact in contacts)


def test_ssrf_validator_rejects_private_ip_and_foreign_domain() -> None:
    with pytest.raises(ValueError):
        validate_public_url(
            "http://127.0.0.1/contact", expected_domain="127.0.0.1", validate_dns=False
        )
    with pytest.raises(ValueError, match="outside"):
        validate_public_url(
            "https://attacker.example/contact", expected_domain="example.com", validate_dns=False
        )
