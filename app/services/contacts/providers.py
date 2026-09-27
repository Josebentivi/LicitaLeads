from __future__ import annotations

import csv
import hashlib
import ipaddress
import logging
import re
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from io import StringIO
from typing import Protocol, runtime_checkable
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from app.services.identifiers import is_valid_cnpj, normalize_cnpj

# Regexes and constructor calls below are clearer when kept as single expressions.
# ruff: noqa: E501

logger = logging.getLogger(__name__)


class ContactType(StrEnum):
    EMAIL = "email"
    PHONE = "phone"
    WHATSAPP = "whatsapp"
    FORM = "form"
    WEBSITE = "website"
    LINKEDIN = "linkedin"


@dataclass(frozen=True, slots=True)
class CompanyContactTarget:
    company_id: str | None
    legal_name: str
    cnpj: str | None
    website: str | None = None
    domain: str | None = None


@dataclass(frozen=True, slots=True)
class CompanyContactCandidate:
    contact_type: ContactType
    contact_value: str
    source_url: str
    is_corporate: bool
    is_personal: bool
    confidence: float
    company_cnpj: str | None = None

    @property
    def fingerprint(self) -> str:
        value = f"{self.company_cnpj or ''}|{self.contact_type.value}|{self.contact_value.lower()}"
        return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ContactImportError:
    line: int
    message: str


@dataclass(frozen=True, slots=True)
class ContactImportResult:
    contacts: tuple[CompanyContactCandidate, ...]
    errors: tuple[ContactImportError, ...]


@runtime_checkable
class ContactEnrichmentProvider(Protocol):
    async def find_contacts(
        self, company: CompanyContactTarget
    ) -> list[CompanyContactCandidate]: ...


class ManualContactProvider:
    REQUIRED_COLUMNS = {"cnpj", "contact_type", "contact_value", "source_url", "is_corporate"}

    def import_csv(self, content: str | bytes) -> ContactImportResult:
        if isinstance(content, bytes):
            content = content.decode("utf-8-sig")
        reader = csv.DictReader(StringIO(content))
        if not reader.fieldnames or not self.REQUIRED_COLUMNS.issubset(reader.fieldnames):
            missing = sorted(self.REQUIRED_COLUMNS - set(reader.fieldnames or []))
            raise ValueError("missing CSV columns: " + ", ".join(missing))
        contacts: list[CompanyContactCandidate] = []
        errors: list[ContactImportError] = []
        seen: set[str] = set()
        for line, row in enumerate(reader, start=2):
            try:
                cnpj = normalize_cnpj(row.get("cnpj"))
                if not is_valid_cnpj(cnpj):
                    raise ValueError("invalid CNPJ")
                contact_type = ContactType((row.get("contact_type") or "").strip().lower())
                value = (row.get("contact_value") or "").strip()
                source_url = (row.get("source_url") or "").strip()
                if not value:
                    raise ValueError("empty contact_value")
                if not source_url.startswith(("https://", "http://")):
                    raise ValueError("source_url must be HTTP(S)")
                is_corporate = _parse_bool(row.get("is_corporate"))
                contact = CompanyContactCandidate(
                    contact_type=contact_type,
                    contact_value=value,
                    source_url=source_url,
                    is_corporate=is_corporate,
                    is_personal=not is_corporate,
                    confidence=0.95 if is_corporate else 0.55,
                    company_cnpj=cnpj,
                )
                if contact.fingerprint not in seen:
                    seen.add(contact.fingerprint)
                    contacts.append(contact)
            except (ValueError, UnicodeError) as exc:
                errors.append(ContactImportError(line, str(exc)))
        return ContactImportResult(tuple(contacts), tuple(errors))


def _parse_bool(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    if normalized in {"1", "true", "sim", "yes", "s"}:
        return True
    if normalized in {"0", "false", "não", "nao", "no", "n"}:
        return False
    raise ValueError("is_corporate must be a boolean")


@dataclass(frozen=True, slots=True)
class FetchedPage:
    url: str
    status_code: int
    content_type: str
    text: str


PageFetcher = Callable[[str], Awaitable[FetchedPage]]

_GENERIC_EMAIL_PREFIXES = {
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
_EMAIL = re.compile(r"(?<![\w.+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w.-])", re.I)
_PHONE = re.compile(r"(?<!\d)(?:\+?55\s*)?(?:\(?\d{2}\)?[\s.-]*)?9?\d{4}[\s.-]*\d{4}(?!\d)")
_PHONE_LABEL = re.compile(
    r"(?:whats(?:app)?|telefone|fone|tel\.?|comercial|atendimento|contato)\s*:?.{0,24}$", re.I
)
_PATHS = ("/", "/contato", "/contact", "/fale-conosco", "/sobre")


def _canonical_domain(value: str) -> str:
    candidate = value.strip().lower()
    parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
    host = (parsed.hostname or "").rstrip(".").encode("idna").decode("ascii")
    if not host or host == "localhost" or "." not in host:
        raise ValueError("a public corporate domain is required")
    return host[4:] if host.startswith("www.") else host


def _is_public_ip(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def validate_public_url(url: str, *, expected_domain: str, validate_dns: bool = True) -> str:
    parsed = urlparse(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise ValueError("only credential-free public HTTP(S) URLs are allowed")
    host = parsed.hostname.rstrip(".").encode("idna").decode("ascii")
    canonical = _canonical_domain(expected_domain)
    if host != canonical and host != f"www.{canonical}":
        raise ValueError("URL is outside the evidenced corporate domain")
    try:
        if not _is_public_ip(host):
            raise ValueError("private or special-purpose address is not allowed")
    except ValueError as exc:
        # A ValueError from ip_address means this is a hostname and needs DNS.
        if "does not appear" not in str(exc):
            raise
    if validate_dns:
        port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
        addresses = {
            str(item[4][0]) for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        }
        if not addresses or any(not _is_public_ip(address) for address in addresses):
            raise ValueError("domain resolves to a private or special-purpose address")
    return url


async def _httpx_fetch(url: str) -> FetchedPage:
    import httpx

    expected_domain = _canonical_domain(url)
    async with httpx.AsyncClient(
        timeout=15, follow_redirects=False, headers={"User-Agent": "LicitaLeadMonitor/0.1"}
    ) as client:
        current = url
        for _ in range(4):
            validate_public_url(current, expected_domain=expected_domain, validate_dns=True)
            response = await client.get(current)
            if response.is_redirect:
                location = response.headers.get("location")
                if not location:
                    break
                current = urljoin(current, location)
                continue
            content = response.content
            if len(content) > 2 * 1024 * 1024:
                raise ValueError("contact page exceeds 2 MB")
            return FetchedPage(
                url=str(response.url),
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                text=response.text,
            )
        raise ValueError("too many redirects")


class PublicWebsiteContactProvider:
    def __init__(
        self,
        *,
        fetcher: PageFetcher | None = None,
        user_agent: str = "LicitaLeadMonitor/0.1",
        validate_dns: bool = True,
        max_pages: int = 5,
    ) -> None:
        self.fetcher = fetcher or _httpx_fetch
        self.user_agent = user_agent
        self.validate_dns = validate_dns
        self.max_pages = min(max_pages, len(_PATHS))

    async def find_contacts(self, company: CompanyContactTarget) -> list[CompanyContactCandidate]:
        if not company.domain and not company.website:
            return []
        domain = _canonical_domain(company.domain or company.website or "")
        base = f"https://{domain}"
        robots_url = validate_public_url(
            f"{base}/robots.txt", expected_domain=domain, validate_dns=self.validate_dns
        )
        robots = RobotFileParser(robots_url)
        try:
            robots_page = await self.fetcher(robots_url)
            if robots_page.status_code < 400:
                robots.parse(robots_page.text.splitlines())
            else:
                robots.parse([])
        except Exception:
            # A missing robots file means no published restriction. Network
            # failures do not justify crawling, so a custom fetcher should raise
            # only for genuine failures; the conservative default is no pages.
            return []

        results: list[CompanyContactCandidate] = []
        for path in _PATHS[: self.max_pages]:
            url = validate_public_url(
                urljoin(base, path), expected_domain=domain, validate_dns=self.validate_dns
            )
            if not robots.can_fetch(self.user_agent, url):
                continue
            try:
                page = await self.fetcher(url)
            except Exception as exc:
                logger.debug(
                    "contact page could not be fetched", extra={"url": url, "error": str(exc)}
                )
                continue
            final_url = validate_public_url(
                page.url, expected_domain=domain, validate_dns=self.validate_dns
            )
            if page.status_code >= 400 or "html" not in page.content_type.lower():
                continue
            results.extend(self._extract_page(page.text, final_url, domain, company.cnpj))

        unique: dict[str, CompanyContactCandidate] = {}
        for contact in results:
            existing = unique.get(contact.fingerprint)
            if existing is None or contact.confidence > existing.confidence:
                unique[contact.fingerprint] = contact
        return sorted(
            unique.values(),
            key=lambda item: (-item.confidence, item.contact_type.value, item.contact_value),
        )

    @staticmethod
    def _extract_page(
        source: str,
        source_url: str,
        domain: str,
        company_cnpj: str | None,
    ) -> list[CompanyContactCandidate]:
        try:
            from bs4 import BeautifulSoup
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("BeautifulSoup is required for contact enrichment") from exc
        soup = BeautifulSoup(source, "html.parser")
        for unwanted in soup(["script", "style", "noscript", "template"]):
            unwanted.decompose()
        text = soup.get_text(" ", strip=True)
        contacts: list[CompanyContactCandidate] = [
            CompanyContactCandidate(
                ContactType.WEBSITE,
                f"https://{domain}",
                source_url,
                True,
                False,
                0.95,
                normalize_cnpj(company_cnpj),
            )
        ]
        email_values = {match.group(1).lower() for match in _EMAIL.finditer(text)}
        for anchor in soup.select('a[href^="mailto:"]'):
            href_value = anchor.get("href", "")
            href = href_value if isinstance(href_value, str) else ""
            if "@" in href:
                email_values.add(href[7:].split("?", 1)[0].lower())
        for email in email_values:
            local, _, email_domain = email.partition("@")
            generic = local.split("+", 1)[0] in _GENERIC_EMAIL_PREFIXES
            if email_domain.rstrip(".") not in {domain, f"www.{domain}"} or not generic:
                continue
            contacts.append(
                CompanyContactCandidate(
                    ContactType.EMAIL,
                    email,
                    source_url,
                    True,
                    False,
                    0.95 if local in {"licitacao", "licitacoes", "juridico"} else 0.90,
                    normalize_cnpj(company_cnpj),
                )
            )
        for match in _PHONE.finditer(text):
            prefix = text[max(0, match.start() - 40) : match.start()]
            label = _PHONE_LABEL.search(prefix)
            if not label:
                continue
            digits = re.sub(r"\D", "", match.group())
            if digits.startswith("55") and len(digits) > 11:
                digits = digits[2:]
            if len(digits) not in {10, 11}:
                continue
            kind = ContactType.WHATSAPP if "whats" in label.group().lower() else ContactType.PHONE
            contacts.append(
                CompanyContactCandidate(
                    kind, digits, source_url, True, False, 0.80, normalize_cnpj(company_cnpj)
                )
            )
        for form in soup.select("form[action]"):
            action_value = form.get("action", "")
            action_path = action_value if isinstance(action_value, str) else ""
            action = urljoin(source_url, action_path)
            parsed = urlparse(action)
            action_domain = (parsed.hostname or "").lower().removeprefix("www.")
            if parsed.scheme in {"http", "https"} and action_domain == domain:
                contacts.append(
                    CompanyContactCandidate(
                        ContactType.FORM,
                        action,
                        source_url,
                        True,
                        False,
                        0.75,
                        normalize_cnpj(company_cnpj),
                    )
                )
        for anchor in soup.select('a[href*="linkedin.com/company/"]'):
            href_value = anchor.get("href", "")
            href = href_value if isinstance(href_value, str) else ""
            if href.startswith(("https://", "http://")):
                contacts.append(
                    CompanyContactCandidate(
                        ContactType.LINKEDIN,
                        href,
                        source_url,
                        True,
                        False,
                        0.75,
                        normalize_cnpj(company_cnpj),
                    )
                )
        return contacts
