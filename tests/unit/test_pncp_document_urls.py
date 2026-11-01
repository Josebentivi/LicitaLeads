"""Official evidence can resolve a resource; it cannot relax destination safety."""

from types import SimpleNamespace

import httpx
import pytest

from app.services.documents.pncp_urls import resolve_pncp_document
from app.services.ingestion.documents import (
    DocumentDownloadError,
    SecureDocumentDownloader,
    UnsafeDocumentURL,
)

PATH = "/v1/orgaos/05962421000117/compras/2026/63/arquivos"
ORIGINAL = "https://pncp.gov.br:48707/pncp-api" + PATH + "/1"
CANONICAL = "https://pncp.gov.br/api/pncp" + PATH + "/1"


def evidence(url=ORIGINAL, *, sequence=1, endpoint=None):
    return SimpleNamespace(
        source="pncp",
        endpoint=endpoint or "https://pncp.gov.br/api/pncp" + PATH,
        raw_payload=[{"url": url, "sequencialDocumento": sequence}],
    )


async def public(_host, _port):
    return ["93.184.216.34"]


@pytest.mark.parametrize("port", [48707, 51546, 45450, 65018])
async def test_internal_ports_are_never_contacted_and_received_url_remains_original(port):
    original = ORIGINAL.replace(":48707", f":{port}")
    proof = evidence(original)
    requests = []

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=b"%PDF-1.4 file")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=public
        )
        with pytest.raises(UnsafeDocumentURL, match="ports"):
            await downloader.validate_url(original)
        result = await downloader.download(
            original, resolution=resolve_pncp_document(original, proof)
        )
    assert requests == [CANONICAL] and result.url == CANONICAL
    assert proof.raw_payload[0]["url"] == original


@pytest.mark.parametrize(
    "url",
    [
        ORIGINAL.replace("pncp.gov.br", "pncp.gov.br.evil.test"),
        ORIGINAL.replace("https://", "https://user:password@"),
        ORIGINAL.replace("https://", "http://"),
        ORIGINAL + "?token=private",
        ORIGINAL + "#fragment",
        ORIGINAL.replace("/arquivos/1", "/arquivos/%31"),
        ORIGINAL.replace("/arquivos/1", "/arquivos/../1"),
        ORIGINAL.replace(":48707", ":invalid"),
    ],
)
def test_ambiguous_and_untrusted_urls_are_not_rewritten(url):
    assert resolve_pncp_document(url, evidence(url)) is None


def test_evidence_must_identify_the_same_listing_and_document():
    assert resolve_pncp_document(ORIGINAL, evidence(sequence=2)) is None
    assert (
        resolve_pncp_document(
            ORIGINAL,
            evidence(endpoint="https://pncp.gov.br/api/pncp" + PATH.replace("/63/", "/64/")),
        )
        is None
    )
    assert resolve_pncp_document(ORIGINAL, None) is None


@pytest.mark.parametrize(
    "location,expected",
    [
        (ORIGINAL, DocumentDownloadError),
        (ORIGINAL[:-1] + "2", UnsafeDocumentURL),
        ("http://127.0.0.1/private", UnsafeDocumentURL),
        ("http://pncp.gov.br/public.pdf", UnsafeDocumentURL),
        (CANONICAL.replace("/arquivos/1", "/arquivos/%32"), UnsafeDocumentURL),
        (CANONICAL + "?arquivo=2", UnsafeDocumentURL),
    ],
)
async def test_resolution_does_not_follow_loops_identity_changes_or_private_redirects(
    location, expected
):
    requests = []

    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(302, headers={"Location": location})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=public
        )
        with pytest.raises(expected):
            await downloader.download(
                ORIGINAL, resolution=resolve_pncp_document(ORIGINAL, evidence())
            )
    assert requests == [CANONICAL]


async def test_canonical_url_still_requires_public_dns():
    async def private(_host, _port):
        return ["127.0.0.1"]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("unsafe connection"))
    ) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=private
        )
        with pytest.raises(UnsafeDocumentURL, match="non-public"):
            await downloader.download(
                ORIGINAL, resolution=resolve_pncp_document(ORIGINAL, evidence())
            )


async def test_resolved_resource_keeps_size_limit():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"%PDF-1.4 too large")
        )
    ) as client:
        downloader = SecureDocumentDownloader(
            allowed_hosts={"pncp.gov.br"}, client=client, resolver=public, max_bytes=5
        )
        with pytest.raises(DocumentDownloadError, match="size limit"):
            await downloader.download(
                ORIGINAL, resolution=resolve_pncp_document(ORIGINAL, evidence())
            )
