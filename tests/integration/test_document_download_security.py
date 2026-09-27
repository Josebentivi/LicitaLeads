"""Network boundary tests for official-document downloads."""

from __future__ import annotations

from hashlib import sha256

import httpx
import pytest

from app.services.ingestion.documents import (
    DocumentDownloadError,
    SecureDocumentDownloader,
    UnsafeDocumentURL,
    store_by_hash,
)


async def _public_resolver(_host: str, _port: int) -> list[str]:
    return ["93.184.216.34"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "allowed_host"),
    [
        ("http://127.0.0.1/internal", "127.0.0.1"),
        ("http://169.254.169.254/latest/meta-data", "169.254.169.254"),
        ("http://[::1]/internal", "::1"),
        ("http://[fc00::1]/internal", "fc00::1"),
    ],
)
async def test_ssrf_private_ipv4_and_ipv6_are_rejected(url: str, allowed_host: str) -> None:
    downloader = SecureDocumentDownloader(allowed_hosts={allowed_host})
    try:
        with pytest.raises(UnsafeDocumentURL, match="non-public"):
            await downloader.validate_url(url)
    finally:
        await downloader.aclose()


@pytest.mark.asyncio
async def test_redirect_is_revalidated_before_following() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(
            302,
            headers={"location": "http://127.0.0.1/private"},
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    downloader = SecureDocumentDownloader(
        allowed_hosts={"official.test"}, client=client, resolver=_public_resolver
    )
    try:
        with pytest.raises(UnsafeDocumentURL, match="allow-listed"):
            await downloader.download("https://official.test/document.pdf")
    finally:
        await client.aclose()

    assert calls == ["https://official.test/document.pdf"]


@pytest.mark.asyncio
async def test_redirect_limit_is_enforced() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(302, headers={"location": f"/redirect/{calls}"}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    downloader = SecureDocumentDownloader(
        allowed_hosts={"official.test"},
        max_redirects=1,
        client=client,
        resolver=_public_resolver,
    )
    try:
        with pytest.raises(DocumentDownloadError, match="redirect limit"):
            await downloader.download("https://official.test/document.pdf")
    finally:
        await client.aclose()

    assert calls == 2


@pytest.mark.asyncio
async def test_declared_download_size_is_bounded_before_storage() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"eleven-bytes",
            headers={"content-length": "12", "content-type": "application/pdf"},
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    downloader = SecureDocumentDownloader(
        allowed_hosts={"official.test"},
        max_bytes=10,
        client=client,
        resolver=_public_resolver,
    )
    try:
        with pytest.raises(DocumentDownloadError, match="size limit"):
            await downloader.download("https://official.test/document.pdf")
    finally:
        await client.aclose()


def test_hash_storage_ignores_path_traversal_in_filename(tmp_path) -> None:
    content = b"official evidence"
    digest = sha256(content).hexdigest()

    stored = store_by_hash(content, digest, tmp_path, "../../outside.pdf")

    assert stored.is_relative_to(tmp_path.resolve())
    assert stored.name == f"{digest}.pdf"
    assert stored.read_bytes() == content
