"""No real network: verify destination pinning, redirect secrecy and stream bounds."""

import gzip

import httpcore
import httpx
import pytest

from app.connectors.bounded_http import (
    SourceDownloadError,
    bounded_request,
    validate_public_url,
)


class RecordingStream(httpcore.AsyncNetworkStream):
    def __init__(self):
        self.incoming = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
        self.server_hostname = None

    async def read(self, max_bytes, timeout=None):  # noqa: ASYNC109 - fixed httpcore stream protocol
        result, self.incoming = self.incoming[:max_bytes], self.incoming[max_bytes:]
        return result

    async def write(self, buffer, timeout=None):  # noqa: ASYNC109 - fixed httpcore stream protocol
        pass

    async def aclose(self):
        pass

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):  # noqa: ASYNC109 - fixed httpcore stream protocol
        self.server_hostname = server_hostname
        return self

    def get_extra_info(self, info):
        return None


class RecordingBackend(httpcore.AsyncNetworkBackend):
    def __init__(self):
        self.hosts = []
        self.stream = RecordingStream()

    async def connect_tcp(self, host, port, **kwargs):
        self.hosts.append(host)
        return self.stream


async def test_actual_connect_uses_a_literal_public_ip_while_tls_and_metadata_keep_hostname():
    transport = httpx.AsyncHTTPTransport()
    backend = RecordingBackend()
    transport._pool._network_backend = backend
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        response = await bounded_request(
            client, "GET", "https://source.test/export", max_bytes=10, public_only=True
        )
    assert backend.hosts == ["8.8.8.8"]
    assert backend.stream.server_hostname == "source.test"
    assert response.request.url == "https://source.test/export"
    assert response.json() == {}


async def test_public_then_private_dns_rebinding_never_reaches_connect_tcp(monkeypatch):
    lookups = 0

    async def rebind(_host, _port):
        nonlocal lookups
        lookups += 1
        return ["8.8.8.8"] if lookups == 1 else ["127.0.0.1"]

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", rebind)
    transport = httpx.AsyncHTTPTransport()
    backend = RecordingBackend()
    transport._pool._network_backend = backend
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        with pytest.raises(SourceDownloadError, match="non-public"):
            await bounded_request(
                client, "GET", "https://source.test/export", max_bytes=10, public_only=True
            )
    assert lookups == 2 and backend.hosts == []


@pytest.mark.parametrize("target", ["https://target.test/final", "http://source.test/final"])
async def test_cross_origin_redirects_cannot_reintroduce_default_auth_or_api_keys(target):
    received = []

    def handler(request):
        received.append(dict(request.headers))
        if len(received) == 1:
            return httpx.Response(302, headers={"Location": target})
        if len(received) == 2:
            return httpx.Response(302, headers={"Location": "/last"})
        return httpx.Response(200, content=b"{}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer synthetic-only", "chave-api-dados": "synthetic-only"},
        auth=httpx.BasicAuth("synthetic-user", "synthetic-password"),
    ) as client:
        await bounded_request(
            client,
            "GET",
            "https://source.test/start",
            max_bytes=10,
            public_only=True,
            allow_http=True,
            headers={"chave-api-dados": "explicit-synthetic"},
        )
    assert "authorization" in received[0]
    assert all("authorization" not in row and "chave-api-dados" not in row for row in received[1:])


@pytest.mark.parametrize(
    "url",
    [
        "ftp://source.test/export",
        "https:///export",
        "https://user:secret@source.test/export",
        "https://source.test:99999/export",
        "https://224.0.0.1/export",
        "https://[::1]/export",
        "http://source.test/export",
    ],
)
async def test_unsafe_source_urls_are_rejected_before_a_request(url):
    with pytest.raises(SourceDownloadError):
        await validate_public_url(url)


async def test_mixed_or_unavailable_dns_is_rejected(monkeypatch):
    async def mixed(_host, _port):
        return ["8.8.8.8", "10.0.0.1"]

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", mixed)
    with pytest.raises(SourceDownloadError, match="non-public"):
        await validate_public_url("https://source.test")

    async def failed(_host, _port):
        raise OSError("synthetic DNS failure")

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", failed)
    with pytest.raises(SourceDownloadError, match="DNS"):
        await validate_public_url("https://source.test")


async def test_proxy_cannot_resolve_the_untrusted_destination_on_our_behalf():
    async with httpx.AsyncClient(proxy="http://proxy.test:8080", trust_env=False) as client:
        with pytest.raises(SourceDownloadError, match="direct HTTP transport"):
            await bounded_request(
                client, "GET", "https://source.test", max_bytes=10, public_only=True
            )


@pytest.mark.parametrize("length", ["invalid", "-1", "100"])
async def test_invalid_or_excessive_declared_size_is_rejected(length):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"Content-Length": length}, content=b"{}")
        )
    ) as client:
        with pytest.raises(SourceDownloadError):
            await bounded_request(client, "GET", "https://source.test", max_bytes=10)


async def test_gzip_body_is_limited_after_decompression_and_is_not_decoded_twice():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"Content-Encoding": "gzip"}, content=gzip.compress(b"{}")
            )
        )
    ) as client:
        response = await bounded_request(client, "GET", "https://source.test", max_bytes=100)
    assert response.json() == {} and "content-encoding" not in response.headers


@pytest.mark.parametrize("location,max_redirects", [(None, 3), ("/loop", 0)])
async def test_unusable_redirects_fail_explicitly(location, max_redirects):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(302, headers={"Location": location} if location else {})
        )
    ) as client:
        with pytest.raises(SourceDownloadError, match="redirect"):
            await bounded_request(
                client, "GET", "https://source.test", max_bytes=10, max_redirects=max_redirects
            )
