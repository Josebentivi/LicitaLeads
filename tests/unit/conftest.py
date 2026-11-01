"""Keep connector URL-policy tests offline while still checking literal IPs."""

import pytest


@pytest.fixture(autouse=True)
def offline_source_dns(monkeypatch):
    async def resolve(_host, _port):
        return ["8.8.8.8"]

    monkeypatch.setattr("app.connectors.bounded_http.resolve_addresses", resolve)
