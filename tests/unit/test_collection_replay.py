"""Saved official responses reconstruct pagination without new prefix requests."""

import asyncio
from datetime import UTC, datetime

import pytest

from app.connectors.base import RawSourceRecord
from app.connectors.continuation import ResponseReplay, page_budget_exhausted
from app.connectors.http import ConnectorContractError, HTTPJSONResponse


class Transport:
    def __init__(self, total=3):
        self.total = total
        self.requests = []

    async def request_json(self, method, url, **kwargs):
        page = kwargs["params"]["pagina"]
        self.requests.append(page)
        return HTTPJSONResponse(
            payload={"data": [page], "totalPaginas": self.total, "totalRegistros": self.total},
            url=url,
            status_code=200,
            request_parameters=kwargs["params"],
            headers={},
            collected_at=datetime.now(UTC),
        )


async def test_resumption_replays_prefix_and_downloads_only_frontier_and_next_page():
    saved = []

    async def save(raw):
        saved.append(raw.model_dump(mode="json"))

    async def invalidate():
        raise AssertionError("unchanged totals")

    previous = []
    for expected in ([1], [1, 2], [2, 3]):
        transport = Transport()
        replay = ResponseReplay(transport, previous, save, invalidate, source="pncp")
        rows = []
        for page in range(1, 4):
            response = await transport.request_json(
                "GET", "https://official.test/records", params={"pagina": page}
            )
            rows.extend(response.payload["data"])
            if page < 3 and page_budget_exhausted(transport, page, 1):
                break
        assert transport.requests == expected
        previous = [
            RawSourceRecord.model_validate(row.model_dump(mode="json"))
            for row in replay.cache.values()
        ]
    assert rows == [1, 2, 3]


async def test_frontier_total_change_requires_restart_and_preserves_received_response():
    old = RawSourceRecord.build(
        source="pncp",
        endpoint="https://official.test/records",
        request_parameters={"pagina": 1},
        raw_payload={"data": [1], "totalPaginas": 2},
        http_status=200,
        collected_at=datetime.now(UTC),
    )
    saved, invalidated = [], []

    async def save(raw):
        saved.append(raw)

    async def invalidate():
        invalidated.append(True)

    transport = Transport(total=3)
    replay = ResponseReplay(transport, [old], save, invalidate, source="pncp")
    with pytest.raises(ConnectorContractError):
        await transport.request_json("GET", old.endpoint, params={"pagina": 1})
    assert invalidated and saved[0].raw_payload["totalPaginas"] == 3
    assert replay.invalidated


async def test_parallel_replay_shares_identical_request_without_cancelling_other_waiter():
    entered, release = asyncio.Event(), asyncio.Event()
    calls, saved = [], []

    class Delayed(Transport):
        async def request_json(self, method, url, **kwargs):
            calls.append(url)
            entered.set()
            await release.wait()
            return await super().request_json(method, url, **kwargs)

    async def save(raw):
        saved.append(raw)

    async def invalidate():
        raise AssertionError("unchanged totals")

    transport = Delayed()
    replay = ResponseReplay(transport, [], save, invalidate, source="pncp")
    first = asyncio.create_task(
        transport.request_json("GET", "https://official.test/records", params={"pagina": 1})
    )
    await entered.wait()
    second = asyncio.create_task(
        transport.request_json("GET", "https://official.test/records", params={"pagina": 1})
    )
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    assert (await second).payload["data"] == [1]
    assert len(calls) == len(saved) == 1 and not replay._inflight


async def test_page_budgets_are_local_to_each_concurrent_operation():
    a_received, b_received = asyncio.Event(), asyncio.Event()

    async def ignore(*args):
        pass

    transport = Transport()
    ResponseReplay(transport, [], ignore, ignore, source="pncp")

    async def a():
        await transport.request_json("GET", "https://official.test/a", params={"pagina": 1})
        a_received.set()
        await b_received.wait()
        return page_budget_exhausted(transport, 1, 1)

    async def b():
        await a_received.wait()
        await transport.request_json("GET", "https://official.test/b", params={"pagina": 1})
        b_received.set()
        return page_budget_exhausted(transport, 1, 2)

    assert await asyncio.gather(a(), b()) == [True, False]


async def test_total_change_invalidates_only_the_affected_partition():
    def old(unit, total):
        return RawSourceRecord.build(
            source="pncp",
            endpoint="https://official.test/records",
            request_parameters={"pagina": 1, "unit": unit},
            raw_payload={"totalPaginas": total},
            http_status=200,
            collected_at=datetime.now(UTC),
        )

    affected, saved = [], []

    async def save(raw):
        saved.append(raw)

    async def invalidate():
        raise AssertionError("must not invalidate the whole source")

    async def partition(operation):
        affected.append(operation)

    transport = Transport(total=3)
    replay = ResponseReplay(
        transport,
        [old("a", 2), old("b", 3)],
        save,
        invalidate,
        source="pncp",
        invalidate_partition=partition,
    )
    with pytest.raises(ConnectorContractError):
        await transport.request_json(
            "GET", "https://official.test/records", params={"pagina": 1, "unit": "a"}
        )
    result = await transport.request_json(
        "GET", "https://official.test/records", params={"pagina": 1, "unit": "b"}
    )
    assert result.payload["totalPaginas"] == 3
    assert len(affected) == 1 and len(saved) == 2
    assert [raw.request_parameters["unit"] for raw in replay.cache.values()] == ["b"]
