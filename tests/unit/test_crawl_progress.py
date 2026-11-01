import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from app.connectors.http import AsyncHTTPClient
from app.connectors.progress import ProgressEvent
from app.models import CrawlRun, CrawlRunStatus
from app.services.crawl_monitor import elapsed_label, monitor_view, nonnegative_int
from app.services.crawl_progress import CrawlProgressRecorder
from app.web import _crawl_view


async def test_recorder_coalesces_signals_and_preserves_cursor_and_twenty_activities():
    moment = [0.0]
    recorder = CrawlProgressRecorder(clock=lambda: moment[0])
    run = CrawlRun(cursor={"sentinel": "keep", "sources": {"pncp": {"status": "running"}}})

    async def save():
        recorder.apply(run)

    persist = AsyncMock(side_effect=save)
    await recorder.observe(
        ProgressEvent(stage="discovery", source="pncp", message="Buscando"), persist
    )
    for page in range(1, 31):
        await recorder.observe(ProgressEvent(page=page, message="Resposta recebida"), persist)
    assert persist.await_count == 1
    assert len(recorder.activity) == 20
    assert run.cursor["progress"].get("page") is None
    moment[0] = 2.0
    await recorder.observe(ProgressEvent(page=31), persist)
    assert run.cursor["progress"]["page"] == 31
    assert run.cursor["sentinel"] == "keep"
    assert run.cursor["sources"]["pncp"]["status"] == "running"
    moment[0] = 2.25
    await recorder.observe(ProgressEvent(stage="details", processed=0, total=5), persist)
    assert persist.await_count == 3
    assert "page" not in run.cursor["progress"]
    assert len(run.cursor["activity"]) == 20


async def test_resource_telemetry_is_nonblocking_and_preserves_current_stage():
    recorder = CrawlProgressRecorder()
    recorder.record(ProgressEvent(stage="details", source="pncp", processed=3, total=8))
    entered, release = asyncio.Event(), asyncio.Event()

    async def persist():
        entered.set()
        await release.wait()

    recorder.enqueue(ProgressEvent(source="compras_gov", http_active=20, http_limit=20), persist)
    await asyncio.wait_for(entered.wait(), timeout=2)
    assert recorder.progress["source"] == "pncp" and recorder.progress["processed"] == 3
    assert recorder.parallel["compras_gov"]["http_active"] == 20
    await recorder.aclose()


async def test_http_signals_real_retries_pages_and_omits_credentials():
    requests = 0
    events = []
    moment = [0.0]

    async def sleep(delay):
        moment[0] += delay

    async def transport(request):
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(200, json={"data": [], "totalPaginas": 4})

    async def observe(event):
        events.append(event)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        http = AsyncHTTPClient(
            client=client,
            max_retries=1,
            sleep=sleep,
            clock=lambda: moment[0],
            on_activity=observe,
            settings={"http_min_request_interval_seconds": 0},
        )
        await http.request_json(
            "GET",
            "https://official.test/api",
            params={"pagina": 2},
            headers={"Authorization": "secret-value"},
        )
    retries = [event for event in events if event.retry_at]
    assert len(retries) == 1 and retries[0].attempt == 2
    assert events[-1].page == 2 and events[-1].total_pages == 4
    assert all(event.http_active is not None for event in events)
    assert "secret-value" not in repr(events) and "official.test" not in repr(events)


def test_monitor_has_honest_stage_percentage_counts_and_independent_source_states():
    now = datetime.now(UTC)
    run = CrawlRun(
        connector="all",
        status=CrawlRunStatus.RUNNING,
        records_found=20,
        records_created=2,
        records_updated=3,
        started_at=now - timedelta(seconds=65),
        heartbeat_at=now - timedelta(seconds=151),
        filters={},
        cursor={
            "progress": {
                "stage": "details",
                "source": "compras_gov",
                "processed": 3,
                "total": 10,
                "subject": "<script>https://user:secret@host/?token=abc",
            },
            "sources": {
                "pncp": {"status": "completed", "records_found": 10},
                "compras_gov": {
                    "status": "running",
                    "records_failed": 1,
                    "progress": {"processed": 3, "total": 10},
                },
            },
        },
    )
    view = monitor_view(run, _crawl_view(run), now=now)
    assert view.percentage == 30 and view.examined == 13
    assert view.created == 2 and view.updated == 3 and view.failed == 1
    assert view.elapsed == "1 min 05 s" and view.stale
    assert [source.status for source in view.sources] == ["completed", "running"]
    assert "secret" not in view.subject
    run.cursor = {"progress": {"stage": "discovery"}}
    view = monitor_view(run, _crawl_view(run), now=now)
    assert view.percentage is None and view.total is None
    assert [source.status for source in view.sources] == ["pending", "pending"]


def test_municipal_atomic_progress_and_legacy_values_do_not_claim_committed_rows():
    now = datetime.now(UTC)
    run = CrawlRun(
        connector="municipal:sao_luis",
        status=CrawlRunStatus.RUNNING,
        records_found=100,
        records_created=0,
        filters={},
        heartbeat_at=now,
        cursor={"stage": "persisting", "processed": 50, "total": 100},
    )
    view = monitor_view(run, _crawl_view(run), now=now)
    assert view.percentage is None and view.created == 0
    assert "aguardando confirmação" in view.message and not view.stale
    run.status = CrawlRunStatus.CANCELLED
    view = monitor_view(run, _crawl_view(run), now=now)
    assert view.stage == "cancelled" and view.percentage is None
    assert elapsed_label(None, now) == "Não informado"
    assert elapsed_label(now + timedelta(days=1), now) == "0 s"
    assert elapsed_label(now - timedelta(hours=2), now) == "2 h 00 min"


@pytest.mark.parametrize("value", [None, True, {}, "bad", -1, float("inf")])
def test_invalid_legacy_counters_are_safe(value):
    assert nonnegative_int(value) == 0


def test_monitor_preserves_old_waiting_and_formats_new_optional_wait_reasons():
    now = datetime.now(UTC)
    run = CrawlRun(
        connector="pncp",
        status=CrawlRunStatus.RUNNING,
        filters={},
        records_found=0,
        records_created=0,
        records_updated=0,
        heartbeat_at=now,
        cursor={"parallel": {"pncp": {"http_limit": 20, "http_waiting": 1}}},
    )
    old = monitor_view(run, _crawl_view(run), now=now).parallel[0]
    assert not old.detailed_waiting and old.http_waiting == 1
    run.cursor["parallel"]["pncp"].update(
        http_waiting=7,
        http_waiting_rate=4,
        http_waiting_capacity=1,
        http_waiting_checkpoint=1,
        http_waiting_retry=1,
        http_rate_interval_seconds=1.25,
        http_cooldown_seconds=3.5,
    )
    current = monitor_view(run, _crawl_view(run), now=now).parallel[0]
    assert current.detailed_waiting and current.http_waiting == 7
    assert (
        current.http_waiting_rate,
        current.http_waiting_capacity,
        current.http_waiting_checkpoint,
        current.http_waiting_retry,
    ) == (4, 1, 1, 1)
    assert current.interval == "1,25" and current.cooldown == "3,5"
    run.status = CrawlRunStatus.CANCELLED
    terminal = monitor_view(run, _crawl_view(run), now=now).parallel[0]
    assert terminal.http_waiting == terminal.http_waiting_rate == 0
    assert terminal.http_waiting_capacity == terminal.http_waiting_checkpoint == 0
    assert terminal.http_waiting_retry == 0 and terminal.cooldown == "0"
