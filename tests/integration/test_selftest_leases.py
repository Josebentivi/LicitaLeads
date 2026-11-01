import asyncio
from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.models import JobLease
from app.services.application_tests import ApplicationTestService, TestReport


async def test_two_services_cannot_start_same_validation_and_old_fence_cannot_publish(
    database, tmp_path
):
    first = ApplicationTestService(
        Settings(_env_file=None),
        session_factory=database.sessions,
        report_path=tmp_path / "report.json",
    )
    second = ApplicationTestService(
        Settings(_env_file=None),
        session_factory=database.sessions,
        report_path=tmp_path / "report.json",
    )
    a, b = await asyncio.gather(
        first._acquire_run_lease("first"), second._acquire_run_lease("second")
    )
    assert sorted([a[0] is None, b[0] is None]) == [False, True]
    winner, identifier = (first, "first") if a[0] else (second, "second")
    async with database.sessions() as session, session.begin():
        lease = await session.get_one(JobLease, "selftest:application")
        lease.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    replacement = ApplicationTestService(
        Settings(_env_file=None),
        session_factory=database.sessions,
        report_path=tmp_path / "report.json",
    )
    assert (await replacement._acquire_run_lease("replacement"))[0]
    now = datetime.now(UTC)
    report = TestReport(identifier, "full", now, now, [])
    assert "Lease perdida" in await winner._save_report(report)
    assert not winner.report_path.exists()
    assert list((tmp_path / "report-history").glob("*.json"))
    assert not await winner._release_run_lease("selftest:application", "selftest:" + identifier)


async def test_denied_admission_archives_failure_without_overwriting_latest(database, tmp_path):
    path = tmp_path / "report.json"
    path.write_text("latest-preserved")
    holder = ApplicationTestService(
        Settings(_env_file=None), session_factory=database.sessions, report_path=path
    )
    assert (await holder._acquire_run_lease("holder"))[0]
    rejected = ApplicationTestService(
        Settings(_env_file=None), session_factory=database.sessions, report_path=path
    )
    state = rejected.start("quick", object())
    await rejected._task
    assert state.status == "failed" and path.read_text() == "latest-preserved"
    assert list((tmp_path / "report-history").glob("*.json"))
