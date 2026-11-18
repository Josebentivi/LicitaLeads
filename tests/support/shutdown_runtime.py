"""Exercise the real shutdown supervisor with controlled local workers only."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

from app.runtime_control import OwnedRuntime


def run_managed():
    from scripts import launcher

    root = Path(os.environ["LICITA_RUNTIME_ROOT"])
    runtime = OwnedRuntime(root / "application.shutdown.json", os.environ["DATABASE_URL"])
    environment = {**os.environ, **runtime.environment}
    scheduler = launcher._Scheduler()
    children = []
    logs = []
    try:
        for role, module in [
            ("scheduler", "tests.support.shutdown_runtime"),
            ("api", "tests.support.runtime"),
        ]:
            log = (root / f"managed-{role}.log").open("wb")
            logs.append(log)
            child = subprocess.Popen(  # noqa: S603 - controlled local test modules
                [sys.executable, "-m", module],
                cwd=launcher.REPO_ROOT,
                env={**environment, "LICITA_RUNTIME_ROLE": role, "LICITA_CONTROL_ROLE": role},
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                start_new_session=os.name != "nt",
            )
            launcher._assign_kill_on_close_job(child)
            children.append(child)
        scheduler.process = children[0]
        if (root / "force-shutdown-test").exists():
            launcher.SHUTDOWN_SECONDS = 3
        (root / "owned-processes.json").write_text(
            json.dumps(
                {
                    "api": children[1].pid,
                    "scheduler": children[0].pid,
                    "launcher": os.getpid(),
                }
            ),
            encoding="utf-8",
        )
        launcher._wait_for_server(children[1], runtime, scheduler)
    finally:
        for child in children:
            launcher._stop_owned_tree(child)
        for log in logs:
            log.close()
        runtime.close()


async def scheduler():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from app.jobs import scheduler as jobs
    from app.services.application_tests import ApplicationTestService

    root = Path(os.environ["LICITA_RUNTIME_ROOT"])

    async def controlled_job():
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import time; time.sleep(300)",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            start_new_session=os.name != "nt",
        )
        (root / "owned-grandchild.pid").write_text(str(child.pid))
        try:
            await asyncio.Event().wait()
        finally:
            if (root / "force-shutdown-test").exists():
                # Deliberately model a hung worker; only the isolated supervisor kills it.
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        pass
            await ApplicationTestService._terminate_process(child)
            (root / "scheduler-job-cancelled").touch()

    def build():
        instance = AsyncIOScheduler()
        instance.add_job(jobs._leased, "date", args=["shutdown-test", controlled_job])
        return instance

    jobs.build_scheduler = build
    await jobs.serve()


if __name__ == "__main__":
    asyncio.run(scheduler())
