"""Supervised subprocess fixture, shared by browser and process tests."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

from app.services.application_tests import ApplicationTestService

REPO = Path(__file__).resolve().parents[2]


def wait_for(predicate, seconds=35):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("Condição do processo não foi alcançada no prazo")


class Runtime:
    def __init__(self, root):
        from app.config import Settings

        self.root = root
        with socket.socket() as port_socket:
            port_socket.bind(("127.0.0.1", 0))
            self.port = port_socket.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.env = ApplicationTestService(Settings(_env_file=None))._pytest_environment(root)
        self.env.update(
            LICITA_RUNTIME_ROOT=str(root),
            LICITA_RUNTIME_PORT=str(self.port),
            CRAWL_MAX_RECORDS_PER_SOURCE="1",
            LOG_LEVEL="WARNING",
        )
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=REPO,
            env=self.env,
            capture_output=True,
            timeout=90,
        )  # noqa: S603
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        self.children = []
        self.logs = []
        self.start("api")

        def healthy():
            try:
                return httpx.get(self.url + "/health", timeout=1).status_code == 200
            except httpx.HTTPError:
                return False

        try:
            wait_for(healthy)
        except AssertionError:
            self.close()
            raise AssertionError((root / "api-0.log").read_text(errors="replace")) from None

    def start(self, role):
        environment = {**self.env, "LICITA_RUNTIME_ROLE": role}
        log = (self.root / f"{role}-{len(self.children)}.log").open("wb")
        command = [sys.executable, "-m", "tests.support.runtime"]
        if os.name == "nt":
            command = [
                sys.executable,
                str(REPO / "app/services/subprocess_supervisor.py"),
                *command,
            ]
        child = subprocess.Popen(  # noqa: S603 - fixed local test runtime, no shell
            command,
            cwd=REPO,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            start_new_session=os.name != "nt",
        )  # noqa: S603
        self.children.append(child)
        self.logs.append(log)
        return child

    def stop(self, child):
        if child.poll() is None:
            if os.name == "nt":
                child.kill()  # Job Object closes and terminates every descendant.
            else:
                import signal

                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=15)

    def close(self):
        for child in reversed(self.children):
            self.stop(child)
        for log in self.logs:
            log.close()
