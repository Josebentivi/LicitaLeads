"""Stdlib-only IPC shared by the launcher and its application children."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

CONTROL_ENV = "LICITA_CONTROL_DIRECTORY"
INSTANCE_ENV = "LICITA_CONTROL_INSTANCE"
ROLE_ENV = "LICITA_CONTROL_ROLE"
SHUTDOWN_SECONDS = 60
_SHARING_ERRORS = {5, 32, 33}
_file_lock = threading.Lock()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".control-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        deadline = time.monotonic() + 1
        while True:
            try:
                os.replace(name, path)
                break
            except OSError as exc:
                # A Windows reader can briefly deny replacement of its open file.
                if getattr(exc, "winerror", None) not in _SHARING_ERRORS or (
                    time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.02)
    finally:
        Path(name).unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def database_key(url: str) -> str:
    return hashlib.sha256(url.replace("+aiosqlite", "").encode()).hexdigest()


class RuntimeChannel:
    def __init__(self, directory: Path, instance: str):
        self.directory, self.instance = directory, instance

    @classmethod
    def from_environment(cls) -> RuntimeChannel | None:
        directory, instance = os.environ.get(CONTROL_ENV), os.environ.get(INSTANCE_ENV)
        if not directory or not instance:
            return None
        channel = cls(Path(directory), instance)
        if channel.read("manifest").get("instance") != instance:
            return None
        return channel

    def read(self, name: str) -> dict[str, Any]:
        return read_json(self.directory / f"{name}.json")

    def write(self, name: str, value: dict[str, Any]) -> None:
        atomic_json(self.directory / f"{name}.json", {**value, "instance": self.instance})

    @property
    def available(self) -> bool:
        heartbeat = self.read("heartbeat").get("at", 0)
        return isinstance(heartbeat, (float, int)) and 0 <= time.time() - heartbeat < 5

    @property
    def requested(self) -> bool:
        return self.read("request").get("instance") == self.instance

    @property
    def receipt_path(self) -> Path:
        return Path(self.read("manifest")["receipt"])

    def receipt(self) -> dict[str, Any]:
        return read_json(self.receipt_path)

    def public_status(self) -> dict[str, Any]:
        owner, api = self.read("launcher"), self.read("api")
        phase = owner.get("phase", api.get("phase", "requested" if self.requested else "running"))
        return {
            "id": self.instance if self.requested else None,
            "available": self.available,
            "phase": phase,
            "detail": api.get("detail", ""),
            "forced": owner.get("forced", []),
            "scheduler_stopped": owner.get("scheduler_stopped", False),
        }


def shutdown_requested() -> bool:
    channel = RuntimeChannel.from_environment()
    return channel is not None and channel.requested


def register_lease(name: str, owner: str, generation: int) -> None:
    """Remember only this instance's leases; never release another worker's lease."""
    channel = RuntimeChannel.from_environment()
    if channel is None:
        return
    with _file_lock:
        key = f"leases-{os.getpid()}"
        leases = channel.read(key).get("leases", [])
        leases = [item for item in leases if item["name"] != name]
        leases.append({"name": name, "owner": owner, "generation": generation})
        channel.write(key, {"leases": leases})


class OwnedRuntime:
    """Private launcher channel. Creating it is never part of --check."""

    def __init__(self, receipt: Path, database_url: str):
        self.temporary = tempfile.TemporaryDirectory(prefix="licitaleads-control-")
        self.channel = RuntimeChannel(Path(self.temporary.name), secrets.token_hex(24))
        self.channel.write(
            "manifest",
            {
                "token": secrets.token_urlsafe(32),
                "receipt": str(receipt.resolve()),
                "database": database_key(database_url),
                "started_at": time.time(),
            },
        )
        self.stop_event = threading.Event()
        self._heartbeat()
        self.thread = threading.Thread(target=self._pulse, daemon=True, name="launcher-control")
        self.thread.start()

    @property
    def environment(self) -> dict[str, str]:
        return {CONTROL_ENV: str(self.channel.directory), INSTANCE_ENV: self.channel.instance}

    def _heartbeat(self) -> None:
        self.channel.write("heartbeat", {"at": time.time()})

    def _pulse(self) -> None:
        while not self.stop_event.wait(0.5):
            try:
                self._heartbeat()
            except OSError:
                return

    def close(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)
        self.temporary.cleanup()
