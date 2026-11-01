"""Job policy, fail-closed setup and argument preservation without joining pytest's job."""

from __future__ import annotations

import ctypes
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.services import subprocess_supervisor as supervisor


@pytest.mark.parametrize("failure", [None, "create", "configure", "assign"])
def test_job_is_kill_on_close_and_setup_errors_do_not_spawn(failure, monkeypatch):
    def configure(handle, information_class, pointer, size):
        limits = pointer._obj
        assert handle == 123
        assert information_class == 9
        assert limits.BasicLimitInformation.LimitFlags == 0x2000
        assert limits.BasicLimitInformation.ActiveProcessLimit == 0
        assert size == ctypes.sizeof(limits)
        return failure != "configure"

    kernel = SimpleNamespace(
        CreateJobObjectW=Mock(return_value=0 if failure == "create" else 123),
        SetInformationJobObject=Mock(side_effect=configure),
        GetCurrentProcess=Mock(return_value=-1),
        AssignProcessToJobObject=Mock(return_value=failure != "assign"),
        CloseHandle=Mock(return_value=True),
    )
    monkeypatch.setattr(supervisor.ctypes, "WinDLL", lambda *args, **kwargs: kernel, raising=False)
    monkeypatch.setattr(supervisor.ctypes, "get_last_error", lambda: 5, raising=False)
    monkeypatch.setattr(
        supervisor.ctypes, "WinError", lambda code: OSError(code, "denied"), raising=False
    )
    if failure:
        with pytest.raises(OSError, match="denied"):
            supervisor._join_job()
        if failure == "create":
            kernel.SetInformationJobObject.assert_not_called()
            kernel.CloseHandle.assert_not_called()
        else:
            kernel.CloseHandle.assert_called_once_with(123)
        if failure != "assign":
            kernel.AssignProcessToJobObject.assert_not_called()
    else:
        assert supervisor._join_job() == 123
        kernel.CreateJobObjectW.assert_called_once_with(None, None)
        kernel.AssignProcessToJobObject.assert_called_once_with(123, -1)
        # The only handle is retained until process teardown; children cannot
        # keep the job alive after the supervisor is killed.
        kernel.CloseHandle.assert_not_called()


@pytest.mark.parametrize("platform", ["nt", "posix"])
def test_supervisor_joins_before_command_and_preserves_exit_code(platform, monkeypatch):
    order = []
    command = ["python.exe", "script with spaces.py", "literal;$value"]

    def join():
        order.append("join")
        return 123

    def run(arguments, *, creationflags):
        order.append("spawn")
        assert arguments == command
        assert creationflags == getattr(supervisor.subprocess, "CREATE_NO_WINDOW", 0)
        return 7

    monkeypatch.setattr(supervisor, "os", SimpleNamespace(name=platform))
    monkeypatch.setattr(supervisor, "_join_job", join)
    monkeypatch.setattr(supervisor.sys, "argv", ["supervisor.py", *command])
    monkeypatch.setattr(supervisor.subprocess, "call", run)
    assert supervisor.main() == 7
    assert order == (["join", "spawn"] if platform == "nt" else ["spawn"])


def test_missing_command_and_failed_job_never_start_target(monkeypatch):
    start = Mock()
    monkeypatch.setattr(supervisor.subprocess, "call", start)
    monkeypatch.setattr(supervisor.sys, "argv", ["supervisor.py"])
    assert supervisor.main() == 2
    monkeypatch.setattr(supervisor, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(supervisor.sys, "argv", ["supervisor.py", "python.exe"])
    monkeypatch.setattr(supervisor, "_join_job", Mock(side_effect=OSError("job unavailable")))
    with pytest.raises(OSError, match="job unavailable"):
        supervisor.main()
    start.assert_not_called()
