"""One-click bootstrap that prepares and starts LicitaLead Monitor.

Only the Python standard library is used, so the launcher can run before the
project virtual environment exists. The first execution provisions everything
(virtual environment, dependencies, configuration, database); later executions
are quick and just start the platform.
"""

from __future__ import annotations

import argparse
import atexit
import ctypes
import hashlib
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing, suppress
from pathlib import Path
from typing import IO

REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_PATH = Path(__file__).resolve()
SCHEDULER_LOG = REPO_ROOT / "data" / "scheduler.log"
SCHEDULER_PID = REPO_ROOT / "data" / "scheduler.pid"
PYTHON_DOWNLOAD_URL = "https://www.python.org/downloads/windows/"
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
CLOUD_SYNC_MARKERS = ("My Drive", "Meu Drive", "Google Drive", "OneDrive")

SETTINGS_SNIPPET = """\
import json

from app.config import get_settings

settings = get_settings()
settings.ensure_directories()
print(json.dumps({
    "host": settings.app_host,
    "port": settings.app_port,
    "scheduler_enabled": settings.scheduler_enabled,
    "database_url": settings.database_url,
}))
"""

DATABASE_SNIPPET = """\
import asyncio
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from app.database import engine


async def check() -> int:
    config = Config(str(Path("alembic.ini")))
    expected = ScriptDirectory.from_config(config).get_current_head()
    try:
        async with engine.connect() as connection:
            current = await connection.scalar(text("SELECT version_num FROM alembic_version"))
    except Exception as exc:
        print(f"banco inacessivel ({exc.__class__.__name__})")
        return 1
    if current != expected:
        print(f"banco na revisao {current!r}; esperada {expected!r}")
        return 1
    print(f"banco na revisao {expected}")
    return 0


raise SystemExit(asyncio.run(check()))
"""


class LauncherError(RuntimeError):
    """Raised when the launcher cannot continue and needs user action."""


def _configure_console() -> None:
    """Use UTF-8 output so Portuguese messages render correctly."""

    for stream in (sys.stdout, sys.stderr):
        if stream is None:
            continue
        with suppress(ValueError, OSError):
            stream.reconfigure(encoding="utf-8", errors="replace")
    if os.name == "nt":
        with suppress(AttributeError, OSError):
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)


def _print_banner() -> None:
    print("=" * 58)
    print(" LicitaLead Monitor - inicializacao local")
    print("=" * 58)


def _venv_python_path(venv_dir: Path) -> Path:
    """Return the interpreter path inside a virtual environment."""

    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _looks_like_cloud_sync_path(path: Path) -> bool:
    """Detect Google Drive/OneDrive style folders by their path segments."""

    markers = tuple(marker.casefold() for marker in CLOUD_SYNC_MARKERS)
    for part in path.parts:
        folded = part.casefold()
        if any(folded == marker or folded.startswith(f"{marker} - ") for marker in markers):
            return True
    return False


def _volume_label(drive: str) -> str | None:
    """Return a Windows volume label, when it can be read."""

    if os.name != "nt":
        return None
    from ctypes import wintypes

    with suppress(AttributeError, OSError):
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetVolumeInformationW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.LPWSTR,
            wintypes.DWORD,
            wintypes.LPDWORD,
            wintypes.LPDWORD,
            wintypes.LPDWORD,
            wintypes.LPWSTR,
            wintypes.DWORD,
        ]
        kernel32.GetVolumeInformationW.restype = wintypes.BOOL
        label = ctypes.create_unicode_buffer(261)
        success = kernel32.GetVolumeInformationW(
            drive, label, len(label), None, None, None, None, 0
        )
        return label.value if success else None
    return None


def _onedrive_roots() -> list[Path]:
    """Return the OneDrive sync roots declared for the current user."""

    roots: list[Path] = []
    for variable in ("OneDrive", "OneDriveConsumer", "OneDriveCommercial"):
        value = os.environ.get(variable)
        if not value:
            continue
        with suppress(OSError, ValueError):
            roots.append(Path(value).resolve())
    return roots


def _is_synced_location(root: Path) -> bool:
    """Detect streamed drives where pip cannot read its CA bundle reliably."""

    if _looks_like_cloud_sync_path(root):
        return True
    try:
        resolved = root.resolve()
    except OSError:
        resolved = root
    for candidate in _onedrive_roots():
        with suppress(ValueError):
            if resolved.is_relative_to(candidate):
                return True
    drive, _ = os.path.splitdrive(str(root))
    label = _volume_label(drive + "\\") if drive else None
    return bool(label) and label.strip().lower() == "google drive"


def _local_venv_fallback() -> Path:
    """Return a virtual environment path on the local disk."""

    base = os.environ.get("LOCALAPPDATA") or os.environ.get("XDG_CACHE_HOME")
    if base:
        return Path(base) / "LicitaLeads" / "venv"
    return Path.home() / ".local" / "share" / "licitaleads" / "venv"


def _default_venv_dir() -> Path:
    """Prefer a local venv when the repository lives on a synced drive."""

    if _is_synced_location(REPO_ROOT):
        return _local_venv_fallback()
    return REPO_ROOT / ".venv"


def _state_file(venv_dir: Path) -> Path:
    return venv_dir / ".bootstrap-state.json"


def _local_data_dir() -> Path:
    """Return the local data directory used for the SQLite database."""

    return _local_venv_fallback().parent / "data"


def _relative_sqlite_filename(database_url: str) -> str | None:
    """Return the filename of a relative SQLite URL, when applicable."""

    if not database_url.startswith("sqlite") or ":memory:" in database_url:
        return None
    if "///" not in database_url:
        return None
    fragment = database_url.split("///", 1)[1].split("?", 1)[0]
    if not fragment:
        return None
    path = Path(fragment)
    if path.is_absolute():
        return None
    return path.name


def _sqlite_integrity_ok(path: Path) -> bool:
    """Return True when SQLite can read the database without corruption.

    A locked database is treated as usable: another process may be writing it
    and the caller must never quarantine a database that is simply in use.
    """

    if not path.exists():
        return False
    try:
        with closing(sqlite3.connect(str(path), timeout=15)) as connection:
            connection.execute("PRAGMA query_only=ON")
            row = connection.execute("PRAGMA quick_check").fetchone()
    except sqlite3.Error as exc:
        return "locked" in str(exc).casefold()
    return bool(row) and row[0] == "ok"


def _recover_with_cli(source: Path, destination: Path) -> bool:
    """Use the optional ``sqlite3`` shell ``.recover`` command."""

    dump = subprocess.run(
        ["sqlite3", str(source), ".recover"],
        capture_output=True,
        check=False,
    )
    if dump.returncode != 0 or not dump.stdout.strip():
        return False
    restore = subprocess.run(
        ["sqlite3", str(destination)],
        input=dump.stdout,
        capture_output=True,
        check=False,
    )
    return restore.returncode == 0


def _recover_with_dump(source: Path, destination: Path) -> bool:
    """Recover readable pages with the stdlib ``iterdump`` fallback."""

    try:
        with closing(sqlite3.connect(str(source), timeout=15)) as connection:
            statements = list(connection.iterdump())
    except sqlite3.Error:
        return False
    if not any(statement.startswith("CREATE TABLE") for statement in statements):
        return False
    try:
        with closing(sqlite3.connect(str(destination))) as target:
            target.executescript("\n".join(statements))
    except sqlite3.Error:
        return False
    return _sqlite_integrity_ok(destination)


def _recover_sqlite(source: Path, destination: Path) -> bool:
    """Best-effort recovery of a corrupt SQLite database.

    Returns True only when ``destination`` holds a readable copy; a failed
    attempt never leaves a partial file behind.
    """

    if not source.exists():
        return False
    with suppress(OSError):
        destination.unlink(missing_ok=True)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if shutil.which("sqlite3") and _recover_with_cli(source, destination):
        if _sqlite_integrity_ok(destination):
            return True
        with suppress(OSError):
            destination.unlink(missing_ok=True)
    if _recover_with_dump(source, destination):
        return True
    with suppress(OSError):
        destination.unlink(missing_ok=True)
    return False


def _quarantine_database(path: Path) -> Path | None:
    """Move a corrupt SQLite database (and its sidecars) aside."""

    stamp = time.strftime("%Y%m%d-%H%M%S")
    target = path.with_name(f"{path.stem}.corrupt-{stamp}{path.suffix}")
    try:
        path.replace(target)
    except OSError:
        return None
    for suffix in ("-wal", "-shm"):
        sidecar = path.with_name(path.name + suffix)
        if sidecar.exists():
            with suppress(OSError):
                sidecar.replace(target.with_name(target.name + suffix))
    return target


def _repair_local_database(path: Path) -> str | None:
    """Recover or quarantine an unusable SQLite database before startup."""

    if not path.exists() or _sqlite_integrity_ok(path):
        return None
    recovered = path.with_name(path.name + ".recovered")
    if _recover_sqlite(path, recovered):
        quarantined = _quarantine_database(path)
        try:
            recovered.replace(path)
        except OSError:
            return (
                f"Banco corrompido em {path} foi recuperado em {recovered}, "
                "mas nao foi possivel substituir o arquivo original."
            )
        suffix = f" Copia do arquivo anterior em {quarantined}." if quarantined else ""
        return f"Banco corrompido em {path} foi reparado.{suffix}"
    with suppress(OSError):
        recovered.unlink(missing_ok=True)
    quarantined = _quarantine_database(path)
    if quarantined is None:
        return (
            f"Banco corrompido em {path} nao pode ser recuperado nem preservado. "
            "Remova o arquivo manualmente para que um banco novo seja criado."
        )
    return (
        f"Banco corrompido em {path} nao pode ser recuperado; copia preservada em "
        f"{quarantined}. Um banco novo sera criado."
    )


def _migrate_database(source: Path, destination: Path) -> bool:
    """Copy a SQLite database to local disk, recovering a corrupt source.

    Healthy databases are copied with SQLite's transactional backup API;
    corrupt ones are rebuilt from readable pages before the copy. Returns True
    only when ``destination`` ends up with a readable database.
    """

    if not source.exists() or destination.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not _sqlite_integrity_ok(source):
        return _recover_sqlite(source, destination)
    try:
        with closing(sqlite3.connect(str(source), timeout=15)) as source_db:
            with closing(sqlite3.connect(str(destination))) as target_db:
                source_db.backup(target_db)
    except sqlite3.Error as exc:
        print(f"Aviso: nao foi possivel migrar o banco existente ({exc}).")
        with suppress(OSError):
            destination.unlink(missing_ok=True)
        return False
    if _sqlite_integrity_ok(destination):
        return True
    with suppress(OSError):
        destination.unlink(missing_ok=True)
    return False


def _database_override(
    settings: dict[str, object],
    *,
    migrate: bool,
) -> tuple[dict[str, str], str | None]:
    """Move the SQLite database to local disk when the repository is synced.

    User-configured absolute paths (or PostgreSQL) are respected; only the
    default repository-relative SQLite URL is redirected.
    """

    if not _is_synced_location(REPO_ROOT):
        return {}, None
    database_url = str(settings.get("database_url", ""))
    filename = _relative_sqlite_filename(database_url)
    if filename is None:
        return {}, None
    local_path = _local_data_dir() / filename
    notice = (
        "Pasta sincronizada detectada: banco SQLite no disco local em "
        f"{local_path} (evita travamentos do Google Drive/OneDrive)."
    )
    if migrate and not local_path.exists():
        legacy = _sqlite_database_path(database_url, REPO_ROOT)
        if legacy is not None and legacy != local_path and legacy.exists():
            if _migrate_database(legacy, local_path):
                notice += f" Dados existentes migrados de {legacy}."
            elif not _sqlite_integrity_ok(legacy):
                quarantined = _quarantine_database(legacy)
                preserved = quarantined if quarantined is not None else legacy
                notice += (
                    " O banco anterior estava corrompido e nao pode ser recuperado; "
                    f"copia preservada em {preserved}. Um banco novo sera criado."
                )
            else:
                notice += f" O banco anterior permanece em {legacy}."
    return {"DATABASE_URL": f"sqlite:///{local_path.as_posix()}"}, notice


def _hash_inputs(pyproject: Path, python_version: tuple[int, int, int]) -> str:
    """Hash the inputs that require a dependency reinstall when changed."""

    digest = hashlib.sha256()
    digest.update(pyproject.read_bytes())
    digest.update(".".join(str(part) for part in python_version).encode())
    return digest.hexdigest()


def _needs_install(state_file: Path, expected_hash: str, *, include_dev: bool = False) -> bool:
    """Decide whether dependencies must be (re)installed."""

    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return True
    if not isinstance(state, dict):
        return True
    if state.get("inputs_sha256") != expected_hash:
        return True
    return bool(state.get("include_dev", False)) != include_dev


def _confirm(question: str, *, input_fn: Callable[[str], str] = input) -> bool:
    """Ask a yes/no question accepting common Portuguese answers."""

    try:
        answer = input_fn(f"{question} [S/n] ").strip().lower()
    except EOFError:
        return False
    return answer in {"", "s", "sim", "y", "yes"}


def _browser_host(host: str) -> str:
    """Map bind-all addresses to a host a browser can reach."""

    if host in {"", "0.0.0.0", "::"}:  # noqa: S104
        return "127.0.0.1"
    return host


def _sqlite_database_path(database_url: str, root: Path) -> Path | None:
    """Return the SQLite file path for a URL, when it points to a file."""

    if not database_url.startswith("sqlite") or ":memory:" in database_url:
        return None
    if "///" not in database_url:
        return None
    fragment = database_url.split("///", 1)[1].split("?", 1)[0]
    if not fragment:
        return None
    path = Path(fragment)
    return path if path.is_absolute() else root / path


def _run_snippet(
    venv_python: Path,
    snippet: str,
    *,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run an inline program with the project interpreter."""

    environment = {**os.environ, **(env or {})}
    return subprocess.run(
        [str(venv_python), "-c", snippet],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )


def _load_settings(
    venv_python: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Read validated application settings or explain what is wrong."""

    result = _run_snippet(venv_python, SETTINGS_SNIPPET, env=env)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise LauncherError(
            "nao foi possivel carregar a configuracao; verifique o .env:\n" + detail
        )
    lines = result.stdout.strip().splitlines()
    try:
        settings = json.loads(lines[-1] if lines else "{}")
    except ValueError as exc:
        raise LauncherError("nao foi possivel ler a configuracao da aplicacao.") from exc
    if not isinstance(settings, dict):
        raise LauncherError("configuracao da aplicacao em formato inesperado.")
    return settings


def _python312_command() -> list[str] | None:
    """Return a command prefix that runs Python 3.12, when available."""

    if sys.version_info[:2] == (3, 12):
        return [sys.executable]
    candidates: list[list[str]] = []
    if os.name == "nt":
        candidates.append(["py", "-3.12"])
    candidates.extend([["python3.12"], ["python312"]])
    for candidate in candidates:
        try:
            probe = subprocess.run(
                [*candidate, "-c", "import sys; print(sys.version_info[:2])"],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            continue
        if probe.returncode == 0 and probe.stdout.strip() == "(3, 12)":
            return candidate
    return None


def _install_python_with_winget() -> bool:
    """Install Python 3.12 through winget when the user agrees."""

    winget = shutil.which("winget")
    if winget is None:
        return False
    print("Instalando o Python 3.12 com o winget (pode demorar alguns minutos)...")
    result = subprocess.run(
        [
            winget,
            "install",
            "--exact",
            "--id",
            "Python.Python.3.12",
            "--source",
            "winget",
            "--accept-package-agreements",
            "--accept-source-agreements",
        ],
        check=False,
    )
    return result.returncode == 0


def _ensure_python312() -> int | None:
    """Re-execute under Python 3.12, installing it when necessary.

    Returns ``None`` when the current interpreter is already 3.12, otherwise
    the process exit code of the nested execution or of the failed attempt.
    """

    if sys.version_info[:2] == (3, 12):
        return None

    print(f"Este launcher exige Python 3.12; versao atual: {sys.version.split()[0]}.", flush=True)
    found = _python312_command()
    if found is None:
        if sys.stdin.isatty() and _confirm("Deseja instalar o Python 3.12 agora via winget?"):
            if _install_python_with_winget():
                found = _python312_command()
            if found is None:
                print("A instalacao automatica nao deixou o Python 3.12 disponivel.")
        if found is None:
            print(f"Instale o Python 3.12 em: {PYTHON_DOWNLOAD_URL}")
            print("Depois feche esta janela e execute o iniciar.bat novamente.")
            return 1
    print("Reiniciando com o Python 3.12 encontrado...", flush=True)
    return subprocess.call(
        [*found, str(LAUNCHER_PATH), *sys.argv[1:]],
        cwd=REPO_ROOT,
    )


def _venv_python(venv_dir: Path) -> Path | None:
    """Return the working virtual environment interpreter, if usable."""

    python = _venv_python_path(venv_dir)
    if not python.exists():
        return None
    probe = subprocess.run(
        [str(python), "-m", "pip", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    return python if probe.returncode == 0 else None


def _prepare_venv(python312: list[str], *, venv_dir: Path, repair: bool) -> Path:
    """Create or reuse the project virtual environment."""

    if repair and venv_dir.exists():
        print("Removendo o ambiente virtual atual...")
        shutil.rmtree(venv_dir, ignore_errors=True)
    python = _venv_python(venv_dir)
    if python is not None:
        return python
    if venv_dir.exists():
        shutil.rmtree(venv_dir, ignore_errors=True)
    print(f"Criando o ambiente virtual em {venv_dir}...")
    venv_dir.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [*python312, "-m", "venv", str(venv_dir)],
        cwd=REPO_ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise LauncherError("falha ao criar a .venv; veja as mensagens acima.")
    python = _venv_python(venv_dir)
    if python is None:
        raise LauncherError("a .venv foi criada, mas o Python interno nao funciona.")
    return python


def _environment_ok(venv_python: Path, *, verbose: bool = False) -> bool:
    """Run the project environment check from inside the virtual environment."""

    check_script = REPO_ROOT / "scripts" / "check_environment.py"
    result = subprocess.run(
        [str(venv_python), str(check_script)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        if verbose and output:
            print(output)
        return False
    return True


def _install_dependencies(
    venv_python: Path,
    *,
    include_dev: bool,
    upgrade_pip: bool,
) -> None:
    """Install/upgrade the project dependencies inside the virtual environment."""

    target = ".[dev]" if include_dev else "."
    environment = os.environ.copy()
    environment["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    if upgrade_pip:
        upgrade = subprocess.run(
            [
                str(venv_python),
                "-m",
                "pip",
                "install",
                "--retries",
                "2",
                "--timeout",
                "30",
                "--upgrade",
                "pip",
            ],
            cwd=REPO_ROOT,
            env=environment,
            check=False,
        )
        if upgrade.returncode != 0:
            print("Aviso: nao foi possivel atualizar o pip; seguindo com a versao atual.")
    install = subprocess.run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--retries",
            "10",
            "--timeout",
            "60",
            "-e",
            target,
        ],
        cwd=REPO_ROOT,
        env=environment,
        check=False,
    )
    if install.returncode != 0:
        raise LauncherError(
            "falha ao instalar as dependencias; verifique a conexao e veja as mensagens acima."
        )


def _save_state(venv_dir: Path, inputs_hash: str, *, include_dev: bool) -> None:
    """Record the provisioning inputs for the next execution."""

    venv_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "inputs_sha256": inputs_hash,
        "include_dev": include_dev,
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    _state_file(venv_dir).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _ensure_env_file() -> bool:
    """Create .env from the example when missing; return True when created."""

    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        return False
    example = REPO_ROOT / ".env.example"
    if not example.exists():
        raise LauncherError(".env ausente e .env.example nao encontrado no repositorio.")
    shutil.copyfile(example, env_path)
    print("Arquivo .env criado a partir de .env.example.")
    return True


def _edit_env_file(env_path: Path) -> None:
    """Open the freshly created .env so the user can review it."""

    if os.name == "nt":
        print(
            "Ajuste o .env no Bloco de Notas (por exemplo DEFAULT_UF) e salve. "
            "O launcher continua quando a janela for fechada."
        )
        subprocess.run(["notepad", str(env_path)], check=False)
    else:
        print(f"Revise o arquivo {env_path} e execute o launcher novamente.")


def _probe_health(host: str, port: int, *, timeout: float = 1.5) -> bool:
    """Return True when the local health endpoint answers."""

    url = f"http://{_browser_host(host)}:{port}/health"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
            return response.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _port_in_use(host: str, port: int, *, timeout: float = 1.0) -> bool:
    """Return True when something accepts TCP connections on the port."""

    with socket.socket() as probe:
        probe.settimeout(timeout)
        return probe.connect_ex((_browser_host(host), port)) == 0


def _start_browser_thread(host: str, port: int, base_url: str, *, timeout: float = 120) -> None:
    """Open the browser once the server becomes healthy."""

    def worker() -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if _probe_health(host, port):
                webbrowser.open(base_url)
                return
            time.sleep(0.5)

    threading.Thread(target=worker, daemon=True).start()


def _assign_kill_on_close_job(process: subprocess.Popen[bytes]) -> object | None:
    """Tie the process lifetime to this launcher on Windows (best effort)."""

    if os.name != "nt":
        return None
    from ctypes import wintypes

    class BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.POINTER(ctypes.c_ulong)),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimitInformation),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = ExtendedLimitInformation()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    configured = kernel32.SetInformationJobObject(
        job,
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    assigned = configured and kernel32.AssignProcessToJobObject(job, process._handle)
    if not assigned:
        kernel32.CloseHandle(job)
        return None
    return job


def _read_scheduler_pid() -> int | None:
    """Return the pid left by a previous launcher session, when present."""

    try:
        text = SCHEDULER_PID.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return int(text) if text.isdigit() else None


def _pid_is_python(pid: int) -> bool:
    """Best-effort check that a live pid belongs to a python process.

    The process-name check avoids killing an unrelated program in the unlikely
    case of Windows pid reuse.
    """

    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return "python" in (result.stdout or "").lower()
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _stop_orphan_scheduler() -> None:
    """Stop a scheduler left behind by a previous launcher session.

    Only the pid recorded by the launcher is touched; a scheduler started
    manually has no pidfile and is left alone.
    """

    pid = _read_scheduler_pid()
    if pid is not None and pid != os.getpid() and _pid_is_python(pid):
        print(f"[--] Encerrando scheduler anterior (pid {pid})...")
        with suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                timeout=15,
            )
    with suppress(OSError):
        SCHEDULER_PID.unlink()


class _Scheduler:
    """Own the optional scheduler process and stop it with the launcher."""

    def __init__(self) -> None:
        self.process: subprocess.Popen[bytes] | None = None
        self.log: IO[bytes] | None = None
        self.job: object | None = None
        self.stopped = False

    def start(self, venv_python: Path, *, env: Mapping[str, str] | None = None) -> None:
        SCHEDULER_LOG.parent.mkdir(parents=True, exist_ok=True)
        self.log = SCHEDULER_LOG.open("ab")
        flags = 0
        if os.name == "nt":
            flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        self.process = subprocess.Popen(
            [str(venv_python), "-m", "app.jobs.scheduler"],
            cwd=REPO_ROOT,
            stdout=self.log,
            stderr=self.log,
            creationflags=flags,
            env={**os.environ, **(env or {})},
        )
        self.job = _assign_kill_on_close_job(self.process)
        with suppress(OSError):
            SCHEDULER_PID.write_text(str(self.process.pid), encoding="utf-8")

    def stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        if self.process is not None and _read_scheduler_pid() == self.process.pid:
            with suppress(OSError):
                SCHEDULER_PID.unlink()
        if self.log is not None:
            self.log.close()


def _install_signal_handlers(stop_callback: Callable[[], None]) -> None:
    """Stop the scheduler on Ctrl+C / termination before exiting."""

    def handler(signum: int, frame: object) -> None:
        del signum, frame
        stop_callback()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        number = getattr(signal, name, None)
        if number is None:
            continue
        with suppress(OSError, ValueError):
            signal.signal(number, handler)


def _run_server(venv_python: Path, *, env: Mapping[str, str] | None = None) -> int:
    """Run the API in the foreground until it exits."""

    environment = os.environ.copy()
    environment.update(env or {})
    scripts_dir = str(Path(venv_python).parent)
    environment["PATH"] = scripts_dir + os.pathsep + environment.get("PATH", "")
    process = subprocess.Popen(
        [str(venv_python), "run.py", "--migrate"],
        cwd=REPO_ROOT,
        env=environment,
    )
    _assign_kill_on_close_job(process)
    try:
        return process.wait()
    except KeyboardInterrupt:
        process.terminate()
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=15)
        return process.returncode or 0


def _check_database_revision(venv_python: Path, *, env: Mapping[str, str] | None = None) -> None:
    """Print the database revision status without changing anything."""

    result = _run_snippet(venv_python, DATABASE_SNIPPET, env=env)
    output = (result.stdout or result.stderr).strip().splitlines()
    detail = output[-1] if output else "sem resposta"
    if result.returncode == 0:
        print(f"[ok] {detail}")
    else:
        print(f"[--] {detail}")


def _run_checks(venv_dir: Path) -> int:
    """Diagnose the local environment without changing anything."""

    _print_banner()
    failures = 0
    print(f"Python: {sys.version.split()[0]} ({sys.executable})")
    print(f"Ambiente virtual: {venv_dir}")
    venv_python = _venv_python(venv_dir)
    if venv_python is None:
        print("[!!] .venv ausente ou inutilizavel: execute o iniciar.bat para preparar.")
        return 1
    print("[ok] .venv encontrado")
    if _environment_ok(venv_python, verbose=True):
        print("[ok] dependencias instaladas")
    else:
        print("[!!] dependencias ausentes ou desatualizadas: execute o iniciar.bat")
        failures += 1
    if (REPO_ROOT / ".env").exists():
        print("[ok] .env encontrado")
    else:
        print("[--] .env ausente: sera criado a partir de .env.example no primeiro clique")
    if failures == 0:
        try:
            settings = _load_settings(venv_python)
        except LauncherError as exc:
            print(f"[!!] configuracao invalida: {exc}")
            failures += 1
        else:
            print("[ok] configuracao valida")
            host = str(settings.get("host", "127.0.0.1"))
            port = int(str(settings.get("port", 8000)))
            override, notice = _database_override(settings, migrate=False)
            if notice:
                print(f"[--] {notice}")
            database_url = str(override.get("DATABASE_URL", settings.get("database_url", "")))
            sqlite_path = _sqlite_database_path(database_url, REPO_ROOT)
            if sqlite_path is not None and not sqlite_path.exists():
                print("[--] banco ainda nao criado: as migrations rodam no primeiro clique")
            else:
                _check_database_revision(venv_python, env=override)
            if _probe_health(host, port):
                print(
                    f"[--] a aplicacao ja esta em execucao em http://{_browser_host(host)}:{port}"
                )
            elif _port_in_use(host, port):
                print(f"[!!] a porta {port} esta ocupada por outro programa")
                failures += 1
            else:
                print(f"[ok] porta {port} livre")
    if failures == 0:
        print("\nDiagnostico concluido: ambiente pronto.")
        return 0
    print("\nDiagnostico encontrou problemas: veja as mensagens marcadas com [!!].")
    return 1


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="launcher",
        description="Prepara o ambiente e inicia o LicitaLead Monitor localmente.",
    )
    parser.add_argument("--check", action="store_true", help="apenas diagnostico, sem alteracoes")
    parser.add_argument("--repair", action="store_true", help="apaga e recria a .venv")
    parser.add_argument(
        "--reinstall",
        action="store_true",
        help="forca a reinstalacao das dependencias",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="instala tambem as dependencias de desenvolvimento",
    )
    parser.add_argument(
        "--upgrade-pip",
        action="store_true",
        help="tenta atualizar o pip da .venv antes de instalar (opcional)",
    )
    parser.add_argument(
        "--venv",
        type=Path,
        default=None,
        metavar="CAMINHO",
        help="caminho do ambiente virtual (padrao: .venv, ou pasta local em drives sincronizados)",
    )
    parser.add_argument("--no-browser", action="store_true", help="nao abre o navegador")
    parser.add_argument(
        "--no-scheduler",
        action="store_true",
        help="inicia somente a API/interface, sem o scheduler",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Prepare the environment and start the platform."""

    args = _parse_args(argv)
    _configure_console()
    os.chdir(REPO_ROOT)

    venv_dir = args.venv if args.venv else _default_venv_dir()
    if not venv_dir.is_absolute():
        venv_dir = REPO_ROOT / venv_dir

    python_exit = _ensure_python312()
    if python_exit is not None:
        return python_exit

    if args.check:
        return _run_checks(venv_dir)

    _print_banner()
    if not args.venv and venv_dir != REPO_ROOT / ".venv":
        print(
            "Pasta sincronizada detectada (Google Drive/OneDrive): usando o ambiente "
            f"virtual local em {venv_dir} para evitar travamentos do pip."
        )
    scheduler = _Scheduler()
    try:
        venv_python = _prepare_venv([sys.executable], venv_dir=venv_dir, repair=args.repair)
        inputs_hash = _hash_inputs(REPO_ROOT / "pyproject.toml", sys.version_info[:3])
        needs_install = (
            args.repair
            or args.reinstall
            or _needs_install(_state_file(venv_dir), inputs_hash, include_dev=args.dev)
            or not _environment_ok(venv_python)
        )
        if needs_install:
            print("Preparando as dependencias (a primeira execucao pode levar alguns minutos)...")
            _install_dependencies(
                venv_python,
                include_dev=args.dev,
                upgrade_pip=args.upgrade_pip,
            )
            if not _environment_ok(venv_python, verbose=True):
                raise LauncherError(
                    "as dependencias foram instaladas, mas a verificacao do ambiente falhou."
                )
            _save_state(venv_dir, inputs_hash, include_dev=args.dev)

        env_created = _ensure_env_file()
        if env_created and sys.stdin.isatty():
            _edit_env_file(REPO_ROOT / ".env")

        settings = _load_settings(venv_python)
        host = str(settings.get("host", "127.0.0.1"))
        port = int(str(settings.get("port", 8000)))
        base_url = f"http://{_browser_host(host)}:{port}"

        if _probe_health(host, port):
            print(f"A aplicacao ja esta em execucao em {base_url}.")
            if not args.no_browser:
                webbrowser.open(base_url)
            return 0
        if _port_in_use(host, port):
            raise LauncherError(
                f"a porta {port} esta ocupada por outro programa. "
                "Ajuste APP_PORT no .env ou encerre o outro programa."
            )

        override, database_notice = _database_override(settings, migrate=True)
        if database_notice:
            print(database_notice)
        database_url = str(override.get("DATABASE_URL", settings.get("database_url", "")))
        database_path = _sqlite_database_path(database_url, REPO_ROOT)
        if database_path is not None:
            repair_notice = _repair_local_database(database_path)
            if repair_notice:
                print(repair_notice)

        if args.no_scheduler or not bool(settings.get("scheduler_enabled", True)):
            _stop_orphan_scheduler()
            print("Scheduler desativado nesta execucao.")
        else:
            _stop_orphan_scheduler()
            scheduler.start(venv_python, env=override)
            print(
                f"Scheduler iniciado em segundo plano (log: {SCHEDULER_LOG.relative_to(REPO_ROOT)})."
            )
        _install_signal_handlers(scheduler.stop)
        atexit.register(scheduler.stop)

        if not args.no_browser:
            _start_browser_thread(host, port, base_url)
        print(f"\nIniciando a interface em {base_url} (pode levar alguns segundos)...")
        print("Pressione Ctrl+C ou feche esta janela para encerrar.\n")
        return _run_server(venv_python, env=override)
    except LauncherError as exc:
        print(f"\nERRO: {exc}", file=sys.stderr)
        return 1
    finally:
        scheduler.stop()


if __name__ == "__main__":
    raise SystemExit(main())
