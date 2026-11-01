"""Prepare browser drivers explicitly; test execution never downloads a driver."""

from __future__ import annotations

import ctypes
import importlib.util
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from app.services.test_reports import atomic_json


def browser_cache() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    return (
        Path(local) / "LicitaLeads" if local else Path.home() / ".cache" / "LicitaLeads"
    ) / "test-browser"


def executable_version(path: Path) -> str:
    if os.name == "nt":
        from ctypes import wintypes

        size = ctypes.windll.version.GetFileVersionInfoSizeW(str(path), None)
        if not size:
            return ""
        data = ctypes.create_string_buffer(size)
        if not ctypes.windll.version.GetFileVersionInfoW(str(path), 0, size, data):
            return ""
        pointer, length = ctypes.c_void_p(), wintypes.UINT()
        if not ctypes.windll.version.VerQueryValueW(
            data, "\\", ctypes.byref(pointer), ctypes.byref(length)
        ):
            return ""
        fields = ctypes.cast(pointer, ctypes.POINTER(wintypes.DWORD))
        return ".".join(
            str(value)
            for value in (fields[2] >> 16, fields[2] & 65535, fields[3] >> 16, fields[3] & 65535)
        )
    result = subprocess.run(  # noqa: S603 - local browser binary, fixed argument
        [str(path), "--version"], capture_output=True, text=True, timeout=10
    )
    match = re.search(r"\d+\.\d+\.\d+\.\d+", result.stdout)
    return match.group() if match else ""


def browser_candidates() -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []
    for browser, relative in (
        ("edge", "Microsoft/Edge/Application/msedge.exe"),
        ("chrome", "Google/Chrome/Application/chrome.exe"),
    ):
        for directory in (
            os.environ.get("PROGRAMFILES(X86)"),
            os.environ.get("PROGRAMFILES"),
            os.environ.get("LOCALAPPDATA"),
        ):
            if directory:
                candidates.append((browser, Path(directory) / relative))
    for browser, command in (
        ("edge", "microsoft-edge"),
        ("chrome", "google-chrome"),
        ("chrome", "chromium"),
    ):
        if executable := shutil.which(command):
            candidates.append((browser, Path(executable)))
    return list(dict.fromkeys((name, path) for name, path in candidates if path.is_file()))


def browser_readiness() -> dict[str, Any]:
    """Only inspect already prepared assets. No browser, network or downloads."""
    try:
        if importlib.util.find_spec("selenium") is None:
            raise ValueError("selenium missing")
        value = json.loads((browser_cache() / "prepared.json").read_text(encoding="utf-8"))
        binary, driver = Path(value["binary"]), Path(value["driver"])
        if not binary.is_file() or not driver.is_file():
            raise ValueError("missing asset")
        version = executable_version(binary)
        if not version or version.split(".")[:3] != value["version"].split(".")[:3]:
            raise ValueError("browser changed")
        return {
            **value,
            "version": version,
            "driver_version": executable_version(driver),
            "available": True,
            "detail": "Navegador e driver preparados para execução offline.",
        }
    except (OSError, ValueError, KeyError, TypeError):
        return {
            "available": False,
            "detail": "Prepare o navegador com prepare-test-browser; nenhum download será feito pela suíte.",
        }


def prepare_test_browser() -> dict[str, Any]:
    ready = browser_readiness()
    if ready["available"]:
        return ready
    try:
        from selenium.webdriver.common.selenium_manager import SeleniumManager
    except ImportError as exc:
        raise RuntimeError(
            "Instale as dependências de desenvolvimento para preparar o navegador."
        ) from exc
    cache = browser_cache()
    cache.mkdir(parents=True, exist_ok=True)
    errors = []
    for browser, binary in browser_candidates():
        try:
            paths = SeleniumManager().binary_paths(
                [
                    "--browser",
                    browser,
                    "--browser-path",
                    str(binary),
                    "--cache-path",
                    str(cache),
                    "--avoid-browser-download",
                    "--avoid-stats",
                ]
            )
            result = {
                "browser": browser,
                "binary": str(binary),
                "driver": paths["driver_path"],
                "version": executable_version(binary),
            }
            atomic_json(cache / "prepared.json", result)
            return {
                **result,
                "available": True,
                "detail": "Driver preparado; testes usarão somente os arquivos locais.",
            }
        except Exception as exc:
            errors.append(f"{browser}: {type(exc).__name__}")
    raise RuntimeError(
        "Não foi possível preparar o driver. "
        + ("; ".join(errors) or "Edge/Chrome não encontrados.")
    )
