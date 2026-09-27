"""Bounds and path validation for untrusted archives."""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile, ZipInfo


class UnsafeArchiveError(ValueError):
    """Raised when an archive violates a configured safety invariant."""


@dataclass(frozen=True, slots=True)
class ArchiveLimits:
    max_members: int = 100
    max_uncompressed_bytes: int = 200 * 1024 * 1024
    max_compression_ratio: float = 100.0
    max_member_bytes: int = 50 * 1024 * 1024


_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")


def safe_archive_name(filename: str) -> str:
    """Normalize a ZIP member name and reject traversal/absolute paths."""

    candidate = filename.replace("\\", "/")
    normalized = posixpath.normpath(candidate)
    path = PurePosixPath(normalized)
    if (
        not candidate
        or candidate.startswith(("/", "\\"))
        or _DRIVE_PREFIX.match(candidate)
        or normalized in {".", ".."}
        or ".." in path.parts
    ):
        raise UnsafeArchiveError(f"unsafe archive member path: {filename!r}")
    return path.as_posix()


def validate_zip_info(info: ZipInfo, limits: ArchiveLimits) -> None:
    safe_archive_name(info.filename)
    if info.flag_bits & 0x1:
        raise UnsafeArchiveError(f"encrypted archive member is not supported: {info.filename!r}")
    if info.file_size > limits.max_member_bytes:
        raise UnsafeArchiveError(f"archive member exceeds size limit: {info.filename!r}")
    if info.file_size and info.compress_size == 0:
        raise UnsafeArchiveError(f"invalid compression size: {info.filename!r}")
    if info.compress_size and info.file_size / info.compress_size > limits.max_compression_ratio:
        raise UnsafeArchiveError(f"archive member exceeds compression ratio: {info.filename!r}")


def read_safe_zip(content: bytes, limits: ArchiveLimits | None = None) -> list[tuple[str, bytes]]:
    limits = limits or ArchiveLimits()
    try:
        with ZipFile(__import__("io").BytesIO(content)) as archive:
            members = [info for info in archive.infolist() if not info.is_dir()]
            if len(members) > limits.max_members:
                raise UnsafeArchiveError("archive contains too many members")
            total = 0
            result: list[tuple[str, bytes]] = []
            for info in members:
                validate_zip_info(info, limits)
                total += info.file_size
                if total > limits.max_uncompressed_bytes:
                    raise UnsafeArchiveError("archive exceeds total uncompressed size limit")
                name = safe_archive_name(info.filename)
                data = archive.read(info)
                if len(data) != info.file_size:
                    raise UnsafeArchiveError(f"archive member size mismatch: {info.filename!r}")
                result.append((name, data))
            return result
    except BadZipFile as exc:
        raise UnsafeArchiveError("invalid ZIP archive") from exc
