"""Bounds and path validation for untrusted archives."""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile, ZipInfo


class UnsafeArchiveError(ValueError):
    """Raised when an archive violates a configured safety invariant."""

    def __init__(
        self, message: str, *, code: str = "archive_rejected", member_path: str | None = None
    ) -> None:
        super().__init__(message)
        self.code, self.member_path = code, member_path


@dataclass(frozen=True, slots=True)
class ArchiveLimits:
    max_members: int = 100
    max_uncompressed_bytes: int = 200 * 1024 * 1024
    max_compression_ratio: float = 100.0
    max_member_bytes: int = 50 * 1024 * 1024


@dataclass(slots=True)
class ArchiveBudget:
    """One budget per input, shared by ZIPs and Office containers at every level."""

    limits: ArchiveLimits
    members: int = 0
    uncompressed_bytes: int = 0

    def reserve(self, info: ZipInfo) -> None:
        if self.members + 1 > self.limits.max_members:
            raise UnsafeArchiveError(
                "archive contains too many members",
                code="archive_members_limit",
                member_path=info.filename,
            )
        if self.uncompressed_bytes + info.file_size > self.limits.max_uncompressed_bytes:
            raise UnsafeArchiveError(
                "archive exceeds total uncompressed size limit",
                code="archive_total_size",
                member_path=info.filename,
            )
        self.members += 1
        self.uncompressed_bytes += info.file_size


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
        raise UnsafeArchiveError(
            f"unsafe archive member path: {filename!r}",
            code="archive_unsafe_path",
            member_path=filename,
        )
    return path.as_posix()


def validate_zip_info(info: ZipInfo, limits: ArchiveLimits) -> None:
    safe_archive_name(info.filename)
    if info.flag_bits & 0x1:
        raise UnsafeArchiveError(
            f"encrypted archive member is not supported: {info.filename!r}",
            code="archive_encrypted",
            member_path=info.filename,
        )
    if info.file_size > limits.max_member_bytes:
        raise UnsafeArchiveError(
            f"archive member exceeds size limit: {info.filename!r}",
            code="archive_member_size",
            member_path=info.filename,
        )
    if info.file_size and info.compress_size == 0:
        raise UnsafeArchiveError(
            f"invalid compression size: {info.filename!r}",
            code="archive_invalid_zip",
            member_path=info.filename,
        )
    if info.compress_size and info.file_size / info.compress_size > limits.max_compression_ratio:
        raise UnsafeArchiveError(
            f"archive member exceeds compression ratio: {info.filename!r}",
            code="archive_compression_ratio",
            member_path=info.filename,
        )


def read_safe_zip(
    content: bytes, limits: ArchiveLimits | None = None, *, budget: ArchiveBudget | None = None
) -> list[tuple[str, bytes]]:
    limits = limits or ArchiveLimits()
    budget = budget or ArchiveBudget(limits)
    try:
        with ZipFile(__import__("io").BytesIO(content)) as archive:
            members = [info for info in archive.infolist() if not info.is_dir()]
            if len(members) > limits.max_members:
                raise UnsafeArchiveError(
                    "archive contains too many members", code="archive_members_limit"
                )
            result: list[tuple[str, bytes]] = []
            for info in members:
                validate_zip_info(info, limits)
                budget.reserve(info)
                name = safe_archive_name(info.filename)
                data = archive.read(info)
                if len(data) != info.file_size:
                    raise UnsafeArchiveError(
                        f"archive member size mismatch: {info.filename!r}",
                        code="archive_invalid_zip",
                        member_path=info.filename,
                    )
                result.append((name, data))
            return result
    except BadZipFile as exc:
        raise UnsafeArchiveError("invalid ZIP archive", code="archive_invalid_zip") from exc
