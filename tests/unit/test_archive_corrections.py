"""ZIP nesting shares one budget, including validated AppleDouble and Office parts."""

import io
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from docx import Document
from openpyxl import Workbook

from app.config import Settings
from app.services.documents.errors import ExtractionFailure
from app.services.documents.extractor import DocumentExtractor
from app.services.documents.security import ArchiveLimits, UnsafeArchiveError


def zipped(*members):
    out = io.BytesIO()
    with ZipFile(out, "w", ZIP_DEFLATED) as z:
        for name, content in members:
            z.writestr(name, content)
    return out.getvalue()


def test_two_levels_preserve_leaf_locations_and_reject_third():
    inner = zipped(("ata.txt", b"Ata oficial com documento de habilitacao."))
    result = DocumentExtractor().extract(zipped(("anexos.zip", inner)), filename="edital.zip")
    assert (
        result.archive_members[0]
        .extraction.archive_members[0]
        .extraction.text.startswith("Ata oficial")
    )
    with pytest.raises(UnsafeArchiveError) as error:
        DocumentExtractor().extract(
            zipped(("externo.zip", zipped(("interno.zip", inner)))), filename="raiz.zip"
        )
    assert error.value.code == "archive_depth_exceeded"
    assert error.value.member_path == "externo.zip / interno.zip"
    with pytest.raises(UnsafeArchiveError):
        DocumentExtractor(max_nested_archives=1).extract(
            zipped(("anexos.zip", inner)), filename="edital.zip"
        )


@pytest.mark.parametrize("kind", ["members", "bytes"])
def test_sibling_archives_cannot_multiply_the_global_budget(kind):
    inner = zipped(("a.txt", b"legal text"), ("b.txt", b"more official text"))
    root = zipped(("one.zip", inner), ("two.zip", inner))
    limits = (
        ArchiveLimits(max_members=5)
        if kind == "members"
        else ArchiveLimits(max_uncompressed_bytes=len(inner) * 2 + 35)
    )
    with pytest.raises(UnsafeArchiveError) as error:
        DocumentExtractor(archive_limits=limits).extract(root, filename="docs.zip")
    assert error.value.code == (
        "archive_members_limit" if kind == "members" else "archive_total_size"
    )


def test_appledouble_is_skipped_only_by_name_and_signature_after_security_checks():
    sidecar = b"\x00\x05\x16\x07" + b"metadata"
    content = zipped(
        ("__MACOSX/._ata.docx", sidecar),
        ("__MACOSX/ata.txt", b"Real official document"),
        ("._other.txt", b"Real text despite the filename"),
    )
    result = DocumentExtractor().extract(content, filename="docs.zip")
    assert len(result.archive_members) == 2 and "Real official" in result.text
    assert result.warnings == ("appledouble_metadata_ignored: 1",)
    with pytest.raises(UnsafeArchiveError):
        DocumentExtractor(archive_limits=ArchiveLimits(max_members=2)).extract(
            content, filename="docs.zip"
        )
    with pytest.raises(UnsafeArchiveError) as error:
        DocumentExtractor().extract(
            zipped(("../__MACOSX/._ata.docx", sidecar)), filename="docs.zip"
        )
    assert error.value.code == "archive_unsafe_path"


@pytest.mark.parametrize("office", ["docx", "xlsx"])
def test_office_containers_share_the_outer_budget(office):
    out = io.BytesIO()
    if office == "docx":
        document = Document()
        document.add_paragraph("Documento oficial")
        document.save(out)
    else:
        workbook = Workbook()
        workbook.active.append(["Documento oficial"])
        workbook.save(out)
    content = zipped((f"ata.{office}", out.getvalue()))
    assert DocumentExtractor().extract(content, filename="docs.zip").text
    with pytest.raises(UnsafeArchiveError) as error:
        DocumentExtractor(archive_limits=ArchiveLimits(max_members=2)).extract(
            content, filename="docs.zip"
        )
    assert error.value.code == "archive_members_limit"


def test_compression_and_per_member_limits_are_not_relaxed_for_sidecars():
    content = zipped(("__MACOSX/._ata.docx", b"\x00\x05\x16\x07" + b"0" * 100000))
    with pytest.raises(UnsafeArchiveError) as error:
        DocumentExtractor().extract(content, filename="docs.zip")
    assert error.value.code == "archive_compression_ratio"
    with pytest.raises(UnsafeArchiveError):
        DocumentExtractor(archive_limits=ArchiveLimits(max_member_bytes=10)).extract(
            content, filename="docs.zip"
        )


@pytest.mark.parametrize("value", [0, 3])
def test_depth_setting_is_bounded(value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(archive_max_depth=value)


@pytest.mark.parametrize("nested", [False, True])
def test_parser_failure_sanitizes_message_and_keeps_member(nested, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("sk-private-secret-value document contents")

    monkeypatch.setattr(DocumentExtractor, "_pdf", fail)
    content = b"%PDF-1.7\nmalformed"
    if nested:
        content = zipped(("inner.zip", zipped(("ata.pdf", content))))
    with pytest.raises(ExtractionFailure) as error:
        DocumentExtractor().extract(content, filename="root.zip" if nested else "ata.pdf")
    assert error.value.code == "parser_error"
    assert error.value.exception_type == "RuntimeError"
    assert error.value.member_path == ("inner.zip / ata.pdf" if nested else None)
    assert "private" not in str(error.value) and "document contents" not in str(error.value)
