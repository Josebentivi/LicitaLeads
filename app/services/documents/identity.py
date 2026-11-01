"""Official PNCP identities and aliases; identity alone never proves equal content."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.entities import Document, SourceRecord
from app.models.enums import ExtractionStatus
from app.models.participation import DocumentAlias, DocumentIdentity
from app.services.documents.pncp_urls import _resource, resolve_pncp_document


def official_document_key(url: str, record: SourceRecord | None) -> str | None:
    resolution = resolve_pncp_document(url, record)
    return "/".join(resolution.resource) if resolution else None


def header_version(url: str, record: SourceRecord | None) -> str | None:
    """Publication is not modification. Only an explicit version can prove reuse."""
    if not official_document_key(url, record) or record is None:
        return None
    payload = record.raw_payload
    rows = (
        payload
        if isinstance(payload, list)
        else next(
            (
                payload[k]
                for k in ("data", "documentos", "itens")
                if isinstance(payload, dict) and isinstance(payload.get(k), list)
            ),
            [],
        )
    )
    row = next(
        (r for r in rows if isinstance(r, dict) and str(r.get("url") or r.get("uri")) == url), None
    )
    if row is None or not any(
        row.get(k) not in (None, "") for k in ("dataAtualizacao", "versao", "sha256", "hashArquivo")
    ):
        return None
    fields = {k: v for k, v in row.items() if k not in {"url", "uri"}}
    return hashlib.sha256(json.dumps(fields, sort_keys=True, default=str).encode()).hexdigest()


def noncanonical_document_ids():
    roots = (
        select(DocumentAlias.document_id)
        .join(DocumentIdentity, DocumentIdentity.id == DocumentAlias.identity_id)
        .where(DocumentAlias.document_id != DocumentIdentity.canonical_document_id)
    ).cte(recursive=True)
    tree = roots.union(
        select(Document.id).join(roots, Document.parent_document_id == roots.c.document_id)
    )
    return select(tree.c.document_id)


async def canonical_document_ids(db: AsyncSession, identifiers) -> list:
    if not identifiers:
        return []
    rows = await db.execute(
        select(DocumentAlias.document_id, DocumentIdentity.canonical_document_id)
        .join(DocumentIdentity, DocumentIdentity.id == DocumentAlias.identity_id)
        .where(DocumentAlias.document_id.in_(identifiers))
    )
    mapping = dict(rows.all())
    return list(dict.fromkeys(mapping.get(identifier, identifier) for identifier in identifiers))


async def is_canonical(db: AsyncSession, identifier) -> bool:
    return (
        await db.scalar(
            select(Document.id).where(
                Document.id == identifier, Document.id.in_(noncanonical_document_ids())
            )
        )
        is None
    )


async def fork_content_version(db: AsyncSession, document: Document, digest: str) -> Document:
    """Retain the old physical file/tree; a changed body gets a distinct version."""
    if not document.file_hash or document.file_hash == digest:
        return document
    identity = await db.scalar(
        select(DocumentIdentity).where(DocumentIdentity.canonical_document_id == document.id)
    )
    if identity is None:
        record = (
            await db.get(SourceRecord, document.source_record_id)
            if document.source_record_id
            else None
        )
        key = official_document_key(document.original_url, record)
        if key is None:
            # Other sources retain their existing identity/recheck contract.
            # Do not manufacture an official file identity from an arbitrary URL.
            return document
        identity = DocumentIdentity(
            source="pncp",
            official_key=key,
            procurement_id=document.procurement_id,
            canonical_document_id=document.id,
            observed_at=document.last_checked_at or document.created_at,
        )
        db.add(identity)
        await db.flush()
    version = Document(
        procurement_id=document.procurement_id,
        parent_document_id=document.parent_document_id,
        source_record_id=document.source_record_id,
        document_type=document.document_type,
        title=document.title,
        original_url=document.original_url,
        published_at=document.published_at,
        is_active=document.is_active,
        deactivated_at=document.deactivated_at,
        extraction_attempts=document.extraction_attempts,
        fingerprint=hashlib.sha256(f"content-version:{document.id}:{digest}".encode()).hexdigest(),
    )
    db.add(version)
    await db.flush()
    old_alias = await db.scalar(
        select(DocumentAlias)
        .where(DocumentAlias.identity_id == identity.id, DocumentAlias.document_id == document.id)
        .limit(1)
    )
    if old_alias is None:
        db.add(
            DocumentAlias(
                identity_id=identity.id,
                document_id=document.id,
                url=document.original_url,
                source_record_id=document.source_record_id,
                equivalent=False,
                proof="retained_current",
                fingerprint=hashlib.sha256(
                    f"version:{identity.id}:{document.id}".encode()
                ).hexdigest(),
            )
        )
    else:
        old_alias.proof = "retained_current"
        old_alias.equivalent = False
    db.add(
        DocumentAlias(
            identity_id=identity.id,
            document_id=version.id,
            url=version.original_url,
            source_record_id=version.source_record_id,
            equivalent=True,
            proof="downloaded_content",
            fingerprint=hashlib.sha256(f"version:{identity.id}:{version.id}".encode()).hexdigest(),
        )
    )
    identity.canonical_document_id = version.id
    from app.services.participation_integrity import audit_change

    await audit_change(
        db,
        entity_type="document",
        entity_id=version.id,
        procurement_id=version.procurement_id,
        field="document_version",
        previous={"id": str(document.id), "sha256": document.file_hash},
        received={"id": str(version.id), "sha256": digest},
        source_record_id=version.source_record_id,
        decision="accepted_content_version",
    )
    await db.flush()
    return version


async def complete_content_version(db: AsyncSession, document: Document) -> None:
    """Withdraw old facts only after the replacement has been completely extracted."""
    current_aliases = (
        await db.scalars(
            select(DocumentAlias).where(
                DocumentAlias.document_id == document.id,
                DocumentAlias.proof == "verification_pending",
            )
        )
    ).all()
    for alias in current_aliases:
        alias.proof, alias.equivalent = "downloaded_content", True
    aliases = list(
        (
            await db.scalars(
                select(DocumentAlias)
                .join(DocumentIdentity, DocumentIdentity.id == DocumentAlias.identity_id)
                .where(
                    DocumentIdentity.canonical_document_id == document.id,
                    DocumentAlias.document_id != document.id,
                )
            )
        ).all()
    )
    previous_ids = set()
    for alias in aliases:
        previous = await db.get(Document, alias.document_id)
        if previous and previous.file_hash and previous.file_hash != document.file_hash:
            alias.proof, alias.equivalent = "previous_content", False
            previous_ids.add(previous.id)
    if previous_ids:
        from app.models.participation import ParticipantValue
        from app.services.participation_integrity import audit_change

        roots = select(Document.id).where(Document.id.in_(previous_ids)).cte(recursive=True)
        tree = roots.union(
            select(Document.id).join(roots, Document.parent_document_id == roots.c.id)
        )
        values = (
            await db.scalars(
                select(ParticipantValue).where(
                    ParticipantValue.document_id.in_(select(tree.c.id)),
                    ParticipantValue.is_current.is_(True),
                )
            )
        ).all()
        for value in values:
            await audit_change(
                db,
                entity_type="participant_value",
                entity_id=value.id,
                procurement_id=value.procurement_id,
                field="is_current",
                previous=True,
                received=False,
                evidence_id=value.evidence_id,
                source_record_id=document.source_record_id,
                decision="superseded_content_version",
            )
            value.is_current = False
    await db.flush()


async def document_resolution(db: AsyncSession, document: Document):
    record = (
        await db.get(SourceRecord, document.source_record_id) if document.source_record_id else None
    )
    resolution = resolve_pncp_document(document.original_url, record)
    if resolution:
        return resolution
    # An alias must itself be present in its exact stored official listing.
    rows = (
        await db.scalars(
            select(DocumentAlias)
            .where(
                DocumentAlias.document_id == document.id,
                DocumentAlias.source_record_id.is_not(None),
            )
            .order_by(DocumentAlias.id)
        )
    ).all()
    for alias in rows:
        source = await db.get(SourceRecord, alias.source_record_id)
        candidate = resolve_pncp_document(alias.url, source)
        if candidate and _resource(document.original_url) == candidate.resource:
            return candidate
    return None


async def existing_document(
    db: AsyncSession, procurement_id, url: str, record: SourceRecord | None
) -> Document | None:
    key = official_document_key(url, record)
    if key is None:
        return None
    identity = await db.scalar(
        select(DocumentIdentity).where(
            DocumentIdentity.source == "pncp", DocumentIdentity.official_key == key
        )
    )
    if identity:
        if identity.procurement_id != procurement_id:
            raise ValueError("identidade documental oficial vinculada a outra contratação")
        return await db.get(Document, identity.canonical_document_id)
    # Legacy lookup uses the official identity, not filenames or text similarity.
    candidates = (
        await db.scalars(
            select(Document)
            .where(Document.procurement_id == procurement_id, Document.parent_document_id.is_(None))
            .order_by(Document.created_at)
        )
    ).all()
    valid = []
    for document in candidates:
        old_record = (
            await db.get(SourceRecord, document.source_record_id)
            if document.source_record_id
            else None
        )
        if official_document_key(document.original_url, old_record) == key:
            valid.append(document)
    return preferred_document(valid) if valid else None


def preferred_document(documents: list[Document]) -> Document:
    minimum = datetime.min.replace(tzinfo=UTC)
    return max(
        documents,
        key=lambda d: (
            d.extraction_status == ExtractionStatus.EXTRACTED,
            d.last_checked_at or minimum,
            d.downloaded_at or minimum,
            str(d.id),
        ),
    )


async def register_alias(
    db: AsyncSession,
    canonical: Document,
    *,
    url: str,
    record: SourceRecord | None,
    physical: Document | None = None,
    allow_schedule: bool = True,
) -> bool:
    """Register provenance and schedule one check when equivalence is unproved."""
    key = official_document_key(url, record)
    if key is None:
        return False
    identity = await db.scalar(
        select(DocumentIdentity).where(
            DocumentIdentity.source == "pncp", DocumentIdentity.official_key == key
        )
    )
    version = header_version(url, record)
    if identity is None:
        old_record = (
            await db.get(SourceRecord, canonical.source_record_id)
            if canonical.source_record_id
            else None
        )
        identity = DocumentIdentity(
            source="pncp",
            official_key=key,
            procurement_id=canonical.procurement_id,
            canonical_document_id=canonical.id,
            header_hash=header_version(canonical.original_url, old_record),
            observed_at=(old_record.collected_at if old_record else datetime.now(UTC)),
        )
        db.add(identity)
        await db.flush()
    if identity.procurement_id != canonical.procurement_id:
        raise ValueError("identidade documental colide entre contratações; revisão necessária")
    physical = physical or canonical
    fingerprint = hashlib.sha256(repr((key, str(physical.id), url)).encode()).hexdigest()
    alias = await db.scalar(select(DocumentAlias).where(DocumentAlias.fingerprint == fingerprint))
    if alias is None:
        alias = await db.scalar(
            select(DocumentAlias)
            .where(
                DocumentAlias.identity_id == identity.id,
                DocumentAlias.document_id == physical.id,
                DocumentAlias.url == url,
            )
            .order_by(DocumentAlias.id)
            .limit(1)
        )
    if identity.canonical_document_id != canonical.id:
        primary = await db.get(Document, identity.canonical_document_id)
        if primary is not None:
            canonical = primary
    same_hash = bool(
        physical.id != canonical.id
        and physical.file_hash
        and canonical.file_hash
        and physical.file_hash == canonical.file_hash
    )
    conflicting_content = bool(
        physical.id != canonical.id
        and physical.file_hash
        and canonical.file_hash
        and physical.file_hash != canonical.file_hash
    )
    same_version = bool(version and version == identity.header_hash and not conflicting_content)
    same_url = physical.id == canonical.id and url == canonical.original_url
    proved = same_hash or same_version or same_url
    if alias is None:
        db.add(
            DocumentAlias(
                identity_id=identity.id,
                document_id=physical.id,
                source_record_id=record.id if record else None,
                url=url,
                equivalent=proved,
                proof="conflicting_content"
                if conflicting_content
                else "same_hash"
                if same_hash
                else "official_version"
                if same_version
                else "original_observation"
                if same_url
                else "verification_pending",
                fingerprint=fingerprint,
            )
        )
    elif conflicting_content and alias.proof != "previous_content":
        alias.equivalent, alias.proof = False, "conflicting_content"
    newer = record is None or record.collected_at >= identity.observed_at
    # Identical identity + a changed URL is not a reusable version by itself.
    pending_check = alias is None or (
        alias.proof in {"verification_pending", "conflicting_content"}
        and record is not None
        and (canonical.last_checked_at is None or record.collected_at > canonical.last_checked_at)
    )
    if allow_schedule and newer and pending_check and not proved and canonical.is_active:
        canonical.extraction_status = ExtractionStatus.PENDING
        canonical.source_record_id = record.id if record else canonical.source_record_id
    if version and newer:
        if (
            identity.header_hash
            and identity.header_hash != version
            and canonical.is_active
            and allow_schedule
        ):
            canonical.extraction_status = ExtractionStatus.PENDING
        identity.header_hash = version
    if newer and record:
        identity.observed_at = record.collected_at
    await db.flush()
    return alias is None


async def reconcile_aliases(
    db: AsyncSession, documents: list[Document], *, apply: bool
) -> dict[str, int]:
    groups: dict[str, list[tuple[Document, SourceRecord]]] = {}
    for document in documents:
        if document.parent_document_id or not document.source_record_id:
            continue
        record = await db.get(SourceRecord, document.source_record_id)
        key = official_document_key(document.original_url, record)
        if key and record:
            groups.setdefault(key, []).append((document, record))
    duplicates = pending = collisions = 0
    for group in groups.values():
        canonical = preferred_document([d for d, _ in group])
        identity = await db.scalar(
            select(DocumentIdentity).where(
                DocumentIdentity.source == "pncp",
                DocumentIdentity.official_key
                == official_document_key(group[0][0].original_url, group[0][1]),
            )
        )
        if identity:
            if identity.procurement_id != canonical.procurement_id:
                collisions += 1
                continue
            canonical = await db.get(Document, identity.canonical_document_id) or canonical
        duplicates += max(0, len(group) - 1)
        historical = (
            set(
                (
                    await db.scalars(
                        select(DocumentAlias.document_id).where(
                            DocumentAlias.identity_id == identity.id,
                            DocumentAlias.proof == "previous_content",
                        )
                    )
                ).all()
            )
            if identity
            else set()
        )
        current_group = [(d, r) for d, r in group if d.id not in historical]
        versions = {header_version(d.original_url, r) for d, r in current_group}
        known_hashes = {d.file_hash for d, _ in current_group if d.file_hash}
        uncertain = len(current_group) > 1 and (
            len(known_hashes) > 1
            or (
                (None in versions or len(versions) > 1)
                and any(
                    d.file_hash is None or d.file_hash != canonical.file_hash
                    for d, _ in current_group
                )
            )
        )
        pending += int(uncertain)
        if apply:
            # Insert the canonical observation first so later aliases cannot win by order.
            group.sort(key=lambda item: item[0].id != canonical.id)
            for document, record in group:
                await register_alias(
                    db,
                    canonical,
                    url=document.original_url,
                    record=record,
                    physical=document,
                    allow_schedule=False,
                )
    return {
        "duplicate_documents": duplicates,
        "document_versions_pending": pending,
        "document_identity_collisions": collisions,
    }
