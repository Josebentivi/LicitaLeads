"""SQL evidence predicates, including every ancestor of an archive member."""

from sqlalchemy import exists, or_, select
from sqlalchemy.orm import aliased

from app.models import Document, Evidence
from app.models.participation import DocumentAlias, DocumentIdentity


def inactive_document_ids():
    previous_content = select(DocumentAlias.document_id).where(
        DocumentAlias.proof == "previous_content"
    )
    inactive_canonical = select(Document.id).where(Document.is_active.is_(False)).correlate(None)
    inactive_aliases = (
        select(DocumentAlias.document_id)
        .join(DocumentIdentity)
        .where(DocumentIdentity.canonical_document_id.in_(inactive_canonical))
    )
    tree = (
        select(Document.id)
        .where(
            or_(
                Document.is_active.is_(False),
                Document.id.in_(previous_content),
                Document.id.in_(inactive_aliases),
            )
        )
        .cte(recursive=True)
    )
    return tree.union(select(Document.id).join(tree, Document.parent_document_id == tree.c.id))


def evidence_current(evidence_id):
    tree = inactive_document_ids()
    proof = aliased(Evidence)
    return or_(
        evidence_id.is_(None),
        exists(
            select(proof.id)
            .where(
                proof.id == evidence_id,
                or_(proof.document_id.is_(None), ~proof.document_id.in_(select(tree.c.id))),
            )
            .correlate_except(proof)
        ),
    )
