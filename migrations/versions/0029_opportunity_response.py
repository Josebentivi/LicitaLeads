"""Record corporate-contact provenance and recognize official appeal openings."""

import sqlalchemy as sa
from alembic import op

from app.models.base import GUID, NAMING_CONVENTION

revision = "0029_opportunity_response"
down_revision = "0028_municipal_spending"
branch_labels = None
depends_on = None

_EVENT_TYPES = (
    "PROPOSAL_SUBMITTED",
    "PROPOSAL_ACCEPTED",
    "PROPOSAL_REJECTED",
    "DISQUALIFIED",
    "QUALIFIED",
    "INELIGIBLE",
    "INTENT_TO_APPEAL",
    "APPEAL_SUBMITTED",
    "COUNTERARGUMENT_OPENED",
    "COUNTERARGUMENT_SUBMITTED",
    "APPEAL_DECIDED",
    "WINNER_DECLARED",
    "ADJUDICATED",
    "HOMOLOGATED",
    "SESSION_SUSPENDED",
    "SESSION_REOPENED",
    "SESSION_OPENED",
    "BEST_OFFER_DECLARED",
    "NEGOTIATION_REQUESTED",
    "DILIGENCE_REQUESTED",
    "DOCUMENTATION_REQUESTED",
    "PARTICIPATION_DETECTED",
    "UNKNOWN",
)
_CONTACT_TYPES = ("email", "phone", "whatsapp", "form", "website", "linkedin")
_EVENT_CHECK = "ck_procurement_events_enum_procurementeventtype"
_CONTACT_CHECK = "ck_company_contacts_enum_contacttype"
_CONTACT_SOURCE_FK = "fk_company_contacts_source_record_id_source_records"
_CONTACT_SOURCE_INDEX = "ix_company_contacts_source_record_id"
_APPEAL_OPENED = "APPEAL_WINDOW_OPENED"


def _enum_expression(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(value) for value in values)})"


def _check_sql(table: str, name: str) -> str:
    return next(
        (
            check.get("sqltext", "")
            for check in sa.inspect(op.get_bind()).get_check_constraints(table)
            if check["name"] == name
        ),
        "",
    )


def _event_check(values: tuple[str, ...]) -> None:
    previous = _check_sql("procurement_events", _EVENT_CHECK)
    if previous and (_APPEAL_OPENED in previous) == (_APPEAL_OPENED in values):
        return
    with op.batch_alter_table("procurement_events", naming_convention=NAMING_CONVENTION) as batch:
        if previous:
            batch.drop_constraint(op.f(_EVENT_CHECK), type_="check")
        batch.create_check_constraint(op.f(_EVENT_CHECK), _enum_expression("event_type", values))


def upgrade() -> None:
    _event_check((*_EVENT_TYPES, _APPEAL_OPENED))
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("company_contacts")}
    previous = _check_sql("company_contacts", _CONTACT_CHECK)
    add_source = "source_record_id" not in columns
    add_address = "'address'" not in previous
    if add_source or add_address:
        with op.batch_alter_table("company_contacts", naming_convention=NAMING_CONVENTION) as batch:
            if add_source:
                batch.add_column(sa.Column("source_record_id", GUID(), nullable=True))
                batch.create_foreign_key(
                    op.f(_CONTACT_SOURCE_FK),
                    "source_records",
                    ["source_record_id"],
                    ["id"],
                    ondelete="SET NULL",
                )
                batch.create_index(_CONTACT_SOURCE_INDEX, ["source_record_id"])
            if add_address:
                if previous:
                    batch.drop_constraint(op.f(_CONTACT_CHECK), type_="check")
                batch.create_check_constraint(
                    op.f(_CONTACT_CHECK),
                    _enum_expression("contact_type", (*_CONTACT_TYPES, "address")),
                )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.scalar(
        sa.text("SELECT 1 FROM procurement_events WHERE event_type = :type LIMIT 1"),
        {"type": _APPEAL_OPENED},
    ) or bind.scalar(
        sa.text("SELECT 1 FROM company_contacts WHERE contact_type = 'address' LIMIT 1")
    ):
        raise RuntimeError("Há prazos recursais ou endereços incompatíveis com a versão anterior.")
    if bind.scalar(
        sa.text("SELECT 1 FROM company_contacts WHERE source_record_id IS NOT NULL LIMIT 1")
    ):
        raise RuntimeError(
            "Há contatos com referências de origem incompatíveis com a versão anterior."
        )
    with op.batch_alter_table("company_contacts", naming_convention=NAMING_CONVENTION) as batch:
        batch.drop_index(_CONTACT_SOURCE_INDEX)
        batch.drop_constraint(op.f(_CONTACT_SOURCE_FK), type_="foreignkey")
        batch.drop_column("source_record_id")
        batch.drop_constraint(op.f(_CONTACT_CHECK), type_="check")
        batch.create_check_constraint(
            op.f(_CONTACT_CHECK), _enum_expression("contact_type", _CONTACT_TYPES)
        )
    _event_check(_EVENT_TYPES)
