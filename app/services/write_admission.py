"""Transaction-level admission for writes during exclusive database maintenance."""

from datetime import UTC, datetime

from sqlalchemy import event, select, text, update
from sqlalchemy.orm import ORMExecuteState, Session

from app.models import Base, JobLease
from app.repositories.crawls import MAINTENANCE_LEASE, MaintenanceInProgress


class ApplicationSession(Session):
    """Sessions whose domain writes participate in the maintenance gate."""


def _admit(session: Session) -> None:
    connection = session.connection()
    if connection.dialect.name == "postgresql":
        connection.execute(text("SELECT pg_advisory_xact_lock(83850117047118)"))
    else:
        connection.execute(
            update(JobLease)
            .where(JobLease.name == MAINTENANCE_LEASE)
            .values(version=JobLease.version)
        )
    lease = connection.execute(
        select(JobLease.owner_id, JobLease.expires_at).where(
            JobLease.name == MAINTENANCE_LEASE, JobLease.expires_at > datetime.now(UTC)
        )
    ).first()
    owner = session.info.get("maintenance_owner")
    if owner and (lease is None or lease.owner_id != owner):
        raise MaintenanceInProgress("a manutenção perdeu a propriedade da lease")
    if lease and lease.owner_id != owner:
        raise MaintenanceInProgress("manutenção exclusiva em andamento; tente novamente depois")


@event.listens_for(ApplicationSession, "before_flush")
def _before_flush(session: Session, _context: object, _instances: object) -> None:
    if any(
        isinstance(instance, Base) and not isinstance(instance, JobLease)
        for instance in session.new.union(session.dirty).union(session.deleted)
    ):
        _admit(session)


@event.listens_for(ApplicationSession, "do_orm_execute")
def _before_execute(state: ORMExecuteState) -> None:
    if state.is_insert or state.is_update or state.is_delete:
        table = getattr(state.statement, "table", None)
        if table is not None and table.name != JobLease.__tablename__:
            _admit(state.session)
