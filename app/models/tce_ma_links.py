"""Active documentary links to São Luís records in SINC-Contrata."""

from uuid import UUID

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import GUID, Base


class TceMaVerifiedLink(Base):
    __tablename__ = "tce_ma_verified_links"
    __table_args__ = (UniqueConstraint("tce_cnpj", "tce_procedure_id"),)

    procurement_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("procurements.id", ondelete="CASCADE"), primary_key=True
    )
    confirmation_id: Mapped[UUID] = mapped_column(GUID(), nullable=False)
    tce_cnpj: Mapped[str] = mapped_column(String(14), nullable=False)
    tce_procedure_id: Mapped[str] = mapped_column(String(100), nullable=False)
    pncp_control: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    pncp_number: Mapped[str] = mapped_column(String(60), nullable=False)
    tce_number: Mapped[str] = mapped_column(String(60), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="CASCADE"), nullable=False
    )
    pncp_agency_evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="CASCADE")
    )
    tce_agency_evidence_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("evidence.id", ondelete="CASCADE")
    )

    @property
    def key(self) -> tuple[str, str]:
        return self.tce_cnpj, self.tce_procedure_id
