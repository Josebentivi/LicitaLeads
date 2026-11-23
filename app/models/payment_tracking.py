"""Operator-selected targets for federal payment collection."""

from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class PaymentTrackingTarget(Base):
    __tablename__ = "payment_tracking_targets"

    cnpj: Mapped[str] = mapped_column(String(14), primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
