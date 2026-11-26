"""Identifiers shared by the two official federal payment feeds."""

import re

from app.services.identifiers import is_valid_cnpj, normalize_cnpj

_SIAFI_DOCUMENT = re.compile(r"[0-9]{15}[A-Z]{2}[0-9]{6}")


def siafi_payment_document(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    document = value.strip().upper()
    return document if _SIAFI_DOCUMENT.fullmatch(document) else None


def creditor_list_code(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    return code if code and len(code) <= 100 else None


def federal_payment_identity(
    document_value: str | None,
    beneficiary_value: str | None,
    role: str,
    list_value: str | None = None,
) -> str | None:
    document = siafi_payment_document(document_value)
    beneficiary = normalize_cnpj(beneficiary_value)
    if not document or not beneficiary or not is_valid_cnpj(beneficiary):
        return None
    if role == "issued":
        return f"issued:{document}:{beneficiary}"
    if role == "final" and (creditor_list := creditor_list_code(list_value)):
        return f"final:{document}:{beneficiary}:{creditor_list}"
    return None
