"""Compare corporate contacts while keeping their recorded text verbatim."""

import re

_PHONE_NUMBER = re.compile(
    r"(?P<number>\+?[0-9 .()-]+?)(?: *(?:(?:ramal|ext\.?|x) *|;ext=)(?P<extension>[0-9]{1,8}))?",
    re.IGNORECASE,
)


def phone_identity(value: str) -> str | None:
    match = _PHONE_NUMBER.fullmatch(value)
    if match is None:
        return None
    number = re.sub(r"[^0-9]", "", match["number"])
    if not 8 <= len(number) <= 15:
        return None
    if number.startswith("55") and len(number) in {12, 13}:
        number = number[2:]
    elif match["number"].startswith("+"):
        number = f"+{number}"
    extension = match["extension"]
    return f"{number};ext={extension}" if extension else number


def contact_identity(kind: str, value: str) -> str:
    if kind == "email":
        local, separator, domain = value.partition("@")
        return local + separator + domain.lower()
    if kind in {"phone", "whatsapp"}:
        return phone_identity(value) or value
    return value
