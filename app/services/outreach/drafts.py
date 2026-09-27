# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from jinja2 import Environment, StrictUndefined, select_autoescape

_TEMPLATE = """Olá, equipe da {{ company_name }}.

Identificamos em fonte pública uma atualização relacionada à participação da empresa no {{ procurement }}, promovido pelo {{ agency }}.

A publicação registra {{ event }}, relacionado ao seguinte ponto:

{{ reason }}

Também identificamos {{ deadline }}.
{% if deadline_estimated %}
O prazo indicado é uma estimativa e deverá ser confirmado diretamente nos documentos e na plataforma oficial.
{% endif %}
Atuamos com análise de licitações e recursos administrativos.

Caso seja útil, podemos realizar uma avaliação inicial dos documentos e do evento publicado.

Fonte pública consultada:
{{ source_label }}
{{ source_url }}

Atenciosamente,

{{ sender_name }}
{{ law_firm }}
{{ sender_contact }}"""


@dataclass(frozen=True, slots=True)
class OutreachContext:
    company_name: str
    procurement: str
    agency: str
    event: str
    reason: str
    deadline: str
    source_label: str
    source_url: str
    sender_name: str
    law_firm: str
    sender_contact: str
    deadline_estimated: bool = False


@dataclass(frozen=True, slots=True)
class OutreachDraft:
    channel: str
    subject: str | None
    message: str
    generation_method: str
    idempotency_key: str
    approved: bool = False
    sent: bool = False


class OutreachDraftService:
    def __init__(self, *, mode: str = "draft_only", template: str = _TEMPLATE) -> None:
        if mode != "draft_only":
            raise ValueError("the MVP supports OUTREACH_MODE=draft_only only")
        self.mode = mode
        self.template_source = template
        self.environment = Environment(
            autoescape=select_autoescape(default_for_string=False),
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.template = self.environment.from_string(template)

    def generate(self, context: OutreachContext, *, channel: str = "email") -> OutreachDraft:
        if channel not in {"email", "whatsapp", "phone_script"}:
            raise ValueError("unsupported outreach channel")
        required = asdict(context)
        missing = [
            key
            for key, value in required.items()
            if key != "deadline_estimated" and not str(value).strip()
        ]
        if missing:
            raise ValueError("outreach facts are missing: " + ", ".join(missing))
        message = self.template.render(**required).strip()
        subject = f"Atualização pública — {context.procurement}" if channel == "email" else None
        facts = json.dumps(required, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        key_source = f"{channel}\0{self.template_source}\0{facts}"
        idempotency_key = hashlib.sha256(key_source.encode("utf-8")).hexdigest()
        return OutreachDraft(
            channel=channel,
            subject=subject,
            message=message,
            generation_method="jinja_deterministic_v1",
            idempotency_key=idempotency_key,
        )
