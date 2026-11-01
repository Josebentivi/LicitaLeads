"""Bounded extraction diagnostics, independent of settings, database and secrets."""

from __future__ import annotations

import re
from typing import Any

_MESSAGES = {
    "archive_rejected": "Arquivo compactado reprovado nas verificações de segurança.",
    "archive_depth_exceeded": "Limite de níveis de ZIP excedido.",
    "archive_members_limit": "Limite global de componentes do arquivo excedido.",
    "archive_total_size": "Limite global de tamanho descompactado excedido.",
    "archive_member_size": "Arquivo interno excede o tamanho permitido.",
    "archive_compression_ratio": "Arquivo interno excede a taxa de compressão permitida.",
    "archive_unsafe_path": "Caminho interno inseguro no arquivo compactado.",
    "archive_encrypted": "Arquivo interno criptografado não é suportado.",
    "archive_invalid_zip": "Conteúdo ZIP inválido ou incompleto.",
    "parser_error": "O extrator não conseguiu interpretar o documento.",
    "input_unavailable": "O arquivo local não pôde ser lido pelo extrator.",
    "worker_spawn_failed": "Não foi possível iniciar o processo de extração.",
    "worker_initialization": "Falha na inicialização do extrator isolado.",
    "worker_exit": "O processo de extração terminou inesperadamente.",
    "worker_output_missing": "O extrator não produziu um resultado.",
    "worker_output_invalid": "O extrator produziu um resultado inválido.",
    "worker_timeout": "Tempo limite do processo de extração excedido.",
}


def _label(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = re.sub(r"[\x00-\x1f\x7f-\x9f]", " ", value)[:400]
    value = re.sub(
        r"(?i)(sk-[\w-]{8,}|(?:bearer|token|api[_-]?key|password|secret)\s*[:= ]\s*\S+)",
        "[redigido]",
        value,
    )
    return value


class ExtractionFailure(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        exception_type: str | None = None,
        member_path: str | None = None,
        input_hash: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        self.code = code if isinstance(code, str) and code in _MESSAGES else "parser_error"
        self.exception_type = (
            exception_type
            if isinstance(exception_type, str)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,80}", exception_type)
            else None
        )
        self.member_path = _label(member_path)
        self.input_hash = (
            input_hash
            if isinstance(input_hash, str) and re.fullmatch(r"[a-f0-9]{64}", input_hash)
            else None
        )
        self.exit_code = exit_code if isinstance(exit_code, int) else None
        message = f"{_MESSAGES[self.code]} [{self.code}]"
        if self.member_path:
            message += f" Arquivo interno: {self.member_path}"
        if self.exception_type:
            message += f" ({self.exception_type})"
        super().__init__(message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": _MESSAGES[self.code],
            "exception_type": self.exception_type,
            "member_path": self.member_path,
            "input_hash": self.input_hash,
            "exit_code": self.exit_code,
        }

    @classmethod
    def from_dict(cls, value: Any, *, exit_code: int | None = None) -> ExtractionFailure:
        value = value if isinstance(value, dict) else {}
        return cls(
            value.get("code", "parser_error"),
            exception_type=value.get("exception_type"),
            member_path=value.get("member_path"),
            input_hash=value.get("input_hash"),
            exit_code=exit_code,
        )

    @classmethod
    def from_exception(cls, exc: Exception, *, input_hash: str | None = None) -> ExtractionFailure:
        return cls(
            getattr(
                exc, "code", "input_unavailable" if isinstance(exc, OSError) else "parser_error"
            ),
            exception_type=type(exc).__name__,
            member_path=getattr(exc, "member_path", None),
            input_hash=input_hash,
        )
