"""Application configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Validated runtime settings.

    Secrets are accepted from the environment but are never included in the
    public settings view or structured logs.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: Literal["development", "test", "production"] = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    database_url: str = "sqlite:///./data/licita_lead.db"

    timezone: str = "America/Sao_Paulo"
    default_uf: str = "MA"
    default_lookback_days: int = Field(default=30, ge=1, le=365)
    default_modalities: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["pregao_eletronico"]
    )

    pncp_base_url: str = "https://pncp.gov.br/api/consulta/v1"
    pncp_integration_base_url: str = "https://pncp.gov.br/api/pncp/v1"
    compras_gov_base_url: str = "https://dadosabertos.compras.gov.br"

    http_timeout_seconds: float = Field(default=30, gt=0, le=120)
    http_max_retries: int = Field(default=4, ge=0, le=10)
    http_max_concurrency: int = Field(default=4, ge=1, le=20)
    http_user_agent: str = "LicitaLeadMonitor/0.1"

    document_storage_path: Path = Path("./data/documents")
    raw_data_storage_path: Path = Path("./data/raw")
    document_max_bytes: int = Field(default=50 * 1024 * 1024, ge=1024)
    archive_max_bytes: int = Field(default=200 * 1024 * 1024, ge=1024)
    archive_max_members: int = Field(default=100, ge=1, le=10_000)
    holiday_calendar_path: Path = Path("./data/holidays.csv")

    llm_enabled: bool = False
    llm_provider: Literal["none", "openai"] = "none"
    llm_api_key: str = ""
    llm_model: str = ""
    llm_base_url: str = "https://api.openai.com/v1"
    llm_timeout_seconds: float = Field(default=60, gt=0, le=600)
    llm_max_retries: int = Field(default=2, ge=0, le=6)
    llm_max_output_tokens: int = Field(default=1200, ge=64, le=16_384)
    llm_max_chunks_per_document: int = Field(default=40, ge=1, le=2_000)
    llm_min_chunk_characters: int = Field(default=200, ge=1)

    contact_search_enabled: bool = False
    contact_search_provider: str = "none"
    contact_search_api_key: str = ""
    contact_domain_discovery_enabled: bool = True
    outreach_mode: Literal["draft_only"] = "draft_only"
    outreach_sender_name: str = "Equipe jurídica"
    outreach_law_firm: str = "LicitaLead Monitor"
    outreach_sender_contact: str = "contato a configurar"

    scheduler_enabled: bool = True
    scheduler_discovery_hours: int = Field(default=6, ge=1)
    scheduler_active_refresh_minutes: int = Field(default=60, ge=5)
    scheduler_documents_hours: int = Field(default=2, ge=1)
    scheduler_deadlines_minutes: int = Field(default=30, ge=5)
    scheduler_contacts_hours: int = Field(default=24, ge=1)

    document_batch_size: int = Field(default=30, ge=1, le=500)
    crawl_max_records_per_source: int = Field(default=0, ge=0)
    crawl_max_pages: int = Field(default=0, ge=0)
    crawl_stale_after_minutes: int = Field(default=180, ge=5)

    min_lead_score: int = Field(default=50, ge=0, le=100)
    manual_review_confidence_threshold: float = Field(default=0.75, ge=0, le=1)
    run_live_contract_tests: bool = False

    @field_validator("default_modalities", mode="before")
    @classmethod
    def parse_modalities(cls, value: object) -> object:
        """Accept comma-separated environment values or a native list."""
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("default_uf")
    @classmethod
    def normalize_uf(cls, value: str) -> str:
        """Normalize and validate a Brazilian state abbreviation."""
        normalized = value.strip().upper()
        if len(normalized) != 2 or not normalized.isalpha():
            raise ValueError("DEFAULT_UF must contain a two-letter state abbreviation")
        return normalized

    @model_validator(mode="after")
    def validate_llm(self) -> Settings:
        """Refuse an enabled LLM without the minimum credentials to call it."""

        if not self.llm_enabled:
            return self
        if self.llm_provider == "none":
            raise ValueError("LLM_PROVIDER must be set when LLM_ENABLED=true")
        if not self.llm_api_key.strip():
            raise ValueError("LLM_API_KEY must be set when LLM_ENABLED=true")
        if not self.llm_model.strip():
            raise ValueError("LLM_MODEL must be set when LLM_ENABLED=true")
        return self

    @property
    def async_database_url(self) -> str:
        """Return a SQLAlchemy async-compatible database URL."""
        if self.database_url.startswith("sqlite+aiosqlite:"):
            return self.database_url
        if self.database_url.startswith("sqlite:"):
            return self.database_url.replace("sqlite:", "sqlite+aiosqlite:", 1)
        return self.database_url

    def ensure_directories(self) -> None:
        """Create runtime directories without touching existing contents."""
        self.document_storage_path.mkdir(parents=True, exist_ok=True)
        self.raw_data_storage_path.mkdir(parents=True, exist_ok=True)
        if self.database_url.startswith("sqlite"):
            db_path = self.database_url.rsplit("/", 1)[-1]
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def public_view(self) -> dict[str, object]:
        """Return a secret-free settings representation for the UI."""
        return {
            "app_env": self.app_env,
            "database_backend": "sqlite"
            if self.database_url.startswith("sqlite")
            else "postgresql",
            "timezone": self.timezone,
            "default_uf": self.default_uf,
            "default_lookback_days": self.default_lookback_days,
            "default_modalities": self.default_modalities,
            "llm_enabled": self.llm_enabled,
            "llm_provider": self.llm_provider,
            "llm_model": self.llm_model,
            "contact_search_enabled": self.contact_search_enabled,
            "contact_domain_discovery_enabled": self.contact_domain_discovery_enabled,
            "outreach_mode": self.outreach_mode,
            "scheduler_enabled": self.scheduler_enabled,
            "scheduler_mode": "separate_process",
            "document_batch_size": self.document_batch_size,
            "crawl_max_records_per_source": self.crawl_max_records_per_source,
            "crawl_max_pages": self.crawl_max_pages,
            "crawl_stale_after_minutes": self.crawl_stale_after_minutes,
        }


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide immutable settings instance."""
    return Settings()
