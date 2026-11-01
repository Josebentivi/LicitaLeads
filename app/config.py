"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from decimal import Decimal
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
        env_file=None if os.environ.get("APP_ENV") == "test" else ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    test_suite_timeout_seconds: int = Field(default=3600, ge=60, le=21600)
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    allowed_hosts: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["127.0.0.1", "localhost", "testserver"]
    )
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
    http_min_request_interval_seconds: float = Field(default=0.5, ge=0, le=10)
    http_user_agent: str = "LicitaLeadMonitor/0.1"

    document_storage_path: Path = Path("./data/documents")
    raw_data_storage_path: Path = Path("./data/raw")
    document_max_bytes: int = Field(default=50 * 1024 * 1024, ge=1024)
    archive_max_bytes: int = Field(default=200 * 1024 * 1024, ge=1024)
    archive_max_members: int = Field(default=100, ge=1, le=10_000)
    archive_max_depth: int = Field(default=2, ge=1, le=2)
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
    llm_max_requests_per_hour: int = Field(default=500, ge=1)
    llm_input_price_per_million_usd: Decimal = Field(
        default=Decimal("0.1"), ge=0, allow_inf_nan=False
    )
    llm_output_price_per_million_usd: Decimal = Field(
        default=Decimal("0.5"), ge=0, allow_inf_nan=False
    )

    contact_search_enabled: bool = False
    contact_search_provider: str = "none"
    contact_search_api_key: str = ""
    contact_domain_discovery_enabled: bool = True
    transparencia_base_url: str = "https://api.portaldatransparencia.gov.br"
    transparencia_api_key: str = ""
    transparencia_timeout_seconds: float = Field(default=30, gt=0, le=300)
    transparencia_max_retries: int = Field(default=1, ge=0, le=5)
    payment_sources: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["transparencia", "manual"]
    )
    municipal_payment_sources: Annotated[list[str], NoDecode] = Field(default_factory=list)
    municipal_payment_include_details: bool = False
    allow_insecure_http_sources: bool = False
    municipal_payment_detail_limit: int = Field(default=500, ge=0, le=20_000)
    municipal_payment_max_records: int = Field(default=10_000, ge=0, le=100_000)
    sao_luis_payments_url: str = "https://www.saoluis.ma.gov.br/dados-abertos"
    outreach_mode: Literal["draft_only"] = "draft_only"
    outreach_sender_name: str = "Equipe jurídica"
    outreach_law_firm: str = "LicitaLead Monitor"
    outreach_sender_contact: str = "contato a configurar"

    scheduler_enabled: bool = True
    scheduler_discovery_hours: int = Field(default=6, ge=1)
    scheduler_price_registry_hours: int = Field(default=24, ge=1)
    price_registry_lookback_days: int | None = Field(default=None, ge=1, le=3650)
    scheduler_active_refresh_minutes: int = Field(default=60, ge=5)
    scheduler_documents_hours: int = Field(default=2, ge=1)
    scheduler_deadlines_minutes: int = Field(default=30, ge=5)
    scheduler_contacts_hours: int = Field(default=24, ge=1)

    document_batch_size: int = Field(default=30, ge=1, le=500)
    document_download_concurrency: int = Field(default=4, ge=1)
    document_process_concurrency: int = Field(default=2, ge=1)
    document_max_attempts: int = Field(default=5, ge=1, le=20)
    document_retry_base_minutes: int = Field(default=15, ge=1, le=1440)
    crawl_max_records_per_source: int = Field(default=0, ge=0)
    crawl_max_pages: int = Field(default=0, ge=0)
    active_refresh_batch_size: int = Field(default=25, ge=1, le=500)
    compras_recheck_batch_size: int = Field(default=100, ge=1, le=500)
    collection_incremental_overlap_days: int = Field(default=2, ge=1, le=30)
    collection_reconciliation_days: int = Field(default=7, ge=1, le=90)
    crawl_stale_after_minutes: int = Field(default=180, ge=5)
    session_poll_enabled: bool = True
    session_poll_minutes: int = Field(default=15, ge=5, le=240)
    session_poll_document_limit: int = Field(default=20, ge=1, le=500)
    payment_grace_days: int = Field(default=60, ge=0, le=3650)
    payment_ingestion_batch_size: int = Field(default=250, ge=1, le=10_000)
    contract_agency_cnpjs: Annotated[list[str], NoDecode] = Field(default_factory=list)
    payment_poll_cnpjs: Annotated[list[str], NoDecode] = Field(default_factory=list)
    scheduler_contract_hours: int = Field(default=24, ge=1)
    scheduler_payment_hours: int = Field(default=24, ge=1)
    scheduler_municipal_payment_hours: int = Field(default=24, ge=1)
    ocr_enabled: bool = False
    ocr_provider: Literal["none", "tesseract"] = "none"
    ocr_language: str = "por"
    ocr_dpi: int = Field(default=200, ge=100, le=600)
    ocr_min_confidence: float = Field(default=0.5, ge=0, le=1)

    min_lead_score: int = Field(default=50, ge=0, le=100)
    manual_review_confidence_threshold: float = Field(default=0.75, ge=0, le=1)
    run_live_contract_tests: bool = False

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("allowed_hosts", "payment_sources", mode="before")
    @classmethod
    def parse_string_lists(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator(
        "contract_agency_cnpjs",
        "payment_poll_cnpjs",
        "municipal_payment_sources",
        mode="before",
    )
    @classmethod
    def parse_cnpj_lists(cls, value: object) -> object:
        """Accept comma-separated environment values or a native list."""

        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("municipal_payment_sources")
    @classmethod
    def normalize_municipal_sources(cls, value: list[str]) -> list[str]:
        """Normalize source identifiers and remove duplicates without losing order."""

        result: list[str] = []
        for item in value:
            source_id = item.strip().lower().replace("-", "_")
            if source_id and source_id not in result:
                result.append(source_id)
        return result

    @field_validator("default_modalities", mode="before")
    @classmethod
    def parse_modalities(cls, value: object) -> object:
        """Accept comma-separated environment values or a native list."""
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("price_registry_lookback_days", mode="before")
    @classmethod
    def optional_registry_period(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

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

    @property
    def effective_price_registry_lookback_days(self) -> int:
        return self.price_registry_lookback_days or self.default_lookback_days

    def public_view(self) -> dict[str, object]:
        """Return a secret-free settings representation for the UI."""
        return {
            "app_env": self.app_env,
            "log_level": self.log_level,
            "test_suite_timeout_seconds": self.test_suite_timeout_seconds,
            "allowed_hosts": self.allowed_hosts,
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
            "llm_max_requests_per_hour": self.llm_max_requests_per_hour,
            "llm_input_price_per_million_usd": str(self.llm_input_price_per_million_usd),
            "llm_output_price_per_million_usd": str(self.llm_output_price_per_million_usd),
            "contact_search_enabled": self.contact_search_enabled,
            "contact_domain_discovery_enabled": self.contact_domain_discovery_enabled,
            "transparencia_configured": bool(self.transparencia_api_key.strip()),
            "transparencia_base_url": self.transparencia_base_url,
            "transparencia_timeout_seconds": self.transparencia_timeout_seconds,
            "transparencia_max_retries": self.transparencia_max_retries,
            "outreach_mode": self.outreach_mode,
            "scheduler_enabled": self.scheduler_enabled,
            "scheduler_mode": "separate_process",
            "scheduler_price_registry_hours": self.scheduler_price_registry_hours,
            "price_registry_lookback_days": self.effective_price_registry_lookback_days,
            "document_batch_size": self.document_batch_size,
            "document_download_concurrency": self.document_download_concurrency,
            "document_process_concurrency": self.document_process_concurrency,
            "document_max_attempts": self.document_max_attempts,
            "document_retry_base_minutes": self.document_retry_base_minutes,
            "crawl_max_records_per_source": self.crawl_max_records_per_source,
            "crawl_max_pages": self.crawl_max_pages,
            "active_refresh_batch_size": self.active_refresh_batch_size,
            "compras_recheck_batch_size": self.compras_recheck_batch_size,
            "collection_incremental_overlap_days": self.collection_incremental_overlap_days,
            "collection_reconciliation_days": self.collection_reconciliation_days,
            "crawl_stale_after_minutes": self.crawl_stale_after_minutes,
            "session_poll_enabled": self.session_poll_enabled,
            "session_poll_minutes": self.session_poll_minutes,
            "session_poll_document_limit": self.session_poll_document_limit,
            "payment_grace_days": self.payment_grace_days,
            "payment_sources": self.payment_sources,
            "municipal_payment_sources": self.municipal_payment_sources,
            "municipal_payment_include_details": self.municipal_payment_include_details,
            "allow_insecure_http_sources": self.allow_insecure_http_sources,
            "municipal_payment_detail_limit": self.municipal_payment_detail_limit,
            "municipal_payment_max_records": self.municipal_payment_max_records,
            "sao_luis_payments_url": self.sao_luis_payments_url,
            "contract_agency_count": len(self.contract_agency_cnpjs),
            "payment_poll_count": len(self.payment_poll_cnpjs),
            "ocr_enabled": self.ocr_enabled,
            "ocr_provider": self.ocr_provider,
            "ocr_language": self.ocr_language,
            "ocr_dpi": self.ocr_dpi,
        }


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide immutable settings instance."""
    return Settings()
