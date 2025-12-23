"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """PostgreSQL database settings."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    host: str = "localhost"
    port: int = 5432
    user: str = "parser"
    password: SecretStr = SecretStr("parser")
    name: str = "parser"
    pool_size: int = 10
    max_overflow: int = 20

    @property
    def url(self) -> str:
        """Get async database URL."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def sync_url(self) -> str:
        """Get sync database URL for Alembic."""
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class RabbitMQSettings(BaseSettings):
    """RabbitMQ settings."""

    model_config = SettingsConfigDict(env_prefix="RMQ_")

    host: str = "localhost"
    port: int = 5672
    user: str = "guest"
    password: SecretStr = SecretStr("guest")
    vhost: str = "/"

    @property
    def url(self) -> str:
        """Get AMQP URL."""
        return (
            f"amqp://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}{self.vhost}"
        )


class MinIOSettings(BaseSettings):
    """MinIO/S3 settings."""

    model_config = SettingsConfigDict(env_prefix="MINIO_")

    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: SecretStr = SecretStr("minioadmin")
    secure: bool = False
    bucket_lake: str = "parser-lake"
    bucket_trash: str = "parser-trash"


class DeltaLakeSettings(BaseSettings):
    """Delta Lake settings."""

    model_config = SettingsConfigDict(env_prefix="DELTA_")

    path: str = "s3://parser-lake/delta/"
    bronze_path: str = "s3://parser-lake/delta/bronze/"
    silver_path: str = "s3://parser-lake/delta/silver/"


class AISettings(BaseSettings):
    """AI module settings."""

    model_config = SettingsConfigDict(env_prefix="AI_")

    provider: Literal["anthropic", "openai", "ollama"] = "anthropic"
    anthropic_api_key: SecretStr | None = None
    openai_api_key: SecretStr | None = None
    ollama_base_url: str = "http://localhost:11434"
    model_name: str = "claude-sonnet-4-20250514"
    max_tokens: int = 4000
    temperature: float = 0.1


class WorkerSettings(BaseSettings):
    """Worker settings."""

    model_config = SettingsConfigDict(env_prefix="WORKER_")

    http_prefetch: int = 10
    http_concurrency: int = 50
    browser_prefetch: int = 2
    browser_sessions: int = 5
    request_timeout: int = 30
    max_retries: int = 3


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    app_name: str = "Universal Parser"
    app_version: str = "1.0.0"
    debug: bool = False
    environment: Literal["development", "staging", "production"] = "development"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_prefix: str = "/api/v1"
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    # Logging
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "console"

    # Sub-settings
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    rmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    delta: DeltaLakeSettings = Field(default_factory=DeltaLakeSettings)
    ai: AISettings = Field(default_factory=AISettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
