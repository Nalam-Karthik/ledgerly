from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "ledgerly"
    environment: str = "dev"

    database_url: str = "postgresql+asyncpg://ledgerly:ledgerly@localhost:5432/ledgerly"
    db_pool_size: int = 20
    db_max_overflow: int = 10

    redis_url: str = "redis://localhost:6379/0"

    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "ledger.events"
    kafka_dlq_topic: str = "ledger.events.dlq"

    jwt_secret: str = "change-me-in-production-please-32b"
    jwt_algorithm: str = "HS256"
    access_token_ttl_s: int = 900
    refresh_token_ttl_s: int = 7 * 24 * 3600
    # Emails that are granted the admin role on registration (bootstrap only).
    admin_emails: list[str] = []

    # Token bucket: `rate_limit_burst` tokens, refilled at `rate_limit_per_s`.
    rate_limit_burst: int = 60
    rate_limit_per_s: float = 20.0

    idempotency_ttl_s: int = 24 * 3600

    outbox_batch_size: int = 100
    outbox_poll_interval_s: float = 0.5

    webhook_max_attempts: int = 5
    webhook_timeout_s: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
