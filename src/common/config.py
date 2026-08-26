"""12-factor configuration. Every value is env-overridable; see .env.example."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap: str = "redpanda:29092"
    kafka_raw_topic: str = "flight.events.raw"
    kafka_partitions: int = 6

    # Synthetic generator. Chaos knobs are config-driven so Phase 1 can sweep them.
    gen_aircraft: int = 50
    gen_rate_hz: float = 1.0
    gen_seed: int = 1337
    chaos_out_of_order_prob: float = 0.0
    chaos_max_skew_s: float = 5.0
    chaos_duplicate_prob: float = 0.0
    chaos_late_prob: float = 0.0
    chaos_late_delay_s: float = 90.0
    chaos_drop_prob: float = 0.0

    postgres_host: str = "timescaledb"
    postgres_port: int = 5432
    postgres_db: str = "contrail"
    postgres_user: str = "contrail"
    postgres_password: str = "contrail"

    redis_url: str = "redis://redis:6379/0"

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
