"""12-factor configuration. Every value is env-overridable; see .env.example."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap: str = "redpanda:29092"
    kafka_raw_topic: str = "flight.events.raw"
    kafka_partitions: int = 6
    kafka_consumer_group: str = "contrail-sink"

    # Synthetic generator. Chaos knobs are config-driven so Phase 1 can sweep them.
    # Data source: "synthetic" (default, controllable chaos, used for every
    # benchmark) or "opensky" (real ADS-B, no control over its messiness).
    source: str = "synthetic"
    opensky_poll_interval_s: float = 15.0
    # Central Europe by default. A bounding box costs fewer OpenSky credits than
    # the whole world and keeps the anonymous daily allowance usable.
    opensky_bbox: str = "45,5,55,15"

    gen_aircraft: int = 50
    gen_rate_hz: float = 1.0
    gen_seed: int = 1337
    chaos_out_of_order_prob: float = 0.0
    chaos_max_skew_s: float = 5.0
    chaos_duplicate_prob: float = 0.0
    chaos_late_prob: float = 0.0
    chaos_late_delay_s: float = 90.0
    chaos_drop_prob: float = 0.0

    # Windowing (Phase 1). allowed_lateness is swept in the 1.3 benchmark.
    window_s: int = 60
    allowed_lateness_s: float = 30.0

    postgres_host: str = "timescaledb"
    postgres_port: int = 5432
    postgres_db: str = "contrail"
    postgres_user: str = "contrail"
    postgres_password: str = "contrail"

    redis_url: str = "redis://redis:6379/0"

    # Auth + rate limiting. jwt_secret MUST be overridden outside local dev;
    # the default exists so `docker compose up` works from a clean checkout.
    jwt_secret: str = "dev-only-change-me"
    jwt_ttl_s: int = 3600
    api_user: str = "operator"
    api_password: str = "contrail"
    rate_limit_rps: float = 10.0
    rate_limit_burst: float = 20.0
    log_level: str = "INFO"
    metrics_port: int = 9100

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
