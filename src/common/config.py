"""12-factor configuration. Every value is env-overridable; see .env.example."""

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Committed placeholders. Anything that still equals one of these is known to the
# whole internet; production mode refuses to start on them.
DEV_JWT_SECRET = "dev-only-change-me"
DEFAULT_API_PASSWORD = "contrail"
MIN_PROD_SECRET_LEN = 32

Probability = Annotated[float, Field(ge=0.0, le=1.0)]


def parse_bbox(spec: str | None) -> tuple[float, float, float, float] | None:
    """`lamin,lomin,lamax,lomax` -> floats, or None for the whole world (empty spec)."""
    if not spec or not spec.strip():
        return None
    try:
        lamin, lomin, lamax, lomax = (float(x) for x in spec.split(","))
    except ValueError:
        raise ValueError(
            f"bounding box must be four comma-separated numbers 'lamin,lomin,lamax,lomax', got {spec!r}"
        ) from None
    if not (-90 <= lamin < lamax <= 90 and -180 <= lomin < lomax <= 180):
        raise ValueError(
            f"bounding box {spec!r} out of range: need -90<=lamin<lamax<=90 and -180<=lomin<lomax<=180"
        )
    return lamin, lomin, lamax, lomax


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap: str = "redpanda:29092"
    kafka_raw_topic: str = "flight.events.raw"
    kafka_partitions: int = Field(6, gt=0)
    kafka_consumer_group: str = "contrail-sink"

    # Synthetic generator. Chaos knobs are config-driven so Phase 1 can sweep them.
    # Data source: "synthetic" (default, controllable chaos, used for every
    # benchmark) or "opensky" (real ADS-B, no control over its messiness).
    source: str = "synthetic"
    opensky_poll_interval_s: float = 15.0
    # Central Europe by default. A bounding box costs fewer OpenSky credits than
    # the whole world and keeps the anonymous daily allowance usable.
    opensky_bbox: str = "45,5,55,15"

    gen_aircraft: int = Field(50, gt=0)
    gen_rate_hz: float = Field(1.0, gt=0)
    gen_seed: int = 1337
    chaos_out_of_order_prob: Probability = 0.0
    chaos_max_skew_s: float = Field(5.0, ge=0)
    chaos_duplicate_prob: Probability = 0.0
    chaos_late_prob: Probability = 0.0
    chaos_late_delay_s: float = Field(90.0, ge=0)
    chaos_drop_prob: Probability = 0.0

    # Windowing (Phase 1). allowed_lateness is swept in the 1.3 benchmark.
    window_s: int = Field(60, gt=0)
    allowed_lateness_s: float = Field(30.0, ge=0)
    # Wall-clock seconds of silence before a Kafka partition stops holding back the
    # watermark. None (default) keeps the windowing deterministic and clock-free;
    # when set it must exceed allowed_lateness_s.
    idle_timeout_s: float | None = None

    # Drop flight_events chunks older than this many days (TimescaleDB retention
    # policy, applied when the schema is initialised). None keeps everything forever.
    retention_days: int | None = Field(None, gt=0)

    postgres_host: str = "timescaledb"
    postgres_port: int = 5432
    postgres_db: str = "contrail"
    postgres_user: str = "contrail"
    postgres_password: str = "contrail"

    redis_url: str = "redis://redis:6379/0"
    # Namespace for the window channel and keys. The default reproduces the historic names
    # (contrail.windows, contrail:windows:current, contrail:windows:late_total); change it to
    # run a second, independent pipeline against the same Redis (or to isolate a test).
    redis_key_prefix: str = Field("contrail", min_length=1)

    # Auth + rate limiting. Leaving jwt_secret at this placeholder makes the API
    # sign with a random per-process secret instead (see api/auth.py) -- the
    # placeholder is committed, so honouring it would let anyone forge a token.
    # "production" turns known-weak credentials into a startup error; "development"
    # (the default, so `docker compose up` needs no configuration) only warns.
    app_env: Literal["development", "production"] = "development"
    jwt_secret: str = DEV_JWT_SECRET
    jwt_ttl_s: int = Field(3600, gt=0)
    api_user: str = "operator"
    api_password: str = DEFAULT_API_PASSWORD
    # Login gets its own, much tighter bucket than the API default: it is the one
    # route where a guessed password is the attack.
    login_rate_limit_rps: float = Field(0.5, gt=0)
    login_rate_limit_burst: float = Field(10.0, gt=0)
    rate_limit_rps: float = Field(10.0, gt=0)
    rate_limit_burst: float = Field(20.0, gt=0)
    # Per-process buckets: idle ones are dropped after this long, and the count is capped.
    rate_limit_idle_ttl_s: float = Field(600.0, gt=0)
    rate_limit_max_buckets: int = Field(10_000, gt=0)
    # Reverse proxies in front of the API that append to X-Forwarded-For. 0 = none:
    # the header is ignored and the TCP peer is the client.
    trusted_proxy_hops: int = Field(0, ge=0)
    log_level: str = "INFO"
    metrics_port: int = 9100

    @field_validator("opensky_bbox")
    @classmethod
    def _bbox_parses(cls, v: str) -> str:
        parse_bbox(v)  # raises with an actionable message
        return v

    @model_validator(mode="after")
    def _idle_timeout_exceeds_lateness(self) -> "Settings":
        if self.idle_timeout_s is not None and self.idle_timeout_s <= self.allowed_lateness_s:
            raise ValueError(
                f"IDLE_TIMEOUT_S ({self.idle_timeout_s}) must exceed "
                f"ALLOWED_LATENESS_S ({self.allowed_lateness_s})"
            )
        return self

    @model_validator(mode="after")
    def _production_rejects_default_credentials(self) -> "Settings":
        if self.app_env != "production":
            return self
        problems = []
        if self.api_password == DEFAULT_API_PASSWORD:
            problems.append("API_PASSWORD is still the default")
        if self.jwt_secret == DEV_JWT_SECRET:
            problems.append("JWT_SECRET is still the committed placeholder")
        elif len(self.jwt_secret) < MIN_PROD_SECRET_LEN:
            problems.append(f"JWT_SECRET is shorter than {MIN_PROD_SECRET_LEN} characters")
        if problems:
            raise ValueError(
                "APP_ENV=production refuses to start: " + "; ".join(problems)
                + ". Generate a secret with: python -c \"import secrets; print(secrets.token_urlsafe(48))\""
            )
        return self

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
