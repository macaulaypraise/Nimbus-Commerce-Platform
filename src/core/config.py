"""Application configuration loaded from environment variables.

Design notes:
  * No ``env_file`` is configured by default. We follow the 12-factor
    principle: configuration comes from the environment, never from
    files silently loaded from disk. Local developers can opt in by
    setting ``NIMBUS_ENV_FILE=.env`` in their shell.
  * Production / staging URL env vars (DATABASE_URL, REDIS_URL, ...) are
    explicitly forbidden in test contexts. ``assert_test_isolation``
    enforces this from inside the process; ``tests/conftest.py`` enforces
    it from the outside. Defense in depth.
  * Comma-separated list env vars (CORS_ORIGINS, KAFKA_BOOTSTRAP_SERVERS,
    ...) use :class:`CsvStrList`, which is a ``list[str]`` annotated with
    :class:`pydantic_settings.NoDecode` so pydantic-settings doesn't
    JSON-parse the raw value, and a ``mode="before"`` field validator
    that splits the CSV string.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

from fastapi import Depends
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

Environment = Literal["development", "staging", "production", "test"]


# ---------------------------------------------------------------------------
# CSV-list type
# ---------------------------------------------------------------------------
#
# pydantic-settings v2.7+ runs ``json.loads()`` on env values for any
# complex (non-primitive) type BEFORE any pydantic ``BeforeValidator``
# runs. That means a CSV string like "127.0.0.1:9092" gets a JSON parse
# error before we get a chance to split it.
#
# The official escape hatch is :class:`NoDecode`, which tells
# pydantic-settings "hand the raw string to pydantic untouched, no
# JSON parsing". Then we split the string with a field validator.
# ---------------------------------------------------------------------------


def _split_csv(value: Any) -> list[str]:
    """Turn a CSV string (or pass-through list) into a clean list of str.

    >>> _split_csv("a,b,c")
    ['a', 'b', 'c']
    >>> _split_csv(" a , , b ")
    ['a', 'b']
    >>> _split_csv(["a", "b"])
    ['a', 'b']
    >>> _split_csv(None)
    []
    """
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    raise TypeError(
        f"Cannot coerce {type(value).__name__!r} to list[str]. "
        f"Provide a comma-separated string or a list of strings."
    )


# Annotated type: list[str] that pydantic-settings must NOT JSON-parse.
# The actual CSV split happens in the field validator below.
CsvStrList = Annotated[list[str], NoDecode]


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Strongly-typed application settings."""

    model_config = SettingsConfigDict(
        # Intentionally not setting ``env_file``: we want env vars only.
        # Developers can opt-in with NIMBUS_ENV_FILE=.env.
        env_file=os.getenv("NIMBUS_ENV_FILE"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Runtime environment -------------------------------------------
    environment: Environment = Field(default="development")
    debug: bool = Field(default=False)

    # --- App metadata ---------------------------------------------------
    app_name: str = "nimbus"
    app_version: str = "0.1.0"
    api_prefix: str = "/api/v1"
    cors_origins: CsvStrList = Field(default_factory=list)

    # --- Database (PostgreSQL via asyncpg) ------------------------------
    database_url: str = Field(
        validation_alias=AliasChoices("DATABASE_URL", "TEST_DATABASE_URL"),
    )
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_pool_timeout_seconds: float = 10.0
    database_echo: bool = False
    database_default_schema: str = "public"
    database_command_timeout_seconds: float = 30.0
    database_health_check_interval_seconds: int = 30
    # Per-module schema allowlist. Each module may only write to its
    # own schema. An empty list disables enforcement (use only in dev).
    database_module_schemas: dict[str, str] = Field(default_factory=dict)

    # --- Redis ----------------------------------------------------------
    redis_url: str = Field(
        validation_alias=AliasChoices("REDIS_URL", "TEST_REDIS_URL"),
    )
    redis_max_connections: int = 20
    redis_socket_timeout_seconds: float = 5.0
    redis_health_check_interval_seconds: int = 30
    redis_circuit_breaker_fail_max: int = 5
    redis_circuit_breaker_reset_timeout_seconds: float = 30.0

    # --- JWT (auth) -----------------------------------------------------
    jwt_secret_key: str = Field(
        default="dev-only-secret-replace-me",
        description=(
            "HMAC secret for JWT signing. MUST be a random 32+ byte "
            "string in production. Distinct from SECRET_KEY so the "
            "JWT secret can be rotated independently."
        ),
    )
    jwt_algorithm: str = "HS256"
    jwt_issuer: str = "nimbus"
    jwt_audience: str = "nimbus-api"
    jwt_access_token_ttl_seconds: int = 900  # 15 minutes
    jwt_refresh_token_ttl_seconds: int = 604_800  # 7 days

    # --- Abuse layer (gateway) -----------------------------------------
    abuse_rate_limit_window_seconds: int = 60
    abuse_rate_limit_max: int = 100  # 429 above this
    abuse_blacklist_threshold: int = 200  # blacklist above this
    abuse_blacklist_ttl_seconds: int = 3_600  # 1 hour
    abuse_trust_forwarded_for: bool = False  # set true behind a trusted proxy

    # --- Kafka ----------------------------------------------------------
    kafka_bootstrap_servers: CsvStrList = Field(
        default_factory=lambda: ["127.0.0.1:9092"],
        validation_alias=AliasChoices(
            "KAFKA_BOOTSTRAP_SERVERS",
            "TEST_KAFKA_BOOTSTRAP_SERVERS",
        ),
    )
    kafka_consumer_group: str = "nimbus"
    kafka_request_timeout_ms: int = 30_000

    # --- Telemetry ------------------------------------------------------
    log_level: str = "INFO"
    log_json: bool | None = Field(
        default=None,
        description=(
            "Force JSON logs. If unset, JSON is used in production/staging, "
            "pretty console output is used elsewhere."
        ),
    )

    # --- Convenience properties ----------------------------------------
    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_staging(self) -> bool:
        return self.environment == "staging"

    @property
    def is_test(self) -> bool:
        return self.environment == "test"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def use_json_logs(self) -> bool:
        """Resolve the effective log format decision."""
        if self.log_json is not None:
            return self.log_json
        return self.is_production or self.is_staging

    # --- Validators -----------------------------------------------------
    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(
                f"Invalid log_level {value!r}. "
                f"Use one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
            )
        return normalized

    @field_validator("cors_origins", "kafka_bootstrap_servers", mode="before")
    @classmethod
    def _parse_csv_fields(cls, value: Any) -> Any:
        """Split a CSV string into a list of strings.

        Runs in ``mode="before"`` so it sees the raw env value (a string
        like ``"a,b,c"``) and returns a list. Pydantic-settings' JSON
        decoder is bypassed by the :class:`NoDecode` annotation on the
        :data:`CsvStrList` type.
        """
        return _split_csv(value)

    # --- Test isolation guard ------------------------------------------
    _PROD_HOST_SUBSTRINGS: tuple[str, ...] = (
        "rds.amazonaws.com",
        "amazonaws.com",
        "azure.com",
        "windows.net",
        "confluent.cloud",
        "memorystore.googleapis.com",
    )

    def assert_test_isolation(self) -> None:
        """Raise ``ValueError`` if this Settings instance looks production-like
        but is loaded in a test environment, or vice versa.

        Called from ``tests/conftest.py`` and from any test fixture that
        touches the database / cache.
        """
        if not self.is_test:
            return

        for url, name in (
            (self.database_url, "TEST_DATABASE_URL"),
            (self.redis_url, "TEST_REDIS_URL"),
        ):
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()

            for forbidden in self._PROD_HOST_SUBSTRINGS:
                if forbidden in host:
                    raise ValueError(
                        f"Test isolation violated: {name}={url!r} contains "
                        f"forbidden production host substring {forbidden!r}."
                    )

            if name == "TEST_DATABASE_URL":
                db_name = parsed.path.lstrip("/").split("?")[0]
                if "test" not in db_name.lower():
                    raise ValueError(
                        f"Test isolation violated: TEST_DATABASE_URL database "
                        f"name {db_name!r} does not contain 'test'."
                    )

            if name == "TEST_REDIS_URL":
                db_segment = parsed.path.lstrip("/").split("?")[0] or "0"
                if db_segment == "0":
                    raise ValueError(
                        "Test isolation violated: TEST_REDIS_URL uses db 0 "
                        "(the default). Use a dedicated test db (1-15)."
                    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-singleton :class:`Settings` instance.

    The cache is intentionally small (size 1) and process-scoped. Tests
    override behavior via ``app.dependency_overrides[get_settings]``,
    which does not interact with this cache.
    """
    settings = Settings()  # type: ignore[call-arg]
    settings.assert_test_isolation()
    return settings


# FastAPI dependency injection alias for ergonomic route signatures.
SettingsDep = Annotated[Settings, Depends(get_settings)]
