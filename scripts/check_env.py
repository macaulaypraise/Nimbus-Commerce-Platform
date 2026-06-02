#!/usr/bin/env python3
"""Validate the active environment configuration without starting the app.

This script has TWO modes:

  1. FAST MODE (default, zero dependencies)
     Parses .env-style files with the stdlib only. Verifies presence of
     required keys and runs the same test-isolation guard as the test
     suite. Useful as a pre-commit-style gate, as a Docker entrypoint
     check, and as a developer sanity check before the full app deps
     are even installed.

  2. STRICT MODE (--strict)
     In addition to the fast checks, imports src.core.config.Settings
     so any alias / validator logic lives next to the application
     code. This is what CI should run.

Exit codes:
  0  Environment is valid for the requested mode.
  1  A non-test guard failure (DEBUG=true in production, missing URL).
  2  Test isolation violation (refused; would have touched production data).
  3  Misuse (bad --mode, file not found, etc.).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Make ``src`` importable for the strict-mode path.
sys.path.insert(0, str(_REPO_ROOT))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Host substrings that strongly indicate a production / managed-service
# endpoint. Mirrors tests/conftest.py::_PROD_HOST_FORBIDDEN_SUBSTRINGS.
_PROD_HOST_FORBIDDEN_SUBSTRINGS: tuple[str, ...] = (
    "rds.amazonaws.com",
    "amazonaws.com",
    "azure.com",
    "windows.net",
    "confluent.cloud",
    "memorystore.googleapis.com",
    "prod-",
    "-prod.",
    ".prod.",
)

# Files we know about, in priority order for auto-detection.
ENV_FILE_CANDIDATES: tuple[str, ...] = (
    ".env.{env}",
    ".env",
)

# Required keys per file. Mirrors the structure of src/core/config.py.
_REQUIRED_KEYS: Mapping[str, frozenset[str]] = {
    ".env": frozenset(
        {
            "ENVIRONMENT",
            "APP_NAME",
            "APP_VERSION",
            "DATABASE_URL",
            "REDIS_URL",
            "KAFKA_BOOTSTRAP_SERVERS",
            "SECRET_KEY",
        }
    ),
    ".env.test": frozenset(
        {
            "ENVIRONMENT",
            "APP_NAME",
            "APP_VERSION",
            "TEST_DATABASE_URL",
            "TEST_REDIS_URL",
            "TEST_KAFKA_BOOTSTRAP_SERVERS",
            "SECRET_KEY",
        }
    ),
    ".env.example": frozenset(
        {
            "ENVIRONMENT",
            "APP_NAME",
            "APP_VERSION",
            "DATABASE_URL",
            "REDIS_URL",
            "KAFKA_BOOTSTRAP_SERVERS",
            "SECRET_KEY",
        }
    ),
    ".env.test.example": frozenset(
        {
            "ENVIRONMENT",
            "APP_NAME",
            "APP_VERSION",
            "TEST_DATABASE_URL",
            "TEST_REDIS_URL",
            "TEST_KAFKA_BOOTSTRAP_SERVERS",
            "SECRET_KEY",
        }
    ),
    ".env.docker": frozenset(
        {
            "ENVIRONMENT",
            "APP_NAME",
            "APP_VERSION",
            "DATABASE_URL",
            "REDIS_URL",
            "KAFKA_BOOTSTRAP_SERVERS",
        }
    ),
}


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def _emit(level: str, message: str) -> None:
    stream = sys.stderr if level in {"FAIL", "WARN"} else sys.stdout
    print(f"check_env: {level:4} — {message}", file=stream)


def _fail(message: str, *, code: int = 1) -> None:
    _emit("FAIL", message)
    sys.exit(code)


def _warn(message: str) -> None:
    _emit("WARN", message)


def _ok(message: str) -> None:
    _emit("OK", message)


# ---------------------------------------------------------------------------
# .env parsing (stdlib only)
# ---------------------------------------------------------------------------


_COMMENT_OR_BLANK = re.compile(r"^\s*(?:#.*)?$")


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse a .env file into a dict.

    Honors ``#`` comments and blank lines. Does NOT support escape
    sequences, multi-line values, or variable expansion — none of those
    are used by our env templates.
    """
    if not path.exists():
        return {}
    parsed: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if _COMMENT_OR_BLANK.match(line):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        parsed[key] = value.strip()
    return parsed


def load_active_env(env: str) -> tuple[Path | None, dict[str, str]]:
    """Load the .env file for ``env`` if it exists.

    Does NOT mutate ``os.environ`` — we want the script to be a pure
    validator. Callers can choose to merge the result into os.environ
    after the script returns 0.
    """
    for candidate_name in ENV_FILE_CANDIDATES:
        candidate = _REPO_ROOT / candidate_name.format(env=env)
        if candidate.exists():
            return candidate, parse_env_file(candidate)
    fallback = _REPO_ROOT / ".env"
    if fallback.exists():
        return fallback, parse_env_file(fallback)
    return None, {}


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def guard_test_isolation(env_vars: Mapping[str, str], env_name: str) -> None:
    """Apply the same rules as tests/conftest.py::_enforce_test_isolation.

    Raises:
        SystemExit(2): on any violation.
    """
    if env_name != "test":
        return

    errors: list[str] = []

    for prod_var in ("DATABASE_URL", "REDIS_URL", "KAFKA_BOOTSTRAP_SERVERS"):
        if env_vars.get(prod_var):
            errors.append(
                f"Production env var {prod_var}={env_vars[prod_var]!r} is "
                f"set in test context. Unset it and rely on TEST_{prod_var}."
            )

    test_db = env_vars.get("TEST_DATABASE_URL", "")
    test_redis = env_vars.get("TEST_REDIS_URL", "")

    if not test_db:
        errors.append("TEST_DATABASE_URL is not set.")
    if not test_redis:
        errors.append("TEST_REDIS_URL is not set.")

    for url, name in ((test_db, "TEST_DATABASE_URL"), (test_redis, "TEST_REDIS_URL")):
        if not url:
            continue
        try:
            parsed = urlparse(url)
        except ValueError as exc:
            errors.append(f"{name}={url!r} is not a valid URL: {exc}")
            continue

        host = (parsed.hostname or "").lower()
        scheme = (parsed.scheme or "").lower()
        for forbidden in _PROD_HOST_FORBIDDEN_SUBSTRINGS:
            if forbidden in host:
                errors.append(
                    f"{name}={url!r} contains forbidden production host "
                    f"substring {forbidden!r}."
                )

        if name == "TEST_DATABASE_URL":
            db_name = parsed.path.lstrip("/").split("?")[0]
            if "test" not in db_name.lower():
                errors.append(
                    f"TEST_DATABASE_URL={url!r} database name {db_name!r} "
                    f"does not contain 'test'."
                )
            if scheme not in ("postgresql", "postgresql+asyncpg", "postgres"):
                errors.append(
                    f"TEST_DATABASE_URL={url!r} has scheme {scheme!r}; "
                    f"expected 'postgresql+asyncpg' or 'postgresql'."
                )

        if name == "TEST_REDIS_URL":
            db_segment = parsed.path.lstrip("/").split("?")[0] or "0"
            if db_segment == "0":
                errors.append(
                    f"TEST_REDIS_URL={url!r} uses Redis db 0. "
                    f"Use a dedicated test database (1-15)."
                )
            if scheme not in ("redis", "rediss"):
                errors.append(
                    f"TEST_REDIS_URL={url!r} has scheme {scheme!r}; "
                    f"expected 'redis' or 'rediss'."
                )

    if errors:
        _emit("FAIL", "TEST ISOLATION VIOLATION — ABORTING")
        for e in errors:
            _emit("FAIL", f"  • {e}")
        _emit("FAIL", "Production data may be at risk. Refusing to run.")
        sys.exit(2)


def guard_production_safety(env_vars: Mapping[str, str], env_name: str) -> None:
    """Apply production-safety checks. Exit 1 on violation."""
    if env_name not in {"production", "staging"}:
        return

    debug = env_vars.get("DEBUG", "false").lower() in {"1", "true", "yes", "on"}
    if debug:
        _fail("DEBUG=true is set in a production-like environment.")

    log_level = env_vars.get("LOG_LEVEL", "INFO").upper()
    if log_level == "DEBUG":
        _fail("LOG_LEVEL=DEBUG in a production-like environment.")

    if not env_vars.get("CORS_ORIGINS", ""):
        _warn("CORS_ORIGINS is empty; CORS is effectively disabled.")

    for url, name in (
        (env_vars.get("DATABASE_URL", ""), "DATABASE_URL"),
        (env_vars.get("REDIS_URL", ""), "REDIS_URL"),
    ):
        if "CHANGE_ME" in url or "change-me" in url.lower():
            _fail(
                f"{name} still contains the CHANGE_ME placeholder. "
                f"Set a real value via your secret manager."
            )

    secret = env_vars.get("SECRET_KEY", "")
    if not secret or "CHANGE_ME" in secret or "test" in secret.lower():
        _fail("SECRET_KEY is unset, placeholder, or looks like a test value.")


def guard_required_keys(env_vars: Mapping[str, str], source_file: Path | None) -> None:
    """Verify the loaded env file declares every required key."""
    if source_file is None:
        return
    required = _REQUIRED_KEYS.get(source_file.name, frozenset())
    if not required:
        return
    missing = sorted(required - env_vars.keys())
    if missing:
        _fail(
            f"{source_file.name} is missing required keys: "
            f"{', '.join(missing)}. See .env.example for the full list."
        )


# ---------------------------------------------------------------------------
# Strict mode: also exercise the real Settings class
# ---------------------------------------------------------------------------


def run_strict_mode(env_vars: Mapping[str, str], env_name: str) -> None:
    """Import src.core.config.Settings and let it validate the env.

    This catches alias / validator logic that lives next to the app code.
    Lazy import: failures are reported with a clear remediation message
    rather than a raw stack trace.
    """
    # Merge parsed env file into os.environ for the Settings constructor.
    # The script does not mutate os.environ BEFORE this point so that the
    # fast-mode checks above are based on a stable view.
    for key, value in env_vars.items():
        os.environ.setdefault(key, value)
    os.environ["ENVIRONMENT"] = env_name

    try:
        from src.core.config import Settings
    except ModuleNotFoundError as exc:
        _fail(
            f"strict mode requires the project to be installed "
            f"(missing: {exc.name!r}). Run: pip install -e '.[dev]'"
        )
    except Exception as exc:
        _fail(f"failed to import src.core.config: {exc!r}")

    try:
        Settings()  # type: ignore[call-arg]  # mypy equivalent, also works in Pyright
    except ValueError as exc:
        _fail(f"src.core.config.Settings rejected the environment: {exc!r}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("auto", "development", "staging", "production", "test"),
        default="auto",
        help="Validation mode. Default: auto (use ENVIRONMENT env var).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Also import src.core.config.Settings so its validators run. "
            "Requires the project to be installed (pip install -e '.[dev]')."
        ),
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Override the .env file to read from.",
    )
    args = parser.parse_args()

    env_name = args.mode if args.mode != "auto" else os.getenv("ENVIRONMENT", "development")

    # --- Load .env file (or use --file) ---------------------------------
    if args.file is not None:
        if not args.file.exists():
            _fail(f"--file {args.file!r} does not exist", code=3)
        source_file, env_vars = args.file, parse_env_file(args.file)
    else:
        source_file, env_vars = load_active_env(env_name)

    if source_file is None:
        _warn(
            f"no .env file found for environment={env_name!r}. "
            f"Falling back to live process env vars."
        )
        env_vars = {k: v for k, v in os.environ.items() if k in _flatten_required()}
    else:
        _ok(f"loaded {source_file.relative_to(_REPO_ROOT)}")

    # --- Guards (always run) -------------------------------------------
    guard_required_keys(env_vars, source_file)
    guard_test_isolation(env_vars, env_name)
    guard_production_safety(env_vars, env_name)

    # --- Strict mode: real Settings construction ------------------------
    if args.strict:
        run_strict_mode(env_vars, env_name)

    # --- Report ---------------------------------------------------------
    _ok(f"environment       = {env_name}")
    _ok(f"app_name          = {env_vars.get('APP_NAME', '<unset>')}")
    _ok(f"app_version       = {env_vars.get('APP_VERSION', '<unset>')}")
    _ok(
        f"database_url      = {_scrub(env_vars.get('TEST_DATABASE_URL') or env_vars.get('DATABASE_URL', ''))}"
    )
    _ok(
        f"redis_url         = {_scrub(env_vars.get('TEST_REDIS_URL') or env_vars.get('REDIS_URL', ''))}"
    )
    _ok(
        f"kafka_servers     = {env_vars.get('TEST_KAFKA_BOOTSTRAP_SERVERS') or env_vars.get('KAFKA_BOOTSTRAP_SERVERS', '<unset>')}"
    )
    _ok("environment check: passed")


def _flatten_required() -> frozenset[str]:
    keys: set[str] = set()
    for group in _REQUIRED_KEYS.values():
        keys.update(group)
    return frozenset(keys)


def _scrub(url: str) -> str:
    """Strip userinfo from a URL for safe display."""
    if not url:
        return "<unset>"
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        netloc = parsed.hostname or ""
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return f"{parsed.scheme}://***@{netloc}{parsed.path}"
    return url


if __name__ == "__main__":
    main()
