#!/usr/bin/env python3
"""Pre-commit hook: env file safety for the Nimbus platform.

Runs in two phases:

  PHASE 1 — BLOCK POPULATED SECRETS FILES
    Refuses to commit .env or .env.test if they contain real-looking
    secrets. The .example templates are always allowed.

  PHASE 2 — REQUIRED-KEY AUDIT
    For every .env* file present in the working tree, verifies the
    presence of the keys required by src/core/config.py. Missing keys
    fail the hook with a clear remediation message.

Exit codes:
  0  All checks passed.
  1  At least one check failed.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Make the hook robust when invoked from any working directory.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Files we audit. Note: .env.docker is intentionally excluded because
# it is itself a committed template.
ENV_FILES: tuple[str, ...] = (
    ".env",
    ".env.test",
    ".env.example",
    ".env.test.example",
    ".env.docker",
)

# Required keys, grouped by file. We check membership only — not values.
REQUIRED_KEYS: dict[str, frozenset[str]] = {
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

# Patterns that indicate a .env or .env.test has been populated with
# something other than a placeholder. If any of these match, the hook
# blocks the commit. Templates (.example) are exempt.
_POPULATED_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^SECRET_KEY=(?!CHANGE_ME|dev-only-secret|test-secret)", re.MULTILINE),
    re.compile(
        r"^DATABASE_URL=postgresql\+asyncpg://(?!CHANGE_ME|nimbus:nimbus@127)", re.MULTILINE
    ),
    re.compile(
        r"^TEST_DATABASE_URL=postgresql\+asyncpg://(?!CHANGE_ME|nimbus:nimbus@127)", re.MULTILINE
    ),
    re.compile(r"^REDIS_URL=redis://(?!CHANGE_ME|127)", re.MULTILINE),
    re.compile(r"^TEST_REDIS_URL=redis://(?!CHANGE_ME|127)", re.MULTILINE),
    re.compile(
        r"https?://[a-zA-Z0-9_-]+:[a-zA-Z0-9_!@#$%^&*()-]{8,}@"
    ),  # URL with embedded credentials
    re.compile(r"AKIA[0-9A-Z]{16}"),  # AWS access key
    re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----"),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log(level: str, message: str) -> None:
    sys.stderr.write(f"[check-nimbus-env] {level}: {message}\n")
    sys.stderr.flush()


def _parse_env_file(path: Path) -> dict[str, str]:
    """Lightweight .env parser sufficient for the key-presence check.

    Honors ``#`` comments and skips blank lines. Does NOT handle escape
    sequences or multi-line values — those do not appear in our files.
    """
    parsed: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        parsed[key.strip()] = value.strip()
    return parsed


# ---------------------------------------------------------------------------
# Phase 1: block populated secret files
# ---------------------------------------------------------------------------


def _phase1_block_populated() -> list[str]:
    errors: list[str] = []
    for filename in (".env", ".env.test"):
        path = REPO_ROOT / filename
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"{filename}: cannot read ({exc})")
            continue

        for pattern in _POPULATED_PATTERNS:
            if pattern.search(content):
                errors.append(
                    f"{filename} appears to contain real secrets or "
                    f"non-placeholder values. Populated .env / .env.test "
                    f"files must NEVER be committed. Either:\n"
                    f"    - revert to placeholder values, OR\n"
                    f"    - delete the file and use .env.example / "
                    f".env.test.example as templates."
                )
                break
    return errors


# ---------------------------------------------------------------------------
# Phase 2: required-key audit
# ---------------------------------------------------------------------------


def _phase2_audit_keys() -> list[str]:
    errors: list[str] = []
    for filename in ENV_FILES:
        path = REPO_ROOT / filename
        if not path.exists():
            # Missing files are tolerated for optional env files; required
            # ones are listed in REQUIRED_KEYS. We only error if a file
            # that IS present is missing required keys.
            continue
        required = REQUIRED_KEYS.get(filename, frozenset())
        if not required:
            continue
        parsed = _parse_env_file(path)
        missing = sorted(required - parsed.keys())
        if missing:
            errors.append(
                f"{filename} is missing required keys: {', '.join(missing)}. "
                f"See .env.example for the full list of supported variables."
            )
    return errors


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    _log("info", "running Nimbus env file safety checks...")
    errors = _phase1_block_populated() + _phase2_audit_keys()
    if errors:
        for err in errors:
            _log("FAIL", err)
        _log("FAIL", f"{len(errors)} env-file issue(s) found. Commit blocked.")
        return 1
    _log("OK", "all env-file checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
