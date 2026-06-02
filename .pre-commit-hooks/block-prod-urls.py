#!/usr/bin/env python3
"""Pre-commit hook: block production-looking URLs in source code.

Scans tracked files under ``src/`` and ``tests/`` for any of the known
production host substrings (RDS, Azure, Confluent Cloud, GCP managed
Redis, ``*.prod.*`` subdomains). This is a defense-in-depth layer on
top of the test-suite isolation guard.

Exit codes:
  0  No production-looking URL found.
  1  At least one match; the commit is blocked.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Production host patterns. Keep in sync with
# tests/conftest.py::_PROD_HOST_FORBIDDEN_SUBSTRINGS.
PROD_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"rds\.amazonaws\.com", re.IGNORECASE),
    re.compile(r"\.amazonaws\.com", re.IGNORECASE),
    re.compile(r"\.azure\.com", re.IGNORECASE),
    re.compile(r"\.windows\.net", re.IGNORECASE),
    re.compile(r"confluent\.cloud", re.IGNORECASE),
    re.compile(r"\.googleapis\.com", re.IGNORECASE),
    re.compile(r"prod\.", re.IGNORECASE),
    re.compile(r"-prod\.", re.IGNORECASE),
    re.compile(r"\.prod$", re.IGNORECASE),
)

# Files that may legitimately mention production hosts (the example env
# files, the conftest guard itself, this hook).
ALLOWLIST: tuple[str, ...] = (
    ".env.example",
    ".env.test.example",
    ".env.docker",
    "tests/conftest.py",
    ".pre-commit-hooks/check-nimbus-env/hooks.py",
    ".pre-commit-hooks/block-prod-urls.py",
    "src/core/config.py",
)


def _list_tracked_files() -> list[Path]:
    """Return the list of tracked files under src/ and tests/."""
    result = subprocess.run(
        ["git", "ls-files", "src/", "tests/"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [REPO_ROOT / line for line in result.stdout.splitlines() if line]


def _scan_file(path: Path) -> list[tuple[int, str]]:
    """Return (line_no, line) tuples that look like production URLs."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    matches: list[tuple[int, str]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for pattern in PROD_PATTERNS:
            if pattern.search(line):
                matches.append((line_no, line))
                break
    return matches


def main() -> int:
    files = [p for p in _list_tracked_files() if p.is_file()]
    if not files:
        sys.stderr.write("[block-prod-urls] no tracked files to scan\n")
        return 0

    sys.stderr.write(f"[block-prod-urls] scanning {len(files)} file(s)...\n")

    problems: list[str] = []
    for path in files:
        if any(part in str(path) for part in ALLOWLIST):
            continue
        for line_no, line in _scan_file(path):
            rel = path.relative_to(REPO_ROOT)
            problems.append(f"{rel}:{line_no}: {line.strip()}")

    if problems:
        sys.stderr.write("[block-prod-urls] FAIL: production-looking URL found in source:\n")
        for problem in problems:
            sys.stderr.write(f"  {problem}\n")
        sys.stderr.write(
            "[block-prod-urls] Use environment variables and Settings "
            "instead of hardcoding hosts.\n"
        )
        return 1

    sys.stderr.write("[block-prod-urls] OK: no production URLs found.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
