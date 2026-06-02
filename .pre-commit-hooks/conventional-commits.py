#!/usr/bin/env python3
"""Pre-commit hook: enforce the Nimbus commit message convention.

Runs on the commit-msg hook. Receives the path to a file containing
the commit message (pre-commit passes the path as $1). Validates the
message against the Conventional Commits specification with our
project-specific extensions.

Exit codes:
  0  Message conforms to the convention.
  1  Message violates the convention; prints a remediation hint.

Usage (pre-commit):
    entry: python .pre-commit-hooks/conventional-commits.py
    args: [--types=feat,fix,perf,...] [--scopes=orders,inventory,...]
    stages: [commit-msg]
    language: system
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Defaults (mirrored from docs/COMMIT_MESSAGES.md)
# ---------------------------------------------------------------------------

DEFAULT_TYPES: frozenset[str] = frozenset(
    {
        "feat",
        "fix",
        "perf",
        "refactor",
        "test",
        "docs",
        "build",
        "ci",
        "chore",
        "revert",
        "style",
    }
)

DEFAULT_SCOPES: frozenset[str] = frozenset(
    {
        # Domain modules
        "orders",
        "inventory",
        "payments",
        "notifications",
        "admin",
        "gateway",
        # Cross-cutting
        "core",
        "deps",
        "precommit",
        "docker",
        "ci",
        "api",
        "models",
        "readme",
        "env",
    }
)

# Conventional Commits regex. See https://www.conventionalcommits.org/
# Group 1: type   Group 2: breaking ('!' or empty)   Group 3: scope
CONVENTIONAL_RE = re.compile(
    r"^(?P<type>[a-z]+)"
    r"(?:\((?P<scope>[a-z0-9_-]+)\))?"
    r"(?P<breaking>!)"
    r": "
    r"(?P<subject>[^\n]+)"
    r"(?:\n\n(?P<body>.+))?",
    re.DOTALL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _emit(level: str, message: str) -> None:
    sys.stderr.write(f"[commit-msg] {level}: {message}\n")
    sys.stderr.flush()


def _slugify(line: str) -> str:
    """Normalize a line for display (strip control chars, collapse whitespace)."""
    return " ".join(line.split())


def _first_line_length(msg: str) -> int:
    return len(msg.split("\n", 1)[0])


def _check_subject(text: str) -> list[str]:
    """Validate the subject line rules.

    Rules:
      * first letter is lowercase
      * no trailing period
      * <= 72 characters
      * imperative mood is *not* automatically checked (a heuristic
        check would generate false positives); reviewers should flag
        this in code review.
    """
    errors: list[str] = []
    if not text:
        errors.append("subject is empty")
        return errors
    if text[0].isupper():
        errors.append(f"subject must start lowercase: {text[0]!r}")
    if text.endswith("."):
        errors.append("subject must not end with a period")
    if len(text) > 72:
        errors.append(f"subject exceeds 72 characters (got {len(text)})")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "message_file",
        type=Path,
        nargs="?",
        default=Path(".git/COMMIT_EDITMSG"),
        help="Path to the file containing the commit message.",
    )
    parser.add_argument(
        "--types",
        type=str,
        default=",".join(sorted(DEFAULT_TYPES)),
        help="Comma-separated list of allowed types.",
    )
    parser.add_argument(
        "--scopes",
        type=str,
        default=",".join(sorted(DEFAULT_SCOPES)),
        help="Comma-separated list of allowed scopes. Use empty string to require scope.",
    )
    args = parser.parse_args()

    if not args.message_file.exists():
        _emit("FAIL", f"message file does not exist: {args.message_file}")
        return 1

    raw = args.message_file.read_text(encoding="utf-8")
    # Strip pre-commit's "comment lines" (lines starting with #) and
    # trailing whitespace-only lines.
    lines = [line for line in raw.splitlines() if not line.lstrip().startswith("#")]
    message = "\n".join(lines).strip()

    if not message:
        _emit("FAIL", "commit message is empty")
        return 1

    # Allow merge / revert / fixup messages to pass through unchanged.
    if message.startswith("Merge "):
        return 0
    if message.startswith("Revert "):
        return 0
    if message.startswith("fixup!") or message.startswith("squash!"):
        return 0

    match = CONVENTIONAL_RE.match(message)
    errors: list[str] = []

    if not match:
        errors.append(
            "subject must match the format: <type>(<scope>): <subject>\n"
            "  example: feat(orders): add idempotency key support"
        )
    else:
        types = {t.strip() for t in args.types.split(",") if t.strip()}
        scopes = {s.strip() for s in args.scopes.split(",") if s.strip()}

        commit_type = match.group("type")
        commit_scope = match.group("scope")
        commit_breaking = bool(match.group("breaking"))
        commit_subject = match.group("subject")
        commit_body = match.group("body") or ""

        if commit_type not in types:
            errors.append(
                f"type {commit_type!r} is not allowed. " f"Use one of: {', '.join(sorted(types))}"
            )

        if commit_scope is not None and commit_scope not in scopes:
            errors.append(
                f"scope {commit_scope!r} is not in the allowed list. "
                f"Use one of: {', '.join(sorted(scopes))}. "
                f"If you need a new scope, add it to .pre-commit-config.yaml "
                f"and docs/COMMIT_MESSAGES.md."
            )

        errors.extend(_check_subject(commit_subject))

        if commit_breaking and "BREAKING CHANGE" not in commit_body.upper():
            errors.append(
                "the '!' breaking-change marker requires a 'BREAKING CHANGE: ' "
                "section in the body footer."
            )

    if errors:
        _emit("FAIL", "commit message does not follow the Nimbus convention:")
        for err in errors:
            _emit("FAIL", f"  • {err}")
        _emit("FAIL", "see docs/COMMIT_MESSAGES.md for the full spec.")
        return 1

    _emit(
        "OK",
        f"commit message conforms to the convention ({_first_line_length(message)} chars on first line).",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
