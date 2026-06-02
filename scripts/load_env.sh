#!/usr/bin/env bash
# =============================================================================
# load_env.sh — source the appropriate .env file for the given environment.
# =============================================================================
# Usage:
#     source scripts/load_env.sh           # auto-detect from ENVIRONMENT
#     source scripts/load_env.sh test
#     source scripts/load_env.sh development
#     source scripts/load_env.sh production
# =============================================================================

set -euo pipefail

# Resolve the repo root regardless of where the script is sourced from.
_REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

_requested_env="${1:-${ENVIRONMENT:-development}}"

case "${_requested_env}" in
  test)
    _env_file="${_REPO_ROOT}/.env.test"
    if [[ ! -f "${_env_file}" ]]; then
      # Fall back to the template so we know what's expected.
      if [[ -f "${_REPO_ROOT}/.env.test.example" ]]; then
        echo "load_env: .env.test missing, falling back to .env.test.example" >&2
        _env_file="${_REPO_ROOT}/.env.test.example"
      else
        echo "load_env: ERROR — neither .env.test nor .env.test.example exists." >&2
        # shellcheck disable=SC2317
        return 1 2>/dev/null || exit 1
      fi
    fi
    # Pin ENVIRONMENT=test so conftest's safety net is unambiguous.
    export ENVIRONMENT=test
    ;;
  development|dev|local)
    _env_file="${_REPO_ROOT}/.env"
    if [[ ! -f "${_env_file}" && -f "${_REPO_ROOT}/.env.example" ]]; then
      echo "load_env: .env missing, falling back to .env.example" >&2
      _env_file="${_REPO_ROOT}/.env.example"
    fi
    export ENVIRONMENT=development
    ;;
  staging|prod|production)
    echo "load_env: refusing to source .env for ${_requested_env}; use your secret manager." >&2
    # shellcheck disable=SC2317
    return 2 2>/dev/null || exit 2
    ;;
  *)
    echo "load_env: unknown environment '${_requested_env}'." >&2
    # shellcheck disable=SC2317
    return 3 2>/dev/null || exit 3
    ;;
esac

# Source the file in a way that respects `set -u` in the parent shell.
set -a
# shellcheck disable=SC1090
source "${_env_file}"
set +a

echo "load_env: loaded ${_env_file} (ENVIRONMENT=${ENVIRONMENT:-unset})" >&2
