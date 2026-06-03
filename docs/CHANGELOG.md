# Changelog

All notable changes to the Nimbus Commerce Platform are documented
in this file. The format follows [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

Initial scaffolding and Phase 2 (API Gateway & Abuse Layer).

### Added

#### Gateway — security

- **Password hashing** with `argon2id` (OWASP-recommended parameters:
  `time_cost=3`, `memory_cost=64 MiB`, `parallelism=4`). Includes
  `hash_password`, `verify_password`, and `needs_rehash` for
  transparent parameter upgrades.
- **JWT issuance and validation** with PyJWT, HS256 by default.
  Separate `access` (15 min) and `refresh` (7 day) token types.
  Tokens carry `sub`, `typ`, `iat`, `nbf`, `exp`, `jti`, `iss`,
  `aud`. Refresh token rotation via `jti` denylist is planned.
- `generate_api_key` for opaque API keys with the `nk_` prefix.

#### Gateway — abuse layer

- **User-agent fingerprinting** via `SHA-256(client_ip + "|" +
  user_agent + "|" + accept_language + "|" + extra_salt)`. The
  hash is the only output; raw components are never logged.
- **Sliding-window rate limiter** implemented as an atomic Redis
  Lua script (sorted set keyed by timestamp). One round-trip per
  request; window-cleanup and increment are atomic.
- **Graduated response system**:
  - 100 requests/minute (configurable) → HTTP 429
    `nimbus.rate_limited`.
  - 200 requests/minute (configurable) → fingerprint added to a
    Redis blacklist for 1 hour; subsequent requests get HTTP 403
    `nimbus.blacklisted`.
- **Graceful degradation**: when the Redis circuit breaker is
  open, the abuse layer logs a `CRITICAL` event and lets the
  request through (`abuse.bypassed`). Rationale: better to accept
  some over-rate traffic than to take the entire API down with
  Redis.
- `AbuseMiddleware` (Starlette) installed in `src/main.py` with
  bypass for `/health`, `/ready`, `/metrics`.

#### Core

- Async SQLAlchemy 2.0 engine factory
  (`src/core/database.py`) with `NullPool` in tests, per-connection
  `SET search_path` enforcement via the SQLAlchemy `checkout`
  event, and a pybreaker-backed connection breaker.
- Async Redis client factory (`src/core/cache.py`) with
  `AsyncCircuitBreaker` (closed / open / half-open state machine)
  and exponential-backoff retry on timeout.
- `lifespan` context initializes the `AbuseLayer` once at startup
  and stores it on `app.state.abuse`.
- `GET /ready` readiness probe that reports per-component status
  for `database` and `redis`.

### Security

- `Settings.assert_test_isolation()` is called on every `Settings`
  construction, so production-looking URLs in test envs are
  rejected with `ValueError` before any module is loaded.
- Test isolation is enforced at three layers: conftest
  module-level guard, `Settings.assert_test_isolation()`, and
  `Settings` URL field validators.

### Changed

- `pyproject.toml` adds `PyJWT[crypto]~=2.10.1` to runtime deps.
- `src/core/config.py` adds `database_default_schema`,
  `database_command_timeout_seconds`, `database_module_schemas`,
  `redis_circuit_breaker_fail_max`,
  `redis_circuit_breaker_reset_timeout_seconds`, the full
  `jwt_*` group, and the `abuse_*` group.

[0.1.0]: #010---unreleased
