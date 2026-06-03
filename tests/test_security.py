"""Tests for the gateway security module.

These tests do not require any external service (no DB, no Redis).
"""

from __future__ import annotations

import time

import pytest

from src.core.config import get_settings
from src.modules.gateway.security import (
    PasswordError,
    TokenError,
    TokenType,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
    generate_api_key,
    hash_password,
    needs_rehash,
    verify_password,
)


class TestPasswordHashing:
    def test_hash_then_verify_round_trip(self) -> None:
        encoded = hash_password("correct horse battery staple")
        assert encoded.startswith("$argon2id$")
        assert verify_password("correct horse battery staple", encoded) is True
        assert verify_password("wrong password", encoded) is False

    def test_hash_distinct_for_same_password(self) -> None:
        # Two hashes of the same password must differ (random salt).
        a = hash_password("hunter2")
        b = hash_password("hunter2")
        assert a != b

    def test_empty_password_raises(self) -> None:
        with pytest.raises(PasswordError):
            hash_password("")

    def test_verify_empty_inputs(self) -> None:
        encoded = hash_password("hunter2")
        assert verify_password("", encoded) is False
        assert verify_password("hunter2", "") is False

    def test_malformed_hash_raises(self) -> None:
        with pytest.raises(PasswordError):
            verify_password("hunter2", "not-a-real-argon2-hash")

    def test_needs_rehash_returns_bool(self) -> None:
        encoded = hash_password("hunter2")
        assert isinstance(needs_rehash(encoded), bool)


class TestAccessToken:
    def test_round_trip(self) -> None:
        token = create_access_token("user-123", extra_claims={"role": "admin"})
        claims = decode_access_token(token)
        assert claims.subject == "user-123"
        assert claims.token_type is TokenType.ACCESS
        assert claims.raw.get("role") == "admin"

    def test_expired_raises(self) -> None:
        # ttl=1, then sleep just over 1 second to force expiration.
        token = create_access_token("user-123", ttl_seconds=1)
        time.sleep(1.5)
        with pytest.raises(TokenError, match="expired"):
            decode_access_token(token)

    def test_wrong_secret_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        token = create_access_token("user-123")
        # Rotate the secret to simulate a key change.
        settings = get_settings()
        monkeypatch.setattr(settings, "jwt_secret_key", "x" * 64)
        with pytest.raises(TokenError):
            decode_access_token(token)

    def test_garbage_token_raises(self) -> None:
        with pytest.raises(TokenError):
            decode_access_token("not-a-jwt")

    def test_ttl_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            create_access_token("user-123", ttl_seconds=0)
        with pytest.raises(ValueError):
            create_access_token("user-123", ttl_seconds=-1)


class TestRefreshToken:
    def test_refresh_cannot_be_used_as_access(self) -> None:
        refresh = create_refresh_token("user-123")
        # Trying to decode a refresh token as an access token must fail.
        with pytest.raises(TokenError, match="access"):
            decode_access_token(refresh)

    def test_access_cannot_be_used_as_refresh(self) -> None:
        access = create_access_token("user-123")
        with pytest.raises(TokenError, match="refresh"):
            decode_refresh_token(access)

    def test_refresh_round_trip(self) -> None:
        token = create_refresh_token("user-123")
        claims = decode_refresh_token(token)
        assert claims.subject == "user-123"
        assert claims.token_type is TokenType.REFRESH

    def test_jti_is_unique(self) -> None:
        a = create_access_token("user-1")
        b = create_access_token("user-1")
        assert decode_access_token(a).jti != decode_access_token(b).jti

    def test_issuer_and_audience_enforced(self, monkeypatch: pytest.MonkeyPatch) -> None:
        token = create_access_token("user-1")
        settings = get_settings()
        monkeypatch.setattr(settings, "jwt_audience", "other-audience")
        with pytest.raises(TokenError):
            decode_access_token(token)


class TestApiKey:
    def test_format(self) -> None:
        key = generate_api_key()
        assert key.startswith("nk_")
        # token_urlsafe(32) -> 43 chars; total = 3 + 43 = 46
        assert len(key) == 46

    def test_unique(self) -> None:
        keys = {generate_api_key() for _ in range(100)}
        assert len(keys) == 100

    def test_invalid_prefix_rejected(self) -> None:
        with pytest.raises(ValueError):
            generate_api_key("a b c")
        with pytest.raises(ValueError):
            generate_api_key("")
