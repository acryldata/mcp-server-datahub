"""
Tests for the static-token authentication provider.

Test scenarios:
1. build_auth_provider returns None when env var is absent
2. build_auth_provider returns None when env var is empty / whitespace-only
3. build_auth_provider returns None when all comma-separated entries are blank
4. build_auth_provider returns a provider with the correct token count
5. Whitespace around token entries is stripped
6. StaticTokenAuthProvider raises ValueError on empty token list
7. verify_token returns AccessToken for a valid token
8. verify_token returns None for an invalid token
9. All configured tokens are accepted (multi-token rotation support)
10. Timing-safe comparison is used (secrets.compare_digest)
"""

import secrets
from unittest.mock import patch

import pytest

from datahub_integrations.mcp.auth import (
    DATAHUB_MCP_AUTH_TOKENS_ENV_VAR,
    StaticTokenAuthProvider,
    build_auth_provider,
)


class TestBuildAuthProvider:
    """Tests for the build_auth_provider factory function."""

    def test_returns_none_when_env_var_absent(self):
        with patch.dict("os.environ", {}, clear=False):
            # Ensure the variable is not set
            import os

            os.environ.pop(DATAHUB_MCP_AUTH_TOKENS_ENV_VAR, None)
            assert build_auth_provider() is None

    def test_returns_none_when_env_var_empty(self):
        with patch.dict("os.environ", {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: ""}):
            assert build_auth_provider() is None

    def test_returns_none_when_env_var_whitespace_only(self):
        with patch.dict("os.environ", {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: "   "}):
            assert build_auth_provider() is None

    def test_returns_none_when_all_entries_blank(self):
        with patch.dict("os.environ", {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: " , , "}):
            assert build_auth_provider() is None

    def test_returns_provider_for_single_token(self):
        with patch.dict(
            "os.environ", {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: "my-secret-token"}
        ):
            provider = build_auth_provider()
            assert provider is not None
            assert isinstance(provider, StaticTokenAuthProvider)

    def test_returns_provider_for_multiple_tokens(self):
        with patch.dict(
            "os.environ",
            {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: "token-a,token-b,token-c"},
        ):
            provider = build_auth_provider()
            assert provider is not None
            assert len(provider._tokens) == 3

    def test_whitespace_around_tokens_is_stripped(self):
        with patch.dict(
            "os.environ",
            {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: "  token-a , token-b  "},
        ):
            provider = build_auth_provider()
            assert provider is not None
            # Tokens should be stored as stripped bytes
            assert b"token-a" in provider._tokens
            assert b"token-b" in provider._tokens

    def test_blank_entries_among_valid_tokens_are_ignored(self):
        with patch.dict(
            "os.environ",
            {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR: "token-a,,token-b,"},
        ):
            provider = build_auth_provider()
            assert provider is not None
            assert len(provider._tokens) == 2


class TestStaticTokenAuthProvider:
    """Tests for StaticTokenAuthProvider.verify_token."""

    def test_raises_on_empty_token_list(self):
        with pytest.raises(ValueError, match="at least one"):
            StaticTokenAuthProvider([])

    def test_raises_on_all_blank_tokens(self):
        with pytest.raises(ValueError, match="at least one"):
            StaticTokenAuthProvider(["", "  "])

    @pytest.mark.asyncio
    async def test_valid_token_returns_access_token(self):
        provider = StaticTokenAuthProvider(["correct-token"])
        result = await provider.verify_token("correct-token")
        assert result is not None
        assert result.token == "correct-token"
        assert result.client_id == "mcp-client"
        assert result.scopes == []

    @pytest.mark.asyncio
    async def test_invalid_token_returns_none(self):
        provider = StaticTokenAuthProvider(["correct-token"])
        result = await provider.verify_token("wrong-token")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_string_token_returns_none(self):
        provider = StaticTokenAuthProvider(["correct-token"])
        result = await provider.verify_token("")
        assert result is None

    @pytest.mark.asyncio
    async def test_all_configured_tokens_are_accepted(self):
        tokens = ["token-alpha", "token-beta", "token-gamma"]
        provider = StaticTokenAuthProvider(tokens)
        for token in tokens:
            result = await provider.verify_token(token)
            assert result is not None, f"Token '{token}' should be accepted"

    @pytest.mark.asyncio
    async def test_only_configured_tokens_are_accepted(self):
        provider = StaticTokenAuthProvider(["valid"])
        assert await provider.verify_token("invalid") is None
        assert await provider.verify_token("VALID") is None  # case-sensitive
        assert await provider.verify_token("valid ") is None  # trailing space

    @pytest.mark.asyncio
    async def test_comparison_uses_secrets_compare_digest(self):
        """Verify we delegate to secrets.compare_digest (timing-safe)."""
        provider = StaticTokenAuthProvider(["my-token"])
        with patch("mcp_server_datahub.auth.secrets.compare_digest") as mock_digest:
            mock_digest.return_value = True
            result = await provider.verify_token("my-token")
            assert mock_digest.called
            assert result is not None

    @pytest.mark.asyncio
    async def test_cryptographically_random_token_is_accepted(self):
        """End-to-end: generate a real random token and verify it round-trips."""
        token = secrets.token_urlsafe(32)
        provider = StaticTokenAuthProvider([token])
        result = await provider.verify_token(token)
        assert result is not None
        assert result.token == token
