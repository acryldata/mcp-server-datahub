"""Tests for per-request DataHub client middleware.

Verifies that _DataHubClientMiddleware:
- Creates a per-request client from Bearer token
- Falls back to the startup client when no auth header
- Closes per-request clients after requests (even on error)
- Does NOT close the fallback client after requests
- Raises RuntimeError when no client is available
- Validates and resolves tokens when token_validator is set
- Passes tokens through when no token_validator (backwards compatible)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_server_datahub.__main__ import _DataHubClientMiddleware
from mcp_server_datahub.mcp_server import get_datahub_client


@pytest.fixture
def mock_fallback_client() -> MagicMock:
    """Create a mock fallback DataHubClient."""
    client = MagicMock()
    client._graph = MagicMock()
    return client


@pytest.fixture
def mock_per_request_client() -> MagicMock:
    """Create a mock per-request DataHubClient."""
    client = MagicMock()
    client._graph = MagicMock()
    return client


def _make_call_next() -> AsyncMock:
    """Create a mock call_next that captures the active client."""
    captured: dict[str, Any] = {}

    async def call_next(context: Any) -> str:
        captured["client"] = get_datahub_client()
        return "ok"

    mock = AsyncMock(side_effect=call_next)
    mock.captured = captured  # type: ignore[attr-defined]
    return mock


# ---------------------------------------------------------------------------
# Basic middleware behavior (no caching)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creates_client_from_bearer_token(mock_per_request_client: MagicMock):
    """Middleware creates a per-request DataHubClient when Bearer token is present."""
    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url="http://datahub:8080",
    )

    call_next = _make_call_next()

    with (
        patch(
            "mcp_server_datahub.__main__.get_http_headers",
            return_value={"authorization": "Bearer test-token-123"},
        ),
        patch(
            "mcp_server_datahub.__main__.DataHubClient",
            return_value=mock_per_request_client,
        ) as mock_client_cls,
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    mock_client_cls.assert_called_once()
    call_args = mock_client_cls.call_args
    assert call_args.kwargs["server"] == "http://datahub:8080"
    assert call_args.kwargs["token"] == "test-token-123"

    # Verify the per-request client was set in the contextvar
    assert call_next.captured["client"] is mock_per_request_client

    # Verify per-request client was closed
    mock_per_request_client._graph.close.assert_called_once()


@pytest.mark.asyncio
async def test_falls_back_to_startup_client(mock_fallback_client: MagicMock):
    """Middleware uses fallback client when no auth header is present."""
    middleware = _DataHubClientMiddleware(
        fallback_client=mock_fallback_client,
        gms_url="http://datahub:8080",
    )

    call_next = _make_call_next()

    with patch(
        "mcp_server_datahub.__main__.get_http_headers",
        return_value={},
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    assert call_next.captured["client"] is mock_fallback_client


@pytest.mark.asyncio
async def test_fallback_client_not_closed_after_request(
    mock_fallback_client: MagicMock,
):
    """Fallback client should NOT be closed after request (it's long-lived)."""
    middleware = _DataHubClientMiddleware(
        fallback_client=mock_fallback_client,
        gms_url="http://datahub:8080",
    )

    call_next = AsyncMock(return_value="ok")

    with patch(
        "mcp_server_datahub.__main__.get_http_headers",
        return_value={},
    ):
        await middleware.on_message({}, call_next)

    # Fallback client should NOT be closed
    mock_fallback_client._graph.close.assert_not_called()


@pytest.mark.asyncio
async def test_per_request_client_closed_on_error(mock_per_request_client: MagicMock):
    """Per-request client is closed even when the handler raises an error."""
    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url="http://datahub:8080",
    )

    async def failing_call_next(context: Any) -> Any:
        raise ValueError("tool error")

    with (
        patch(
            "mcp_server_datahub.__main__.get_http_headers",
            return_value={"authorization": "Bearer test-token"},
        ),
        patch(
            "mcp_server_datahub.__main__.DataHubClient",
            return_value=mock_per_request_client,
        ),
    ):
        with pytest.raises(ValueError, match="tool error"):
            await middleware.on_message({}, failing_call_next)

    # Even on error, per-request client should be closed
    mock_per_request_client._graph.close.assert_called_once()


@pytest.mark.asyncio
async def test_raises_when_no_client_available():
    """Middleware raises RuntimeError when no token and no fallback client."""
    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url=None,
    )

    call_next = AsyncMock()

    with patch(
        "mcp_server_datahub.__main__.get_http_headers",
        return_value={},
    ):
        with pytest.raises(RuntimeError, match="No DataHub client available"):
            await middleware.on_message({}, call_next)

    # call_next should not have been called
    call_next.assert_not_awaited()


@pytest.mark.asyncio
async def test_bearer_token_without_gms_url_uses_fallback(
    mock_fallback_client: MagicMock,
):
    """When Bearer token is present but no gms_url, falls back to fallback client."""
    middleware = _DataHubClientMiddleware(
        fallback_client=mock_fallback_client,
        gms_url=None,  # No GMS URL configured
    )

    call_next = _make_call_next()

    with patch(
        "mcp_server_datahub.__main__.get_http_headers",
        return_value={"authorization": "Bearer some-token"},
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    # Should use fallback since gms_url is None
    assert call_next.captured["client"] is mock_fallback_client


@pytest.mark.asyncio
async def test_stdio_mode_uses_fallback(mock_fallback_client: MagicMock):
    """In stdio mode (no HTTP headers), middleware uses fallback client."""
    middleware = _DataHubClientMiddleware(
        fallback_client=mock_fallback_client,
        gms_url="http://datahub:8080",
    )

    call_next = _make_call_next()

    # get_http_headers returns {} in stdio mode
    with patch(
        "mcp_server_datahub.__main__.get_http_headers",
        return_value={},
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    assert call_next.captured["client"] is mock_fallback_client


# ---------------------------------------------------------------------------
# Token validator integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bearer_token_with_validator_resolves_token(
    mock_per_request_client: MagicMock,
):
    """When token_validator is set, the token is validated and resolved before client creation."""
    mock_validator = MagicMock()
    mock_validator.validate_and_resolve.return_value = "resolved-token"

    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url="http://datahub:8080",
        token_validator=mock_validator,
    )

    call_next = _make_call_next()

    with (
        patch(
            "mcp_server_datahub.__main__.get_http_headers",
            return_value={"authorization": "Bearer original-token"},
        ),
        patch(
            "mcp_server_datahub.__main__.DataHubClient",
            return_value=mock_per_request_client,
        ) as mock_client_cls,
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    # Validator should have been called with the original token
    mock_validator.validate_and_resolve.assert_called_once_with("original-token")
    # DataHubClient should be created with the resolved token
    mock_client_cls.assert_called_once()
    assert mock_client_cls.call_args.kwargs["token"] == "resolved-token"


@pytest.mark.asyncio
async def test_bearer_token_without_validator_passes_through(
    mock_per_request_client: MagicMock,
):
    """Without token_validator, the original token is passed to DataHubClient (backwards compatible)."""
    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url="http://datahub:8080",
        token_validator=None,
    )

    call_next = _make_call_next()

    with (
        patch(
            "mcp_server_datahub.__main__.get_http_headers",
            return_value={"authorization": "Bearer raw-token-123"},
        ),
        patch(
            "mcp_server_datahub.__main__.DataHubClient",
            return_value=mock_per_request_client,
        ) as mock_client_cls,
    ):
        result = await middleware.on_message({}, call_next)

    assert result == "ok"
    # Token should be passed through unchanged
    assert mock_client_cls.call_args.kwargs["token"] == "raw-token-123"


@pytest.mark.asyncio
async def test_validator_error_propagates():
    """When token_validator raises, the error propagates (no client created)."""
    mock_validator = MagicMock()
    mock_validator.validate_and_resolve.side_effect = Exception("invalid token")

    middleware = _DataHubClientMiddleware(
        fallback_client=None,
        gms_url="http://datahub:8080",
        token_validator=mock_validator,
    )

    call_next = AsyncMock()

    with (
        patch(
            "mcp_server_datahub.__main__.get_http_headers",
            return_value={"authorization": "Bearer bad-token"},
        ),
        pytest.raises(Exception, match="invalid token"),
    ):
        await middleware.on_message({}, call_next)

    call_next.assert_not_awaited()
