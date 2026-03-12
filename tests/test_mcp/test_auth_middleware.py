"""Unit tests for per-request auth token extraction and _DataHubClientMiddleware."""

from typing import Any, Optional
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastmcp.server.auth import TokenVerifier
from fastmcp.server.auth.auth import AccessToken

import mcp_server_datahub.__main__  # noqa: F401 -- registers /health route on mcp
from mcp_server_datahub.__main__ import (
    _DataHubClientMiddleware,
    _DataHubTokenVerifier,
    _token_from_request,
    create_app,
)
from mcp_server_datahub.mcp_server import mcp


# ---------------------------------------------------------------------------
# _token_from_request
# ---------------------------------------------------------------------------


def _make_mock_request(headers: dict, query_params: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {k.lower(): v for k, v in headers.items()}
    req.query_params = query_params or {}
    return req


def test_token_from_bearer_header() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({"Authorization": "Bearer mytoken123"}),
    ):
        assert _token_from_request() == "mytoken123"


def test_no_token_returns_none() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({}),
    ):
        assert _token_from_request() is None


def test_no_http_request_returns_none() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        side_effect=RuntimeError("No active HTTP request found."),
    ):
        assert _token_from_request() is None


# ---------------------------------------------------------------------------
# _DataHubClientMiddleware._client_for_request
# ---------------------------------------------------------------------------


def test_per_request_token_returns_new_client() -> None:
    middleware = _DataHubClientMiddleware("http://datahub:8080", use_global_client=True)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value="per-request"
    ):
        with patch("mcp_server_datahub.__main__._build_client") as mock_build:
            client = middleware._client_for_request()
            mock_build.assert_called_once_with("http://datahub:8080", "per-request")
            assert client is mock_build.return_value


def test_falls_back_to_global_client_when_no_request_token() -> None:
    middleware = _DataHubClientMiddleware("http://datahub:8080", use_global_client=True)
    with patch("mcp_server_datahub.__main__._token_from_request", return_value=None):
        with patch(
            "mcp_server_datahub.__main__._build_global_client"
        ) as mock_build_global:
            result = middleware._client_for_request()
            mock_build_global.assert_called_once()
            assert result is mock_build_global.return_value


def test_raises_when_no_token_and_no_default_client() -> None:
    middleware = _DataHubClientMiddleware(
        "http://datahub:8080", use_global_client=False
    )
    with patch("mcp_server_datahub.__main__._token_from_request", return_value=None):
        with pytest.raises(ValueError, match="No DataHub token provided"):
            middleware._client_for_request()


def test_per_request_token_does_not_use_global_client() -> None:
    middleware = _DataHubClientMiddleware("http://datahub:8080", use_global_client=True)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value="req-token"
    ):
        with patch("mcp_server_datahub.__main__._build_client") as mock_build:
            with patch(
                "mcp_server_datahub.__main__._build_global_client"
            ) as mock_build_global:
                result = middleware._client_for_request()
                mock_build.assert_called_once_with("http://datahub:8080", "req-token")
                mock_build_global.assert_not_called()
                assert result is mock_build.return_value


# ---------------------------------------------------------------------------
# _DataHubTokenVerifier
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_token_verifier_returns_access_token_on_valid_token() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch("mcp_server_datahub.__main__._build_client") as mock_build:
        with patch("mcp_server_datahub.__main__._verify_client"):
            result = await verifier.verify_token("good-token")
    assert result is not None
    assert result.token == "good-token"
    mock_build.assert_called_once_with("http://datahub:8080", "good-token")


@pytest.mark.anyio
async def test_token_verifier_returns_none_on_invalid_token() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch(
        "mcp_server_datahub.__main__._verify_client",
        side_effect=Exception("401 Unauthorized"),
    ):
        with patch("mcp_server_datahub.__main__._build_client"):
            result = await verifier.verify_token("bad-token")
    assert result is None


@pytest.mark.anyio
async def test_token_verifier_returns_none_on_any_exception() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch(
        "mcp_server_datahub.__main__._verify_client",
        side_effect=RuntimeError("connection refused"),
    ):
        with patch("mcp_server_datahub.__main__._build_client"):
            result = await verifier.verify_token("some-token")
    assert result is None


# ---------------------------------------------------------------------------
# create_app — startup behaviour
# ---------------------------------------------------------------------------


def test_create_app_requires_datahub_gms_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with pytest.raises(RuntimeError, match="DATAHUB_GMS_URL"):
            create_app()
    finally:
        main_mod._app_initialized = original


def test_create_app_no_token_builds_no_default_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with patch("mcp_server_datahub.__main__._DataHubClientMiddleware") as MockMW:
            with patch.object(mcp, "add_middleware"):
                create_app()
            kwargs = MockMW.call_args.kwargs if MockMW.call_args else {}
            assert kwargs.get("use_global_client") is False
    finally:
        main_mod._app_initialized = original


def test_create_app_with_token_passes_use_global_client_to_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "globaltoken")
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with patch("mcp_server_datahub.__main__._build_global_client"):
            with patch("mcp_server_datahub.__main__._verify_client"):
                with patch(
                    "mcp_server_datahub.__main__._DataHubClientMiddleware"
                ) as MockMW:
                    with patch.object(mcp, "add_middleware"):
                        create_app()
            args = MockMW.call_args.args
            kwargs = MockMW.call_args.kwargs
            assert args[0] == "http://datahub:8080"
            assert kwargs.get("use_global_client") is True
    finally:
        main_mod._app_initialized = original


# ---------------------------------------------------------------------------
# HTTP auth smoke tests — verify the Bearer-token gating works end-to-end
# ---------------------------------------------------------------------------


class _StaticTokenVerifier(TokenVerifier):
    """Test verifier that accepts only a fixed token."""

    def __init__(self, valid_token: str) -> None:
        super().__init__()
        self._valid_token = valid_token

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        if token == self._valid_token:
            return AccessToken(client_id="test-client", scopes=[], token=token)
        return None


def _make_http_app(verifier: TokenVerifier):  # type: ignore[return]
    """Return the FastMCP HTTP ASGI app with *verifier* installed."""
    from fastmcp import FastMCP

    test_mcp = FastMCP("smoke-test")
    test_mcp.auth = verifier
    return test_mcp.http_app(transport="streamable-http", stateless_http=True)


_MCP_INIT_PAYLOAD = {
    "jsonrpc": "2.0",
    "method": "initialize",
    "id": 1,
    "params": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "smoke-test", "version": "1"},
    },
}


@pytest.mark.anyio
async def test_http_auth_missing_token_returns_401() -> None:
    """MCP HTTP endpoint must reject requests that carry no Authorization header."""
    app = _make_http_app(_StaticTokenVerifier("secret"))
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=True
    ) as client:
        resp = await client.post("/mcp/", json=_MCP_INIT_PAYLOAD)
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_http_auth_invalid_token_returns_401() -> None:
    """MCP HTTP endpoint must reject requests that carry an invalid Bearer token."""
    app = _make_http_app(_StaticTokenVerifier("secret"))
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=True
    ) as client:
        resp = await client.post(
            "/mcp/",
            json=_MCP_INIT_PAYLOAD,
            headers={"Authorization": "Bearer wrong-token"},
        )
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_http_auth_valid_token_is_not_rejected() -> None:
    """MCP HTTP endpoint must not return 401 when a valid Bearer token is supplied."""
    app = _make_http_app(_StaticTokenVerifier("secret"))
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with app.lifespan(app):
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", follow_redirects=True
        ) as client:
            resp = await client.post(
                "/mcp/",
                json=_MCP_INIT_PAYLOAD,
                headers={"Authorization": "Bearer secret"},
            )
    assert resp.status_code != 401


@pytest.mark.anyio
async def test_main_http_mode_installs_token_verifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() with transport=http and no DATAHUB_GMS_TOKEN must set mcp.auth."""
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    original_initialized = main_mod._app_initialized
    original_auth = mcp.auth
    main_mod._app_initialized = False
    try:
        with patch("mcp_server_datahub.__main__._verify_client"):
            with patch("mcp_server_datahub.__main__._build_client"):
                with patch.object(mcp, "add_middleware"):
                    with patch.object(mcp, "run"):
                        from click.testing import CliRunner

                        runner = CliRunner()
                        runner.invoke(main_mod.main, ["--transport", "http"])
        assert isinstance(mcp.auth, _DataHubTokenVerifier)
    finally:
        main_mod._app_initialized = original_initialized
        mcp.auth = original_auth  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Smoke tests — token passed as Authorization header reaches DataHub client
# ---------------------------------------------------------------------------


def _make_real_http_app(monkeypatch: pytest.MonkeyPatch, valid_token: str):
    """
    Build the real MCP HTTP ASGI app using create_app() with auth wired up.

    The DataHub network calls are patched out:
    - _build_client returns a MagicMock so no real HTTP is made.
    - _verify_client is a no-op.
    - _DataHubTokenVerifier.verify_token is patched to accept only *valid_token*.
    """
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)

    # Reset so create_app() actually runs.
    main_mod._app_initialized = False

    mock_client = MagicMock()

    async def _patched_verify_token(self: Any, token: str) -> Optional[AccessToken]:
        if token == valid_token:
            return AccessToken(client_id="smoke-test", scopes=[], token=token)
        return None

    with patch("mcp_server_datahub.__main__._build_client", return_value=mock_client):
        with patch("mcp_server_datahub.__main__._verify_client"):
            with patch.object(
                _DataHubTokenVerifier, "verify_token", _patched_verify_token
            ):
                create_app()
                mcp.auth = _DataHubTokenVerifier("http://datahub:8080")
                # Patch verify_token on the installed instance too.
                mcp.auth.verify_token = lambda token: _patched_verify_token(  # type: ignore[method-assign]
                    mcp.auth, token
                )
                app = mcp.http_app(transport="streamable-http", stateless_http=True)

    # Re-patch _build_client for the lifetime of the app so per-request client
    # construction also never makes real network calls.
    monkeypatch.setattr(main_mod, "_build_client", lambda *_: mock_client)
    return app


@pytest.mark.anyio
async def test_smoke_missing_token_returns_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real app must reject requests with no Authorization header with 401."""
    import mcp_server_datahub.__main__ as main_mod

    original_initialized = main_mod._app_initialized
    original_auth = mcp.auth
    try:
        app = _make_real_http_app(monkeypatch, valid_token="real-secret")
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", follow_redirects=True
        ) as client:
            resp = await client.post("/mcp/", json=_MCP_INIT_PAYLOAD)
        assert resp.status_code == 401
    finally:
        main_mod._app_initialized = original_initialized
        mcp.auth = original_auth  # type: ignore[assignment]


@pytest.mark.anyio
async def test_smoke_invalid_token_returns_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real app must reject requests with a wrong Bearer token with 401."""
    import mcp_server_datahub.__main__ as main_mod

    original_initialized = main_mod._app_initialized
    original_auth = mcp.auth
    try:
        app = _make_real_http_app(monkeypatch, valid_token="real-secret")
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", follow_redirects=True
        ) as client:
            resp = await client.post(
                "/mcp/",
                json=_MCP_INIT_PAYLOAD,
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert resp.status_code == 401
    finally:
        main_mod._app_initialized = original_initialized
        mcp.auth = original_auth  # type: ignore[assignment]


@pytest.mark.anyio
async def test_smoke_valid_token_is_not_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real app must not return 401 when the correct Bearer token is supplied."""
    import mcp_server_datahub.__main__ as main_mod

    original_initialized = main_mod._app_initialized
    original_auth = mcp.auth
    try:
        app = _make_real_http_app(monkeypatch, valid_token="real-secret")
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with app.lifespan(app):
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", follow_redirects=True
            ) as client:
                resp = await client.post(
                    "/mcp/",
                    json=_MCP_INIT_PAYLOAD,
                    headers={"Authorization": "Bearer real-secret"},
                )
        assert resp.status_code != 401
    finally:
        main_mod._app_initialized = original_initialized
        mcp.auth = original_auth  # type: ignore[assignment]
