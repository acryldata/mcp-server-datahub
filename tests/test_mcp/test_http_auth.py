"""Tests for HTTP bearer authentication and legacy local configuration."""

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from click.testing import CliRunner
from datahub.ingestion.graph.config import ClientMode
from fastmcp import FastMCP
from fastmcp.server.auth.auth import AccessToken

from mcp_server_datahub.__main__ import (
    _AuthenticatedDataHubClientMiddleware,
    _DataHubClientMiddleware,
    _DataHubTokenVerifier,
    create_http_app,
    create_legacy_app,
    http_main,
    main,
)
from mcp_server_datahub._version import __version__
from mcp_server_datahub.mcp_server import mcp


@pytest.mark.anyio
async def test_token_verifier_caches_verified_client() -> None:
    verifier = _DataHubTokenVerifier("https://datahub.example")
    client = MagicMock()

    with patch(
        "mcp_server_datahub.__main__._build_and_verify_http_client",
        return_value=client,
    ) as build_client:
        first = await verifier.verify_token("valid-token")
        second = await verifier.verify_token("valid-token")

    assert first is not None
    assert second is not None
    assert first.token == "valid-token"
    build_client.assert_called_once_with("https://datahub.example", "valid-token")


@pytest.mark.anyio
async def test_token_verifier_rejects_invalid_token() -> None:
    verifier = _DataHubTokenVerifier("https://datahub.example")

    with patch(
        "mcp_server_datahub.__main__._build_and_verify_http_client",
        side_effect=RuntimeError("401 Unauthorized"),
    ):
        assert await verifier.verify_token("invalid-token") is None


@pytest.mark.anyio
async def test_token_verifier_does_not_cache_failures() -> None:
    verifier = _DataHubTokenVerifier("https://datahub.example")

    with patch(
        "mcp_server_datahub.__main__._build_and_verify_http_client",
        side_effect=RuntimeError("401 Unauthorized"),
    ) as build_client:
        assert await verifier.verify_token("invalid-token") is None
        assert await verifier.verify_token("invalid-token") is None

    assert build_client.call_count == 2


@pytest.mark.anyio
async def test_token_verifier_bounds_concurrent_validation() -> None:
    verifier = _DataHubTokenVerifier(
        "https://datahub.example",
        max_concurrent_validations=2,
    )
    release = asyncio.Event()
    both_started = asyncio.Event()
    active = 0
    peak = 0

    async def fake_to_thread(function: Any, *args: Any) -> MagicMock:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 2:
            both_started.set()
        await release.wait()
        active -= 1
        return MagicMock()

    with patch("mcp_server_datahub.__main__.asyncio.to_thread", new=fake_to_thread):
        tasks = [
            asyncio.create_task(verifier.verify_token(f"token-{index}"))
            for index in range(4)
        ]
        await asyncio.wait_for(both_started.wait(), timeout=1)
        assert peak == 2
        release.set()
        assert all(result is not None for result in await asyncio.gather(*tasks))

    assert peak == 2


@pytest.mark.anyio
async def test_authenticated_middleware_uses_verified_client() -> None:
    verifier = _DataHubTokenVerifier("https://datahub.example")
    client = MagicMock()
    verifier.get_client = AsyncMock(return_value=client)  # type: ignore[method-assign]
    middleware = _AuthenticatedDataHubClientMiddleware(verifier)
    call_next = AsyncMock(return_value="response")
    access_token = AccessToken(
        token="valid-token",
        client_id="test-client",
        scopes=[],
    )

    with patch(
        "mcp_server_datahub.__main__.get_access_token",
        return_value=access_token,
    ):
        result = await middleware.on_message(MagicMock(), call_next)

    assert result == "response"
    verifier.get_client.assert_awaited_once_with("valid-token")  # type: ignore[attr-defined]
    call_next.assert_awaited_once()


@pytest.mark.anyio
async def test_authenticated_middleware_never_falls_back_without_token() -> None:
    verifier = _DataHubTokenVerifier("https://datahub.example")
    verifier.get_client = AsyncMock()  # type: ignore[method-assign]
    middleware = _AuthenticatedDataHubClientMiddleware(verifier)
    call_next = AsyncMock()

    with (
        patch("mcp_server_datahub.__main__.get_access_token", return_value=None),
        pytest.raises(RuntimeError, match="no access token"),
    ):
        await middleware.on_message(MagicMock(), call_next)

    verifier.get_client.assert_not_awaited()  # type: ignore[attr-defined]
    call_next.assert_not_awaited()


def test_create_legacy_app_preserves_from_env_configuration() -> None:
    client = MagicMock()

    with patch(
        "mcp_server_datahub.__main__.DataHubClient.from_env",
        return_value=client,
    ) as from_env:
        with patch("mcp_server_datahub.__main__._configure_app") as configure:
            create_legacy_app()

    from_env.assert_called_once_with(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )
    assert configure.call_args.args[0] == "legacy"
    middleware = configure.call_args.args[1]
    assert isinstance(middleware, _DataHubClientMiddleware)
    assert middleware._client is client


def test_create_http_app_requires_server_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="DATAHUB_GMS_URL"):
        create_http_app()


def test_create_http_app_rejects_shared_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://datahub.example")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "shared-token")

    with pytest.raises(RuntimeError, match="not allowed in HTTP mode"):
        create_http_app()


def test_create_http_app_installs_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://datahub.example")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)

    with (
        patch("mcp_server_datahub.__main__._configure_app") as configure,
    ):
        create_http_app()

    assert configure.call_args.args[0] == "http"
    middleware = configure.call_args.args[1]
    verifier = configure.call_args.kwargs["auth"]
    assert isinstance(verifier, _DataHubTokenVerifier)
    assert isinstance(middleware, _AuthenticatedDataHubClientMiddleware)
    assert middleware._token_verifier is verifier


def test_legacy_cli_does_not_offer_http_transport() -> None:
    result = CliRunner().invoke(main, ["--transport", "http"])

    assert result.exit_code == 2
    assert "Invalid value for '--transport'" in result.output


def test_http_cli_uses_only_http_factory() -> None:
    app = MagicMock()

    with patch(
        "mcp_server_datahub.__main__.create_http_app",
        return_value=app,
    ) as create:
        result = CliRunner().invoke(http_main)

    assert result.exit_code == 0
    create.assert_called_once_with()
    app.run.assert_called_once_with(
        transport="http",
        show_banner=False,
        stateless_http=True,
    )


class _StaticTokenVerifier(_DataHubTokenVerifier):
    def __init__(self, valid_token: str) -> None:
        super().__init__("https://datahub.example")
        self._valid_token = valid_token

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        if token != self._valid_token:
            return None
        return AccessToken(token=token, client_id="test-client", scopes=[])

    async def get_client(self, token: str) -> Any:
        return MagicMock()


def _make_authenticated_http_app(valid_token: str):  # type: ignore[no-untyped-def]
    verifier = _StaticTokenVerifier(valid_token)
    server = FastMCP(
        "auth-test",
        auth=verifier,
        middleware=[_AuthenticatedDataHubClientMiddleware(verifier)],
    )
    return server.http_app(stateless_http=True)


_INITIALIZE_REQUEST = {
    "jsonrpc": "2.0",
    "method": "initialize",
    "id": 1,
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "auth-test", "version": "1"},
    },
}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("headers", "path"),
    [
        ({}, "/mcp"),
        ({"Authorization": "Bearer invalid-token"}, "/mcp"),
        ({}, "/mcp?access_token=valid-token"),
        ({}, "/mcp?token=valid-token"),
    ],
)
async def test_http_rejects_missing_invalid_or_query_token(
    headers: dict[str, str],
    path: str,
) -> None:
    app = _make_authenticated_http_app("valid-token")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
        follow_redirects=True,
    ) as client:
        response = await client.post(path, json=_INITIALIZE_REQUEST, headers=headers)

    assert response.status_code == 401


@pytest.mark.anyio
async def test_http_accepts_valid_token() -> None:
    app = _make_authenticated_http_app("valid-token")
    async with app.lifespan(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            follow_redirects=True,
        ) as client:
            response = await client.post(
                "/mcp",
                json=_INITIALIZE_REQUEST,
                headers={"Authorization": "Bearer valid-token"},
            )

    assert response.status_code != 401


@pytest.mark.anyio
async def test_production_http_app_keeps_health_public_and_mcp_private(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server_datahub.__main__ as main_module

    original_mode = main_module._app_mode
    original_auth = mcp.auth
    original_middleware = list(mcp.middleware)
    main_module._app_mode = None
    mcp.auth = None
    mcp.middleware.clear()
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://datahub.example")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)

    try:
        app = create_http_app().http_app(stateless_http=True)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            follow_redirects=True,
        ) as client:
            health_response = await client.get("/health")
            mcp_response = await client.post("/mcp", json=_INITIALIZE_REQUEST)

        assert health_response.status_code == 200
        assert health_response.json() == {"status": "ok"}
        assert mcp_response.status_code == 401
    finally:
        main_module._app_mode = original_mode
        mcp.auth = original_auth
        mcp.middleware.clear()
        mcp.middleware.extend(original_middleware)
