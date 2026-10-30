"""Tests for OAuth login in HTTP mode."""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastmcp import FastMCP
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration, OIDCProxy

from mcp_server_datahub.__main__ import _OAuthUserClientMiddleware, create_http_app
from mcp_server_datahub.oauth import (
    DataHubSessionBroker,
    OAuthSettings,
    _GoogleProvider,
    _OIDCProvider,
    build_oauth_asgi_app,
    build_oauth_provider,
)

_ENV = {
    "MCP_OAUTH_CLIENT_ID": "id.apps.googleusercontent.com",
    "MCP_OAUTH_CLIENT_SECRET": "google-secret",
    "MCP_OAUTH_BASE_URL": "https://datahub.example.com/datahub-mcp/",
    "MCP_OAUTH_JWT_SIGNING_KEY": "signing-key",
    "DATAHUB_SYSTEM_CLIENT_SECRET": "system-secret",
}


def _settings(**overrides: Any) -> OAuthSettings:
    values: dict[str, Any] = {
        "client_id": "id.apps.googleusercontent.com",
        "client_secret": "google-secret",
        "base_url": "https://datahub.example.com/datahub-mcp",
        "jwt_signing_key": "signing-key",
        "system_client_id": "__datahub_system",
        "system_client_secret": "system-secret",
    }
    values.update(overrides)
    return OAuthSettings(**values)


def _set_env(monkeypatch: pytest.MonkeyPatch, **extra: str) -> None:
    for name, value in {**_ENV, **extra}.items():
        monkeypatch.setenv(name, value)


def test_settings_disabled_without_client_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MCP_OAUTH_CLIENT_ID", raising=False)
    assert OAuthSettings.from_env() is None


def test_settings_require_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch)
    monkeypatch.delenv("DATAHUB_SYSTEM_CLIENT_SECRET")
    with pytest.raises(RuntimeError, match="DATAHUB_SYSTEM_CLIENT_SECRET"):
        OAuthSettings.from_env()


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, MCP_OAUTH_ALLOWED_DOMAINS="Example.com, other.org")
    settings = OAuthSettings.from_env()

    assert settings is not None
    assert settings.base_url == "https://datahub.example.com/datahub-mcp"
    assert settings.mount_path == "/datahub-mcp"
    assert settings.system_client_id == "__datahub_system"
    assert settings.allowed_domains == ("example.com", "other.org")


def test_settings_from_env_for_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(
        monkeypatch,
        MCP_OAUTH_OIDC_CONFIG_URL="https://idp.example.com/.well-known/openid-configuration",
        MCP_OAUTH_SCOPES="openid,profile email",
        MCP_OAUTH_USER_CLAIM="preferred_username",
        MCP_OAUTH_REQUIRE_VERIFIED_EMAIL="false",
    )
    settings = OAuthSettings.from_env()

    assert settings is not None
    assert settings.oidc_config_url.startswith("https://idp.example.com/")
    assert settings.scopes == ("openid", "profile", "email")
    assert settings.user_claim == "preferred_username"
    assert not settings.require_verified_email


def test_allowed_domains() -> None:
    settings = _settings(allowed_domains=("example.com",))
    assert settings.is_allowed_user("jane@EXAMPLE.com")
    assert not settings.is_allowed_user("jane@evil.com")
    assert _settings().is_allowed_user("anyone@anywhere.org")


def test_username_mapping() -> None:
    assert _settings().datahub_username("jane@example.com") == "jane@example.com"
    stripped = _settings(username_regex="([^@]+)")
    assert stripped.datahub_username("jane@example.com") == "jane"


def _token(**claims: Any) -> AccessToken:
    return AccessToken(token="t", client_id="c", scopes=[], claims=claims)


def test_user_identity_requires_verified_email() -> None:
    settings = _settings()
    assert settings.user_identity(_token(email="a@b.c", email_verified=True)) == "a@b.c"
    assert settings.user_identity(_token(email="a@b.c", email_verified=False)) is None
    assert settings.user_identity(_token(email_verified=True)) is None


def test_user_identity_with_custom_claim() -> None:
    settings = _settings(
        user_claim="preferred_username",
        require_verified_email=False,
        allowed_domains=("example.com",),
    )
    assert (
        settings.user_identity(_token(preferred_username="jane@example.com"))
        == "jane@example.com"
    )
    assert settings.user_identity(_token(preferred_username="jane@evil.com")) is None
    assert settings.user_identity(_token(email="jane@example.com")) is None


def test_build_oidc_provider() -> None:
    config = OIDCConfiguration(
        strict=False,
        issuer="https://idp.example.com",
        authorization_endpoint="https://idp.example.com/authorize",
        token_endpoint="https://idp.example.com/token",
        jwks_uri="https://idp.example.com/jwks",
    )
    settings = _settings(
        oidc_config_url="https://idp.example.com/.well-known/openid-configuration"
    )
    with patch.object(OIDCProxy, "get_oidc_configuration", return_value=config):
        provider = build_oauth_provider(settings)

    assert isinstance(provider, _OIDCProvider)
    assert isinstance(build_oauth_provider(_settings()), _GoogleProvider)


@pytest.mark.anyio
async def test_provider_rejects_disallowed_domain() -> None:
    provider = build_oauth_provider(_settings(allowed_domains=("example.com",)))

    async def load(email: str) -> Any:
        upstream = AccessToken(
            token="t",
            client_id="c",
            scopes=[],
            claims={"email": email, "email_verified": True},
        )
        with patch(
            "fastmcp.server.auth.providers.google.GoogleProvider.load_access_token",
            return_value=upstream,
        ):
            return await provider.load_access_token("jwt")

    assert await load("jane@example.com") is not None
    assert await load("jane@evil.com") is None


@pytest.mark.anyio
async def test_session_broker_mints_and_caches_tokens() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"accessToken": "session-token"})

    factory = MagicMock(return_value="client")
    broker = DataHubSessionBroker(
        "http://gms:8080/", _settings(username_regex="([^@]+)"), factory
    )
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    with patch(
        "mcp_server_datahub.oauth.httpx.AsyncClient",
        lambda **kw: real_client(transport=transport, **kw),
    ):
        assert await broker.get_client("jane@example.com") == "client"
        assert await broker.get_client("jane@example.com") == "client"

    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "http://gms:8080/auth/generateSessionTokenForUser"
    assert request.headers["Authorization"] == "Basic __datahub_system:system-secret"
    assert json.loads(request.content) == {"userId": "jane"}
    factory.assert_called_once_with("http://gms:8080", "session-token")


@pytest.mark.anyio
async def test_session_broker_denied_user() -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(403))
    real_client = httpx.AsyncClient
    broker = DataHubSessionBroker("http://gms:8080", _settings(), MagicMock())

    with patch(
        "mcp_server_datahub.oauth.httpx.AsyncClient",
        lambda **kw: real_client(transport=transport, **kw),
    ):
        with pytest.raises(PermissionError, match="HTTP 403"):
            await broker.get_client("jane@example.com")


def test_create_http_app_uses_oauth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    _set_env(monkeypatch)

    with patch("mcp_server_datahub.__main__._configure_app") as configure:
        create_http_app()

    assert isinstance(configure.call_args.args[1], _OAuthUserClientMiddleware)
    assert isinstance(configure.call_args.kwargs["auth"], _GoogleProvider)


@pytest.mark.anyio
async def test_oauth_app_serves_prefixed_and_discovery_routes() -> None:
    settings = _settings()
    provider = build_oauth_provider(settings)
    server = FastMCP("oauth-test", auth=provider)
    app = build_oauth_asgi_app(server, provider, settings)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://datahub.example.com",
        ) as client:
            health = await client.get("/health")
            unauthorized = await client.post("/datahub-mcp/mcp", json={})
            as_metadata = await client.get(
                "/.well-known/oauth-authorization-server/datahub-mcp"
            )
            resource = await client.get(
                "/.well-known/oauth-protected-resource/datahub-mcp/mcp"
            )

    assert health.status_code == 200
    assert unauthorized.status_code == 401
    assert (
        "/.well-known/oauth-protected-resource/datahub-mcp/mcp"
        in unauthorized.headers["www-authenticate"]
    )
    assert as_metadata.status_code == 200
    assert (
        as_metadata.json()["authorization_endpoint"]
        == "https://datahub.example.com/datahub-mcp/authorize"
    )
    assert resource.status_code == 200
    assert resource.json()["resource"] == "https://datahub.example.com/datahub-mcp/mcp"
