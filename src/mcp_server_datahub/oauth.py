"""OAuth login for the HTTP server.

MCP clients log users in with an identity provider (Google or any OpenID Connect
provider) through FastMCP's OAuth proxy. The user's identity claim (by default
the verified email) is mapped to a DataHub username and exchanged for a DataHub
session token via GMS ``/auth/generateSessionTokenForUser`` using the DataHub
system client credentials - the same mechanism datahub-frontend uses after an
SSO login.
"""

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.parse import urlparse

import httpx
from cachetools import TTLCache
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.auth.oauth_proxy import OAuthProxy
from fastmcp.server.auth.oidc_proxy import OIDCProxy
from fastmcp.server.auth.providers.google import GoogleProvider
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

logger = logging.getLogger(__name__)

_GENERATE_SESSION_TOKEN_PATH = "/auth/generateSessionTokenForUser"
_SESSION_CACHE_TTL_SECONDS = 3600
_SESSION_CACHE_MAX_SIZE = 1024
_MCP_PATH = "/mcp"


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class OAuthSettings:
    client_id: str
    client_secret: str
    base_url: str
    jwt_signing_key: str
    system_client_id: str
    system_client_secret: str
    # Empty means Google; otherwise the IdP's OIDC discovery document URL.
    oidc_config_url: str = ""
    scopes: tuple[str, ...] = ("openid", "email")
    user_claim: str = "email"
    require_verified_email: bool = True
    allowed_domains: tuple[str, ...] = ()
    username_regex: str = "(.*)"

    @classmethod
    def from_env(cls) -> Optional["OAuthSettings"]:
        """Read settings; returns None when OAuth login is not enabled."""

        client_id = os.environ.get("MCP_OAUTH_CLIENT_ID")
        if not client_id:
            return None

        required = {
            name: os.environ.get(name, "")
            for name in (
                "MCP_OAUTH_CLIENT_SECRET",
                "MCP_OAUTH_BASE_URL",
                "MCP_OAUTH_JWT_SIGNING_KEY",
                "DATAHUB_SYSTEM_CLIENT_SECRET",
            )
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(
                f"OAuth login is enabled but {', '.join(missing)} is not set"
            )

        domains = os.environ.get("MCP_OAUTH_ALLOWED_DOMAINS", "")
        scopes = os.environ.get("MCP_OAUTH_SCOPES", "openid email")
        return cls(
            client_id=client_id,
            client_secret=required["MCP_OAUTH_CLIENT_SECRET"],
            base_url=required["MCP_OAUTH_BASE_URL"].rstrip("/"),
            jwt_signing_key=required["MCP_OAUTH_JWT_SIGNING_KEY"],
            system_client_id=os.environ.get(
                "DATAHUB_SYSTEM_CLIENT_ID", "__datahub_system"
            ),
            system_client_secret=required["DATAHUB_SYSTEM_CLIENT_SECRET"],
            oidc_config_url=os.environ.get("MCP_OAUTH_OIDC_CONFIG_URL", ""),
            scopes=tuple(scopes.replace(",", " ").split()),
            user_claim=os.environ.get("MCP_OAUTH_USER_CLAIM", "email"),
            require_verified_email=_env_flag("MCP_OAUTH_REQUIRE_VERIFIED_EMAIL", True),
            allowed_domains=tuple(
                d.strip().lower() for d in domains.split(",") if d.strip()
            ),
            username_regex=os.environ.get("MCP_OAUTH_USERNAME_REGEX", "(.*)"),
        )

    @property
    def mount_path(self) -> str:
        """Path prefix of the public base URL, e.g. ``/datahub-mcp``."""
        return urlparse(self.base_url).path.rstrip("/")

    def is_allowed_user(self, user: str) -> bool:
        if not self.allowed_domains:
            return True
        return user.rsplit("@", 1)[-1].lower() in self.allowed_domains

    def user_identity(self, access_token: AccessToken) -> Optional[str]:
        """Return the allowed user identity claim of a token, or None."""
        claims = access_token.claims or {}
        user = claims.get(self.user_claim)
        if not user:
            return None
        if self.require_verified_email and claims.get("email_verified") not in (
            True,
            "true",
        ):
            return None
        if not self.is_allowed_user(str(user)):
            return None
        return str(user)

    def datahub_username(self, user: str) -> str:
        """Map a user identity to a DataHub username like AUTH_OIDC_USER_NAME_CLAIM_REGEX."""
        match = re.search(self.username_regex, user)
        if match is None:
            raise ValueError("User does not match MCP_OAUTH_USERNAME_REGEX")
        return match.group(1) if match.groups() else match.group(0)


class _UserRestrictedProvider(OAuthProxy):
    """Only accept tokens whose user identity passes the settings' checks."""

    _settings: OAuthSettings

    async def load_access_token(self, token: str) -> Optional[AccessToken]:  # type: ignore[override]
        access_token = await super().load_access_token(token)
        if not isinstance(access_token, AccessToken):
            return None
        if self._settings.user_identity(access_token) is None:
            logger.warning("Rejected OAuth login without an allowed user identity")
            return None
        return access_token


class _GoogleProvider(_UserRestrictedProvider, GoogleProvider):
    pass


class _OIDCProvider(_UserRestrictedProvider, OIDCProxy):
    pass


def build_oauth_provider(settings: OAuthSettings) -> OAuthProxy:
    common: dict = dict(
        client_id=settings.client_id,
        client_secret=settings.client_secret,
        base_url=settings.base_url,
        jwt_signing_key=settings.jwt_signing_key,
        required_scopes=list(settings.scopes),
    )
    provider: _UserRestrictedProvider
    if settings.oidc_config_url:
        # ID tokens carry the identity claims; access tokens often do not.
        provider = _OIDCProvider(
            config_url=settings.oidc_config_url, verify_id_token=True, **common
        )
    else:
        provider = _GoogleProvider(**common)
    provider._settings = settings
    return provider


class DataHubSessionBroker:
    """Create and cache per-user DataHub clients backed by GMS session tokens."""

    def __init__(
        self,
        gms_url: str,
        settings: OAuthSettings,
        client_factory: Callable[[str, str], DataHubClient],
    ) -> None:
        self._gms_url = gms_url.rstrip("/")
        self.settings = settings
        self._client_factory = client_factory
        self._clients: TTLCache[str, DataHubClient] = TTLCache(
            maxsize=_SESSION_CACHE_MAX_SIZE, ttl=_SESSION_CACHE_TTL_SECONDS
        )
        self._lock = asyncio.Lock()

    async def get_client(self, user: str) -> DataHubClient:
        username = self.settings.datahub_username(user)
        client = self._clients.get(username)
        if client is not None:
            return client

        async with self._lock:
            client = self._clients.get(username)
            if client is None:
                token = await self._generate_session_token(username)
                client = self._client_factory(self._gms_url, token)
                self._clients[username] = client
            return client

    def invalidate(self, user: str) -> None:
        self._clients.pop(self.settings.datahub_username(user), None)

    async def _generate_session_token(self, username: str) -> str:
        # DataHubSystemAuthenticator expects "Basic <id>:<secret>" (not base64).
        authorization = (
            f"Basic {self.settings.system_client_id}:"
            f"{self.settings.system_client_secret}"
        )
        async with httpx.AsyncClient(timeout=10.0) as http:
            response = await http.post(
                f"{self._gms_url}{_GENERATE_SESSION_TOKEN_PATH}",
                json={"userId": username},
                headers={"Authorization": authorization},
            )
        if response.status_code != 200:
            raise PermissionError(
                f"DataHub refused a session for user {username!r} "
                f"(HTTP {response.status_code})"
            )
        token = response.json().get("accessToken")
        if not token:
            raise RuntimeError("DataHub returned no session token")
        logger.info("Created DataHub session for user %s", username)
        return token


async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def build_oauth_asgi_app(
    server: FastMCP, provider: OAuthProxy, settings: OAuthSettings
) -> Starlette:
    """Serve the MCP app below the base URL path, e.g. ``/datahub-mcp/mcp``.

    OAuth discovery documents (RFC 8414 / RFC 9728) must live at the host root,
    so they are mounted there in addition to the prefixed app. ``/health`` stays
    at the root too for the container health check.
    """

    mcp_app = server.http_app(path=_MCP_PATH, stateless_http=True)
    mount_path = settings.mount_path
    if not mount_path:
        return mcp_app

    return Starlette(
        routes=[
            *provider.get_well_known_routes(_MCP_PATH),
            Route("/health", _health),
            Mount(mount_path, app=mcp_app),
        ],
        lifespan=mcp_app.lifespan,
    )
