"""Entra ID On-Behalf-Of (OBO) authentication for HTTP transport.

When the MCP server runs behind an AI client (GitHub Copilot, Copilot Studio)
that authenticates users via Microsoft Entra ID, this module:

1. Validates incoming Entra ID JWTs (signature, issuer, audience, expiry)
2. Exchanges the validated token for a DataHub-scoped token via OBO
3. Stores the exchanged token in ``claims["datahub_token"]`` so that
   :class:`PerUserClientMiddleware` creates per-user DataHub clients

Environment Variables:
    AZURE_TENANT_ID:
        Entra ID tenant (GUID, "organizations", or "consumers").

    MCP_OAUTH_CLIENT_ID:
        App registration client ID for this MCP server.

    MCP_OAUTH_CLIENT_SECRET:
        App registration client secret (use Managed Identity in production).

    DATAHUB_OAUTH_SCOPE:
        Target scope for OBO exchange, e.g. ``api://<datahub-app-id>/.default``.

    MCP_SERVER_BASE_URL:
        Optional.  Public URL of this MCP server (e.g.
        ``https://mcp.example.com``).  When set, the server exposes
        ``.well-known/oauth-protected-resource`` metadata so that MCP
        clients like GitHub Copilot can auto-discover the Entra ID
        authorization server.

    MCP_OAUTH_REQUIRED_SCOPES:
        Optional.  Comma-separated list of Entra scopes required on incoming
        tokens (e.g. ``access_as_user``).  When omitted, any valid Entra JWT
        for the correct audience is accepted.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from loguru import logger

from fastmcp.server.auth.auth import AccessToken, RemoteAuthProvider, TokenVerifier
from fastmcp.server.auth.providers.azure import AzureJWTVerifier

# ---------------------------------------------------------------------------
# Environment-variable helpers
# ---------------------------------------------------------------------------

_AZURE_TENANT_ID = "AZURE_TENANT_ID"
_MCP_OAUTH_CLIENT_ID = "MCP_OAUTH_CLIENT_ID"
_MCP_OAUTH_CLIENT_SECRET = "MCP_OAUTH_CLIENT_SECRET"
_DATAHUB_OAUTH_SCOPE = "DATAHUB_OAUTH_SCOPE"
_MCP_SERVER_BASE_URL = "MCP_SERVER_BASE_URL"
_MCP_OAUTH_REQUIRED_SCOPES = "MCP_OAUTH_REQUIRED_SCOPES"


class OBOConfig:
    """Validated OBO configuration from environment variables."""

    __slots__ = (
        "tenant_id",
        "client_id",
        "client_secret",
        "datahub_scope",
        "base_url",
        "required_scopes",
    )

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        datahub_scope: str,
        base_url: str | None = None,
        required_scopes: list[str] | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.datahub_scope = datahub_scope
        self.base_url = base_url
        self.required_scopes = required_scopes


def get_obo_config_from_env() -> OBOConfig | None:
    """Read and validate OBO configuration from environment variables.

    Returns ``None`` if any of the four required variables are missing,
    allowing the server to fall back to PAT-only authentication.
    """
    tenant_id = os.environ.get(_AZURE_TENANT_ID, "")
    client_id = os.environ.get(_MCP_OAUTH_CLIENT_ID, "")
    client_secret = os.environ.get(_MCP_OAUTH_CLIENT_SECRET, "")
    datahub_scope = os.environ.get(_DATAHUB_OAUTH_SCOPE, "")

    if not all([tenant_id, client_id, client_secret, datahub_scope]):
        return None

    base_url = os.environ.get(_MCP_SERVER_BASE_URL, "") or None

    raw_scopes = os.environ.get(_MCP_OAUTH_REQUIRED_SCOPES, "")
    required_scopes = (
        [s.strip() for s in raw_scopes.split(",") if s.strip()] if raw_scopes else None
    )

    return OBOConfig(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        datahub_scope=datahub_scope,
        base_url=base_url,
        required_scopes=required_scopes,
    )


# ---------------------------------------------------------------------------
# OBO Token Exchange
# ---------------------------------------------------------------------------


class OBOTokenExchanger:
    """Exchange an Entra ID user-assertion for a DataHub-scoped token via MSAL.

    The underlying :class:`msal.ConfidentialClientApplication` maintains an
    in-memory token cache, so repeated calls for the same user and scope are
    served from cache until the token expires.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        datahub_scope: str,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = [datahub_scope]
        self._app: Any = None  # created lazily on first exchange

    def _get_app(self) -> Any:
        """Return (and lazily create) the MSAL ConfidentialClientApplication."""
        if self._app is None:
            import msal

            authority = f"https://login.microsoftonline.com/{self._tenant_id}"
            self._app = msal.ConfidentialClientApplication(
                client_id=self._client_id,
                client_credential=self._client_secret,
                authority=authority,
            )
        return self._app

    def exchange(self, user_assertion: str) -> str:
        """Exchange *user_assertion* (incoming Entra JWT) for a DataHub token.

        Raises :class:`RuntimeError` if the exchange fails (e.g. consent not
        granted, invalid assertion, scope not configured).
        """
        result: dict[str, Any] = self._get_app().acquire_token_on_behalf_of(
            user_assertion=user_assertion,
            scopes=self._scopes,
        )

        if "access_token" in result:
            return result["access_token"]

        error = result.get("error", "unknown_error")
        description = result.get("error_description", "no description")
        raise RuntimeError(f"OBO token exchange failed: {error} — {description}")


# ---------------------------------------------------------------------------
# Entra OBO Token Verifier
# ---------------------------------------------------------------------------


class EntraOBOVerifier(TokenVerifier):
    """Validate Entra ID JWTs and exchange them for DataHub tokens via OBO.

    Combines :class:`AzureJWTVerifier` (JWT signature / claims validation)
    with :class:`OBOTokenExchanger` (MSAL OBO flow).  The exchanged DataHub
    token is stored in ``claims["datahub_token"]`` so that the existing
    :class:`PerUserClientMiddleware` can create a per-user ``DataHubClient``
    without any modification.
    """

    def __init__(self, config: OBOConfig) -> None:
        super().__init__(
            base_url=config.base_url,
            required_scopes=config.required_scopes,
        )

        self._jwt_verifier = AzureJWTVerifier(
            client_id=config.client_id,
            tenant_id=config.tenant_id,
            required_scopes=config.required_scopes,
        )

        self._exchanger = OBOTokenExchanger(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
            datahub_scope=config.datahub_scope,
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        """Validate *token* as an Entra ID JWT and exchange it via OBO.

        Returns an :class:`AccessToken` with ``claims["datahub_token"]`` set
        to the exchanged DataHub token on success, or ``None`` on failure.
        """
        # Step 1: Validate the JWT (signature, issuer, audience, expiry, scopes)
        access_token = await self._jwt_verifier.verify_token(token)
        if access_token is None:
            return None

        # Step 2: Exchange via OBO (blocking MSAL call → run in thread)
        try:
            datahub_token = await asyncio.to_thread(self._exchanger.exchange, token)
        except RuntimeError:
            logger.warning("OBO token exchange failed", exc_info=True)
            return None

        # Step 3: Return AccessToken with the exchanged token in claims.
        # We preserve the original Entra claims and add our custom ones.
        claims = dict(access_token.claims)
        claims["datahub_token"] = datahub_token
        claims["auth_method"] = "entra_obo"

        # Map Entra claim names to what PerUserClientMiddleware expects
        if "sub" not in claims and "oid" in claims:
            claims["sub"] = claims["oid"]
        if "username" not in claims:
            claims["username"] = (
                claims.get("preferred_username")
                or claims.get("upn")
                or claims.get("sub", "unknown")
            )

        logger.debug(
            "Verified Entra token and exchanged via OBO for user: {} ({})",
            claims.get("username"),
            claims.get("sub"),
        )

        return AccessToken(
            token=datahub_token,  # downstream code sees the exchanged token
            client_id=access_token.client_id,
            scopes=access_token.scopes,
            expires_at=access_token.expires_at,
            claims=claims,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_obo_auth(
    config: OBOConfig,
) -> RemoteAuthProvider | EntraOBOVerifier:
    """Build an auth provider for Entra ID OBO.

    When ``config.base_url`` is set, returns a :class:`RemoteAuthProvider`
    that exposes ``.well-known/oauth-protected-resource`` metadata (needed
    for clients like GitHub Copilot that auto-discover auth).

    Otherwise returns the bare :class:`EntraOBOVerifier` (sufficient when
    the client is configured manually, e.g. Copilot Studio).
    """
    from pydantic import AnyHttpUrl

    verifier = EntraOBOVerifier(config)

    if config.base_url:
        auth_server_url = AnyHttpUrl(
            f"https://login.microsoftonline.com/{config.tenant_id}/v2.0"
        )
        return RemoteAuthProvider(
            token_verifier=verifier,
            authorization_servers=[auth_server_url],
            base_url=config.base_url,
            resource_name="DataHub MCP Server",
        )

    return verifier
