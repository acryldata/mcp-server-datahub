"""OAuth / token auth passthrough for HTTP transport.

When the MCP server runs over HTTP, incoming requests can carry a DataHub
Personal Access Token (PAT) as a Bearer token.  This module:

1. Verifies the token against the DataHub backend (``/me`` query)
2. Creates a per-request ``DataHubClient`` so mutations are attributed to the
   calling user rather than a shared service account

Environment Variables:
    DATAHUB_MCP_AUTH_ENABLED:
        Set to ``"true"`` to enable token verification on HTTP transport.
        Default: ``false`` (all requests use the service-account client
        created from ``DATAHUB_GMS_URL`` / ``DATAHUB_GMS_TOKEN``).

    DATAHUB_GMS_URL:
        Required when auth is enabled — the DataHub GMS backend used both
        for token verification and for creating per-user clients.

Design notes:
    * STDIO transport is **never** affected — it always uses the env-var
      client, since STDIO is a local subprocess with no HTTP headers.
    * When auth is disabled, HTTP transport also uses the env-var client
      (backwards compatible).
"""

import os
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from fastmcp.server.auth import AccessToken, TokenVerifier
from fastmcp.server.middleware import CallNext, Middleware
from loguru import logger

from ._version import __version__
from .mcp_server import with_datahub_client

# Environment variable to enable auth
DATAHUB_MCP_AUTH_ENABLED_ENV_VAR = "DATAHUB_MCP_AUTH_ENABLED"


def is_auth_enabled() -> bool:
    """Check if token auth is enabled via environment variable."""
    return os.environ.get(DATAHUB_MCP_AUTH_ENABLED_ENV_VAR, "").lower() == "true"


class DataHubTokenVerifier(TokenVerifier):
    """Verify DataHub PATs by querying the ``me`` endpoint on the GMS backend.

    Each valid token is returned as an ``AccessToken`` with the DataHub
    user's corpuser URN stored in ``claims["sub"]`` and the raw token in
    ``claims["datahub_token"]``.
    """

    def __init__(self, gms_url: str) -> None:
        super().__init__()
        self._gms_url = gms_url.rstrip("/")

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        """Verify *token* by issuing a lightweight ``me`` query to DataHub.

        Returns an ``AccessToken`` on success, or ``None`` if the token is
        invalid / expired.
        """
        import asyncio

        # Run the blocking GraphQL call in a thread
        try:
            result = await asyncio.to_thread(self._verify_sync, token)
            return result
        except Exception:
            logger.debug("Token verification failed", exc_info=True)
            return None

    def _verify_sync(self, token: str) -> Optional[AccessToken]:
        """Blocking helper — runs on the default executor."""
        from .graphql_helpers import execute_graphql

        config = DatahubClientConfig(
            server=self._gms_url,
            token=token,
        )
        graph = DataHubGraph(config)

        try:
            result = execute_graphql(
                graph,
                query="""
                    query getMe {
                        me {
                            corpUser {
                                urn
                                username
                            }
                        }
                    }
                """,
                operation_name="getMe",
            )

            me = result.get("me", {})
            corp_user = me.get("corpUser", {})
            urn = corp_user.get("urn")
            username = corp_user.get("username")

            if not urn:
                return None

            logger.debug(f"Verified token for user: {username} ({urn})")

            return AccessToken(
                token=token,
                client_id=username or "unknown",
                scopes=[],
                claims={
                    "sub": urn,
                    "username": username,
                    "datahub_token": token,
                },
            )
        except Exception:
            logger.debug("Token verification query failed", exc_info=True)
            return None


class PerUserClientMiddleware(Middleware):
    """Create a per-request ``DataHubClient`` from the authenticated user's token.

    This middleware reads the ``AccessToken`` set by FastMCP's auth layer
    and creates a ``DataHubClient`` using the user's own PAT.  This means
    mutations are attributed to the calling user, not the service account.

    Must be placed **after** ``_DataHubClientMiddleware`` in the middleware
    chain so it can override the default client when auth is present.
    """

    def __init__(self, gms_url: str) -> None:
        self._gms_url = gms_url.rstrip("/")

    async def on_message(
        self,
        context: Any,
        call_next: CallNext,
    ) -> Any:
        # Try to get the access token from the request context
        access_token = self._get_access_token(context)

        if access_token and access_token.claims.get("datahub_token"):
            user_token = access_token.claims["datahub_token"]
            user_client = DataHubClient(
                config=DatahubClientConfig(
                    server=self._gms_url,
                    token=user_token,
                    client_mode=ClientMode.SDK,
                ),
                datahub_component=f"mcp-server-datahub/{__version__}",
            )
            logger.debug(
                f"Using per-user client for {access_token.claims.get('username', 'unknown')}"
            )
            with with_datahub_client(user_client):
                return await call_next(context)

        # No auth token — fall through to the default service-account client
        return await call_next(context)

    @staticmethod
    def _get_access_token(context: Any) -> Optional[AccessToken]:
        """Extract the AccessToken from the current request context.

        FastMCP's auth stack sets the authenticated user in a contextvar
        via ``AuthContextMiddleware``.  We read it from there.
        """
        try:
            from mcp.server.auth.middleware.auth_context import get_access_token

            return get_access_token()
        except ImportError:
            return None
