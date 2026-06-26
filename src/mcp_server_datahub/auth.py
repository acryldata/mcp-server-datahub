"""Static-token authentication provider for the DataHub MCP server.

This module provides a simple shared-key authentication mechanism for
self-hosted deployments of the DataHub MCP server. It is intentionally
minimal: no OAuth, no JWTs, no key-management service required.

Configuration
-------------
Set the ``DATAHUB_MCP_AUTH_TOKENS`` environment variable to a
comma-separated list of opaque tokens before starting the server::

    export DATAHUB_MCP_AUTH_TOKENS="token-abc123,token-xyz789"

Clients authenticate by sending the token as a standard HTTP Bearer
credential::

    Authorization: Bearer token-abc123

Multiple tokens are supported so that keys can be rotated without
downtime — issue a new token, distribute it, then remove the old one.

Transport behaviour
-------------------
Authentication is enforced **only for HTTP-based transports** (``sse``
and ``http``).  The ``stdio`` transport runs as a subprocess of the MCP
client and has no network exposure, so auth is neither needed nor
possible there.

If ``DATAHUB_MCP_AUTH_TOKENS`` is not set the auth provider is not
installed and the server remains open (same behaviour as before).  A
warning is logged when running with an HTTP transport without auth so
operators are aware of the exposure.

Security notes
--------------
- Tokens are compared with :func:`secrets.compare_digest` to avoid
  timing-based side-channel attacks.
- Tokens should be treated as secrets: use a secret manager or
  environment-variable injection (e.g. Kubernetes Secrets, Vault) rather
  than hard-coding them in config files.
- There is no built-in expiry; rotate tokens by updating the env var and
  restarting (or hot-reloading) the server.
"""

from __future__ import annotations

import os
import secrets
from typing import Optional

from fastmcp.server.auth import AccessToken, TokenVerifier
from loguru import logger

# Environment variable that holds the comma-separated list of valid tokens.
DATAHUB_MCP_AUTH_TOKENS_ENV_VAR = "DATAHUB_MCP_AUTH_TOKENS"


class StaticTokenAuthProvider(TokenVerifier):
    """Authenticates requests against a fixed set of pre-shared tokens.

    Each incoming ``Authorization: Bearer <token>`` header value is
    checked against the configured token set using a constant-time
    comparison.  If the token matches any entry the request is accepted;
    otherwise a 401 is returned by FastMCP's authentication middleware.

    Parameters
    ----------
    tokens:
        Non-empty list of valid opaque token strings.  Leading/trailing
        whitespace is stripped from each entry.
    """

    def __init__(self, tokens: list[str]) -> None:
        super().__init__()
        cleaned = [t.strip() for t in tokens if t.strip()]
        if not cleaned:
            raise ValueError(
                "StaticTokenAuthProvider requires at least one non-empty token. "
                f"Check the {DATAHUB_MCP_AUTH_TOKENS_ENV_VAR} environment variable."
            )
        # frozenset for O(1) membership, but we still iterate to use
        # compare_digest — a frozenset.__contains__ check would be
        # susceptible to early-exit timing differences.
        self._tokens: tuple[bytes, ...] = tuple(t.encode("utf-8") for t in cleaned)
        logger.info(
            "DataHub MCP auth enabled — {} token(s) configured.",
            len(self._tokens),
        )

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        """Return an :class:`AccessToken` if *token* is valid, else ``None``."""
        token_bytes = token.encode("utf-8")
        for valid in self._tokens:
            if secrets.compare_digest(token_bytes, valid):
                return AccessToken(
                    token=token,
                    # Use a generic client ID — we don't issue per-client tokens.
                    client_id="mcp-client",
                    scopes=[],
                )
        return None


def build_auth_provider() -> Optional[StaticTokenAuthProvider]:
    """Build a :class:`StaticTokenAuthProvider` from environment variables.

    Reads :data:`DATAHUB_MCP_AUTH_TOKENS_ENV_VAR`, splits on commas and
    returns a configured provider.  Returns ``None`` when the variable is
    absent or empty so callers can treat *no auth* as a valid (if
    discouraged) configuration.
    """
    raw = os.environ.get(DATAHUB_MCP_AUTH_TOKENS_ENV_VAR, "").strip()
    if not raw:
        return None
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    if not tokens:
        return None
    return StaticTokenAuthProvider(tokens)
