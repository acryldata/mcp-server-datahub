"""OAuth 2.1 proxy endpoints for upstream identity provider authentication.

Implements the OAuth 2.1 authorization code flow as a proxy to an upstream
identity provider (IdP), enabling MCP clients (Claude Code, Cursor, etc.)
to authenticate automatically via the standard MCP OAuth discovery mechanism.

All IdP-specific URLs are configured via environment variables — no
provider-specific defaults are hardcoded.

Required env vars (when OAuth is enabled via ``OIDC_CLIENT_ID``):
- ``OIDC_CLIENT_ID`` — OAuth client ID registered with the upstream IdP
- ``OAUTH_AUTHORIZE_ENDPOINT`` — upstream IdP authorization URL
- ``OAUTH_TOKEN_ENDPOINT`` — upstream IdP token exchange URL

Optional env vars:
- ``OAUTH_ISSUER_URL`` — IdP issuer (used for WWW-Authenticate realm)
- ``OAUTH_RESOURCE_URL`` — resource URL advertised in metadata (auto-detected)
- ``OAUTH_DEFAULT_SCOPE`` — default scope if not provided by client
  (default: ``openid profile email``)

Endpoints:
- ``/.well-known/oauth-authorization-server`` — Authorization server metadata (RFC 8414)
- ``/.well-known/oauth-protected-resource`` — Protected resource metadata (RFC 9728)
- ``/oauth/register`` — Dynamic client registration (returns pre-configured client)
- ``/oauth/authorize`` — Proxies authorization to upstream IdP
- ``/oauth/callback`` — Receives auth code from IdP, redirects to MCP client
- ``/oauth/token`` — Proxies token exchange to IdP (no caching, no refresh tokens)
"""

import logging
import os
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from mcp_server_datahub.mcp_server import mcp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Session storage (in-memory, short-lived, NOT token caching)
# ---------------------------------------------------------------------------

_SESSION_TTL_SECONDS = 300  # 5 minutes — flow state only


@dataclass
class _OAuth2Session:
    original_redirect_uri: str
    client_id: str
    scope: str
    timestamp: float = field(default_factory=time.time)


# state -> session mapping (cleaned up on access)
_sessions: dict[str, _OAuth2Session] = {}


def _store_session(state: str, redirect_uri: str, client_id: str, scope: str) -> None:
    _cleanup_expired()
    _sessions[state] = _OAuth2Session(
        original_redirect_uri=redirect_uri,
        client_id=client_id,
        scope=scope,
    )
    logger.debug("Stored OAuth session for state: %s…", state[:10])


def _pop_session(state: str) -> _OAuth2Session | None:
    _cleanup_expired()
    session = _sessions.pop(state, None)
    if session and (time.time() - session.timestamp) > _SESSION_TTL_SECONDS:
        logger.warning("OAuth session expired for state: %s…", state[:10])
        return None
    return session


def _cleanup_expired() -> None:
    now = time.time()
    expired = [
        k for k, v in _sessions.items() if now - v.timestamp > _SESSION_TTL_SECONDS
    ]
    for k in expired:
        _sessions.pop(k, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_server_url(request: Request) -> str:
    """Derive the external base URL from the incoming request."""
    host = request.headers.get("host")
    if not host:
        return "http://localhost:8000"
    proto = "https"
    fwd = request.headers.get("x-forwarded-proto")
    if fwd:
        proto = fwd
    elif ":" in host:
        port = host.split(":")[-1]
        if port not in ("443",):
            proto = "http"
    return f"{proto}://{host}"


# ---------------------------------------------------------------------------
# Configuration — all read from environment, no hardcoded defaults
# ---------------------------------------------------------------------------


def _oauth_client_id() -> str:
    return os.getenv("OIDC_CLIENT_ID", "")


def _oauth_authorize_endpoint() -> str:
    """Upstream IdP authorization endpoint (required when OAuth is enabled)."""
    val = os.getenv("OAUTH_AUTHORIZE_ENDPOINT", "")
    if not val:
        raise RuntimeError(
            "OAUTH_AUTHORIZE_ENDPOINT must be set when OIDC_CLIENT_ID is configured"
        )
    return val


def _oauth_token_endpoint() -> str:
    """Upstream IdP token endpoint (required when OAuth is enabled)."""
    val = os.getenv("OAUTH_TOKEN_ENDPOINT", "")
    if not val:
        raise RuntimeError(
            "OAUTH_TOKEN_ENDPOINT must be set when OIDC_CLIENT_ID is configured"
        )
    return val


def _oauth_issuer_url() -> str:
    """Upstream IdP issuer URL (optional, used for WWW-Authenticate realm)."""
    return os.getenv("OAUTH_ISSUER_URL", "")


def _oauth_resource_url(request: Request) -> str:
    """Resource URL advertised in protected-resource metadata."""
    return os.getenv("OAUTH_RESOURCE_URL", _get_server_url(request))


# ---------------------------------------------------------------------------
# Discovery endpoints
# ---------------------------------------------------------------------------


@mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
async def authorization_server_metadata(request: Request) -> Response:
    base = _get_server_url(request)
    metadata = {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
    }
    logger.info("Serving OAuth2.1 authorization server metadata for: %s", base)
    return JSONResponse(metadata)


@mcp.custom_route("/.well-known/oauth-protected-resource", methods=["GET"])
async def protected_resource_metadata(request: Request) -> Response:
    base = _get_server_url(request)
    resource_url = _oauth_resource_url(request)
    metadata = {
        "resource": resource_url,
        "authorization_servers": [base],
        "bearer_methods_supported": ["header"],
    }
    logger.info(
        "Serving OAuth2.1 protected resource metadata — resource: %s, auth server: %s",
        resource_url,
        base,
    )
    return JSONResponse(metadata)


# ---------------------------------------------------------------------------
# OAuth proxy endpoints
# ---------------------------------------------------------------------------


@mcp.custom_route("/oauth/register", methods=["POST"])
async def register_client(request: Request) -> Response:
    """Dynamic client registration — returns pre-configured OAuth client ID."""
    body = await request.json()
    logger.info("Dynamic client registration request: %s", body)

    base = _get_server_url(request)
    client_id = _oauth_client_id()

    response = {
        "client_id": client_id,
        "redirect_uris": [f"{base}/oauth/callback"],
        "response_types": ["code"],
        "grant_types": ["authorization_code"],
        "token_endpoint_auth_method": "none",
        "client_id_issued_at": int(time.time()),
        "registration_client_uri": f"{base}/oauth/client/{client_id}",
    }
    logger.info("Returning pre-configured OAuth client: %s", client_id)
    return JSONResponse(response)


@mcp.custom_route("/oauth/authorize", methods=["GET"])
async def authorize_proxy(request: Request) -> Response:
    """Proxy authorization request to upstream IdP, rewriting redirect_uri."""
    params = dict(request.query_params)
    logger.info("Intercepting authorization request from MCP client")

    original_redirect_uri = params.get("redirect_uri", "")
    state = params.get("state", "")
    client_id = params.get("client_id", "")
    scope = params.get("scope", "")

    if state and original_redirect_uri:
        _store_session(state, original_redirect_uri, client_id, scope)

    # Rewrite redirect_uri to our callback
    base = _get_server_url(request)
    params["redirect_uri"] = f"{base}/oauth/callback"

    # Remove resource parameter if upstream IdP does not support it
    if "resource" in params:
        logger.info("Removing resource parameter: %s", params["resource"])
        del params["resource"]

    # Ensure scope is set if required by IdP
    if not params.get("scope"):
        default_scope = os.getenv("OAUTH_DEFAULT_SCOPE", "openid profile email")
        params["scope"] = default_scope
        logger.info("Added default scope: %s", default_scope)

    authorize_endpoint = _oauth_authorize_endpoint()
    auth_url = f"{authorize_endpoint}?{urlencode(params)}"
    logger.info(
        "Proxying authorization request to upstream IdP: %s", authorize_endpoint
    )
    return RedirectResponse(url=auth_url, status_code=302)


@mcp.custom_route("/oauth/callback", methods=["GET"])
async def oauth_callback(request: Request) -> Response:
    """Receive auth code from upstream IdP, redirect back to MCP client."""
    params = dict(request.query_params)
    logger.info("Received OAuth callback from upstream IdP")

    code = params.get("code")
    state = params.get("state")
    error = params.get("error")

    if error:
        logger.error(
            "OAuth callback error: %s — %s", error, params.get("error_description")
        )
        return JSONResponse(
            {"error": error, "error_description": params.get("error_description", "")},
            status_code=400,
        )

    if not code or not state:
        logger.error("Missing authorization code or state in callback")
        return JSONResponse(
            {"error": "invalid_request", "error_description": "Missing code or state"},
            status_code=400,
        )

    session = _pop_session(state)
    if session is None:
        logger.error("No session found for state: %s…", state[:10])
        return JSONResponse(
            {"error": "invalid_state", "error_description": "Unknown or expired state"},
            status_code=400,
        )

    # Build redirect back to MCP client with code + state + any extra params
    callback_params = {"code": code, "state": state}
    for key, value in params.items():
        if key not in ("code", "state"):
            callback_params[key] = value

    callback_url = f"{session.original_redirect_uri}?{urlencode(callback_params)}"
    logger.info("Redirecting back to MCP client: %s", session.original_redirect_uri)
    return RedirectResponse(url=callback_url, status_code=302)


@mcp.custom_route("/oauth/token", methods=["POST"])
async def token_proxy(request: Request) -> Response:
    """Proxy token exchange to upstream IdP. No caching, no refresh tokens."""
    body = await request.body()
    form_data = {k: v[0] for k, v in parse_qs(body.decode()).items()}
    logger.info(
        "Intercepting token exchange — grant_type: %s", form_data.get("grant_type")
    )

    # Rewrite redirect_uri to match what we sent to the IdP during authorize
    base = _get_server_url(request)
    if "redirect_uri" in form_data:
        original = form_data["redirect_uri"]
        form_data["redirect_uri"] = f"{base}/oauth/callback"
        logger.info(
            "Rewritten redirect_uri: %s -> %s", original, form_data["redirect_uri"]
        )

    # Proxy to upstream IdP token endpoint
    token_endpoint = _oauth_token_endpoint()
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                token_endpoint,
                data=form_data,
                timeout=30.0,
            )

        token_response = resp.json()

        if resp.status_code != 200:
            logger.error("IdP token exchange failed: %s", token_response)
            return JSONResponse(token_response, status_code=resp.status_code)

        logger.info("Token exchange successful")
        return JSONResponse(
            token_response,
            headers={
                "Cache-Control": "no-store",
                "Pragma": "no-cache",
            },
        )

    except Exception as e:
        logger.error("Token exchange failed: %s", e, exc_info=True)
        return JSONResponse(
            {
                "error": "token_exchange_failed",
                "error_description": "Token exchange failed. Please try again.",
            },
            status_code=500,
        )


# ---------------------------------------------------------------------------
# ASGI middleware: Bearer token detection (runs at HTTP layer)
# ---------------------------------------------------------------------------

# Paths that never require authentication
_AUTH_EXEMPT_PATHS = frozenset(
    {
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-protected-resource",
        "/.well-known/openid-configuration",
        "/.well-known/jwks.json",
        "/oauth/register",
        "/oauth/authorize",
        "/oauth/callback",
        "/oauth/token",
        "/health",
    }
)


class BearerTokenDetectionMiddleware:
    """Starlette ASGI middleware that enforces Bearer token presence.

    When ``OIDC_CLIENT_ID`` is set, no fallback client is available, and no
    Bearer token is present on a request that requires authentication,
    returns HTTP 401 with a ``WWW-Authenticate`` header so that MCP clients
    can discover the OAuth 2.1 endpoints and initiate the authorization flow.

    If a fallback client is available (``DATAHUB_GMS_TOKEN`` is set), requests
    without Bearer tokens are allowed through — the MCP-level middleware will
    use the fallback client.

    This runs at the HTTP layer — *before* the MCP JSON-RPC protocol
    layer — ensuring the 401 is a proper HTTP response (not a JSON-RPC
    error).
    """

    def __init__(self, app: ASGIApp, has_fallback_client: bool = False) -> None:
        self.app = app
        self._has_fallback_client = has_fallback_client

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        client_id = os.getenv("OIDC_CLIENT_ID", "")
        if not client_id:
            # OAuth flow not configured — pass through
            await self.app(scope, receive, send)
            return

        # If a fallback client exists (DATAHUB_GMS_TOKEN set), requests
        # without Bearer tokens should pass through to the MCP layer where
        # _DataHubClientMiddleware will use the fallback client.
        if self._has_fallback_client:
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        path = request.url.path.rstrip("/") or "/"

        # Allow auth-exempt paths
        if path in _AUTH_EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        # Allow OPTIONS (CORS preflight)
        if request.method == "OPTIONS":
            await self.app(scope, receive, send)
            return

        # Check for Bearer token
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            await self.app(scope, receive, send)
            return

        # No Bearer token and auth required — return 401
        issuer_url = _oauth_issuer_url()
        realm = _extract_realm(issuer_url) if issuer_url else "oauth"
        resource_url = os.getenv("OAUTH_RESOURCE_URL", _get_server_url(request))

        www_auth = f'Bearer realm="{realm}"'
        if client_id:
            www_auth += f', client_id="{client_id}"'
        www_auth += f', resource="{resource_url}"'

        response = JSONResponse(
            {
                "error": "unauthorized",
                "error_description": "Bearer token required for MCP tool calls",
            },
            status_code=401,
            headers={
                "WWW-Authenticate": www_auth,
            },
        )
        await response(scope, receive, send)


def _extract_realm(issuer_url: str) -> str:
    """Extract hostname from issuer URL for WWW-Authenticate realm."""
    try:
        return urlparse(issuer_url).hostname or "oauth"
    except Exception:
        return "oauth"
