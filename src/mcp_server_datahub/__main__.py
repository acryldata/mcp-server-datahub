import asyncio
import logging
import os
from typing import Any, Optional

from cachetools import TTLCache

import click
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp import FastMCP
from fastmcp.server.auth import TokenVerifier
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import Middleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client
from mcp_server_datahub.version_requirements import VersionFilterMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)


_GET_ME_QUERY = "query getMe { me { corpUser { urn username } } }"


def _build_client(server_url: str, token: str) -> DataHubClient:
    return DataHubClient(
        config=DatahubClientConfig(
            server=server_url,
            token=token,
            client_mode=ClientMode.SDK,
            datahub_component=f"mcp-server-datahub/{__version__}",
        )
    )


_global_client: Optional[DataHubClient] = None


def _get_global_client() -> DataHubClient:
    global _global_client
    if _global_client is None:
        _global_client = DataHubClient.from_env(
            client_mode=ClientMode.SDK,
            datahub_component=f"mcp-server-datahub/{__version__}",
        )
    return _global_client


def _verify_client(client: DataHubClient) -> None:
    """Verify the client can authenticate by calling the me query."""
    client._graph.execute_graphql(_GET_ME_QUERY)


def _build_and_verify_client(server_url: str, token: str) -> DataHubClient:
    client = _build_client(server_url, token)
    _verify_client(client)
    return client


def _token_from_request() -> Optional[str]:
    """Extract a DataHub token from the current HTTP request.

    Reads the ``Authorization: Bearer <token>`` header.
    Returns None if there is no active HTTP request.
    """
    try:
        request = get_http_request()
    except RuntimeError:
        # get_http_request raises RuntimeError when there is no active HTTP request,
        # e.g. when the server is running in stdio mode instead of HTTP/SSE mode.
        return None
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer ") :]
    return None


class _DataHubTokenVerifier(TokenVerifier):
    """FastMCP TokenVerifier that validates DataHub bearer tokens.

    Called by FastMCP's BearerAuthBackend for every HTTP request that carries
    an Authorization: Bearer header.  If the token is valid a synthetic
    AccessToken is returned; otherwise None causes FastMCP to reply with
    401 WWW-Authenticate: Bearer automatically.

    The verified client is cached (TTL 5 minutes, max 1024 entries) and shared
    with ``_HeaderTokenMiddleware`` to avoid redundant client construction.
    """

    def __init__(self, server_url: str) -> None:
        super().__init__()
        self._server_url = server_url
        # Cache keyed by token → DataHubClient; TTL of 5 minutes, max 1024 entries.
        self.client_cache: TTLCache = TTLCache(maxsize=1024, ttl=300)

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        if self.client_cache.get(token) is not None:
            return AccessToken(
                client_id=f"mcp-server-datahub/{__version__}", scopes=[], token=token
            )
        try:
            loop = asyncio.get_running_loop()
            # Build and verify in a thread (blocking I/O); cache write stays on
            # the event loop after the executor returns to avoid TTLCache data races.
            client = await loop.run_in_executor(
                None, _build_and_verify_client, self._server_url, token
            )
            self.client_cache[token] = client
        except Exception as e:
            logger.warning("DataHub token verification failed: %s", e)
            return None
        return AccessToken(
            client_id=f"mcp-server-datahub/{__version__}", scopes=[], token=token
        )


class _GlobalTokenMiddleware(Middleware):
    """Middleware for the global-token codepath (DATAHUB_GMS_TOKEN is set).

    Uses the pre-verified global client singleton for every request.  If a
    per-request Authorization header is also present it takes precedence, but
    no additional verification is performed — the header token is used as-is to
    build a client on the fly.

    Must be added as the first middleware so it wraps all other middlewares.
    """

    def __init__(self, server_url: str) -> None:
        self._server_url = server_url

    def _client_for_request(self) -> DataHubClient:
        # Per-request header token overrides the global token.
        token = _token_from_request()
        if token is not None:
            return _build_client(self._server_url, token)
        return _get_global_client()

    async def on_message(self, context: Any, call_next: Any) -> Any:
        with with_datahub_client(self._client_for_request()):
            return await call_next(context)


class _HeaderTokenMiddleware(Middleware):
    """Middleware for the header-token codepath (no DATAHUB_GMS_TOKEN configured).

    Requires every request to supply an ``Authorization: Bearer <token>`` header.
    The token must have already been validated by ``_DataHubTokenVerifier``; the
    verified client is reused from the verifier's TTL cache.  On a cache miss a
    new (unverified) client is built — this only happens in non-HTTP transports
    where ``_DataHubTokenVerifier`` is not installed.

    Must be added as the first middleware so it wraps all other middlewares.
    """

    def __init__(self, server_url: str, token_verifier: _DataHubTokenVerifier) -> None:
        self._server_url = server_url
        self._token_verifier = token_verifier

    def _client_for_request(self) -> DataHubClient:
        token = _token_from_request()
        if token is None:
            raise ValueError(
                "No DataHub token provided. Supply a token via the Authorization header."
            )
        # Token already validated by _DataHubTokenVerifier; reuse cached client.
        cached = self._token_verifier.client_cache.get(token)
        if cached is not None:
            return cached
        # Cache miss — build a new client (non-HTTP transport or evicted entry).
        return _build_client(self._server_url, token)

    async def on_message(self, context: Any, call_next: Any) -> Any:
        with with_datahub_client(self._client_for_request()):
            return await call_next(context)


# Adds a health route to the MCP Server.
# Notice that this is only available when the MCP Server is run in HTTP/SSE modes.
# Doesn't make much sense to have it in the stdio mode since it is usually used as a subprocess of the client.
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


_app_initialized = False
_token_verifier: Optional[_DataHubTokenVerifier] = None


def create_app() -> FastMCP:
    """Create and configure the MCP server with DataHub client and middlewares.

    This is the factory function used by ``fastmcp dev`` / ``fastmcp run``
    (via the ``__main__.py:create_app`` entrypoint) and is also called by the
    CLI ``main()`` entrypoint.

    The function is idempotent — calling it more than once returns the same
    ``mcp`` instance without adding duplicate middlewares.
    """
    global _app_initialized, _token_verifier
    if _app_initialized:
        return mcp

    server_url = os.environ.get("DATAHUB_GMS_URL")
    if not server_url:
        raise RuntimeError("DATAHUB_GMS_URL environment variable is required.")

    # The client middleware must be first so the client ContextVar is available
    # to all subsequent middlewares and tool handlers.  This is especially
    # important for HTTP transport where each request runs in a separate async
    # context.
    has_global_token = bool(os.environ.get("DATAHUB_GMS_TOKEN"))
    if has_global_token:
        _verify_client(_get_global_client())
        mcp.add_middleware(_GlobalTokenMiddleware(server_url))
    else:
        _token_verifier = _DataHubTokenVerifier(server_url)
        mcp.add_middleware(_HeaderTokenMiddleware(server_url, _token_verifier))
    mcp.add_middleware(TelemetryMiddleware())
    mcp.add_middleware(VersionFilterMiddleware())
    mcp.add_middleware(DocumentToolsMiddleware())

    _app_initialized = True
    return mcp


@click.command()
@click.version_option(version=__version__)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "http"]),
    default="stdio",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(transport: Literal["stdio", "sse", "http"], debug: bool) -> None:
    if debug:
        # Add LoggingMiddleware before create_app() so it becomes the
        # outermost middleware (FastMCP reverses the list) and logs the
        # full request/response including all other middleware effects.
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))

    create_app()

    if transport == "http":
        if _token_verifier is not None:
            mcp.auth = _token_verifier
        mcp.run(
            transport=transport,
            show_banner=False,
            stateless_http=True,
            host="0.0.0.0",
        )
    else:
        mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
