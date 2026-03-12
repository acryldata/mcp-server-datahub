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


def _build_global_client() -> DataHubClient:
    return DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )


def _verify_client(client: DataHubClient) -> None:
    """Verify the client can authenticate by calling the me query."""
    client._graph.execute_graphql(_GET_ME_QUERY)


def _token_from_request() -> Optional[str]:
    """Extract a DataHub token from the current HTTP request.

    Reads the ``Authorization: Bearer <token>`` header.
    Returns None if there is no active HTTP request.
    """
    try:
        request = get_http_request()
    except RuntimeError:
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
    """

    def __init__(self, server_url: str) -> None:
        super().__init__()
        self._server_url = server_url
        # Cache keyed by (server_url, token); TTL of 5 minutes, max 1024 entries.
        self._cache: TTLCache = TTLCache(maxsize=1024, ttl=300)

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        cache_key = (self._server_url, token)
        if cache_key in self._cache:
            return self._cache[cache_key]
        try:
            client = _build_client(self._server_url, token)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _verify_client, client)
            result: Optional[AccessToken] = AccessToken(
                client_id=f"mcp-server-datahub/{__version__}", scopes=[], token=token
            )
        except Exception as e:
            logger.warning("DataHub token verification failed: %s", e)
            return None
        self._cache[cache_key] = result
        return result


class _DataHubClientMiddleware(Middleware):
    """Middleware that propagates the DataHub client ContextVar into each request.

    When running with HTTP transport (stateless_http=True), each request is handled
    in a separate async context that does not inherit ContextVars from the main
    thread. This middleware ensures the DataHub client is available in every request
    context by setting the ContextVar at the start of each MCP message.

    Token validation is handled upstream by ``_DataHubTokenVerifier`` for Bearer
    tokens. This middleware only needs to build the client for the current request
    (or fall back to the global client when a global token is configured).

    Must be added as the first middleware so it wraps all other middlewares.
    """

    def __init__(
        self,
        server_url: str,
        use_global_client: bool = False,
    ) -> None:
        self._server_url = server_url
        self._use_global_client = use_global_client

    def _client_for_request(self) -> DataHubClient:
        token = _token_from_request()
        if token is not None:
            # Token already validated by _DataHubTokenVerifier.
            return _build_client(self._server_url, token)
        if self._use_global_client:
            return _build_global_client()
        raise ValueError(
            "No DataHub token provided. Supply a token via the Authorization header."
        )

    async def on_message(
        self,
        context: Any,
        call_next: Any,
    ) -> Any:
        with with_datahub_client(self._client_for_request()):
            return await call_next(context)


# Adds a health route to the MCP Server.
# Notice that this is only available when the MCP Server is run in HTTP/SSE modes.
# Doesn't make much sense to have it in the stdio mode since it is usually used as a subprocess of the client.
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


_app_initialized = False


def create_app() -> FastMCP:
    """Create and configure the MCP server with DataHub client and middlewares.

    This is the factory function used by ``fastmcp dev`` / ``fastmcp run``
    (via the ``__main__.py:create_app`` entrypoint) and is also called by the
    CLI ``main()`` entrypoint.

    The function is idempotent — calling it more than once returns the same
    ``mcp`` instance without adding duplicate middlewares.
    """
    global _app_initialized
    if _app_initialized:
        return mcp

    server_url = os.environ.get("DATAHUB_GMS_URL")
    if not server_url:
        raise RuntimeError("DATAHUB_GMS_URL environment variable is required.")

    has_global_token = bool(os.environ.get("DATAHUB_GMS_TOKEN"))
    if has_global_token:
        _verify_client(_build_global_client())

    # _DataHubClientMiddleware must be first so the client ContextVar is
    # available to all subsequent middlewares and tool handlers.  This is
    # especially important for HTTP transport where each request runs in a
    # separate async context.
    mcp.add_middleware(
        _DataHubClientMiddleware(server_url, use_global_client=has_global_token)
    )
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
        server_url = os.environ.get("DATAHUB_GMS_URL", "")
        if not os.environ.get("DATAHUB_GMS_TOKEN"):
            mcp.auth = _DataHubTokenVerifier(server_url)
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
