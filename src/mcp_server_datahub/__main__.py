import logging
import os
from typing import Any, Optional

import click
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp import FastMCP
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

# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)


_GET_ME_QUERY = "query getMe { me { corpUser { urn username } } }"


def _verify_client(client: DataHubClient) -> None:
    """Verify the client can authenticate by calling the me query."""
    client._graph.execute_graphql(_GET_ME_QUERY)


def _token_from_request() -> Optional[str]:
    """Extract a DataHub token from the current HTTP request.

    Checks, in order:
    1. ``Authorization: Bearer <token>`` header
    2. ``token`` query parameter
    """
    try:
        request = get_http_request()
    except RuntimeError:
        return None
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):]
    return request.query_params.get("token") or None


class _DataHubClientMiddleware(Middleware):
    """Middleware that propagates the DataHub client ContextVar into each request.

    When running with HTTP transport (stateless_http=True), each request is handled
    in a separate async context that does not inherit ContextVars from the main
    thread. This middleware ensures the DataHub client is available in every request
    context by setting the ContextVar at the start of each MCP message.

    If the request carries a token (via ``Authorization: Bearer`` header or ``token``
    query parameter) a per-request client is constructed using that token against the
    same server URL. Otherwise the env-configured default client is used.

    Must be added as the first middleware so it wraps all other middlewares.
    """

    def __init__(self, server_url: str, default_client: Optional[DataHubClient]) -> None:
        self._server_url = server_url
        self._default_client = default_client

    def _client_for_request(self) -> DataHubClient:
        token = _token_from_request()
        if token is not None:
            client = DataHubClient(
                config=DatahubClientConfig(
                    server=self._server_url,
                    token=token,
                    client_mode=ClientMode.SDK,
                    datahub_component=f"mcp-server-datahub/{__version__}",
                )
            )
            _verify_client(client)
            return client
        elif self._default_client is not None:
            return self._default_client
        raise ValueError(
            "No DataHub token provided. Supply a token via the Authorization header or ?token= query parameter."
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

    global_token = os.environ.get("DATAHUB_GMS_TOKEN")
    default_client: Optional[DataHubClient] = None
    if global_token:
        default_client = DataHubClient(
            config=DatahubClientConfig(
                server=server_url,
                token=global_token,
                client_mode=ClientMode.SDK,
                datahub_component=f"mcp-server-datahub/{__version__}",
            )
        )
        default_client.test_connection()

    # _DataHubClientMiddleware must be first so the client ContextVar is
    # available to all subsequent middlewares and tool handlers.  This is
    # especially important for HTTP transport where each request runs in a
    # separate async context.
    mcp.add_middleware(_DataHubClientMiddleware(server_url, default_client))
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
        mcp.run(transport=transport, show_banner=False, stateless_http=True, host="0.0.0.0")
    else:
        mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
