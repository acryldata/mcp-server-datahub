import logging
import os
from typing import Any

import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from mcp_server_datahub._auth import (
    DataHubTokenVerifier,
    PerUserClientMiddleware,
    is_auth_enabled,
)
from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client
from mcp_server_datahub.version_requirements import VersionFilterMiddleware

logging.basicConfig(level=logging.INFO)

# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)


class _DataHubClientMiddleware(Middleware):
    """Middleware that propagates the DataHub client ContextVar into each request.

    When running with HTTP transport (stateless_http=True), each request is handled
    in a separate async context that does not inherit ContextVars from the main
    thread. This middleware ensures the DataHub client is available in every request
    context by setting the ContextVar at the start of each MCP message.

    Must be added as the first middleware so it wraps all other middlewares.
    """

    def __init__(self, client: DataHubClient) -> None:
        self._client = client

    async def on_message(
        self,
        context: Any,
        call_next: Any,
    ) -> Any:
        with with_datahub_client(self._client):
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

    When ``DATAHUB_MCP_AUTH_ENABLED=true``, the server is configured with
    a :class:`DataHubTokenVerifier` that validates incoming Bearer tokens
    against the DataHub backend.  A :class:`PerUserClientMiddleware` then
    creates a per-request ``DataHubClient`` so mutations are attributed to the
    calling user.  STDIO transport is unaffected by this setting.
    """
    global _app_initialized
    if _app_initialized:
        return mcp

    # --- Auth provider (HTTP transport only) ---
    if is_auth_enabled():
        gms_url = os.environ.get("DATAHUB_GMS_URL", "")
        if not gms_url:
            raise RuntimeError(
                "DATAHUB_MCP_AUTH_ENABLED=true requires DATAHUB_GMS_URL to be set"
            )

        from mcp_server_datahub._auth_obo import build_obo_auth, get_obo_config_from_env

        obo_config = get_obo_config_from_env()
        pat_verifier = DataHubTokenVerifier(gms_url)

        if obo_config:
            from fastmcp.server.auth.auth import MultiAuth

            obo_auth = build_obo_auth(obo_config)
            # OBO provider first (quick JWT rejection for non-JWTs),
            # PAT verifier as fallback for DataHub native tokens.
            if hasattr(obo_auth, "token_verifier"):
                # RemoteAuthProvider — use as server (provides discovery routes)
                mcp.auth = MultiAuth(server=obo_auth, verifiers=[pat_verifier])
            else:
                # Bare EntraOBOVerifier (no base_url → no discovery routes)
                mcp.auth = MultiAuth(verifiers=[obo_auth, pat_verifier])

            logging.getLogger(__name__).info(
                "Auth enabled — Entra ID OBO + DataHub PAT verification active"
            )
        else:
            mcp.auth = pat_verifier
            logging.getLogger(__name__).info(
                "Auth enabled — DataHub PAT verification active (no OBO config)"
            )

    # --- Service-account client (always created as fallback) ---
    client = DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )

    # _DataHubClientMiddleware must be first so the client ContextVar is
    # available to all subsequent middlewares and tool handlers.  This is
    # especially important for HTTP transport where each request runs in a
    # separate async context.
    mcp.add_middleware(_DataHubClientMiddleware(client))

    # When auth is enabled, PerUserClientMiddleware overrides the default
    # client with a per-request client using the authenticated user's token.
    if is_auth_enabled():
        gms_url = os.environ.get("DATAHUB_GMS_URL", "")
        mcp.add_middleware(PerUserClientMiddleware(gms_url))

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
        mcp.run(transport=transport, show_banner=False, stateless_http=True)
    else:
        mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
