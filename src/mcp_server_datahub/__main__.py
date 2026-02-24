import logging

import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp.server.middleware.logging import LoggingMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
from mcp_server_datahub.mcp_server import (
    create_mcp_server,
    register_all_tools,
    with_datahub_client,
)
from mcp_server_datahub.version_requirements import VersionFilterMiddleware

logging.basicConfig(level=logging.INFO)

mcp = create_mcp_server()

# Register tools with OSS-compatible descriptions
register_all_tools(mcp, is_oss=True)


# Adds a health route to the MCP Server.
# Notice that this is only available when the MCP Server is run in HTTP/SSE modes.
# Doesn't make much sense to have it in the stdio mode since it is usually used as a subprocess of the client.
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


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
    client = DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )

    if debug:
        # logging.getLogger("datahub").setLevel(logging.DEBUG)
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))
    mcp.add_middleware(TelemetryMiddleware())
    mcp.add_middleware(VersionFilterMiddleware())
    mcp.add_middleware(DocumentToolsMiddleware())

    with with_datahub_client(client):
        if transport == "http":
            mcp.run(transport=transport, show_banner=False, stateless_http=True)
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
