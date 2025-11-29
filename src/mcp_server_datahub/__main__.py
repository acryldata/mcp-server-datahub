import logging

import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp.server.middleware.logging import LoggingMiddleware
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client

logging.basicConfig(level=logging.INFO)

# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)


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
@click.option(
    "--host",
    type=str,
    default="0.0.0.0",
    help="Host to bind the server to (default: 0.0.0.0 for all interfaces)",
)
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Port to bind the server to (default: 8000)",
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(
    transport: Literal["stdio", "sse", "http"], debug: bool, host: str, port: int
) -> None:
    client = DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )

    if debug:
        # logging.getLogger("datahub").setLevel(logging.DEBUG)
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))
    mcp.add_middleware(TelemetryMiddleware())

    with with_datahub_client(client):
        if transport == "http":
            mcp.run(
                transport=transport,
                show_banner=False,
                stateless_http=True,
                host=host,
                port=port,
            )
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
