import logging
import os

import click
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp.server.middleware.logging import LoggingMiddleware
from loguru import logger
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.mcp_server import mcp, with_datahub_client

logging.basicConfig(level=logging.INFO)


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
    "--disable-ssl-verification",
    is_flag=True,
    default=False,
    help="Disable SSL certificate verification for DataHub client connections. Use with caution.",
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(
    transport: Literal["stdio", "sse", "http"],
    debug: bool,
    disable_ssl_verification: bool,
) -> None:
    # Check if SSL verification should be disabled via environment variable or CLI flag
    disable_ssl = disable_ssl_verification or os.getenv(
        "DATAHUB_DISABLE_SSL_VERIFICATION", "false"
    ).lower() in ("true", "1", "yes")

    if disable_ssl:
        logger.warning(
            "SSL certificate verification is disabled. This is not recommended for production use."
        )

        # Get environment variables for DataHub connection
        server = os.getenv("DATAHUB_GMS_URL")
        token = os.getenv("DATAHUB_GMS_TOKEN")

        if not server:
            raise ValueError(
                "DATAHUB_GMS_URL environment variable is required when using custom configuration"
            )

        # Create custom config with SSL verification disabled
        config = DatahubClientConfig(
            server=server,
            token=token,
            disable_ssl_verification=True,
            client_mode=ClientMode.SDK,
            datahub_component=f"mcp-server-datahub/{__version__}",
        )

        # Create client with custom config
        graph = DataHubGraph(config)
        client = DataHubClient(graph=graph)
    else:
        # Use the default from_env method
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
            mcp.run(transport=transport, show_banner=False, stateless_http=True)
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
