import importlib.metadata

import click
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from typing_extensions import Literal

from mcp_server_datahub.mcp_server import mcp, with_datahub_client


@click.command()
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "http"]),
    default="stdio",
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(transport: Literal["stdio", "sse", "http"]) -> None:
    # Because we want to override the datahub_component, we can't use DataHubClient.from_env()
    # and need to use the DataHubClient constructor directly.
    mcp_version = importlib.metadata.version("mcp-server-datahub")
    graph = get_default_graph(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{mcp_version}",
    )
    client = DataHubClient(graph=graph)

    with with_datahub_client(client):
        if transport == "http":
            mcp.run(transport=transport, show_banner=False, stateless_http=True)
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
