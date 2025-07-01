import importlib.metadata

import click
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from typing_extensions import Literal

from mcp_server_datahub.mcp_server import mcp, set_client


@click.command()
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "streamable-http"]),
    default="stdio",
)
def main(transport: Literal["stdio", "sse", "streamable-http"]) -> None:
    # Because we want to override the datahub_component, we can't use DataHubClient.from_env()
    # and need to use the DataHubClient constructor directly.
    mcp_version = importlib.metadata.version("mcp-server-datahub")
    graph = get_default_graph(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{mcp_version}",
    )
    set_client(DataHubClient(graph=graph))

    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
