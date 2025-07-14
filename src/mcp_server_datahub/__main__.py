import logging

import click
import mcp.types as mt
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.server.middleware.logging import LoggingMiddleware
from typing_extensions import Literal

from mcp_server_datahub._version import __version__
from mcp_server_datahub.mcp_server import mcp, with_datahub_client

logging.basicConfig(level=logging.INFO)
telemetry.telemetry_instance.add_global_property(
    "mcp_server_datahub_version", __version__
)


class TelemetryMiddleware(Middleware):
    """Middleware that logs tool calls."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        with PerfTimer() as timer:
            result = await call_next(context)

        telemetry.telemetry_instance.ping(
            "mcp-server-tool-call",
            {
                "tool": context.message.name,
                "source": context.source,
                "type": context.type,
                "method": context.method,
                "duration_seconds": timer.elapsed_seconds(),
            },
        )

        return result


@click.command()
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
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))
    mcp.add_middleware(TelemetryMiddleware())

    with with_datahub_client(client):
        if transport == "http":
            mcp.run(transport=transport, show_banner=False, stateless_http=True)
        else:
            mcp.run(transport=transport, show_banner=False)


if __name__ == "__main__":
    main()
