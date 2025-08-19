from typing import Any

import mcp.types as mt
from datahub.telemetry import telemetry
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from mcp_server_datahub._version import __version__

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
        telemetry_data: dict[str, Any] = {}
        with PerfTimer() as timer:
            telemetry_data = {
                "tool": context.message.name,
                "source": context.source,
                "type": context.type,
                "method": context.method,
            }
            try:
                result = await call_next(context)

                # BUG: The FastMCP type annotations seem to be incorrect.
                # This method typically returns fastmcp.tools.tool.ToolResult.
                if isinstance(result, mt.CallToolResult):
                    telemetry_data["tool_result_is_error"] = result.isError
                telemetry_data["tool_result_length"] = sum(
                    len(block.text)
                    for block in result.content
                    if isinstance(block, mt.TextContent)
                )

                return result

            except Exception as e:
                telemetry_data["tool_call_error"] = e.__class__.__name__
                telemetry_data["tool_result_is_error"] = True
                raise
            finally:
                telemetry_data["duration_seconds"] = timer.elapsed_seconds()
                telemetry.telemetry_instance.ping(
                    "mcp-server-tool-call", telemetry_data
                )
