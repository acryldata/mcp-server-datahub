import asyncio
import logging
from typing import Any

import mcp.types as mt
from datahub.telemetry import telemetry
from datahub.utilities.perf_timer import PerfTimer
from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools import ToolResult

from mcp_server_datahub._version import __version__

logger = logging.getLogger(__name__)

telemetry.telemetry_instance.add_global_property(
    "mcp_server_datahub_version", __version__
)

# Strong references to in-flight telemetry tasks. asyncio only holds a weak
# reference to a running task, so without this the task can be garbage
# collected before it completes.
_background_pings: set["asyncio.Task[None]"] = set()


def _on_ping_done(task: "asyncio.Task[None]") -> None:
    _background_pings.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.debug("Telemetry ping failed", exc_info=task.exception())


def _ping_in_background(event_name: str, properties: dict[str, Any]) -> None:
    """Send a telemetry ping without making the caller wait for it.

    `telemetry_instance.ping()` performs a blocking HTTP request. Calling it
    inline from the request path means every tool call waits for the telemetry
    endpoint to answer, and blocks the event loop while it waits — so a slow or
    unreachable endpoint delays each call by however long that request takes.

    Telemetry is best-effort, so hand it to a worker thread and do not await it.
    """
    try:
        task = asyncio.create_task(
            asyncio.to_thread(telemetry.telemetry_instance.ping, event_name, properties)
        )
    except RuntimeError:
        # No running event loop — there is nothing to block, so send inline.
        telemetry.telemetry_instance.ping(event_name, properties)
        return

    _background_pings.add(task)
    task.add_done_callback(_on_ping_done)


def _get_client_info(context: MiddlewareContext[Any]) -> dict[str, str]:
    """Extract MCP client identity and HTTP user-agent from the request context."""
    info: dict[str, str] = {}
    try:
        # MCP clients send clientInfo (name + version) during the initialize
        # handshake.  The ServerSession persists it in client_params, so it is
        # available on every subsequent request regardless of transport.
        ctx = context.fastmcp_context
        if ctx and ctx.request_context:
            session = ctx.request_context.session
            client_params = getattr(session, "client_params", None)
            if client_params and client_params.clientInfo:
                info["client_name"] = client_params.clientInfo.name
                info["client_version"] = client_params.clientInfo.version
    except Exception:
        logger.debug("Failed to extract MCP clientInfo", exc_info=True)

    try:
        # get_http_headers() returns {} when there is no HTTP layer (stdio),
        # so this only populates http_user_agent for HTTP/SSE transports.
        headers = get_http_headers()
        ua = headers.get("user-agent")
        if ua:
            info["http_user_agent"] = ua
    except Exception:
        pass

    return info


class TelemetryMiddleware(Middleware):
    """Middleware that logs tool calls."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        telemetry_data: dict[str, Any] = {}
        with PerfTimer() as timer:
            telemetry_data = {
                "tool": context.message.name,
                "source": context.source,
                "type": context.type,
                "method": context.method,
                **_get_client_info(context),
            }
            try:
                result = await call_next(context)

                telemetry_data["tool_result_length"] = sum(
                    len(block.text)
                    for block in result.content
                    if isinstance(block, mt.TextContent)
                )

                return result

            except Exception as e:
                telemetry_data["tool_call_error"] = e.__class__.__name__
                telemetry_data["tool_call_error_message"] = str(e)[:500]
                telemetry_data["tool_result_is_error"] = True
                raise
            finally:
                telemetry_data["duration_seconds"] = timer.elapsed_seconds()
                _ping_in_background("mcp-server-tool-call", telemetry_data)
