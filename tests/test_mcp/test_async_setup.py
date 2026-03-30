from fastmcp.tools.function_tool import FunctionTool
from fastmcp.tools.tool import Tool

from datahub_integrations.mcp.mcp_server import (
    get_valid_tools_from_mcp,
    register_all_tools,
)

# Register tools so we have something to test
register_all_tools(is_oss=True)


def test_all_tools_are_function_tools() -> None:
    # FastMCP v3 auto-dispatches sync functions to a threadpool,
    # so we just verify all tools are properly registered FunctionTool instances.
    tools = get_valid_tools_from_mcp()
    assert len(tools) > 0
    for tool in tools:
        assert isinstance(tool, Tool)
        assert isinstance(tool, FunctionTool)
