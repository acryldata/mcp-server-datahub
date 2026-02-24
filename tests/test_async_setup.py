import inspect
import time
import weakref

import anyio
import fastmcp.tools.tool
import pytest
import mcp_server_datahub.mcp_server as mcp_server_module

from mcp_server_datahub.mcp_server import (
    async_background,
    create_mcp_server,
    register_all_tools,
)

mcp = create_mcp_server()
register_all_tools(mcp, is_oss=True)


@pytest.mark.anyio
async def test_async_background() -> None:
    @async_background
    def my_sleep(sec: float) -> None:
        time.sleep(sec)

    start_time = time.time()

    async with anyio.create_task_group() as tg:
        tg.start_soon(my_sleep, 0.5)
        tg.start_soon(my_sleep, 0.6)
        tg.start_soon(my_sleep, 0.7)

    end_time = time.time()
    duration = end_time - start_time
    # The calls should not be serialized, so the duration should be less than the sum of the durations.
    assert 0.5 <= duration < 1.8


def test_all_tools_are_async() -> None:
    # If any tools are sync, the tool execution will block the main event loop.
    for tool in mcp._tool_manager._tools.values():
        assert isinstance(tool, fastmcp.tools.tool.FunctionTool)
        assert inspect.iscoroutinefunction(tool.fn)


def test_register_all_tools_registers_each_instance_independently() -> None:
    mcp_one = create_mcp_server(name="datahub-test-one")
    mcp_two = create_mcp_server(name="datahub-test-two")

    register_all_tools(mcp_one, is_oss=True)
    register_all_tools(mcp_two, is_oss=True)

    assert "search" in mcp_one._tool_manager._tools
    assert "search" in mcp_two._tool_manager._tools

    # Multiple calls to register_all_tools with the same MCP instance should not register duplicate tools.
    initial_tool_count = len(mcp_one._tool_manager._tools)
    register_all_tools(mcp_one, is_oss=True)
    assert len(mcp_one._tool_manager._tools) == initial_tool_count
