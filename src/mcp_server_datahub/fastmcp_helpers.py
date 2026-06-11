"""Helpers for listing tools from FastMCP servers (v3+).

FastMCP 3 removed the internal ``_tool_manager._tools`` registry; use
``list_tools(run_middleware=False)`` instead (async). These helpers bridge to sync
callers via asyncer.
"""

from __future__ import annotations

import asyncer
from fastmcp import FastMCP
from fastmcp.tools import Tool as FastMCPTool


def list_mcp_tools_sync(mcp: FastMCP) -> list[FastMCPTool]:
    """Return registered tools without running MCP wire middleware."""

    async def _list() -> list[FastMCPTool]:
        tools = await mcp.list_tools(run_middleware=False)
        return list(tools)

    return asyncer.syncify(_list, raise_sync_error=False)()
