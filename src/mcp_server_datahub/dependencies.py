"""FastMCP v3 dependency injection for DataHub MCP tools.

This module provides ``Depends()``-based dependency injection for the DataHub
client and MCP context.  When a tool function declares a parameter with one of
these dependencies, FastMCP automatically resolves it at call time and hides
it from the LLM-facing tool schema.

Usage in a tool function::

    from uncalled_for import Depends
    from ..dependencies import get_client_dep, get_context_dep

    def my_tool(
        query: str,
        client: DataHubClient = Depends(get_client_dep),
        mcp_ctx: MCPContext = Depends(get_context_dep),
    ) -> dict:
        ...

The underlying mechanism still uses the ContextVar set by
``_DataHubClientMiddleware``, but ``Depends()`` makes the dependency explicit,
testable, and auto-hidden from the LLM.

Note: When calling these tool functions *directly* in tests (outside of
FastMCP), pass the dependency explicitly or ensure the ContextVar is set.
"""

from datahub.sdk.main_client import DataHubClient

from .graphql_helpers import MCPContext, get_datahub_client, get_mcp_context


def get_client_dep() -> DataHubClient:
    """Dependency that resolves the current DataHub client from the ContextVar.

    Use with ``Depends(get_client_dep)`` in tool function signatures.
    """
    return get_datahub_client()


def get_context_dep() -> MCPContext:
    """Dependency that resolves the current MCP context from the ContextVar.

    Use with ``Depends(get_context_dep)`` in tool function signatures.
    The MCPContext includes both the DataHub client and tool context (e.g.,
    view preferences).
    """
    return get_mcp_context()
