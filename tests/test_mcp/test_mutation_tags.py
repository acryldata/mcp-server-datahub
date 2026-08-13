"""Every tool registered by ``register_mutation_tools`` writes to DataHub.

The registration is the only place that says so. FastMCP's ``disable(tags=...)`` and
the ``exclude_tags`` option in ``fastmcp.json`` both select on tags, so a write tool
registered without ``ToolType.MUTATION`` stays exposed after an operator has asked for
the mutation surface to be removed.

These tests assert the invariant rather than a list of names, so a write tool added
later cannot arrive untagged.
"""

import os
from unittest.mock import patch

import pytest
from fastmcp import FastMCP

from datahub_integrations.mcp.fastmcp_helpers import list_mcp_tools_sync
from datahub_integrations.mcp.mcp_server import (
    ToolType,
    register_mutation_tools,
    register_search_tools,
)

MUTATION_TAG = ToolType.MUTATION.value


def _mutation_server() -> FastMCP:
    """A server carrying only the mutation surface, with every gate opened."""
    mcp: FastMCP = FastMCP[None](name="test-mutations")
    with patch.dict(
        os.environ,
        {"TOOLS_IS_MUTATION_ENABLED": "true", "SAVE_DOCUMENT_TOOL_ENABLED": "true"},
    ):
        register_mutation_tools(mcp)
    return mcp


def test_every_registered_mutation_tool_carries_the_mutation_tag() -> None:
    tools = list_mcp_tools_sync(_mutation_server())

    assert tools, "register_mutation_tools registered nothing; the gate did not open"

    untagged = sorted(tool.name for tool in tools if MUTATION_TAG not in tool.tags)
    assert untagged == [], (
        f"write tools registered without the {MUTATION_TAG!r} tag: {untagged}"
    )


def test_disabling_the_mutation_tag_removes_every_write_tool() -> None:
    """The consequence: tag-based filtering must leave no write tool behind."""
    mcp = _mutation_server()
    mcp.disable(tags={MUTATION_TAG})

    remaining = sorted(tool.name for tool in list_mcp_tools_sync(mcp))
    assert remaining == [], (
        f"still exposed after disabling the {MUTATION_TAG!r} tag: {remaining}"
    )


@pytest.mark.parametrize("is_oss", [True, False])
def test_read_tools_are_not_tagged_as_mutations(is_oss: bool) -> None:
    """The tag must stay meaningful: reads carry it in neither deployment shape."""
    mcp: FastMCP = FastMCP[None](name="test-reads")
    register_search_tools(mcp, is_oss=is_oss)

    mislabelled = sorted(
        tool.name for tool in list_mcp_tools_sync(mcp) if MUTATION_TAG in tool.tags
    )
    assert mislabelled == [], f"read tools tagged as mutations: {mislabelled}"
