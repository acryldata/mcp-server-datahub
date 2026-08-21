"""Tests for mutation tool registration."""

from unittest.mock import MagicMock

from datahub_integrations.mcp.mcp_server import register_mutation_tools


def test_ensure_tag_unavailable_when_mutations_disabled(monkeypatch):
    monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", "false")
    mcp = MagicMock()

    register_mutation_tools(mcp)

    mcp.tool.assert_not_called()
