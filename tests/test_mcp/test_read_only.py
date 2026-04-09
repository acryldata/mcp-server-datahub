"""
Tests for the @read_only decorator and its integration with _register_tool.

Test scenarios:
1. @read_only sets _read_only_hint = True on the decorated function
2. Undecorated functions do not have _read_only_hint
3. @read_only preserves function identity (name, docstring, return value)
4. _register_tool passes readOnlyHint=True in annotations for @read_only functions
5. _register_tool passes no annotations for undecorated functions
"""

from unittest.mock import MagicMock, patch

from datahub_integrations.mcp.version_requirements import read_only
from datahub_integrations.mcp.mcp_server import _register_tool


class TestReadOnlyDecorator:
    """Tests for the @read_only decorator."""

    def test_sets_read_only_hint_attribute(self):
        @read_only
        def my_tool():
            pass

        assert hasattr(my_tool, "_read_only_hint")
        assert my_tool._read_only_hint is True

    def test_undecorated_function_has_no_attribute(self):
        def my_tool():
            pass

        assert not hasattr(my_tool, "_read_only_hint")

    def test_preserves_function_identity(self):
        @read_only
        def my_tool():
            """My docstring."""
            return 42

        assert my_tool() == 42
        assert my_tool.__doc__ == "My docstring."
        assert my_tool.__name__ == "my_tool"

    def test_composable_with_min_version(self):
        from datahub_integrations.mcp.version_requirements import min_version

        @read_only
        @min_version(cloud="0.3.16", oss="1.4.0")
        def my_tool():
            pass

        assert my_tool._read_only_hint is True
        assert hasattr(my_tool, "_version_requirement")


class TestRegisterToolAnnotations:
    """Tests that _register_tool correctly converts _read_only_hint to readOnlyHint."""

    def _make_mock_mcp(self):
        """Return a mock FastMCP instance that captures tool() call kwargs."""
        mock_mcp = MagicMock()
        mock_mcp.tool.return_value = lambda fn: fn
        return mock_mcp

    def test_read_only_function_gets_readonly_annotation(self):
        @read_only
        def my_tool():
            """A read-only tool."""

        mock_mcp = self._make_mock_mcp()

        with patch("datahub_integrations.mcp.mcp_server.async_background", side_effect=lambda fn: fn):
            _register_tool(mock_mcp, "my_tool", my_tool)

        mock_mcp.tool.assert_called_once()
        _, kwargs = mock_mcp.tool.call_args
        assert kwargs.get("annotations") == {"readOnlyHint": True}

    def test_undecorated_function_has_no_annotations(self):
        def my_tool():
            """A plain tool."""

        mock_mcp = self._make_mock_mcp()

        with patch("datahub_integrations.mcp.mcp_server.async_background", side_effect=lambda fn: fn):
            _register_tool(mock_mcp, "my_tool", my_tool)

        mock_mcp.tool.assert_called_once()
        _, kwargs = mock_mcp.tool.call_args
        assert kwargs.get("annotations") is None
