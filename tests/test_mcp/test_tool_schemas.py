"""No tool publishes a parameter schema that is a union of different JSON types.

Several OpenAI-compatible inference endpoints reject the whole tool list when one
parameter is, e.g., ``anyOf: [{"type": "array"}, {"type": "string"}]``, so every request
fails with a 422 before any tool runs (acryldata/mcp-server-datahub#173). The nullable
idiom ``anyOf: [T, {"type": "null"}]`` is widely accepted and is allowed here.

The first test asserts the invariant over every registered tool rather than a list of
names, so a union added later fails here. The rest check that narrowing the schema did
not narrow what the tools accept at runtime.
"""

import json
import os
from typing import Any, Iterator
from unittest.mock import patch

from fastmcp import FastMCP
from pydantic import TypeAdapter

from datahub_integrations.mcp.fastmcp_helpers import list_mcp_tools_sync
from datahub_integrations.mcp.mcp_server import (
    register_data_quality_tools,
    register_mutation_tools,
    register_search_tools,
    register_user_tools,
)
from datahub_integrations.mcp.tools.entities import StrListArg
from datahub_integrations.mcp.tools.structured_properties import (
    StructuredPropertyValue,
)

ALL_TOOL_GATES = {
    "SEMANTIC_SEARCH_ENABLED": "true",
    "TOOLS_IS_MUTATION_ENABLED": "true",
    "TOOLS_IS_USER_ENABLED": "true",
    "DATA_QUALITY_TOOLS_ENABLED": "true",
    "SAVE_DOCUMENT_TOOL_ENABLED": "true",
}


def _full_server() -> FastMCP:
    """A server carrying every tool, with every gate opened."""
    mcp: FastMCP = FastMCP[None](name="test-schemas")
    with patch.dict(os.environ, ALL_TOOL_GATES):
        for register in (
            register_search_tools,
            register_mutation_tools,
            register_user_tools,
            register_data_quality_tools,
        ):
            register(mcp, is_oss=True)
    return mcp


def _multi_type_unions(node: Any, path: str) -> Iterator[str]:
    """Yield the path of every anyOf/oneOf whose non-null branches differ in type."""
    if isinstance(node, dict):
        for key in ("anyOf", "oneOf"):
            branches = node.get(key)
            if isinstance(branches, list):
                types = {
                    json.dumps(branch.get("type"))
                    for branch in branches
                    if isinstance(branch, dict) and branch.get("type") != "null"
                }
                if len(types) > 1:
                    yield f"{path}.{key}: {json.dumps(branches)}"
        for key, value in node.items():
            yield from _multi_type_unions(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from _multi_type_unions(value, f"{path}[{i}]")


def test_no_tool_parameter_is_a_multi_type_union() -> None:
    tools = list_mcp_tools_sync(_full_server())

    assert tools, "no tools registered; the gates did not open"

    offenders = [
        offender
        for tool in tools
        for offender in _multi_type_unions(tool.parameters, tool.name)
    ]
    assert offenders == [], "multi-type unions in tool schemas:\n" + "\n".join(
        offenders
    )


def test_str_list_arg_publishes_an_array_but_still_accepts_a_string() -> None:
    adapter = TypeAdapter(StrListArg)

    assert adapter.json_schema() == {"type": "array", "items": {"type": "string"}}
    # get_entities returns a single dict for a bare URN, so the string must reach the
    # tool body unchanged rather than be wrapped into a list.
    assert adapter.validate_python("urn:li:corpuser:a") == "urn:li:corpuser:a"
    assert adapter.validate_python(["urn:li:corpuser:a"]) == ["urn:li:corpuser:a"]


def test_structured_property_value_keeps_integers_as_integers() -> None:
    adapter = TypeAdapter(StructuredPropertyValue)

    assert adapter.json_schema() == {"type": ["string", "number"]}
    assert type(adapter.validate_python(90)) is int
    assert type(adapter.validate_python(0.95)) is float
    assert adapter.validate_python("HIGH") == "HIGH"
