"""No tool publishes a parameter schema that is a union of different JSON types.

Several OpenAI-compatible inference endpoints reject the whole tool list when one
parameter is, e.g., ``anyOf: [{"type": "array"}, {"type": "string"}]``, so every request
fails with a 422 before any tool runs (acryldata/mcp-server-datahub#173). Gemini accepts
only a single ``type`` (#98), so the array form ``{"type": ["string", "number"]}`` is
rejected too. The nullable idiom (``anyOf: [T, {"type": "null"}]`` or
``{"type": [T, "null"]}``) is widely accepted and is allowed here.

The invariants are asserted over every registered tool rather than a list of names, so
a union or an uncoerced list parameter added later fails here. The rest check that
narrowing the schema did not narrow what the tools accept at runtime.
"""

import inspect
import json
import os
import typing
from typing import Any, Iterator, Optional
from unittest.mock import patch

import pytest
from fastmcp import FastMCP
from pydantic import TypeAdapter, ValidationError

from datahub_integrations.mcp.arg_validation import (
    OptionalStrList,
    StrList,
    StrListOrStr,
    coerce_str_list,
)

from datahub_integrations.mcp.fastmcp_helpers import list_mcp_tools_sync
from datahub_integrations.mcp.mcp_server import (
    register_data_quality_tools,
    register_mutation_tools,
    register_search_tools,
    register_user_tools,
)
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

# List parameters whose tool body must see a bare string as-is, so they use
# StrListOrStr instead of StrList. get_entities returns a single dict for a bare URN;
# get_aspect_history runs its own bounded normalization.
PASSTHROUGH_LIST_PARAMS = {
    ("get_entities", "urns"),
    ("get_aspect_history", "urns"),
    ("get_aspect_history", "aspect_names"),
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
    """Yield the path of every anyOf/oneOf whose non-null branches differ in type, and of
    every ``type`` array naming more than one non-null type."""
    if isinstance(node, dict):
        declared = node.get("type")
        if isinstance(declared, list) and len(set(declared) - {"null"}) > 1:
            yield f"{path}.type: {json.dumps(declared)}"
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


def test_every_list_parameter_accepts_a_string() -> None:
    """Clients often send a list as a bare or JSON-stringified string; none is rejected."""
    checked = 0
    for tool in list_mcp_tools_sync(_full_server()):
        hints = typing.get_type_hints(inspect.unwrap(tool.fn), include_extras=True)
        for name, schema in tool.parameters.get("properties", {}).items():
            if not any(b.get("type") == "array" for b in schema.get("anyOf", [schema])):
                continue
            checked += 1
            adapter = TypeAdapter(hints[name])
            if (tool.name, name) in PASSTHROUGH_LIST_PARAMS:
                assert adapter.validate_python("a") == "a"
            else:
                assert adapter.validate_python("a") == ["a"], f"{tool.name}.{name}"
                assert adapter.validate_python('["a", "b"]') == ["a", "b"], (
                    f"{tool.name}.{name}"
                )

    assert checked > len(PASSTHROUGH_LIST_PARAMS), "no list parameters found"


@pytest.mark.parametrize(
    ("sent", "expected"),
    [
        ("a", ["a"]),
        ("  urn:li:tag:pii  ", ["urn:li:tag:pii"]),
        ('["a", "b"]', ["a", "b"]),
        (' ["a"] ', ["a"]),
        # A JSON value that is not an array is a single value, kept verbatim.
        ('"a"', ['"a"']),
        # Malformed JSON is not repaired; it is treated as a single value.
        ('["a",', ['["a",']),
        # "No value" spellings: omitted for an optional param, rejected for a required one.
        ("", None),
        ("   ", None),
        ("null", None),
        (" null ", None),
        ("[]", []),
        (["a", "b"], ["a", "b"]),
        (None, None),
    ],
)
def test_coerce_str_list(sent: Any, expected: Optional[list]) -> None:
    assert coerce_str_list(sent) == expected


def test_null_string_is_omitted_for_optional_and_rejected_for_required() -> None:
    optional = TypeAdapter(OptionalStrList)
    required = TypeAdapter(StrList)

    assert optional.validate_python("null") is None
    assert optional.validate_python("") is None
    with pytest.raises(ValidationError):
        required.validate_python("null")
    with pytest.raises(ValidationError):
        required.validate_python("")


def test_str_list_or_str_publishes_an_array_but_passes_a_string_through() -> None:
    adapter = TypeAdapter(StrListOrStr)

    assert adapter.json_schema() == {"type": "array", "items": {"type": "string"}}
    # get_entities returns a single dict for a bare URN, so the string must reach the
    # tool body unchanged rather than be wrapped into a list.
    assert adapter.validate_python("urn:li:corpuser:a") == "urn:li:corpuser:a"
    assert adapter.validate_python(["urn:li:corpuser:a"]) == ["urn:li:corpuser:a"]


def test_structured_property_value_keeps_integers_as_integers() -> None:
    adapter = TypeAdapter(StructuredPropertyValue)

    assert adapter.json_schema() == {"type": "string"}
    assert type(adapter.validate_python(90)) is int
    assert type(adapter.validate_python(0.95)) is float
    assert adapter.validate_python("HIGH") == "HIGH"
