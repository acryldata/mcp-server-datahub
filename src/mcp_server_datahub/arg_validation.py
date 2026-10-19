"""Parameter types for list arguments that clients may send as a string.

MCP clients and LLMs frequently serialize a list-typed argument as a *string*: either a
JSON-encoded array (``'["a", "b"]'``) or a single bare value (``'a'``). Pydantic rejects
that against a plain ``List[str]`` before the tool body runs.

Widening the annotation to ``List[str] | str`` would accept it, but publishes
``anyOf: [array, string]``. Strict function-calling APIs reject a parameter with no
single declared type: Google Gemini (#98), and several OpenAI-compatible endpoints,
which reject the *whole* tool list with a 422 (#173). So:

- ``StrList`` (the default): coerce the string to a list in a ``BeforeValidator``. The
  schema stays a plain array and the tool body always receives a list.
- ``StrListOrStr``: only for tools whose body must see the original string (e.g.
  ``get_entities`` returns a single dict for a bare URN). The runtime type stays the
  union and ``WithJsonSchema`` publishes a plain array.
"""

from typing import Annotated, List, Optional

from pydantic import BeforeValidator, WithJsonSchema
from pydantic_core import from_json


def coerce_str_list(
    value: Optional[List[str] | str],
) -> Optional[List[str]]:
    """Normalize a list argument that an MCP client may have sent as a string.

    Parsing is delegated to pydantic's own strict JSON parser: a string that parses to a
    JSON array is used as-is, anything else is treated as a single value. Item
    validation stays with the outer ``List[str]``, so this only decides list-vs-scalar.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    try:
        parsed = from_json(stripped)
    except ValueError:
        return [stripped]
    return parsed if isinstance(parsed, list) else [stripped]


StrList = Annotated[List[str], BeforeValidator(coerce_str_list)]
"""A list of strings; a bare or JSON-stringified string is coerced to a list."""

NullableItemStrList = Annotated[List[Optional[str]], BeforeValidator(coerce_str_list)]
"""Like ``StrList``, but items may be null (e.g. ``column_paths``)."""

StrListOrStr = Annotated[
    List[str] | str,
    WithJsonSchema({"type": "array", "items": {"type": "string"}}),
]
"""A list of strings in the schema; a single string reaches the tool body unchanged."""
