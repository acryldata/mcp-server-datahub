"""Parameter types that publish a narrower JSON schema than they accept.

Several OpenAI-compatible inference endpoints reject the *whole* tool list when any
parameter's schema is a union of different JSON types, e.g.
``anyOf: [{"type": "array"}, {"type": "string"}]`` (acryldata/mcp-server-datahub#173).
A plain ``List[str] | str`` annotation produces exactly that.

The aliases here keep the lenient runtime type, so a client or LLM that still sends a
bare or JSON-stringified string is accepted as before, but override the published
schema with ``WithJsonSchema`` so it declares a single type.
"""

from typing import Annotated, List

from pydantic import WithJsonSchema

StrListArg = Annotated[
    List[str] | str,
    WithJsonSchema({"type": "array", "items": {"type": "string"}}),
]
"""A list of strings in the schema; a single string is also accepted at runtime."""
