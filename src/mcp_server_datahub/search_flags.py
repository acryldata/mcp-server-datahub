import copy
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping


@dataclass(frozen=True)
class SearchFlagsOverride:
    """GraphQL ``searchFlags`` merged over a query's defaults for one request.

    Injected through the request's ToolContext, the same way ViewPreference is,
    so whoever runs the tools can change how DataHub executes a query without
    the MCP tool schema growing a parameter for it.

    Each value replaces the default of the same name and every other default is
    kept. ``SearchFlagsOverride({"skipCache": True})`` asks DataHub to bypass its
    lineage search cache, for a caller about to act on what it reads.

    Honored today by ``get_lineage`` and ``get_lineage_paths_between``. The
    other search tools set their own ``searchFlags`` and ignore it. Keys go to
    DataHub as given, so pass only flags the server supports.

    The flags are deep-copied on construction and on every ``apply``, so
    neither the caller's dict nor a consumer of the merged result can change
    what a later request sends. Treat ``flags`` as read-only: build a new
    override rather than changing one in place.

    Example:
        ctx = ToolContext([SearchFlagsOverride({"skipCache": True})])
        with with_datahub_client(client, tool_context=ctx):
            get_lineage(urn=urn, upstream=False)
    """

    flags: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "flags", copy.deepcopy(dict(self.flags)))

    def apply(self, defaults: Mapping[str, Any]) -> Dict[str, Any]:
        """Return a new dict of the defaults with these flags merged over them."""
        return {**defaults, **copy.deepcopy(dict(self.flags))}
