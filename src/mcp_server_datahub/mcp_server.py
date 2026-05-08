"""DataHub MCP Server Implementation.

IMPORTANT: This file is kept in sync between two repositories.

When making changes, ensure both versions remain identical. Use relative imports
(e.g., `from ._token_estimator import ...`) instead of absolute imports to maintain
compatibility across both repositories.
"""

import functools
import inspect
import string
import threading
from enum import Enum
from typing import (
    Awaitable,
    Callable,
    List,
    Optional,
    ParamSpec,
    TypeVar,
)

import asyncer
from datahub.cli.env_utils import get_boolean_env_variable
from fastmcp import FastMCP
from fastmcp.tools import Tool as FastMCPTool
from loguru import logger

from .fastmcp_helpers import list_mcp_tools_sync

# IMPORTANT: Use relative imports to maintain compatibility across repositories
from .graphql_helpers import (  # noqa: F401 (re-exported for backward compat)
    DESCRIPTION_LENGTH_HARD_LIMIT,
    DOCUMENT_CONTENT_CHAR_LIMIT,
    ENTITY_SCHEMA_TOKEN_BUDGET,
    QUERY_LENGTH_HARD_LIMIT,
    TOOL_RESPONSE_TOKEN_LIMIT,
    MCPContext,
    _clean_schema_fields,
    _disable_cloud_fields,
    _disable_newer_gms_fields,
    _enable_cloud_fields,
    _enable_newer_gms_fields,
    _is_datahub_cloud,
    _select_results_within_budget,
    _sort_fields_by_priority,
    clean_get_entities_response,
    clean_gql_response,
    clean_related_documents_response,
    execute_graphql,
    get_datahub_client,
    get_mcp_context,
    inject_urls_for_urns,
    maybe_convert_to_schema_field_urn,
    sanitize_and_truncate_description,
    sanitize_html_content,
    sanitize_markdown_content,
    select_results_within_budget,
    set_datahub_client,
    truncate_descriptions,
    truncate_query,
    truncate_with_ellipsis,
    with_datahub_client,
)
from .search_filter_parser import FILTER_DOCS
from .tools.assertions import get_dataset_assertions
from .tools.dataset_queries import get_dataset_queries
from .tools.descriptions import update_description
from .tools.documents import grep_documents, search_documents
from .tools.domains import remove_domains, set_domains
from .tools.entities import get_entities, list_schema_fields
from .tools.get_me import get_me
from .tools.lineage import (  # noqa: F401 (re-exported for backward compat)
    AssetLineageAPI,
    AssetLineageDirective,
    _extract_lineage_columns_from_paths,
    _find_lineage_path,
    _find_result_with_target_urn,
    _find_upstream_lineage_path,
    get_lineage,
    get_lineage_paths_between,
)
from .tools.owners import add_owners, remove_owners
from .tools.save_document import is_save_document_enabled, save_document
from .tools.search import (  # noqa: F401 (re-exported for backward compat)
    _search_implementation,
    enhanced_search,
    search,
)
from .tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from .tools.tags import add_tags, remove_tags
from .tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)
from .version_requirements import TOOL_VERSION_REQUIREMENTS
from .view_helpers import (  # noqa: F401 (re-exported for backward compat)
    DISABLE_DEFAULT_VIEW,
    VIEW_CACHE_TTL_SECONDS,
    _user_view_cache,
    fetch_global_default_view,
    fetch_user_default_view,
)

_P = ParamSpec("_P")
_R = TypeVar("_R")


class ToolType(Enum):
    """Tool type enumeration for different tool types."""

    SEARCH = "search"  # Datahub search tools
    MUTATION = "mutation"  # Datahub mutation tools
    USER = "user"  # Datahub user tools
    DEFAULT = "default"  # Fallback tag


DATA_QUALITY_TOOLS = [
    (
        "get_dataset_assertions",
        get_dataset_assertions,
        {ToolType.SEARCH.value},
    ),
]


# See https://github.com/jlowin/fastmcp/issues/864#issuecomment-3103678258
# for why we need to wrap sync functions with asyncify.
def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await asyncer.asyncify(fn)(*args, **kwargs)
        except Exception:
            # Log with full stack trace before FastMCP catches it
            logger.exception(
                f"Tool function {fn.__name__} failed with args={args}, kwargs={kwargs}"
            )
            raise

    return wrapper


def _register_tool(
    mcp_instance: FastMCP,
    name: str,
    fn: Callable,
    *,
    description: Optional[str] = None,
    tags: Optional[set] = None,
) -> None:
    """Register a tool on the MCP instance and capture its version requirement.

    This is a convenience wrapper that:
    1. Wraps the sync function with async_background
    2. Registers it on the MCP instance
    3. Reads the _version_requirement attribute (set by @min_version decorator)
       and populates TOOL_VERSION_REQUIREMENTS
    4. Reads the _read_only_hint attribute (set by @read_only decorator)
       and sets readOnlyHint=True in the tool's MCP annotations

    Args:
        mcp_instance: The FastMCP instance to register on.
        name: The tool name (may differ from fn.__name__).
        fn: The tool function (sync).
        description: Tool description. Defaults to fn.__doc__.
        tags: Optional set of tag strings.
    """
    annotations: Optional[dict] = None
    if getattr(fn, "_read_only_hint", False):
        annotations = {"readOnlyHint": True}

    mcp_instance.tool(
        name=name,
        description=description or fn.__doc__,
        tags=tags,
        annotations=annotations,
    )(async_background(fn))

    req = getattr(fn, "_version_requirement", None)
    if req is not None:
        TOOL_VERSION_REQUIREMENTS[name] = req


mcp = FastMCP[None](
    name="datahub",
)


def _is_semantic_search_enabled() -> bool:
    """Check if semantic search is enabled via environment variable.

    IMPORTANT: Semantic search is an EXPERIMENTAL feature that is ONLY available on
    DataHub Cloud deployments with specific versions and configurations. This feature
    must be explicitly enabled by the DataHub team for your Cloud instance.

    Note:
        This function only checks the environment variable. Actual feature
        availability is validated when the DataHub client is used.
    """
    return get_boolean_env_variable("SEMANTIC_SEARCH_ENABLED", default=False)


# Track if tools have been registered to prevent duplicate registration
_tools_registered = False
_tools_registration_lock = threading.Lock()


def register_mutation_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register mutation tools on an MCP instance.

    This is the core registration logic that can be used by both production code
    (via register_all_tools) and tests (with isolated MCP instances).

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).
    """

    enabled = get_boolean_env_variable("TOOLS_IS_MUTATION_ENABLED")

    logger.info(f"Mutation Tools {'ENABLED' if enabled else 'DISABLED'} MCP Server.")

    if not enabled:
        return

    _register_tool(mcp_instance, "add_tags", add_tags, tags={ToolType.MUTATION.value})
    _register_tool(
        mcp_instance, "remove_tags", remove_tags, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "add_terms", add_glossary_terms, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance,
        "remove_terms",
        remove_glossary_terms,
        tags={ToolType.MUTATION.value},
    )
    _register_tool(
        mcp_instance, "add_owners", add_owners, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "remove_owners", remove_owners, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "set_domains", set_domains, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "remove_domains", remove_domains, tags={ToolType.MUTATION.value}
    )
    _register_tool(mcp_instance, "update_description", update_description)
    _register_tool(mcp_instance, "add_structured_properties", add_structured_properties)
    _register_tool(
        mcp_instance, "remove_structured_properties", remove_structured_properties
    )

    # Register save_document tool (only if enabled via environment variable)
    if is_save_document_enabled():
        logger.info("Save Document ENABLED - registering save_document tool")
        _register_tool(
            mcp_instance, "save_document", save_document, tags={ToolType.MUTATION.value}
        )
    else:
        logger.info("Save Document DISABLED - save_document tool not registered")


def register_user_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register user information tools on an MCP instance.

    This includes tools for fetching authenticated user information.

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions.
                If False, use Cloud descriptions.
    """

    enabled = get_boolean_env_variable("TOOLS_IS_USER_ENABLED")
    logger.info(f"User Tools {'ENABLED' if enabled else 'DISABLED'} MCP Server.")

    if not enabled:
        return

    _register_tool(mcp_instance, "get_me", get_me, tags={ToolType.USER.value})


def register_search_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register search and entity tools on an MCP instance.

    This is the core registration logic that can be used by both production code
    (via register_all_tools) and tests (with isolated MCP instances).

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).
    """
    # Choose sorting documentation based on deployment type
    if not is_oss:
        sorting_docs = """Available sort fields for datasets:
    - queryCountLast30DaysFeature: Number of queries in last 30 days
    - rowCountFeature: Table row count
    - sizeInBytesFeature: Table size in bytes
    - writeCountLast30DaysFeature: Number of writes/updates in last 30 days

    Sorting examples:
    - Most queried datasets:
      search(query="*", filter="entity_type = dataset", sort_by="queryCountLast30DaysFeature", num_results=10)
    - Largest tables:
      search(query="*", filter="entity_type = dataset", sort_by="sizeInBytesFeature", num_results=10)
    - Smallest tables first:
      search(query="*", filter="entity_type = dataset", sort_by="sizeInBytesFeature", sort_order="asc", num_results=10)"""
    else:
        sorting_docs = """Available sort fields:
    - lastOperationTime: Last modified timestamp in source system

    Sorting examples:
    - Most recently updated:
      search(query="*", filter="entity_type = dataset", sort_by="lastOperationTime", sort_order="desc", num_results=10)"""

    # Build full description with interpolated sorting docs using Template
    if search.__doc__ is None:
        raise ValueError("search function must have a docstring")
    search_description = string.Template(search.__doc__).substitute(
        FILTER_DOCS=FILTER_DOCS,
        SORTING_FIELDS_DOCS=sorting_docs,
    )

    # Register search tool
    if _is_semantic_search_enabled():
        # Note: Actual semantic search availability is validated at runtime when used
        # This allows the tool to be registered even if validation would fail,
        # but provides clear error messages when semantic search is actually attempted
        _register_tool(
            mcp_instance, "search", enhanced_search, tags={ToolType.SEARCH.value}
        )
    else:
        # Register original search tool with deployment-specific description
        _register_tool(
            mcp_instance,
            "search",
            search,
            description=search_description,
            tags={ToolType.SEARCH.value},
        )

    _register_tool(
        mcp_instance, "get_lineage", get_lineage, tags={ToolType.SEARCH.value}
    )
    _register_tool(
        mcp_instance,
        "get_dataset_queries",
        get_dataset_queries,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(
        mcp_instance, "get_entities", get_entities, tags={ToolType.SEARCH.value}
    )
    _register_tool(
        mcp_instance,
        "list_schema_fields",
        list_schema_fields,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(
        mcp_instance,
        "get_lineage_paths_between",
        get_lineage_paths_between,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(mcp_instance, "search_documents", search_documents)
    _register_tool(mcp_instance, "grep_documents", grep_documents)


def register_data_quality_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register data quality tools on an MCP instance.

    Gated by the DATA_QUALITY_TOOLS_ENABLED environment variable (default: False).

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: Kept for signature consistency with other register_* functions.
                Not currently used but available for future OSS/Cloud differentiation.
    """
    enabled = get_boolean_env_variable("DATA_QUALITY_TOOLS_ENABLED", default=False)
    logger.info(
        f"Data Quality Tools {'ENABLED' if enabled else 'DISABLED'} MCP Server."
    )

    if not enabled:
        return

    for name, fn, tags in DATA_QUALITY_TOOLS:
        _register_tool(mcp_instance, name, fn, tags=tags)


def register_all_tools(is_oss: bool = False) -> None:
    """Register all MCP tools on the global mcp instance.

    Args:
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).

    Note: Thread-safe. Can be called multiple times from different threads.
          Only the first call will register tools, subsequent calls are no-ops.
    """
    global _tools_registered

    # Thread-safe check-and-set using lock
    with _tools_registration_lock:
        if _tools_registered:
            logger.debug("Tools already registered, skipping duplicate registration")
            return

        _tools_registered = True
        logger.info(f"Registering MCP tools (is_oss={is_oss})")

    # Call the core registration logic on the global mcp instance
    register_search_tools(mcp, is_oss)

    register_mutation_tools(mcp, is_oss)

    register_user_tools(mcp, is_oss)

    register_data_quality_tools(mcp, is_oss)


def get_valid_tools_from_mcp(
    filter_fn: Optional[Callable[[FastMCPTool], bool]] = None,
) -> List[FastMCPTool]:
    """Get valid tools from MCP, optionally filtered.

    Args:
        filter_fn: Optional function to filter tools. Receives a Tool and returns True to include it.

    Returns:
        List of Tool objects that pass the filter (or all tools if no filter provided).

    Example filtering by tag values:
        # Filter tools that have the "mutation" tag
        tools = get_valid_tools_from_mcp(
            filter_fn=lambda tool: "mutation" in (tool.tags or set())
        )

        # Filter tools that have either "search" or "user" tags
        tools = get_valid_tools_from_mcp(
            filter_fn=lambda tool: bool((tool.tags or set()) & {"search", "user"})
        )
    """
    tools = list_mcp_tools_sync(mcp)
    if filter_fn:
        return [tool for tool in tools if filter_fn(tool)]
    return tools
