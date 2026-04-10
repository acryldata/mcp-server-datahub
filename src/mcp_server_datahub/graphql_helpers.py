"""Shared GraphQL helpers for DataHub MCP tools.

Provides the common infrastructure that all MCP tools depend on:
- Client context management (MCPContext, get_datahub_client)
- GraphQL execution with Cloud/OSS field management
- Response cleaning and truncation for LLM consumption
- Token budget management for large responses
"""

import contextlib
import contextvars
import html
import os
import pathlib
import re
from dataclasses import dataclass, field as dataclass_field
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    TypeVar,
)

import jmespath
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter
from loguru import logger

from ._token_estimator import TokenCountEstimator
from .search_filter_parser import parse_filter_string
from .sub_entity_urls import SubEntityResolver
from .tool_context import ToolContext

GQL_DIR = pathlib.Path(__file__).parent / "gql"

T = TypeVar("T")
DESCRIPTION_LENGTH_HARD_LIMIT = 1000
QUERY_LENGTH_HARD_LIMIT = 5000
DOCUMENT_CONTENT_CHAR_LIMIT = 8000

# Maximum token count for tool responses to prevent context window issues
# As per telemetry tool result length goes upto
TOOL_RESPONSE_TOKEN_LIMIT = int(os.getenv("TOOL_RESPONSE_TOKEN_LIMIT", 80000))

# Per-entity schema token budget for field truncation
# Assumes ~5 entities per response: 80K total / 5 = 16K per entity
ENTITY_SCHEMA_TOKEN_BUDGET = int(os.getenv("ENTITY_SCHEMA_TOKEN_BUDGET", "16000"))


def select_results_within_budget(
    results: Iterator[T],
    fetch_entity: Callable[[T], dict],
    max_results: int = 10,
    token_budget: Optional[int] = None,
) -> Generator[T, None, None]:
    """
    Generator that yields results within token budget.

    Generic helper that works for any result structure. Caller provides a function
    to extract/clean entity for token counting (can mutate the result).

    Yields results until:
    - max_results reached, OR
    - token_budget would be exceeded (and we have at least 1 result)

    Args:
        results: Iterator of result objects of any type T (memory efficient)
        fetch_entity: Function that extracts entity dict from result for token counting.
                   Can mutate the result to clean/update entity in place.
                   Signature: T -> dict (entity for token counting)
                   Example: lambda r: (r.__setitem__("entity", clean(r["entity"])), r["entity"])[1]
        max_results: Maximum number of results to return
        token_budget: Token budget (defaults to 90% of TOOL_RESPONSE_TOKEN_LIMIT)

    Yields:
        Original result objects of type T (possibly mutated by fetch_entity)
    """
    if token_budget is None:
        # Use 90% of limit as safety buffer:
        # - Token estimation is approximate, not exact
        # - Response wrapper adds overhead
        # - Better to return fewer results that fit than exceed limit
        token_budget = int(TOOL_RESPONSE_TOKEN_LIMIT * 0.9)

    total_tokens = 0
    results_count = 0

    # Consume iterator up to max_results
    for i, result in enumerate(results):
        if i >= max_results:
            break
        # Extract (and possibly clean) entity using caller's lambda
        # Note: fetch_entity may mutate result to clean/update entity in place
        entity = fetch_entity(result)

        # Estimate token cost
        entity_tokens = TokenCountEstimator.estimate_dict_tokens(entity)

        # Check if adding this entity would exceed budget
        if total_tokens + entity_tokens > token_budget:
            if results_count == 0:
                # Always yield at least 1 result
                logger.warning(
                    f"First result ({entity_tokens:,} tokens) exceeds budget ({token_budget:,}), "
                    "yielding it anyway"
                )
                yield result  # Yield original result structure
                results_count += 1
                total_tokens += entity_tokens
            else:
                # Have at least 1 result, stop here to stay within budget
                logger.info(
                    f"Stopping at {results_count} results (next would exceed {token_budget:,} token budget)"
                )
                break
        else:
            yield result  # Yield original result structure
            results_count += 1
            total_tokens += entity_tokens

    logger.info(
        f"Selected {results_count} results using {total_tokens:,} tokens "
        f"(budget: {token_budget:,})"
    )


# Backward-compatible alias
_select_results_within_budget = select_results_within_budget


def sanitize_html_content(text: str) -> str:
    """
    Remove HTML tags and decode HTML entities from text.

    Uses a bounded regex pattern to prevent ReDoS (Regular Expression Denial of Service)
    attacks. The pattern limits matching to tags with at most 100 characters between < and >,
    which prevents backtracking on malicious input like "<" followed by millions of characters
    without a closing ">".
    """
    if not text:
        return text

    # Use bounded regex to prevent ReDoS (max 100 chars between < and >)
    text = re.sub(r"<[^<>]{0,100}>", "", text)

    # Decode HTML entities
    text = html.unescape(text)

    return text.strip()


def truncate_with_ellipsis(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to max_length and add suffix if truncated."""
    if not text or len(text) <= max_length:
        return text

    # Account for suffix length
    actual_max = max_length - len(suffix)
    return text[:actual_max] + suffix


def sanitize_markdown_content(text: str) -> str:
    """Remove markdown-style embeds that contain encoded data from text, but preserve alt text."""
    if not text:
        return text

    # Remove markdown embeds with data URLs (base64 encoded content) but preserve alt text
    # Pattern: ![alt text](data:image/type;base64,encoded_data) -> alt text
    text = re.sub(r"!\[([^\]]*)\]\(data:[^)]+\)", r"\1", text)

    return text.strip()


def sanitize_and_truncate_description(text: str, max_length: int) -> str:
    """Sanitize HTML content and truncate to specified length."""
    if not text:
        return text

    try:
        # First sanitize HTML content
        sanitized = sanitize_html_content(text)

        # Then sanitize markdown content (preserving alt text)
        sanitized = sanitize_markdown_content(sanitized)

        # Then truncate if needed
        return truncate_with_ellipsis(sanitized, max_length)
    except Exception as e:
        logger.warning(f"Error sanitizing and truncating description: {e}")
        return text[:max_length] if len(text) > max_length else text


def truncate_descriptions(
    data: dict | list, max_length: int = DESCRIPTION_LENGTH_HARD_LIMIT
) -> None:
    """
    Recursively truncates values of keys named 'description' in a dictionary in place.
    """
    # TODO: path-aware truncate, for different length limits per entity type
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "description" and isinstance(value, str):
                data[key] = sanitize_and_truncate_description(value, max_length)
            elif isinstance(value, (dict, list)):
                truncate_descriptions(value)
    elif isinstance(data, list):
        for item in data:
            truncate_descriptions(item)


def truncate_query(query: str) -> str:
    """
    Truncate a SQL query if it exceeds the maximum length.
    """
    return truncate_with_ellipsis(
        query, QUERY_LENGTH_HARD_LIMIT, suffix="... [truncated]"
    )


def parse_filter_input(filter_str: Optional[str]) -> Optional[Filter]:
    """Parse a SQL-like filter string into a Filter object.

    Returns None for None or blank input, otherwise delegates to parse_filter_string.
    """
    if filter_str is None:
        return None

    stripped = filter_str.strip()
    if not stripped:
        return None

    return parse_filter_string(stripped)


@dataclass
class MCPContext:
    """Per-request context for MCP tool execution."""

    client: DataHubClient
    tool_context: ToolContext = dataclass_field(default_factory=ToolContext)


_mcp_context = contextvars.ContextVar[MCPContext]("_mcp_context")


def get_mcp_context() -> MCPContext:
    """Get the current MCP context. Raises LookupError if not set."""
    return _mcp_context.get()


def get_datahub_client() -> DataHubClient:
    return get_mcp_context().client


def set_datahub_client(
    client: DataHubClient,
    tool_context: ToolContext | None = None,
) -> None:
    _mcp_context.set(
        MCPContext(client=client, tool_context=tool_context or ToolContext())
    )


@contextlib.contextmanager
def with_datahub_client(
    client: DataHubClient,
    tool_context: ToolContext | None = None,
) -> Iterator[None]:
    ctx = MCPContext(client=client, tool_context=tool_context or ToolContext())
    token = _mcp_context.set(ctx)
    try:
        yield
    finally:
        _mcp_context.reset(token)


def _enable_newer_gms_fields(query: str) -> str:
    """
    Enable newer GMS fields by removing the #[NEWER_GMS] marker suffix.

    Converts:
        someField  #[NEWER_GMS]
    To:
        someField
    """
    lines = query.split("\n")
    cleaned_lines = [
        line.replace(" #[NEWER_GMS]", "").replace("\t#[NEWER_GMS]", "")
        for line in lines
    ]
    return "\n".join(cleaned_lines)


def _disable_newer_gms_fields(query: str) -> str:
    """
    Disable newer GMS fields by commenting out lines with #[NEWER_GMS] marker.

    Converts:
        someField  #[NEWER_GMS]
    To:
        # someField  #[NEWER_GMS]
    """
    lines = query.split("\n")
    processed_lines = []
    for line in lines:
        if "#[NEWER_GMS]" in line:
            # Comment out the line by prefixing with #
            processed_lines.append("# " + line)
        else:
            processed_lines.append(line)
    return "\n".join(processed_lines)


def _enable_cloud_fields(query: str) -> str:
    """
    Enable cloud fields by removing the #[CLOUD] marker suffix.

    Converts:
        someField  #[CLOUD]
    To:
        someField
    """
    lines = query.split("\n")
    cleaned_lines = [
        line.replace(" #[CLOUD]", "").replace("\t#[CLOUD]", "") for line in lines
    ]
    return "\n".join(cleaned_lines)


def _disable_cloud_fields(query: str) -> str:
    """
    Disable cloud fields by commenting out lines with #[CLOUD] marker.

    Converts:
        someField  #[CLOUD]
    To:
        # someField  #[CLOUD]
    """
    lines = query.split("\n")
    processed_lines = []
    for line in lines:
        if "#[CLOUD]" in line:
            # Comment out the line by prefixing with #
            processed_lines.append("# " + line)
        else:
            processed_lines.append(line)
    return "\n".join(processed_lines)


# Cache to track whether newer GMS fields are supported for each graph instance
# Key: id(graph), Value: bool indicating if newer GMS fields are supported
_newer_gms_fields_support_cache: dict[int, bool] = {}


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    """Check if the graph instance is DataHub Cloud.

    Cloud instances typically have newer GMS versions with additional fields.
    This heuristic uses the presence of frontend_base_url to detect Cloud instances.
    """
    # Allow disabling newer GMS field detection via environment variable
    # This is useful when the GMS version doesn't support all newer fields
    if get_boolean_env_variable("DISABLE_NEWER_GMS_FIELD_DETECTION", default=False):
        logger.debug(
            "Newer GMS field detection disabled via DISABLE_NEWER_GMS_FIELD_DETECTION"
        )
        return False

    try:
        # Only DataHub Cloud has a frontend base url.
        # Cloud instances typically run newer GMS versions with additional fields.
        _ = graph.frontend_base_url
    except ValueError:
        return False
    return True


def _is_field_validation_error(error_msg: str) -> bool:
    """Check if the error is a GraphQL field/type validation or syntax error.

    Includes InvalidSyntax because unknown types (like Document on older GMS)
    cause syntax errors rather than validation errors.
    """
    return (
        "FieldUndefined" in error_msg
        or "ValidationError" in error_msg
        or "InvalidSyntax" in error_msg
    )


def execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    graph_id = id(graph)
    original_query = query  # Keep original for fallback

    # Detect if this is a DataHub Cloud instance
    is_cloud = _is_datahub_cloud(graph)

    # Process CLOUD tags
    if is_cloud:
        query = _enable_cloud_fields(query)
    else:
        query = _disable_cloud_fields(query)

    # Process NEWER_GMS tags
    # Check if we've already determined newer GMS fields support for this graph
    newer_gms_enabled_for_this_query = False
    if graph_id in _newer_gms_fields_support_cache:
        supports_newer_fields = _newer_gms_fields_support_cache[graph_id]
        if supports_newer_fields:
            query = _enable_newer_gms_fields(query)
            newer_gms_enabled_for_this_query = True
        else:
            query = _disable_newer_gms_fields(query)
    else:
        # First attempt: try with newer GMS fields if it's detected as cloud
        # (Cloud instances typically run newer GMS versions)
        if is_cloud:
            query = _enable_newer_gms_fields(query)
            newer_gms_enabled_for_this_query = True
        else:
            query = _disable_newer_gms_fields(query)
        # Cache the initial detection result
        _newer_gms_fields_support_cache[graph_id] = is_cloud

    logger.debug(
        f"Executing GraphQL {operation_name or 'query'}: "
        f"is_cloud={is_cloud}, newer_gms_enabled={newer_gms_enabled_for_this_query}"
    )
    logger.debug(
        f"GraphQL query for {operation_name or 'query'}:\n{query}\nVariables: {variables}"
    )

    try:
        # Execute the GraphQL query
        result = graph.execute_graphql(
            query=query, variables=variables, operation_name=operation_name
        )
        return result

    except Exception as e:
        error_msg = str(e)

        # Check if this is a field validation error and we tried with newer GMS fields enabled
        # Only retry if we had newer GMS fields enabled in the query that just failed
        if _is_field_validation_error(error_msg) and newer_gms_enabled_for_this_query:
            logger.warning(
                f"GraphQL schema validation error detected for {operation_name or 'query'}. "
                f"Retrying without newer GMS fields as fallback."
            )
            logger.exception(e)

            # Update cache to indicate newer GMS fields are NOT supported
            _newer_gms_fields_support_cache[graph_id] = False

            # Retry with newer GMS fields disabled - process both tags again
            try:
                fallback_query = original_query
                # Reprocess CLOUD tags
                if is_cloud:
                    fallback_query = _enable_cloud_fields(fallback_query)
                else:
                    fallback_query = _disable_cloud_fields(fallback_query)
                # Disable newer GMS fields for fallback
                fallback_query = _disable_newer_gms_fields(fallback_query)

                logger.debug(
                    f"Retry {operation_name or 'query'} with NEWER_GMS fields disabled: "
                    f"is_cloud={is_cloud}"
                )

                result = graph.execute_graphql(
                    query=fallback_query,
                    variables=variables,
                    operation_name=operation_name,
                )
                logger.info(
                    f"Fallback query succeeded without newer GMS fields for operation: {operation_name}"
                )
                return result
            except Exception as fallback_error:
                logger.exception(
                    f"Fallback query also failed for {operation_name or 'query'}: {fallback_error}"
                )
                raise fallback_error
        elif (
            _is_field_validation_error(error_msg)
            and not newer_gms_enabled_for_this_query
        ):
            # Field validation error but NEWER_GMS fields were already disabled
            logger.error(
                f"GraphQL schema validation error for {operation_name or 'query'} "
                f"but NEWER_GMS fields were already disabled (is_cloud={is_cloud}). "
                f"This may indicate a CLOUD-only field being used on a non-cloud instance, "
                f"or a field that's unavailable in this GMS version."
            )
            logger.exception(e)

        # Keep essential error logging for troubleshooting with full stack trace
        logger.exception(
            f"GraphQL {operation_name or 'query'} failed: {e}\n"
            f"Cloud instance: {is_cloud}\n"
            f"Newer GMS fields enabled: {_newer_gms_fields_support_cache.get(graph_id, 'unknown')}\n"
            f"Variables: {variables}"
        )
        raise


def inject_urls_for_urns(
    graph: DataHubGraph,
    response: Any,
    json_paths: List[str],
) -> None:
    if not _is_datahub_cloud(graph):
        return

    # _is_datahub_cloud already confirmed this attribute exists.
    frontend_base_url: str = graph.frontend_base_url
    # SubEntityResolver is lightweight (two attribute assignments, no allocations);
    # the actual cache lives at module level in sub_entity_urls.
    resolver = SubEntityResolver(graph)

    for path in json_paths:
        for item in jmespath.search(path, response) if path else [response]:
            if isinstance(item, dict) and item.get("urn"):
                urn = item["urn"]
                url = resolver.url_for_urn(frontend_base_url, urn)
                # Update item in place with url, ensuring that urn and url are first.
                # Omit url entirely when resolution fails (e.g. sub-entity with no parent)
                # so downstream consumers don't see url: null.
                new_item: dict = {"urn": urn}
                if url is not None:
                    new_item["url"] = url
                new_item.update({k: v for k, v in item.items() if k != "urn"})
                item.clear()
                item.update(new_item)


def maybe_convert_to_schema_field_urn(urn: str, column: Optional[str]) -> str:
    if column:
        maybe_dataset_urn = Urn.from_string(urn)
        if not isinstance(maybe_dataset_urn, DatasetUrn):
            raise ValueError(
                f"Input urn should be a dataset urn if column is provided, but got {urn}."
            )
        urn = str(SchemaFieldUrn(maybe_dataset_urn, column))
    return urn


def clean_gql_response(response: Any) -> Any:
    """
    Clean GraphQL response by removing metadata and empty values.

    Recursively removes:
    - __typename fields (GraphQL metadata not useful for consumers)
    - None values
    - Empty arrays []
    - Empty dicts {} (after cleaning)
    - Base64-encoded images from description fields (can be huge - 2MB!)

    Args:
        response: Raw GraphQL response (dict, list, or primitive)

    Returns:
        Cleaned response with same structure but without noise
    """
    if isinstance(response, dict):
        banned_keys = {
            "__typename",
        }

        cleaned_response = {}
        for k, v in response.items():
            if k in banned_keys or v is None or v == []:
                continue
            cleaned_v = clean_gql_response(v)
            # Strip base64 images from description fields
            if (
                k == "description"
                and isinstance(cleaned_v, str)
                and "base64" in cleaned_v
            ):
                import re

                cleaned_v = re.sub(
                    r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+",
                    "[image removed]",
                    cleaned_v,
                )
                cleaned_v = re.sub(
                    r"!\[[^\]]*\]\(data:image/[^)]+\)", "[image removed]", cleaned_v
                )

            if cleaned_v is not None and cleaned_v != {}:
                cleaned_response[k] = cleaned_v

        return cleaned_response
    elif isinstance(response, list):
        return [clean_gql_response(item) for item in response]
    else:
        return response


def _sort_fields_by_priority(fields: List[dict]) -> Iterator[dict]:
    """
    Yield schema fields sorted by priority for deterministic truncation.

    Priority order:
    1. Primary/partition keys (isPartOfKey, isPartitioningKey)
    2. Fields with descriptions
    3. Fields with tags or glossary terms
    4. Alphabetically by fieldPath

    Each field gets a score tuple for sorting:
    - key_score: 2 if isPartOfKey, 1 if isPartitioningKey, 0 otherwise
    - has_description: 1 if description exists, 0 otherwise
    - has_tags: 1 if tags or glossary terms exist, 0 otherwise
    - fieldPath: for alphabetical tiebreaker

    Sorted in descending order by score components, then ascending by fieldPath.

    Args:
        fields: List of field dicts from GraphQL response

    Yields:
        Fields in priority order (generator for memory efficiency)
    """
    # Score each field with tuple: (key_score, has_description, has_tags, fieldPath, index)
    scored_fields = []
    for idx, field in enumerate(fields):
        # Score key fields (highest priority)
        key_score = 0
        if field.get("isPartOfKey"):
            key_score = 2
        elif field.get("isPartitioningKey"):
            key_score = 1

        # Score fields with descriptions
        has_description = 1 if field.get("description") else 0

        # Score fields with tags or glossary terms
        has_tags_or_terms = 0
        if field.get("tags") or field.get("glossaryTerms"):
            has_tags_or_terms = 1

        # Get fieldPath for alphabetical sorting (tiebreaker)
        field_path = field.get("fieldPath", "")

        # Store as (score_tuple, original_index, field)
        # Sort descending by scores, ascending by fieldPath
        score_tuple = (-key_score, -has_description, -has_tags_or_terms, field_path)
        scored_fields.append((score_tuple, idx, field))

    # Sort by score tuple
    scored_fields.sort(key=lambda x: x[0])

    # Yield fields in sorted order
    for _, _, field in scored_fields:
        yield field


def _clean_schema_fields(
    sorted_fields: Iterator[dict], editable_map: dict[str, dict]
) -> Iterator[dict]:
    """
    Clean and normalize schema fields for response.

    Yields cleaned field dicts with only essential properties for SQL generation
    and understanding schema structure. Merges user-edited metadata (descriptions,
    tags, glossary terms) into fields with "edited*" prefix when they differ.

    Note: All fields are expected to have fieldPath (always requested in GraphQL).
    If fieldPath is missing, it indicates a data quality issue.

    Args:
        sorted_fields: Iterator of fields in priority order
        editable_map: Map of fieldPath -> editable field data for merging

    Yields:
        Cleaned field dicts with merged editable data (generator for memory efficiency)
    """
    for f in sorted_fields:
        # fieldPath is required - it's always requested in GraphQL and is essential
        # for identifying the field. If missing, fail fast rather than silently skipping.
        field_dict = {"fieldPath": f["fieldPath"]}

        # Add type if present (essential for SQL)
        if field_type := f.get("type"):
            field_dict["type"] = field_type

        # Add nativeDataType if present (important for SQL type casting)
        if native_type := f.get("nativeDataType"):
            field_dict["nativeDataType"] = native_type

        # Add description if present (truncated)
        if description := f.get("description"):
            field_dict["description"] = description[:120]

        # Add nullable if present (important for SQL NULL handling)
        if f.get("nullable") is not None:
            field_dict["nullable"] = f.get("nullable")

        # Add label if present (useful for human-readable names)
        if label := f.get("label"):
            field_dict["label"] = label

        # Add isPartOfKey only if truthy (important for joins)
        if f.get("isPartOfKey"):
            field_dict["isPartOfKey"] = True

        # Add isPartitioningKey only if truthy (important for query optimization)
        if f.get("isPartitioningKey"):
            field_dict["isPartitioningKey"] = True

        # Add recursive only if truthy
        if f.get("recursive"):
            field_dict["recursive"] = True

        # Add deprecation status if present (warn about deprecated fields)
        if schema_field_entity := f.get("schemaFieldEntity"):
            if deprecation := schema_field_entity.get("deprecation"):
                if deprecation.get("deprecated"):
                    field_dict["deprecated"] = {
                        "deprecated": True,
                        "note": deprecation.get("note", "")[:120],  # Truncate note
                    }

        # Add tags if present (keep minimal info for classification context)
        if tags := f.get("tags"):
            if tag_list := tags.get("tags"):
                # Keep just tag names for context
                field_dict["tags"] = [
                    t["tag"]["properties"]["name"]
                    for t in tag_list
                    if t.get("tag", {}).get("properties")
                    and t["tag"]["properties"].get("name")
                ]

        # Add glossary terms if present (keep minimal info for business context)
        if glossary_terms := f.get("glossaryTerms"):
            if terms_list := glossary_terms.get("terms"):
                # Keep just term names for context
                field_dict["glossaryTerms"] = [
                    t["term"]["properties"]["name"]
                    for t in terms_list
                    if t.get("term", {}).get("properties")
                    and t["term"]["properties"].get("name")
                ]

        # Merge editable metadata if available for this field
        field_path = f["fieldPath"]
        if editable := editable_map.get(field_path):
            # Add editedDescription if it differs from system description
            if editable_desc := editable.get("description"):
                system_desc = field_dict.get("description", "")
                # Only add if different (token optimization)
                if editable_desc[:120] != system_desc:  # Compare truncated versions
                    field_dict["editedDescription"] = editable_desc[:120]

            # Add editedTags if present and different
            if editable_tags := editable.get("tags"):
                if tag_list := editable_tags.get("tags"):
                    edited_tag_names = [
                        t["tag"]["properties"]["name"]
                        for t in tag_list
                        if t.get("tag", {}).get("properties")
                        and t["tag"]["properties"].get("name")
                    ]
                    if edited_tag_names:
                        system_tags = field_dict.get("tags", [])
                        if edited_tag_names != system_tags:
                            field_dict["editedTags"] = edited_tag_names

            # Add editedGlossaryTerms if present and different
            if editable_terms := editable.get("glossaryTerms"):
                if terms_list := editable_terms.get("terms"):
                    edited_term_names = [
                        t["term"]["properties"]["name"]
                        for t in terms_list
                        if t.get("term", {}).get("properties")
                        and t["term"]["properties"].get("name")
                    ]
                    if edited_term_names:
                        system_terms = field_dict.get("glossaryTerms", [])
                        if edited_term_names != system_terms:
                            field_dict["editedGlossaryTerms"] = edited_term_names

        yield field_dict


def clean_get_entities_response(
    raw_response: dict,
    *,
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> dict:
    """
    Clean and optimize entity responses for LLM consumption.

    Performs several transformations to reduce token usage while preserving essential information:

    1. **Clean GraphQL artifacts**: Removes __typename, null values, empty objects/arrays
       (via clean_gql_response)

    2. **Schema field processing** (if schemaMetadata.fields exists):
       - Sorts fields using sort_fn (defaults to _sort_fields_by_priority)
       - Cleans each field to keep only essential properties (fieldPath, type, description, etc.)
       - Merges editableSchemaMetadata into fields with "edited*" prefix (editedDescription,
         editedTags, editedGlossaryTerms) - only included when they differ from system values
       - Applies pagination (offset/limit) with token budget constraint
       - Field selection stops when EITHER limit is reached OR ENTITY_SCHEMA_TOKEN_BUDGET is exceeded
       - Adds schemaFieldsTruncated metadata when fields are cut

    3. **Remove duplicates**: Deletes editableSchemaMetadata after merging into schemaMetadata

    4. **Truncate view definitions**: Limits SQL view logic to QUERY_LENGTH_HARD_LIMIT

    The result is optimized for LLM tool responses: reduced token usage, no duplication,
    clear distinction between system-generated and user-curated content.

    Args:
        raw_response: Raw entity dict from GraphQL query
        sort_fn: Optional custom function to sort fields. If None, uses _sort_fields_by_priority.
                 Should take a list of field dicts and return an iterator of sorted fields.
        offset: Number of fields to skip after sorting (default: 0)
        limit: Maximum number of fields to include after offset (default: None = unlimited)

    Returns:
        Cleaned entity dict optimized for LLM consumption
    """
    response = clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Clean schemaMetadata.fields to keep important fields while reducing size
        # Keep fields essential for SQL generation and understanding schema structure
        if fields := schema_metadata.get("fields"):
            total_fields = len(fields)  # Use original count before any filtering

            # Build editable map from editableSchemaMetadata for merging
            # Make this safe - if duplicate fieldPaths exist, last one wins (no failure)
            editable_map = {}
            if editable_schema := response.get("editableSchemaMetadata"):
                if editable_fields := editable_schema.get("editableSchemaFieldInfo"):
                    for editable_field in editable_fields:
                        if field_path := editable_field.get("fieldPath"):
                            editable_map[field_path] = editable_field

            # Sort fields using custom function or default priority sorting
            sort_function = sort_fn if sort_fn is not None else _sort_fields_by_priority
            sorted_fields = sort_function(fields)
            cleaned_fields = _clean_schema_fields(sorted_fields, editable_map)

            # Apply offset, limit, and token budget to select fields
            selected_fields: list[dict] = []
            accumulated_tokens = 0
            fields_remaining = limit  # None means unlimited

            for idx, field in enumerate(cleaned_fields):
                # Skip fields before offset
                if idx < offset:
                    continue

                field_tokens = TokenCountEstimator.estimate_dict_tokens(field)

                # Stop if we exceed token budget (keep at least 1 field after offset)
                if (
                    accumulated_tokens + field_tokens > ENTITY_SCHEMA_TOKEN_BUDGET
                    and selected_fields
                ):
                    logger.info(
                        f"Truncating schema fields: {len(selected_fields)}/{total_fields - offset} "
                        f"fields fit in {ENTITY_SCHEMA_TOKEN_BUDGET:,} token budget "
                        f"(accumulated {accumulated_tokens:,} tokens, offset={offset})"
                    )
                    break

                # Stop if we've hit the limit
                if fields_remaining is not None and fields_remaining <= 0:
                    logger.info(
                        f"Reached limit: {len(selected_fields)} fields selected (limit={limit}, offset={offset})"
                    )
                    break

                selected_fields.append(field)
                accumulated_tokens += field_tokens
                if fields_remaining is not None:
                    fields_remaining -= 1

            # Add truncation metadata if fields were cut
            # Truncation occurs if we have fewer fields than (total - offset)
            fields_after_offset = total_fields - offset
            if len(selected_fields) < fields_after_offset:
                schema_metadata["schemaFieldsTruncated"] = {
                    "totalFields": total_fields,
                    "includedFields": len(selected_fields),
                    "offset": offset,
                }
                logger.warning(
                    f"Schema fields truncated: included {len(selected_fields)}/{fields_after_offset} fields "
                    f"(offset={offset}, {accumulated_tokens:,} tokens, budget: {ENTITY_SCHEMA_TOKEN_BUDGET:,})"
                )

            schema_metadata["fields"] = selected_fields

    # Remove editableSchemaMetadata - data has been merged into schemaMetadata fields
    if "editableSchemaMetadata" in response:
        del response["editableSchemaMetadata"]

    # Truncate long view definition to prevent context window issues
    if response and (view_properties := response.get("viewProperties")):
        if view_properties.get("logic"):
            view_properties["logic"] = truncate_query(view_properties["logic"])

    # Truncate document content to prevent context window issues
    if response and (info := response.get("info")):
        if contents := info.get("contents"):
            if text := contents.get("text"):
                if len(text) > DOCUMENT_CONTENT_CHAR_LIMIT:
                    original_length = len(text)
                    truncate_at = DOCUMENT_CONTENT_CHAR_LIMIT
                    contents["text"] = (
                        text[:truncate_at]
                        + "\n\n[Content truncated. Use grep_documents(start_offset={}) to continue.]".format(
                            truncate_at
                        )
                    )
                    contents["_truncated"] = True
                    contents["_originalLengthChars"] = original_length
                    contents["_truncatedAtChar"] = truncate_at
                    logger.info(
                        f"Document content truncated: {original_length:,} -> {truncate_at:,} chars"
                    )

    return response


def clean_related_documents_response(raw_response: dict) -> dict:
    """
    Clean and optimize related documents response for LLM consumption.

    Applies basic GraphQL cleaning to remove __typename, null values, empty objects/arrays.
    This is a simpler version of clean_get_entities_response focused on related documents.

    Args:
        raw_response: Raw related documents dict from GraphQL query (RelatedDocumentsResult)

    Returns:
        Cleaned related documents dict optimized for LLM consumption
    """
    return clean_gql_response(raw_response)
