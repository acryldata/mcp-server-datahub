"""Entity retrieval tools for DataHub MCP server."""

import json
from typing import Callable, Iterator, List, Optional, Set

from fastmcp import Context
from fastmcp.exceptions import ToolError
from json_repair import repair_json
from loguru import logger

from .. import graphql_helpers

# ---------------------------------------------------------------------------
# Aspect group filtering
# ---------------------------------------------------------------------------
# DataHub entities share a consistent metadata model: each entity has a set of
# "aspects" (ownership, tags, glossaryTerms, …).  The mapping below lets
# callers request only the aspect groups they need, keeping responses small.
#
# Keys that are NOT mapped to any group (urn, type, url, name, subTypes,
# entity-specific identifiers like dashboardId, flowId, …) are **always
# retained** regardless of the ``include`` selection.
# ---------------------------------------------------------------------------

ASPECT_GROUPS: dict[str, set[str]] = {
    # Core identity — name, description, custom properties.
    # Maps to "properties" / "editableProperties" on most entities,
    # "info" on Assertions & Documents, "definition" on StructuredProperties.
    "properties": {
        "properties",
        "editableProperties",
        "customProperties",
        "info",
        "definition",
    },
    "ownership": {"ownership"},
    "tags": {"tags"},
    "glossary_terms": {"glossaryTerms"},
    "structured_properties": {"structuredProperties"},
    "schema": {"schemaMetadata", "editableSchemaMetadata", "viewProperties"},
    "deprecation": {"deprecation"},
    "platform": {"platform"},
    "domain": {"domain"},
    "data_product": {"dataProduct"},
    "incidents": {"activeIncidents"},
    "assertions": {"assertions", "runEvents"},
    "status": {"status", "health", "statsSummary"},
    "documents": {"relatedDocuments"},
}

# Sentinel: when the caller passes "all", every group is included.
_ALL_GROUP = "all"


def _parse_include(
    include: Optional[List[str]] | str,
) -> Optional[Set[str]]:
    """Normalise the ``include`` parameter into a validated set of group names.

    Returns ``None`` when *all* groups should be included (i.e. ``"all"`` was
    requested or the resulting set covers every group).
    """
    if include is None:
        # Default: properties only — minimal response.
        return {"properties"}

    # Handle JSON-stringified lists from LLMs.
    raw: List[str]
    if isinstance(include, str):
        try:
            parsed = json.loads(repair_json(include.strip()))
            raw = (
                [str(v) for v in parsed] if isinstance(parsed, list) else [str(parsed)]
            )
        except Exception:
            raw = [include.strip()]
    else:
        raw = include

    groups: set[str] = {g.strip().lower() for g in raw}

    # "all" → return everything.
    if _ALL_GROUP in groups:
        return None

    unknown = groups - ASPECT_GROUPS.keys()
    if unknown:
        logger.warning(f"Unknown aspect groups ignored: {unknown}")
        groups -= unknown

    # If every group was requested explicitly, short-circuit.
    if groups == ASPECT_GROUPS.keys():
        return None

    return groups


def _filter_aspects(entity: dict, include_groups: Set[str]) -> dict:
    """Remove aspect keys that belong to non-requested groups.

    Keys not mapped to any group (e.g. urn, type, name, url, subTypes)
    are always retained.
    """
    excluded_keys: set[str] = set()
    for group_name, group_keys in ASPECT_GROUPS.items():
        if group_name not in include_groups:
            excluded_keys.update(group_keys)

    return {k: v for k, v in entity.items() if k not in excluded_keys}


entity_details_fragment_gql = (
    graphql_helpers.GQL_DIR / "entity_details.gql"
).read_text()
query_entity_gql = (graphql_helpers.GQL_DIR / "query_entity.gql").read_text()
related_documents_gql = (graphql_helpers.GQL_DIR / "related_documents.gql").read_text()


def get_entities(
    urns: List[str] | str,
    include: Optional[List[str]] = None,
    ctx: Optional[Context] = None,
) -> List[dict] | dict:
    """Get detailed information about one or more entities by their DataHub URNs.

    IMPORTANT: Pass an array of URNs to retrieve multiple entities in a single call - this is much
    more efficient than calling this tool multiple times. When examining search results, always pass
    an array with the top 3-10 result URNs to compare and find the best match.

    Accepts an array of URNs or a single URN. Supports all entity types including datasets,
    assertions, incidents, dashboards, charts, users, groups, and more.

    DataHub's metadata model is aspect-based: every entity carries a set of standard
    aspects (properties, ownership, tags, …). Use ``include`` to request only the
    aspects you need — this dramatically reduces response size and token usage.

    Args:
        urns: One or more DataHub URNs.
        include: Aspect groups to return.  **Defaults to ["properties"]** (name,
            description, custom properties) for a minimal response.  Add more
            groups as needed, or pass ["all"] for the full entity.

            Available groups (consistent across all entity types):
            - "properties": name, description, custom properties. The baseline
              for almost every query — always include unless you only need
              structural metadata. (default when include is omitted)
            - "ownership": owners (users/groups) and ownership types. Use when
              the user asks "who owns …", "who is responsible for …", or for
              access / contact questions.
            - "tags": classification tags applied to the entity. Use for
              compliance (PII, sensitive), categorization, or filtering questions.
            - "glossary_terms": business glossary terms linked to the entity.
              Use when the user asks about business definitions, data meaning,
              or standardization.
            - "structured_properties": custom key-value metadata defined by
              admins. Use when asking about custom attributes, business metadata
              fields, or organization-specific properties.
            - "schema": column-level schema fields, types, and view SQL
              (datasets only). Use when the user asks about table structure,
              columns, data types, or wants to write queries. Can be large.
            - "deprecation": whether the entity is deprecated, with notes and
              replacement info. Use when checking if something is still active
              or finding alternatives.
            - "platform": which data platform hosts this entity (e.g. Snowflake,
              Kafka, Looker). Use when asking "where does this live?" or
              filtering by technology.
            - "domain": the business domain the entity belongs to. Use for
              organizational questions like "which team/domain owns this?".
            - "data_product": which data product this entity is part of. Use
              when asking about data products or logical groupings.
            - "incidents": active incidents on the entity. Use when asking about
              ongoing issues, broken pipelines, or data freshness problems.
            - "assertions": data quality assertions and their latest run results.
              Use for data quality checks, test results, or SLA questions.
            - "status": health signals, soft-delete status, and usage statistics
              (query counts, user counts — Cloud only). Use when asking "is this
              healthy?", "is this actively used?", or "how popular is this?".
            - "documents": related documentation (wiki pages, design docs).
              Use when the user asks for documentation or context beyond the
              description.
            - "all": return every aspect. Use sparingly — produces large
              responses. Prefer selecting specific groups.

            Fields that identify the entity (urn, type, subTypes, entity-specific
            IDs) are always included regardless of this parameter.

            Examples:
            - Quick lookup: include=["properties"]
            - Governance review: include=["properties", "ownership", "tags", "glossary_terms"]
            - Data quality: include=["properties", "assertions", "incidents"]
            - Schema exploration: include=["properties", "schema", "platform"]
            - Full detail: include=["all"]
    """
    client = graphql_helpers.get_datahub_client()

    # Parse and validate include parameter.
    # None → apply default filtering; set → filter to those groups.
    include_set = _parse_include(include)

    # Handle JSON-stringified arrays (same issue as filters in search tool)
    # Some MCP clients/LLMs pass arrays as JSON strings instead of proper lists
    if isinstance(urns, str):
        urns_str = urns.strip()  # Remove leading/trailing whitespace

        # Try to parse as JSON array first
        if urns_str.startswith("["):
            try:
                # Use json_repair to handle malformed JSON from LLMs
                urns = json.loads(repair_json(urns_str))
                return_single = False
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(
                    f"Failed to parse URNs as JSON array: {e}. Treating as single URN."
                )
                # Not valid JSON, treat as single URN string
                urns = [urns_str]
                return_single = True
        else:
            # Single URN string
            urns = [urns_str]
            return_single = True
    else:
        return_single = False

    # Trim whitespace from each URN (defensive against string concatenation issues)
    urns = [urn.strip() for urn in urns]

    results = []
    total_urns = len(urns)
    for idx, urn in enumerate(urns):
        try:
            # Report progress for batch requests via MCP Context
            if ctx is not None and total_urns > 1:
                ctx.report_progress(progress=idx, total=total_urns)

            # Check if entity exists first
            if not client._graph.exists(urn):
                logger.warning(f"Entity not found during existence check: {urn}")
                if return_single:
                    raise ToolError(f"Entity {urn} not found")
                results.append({"error": f"Entity {urn} not found", "urn": urn})
                continue

            # Special handling for Query entities (not part of Entity union type)
            is_query = urn.startswith("urn:li:query:")

            # Execute the appropriate GraphQL query
            variables = {"urn": urn}
            if is_query:
                result = graphql_helpers.execute_graphql(
                    client._graph,
                    query=query_entity_gql,
                    variables=variables,
                    operation_name="GetQueryEntity",
                )["entity"]
            else:
                result = graphql_helpers.execute_graphql(
                    client._graph,
                    query=entity_details_fragment_gql,
                    variables=variables,
                    operation_name="GetEntity",
                )["entity"]

            # Check if entity data was returned
            if result is None:
                raise ToolError(
                    f"Entity {urn} exists but no data could be retrieved. "
                    f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
                )

            # Fetch related documents only when requested (saves a GraphQL call).
            if include_set is None or "documents" in include_set:
                try:
                    related_docs_input = {"start": 0, "count": 10}
                    related_docs_result = graphql_helpers.execute_graphql(
                        client._graph,
                        query=related_documents_gql,
                        variables={"urn": urn, "input": related_docs_input},
                        operation_name="getRelatedDocuments",
                    )
                    if (
                        related_docs_result
                        and related_docs_result.get("entity")
                        and related_docs_result["entity"].get("relatedDocuments")
                    ):
                        result["relatedDocuments"] = (
                            graphql_helpers.clean_related_documents_response(
                                related_docs_result["entity"]["relatedDocuments"]
                            )
                        )
                except Exception as e:
                    logger.debug(
                        f"Could not fetch related documents for {urn}: {e}. This entity type may not support related documents."
                    )

            graphql_helpers.inject_urls_for_urns(client._graph, result, [""])
            graphql_helpers.truncate_descriptions(result)

            cleaned = graphql_helpers.clean_get_entities_response(result)
            if include_set is not None:
                cleaned = _filter_aspects(cleaned, include_set)
            results.append(cleaned)

        except ToolError as te:
            if return_single:
                raise
            results.append({"error": str(te), "urn": urn})
        except Exception as e:
            logger.warning(f"Error fetching entity {urn}: {e}")
            if return_single:
                raise ToolError(f"Error fetching entity {urn}: {e}") from e
            results.append({"error": str(e), "urn": urn})

    # Report completion
    if ctx is not None and total_urns > 1:
        ctx.report_progress(progress=total_urns, total=total_urns)

    # Return single dict if single URN was passed, array otherwise
    return results[0] if return_single else results


def list_schema_fields(
    urn: str,
    keywords: Optional[List[str]] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List schema fields for a dataset, with optional keyword filtering and pagination.

    Useful when schema fields were truncated in search results (schemaFieldsTruncated present)
    and you need to explore specific columns. Supports pagination for large schemas.

    Args:
        urn: Dataset URN
        keywords: Optional list of keywords to filter schema fields (OR matching).
                 - Single keyword: Treated as one keyword (NOT split on whitespace). Use for field names or exact phrases.
                 - Multiple keywords: Multiple keywords, matches any (OR logic).
                 - None or empty list: Returns all fields in priority order (same as get_entities).
                 Matches against fieldPath, description, label, tags, and glossary terms.
                 Matching fields are returned first, sorted by match count.
        limit: Maximum number of fields to return (default: 100)
        offset: Number of fields to skip for pagination (default: 0)

    Returns:
        Dictionary with:
        - urn: The dataset URN
        - fields: List of schema fields (paginated)
        - totalFields: Total number of fields in the schema
        - returned: Number of fields actually returned
        - remainingCount: Number of fields not included after offset (accounts for limit and token budget)
        - matchingCount: Number of fields that matched keywords (if keywords provided, None otherwise)
        - offset: The offset used

    Examples:
        # Single keyword (list) - search for exact field name or phrase
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["user_email"])
        # Returns fields matching "user_email" (like user_email_address, primary_user_email)

        # Multiple keywords (list) - OR matching
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["email", "user"])
        # Returns fields containing "email" OR "user" (user_email, contact_email, user_id, etc.)

        # Pagination through all fields
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=0)   # First 100
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=100) # Next 100

        # Combine filtering + pagination
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["user"], limit=50, offset=0)
    """
    client = graphql_helpers.get_datahub_client()

    # Normalize keywords to list (None means no filtering)
    keywords_lower = None
    if keywords is not None:
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords_lower = [kw.lower() for kw in keywords]

    # Fetch entity
    if not client._graph.exists(urn):
        raise ToolError(f"Entity {urn} not found")

    # Execute GraphQL query to get full schema
    variables = {"urn": urn}
    result = graphql_helpers.execute_graphql(
        client._graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    # Check if entity data was returned
    if result is None:
        raise ToolError(
            f"Entity {urn} exists but no data could be retrieved. "
            f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
        )

    # Apply same preprocessing as get_entities
    graphql_helpers.inject_urls_for_urns(client._graph, result, [""])
    graphql_helpers.truncate_descriptions(result)

    # Extract total field count before processing
    total_fields = len((result.get("schemaMetadata") or {}).get("fields", []))

    if total_fields == 0:
        return {
            "urn": urn,
            "fields": [],
            "totalFields": 0,
            "returned": 0,
            "remainingCount": 0,
            "matchingCount": None,
            "offset": offset,
        }

    # Define custom sorting function for keyword matching
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]] = None
    matching_count = None

    if keywords_lower:
        # Helper function to score a field by keyword matches
        def score_field_by_keywords(field: dict) -> int:
            """
            Score a field by counting keyword match coverage across its metadata.

            Scoring logic (OR matching):
            - Each keyword gets +1 if it appears in ANY searchable text (substring match)
            - Multiple occurrences of the same keyword in one text still count as +1
            - Higher score = more aspects of the field match the keywords

            Searchable texts (in order of priority):
            1. fieldPath (column name)
            2. description
            3. label
            4. tag names
            5. glossary term names

            Example:
                keywords = ["email", "user"]
                field = {
                    "fieldPath": "user_email",        # matches both
                    "description": "User's email",    # matches both
                    "tags": ["PII"]                   # matches neither
                }
                Score = 4 (email in fieldPath + email in desc + user in fieldPath + user in desc)

            Returns:
                Integer score (0 = no matches, higher = more coverage)
            """
            searchable_texts = [
                field.get("fieldPath", ""),
                field.get("description", ""),
                field.get("label", ""),
            ]

            # Add tag names
            if tags := field.get("tags"):
                if tag_list := tags.get("tags"):
                    searchable_texts.extend(
                        [
                            (t.get("tag", {}).get("properties") or {}).get("name", "")
                            for t in tag_list
                        ]
                    )

            # Add glossary term names
            if glossary_terms := field.get("glossaryTerms"):
                if terms_list := glossary_terms.get("terms"):
                    searchable_texts.extend(
                        [
                            (t.get("term", {}).get("properties") or {}).get("name", "")
                            for t in terms_list
                        ]
                    )

            # Count keyword coverage: +1 for each (keyword, text) pair that matches
            # Note: Substring matching, case-insensitive
            return sum(
                1
                for kw in keywords_lower
                for text in searchable_texts
                if text and kw in text.lower()
            )

        # Pre-compute matching count (need all fields for this)
        fields_for_counting = result.get("schemaMetadata", {}).get("fields", [])
        matching_count = sum(
            1 for field in fields_for_counting if score_field_by_keywords(field) > 0
        )

        # Define sort function for clean_get_entities_response
        def sort_by_keyword_match(fields: List[dict]) -> Iterator[dict]:
            """Sort fields by keyword match count (descending), then alphabetically."""
            scored_fields = [
                (score_field_by_keywords(field), field) for field in fields
            ]
            scored_fields.sort(key=lambda x: (-x[0], x[1].get("fieldPath", "")))
            return iter(field for _, field in scored_fields)

        sort_fn = sort_by_keyword_match

    # Use clean_get_entities_response for consistent processing
    cleaned_entity = graphql_helpers.clean_get_entities_response(
        result,
        sort_fn=sort_fn,
        offset=offset,
        limit=limit,
    )

    # Extract the cleaned fields and metadata
    schema_metadata = cleaned_entity.get("schemaMetadata", {})
    cleaned_fields = schema_metadata.get("fields", [])

    # Calculate how many fields remain after what we returned
    # This accounts for both pagination and token budget constraints
    remaining_count = total_fields - offset - len(cleaned_fields)

    return {
        "urn": urn,
        "fields": cleaned_fields,
        "totalFields": total_fields,
        "returned": len(cleaned_fields),
        "remainingCount": remaining_count,
        "matchingCount": matching_count,
        "offset": offset,
    }
