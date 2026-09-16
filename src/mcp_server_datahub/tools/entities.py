"""Entity retrieval tools for DataHub MCP server."""

import json
from typing import Any, Callable, Iterator, List, Optional

from datahub.errors import ItemNotFoundError
from json_repair import repair_json
from loguru import logger

from .. import graphql_helpers
from ..version_requirements import read_only

entity_details_fragment_gql = (
    graphql_helpers.GQL_DIR / "entity_details.gql"
).read_text()
query_entity_gql = (graphql_helpers.GQL_DIR / "query_entity.gql").read_text()
related_documents_gql = (graphql_helpers.GQL_DIR / "related_documents.gql").read_text()


def _normalize_non_negative_int(value: Any, *, path: str) -> int:
    """Normalize a JSON integer while rejecting ambiguous or invalid values."""
    if isinstance(value, bool):
        raise ValueError(f"Invalid {path}: expected a non-negative integer")

    if isinstance(value, int):
        normalized = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped.isdigit():
            raise ValueError(f"Invalid {path}: expected a non-negative integer")
        normalized = int(stripped)
    else:
        raise ValueError(f"Invalid {path}: expected a non-negative integer")

    if normalized < 0:
        raise ValueError(f"Invalid {path}: expected a non-negative integer")
    return normalized


def _normalize_actor(value: Any, *, path: str) -> str:
    """Normalize an audit actor to a non-empty URN string."""
    actor = value
    if isinstance(value, dict):
        actor = value.get("urn")

    if not isinstance(actor, str) or not actor.strip():
        raise ValueError(
            f"Invalid {path}: expected a non-empty actor URN string or object"
        )
    return actor.strip()


def _normalize_audit_stamp(value: Any, *, path: str) -> dict:
    """Normalize a DataHub AuditStamp to stable actor/time scalar fields."""
    if not isinstance(value, dict):
        raise ValueError(f"Invalid {path}: expected an audit stamp object")
    if "actor" not in value or "time" not in value:
        raise ValueError(f"Invalid {path}: audit stamp requires actor and time")

    normalized = {
        "actor": _normalize_actor(value["actor"], path=f"{path}.actor"),
        "time": _normalize_non_negative_int(value["time"], path=f"{path}.time"),
    }

    if value.get("impersonator") is not None:
        normalized["impersonator"] = _normalize_actor(
            value["impersonator"], path=f"{path}.impersonator"
        )

    if value.get("message") is not None:
        message = value["message"]
        if not isinstance(message, str):
            raise ValueError(f"Invalid {path}.message: expected a string")
        if message:
            normalized["message"] = message

    return normalized


def _normalize_system_metadata(value: Any, *, path: str) -> dict:
    """Select and normalize bounded provenance fields from SystemMetadata."""
    if not isinstance(value, dict):
        raise ValueError(f"Invalid {path}: expected a systemMetadata object")

    normalized: dict[str, Any] = {}

    for field_name in (
        "runId",
        "lastRunId",
        "pipelineName",
        "registryName",
        "registryVersion",
    ):
        field_value = value.get(field_name)
        if field_value is None:
            continue
        if not isinstance(field_value, str):
            raise ValueError(f"Invalid {path}.{field_name}: expected a string")
        normalized[field_name] = field_value

    for field_name in ("lastObserved", "schemaVersion", "version"):
        field_value = value.get(field_name)
        if field_value is not None:
            normalized[field_name] = _normalize_non_negative_int(
                field_value, path=f"{path}.{field_name}"
            )

    for field_name in ("aspectCreated", "aspectModified"):
        field_value = value.get(field_name)
        if field_value is not None:
            normalized[field_name] = _normalize_audit_stamp(
                field_value, path=f"{path}.{field_name}"
            )

    # Deliberately omit SystemMetadata.properties. It is an arbitrary map and can
    # be large or connector-specific; this opt-in response is an audit summary,
    # not a second copy of the raw aspect envelope.
    return normalized


def _normalize_aspect_metadata(raw_response: Any, *, expected_urn: str) -> dict:
    """Build a compact, fail-closed aspect audit map from entitiesV2."""
    if not isinstance(raw_response, dict):
        raise ValueError("Invalid aspect metadata response: expected an object")

    response_urn = raw_response.get("urn")
    if response_urn != expected_urn:
        raise ValueError(
            "Invalid aspect metadata response: "
            f"expected URN {expected_urn}, received {response_urn!r}"
        )

    aspects = raw_response.get("aspects")
    if not isinstance(aspects, dict) or not aspects:
        raise ValueError(
            f"Invalid aspect metadata response for {expected_urn}: "
            "expected a non-empty aspects map"
        )

    normalized_aspects: dict[str, dict] = {}
    for aspect_name in sorted(aspects):
        if not isinstance(aspect_name, str) or not aspect_name:
            raise ValueError(
                f"Invalid aspect metadata response for {expected_urn}: "
                "aspect names must be non-empty strings"
            )

        aspect_path = f"aspectMetadata.{aspect_name}"
        envelope = aspects[aspect_name]
        if not isinstance(envelope, dict):
            raise ValueError(f"Invalid {aspect_path}: expected an aspect envelope")

        envelope_name = envelope.get("name")
        if envelope_name is not None and envelope_name != aspect_name:
            raise ValueError(
                f"Invalid {aspect_path}.name: expected {aspect_name}, "
                f"received {envelope_name!r}"
            )

        normalized_envelope: dict[str, Any] = {}

        aspect_type = envelope.get("type")
        if aspect_type is not None:
            if not isinstance(aspect_type, str) or not aspect_type:
                raise ValueError(f"Invalid {aspect_path}.type: expected a string")
            normalized_envelope["type"] = aspect_type

        for field_name in ("version", "timestamp"):
            field_value = envelope.get(field_name)
            if field_value is not None:
                normalized_envelope[field_name] = _normalize_non_negative_int(
                    field_value, path=f"{aspect_path}.{field_name}"
                )

        if envelope.get("created") is not None:
            normalized_envelope["created"] = _normalize_audit_stamp(
                envelope["created"], path=f"{aspect_path}.created"
            )

        if envelope.get("systemMetadata") is not None:
            system_metadata = _normalize_system_metadata(
                envelope["systemMetadata"],
                path=f"{aspect_path}.systemMetadata",
            )
            if system_metadata:
                normalized_envelope["systemMetadata"] = system_metadata

        normalized_aspects[aspect_name] = normalized_envelope

    return normalized_aspects


@read_only
def get_entities(
    urns: List[str] | str, include_system_metadata: bool = False
) -> List[dict] | dict:
    """Get detailed information about one or more entities by their DataHub URNs.

    IMPORTANT: Pass an array of URNs to retrieve multiple entities in a single call - this is much
    more efficient than calling this tool multiple times. When examining search results, always pass
    an array with the top 3-10 result URNs to compare and find the best match.

    Accepts an array of URNs or a single URN. Supports all entity types including datasets,
    assertions, incidents, dashboards, charts, users, groups, and more. The response fields vary
    based on the entity type.

    Set include_system_metadata=true to add an aspectMetadata map keyed by aspect name. Each
    entry contains available aspect envelope audit data and selected systemMetadata fields, with
    actor values normalized to URN strings and time values normalized to epoch-millisecond
    integers. This is ingestion/catalog processing context, not evidence that the metadata was
    validated or is semantically correct. The opt-in audit read fails closed: malformed or
    mismatched metadata returns an error instead of silently omitting the requested context.
    """
    client = graphql_helpers.get_datahub_client()

    if not isinstance(include_system_metadata, bool):
        raise ValueError("include_system_metadata must be a boolean")

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
            except (json.JSONDecodeError, Exception) as e:
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
    for urn in urns:
        try:
            # Check if entity exists first
            if not client._graph.exists(urn):
                logger.warning(f"Entity not found during existence check: {urn}")
                if return_single:
                    raise ItemNotFoundError(f"Entity {urn} not found")
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
                raise ItemNotFoundError(
                    f"Entity {urn} exists but no data could be retrieved. "
                    f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
                )

            # Fetch related documents for supported entity types
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

            cleaned_result = graphql_helpers.clean_get_entities_response(result)

            if include_system_metadata:
                raw_aspect_metadata = client._graph.get_entity_raw(urn)
                cleaned_result["aspectMetadata"] = _normalize_aspect_metadata(
                    raw_aspect_metadata,
                    expected_urn=urn,
                )

            results.append(cleaned_result)

        except Exception as e:
            logger.warning(f"Error fetching entity {urn}: {e}")
            if return_single:
                raise
            results.append({"error": str(e), "urn": urn})

    # Return single dict if single URN was passed, array otherwise
    return results[0] if return_single else results


@read_only
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
        raise ItemNotFoundError(f"Entity {urn} not found")

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
        raise ItemNotFoundError(
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
