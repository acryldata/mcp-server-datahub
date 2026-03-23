"""Shared helpers for batch mutation tools (tags, glossary terms).

This module extracts the common pattern used by tags.py and terms.py:
1. Validate that URNs exist and are the correct entity type
2. Build a resources list from entity_urns + column_paths
3. Execute a batch add/remove GraphQL mutation
"""

import logging
from typing import List, Literal, Optional

from datahub.sdk.main_client import DataHubClient
from fastmcp.exceptions import ToolError

from .. import graphql_helpers

logger = logging.getLogger(__name__)


def validate_urns(
    client: DataHubClient,
    urns: List[str],
    *,
    entity_label: str,
    expected_type: str,
    query: str,
    operation_name: str,
) -> None:
    """Validate that all URNs exist in DataHub and are the expected type.

    Args:
        client: DataHub client to use for the query.
        urns: List of URNs to validate.
        entity_label: Human-readable label for error messages (e.g. "tag", "glossary term").
        expected_type: Expected entity type string (e.g. "TAG", "GLOSSARY_TERM").
        query: GraphQL query to fetch entities by URNs.
        operation_name: GraphQL operation name.

    Raises:
        ValueError: If any URN does not exist or is the wrong entity type.
    """
    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables={"urns": urns},
            operation_name=operation_name,
        )

        entities = result.get("entities", [])
        found_urns = {entity["urn"] for entity in entities if entity is not None}
        missing_urns = [urn for urn in urns if urn not in found_urns]

        if missing_urns:
            raise ToolError(
                f"The following {entity_label} URNs do not exist in DataHub: {', '.join(missing_urns)}. "
                f"Please use the search tool with entity_type filter to find existing {entity_label}s, "
                f"or create the {entity_label}s first before assigning them."
            )

        wrong_type = [
            entity["urn"]
            for entity in entities
            if entity and entity.get("type") != expected_type
        ]
        if wrong_type:
            raise ToolError(
                f"The following URNs are not {entity_label} entities: {', '.join(wrong_type)}"
            )

    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"Failed to validate {entity_label} URNs: {str(e)}") from e


def build_resources(
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
) -> List[dict]:
    """Build the resources list for a batch mutation, handling column_paths.

    Args:
        entity_urns: List of entity URNs to operate on.
        column_paths: Optional per-entity column paths for column-level operations.

    Returns:
        List of resource input dicts for the GraphQL mutation.

    Raises:
        ValueError: If column_paths length doesn't match entity_urns.
    """
    if column_paths is None:
        column_paths = [None] * len(entity_urns)
    elif len(column_paths) != len(entity_urns):
        raise ToolError(
            f"column_paths length ({len(column_paths)}) must match entity_urns length ({len(entity_urns)})"
        )

    resources = []
    for resource_urn, column_path in zip(entity_urns, column_paths, strict=True):
        resource_input: dict = {"resourceUrn": resource_urn}
        if column_path:
            resource_input["subResource"] = column_path
            resource_input["subResourceType"] = "DATASET_FIELD"
        resources.append(resource_input)

    return resources


def batch_modify(
    item_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: Literal["add", "remove"],
    *,
    item_urns_key: str,
    entity_label: str,
    add_mutation: str,
    add_operation_name: str,
    add_input_type: str,
    remove_mutation: str,
    remove_operation_name: str,
    remove_input_type: str,
    validate_fn: object,
) -> dict:
    """Execute a batch add/remove mutation.

    This is the shared core of _batch_modify_tags and _batch_modify_glossary_terms.

    Args:
        item_urns: URNs of items to add/remove (tags or terms).
        entity_urns: URNs of target entities.
        column_paths: Optional column paths for column-level operations.
        operation: "add" or "remove".
        item_urns_key: Key for URNs in the mutation input (e.g. "tagUrns", "termUrns").
        entity_label: Human-readable label (e.g. "tag", "glossary term").
        add_mutation: GraphQL mutation string for add.
        add_operation_name: Operation name for add mutation.
        add_input_type: GraphQL input type name for add (unused but documents the API).
        remove_mutation: GraphQL mutation string for remove.
        remove_operation_name: Operation name for remove mutation.
        remove_input_type: GraphQL input type name for remove (unused but documents the API).
        validate_fn: Callable(client, urns) to validate item URNs exist.

    Returns:
        Dict with "success" and "message" keys.

    Raises:
        ValueError: If inputs are invalid.
        RuntimeError: If the mutation fails.
    """
    client = graphql_helpers.get_datahub_client()

    if not item_urns:
        raise ToolError(f"{item_urns_key} cannot be empty")
    if not entity_urns:
        raise ToolError("entity_urns cannot be empty")

    # Validate URNs exist
    validate_fn(client, item_urns)  # type: ignore[operator]

    resources = build_resources(entity_urns, column_paths)

    if operation == "add":
        mutation = add_mutation
        operation_name = add_operation_name
        success_verb = "added"
        failure_verb = "add"
    else:
        mutation = remove_mutation
        operation_name = remove_operation_name
        success_verb = "removed"
        failure_verb = "remove"

    variables = {"input": {item_urns_key: item_urns, "resources": resources}}

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables=variables,
            operation_name=operation_name,
        )

        success = result.get(operation_name, False)
        if success:
            preposition = "to" if operation == "add" else "from"
            return {
                "success": True,
                "message": (
                    f"Successfully {success_verb} {len(item_urns)} {entity_label}(s) "
                    f"{preposition} {len(entity_urns)} entit(ies)"
                ),
            }
        else:
            raise ToolError(
                f"Failed to {failure_verb} {entity_label}s - operation returned false"
            )

    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"Error {failure_verb} {entity_label}s: {str(e)}") from e
