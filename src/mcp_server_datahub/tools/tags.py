"""Tag management tools for DataHub MCP server."""

import logging
from typing import List, Optional

from datahub.sdk.main_client import DataHubClient

from ..version_requirements import min_version
from ._mutation_helpers import batch_modify, validate_urns

logger = logging.getLogger(__name__)

# --- Tag-specific constants ---

_VALIDATE_TAGS_QUERY = """
    query getTags($urns: [String!]!) {
        entities(urns: $urns) {
            urn
            type
            ... on Tag {
                properties {
                    name
                }
            }
        }
    }
"""

_ADD_TAGS_MUTATION = """
    mutation batchAddTags($input: BatchAddTagsInput!) {
        batchAddTags(input: $input)
    }
"""

_REMOVE_TAGS_MUTATION = """
    mutation batchRemoveTags($input: BatchRemoveTagsInput!) {
        batchRemoveTags(input: $input)
    }
"""


def _validate_tag_urns(client: DataHubClient, tag_urns: List[str]) -> None:
    """Validate that all tag URNs exist in DataHub."""
    validate_urns(
        client,
        tag_urns,
        entity_label="tag",
        expected_type="TAG",
        query=_VALIDATE_TAGS_QUERY,
        operation_name="getTags",
    )


def _batch_modify_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: str,
) -> dict:
    """Internal helper for batch tag operations (add/remove)."""
    return batch_modify(
        item_urns=tag_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation=operation,  # type: ignore[arg-type]
        item_urns_key="tagUrns",
        entity_label="tag",
        add_mutation=_ADD_TAGS_MUTATION,
        add_operation_name="batchAddTags",
        add_input_type="BatchAddTagsInput",
        remove_mutation=_REMOVE_TAGS_MUTATION,
        remove_operation_name="batchRemoveTags",
        remove_input_type="BatchRemoveTagsInput",
        validate_fn=_validate_tag_urns,
    )


@min_version(cloud="0.3.16", oss="1.4.0")
def add_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Add one or more tags to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to tag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tagging operations like marking multiple datasets as PII, deprecated, or applying
    governance classifications.


    Args:
        tag_urns: List of tag URNs to add (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to tag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tags.
                     For column-level tags, provide the column name (e.g., "email_address").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add tags to multiple datasets
        add_tags(
            tag_urns=["urn:li:tag:PII", "urn:li:tag:Sensitive"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add tags to specific columns
        add_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "phone_number"]
        )

        # Mix entity-level and column-level tags
        add_tags(
            tag_urns=["urn:li:tag:Deprecated"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "deprecated_column"]  # Tag whole table and a specific column
        )
    """
    return _batch_modify_tags(tag_urns, entity_urns, column_paths, "add")


@min_version(cloud="0.3.16", oss="1.4.0")
def remove_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Remove one or more tags from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to untag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tag removal operations like removing deprecated tags, correcting misapplied classifications,
    or cleaning up governance metadata.

    Args:
        tag_urns: List of tag URNs to remove (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to untag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tag removal.
                     For column-level tag removal, provide the column name (e.g., "email_address").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove tags from multiple datasets
        remove_tags(
            tag_urns=["urn:li:tag:Deprecated", "urn:li:tag:Legacy"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove tags from specific columns
        remove_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_email_field", "deprecated_phone"]
        )

        # Mix entity-level and column-level tag removal
        remove_tags(
            tag_urns=["urn:li:tag:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "test_column"]  # Remove from whole table and a specific column
        )
    """
    return _batch_modify_tags(tag_urns, entity_urns, column_paths, "remove")
