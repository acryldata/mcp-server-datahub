"""Terms management tools for DataHub MCP server."""

import logging
from typing import List, Optional

from datahub.sdk.main_client import DataHubClient

from ..version_requirements import min_version
from ._mutation_helpers import batch_modify, validate_urns

logger = logging.getLogger(__name__)

# --- Glossary-term-specific constants ---

_VALIDATE_TERMS_QUERY = """
    query getGlossaryTerms($urns: [String!]!) {
        entities(urns: $urns) {
            urn
            type
            ... on GlossaryTerm {
                name
            }
        }
    }
"""

_ADD_TERMS_MUTATION = """
    mutation batchAddTerms($input: BatchAddTermsInput!) {
        batchAddTerms(input: $input)
    }
"""

_REMOVE_TERMS_MUTATION = """
    mutation batchRemoveTerms($input: BatchRemoveTermsInput!) {
        batchRemoveTerms(input: $input)
    }
"""


def _validate_glossary_term_urns(client: DataHubClient, term_urns: List[str]) -> None:
    """Validate that all glossary term URNs exist in DataHub."""
    validate_urns(
        client,
        term_urns,
        entity_label="glossary term",
        expected_type="GLOSSARY_TERM",
        query=_VALIDATE_TERMS_QUERY,
        operation_name="getGlossaryTerms",
    )


def _batch_modify_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: str,
) -> dict:
    """Internal helper for batch glossary term operations (add/remove)."""
    return batch_modify(
        item_urns=term_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation=operation,  # type: ignore[arg-type]
        item_urns_key="termUrns",
        entity_label="glossary term",
        add_mutation=_ADD_TERMS_MUTATION,
        add_operation_name="batchAddTerms",
        add_input_type="BatchAddTermsInput",
        remove_mutation=_REMOVE_TERMS_MUTATION,
        remove_operation_name="batchRemoveTerms",
        remove_input_type="BatchRemoveTermsInput",
        validate_fn=_validate_glossary_term_urns,
    )


@min_version(cloud="0.3.16", oss="1.4.0")
def add_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Add one or more glossary terms (terms) to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to associate multiple entities or their columns with multiple glossary terms in a single operation.
    Useful for bulk term assignment operations like applying business definitions, standardizing terminology,
    or enriching metadata with domain knowledge.

    Args:
        term_urns: List of glossary term URNs to add (e.g., ["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:SensitiveInfo"])
        entity_urns: List of entity URNs to annotate (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary terms.
                     For column-level glossary terms, provide the column name (e.g., "customer_email").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add glossary terms to multiple datasets
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:PersonalInformation"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add glossary terms to specific columns
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:EmailAddress"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "contact_email"]
        )

        # Mix entity-level and column-level glossary terms
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Revenue"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)"
            ],
            column_paths=[None, "total_amount"]  # Term for whole table and a specific column
        )
    """
    return _batch_modify_glossary_terms(term_urns, entity_urns, column_paths, "add")


@min_version(cloud="0.3.16", oss="1.4.0")
def remove_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Remove one or more glossary terms (terms) from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to disassociate multiple entities or their columns from multiple glossary terms in a single operation.
    Useful for bulk term removal operations like correcting misapplied business definitions, updating terminology,
    or cleaning up metadata.

    Args:
        term_urns: List of glossary term URNs to remove (e.g., ["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:Legacy"])
        entity_urns: List of entity URNs to remove terms from (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary term removal.
                     For column-level glossary term removal, provide the column name (e.g., "old_field").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove glossary terms from multiple datasets
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:LegacySystem"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove glossary terms from specific columns
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Confidential"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_ssn_field", "legacy_tax_id"]
        )

        # Mix entity-level and column-level glossary term removal
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.production_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "beta_feature"]  # Remove from whole table and a specific column
        )
    """
    return _batch_modify_glossary_terms(term_urns, entity_urns, column_paths, "remove")
