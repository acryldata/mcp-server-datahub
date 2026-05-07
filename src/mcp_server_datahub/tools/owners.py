"""Owner management tools for DataHub MCP server."""

import logging
from enum import Enum
from typing import List, Literal, Optional, Union

from datahub.sdk.main_client import DataHubClient

from .. import graphql_helpers
from ..version_requirements import min_version

logger = logging.getLogger(__name__)

OWNERSHIP_TYPE_URN_PREFIX = "urn:li:ownershipType:"


class OwnershipType(str, Enum):
    """Built-in ownership types in DataHub."""

    TECHNICAL_OWNER = "__system__technical_owner"
    BUSINESS_OWNER = "__system__business_owner"
    DATA_STEWARD = "__system__data_steward"

    def to_urn(self) -> str:
        """Convert the ownership type to its URN form."""
        return f"{OWNERSHIP_TYPE_URN_PREFIX}{self.value}"


OwnershipTypeInput = Union[OwnershipType, str]


def _resolve_ownership_type_urn(
    client: DataHubClient, ownership_type: OwnershipTypeInput
) -> str:
    """Convert an OwnershipType enum or custom ownership-type name to a validated URN.

    Built-in OwnershipType enums are trusted and returned directly. Custom inputs are
    resolved by name (case-insensitive) via `listOwnershipTypes` — this matches what a
    user sees in the UI, rather than requiring them to know the underlying URN.
    """
    if isinstance(ownership_type, OwnershipType):
        return ownership_type.to_urn()

    if not isinstance(ownership_type, str) or not ownership_type.strip():
        raise ValueError(
            "ownership_type must be an OwnershipType or a non-empty string"
        )

    candidate = ownership_type.strip()

    # Fast-path: built-in passed as a string, either by enum name
    # ("TECHNICAL_OWNER") or enum value ("__system__technical_owner").
    # Avoids a GraphQL round-trip and makes the previously-accidental
    # enum-value-as-string case intentional.
    if candidate in OwnershipType.__members__:
        return OwnershipType[candidate].to_urn()
    if candidate in OwnershipType._value2member_map_:
        return OwnershipType(candidate).to_urn()

    # Fast-path: caller already passed a fully-qualified ownership type URN
    # (e.g. "urn:li:ownershipType:__system__technical_owner" or a custom URN).
    # Trust it as-is rather than treating it as a name to look up.
    if candidate.startswith(OWNERSHIP_TYPE_URN_PREFIX):
        return candidate

    # We look up by name rather than by URN because the URN is an opaque internal
    # identifier (often a generated id like `urn:li:ownershipType:abc-123`) that
    # users/LLMs don't know. The name is what's shown in the UI and what callers
    # would naturally reference, so we resolve name -> URN via listOwnershipTypes.
    query = """
        query listOwnershipTypes($input: ListOwnershipTypesInput!) {
            listOwnershipTypes(input: $input) {
                ownershipTypes {
                    urn
                    info {
                        name
                    }
                }
            }
        }
    """

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables={"input": {"start": 0, "count": 1000, "query": candidate}},
            operation_name="listOwnershipTypes",
        )
    except Exception as e:
        raise ValueError(
            f"Failed to look up ownership type '{ownership_type}': {str(e)}"
        ) from e

    ownership_types = (result.get("listOwnershipTypes") or {}).get(
        "ownershipTypes"
    ) or []

    # listOwnershipTypes returns partial/prefix matches — filter for an exact name
    # match (case-insensitive) since name is the user-facing identifier.
    matches = [
        ot
        for ot in ownership_types
        if ((ot.get("info") or {}).get("name") or "").strip().lower()
        == candidate.lower()
    ]

    if not matches:
        raise ValueError(
            f"Ownership type with name '{ownership_type}' was not found in DataHub. "
            f"Use one of the built-in OwnershipType values or create the custom ownership type first."
        )
    if len(matches) > 1:
        found_urns = ", ".join(m.get("urn", "<unknown>") for m in matches)
        raise ValueError(
            f"Ownership type name '{ownership_type}' is ambiguous — matched multiple entries: {found_urns}."
        )

    urn = matches[0].get("urn")
    if not urn:
        raise ValueError(
            f"Ownership type '{ownership_type}' was found but is missing a URN."
        )
    return urn


def _validate_owner_urns(client: DataHubClient, owner_urns: List[str]) -> None:
    """
    Validate that all owner URNs exist in DataHub and are either CorpUser or CorpGroup entities.

    Raises:
        ValueError: If any owner URN does not exist or is not a valid owner entity type
    """
    # Query to check if owners exist and are valid types
    query = """
        query getOwners($urns: [String!]!) {
            entities(urns: $urns) {
                urn
                type
                ... on CorpUser {
                    username
                }
                ... on CorpGroup {
                    name
                }
            }
        }
    """

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables={"urns": owner_urns},
            operation_name="getOwners",
        )

        entities = result.get("entities", [])

        # Build a map of found URNs
        found_urns = {entity["urn"] for entity in entities if entity is not None}

        # Check for missing owners
        missing_urns = [urn for urn in owner_urns if urn not in found_urns]

        if missing_urns:
            raise ValueError(
                f"The following owner URNs do not exist in DataHub: {', '.join(missing_urns)}. "
                f"Please use the search tool with entity_type filter to find existing users or groups, "
                f"or create the owners first before assigning them."
            )

        # Verify all returned entities are either CorpUser or CorpGroup
        invalid_type_entities = [
            entity["urn"]
            for entity in entities
            if entity and entity.get("type") not in ("CORP_USER", "CORP_GROUP")
        ]
        if invalid_type_entities:
            raise ValueError(
                f"The following URNs are not valid owner entities (must be CorpUser or CorpGroup): {', '.join(invalid_type_entities)}"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate owner URNs: {str(e)}") from e


def _batch_modify_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type: Optional[OwnershipTypeInput],
    operation: Literal["add", "remove"],
) -> dict:
    """
    Internal helper for batch owner operations (add/remove).

    Validates inputs, constructs GraphQL mutation, and executes the operation.
    """
    client = graphql_helpers.get_datahub_client()

    # Validate inputs
    if not owner_urns:
        raise ValueError("owner_urns cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate that all owner URNs exist and are valid types
    _validate_owner_urns(client, owner_urns)

    # Build the resources list for GraphQL mutation
    resources = []
    for resource_urn in entity_urns:
        resource_input = {"resourceUrn": resource_urn}
        resources.append(resource_input)

    # Resolve & validate ownership type (built-in enum or custom URN/id)
    ownership_type_urn = (
        _resolve_ownership_type_urn(client, ownership_type)
        if ownership_type is not None
        else None
    )

    # Determine mutation and operation name based on operation type
    if operation == "add":
        # For adding owners, we need to include ownerEntityType
        # Determine owner entity types from URNs
        owners = []
        for owner_urn in owner_urns:
            owner_entity_type = (
                "CORP_USER" if ":corpuser:" in owner_urn.lower() else "CORP_GROUP"
            )
            owner_input: dict = {
                "ownerUrn": owner_urn,
                "ownerEntityType": owner_entity_type,
            }
            # Add ownership type if provided
            if ownership_type_urn:
                owner_input["ownershipTypeUrn"] = ownership_type_urn

            owners.append(owner_input)

        mutation = """
            mutation batchAddOwners($input: BatchAddOwnersInput!) {
                batchAddOwners(input: $input)
            }
        """
        add_input: dict = {
            "owners": owners,
            "resources": resources,
        }
        if ownership_type_urn:
            add_input["ownershipTypeUrn"] = ownership_type_urn

        variables = {"input": add_input}

        operation_name = "batchAddOwners"
        success_verb = "added"
        failure_verb = "add"
    else:  # remove
        mutation = """
            mutation batchRemoveOwners($input: BatchRemoveOwnersInput!) {
                batchRemoveOwners(input: $input)
            }
        """
        remove_input: dict = {
            "ownerUrns": owner_urns,
            "resources": resources,
        }
        if ownership_type_urn:
            remove_input["ownershipTypeUrn"] = ownership_type_urn

        variables = {"input": remove_input}

        operation_name = "batchRemoveOwners"
        success_verb = "removed"
        failure_verb = "remove"

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
                "message": f"Successfully {success_verb} {len(owner_urns)} owner(s) {preposition} {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError(
                f"Failed to {failure_verb} owners - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error {failure_verb} owners: {str(e)}") from e


@min_version(cloud="0.3.16", oss="1.4.0")
def add_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type: OwnershipTypeInput,
) -> dict:
    """Add one or more owners to multiple DataHub entities.

    This tool allows you to assign multiple entities with multiple owners in a single operation.
    Useful for bulk ownership assignment operations like assigning data stewards, technical owners,
    or business owners to datasets, dashboards, and other DataHub entities.

    Note: Ownership in DataHub is entity-level only. For field-level metadata, use tags or glossary terms instead.

    Args:
        owner_urns: List of owner URNs to add (must be CorpUser or CorpGroup URNs).
                   Examples: ["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"]
        entity_urns: List of entity URNs to assign ownership to (e.g., dataset URNs, dashboard URNs)
        ownership_type: The type of ownership to assign. Accepts either:
                       - A built-in OwnershipType enum value:
                         - TECHNICAL_OWNER: Involved in production, maintenance, or distribution
                         - BUSINESS_OWNER: Principle stakeholders or domain experts
                         - DATA_STEWARD: Involved in governance
                       - A custom ownership type, passed by its user-facing name
                         (e.g. "Producer"). Custom types are looked up by name
                         (case-insensitive) via GraphQL and must already exist.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add technical owners to multiple datasets
        add_owners(
            owner_urns=["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type=TECHNICAL_OWNER
        )

        # Add business owner
        add_owners(
            owner_urns=["urn:li:corpuser:jane.smith"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type=BUSINESS_OWNER
        )

        # Add data steward to multiple entities
        add_owners(
            owner_urns=["urn:li:corpuser:data.steward"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,sales_dashboard,PROD)"
            ],
            ownership_type=DATA_STEWARD
        )
    """
    return _batch_modify_owners(owner_urns, entity_urns, ownership_type, "add")


@min_version(cloud="0.3.16", oss="1.4.0")
def remove_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type: Optional[OwnershipTypeInput] = None,
) -> dict:
    """Remove one or more owners from multiple DataHub entities.

    This tool allows you to unassign multiple entities from multiple owners in a single operation.
    Useful for bulk ownership removal operations like removing owners when they change roles,
    cleaning up stale ownership, or correcting misassigned ownership.

    Note: Ownership in DataHub is entity-level only. For field-level metadata, use tags or glossary terms instead.

    Args:
        owner_urns: List of owner URNs to remove (must be CorpUser or CorpGroup URNs).
                   Examples: ["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"]
        entity_urns: List of entity URNs to remove ownership from (e.g., dataset URNs, dashboard URNs)
        ownership_type: Optional ownership type to specify which type of ownership to remove.
                       If not provided, will remove ownership regardless of type.
                       Accepts either a built-in OwnershipType enum value
                       (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD) or a custom ownership
                       type by its user-facing name (e.g. "Producer"). Custom types are looked
                       up by name (case-insensitive) and must already exist in DataHub.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove owners from multiple datasets (any ownership type)
        remove_owners(
            owner_urns=["urn:li:corpuser:former.employee", "urn:li:corpGroup:old-team"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Remove technical owner with specific ownership type
        remove_owners(
            owner_urns=["urn:li:corpuser:john.doe"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type=TECHNICAL_OWNER
        )

        # Remove temporary owner from multiple entities
        remove_owners(
            owner_urns=["urn:li:corpuser:temp.owner"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,temp_dashboard,PROD)"
            ]
        )
    """
    return _batch_modify_owners(owner_urns, entity_urns, ownership_type, "remove")
