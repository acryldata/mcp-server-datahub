"""Incident management tools for DataHub MCP server."""

import json
import logging
from typing import Any, Dict, Optional

from datahub.metadata.schema_classes import IncidentInfoClass, IncidentTypeClass
from datahub.metadata.urns import Urn

from .. import graphql_helpers

logger = logging.getLogger(__name__)

RAISE_INCIDENT_MUTATION = """
mutation raiseIncident($input: RaiseIncidentInput!) {
  raiseIncident(input: $input)
}
"""

#: Values accepted by the GraphQL ``IncidentPriority`` enum. Note this is an enum
#: name and not the integer the ``incidentInfo`` aspect stores underneath.
VALID_PRIORITIES = ("LOW", "MEDIUM", "HIGH", "CRITICAL")


def _valid_incident_types() -> frozenset:
    """Return every incident type the installed metadata model accepts.

    Read off the enum rather than hardcoded, so the tool cannot drift from the
    server it is talking to. Note there is no ``COLUMN`` type: the column-scoped
    one is ``FIELD``.
    """
    return frozenset(
        value
        for name, value in vars(IncidentTypeClass).items()
        if not name.startswith("_") and isinstance(value, str)
    )


def _valid_entity_types() -> frozenset:
    """Return the entity types an incident may attach to.

    Read off the ``incidentInfo`` aspect's own relationship annotation, for the
    same no-drift reason as :func:`_valid_incident_types`. Notably this does not
    include ``mlModel``: GMS rejects one with a 500.
    """
    schema = json.loads(str(IncidentInfoClass.RECORD_SCHEMA))
    for field in schema["fields"]:
        if field["name"] == "entities":
            return frozenset(field["Relationship"]["/*"]["entityTypes"])
    raise RuntimeError(
        "incidentInfo has no 'entities' field; the metadata model changed"
    )


def _validate_resource_urn(resource_urn: str) -> None:
    """Validate that an incident can actually be attached to this entity.

    Raises:
        ValueError: The URN is malformed, or names an entity type that cannot
            carry an incident.
    """
    try:
        entity_type = Urn.from_string(resource_urn).entity_type
    except Exception as e:
        raise ValueError(
            f"{resource_urn!r} is not a valid DataHub URN: {e}. "
            f"Use the search tool to find the entity you want to raise an incident on."
        ) from e

    allowed = _valid_entity_types()
    if entity_type not in allowed:
        raise ValueError(
            f"DataHub cannot raise an incident on a {entity_type}. "
            f"Allowed entity types: {', '.join(sorted(allowed))}. "
            f"Attach the incident to the dataset or schemaField the problem concerns."
        )


def raise_incident(
    resource_urn: str,
    title: str,
    description: str,
    incident_type: str = "CUSTOM",
    priority: Optional[str] = None,
) -> dict:
    """Raise an incident on a DataHub entity to record a data quality problem.

    Use this to record a problem you have found so it is visible to the asset's
    owners and to the next agent or person who looks at it. An incident is the
    durable, catalog-native way to say "this asset is currently broken", as
    opposed to a tag or a description edit.

    The incident is created in the ACTIVE state and appears on the entity's
    Incidents tab in the DataHub UI.

    Args:
        resource_urn: URN of the entity to raise the incident on (e.g. a dataset
            or schemaField URN). Note that an mlModel cannot carry an incident;
            attach it to the dataset the model consumes instead.
        title: Short, human-readable summary of the problem
            (e.g. "orders_raw has not refreshed in 30 hours").
        description: The incident body. Include what was observed, how it was
            measured, and what the impact is.
        incident_type: One of the DataHub incident types, e.g. "FRESHNESS",
            "DATA_SCHEMA", "FIELD", "OPERATIONAL", "CUSTOM" (default "CUSTOM").
        priority: Optional priority, one of "LOW", "MEDIUM", "HIGH", "CRITICAL".

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - urn: The URN of the newly created incident
        - message: Success message

    Examples:
        # Record a stale table
        raise_incident(
            resource_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.orders_raw,PROD)",
            title="orders_raw has not refreshed in 30 hours",
            description="Last operation was 30h ago against a 6h freshness SLA.",
            incident_type="FRESHNESS",
            priority="HIGH",
        )

        # Record a column-level problem
        raise_incident(
            resource_urn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.public.users,PROD),email)",
            title="email is 40% null after the latest load",
            description="Null rate moved from 0.1% to 40% between runs.",
            incident_type="FIELD",
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not title or not title.strip():
        raise ValueError("title cannot be empty")
    if not description or not description.strip():
        raise ValueError("description cannot be empty")

    allowed_types = _valid_incident_types()
    if incident_type not in allowed_types:
        raise ValueError(
            f"{incident_type!r} is not a DataHub incident type. "
            f"Allowed types: {', '.join(sorted(allowed_types))}."
        )

    if priority is not None and priority not in VALID_PRIORITIES:
        raise ValueError(
            f"{priority!r} is not a valid incident priority. "
            f"Allowed priorities: {', '.join(VALID_PRIORITIES)}."
        )

    _validate_resource_urn(resource_urn)

    incident_input: Dict[str, Any] = {
        "resourceUrn": resource_urn,
        "type": incident_type,
        "title": title,
        "description": description,
    }
    if priority is not None:
        incident_input["priority"] = priority

    result = graphql_helpers.execute_graphql(
        client._graph,
        query=RAISE_INCIDENT_MUTATION,
        variables={"input": incident_input},
        operation_name="raiseIncident",
    )

    incident_urn = result.get("raiseIncident")
    if not incident_urn:
        raise ValueError(
            f"raiseIncident returned no URN for {resource_urn}. Response: {result}"
        )

    logger.info(f"Raised incident {incident_urn} on {resource_urn}")

    return {
        "success": True,
        "urn": incident_urn,
        "message": f"Raised {incident_type} incident on {resource_urn}",
    }
