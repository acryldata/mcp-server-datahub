"""Incident management tools for DataHub MCP server."""

import logging
from typing import Optional

from .. import graphql_helpers

logger = logging.getLogger(__name__)

VALID_PRIORITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
VALID_STATES = {"ACTIVE", "RESOLVED"}


def raise_incident(
    dataset_urn: str,
    title: str,
    description: str,
    priority: Optional[str] = None,
) -> dict:
    """Raise a native DataHub incident on an asset.

    The incident appears on the asset's Incidents tab in the DataHub UI and
    contributes to the asset's health signals. Use this to flag operational
    problems (bad data, broken pipelines, freshness issues) so that downstream
    consumers and owners are alerted.

    Args:
        dataset_urn: URN of the asset to raise the incident on
                     (e.g., "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)").
                     Datasets are the most common target, but any asset that
                     supports incidents (dashboards, charts, data flows, etc.) works.
        title: Short human-readable title for the incident.
        description: Longer description of the problem, e.g. what was observed
                     and what the suspected root cause is. Supports markdown.
        priority: Optional incident priority. One of: CRITICAL, HIGH, MEDIUM, LOW.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message
        - incident_urn: URN of the newly created incident

    Examples:
        # Raise a high-priority incident on a dataset
        raise_incident(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            title="Null user_ids in users table",
            description="~14% of rows loaded after 2024-05-01 have NULL user_id.",
            priority="HIGH"
        )

        # Raise an incident without an explicit priority
        raise_incident(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)",
            title="Orders table is stale",
            description="No new partitions have landed in 2 days."
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not dataset_urn:
        raise ValueError("dataset_urn cannot be empty")
    if not title:
        raise ValueError("title cannot be empty")

    incident_input: dict = {
        "type": "OPERATIONAL",
        "title": title,
        "description": description,
        "resourceUrn": dataset_urn,
    }

    if priority is not None:
        normalized_priority = priority.upper()
        if normalized_priority not in VALID_PRIORITIES:
            raise ValueError(
                f"priority must be one of {sorted(VALID_PRIORITIES)}, got {priority!r}"
            )
        incident_input["priority"] = normalized_priority

    mutation = """
        mutation raiseIncident($input: RaiseIncidentInput!) {
            raiseIncident(input: $input)
        }
    """

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables={"input": incident_input},
            operation_name="raiseIncident",
        )

        incident_urn = result.get("raiseIncident")
        if not incident_urn:
            raise RuntimeError(
                "Failed to raise incident - operation did not return an incident URN"
            )

        return {
            "success": True,
            "message": f"Successfully raised incident on {dataset_urn}",
            "incident_urn": incident_urn,
        }

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error raising incident: {str(e)}") from e


def update_incident_status(
    incident_urn: str,
    state: str,
    message: Optional[str] = None,
) -> dict:
    """Update the status of an existing DataHub incident.

    Use this to resolve an incident once the underlying problem is fixed, or to
    re-open a previously resolved incident. The status change (and optional
    message) is reflected on the asset's Incidents tab in the DataHub UI.

    Args:
        incident_urn: URN of the incident to update
                      (e.g., "urn:li:incident:8ea4a5a0-...").
                      Incident URNs are returned by raise_incident and can also
                      be found via the asset's incidents in the UI.
        state: New incident state. One of: ACTIVE, RESOLVED.
        message: Optional message describing the status change, e.g. how the
                 incident was resolved.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Resolve an incident with a closing message
        update_incident_status(
            incident_urn="urn:li:incident:8ea4a5a0-2b96-4b28-9a3c-1a3d5e2f0c11",
            state="RESOLVED",
            message="Backfilled missing partitions; upstream job fixed."
        )

        # Re-open an incident
        update_incident_status(
            incident_urn="urn:li:incident:8ea4a5a0-2b96-4b28-9a3c-1a3d5e2f0c11",
            state="ACTIVE"
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not incident_urn:
        raise ValueError("incident_urn cannot be empty")

    normalized_state = state.upper() if state else ""
    if normalized_state not in VALID_STATES:
        raise ValueError(f"state must be one of {sorted(VALID_STATES)}, got {state!r}")

    status_input: dict = {"state": normalized_state}
    if message is not None:
        status_input["message"] = message

    mutation = """
        mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
            updateIncidentStatus(urn: $urn, input: $input)
        }
    """

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables={"urn": incident_urn, "input": status_input},
            operation_name="updateIncidentStatus",
        )

        success = result.get("updateIncidentStatus", False)
        if success:
            return {
                "success": True,
                "message": f"Successfully updated incident {incident_urn} to {normalized_state}",
            }
        else:
            raise RuntimeError(
                "Failed to update incident status - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error updating incident status: {str(e)}") from e
