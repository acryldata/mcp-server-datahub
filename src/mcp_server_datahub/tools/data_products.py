"""Data Product management tools for DataHub MCP server."""

import logging
from typing import List, Optional

from datahub.sdk.main_client import DataHubClient

from .. import graphql_helpers
from ..version_requirements import min_version

logger = logging.getLogger(__name__)

# Cache of whether the connected instance supports assigning multiple Data Products
# to a single asset, keyed by id(graph). Populated lazily on first use.
_multiple_dp_feature_flag_cache: dict[int, bool] = {}


def _validate_domain_urn(client: DataHubClient, domain_urn: str) -> None:
    """
    Validate that the domain URN exists in DataHub.

    Raises:
        ValueError: If the domain URN does not exist or is invalid
    """
    query = """
        query getDomain($urn: String!) {
            entity(urn: $urn) {
                urn
                type
                ... on Domain {
                    properties {
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
            variables={"urn": domain_urn},
            operation_name="getDomain",
        )

        entity = result.get("entity")

        if entity is None:
            raise ValueError(
                f"Domain URN does not exist in DataHub: {domain_urn}. "
                f"Please use the search tool with entity_type filter to find existing domains, "
                f"or create the domain first before assigning it."
            )

        if entity.get("type") != "DOMAIN":
            raise ValueError(
                f"The URN is not a domain entity: {domain_urn} (type: {entity.get('type')})"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate domain URN: {str(e)}") from e


def _check_data_product_entity(entity: Optional[dict], data_product_urn: str) -> None:
    """Raise ValueError if `entity` (the result of an `entity(urn: ...)` lookup)
    is not a valid, existing Data Product."""
    if entity is None:
        raise ValueError(
            f"Data Product URN does not exist in DataHub: {data_product_urn}. "
            f"Please use the search tool with entity_type filter to find existing data "
            f"products, or create it first with create_data_product."
        )

    if entity.get("type") != "DATA_PRODUCT":
        raise ValueError(
            f"The URN is not a Data Product entity: {data_product_urn} "
            f"(type: {entity.get('type')})"
        )


def _validate_data_product_urn(client: DataHubClient, data_product_urn: str) -> None:
    """
    Validate that the Data Product URN exists in DataHub.

    Raises:
        ValueError: If the Data Product URN does not exist or is invalid
    """
    query = """
        query getDataProduct($urn: String!) {
            entity(urn: $urn) {
                urn
                type
                ... on DataProduct {
                    properties {
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
            variables={"urn": data_product_urn},
            operation_name="getDataProduct",
        )

        _check_data_product_entity(result.get("entity"), data_product_urn)

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate Data Product URN: {str(e)}") from e


def _multiple_data_products_per_asset_enabled(client: DataHubClient) -> bool:
    """Check whether the connected instance allows assigning multiple Data Products
    to a single asset (vs. the older one-Data-Product-per-asset model).

    The underlying GraphQL field is newer than the base version this tool otherwise
    supports, so it's tagged #[NEWER_GMS] and this helper fails closed to False on
    any error (older server, field not present, etc.) — callers then use the
    always-available single-Data-Product-per-asset mutation instead.

    This is intentionally a separate GraphQL call from Data Product URN validation
    (rather than combined into one query) so that a transient failure here can
    fail closed without also blocking validation, which must succeed for the
    calling mutation to proceed at all.
    """
    graph_id = id(client._graph)
    if graph_id in _multiple_dp_feature_flag_cache:
        return _multiple_dp_feature_flag_cache[graph_id]

    query = """
        query getMultipleDataProductsFeatureFlag {
            appConfig {
                featureFlags {
                    multipleDataProductsPerAsset  #[NEWER_GMS]
                }
            }
        }
    """

    enabled = False
    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables={},
            operation_name="getMultipleDataProductsFeatureFlag",
        )
        enabled = bool(
            ((result.get("appConfig") or {}).get("featureFlags") or {}).get(
                "multipleDataProductsPerAsset", False
            )
        )
    except Exception as e:
        logger.warning(
            f"Failed to check multipleDataProductsPerAsset feature flag, assuming "
            f"disabled (will use the single-Data-Product-per-asset mutation): {e}"
        )

    _multiple_dp_feature_flag_cache[graph_id] = enabled
    return enabled


def _get_current_data_product_urns(
    client: DataHubClient, entity_urns: List[str]
) -> dict:
    """Look up which Data Product, if any, each of the given entities is currently
    associated with, via the generic DataProductContains incoming relationship
    defined on the Entity interface.

    Returns a mapping of entity_urn -> its current Data Product urn, or None if
    the entity has no Data Product association.
    """
    var_names = [f"urn{i}" for i in range(len(entity_urns))]
    var_decls = ", ".join(f"${name}: String!" for name in var_names)
    fields = "\n".join(
        f"""
        e{i}: entity(urn: ${var_names[i]}) {{
            relationships(
                input: {{
                    types: ["DataProductContains"]
                    direction: INCOMING
                    start: 0
                    count: 1
                }}
            ) {{
                relationships {{
                    entity {{
                        urn
                    }}
                }}
            }}
        }}
        """
        for i in range(len(entity_urns))
    )
    query = f"query getCurrentDataProducts({var_decls}) {{ {fields} }}"
    variables = dict(zip(var_names, entity_urns))

    result = graphql_helpers.execute_graphql(
        client._graph,
        query=query,
        variables=variables,
        operation_name="getCurrentDataProducts",
    )

    membership: dict = {}
    for i, entity_urn in enumerate(entity_urns):
        entity_result = result.get(f"e{i}") or {}
        relationships = (entity_result.get("relationships") or {}).get(
            "relationships"
        ) or []
        membership[entity_urn] = (
            relationships[0]["entity"]["urn"] if relationships else None
        )
    return membership


def _execute_bool_mutation(
    client: DataHubClient,
    *,
    query: str,
    variables: dict,
    operation_name: str,
    success_message: str,
    failure_message: str,
    error_prefix: str,
) -> dict:
    """Execute a GraphQL mutation that returns a plain boolean success flag, and
    translate the result into this module's standard {success, message} shape.

    Raises:
        RuntimeError: If the mutation call raises, or returns a falsy result.
    """
    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables=variables,
            operation_name=operation_name,
        )

        if result.get(operation_name, False):
            return {"success": True, "message": success_message}
        else:
            raise RuntimeError(failure_message)

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"{error_prefix}: {str(e)}") from e


def _execute_urn_mutation(
    client: DataHubClient,
    *,
    query: str,
    variables: dict,
    operation_name: str,
    failure_message: str,
    error_prefix: str,
) -> dict:
    """Execute a GraphQL mutation that returns an object with a `urn` field
    (e.g. createDataProduct, updateDataProduct), and return that object.

    Raises:
        RuntimeError: If the mutation call raises, or returns no urn.
    """
    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=query,
            variables=variables,
            operation_name=operation_name,
        )

        payload = result.get(operation_name)
        if payload and payload.get("urn"):
            return payload
        else:
            raise RuntimeError(failure_message)

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"{error_prefix}: {str(e)}") from e


@min_version(cloud="0.3.16", oss="1.4.0")
def create_data_product(
    name: str,
    domain_urn: str,
    description: Optional[str] = None,
    id: Optional[str] = None,
) -> dict:
    """Create a new Data Product under a Domain.

    A Data Product groups related data assets (datasets, dashboards, etc.) that
    together serve a business purpose, under a single ownable, documentable entity.

    Note: The calling user is automatically set as an owner of the new Data Product.
    Use add_owners/add_tags/add_terms/add_structured_properties afterward to add
    further metadata. Use add_assets_to_data_product to attach data assets.

    Args:
        name: Display name for the Data Product (e.g., "Customer 360")
        domain_urn: URN of the Domain this Data Product belongs to. The domain must
                   already exist (e.g., "urn:li:domain:marketing")
        description: Optional description of the Data Product's purpose
        id: Optional custom identifier for the Data Product. If not provided,
            DataHub generates one automatically.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - urn: The URN of the newly created Data Product
        - message: Success or error message

    Examples:
        # Create a Data Product under the marketing domain
        create_data_product(
            name="Customer 360",
            domain_urn="urn:li:domain:marketing",
            description="Unified view of customer data across all touchpoints"
        )

        # Create a Data Product with a custom id
        create_data_product(
            name="Order Analytics",
            domain_urn="urn:li:domain:sales",
            description="Aggregated order and fulfillment metrics",
            id="order-analytics"
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not name or not name.strip():
        raise ValueError("name cannot be empty")
    if not domain_urn:
        raise ValueError("domain_urn cannot be empty")

    _validate_domain_urn(client, domain_urn)

    properties: dict = {"name": name}
    if description:
        properties["description"] = description

    input_data: dict = {"properties": properties, "domainUrn": domain_urn}
    if id:
        input_data["id"] = id

    mutation = """
        mutation createDataProduct($input: CreateDataProductInput!) {
            createDataProduct(input: $input) {
                urn
            }
        }
    """

    created = _execute_urn_mutation(
        client,
        query=mutation,
        variables={"input": input_data},
        operation_name="createDataProduct",
        failure_message="Failed to create Data Product - mutation returned no urn",
        error_prefix="Error creating Data Product",
    )

    return {
        "success": True,
        "urn": created["urn"],
        "message": f"Successfully created Data Product '{name}' ({created['urn']})",
    }


@min_version(cloud="0.3.16", oss="1.4.0")
def update_data_product(
    data_product_urn: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> dict:
    """Update the name and/or description of an existing Data Product.

    Args:
        data_product_urn: URN of the Data Product to update
        name: New display name for the Data Product. Omit to leave unchanged.
        description: New description for the Data Product. Omit to leave unchanged.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Update the description of an existing Data Product
        update_data_product(
            data_product_urn="urn:li:dataProduct:customer-360",
            description="Unified view of customer data across all touchpoints, including support tickets"
        )

        # Rename a Data Product
        update_data_product(
            data_product_urn="urn:li:dataProduct:customer-360",
            name="Customer 360 (v2)"
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not data_product_urn:
        raise ValueError("data_product_urn cannot be empty")
    if name is None and description is None:
        raise ValueError("At least one of name or description must be provided")
    if name is not None and not name.strip():
        raise ValueError("name cannot be empty")

    _validate_data_product_urn(client, data_product_urn)

    input_data: dict = {}
    if name is not None:
        input_data["name"] = name
    if description is not None:
        input_data["description"] = description

    mutation = """
        mutation updateDataProduct($urn: String!, $input: UpdateDataProductInput!) {
            updateDataProduct(urn: $urn, input: $input) {
                urn
            }
        }
    """

    _execute_urn_mutation(
        client,
        query=mutation,
        variables={"urn": data_product_urn, "input": input_data},
        operation_name="updateDataProduct",
        failure_message="Failed to update Data Product - mutation returned no urn",
        error_prefix="Error updating Data Product",
    )

    return {
        "success": True,
        "message": f"Successfully updated Data Product {data_product_urn}",
    }


@min_version(cloud="0.3.16", oss="1.4.0")
def delete_data_product(data_product_urn: str) -> dict:
    """Delete a Data Product by URN.

    Note: Assets do not need to be detached first. DataHub asynchronously clears
    asset associations after the Data Product entity itself is deleted.

    Args:
        data_product_urn: URN of the Data Product to delete

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        delete_data_product(data_product_urn="urn:li:dataProduct:customer-360")
    """
    client = graphql_helpers.get_datahub_client()

    if not data_product_urn:
        raise ValueError("data_product_urn cannot be empty")

    _validate_data_product_urn(client, data_product_urn)

    mutation = """
        mutation deleteDataProduct($urn: String!) {
            deleteDataProduct(urn: $urn)
        }
    """

    return _execute_bool_mutation(
        client,
        query=mutation,
        variables={"urn": data_product_urn},
        operation_name="deleteDataProduct",
        success_message=f"Successfully deleted Data Product {data_product_urn}",
        failure_message="Failed to delete Data Product - operation returned false",
        error_prefix="Error deleting Data Product",
    )


@min_version(cloud="0.3.16", oss="1.4.0")
def add_assets_to_data_product(
    data_product_urn: str,
    entity_urns: List[str],
) -> dict:
    """Attach one or more data assets to a Data Product.

    Note: DataHub instances may allow either multiple Data Products per asset, or
    only one at a time, depending on server configuration. This tool detects which
    mode is active:
    - If multiple Data Products per asset are allowed, assets are added to this
      Data Product in addition to any others they already belong to.
    - Otherwise, assigning this Data Product REPLACES any prior Data Product
      assignment on each asset. The returned message states which mode ran.

    Args:
        data_product_urn: URN of the Data Product to attach assets to
        entity_urns: List of asset URNs to attach (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message, noting whether assets were added
                   additively or replaced any prior Data Product assignment

    Examples:
        # Attach datasets to a Data Product
        add_assets_to_data_product(
            data_product_urn="urn:li:dataProduct:customer-360",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
            ]
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not data_product_urn:
        raise ValueError("data_product_urn cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    _validate_data_product_urn(client, data_product_urn)

    if _multiple_data_products_per_asset_enabled(client):
        mutation = """
            mutation batchAddToDataProducts($input: BatchSetDataProductsInput!) {
                batchAddToDataProducts(input: $input)
            }
        """
        variables: dict = {
            "input": {
                "dataProductUrns": [data_product_urn],
                "resourceUrns": entity_urns,
            }
        }
        operation_name = "batchAddToDataProducts"
        mode_message = "added to"
    else:
        mutation = """
            mutation batchSetDataProduct($input: BatchSetDataProductInput!) {
                batchSetDataProduct(input: $input)
            }
        """
        variables = {
            "input": {
                "dataProductUrn": data_product_urn,
                "resourceUrns": entity_urns,
            }
        }
        operation_name = "batchSetDataProduct"
        mode_message = "set on (replacing any prior Data Product assignment for)"

    return _execute_bool_mutation(
        client,
        query=mutation,
        variables=variables,
        operation_name=operation_name,
        success_message=(
            f"Successfully {mode_message} {len(entity_urns)} asset(s) "
            f"for Data Product {data_product_urn}"
        ),
        failure_message=(
            f"Failed to add assets to Data Product - {operation_name} returned false"
        ),
        error_prefix="Error adding assets to Data Product",
    )


@min_version(cloud="0.3.16", oss="1.4.0")
def remove_assets_from_data_product(
    data_product_urn: str,
    entity_urns: List[str],
) -> dict:
    """Detach one or more data assets from a Data Product.

    Note: DataHub instances may allow either multiple Data Products per asset, or
    only one at a time, depending on server configuration.
    - If multiple Data Products per asset are allowed, only this asset's
      association with THIS Data Product is removed; any other Data Products it
      belongs to are left untouched.
    - Otherwise, each asset can have at most one Data Product at a time. This
      tool only detaches assets that are currently assigned to THIS Data
      Product; assets that are not currently assigned to it are left unchanged
      and reported as skipped in the response message.

    Args:
        data_product_urn: URN of the Data Product to detach assets from
        entity_urns: List of asset URNs to detach (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message, noting any assets that were
                   skipped because they weren't assigned to this Data Product

    Examples:
        remove_assets_from_data_product(
            data_product_urn="urn:li:dataProduct:customer-360",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.deprecated_table,PROD)"
            ]
        )
    """
    client = graphql_helpers.get_datahub_client()

    if not data_product_urn:
        raise ValueError("data_product_urn cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    _validate_data_product_urn(client, data_product_urn)

    skipped_urns: List[str] = []

    if _multiple_data_products_per_asset_enabled(client):
        mutation = """
            mutation batchRemoveFromDataProducts($input: BatchSetDataProductsInput!) {
                batchRemoveFromDataProducts(input: $input)
            }
        """
        variables: dict = {
            "input": {
                "dataProductUrns": [data_product_urn],
                "resourceUrns": entity_urns,
            }
        }
        operation_name = "batchRemoveFromDataProducts"
        target_urns = entity_urns
    else:
        # Each asset can only have at most one Data Product in this mode, and the
        # only mutation available to unset it (batchSetDataProduct with a null
        # dataProductUrn) clears WHATEVER Data Product is currently set, with no
        # regard for which urn was requested. So we must check current membership
        # ourselves first, and only unset assets that actually belong to this
        # Data Product, otherwise we'd risk silently detaching an asset from a
        # different Data Product than the one the caller asked about.
        current_membership = _get_current_data_product_urns(client, entity_urns)
        target_urns = [
            urn
            for urn in entity_urns
            if current_membership.get(urn) == data_product_urn
        ]
        skipped_urns = [urn for urn in entity_urns if urn not in target_urns]

        if not target_urns:
            return {
                "success": True,
                "message": (
                    f"No assets were removed: none of the given asset(s) are "
                    f"currently assigned to Data Product {data_product_urn}."
                ),
            }

        mutation = """
            mutation batchSetDataProduct($input: BatchSetDataProductInput!) {
                batchSetDataProduct(input: $input)
            }
        """
        variables = {
            "input": {
                "dataProductUrn": None,
                "resourceUrns": target_urns,
            }
        }
        operation_name = "batchSetDataProduct"

    result = _execute_bool_mutation(
        client,
        query=mutation,
        variables=variables,
        operation_name=operation_name,
        success_message=(
            f"Successfully removed {len(target_urns)} asset(s) from "
            f"Data Product {data_product_urn}"
        ),
        failure_message=(
            f"Failed to remove assets from Data Product - {operation_name} returned false"
        ),
        error_prefix="Error removing assets from Data Product",
    )

    if skipped_urns:
        result["message"] += (
            f". Skipped {len(skipped_urns)} asset(s) not currently assigned to "
            f"this Data Product: {', '.join(skipped_urns)}"
        )

    return result
