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

        entity = result.get("entity")

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

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables={"input": input_data},
            operation_name="createDataProduct",
        )

        created = result.get("createDataProduct")
        if created and created.get("urn"):
            return {
                "success": True,
                "urn": created["urn"],
                "message": f"Successfully created Data Product '{name}' ({created['urn']})",
            }
        else:
            raise RuntimeError(
                "Failed to create Data Product - mutation returned no urn"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error creating Data Product: {str(e)}") from e


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

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables={"urn": data_product_urn, "input": input_data},
            operation_name="updateDataProduct",
        )

        if (result.get("updateDataProduct") or {}).get("urn"):
            return {
                "success": True,
                "message": f"Successfully updated Data Product {data_product_urn}",
            }
        else:
            raise RuntimeError(
                "Failed to update Data Product - mutation returned no urn"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error updating Data Product: {str(e)}") from e


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

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables={"urn": data_product_urn},
            operation_name="deleteDataProduct",
        )

        if result.get("deleteDataProduct", False):
            return {
                "success": True,
                "message": f"Successfully deleted Data Product {data_product_urn}",
            }
        else:
            raise RuntimeError(
                "Failed to delete Data Product - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error deleting Data Product: {str(e)}") from e


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

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables=variables,
            operation_name=operation_name,
        )

        if result.get(operation_name, False):
            return {
                "success": True,
                "message": (
                    f"Successfully {mode_message} {len(entity_urns)} asset(s) "
                    f"for Data Product {data_product_urn}"
                ),
            }
        else:
            raise RuntimeError(
                f"Failed to add assets to Data Product - {operation_name} returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error adding assets to Data Product: {str(e)}") from e


@min_version(cloud="0.3.16", oss="1.4.0")
def remove_assets_from_data_product(
    data_product_urn: str,
    entity_urns: List[str],
) -> dict:
    """Detach one or more data assets from a Data Product.

    Args:
        data_product_urn: URN of the Data Product to detach assets from
        entity_urns: List of asset URNs to detach (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

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
    else:
        mutation = """
            mutation batchSetDataProduct($input: BatchSetDataProductInput!) {
                batchSetDataProduct(input: $input)
            }
        """
        variables = {
            "input": {
                "dataProductUrn": None,
                "resourceUrns": entity_urns,
            }
        }
        operation_name = "batchSetDataProduct"

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=mutation,
            variables=variables,
            operation_name=operation_name,
        )

        if result.get(operation_name, False):
            return {
                "success": True,
                "message": (
                    f"Successfully removed {len(entity_urns)} asset(s) from "
                    f"Data Product {data_product_urn}"
                ),
            }
        else:
            raise RuntimeError(
                f"Failed to remove assets from Data Product - {operation_name} returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error removing assets from Data Product: {str(e)}") from e
