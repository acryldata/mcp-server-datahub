"""Tests for Data Product management tools."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.data_products import (
    add_assets_to_data_product,
    create_data_product,
    delete_data_product,
    remove_assets_from_data_product,
    update_data_product,
)


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client."""
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


# create_data_product tests


def test_create_data_product_success(mock_datahub_client):
    """Test creating a Data Product under an existing domain."""
    domain_urn = "urn:li:domain:marketing"

    mock_datahub_client._graph.execute_graphql.side_effect = [
        # First call: domain validation
        {
            "entity": {
                "urn": domain_urn,
                "type": "DOMAIN",
                "properties": {"name": "Marketing"},
            }
        },
        # Second call: createDataProduct mutation
        {"createDataProduct": {"urn": "urn:li:dataProduct:customer-360"}},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = create_data_product(
            name="Customer 360", domain_urn=domain_urn, description="Unified view"
        )

    assert result["success"] is True
    assert result["urn"] == "urn:li:dataProduct:customer-360"

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "createDataProduct"
    assert mutation_call.kwargs["variables"]["input"]["domainUrn"] == domain_urn
    assert mutation_call.kwargs["variables"]["input"]["properties"]["name"] == (
        "Customer 360"
    )
    assert mutation_call.kwargs["variables"]["input"]["properties"]["description"] == (
        "Unified view"
    )


def test_create_data_product_with_custom_id(mock_datahub_client):
    """Test creating a Data Product with a custom id."""
    domain_urn = "urn:li:domain:sales"

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": domain_urn, "type": "DOMAIN"}},
        {"createDataProduct": {"urn": "urn:li:dataProduct:order-analytics"}},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = create_data_product(
            name="Order Analytics", domain_urn=domain_urn, id="order-analytics"
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["variables"]["input"]["id"] == "order-analytics"


def test_create_data_product_empty_name(mock_datahub_client):
    """Test that empty name raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="name cannot be empty"):
            create_data_product(name="", domain_urn="urn:li:domain:marketing")


def test_create_data_product_empty_domain_urn(mock_datahub_client):
    """Test that empty domain_urn raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="domain_urn cannot be empty"):
            create_data_product(name="Customer 360", domain_urn="")


def test_create_data_product_nonexistent_domain(mock_datahub_client):
    """Test that a nonexistent domain URN returns an error."""
    mock_datahub_client._graph.execute_graphql.return_value = {"entity": None}

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Domain URN does not exist"):
            create_data_product(
                name="Customer 360", domain_urn="urn:li:domain:nonexistent"
            )


def test_create_data_product_mutation_returns_no_urn(mock_datahub_client):
    """Test handling when the create mutation returns no urn."""
    domain_urn = "urn:li:domain:marketing"
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": domain_urn, "type": "DOMAIN"}},
        {"createDataProduct": None},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed to create Data Product"):
            create_data_product(name="Customer 360", domain_urn=domain_urn)


# update_data_product tests


def test_update_data_product_success(mock_datahub_client):
    """Test updating a Data Product's description."""
    dp_urn = "urn:li:dataProduct:customer-360"
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"updateDataProduct": {"urn": dp_urn}},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = update_data_product(
            data_product_urn=dp_urn, description="Updated description"
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["variables"]["input"]["description"] == (
        "Updated description"
    )
    assert "name" not in mutation_call.kwargs["variables"]["input"]


def test_update_data_product_no_fields_provided(mock_datahub_client):
    """Test that omitting both name and description raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="At least one of name or description"):
            update_data_product(data_product_urn="urn:li:dataProduct:customer-360")


def test_update_data_product_nonexistent(mock_datahub_client):
    """Test that a nonexistent Data Product URN returns an error."""
    mock_datahub_client._graph.execute_graphql.return_value = {"entity": None}

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Data Product URN does not exist"):
            update_data_product(
                data_product_urn="urn:li:dataProduct:nonexistent", name="New Name"
            )


# delete_data_product tests


def test_delete_data_product_success(mock_datahub_client):
    """Test deleting a Data Product."""
    dp_urn = "urn:li:dataProduct:customer-360"
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"deleteDataProduct": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = delete_data_product(data_product_urn=dp_urn)

    assert result["success"] is True


def test_delete_data_product_empty_urn(mock_datahub_client):
    """Test that empty data_product_urn raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="data_product_urn cannot be empty"):
            delete_data_product(data_product_urn="")


def test_delete_data_product_mutation_returns_false(mock_datahub_client):
    """Test handling when the delete mutation returns false."""
    dp_urn = "urn:li:dataProduct:customer-360"
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"deleteDataProduct": False},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed to delete Data Product"):
            delete_data_product(data_product_urn=dp_urn)


# add_assets_to_data_product / remove_assets_from_data_product tests


def test_add_assets_multiple_dp_per_asset_enabled(mock_datahub_client):
    """When the feature flag is on, assets are added via batchAddToDataProducts."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},  # validate DP
        {"appConfig": {"featureFlags": {"multipleDataProductsPerAsset": True}}},
        {"batchAddToDataProducts": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_assets_to_data_product(
            data_product_urn=dp_urn, entity_urns=[dataset_urn]
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    assert mutation_call.kwargs["operation_name"] == "batchAddToDataProducts"
    assert mutation_call.kwargs["variables"]["input"]["dataProductUrns"] == [dp_urn]
    assert mutation_call.kwargs["variables"]["input"]["resourceUrns"] == [dataset_urn]


def test_add_assets_multiple_dp_per_asset_disabled(mock_datahub_client):
    """When the feature flag is off, assets are added via batchSetDataProduct."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"appConfig": {"featureFlags": {"multipleDataProductsPerAsset": False}}},
        {"batchSetDataProduct": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_assets_to_data_product(
            data_product_urn=dp_urn, entity_urns=[dataset_urn]
        )

    assert result["success"] is True
    assert "replacing any prior Data Product assignment" in result["message"]
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    assert mutation_call.kwargs["operation_name"] == "batchSetDataProduct"
    assert mutation_call.kwargs["variables"]["input"]["dataProductUrn"] == dp_urn
    assert mutation_call.kwargs["variables"]["input"]["resourceUrns"] == [dataset_urn]


def test_add_assets_feature_flag_check_fails_closed(mock_datahub_client):
    """When the feature-flag query itself errors, fall back to batchSetDataProduct."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    # Use a generic error (not a field-validation-style message) so the real
    # graphql_helpers.execute_graphql wrapper doesn't itself retry the query --
    # this test is about our own fail-closed handling, not that retry path.
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        Exception("Network timeout contacting GMS"),
        {"batchSetDataProduct": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_assets_to_data_product(
            data_product_urn=dp_urn, entity_urns=[dataset_urn]
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    assert mutation_call.kwargs["operation_name"] == "batchSetDataProduct"


def test_add_assets_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            add_assets_to_data_product(
                data_product_urn="urn:li:dataProduct:customer-360", entity_urns=[]
            )


def test_remove_assets_multiple_dp_per_asset_enabled(mock_datahub_client):
    """When the feature flag is on, assets are removed via batchRemoveFromDataProducts."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"appConfig": {"featureFlags": {"multipleDataProductsPerAsset": True}}},
        {"batchRemoveFromDataProducts": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_assets_from_data_product(
            data_product_urn=dp_urn, entity_urns=[dataset_urn]
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    assert mutation_call.kwargs["operation_name"] == "batchRemoveFromDataProducts"
    assert mutation_call.kwargs["variables"]["input"]["dataProductUrns"] == [dp_urn]


def test_remove_assets_multiple_dp_per_asset_disabled(mock_datahub_client):
    """When the feature flag is off, assets are unset via batchSetDataProduct(null)."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"appConfig": {"featureFlags": {"multipleDataProductsPerAsset": False}}},
        {"batchSetDataProduct": True},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_assets_from_data_product(
            data_product_urn=dp_urn, entity_urns=[dataset_urn]
        )

    assert result["success"] is True
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    assert mutation_call.kwargs["operation_name"] == "batchSetDataProduct"
    assert mutation_call.kwargs["variables"]["input"]["dataProductUrn"] is None


def test_remove_assets_mutation_returns_false(mock_datahub_client):
    """Test handling when the remove mutation returns false."""
    dp_urn = "urn:li:dataProduct:customer-360"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    )

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": dp_urn, "type": "DATA_PRODUCT"}},
        {"appConfig": {"featureFlags": {"multipleDataProductsPerAsset": False}}},
        {"batchSetDataProduct": False},
    ]

    with patch(
        "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed to remove assets"):
            remove_assets_from_data_product(
                data_product_urn=dp_urn, entity_urns=[dataset_urn]
            )
