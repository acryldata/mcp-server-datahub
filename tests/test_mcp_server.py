import json
from typing import AsyncGenerator, Iterable

import pytest
from datahub.sdk.main_client import DataHubClient
from fastmcp import Client

from mcp_server_datahub.mcp_server import (
    get_dataset_queries,
    get_entity,
    get_lineage,
    mcp,
    with_datahub_client,
)

_test_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"
_test_domain = "urn:li:domain:0da1ef03-8870-45db-9f47-ef4f592f095c"


@pytest.fixture(autouse=True, scope="session")
def setup_client() -> Iterable[None]:
    with with_datahub_client(DataHubClient.from_env()):
        yield


@pytest.fixture
async def mcp_client() -> AsyncGenerator[Client, None]:
    async with Client(mcp) as mcp_client:
        yield mcp_client


@pytest.mark.anyio
async def test_list_tools(mcp_client: Client) -> None:
    tools = await mcp_client.list_tools()
    assert len(tools) > 0


def test_get_dataset() -> None:
    res = get_entity.fn(_test_urn)
    assert res is not None

    assert res["url"] is not None


def test_get_domain() -> None:
    res = get_entity.fn(_test_domain)
    assert res is not None

    assert res["url"] is not None


def test_get_lineage() -> None:
    res = get_lineage.fn(_test_urn, upstream=True, max_hops=1)
    assert res is not None

    # Ensure that URL injection did something.
    assert "https://longtailcompanions.acryl.io/" in json.dumps(res)


def test_get_dataset_queries() -> None:
    res = get_dataset_queries.fn(_test_urn)
    assert res is not None


@pytest.mark.anyio
async def test_search(mcp_client: Client) -> None:
    filters_json = {
        "and": [
            {"entity_type": ["DATASET"]},
            {"entity_subtype": "Table"},
            {"not": {"platform": ["snowflake"]}},
        ]
    }
    res = await mcp_client.call_tool(
        "search",
        arguments={"query": "*", "filters": filters_json},
    )
    assert res.is_error is False
    assert res.data is not None


if __name__ == "__main__":
    import pytest

    pytest.main()
