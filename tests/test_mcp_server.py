import json
from typing import Iterable

import pydantic
import pytest
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import Filter

from mcp_server_datahub.mcp_server import (
    get_dataset_queries,
    get_entity,
    get_lineage,
    search,
    with_client,
)

_test_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"
_test_domain = "urn:li:domain:0da1ef03-8870-45db-9f47-ef4f592f095c"


@pytest.fixture(autouse=True, scope="session")
def setup_client() -> Iterable[None]:
    with with_client(DataHubClient.from_env()):
        yield


def test_get_dataset() -> None:
    res = get_entity(_test_urn)
    assert res is not None

    assert res["url"] is not None


def test_get_domain() -> None:
    res = get_entity(_test_domain)
    assert res is not None

    assert res["url"] is not None


def test_get_lineage() -> None:
    res = get_lineage(_test_urn, upstream=True, max_hops=1)
    assert res is not None

    # Ensure that URL injection did something.
    assert "https://longtailcompanions.acryl.io/" in json.dumps(res)


def test_get_dataset_queries() -> None:
    res = get_dataset_queries(_test_urn)
    assert res is not None


def test_search() -> None:
    filters_json = {
        "and_": [
            {"entity_type": ["DATASET"]},
            {"entity_subtype": "Table"},
            {"platform": ["snowflake"]},
        ]
    }
    res = search(
        query="*",
        filters=pydantic.TypeAdapter(Filter).validate_python(filters_json),
    )
    assert res is not None


if __name__ == "__main__":
    import pytest

    pytest.main()
