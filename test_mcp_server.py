import pydantic
from datahub.sdk.search_filters import Filter

from mcp_server import get_dataset_queries, get_entity, get_lineage, search

_test_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"


def test_get_entity() -> None:
    res = get_entity(_test_urn)
    assert res is not None


def test_get_lineage() -> None:
    res = get_lineage(_test_urn, upstream=True, num_hops=1)
    assert res is not None


def test_get_dataset_queries() -> None:
    res = get_dataset_queries(_test_urn)
    assert res is not None


def test_search() -> None:
    filters_json = {
        "and_": [
            {"entity_type": ["DATASET"]},
            {"entity_type": "dataset", "entity_subtype": "Table"},
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
