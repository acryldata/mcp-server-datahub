"""Regression tests for Dataset properties.lastModified in the entity_details projection.

`lastModified` is what answers "when did this table last change?" — the freshness
question agents ask most often about a dataset. It is requested inside `properties`
for Dashboard, Chart, Document and BusinessAttribute in `entityPreview`, and appears
to have been missed for Dataset, so `get_entities` returned datasets without a
timestamp even where GMS holds one.

Two things have to hold for a consumer to actually receive it, and they fail
independently, so they are asserted separately:

  1. The projection requests it. Covered by the document tests below.
  2. It survives the response cleaning. `clean_gql_response` drops None, empty lists
     and (post-clean) empty dicts, and `lastModified` is an object — so "the fragment
     asks for it" does not by itself mean "the caller sees it". Covered by the path
     tests below, which run the real `get_entities` with only the GraphQL call stubbed.

The document tests assert against the GraphQL document rather than a live instance, so
they need no credentials and fail for the right reason: a projection regression, rather
than an unreachable environment.

`entity_details.gql` contains several `... on Dataset` inline fragments for different
purposes. The one `get_entities` resolves through is the one in `fragment entityPreview`,
so these tests target that fragment by name — a test scoped to "anywhere in the file"
would pass on an unrelated occurrence.
"""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background
from mcp_server_datahub.graphql_helpers import GQL_DIR

pytestmark = pytest.mark.anyio

ENTITY_DETAILS = (GQL_DIR / "entity_details.gql").read_text()

PREVIEW_FRAGMENT = "entityPreview"

_TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
_TEST_TIME = 1754130964271


def _balanced_block(document: str, opening: str, search_from: int = 0) -> str:
    """Return the brace-balanced body that follows `opening`.

    Regex cannot do this — the blocks nest — so this tracks depth. Raises rather than
    returning a partial body, so a malformed document fails loudly instead of silently
    satisfying an assertion.
    """
    start = document.index(opening, search_from) + len(opening)
    depth = 1
    for index in range(start, len(document)):
        char = document[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return document[start:index]
    raise AssertionError(f"unterminated block after {opening!r}")


def _dataset_properties_in_preview() -> str:
    """The body of Dataset.properties inside `fragment entityPreview`."""
    preview = _balanced_block(
        ENTITY_DETAILS, f"fragment {PREVIEW_FRAGMENT} on Entity {{"
    )
    dataset = _balanced_block(preview, "... on Dataset {")
    return _balanced_block(dataset, "properties {")


class TestDatasetLastModifiedIsRequested:
    """The projection asks for the field."""

    def test_dataset_properties_requests_last_modified(self) -> None:
        """The field a freshness question depends on is actually requested."""
        properties = _dataset_properties_in_preview()
        assert "lastModified" in properties, (
            f"Dataset.properties in fragment {PREVIEW_FRAGMENT} no longer requests "
            "lastModified. get_entities will return datasets without a timestamp "
            "even when GMS holds one."
        )

    def test_last_modified_requests_the_time_subfield(self) -> None:
        """`lastModified` is an AuditStamp, so the block has to name what it reads.

        Requesting the object without `time` would leave callers with a shape that
        carries no timestamp, which the field-presence test above cannot see.
        """
        properties = _dataset_properties_in_preview()
        last_modified = _balanced_block(properties, "lastModified {")
        assert "time" in last_modified, (
            "Dataset.properties.lastModified no longer requests `time`, so the "
            "timestamp is unreachable even though the field is projected."
        )

    def test_sibling_fields_still_present(self) -> None:
        """Guards against the block being emptied rather than the field added.

        Without this, deleting the whole properties body would leave the tests above
        failing for a misleading reason.
        """
        properties = _dataset_properties_in_preview()
        for field in ("name", "description", "customProperties"):
            assert field in properties, f"Dataset.properties lost {field}"

    def test_dataset_is_not_an_exception_among_timestamped_entities(self) -> None:
        """Dataset sits alongside the other entity types carrying a modification time.

        Asserted as a subset so adding lastModified to a further type does not fail
        here, while removing it from Dataset does.
        """
        preview = _balanced_block(
            ENTITY_DETAILS, f"fragment {PREVIEW_FRAGMENT} on Entity {{"
        )
        exposing = set()
        cursor = 0
        marker = "... on "
        while True:
            try:
                at = preview.index(marker, cursor)
            except ValueError:
                break
            type_name = preview[at + len(marker) : preview.index(" {", at)]
            body = _balanced_block(preview, f"... on {type_name} {{", at)
            if "lastModified" in body:
                exposing.add(type_name)
            cursor = at + len(marker)

        missing = {"Dashboard", "Chart", "Dataset"} - exposing
        assert not missing, f"entity types no longer requesting lastModified: {missing}"


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    client = Mock()
    client._graph = Mock()
    return client


def _dataset_response(last_modified: object) -> dict:
    """A Dataset entity shaped as entityPreview returns it, with `properties` populated."""
    return {
        "__typename": "Dataset",
        "urn": _TEST_URN,
        "name": "table",
        "properties": {
            "__typename": "DatasetProperties",
            "name": "table",
            "description": "A sample table",
            "lastModified": last_modified,
            "customProperties": [{"key": "env", "value": "PROD"}],
        },
    }


class TestDatasetLastModifiedSurvivesCleaning:
    """The field the projection asks for actually reaches the caller.

    `clean_gql_response` removes None, empty lists and post-clean empty dicts, and it
    runs over every `get_entities` response. These tests exercise the real code path
    with only the GraphQL call stubbed, so a cleaning change that dropped a populated
    timestamp would fail here rather than at a consumer.
    """

    async def test_populated_last_modified_reaches_the_caller(
        self, mock_client
    ) -> None:
        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.graphql_helpers.execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = [
                    {"entity": _dataset_response({"time": _TEST_TIME})},
                    {"entity": {}},
                ]

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(_TEST_URN)

                assert result["properties"]["lastModified"] == {"time": _TEST_TIME}

    async def test_absent_last_modified_is_still_omitted(self, mock_client) -> None:
        """A dataset GMS holds no timestamp for stays absent, as before.

        This pins the existing cleaning behaviour, so the test above cannot be
        satisfied by weakening the cleaning rather than by projecting the field.
        """
        with patch(
            "datahub_integrations.mcp.graphql_helpers.get_datahub_client",
            return_value=mock_client,
        ):
            mock_client._graph.exists.return_value = True

            with patch(
                "datahub_integrations.mcp.graphql_helpers.execute_graphql"
            ) as mock_gql:
                mock_gql.side_effect = [
                    {"entity": _dataset_response(None)},
                    {"entity": {}},
                ]

                from datahub_integrations.mcp.mcp_server import get_entities

                result = await async_background(get_entities)(_TEST_URN)

                assert "lastModified" not in result["properties"]
                assert result["properties"]["name"] == "table"
