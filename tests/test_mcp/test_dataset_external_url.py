"""Regression tests for Dataset.externalUrl in the entity_details projection.

`externalUrl` is populated on Dataset by ingestion sources that know where the
entity's source lives — the dbt source derives it from `git_info`, and pins it
to a commit when `branch` is set to a SHA. It is the only field carrying a
resolvable link from a dataset to the code that produces it.

It was requested for CorpGroup, Dashboard, Chart, Assertion and Document but not
for Dataset, so `get_entities` returned datasets without a source link even when
GMS held a value. These tests pin it into the projection so that cannot recur.

They assert against the GraphQL document rather than a live instance, so they
need no credentials and fail for the right reason: a projection regression,
rather than an unreachable environment.

`entity_details.gql` contains several `... on Dataset` inline fragments for
different purposes. The one `get_entities` resolves through is the one in
`fragment entityPreview`, so these tests target that fragment by name — a test
scoped to "anywhere in the file" would pass on an unrelated occurrence.
"""

from mcp_server_datahub.graphql_helpers import GQL_DIR

ENTITY_DETAILS = (GQL_DIR / "entity_details.gql").read_text()

PREVIEW_FRAGMENT = "entityPreview"


def _balanced_block(document: str, opening: str, search_from: int = 0) -> str:
    """Return the brace-balanced body that follows `opening`.

    Regex cannot do this — the blocks nest — so this tracks depth. Raises rather
    than returning a partial body, so a malformed document fails loudly instead
    of silently satisfying an assertion.
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
    preview = _balanced_block(ENTITY_DETAILS, f"fragment {PREVIEW_FRAGMENT} on Entity {{")
    dataset = _balanced_block(preview, "... on Dataset {")
    return _balanced_block(dataset, "properties {")


class TestDatasetExternalUrl:
    def test_dataset_properties_requests_external_url(self) -> None:
        """The field get_entities depends on is actually requested."""
        properties = _dataset_properties_in_preview()
        assert "externalUrl" in properties, (
            f"Dataset.properties in fragment {PREVIEW_FRAGMENT} no longer requests "
            "externalUrl. get_entities will return datasets without a source link "
            "even when GMS holds one."
        )

    def test_sibling_fields_still_present(self) -> None:
        """Guards against the block being emptied rather than the field added.

        Without this, deleting the whole properties body would leave the test
        above failing for a misleading reason.
        """
        properties = _dataset_properties_in_preview()
        for field in ("name", "description", "customProperties"):
            assert field in properties, f"Dataset.properties lost {field}"

    def test_dataset_is_not_an_exception_among_linkable_entities(self) -> None:
        """Dataset sits alongside the other entity types carrying a source link.

        Asserted as a subset so adding externalUrl to a further type does not
        fail here, while removing it from Dataset does.
        """
        preview = _balanced_block(ENTITY_DETAILS, f"fragment {PREVIEW_FRAGMENT} on Entity {{")
        exposing = set()
        cursor = 0
        while True:
            marker = "... on "
            try:
                at = preview.index(marker, cursor)
            except ValueError:
                break
            type_name = preview[at + len(marker) : preview.index(" {", at)]
            body = _balanced_block(preview, f"... on {type_name} {{", at)
            if "externalUrl" in body:
                exposing.add(type_name)
            cursor = at + len(marker)

        missing = {"Dashboard", "Chart", "Dataset"} - exposing
        assert not missing, f"entity types no longer requesting externalUrl: {missing}"
