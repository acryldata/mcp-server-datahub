"""Regression tests for the entity details GraphQL fragments."""

from datahub_integrations.mcp import graphql_helpers


def _extract_block(text: str, header: str, start: int = 0) -> str:
    """Return the brace-delimited block that begins at `header` (searching from `start`)."""
    begin = text.index(header, start)
    depth = 0
    for i in range(text.index("{", begin), len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[begin : i + 1]
    raise AssertionError(f"Unbalanced braces after {header!r}")


def test_data_product_preview_does_not_query_entities():
    """DataProduct.entities crashes on some GMS versions (e.g. OSS v1.6.0) with a 500,
    which fails get_entities() for every DataProduct URN. See issue #221."""
    gql = (graphql_helpers.GQL_DIR / "entity_details.gql").read_text()

    entity_preview = _extract_block(gql, "fragment entityPreview on Entity")
    data_product = _extract_block(entity_preview, "... on DataProduct")

    assert "properties" in data_product
    assert "entities(" not in data_product
