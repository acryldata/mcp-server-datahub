"""Tests for deciding whether a server serves the #[NEWER_GMS] fields.

The regression these cover: the decision used to be "is this DataHub Cloud?",
which stripped every #[NEWER_GMS] field from queries sent to a self-hosted
server no matter how new it was. Because the entire ``... on Document`` block in
``entity_details.gql`` is #[NEWER_GMS]-tagged, ``get_entities`` on a Document URN
came back as ``{"urn": ...}`` with no title and no contents on OSS -- while the
same server answered the same query in full when the fields were left in.
"""

from unittest.mock import MagicMock, PropertyMock, patch

from datahub_integrations.mcp.mcp_server import _supports_newer_gms_fields


def _graph(version: tuple[int, int, int, int] | None, *, cloud: bool) -> MagicMock:
    """A stand-in DataHubGraph reporting a given version and deployment type."""
    graph = MagicMock()
    config = MagicMock()
    config.parsed_version = version
    config.is_version_at_least.side_effect = lambda *want: (
        version or (0, 0, 0, 0)
    ) >= tuple(list(want) + [0] * (4 - len(want)))
    type(graph).server_config = PropertyMock(return_value=config)
    return graph


def test_oss_on_a_new_enough_server_gets_the_newer_fields():
    """The reported bug: OSS 1.5.0.6 serves these fields and was denied them."""
    assert (
        _supports_newer_gms_fields(_graph((1, 5, 0, 6), cloud=False), is_cloud=False)
        is True
    )


def test_oss_on_an_older_server_does_not():
    assert (
        _supports_newer_gms_fields(_graph((1, 4, 0, 0), cloud=False), is_cloud=False)
        is False
    )


def test_cloud_is_unchanged_and_does_not_depend_on_the_version_lookup():
    """Cloud took this path before and must keep taking it, version or no version."""
    graph = _graph(None, cloud=True)
    assert _supports_newer_gms_fields(graph, is_cloud=True) is True


def test_the_opt_out_still_wins():
    with patch(
        "mcp_server_datahub.graphql_helpers.get_boolean_env_variable", return_value=True
    ):
        assert (
            _supports_newer_gms_fields(
                _graph((1, 5, 0, 6), cloud=False), is_cloud=False
            )
            is False
        )


def test_an_unreadable_config_falls_back_instead_of_raising():
    """A server whose /config cannot be read must not break field selection."""
    graph = MagicMock()
    type(graph).server_config = PropertyMock(
        side_effect=Exception("connection refused")
    )
    assert _supports_newer_gms_fields(graph, is_cloud=False) is False
