"""Tests for the SQL-like filter parser's error messages.

The caller here is usually a model, and it cannot ask a follow-up question. A
parse error is the only feedback it gets, so an error that names the problem but
not the fix costs a whole retry — or several, if the model keeps rephrasing a
filter that was never the problem.
"""

import pytest

from mcp_server_datahub.search_filter_parser import parse_filter_string


def test_empty_in_list_says_what_to_do_instead():
    """An empty IN list is a caller that interpolated an empty collection."""
    with pytest.raises(ValueError) as exc:
        parse_filter_string("platform IN ()")

    message = str(exc.value)
    assert "Empty IN list" in message
    assert "platform" in message, "the message should name the offending field"
    assert "drop the condition" in message


def test_empty_in_list_with_whitespace_is_still_empty():
    with pytest.raises(ValueError, match="Empty IN list"):
        parse_filter_string("entity_type IN ( )")


def test_a_single_value_in_list_is_fine():
    """The empty check must not swallow the smallest valid list."""
    parse_filter_string("platform IN (snowflake)")


def test_a_trailing_comma_is_still_a_missing_value_not_an_empty_list():
    """`IN (a,)` is a different mistake and should not claim the list is empty."""
    with pytest.raises(ValueError) as exc:
        parse_filter_string("platform IN (snowflake,)")
    assert "Empty IN list" not in str(exc.value)
