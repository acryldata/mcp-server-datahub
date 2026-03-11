"""Unit tests for per-request auth token extraction and _DataHubClientMiddleware."""

import os
from typing import AsyncGenerator
from unittest.mock import MagicMock, patch

import httpx
import pytest

import mcp_server_datahub.__main__  # noqa: F401 -- registers /health route on mcp
from mcp_server_datahub.__main__ import (
    _DataHubClientMiddleware,
    _token_from_request,
    create_app,
)
from mcp_server_datahub.mcp_server import mcp


# ---------------------------------------------------------------------------
# _token_from_request
# ---------------------------------------------------------------------------


def _make_mock_request(headers: dict, query_params: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {k.lower(): v for k, v in headers.items()}
    req.query_params = query_params or {}
    return req


def test_token_from_bearer_header() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({"Authorization": "Bearer mytoken123"}),
    ):
        assert _token_from_request() == "mytoken123"


def test_token_from_query_param() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({}, {"token": "querytoken"}),
    ):
        assert _token_from_request() == "querytoken"


def test_bearer_header_takes_priority_over_query_param() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request(
            {"Authorization": "Bearer headertoken"}, {"token": "querytoken"}
        ),
    ):
        assert _token_from_request() == "headertoken"


def test_no_token_returns_none() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({}),
    ):
        assert _token_from_request() is None


def test_no_http_request_returns_none() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        side_effect=RuntimeError("No active HTTP request found."),
    ):
        assert _token_from_request() is None


def test_empty_query_token_returns_none() -> None:
    with patch(
        "mcp_server_datahub.__main__.get_http_request",
        return_value=_make_mock_request({}, {"token": ""}),
    ):
        assert _token_from_request() is None


# ---------------------------------------------------------------------------
# _DataHubClientMiddleware._client_for_request
# ---------------------------------------------------------------------------


def _make_middleware(
    server_url: str = "http://datahub:8080",
    default_token: str | None = None,
) -> _DataHubClientMiddleware:
    default_client = None
    if default_token is not None:
        default_client = MagicMock()
    return _DataHubClientMiddleware(server_url, default_client)


def test_per_request_token_returns_new_client() -> None:
    middleware = _make_middleware(default_token="default")
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value="per-request"
    ):
        with patch("mcp_server_datahub.__main__.DataHubClient") as MockClient:
            client = middleware._client_for_request()
            MockClient.assert_called_once()
            call_config = MockClient.call_args.kwargs["config"]
            assert call_config.token == "per-request"
            assert call_config.server == "http://datahub:8080"
            assert client is MockClient.return_value


def test_falls_back_to_default_client_when_no_request_token() -> None:
    default_client = MagicMock()
    middleware = _DataHubClientMiddleware("http://datahub:8080", default_client)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value=None
    ):
        result = middleware._client_for_request()
        assert result is default_client


def test_raises_when_no_token_and_no_default_client() -> None:
    middleware = _DataHubClientMiddleware("http://datahub:8080", None)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value=None
    ):
        with pytest.raises(ValueError, match="No DataHub token provided"):
            middleware._client_for_request()


def test_per_request_token_does_not_use_default_client() -> None:
    default_client = MagicMock()
    middleware = _DataHubClientMiddleware("http://datahub:8080", default_client)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value="req-token"
    ):
        with patch("mcp_server_datahub.__main__.DataHubClient") as MockClient:
            result = middleware._client_for_request()
            assert result is MockClient.return_value
            # default_client should never be returned
            assert result is not default_client


# ---------------------------------------------------------------------------
# create_app — startup behaviour
# ---------------------------------------------------------------------------


def test_create_app_requires_datahub_gms_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    # Reset idempotency guard so create_app actually runs
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with pytest.raises(RuntimeError, match="DATAHUB_GMS_URL"):
            create_app()
    finally:
        main_mod._app_initialized = original


def test_create_app_no_token_builds_no_default_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with patch("mcp_server_datahub.__main__._DataHubClientMiddleware") as MockMW:
            with patch.object(mcp, "add_middleware"):
                create_app()
            _, kwargs = MockMW.call_args if MockMW.call_args else (None, {})
            args = MockMW.call_args.args if MockMW.call_args else ()
            # Second positional arg is default_client — should be None
            assert args[1] is None
    finally:
        main_mod._app_initialized = original


def test_create_app_with_token_builds_default_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.setenv("DATAHUB_GMS_URL", "http://datahub:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "globaltoken")
    original = main_mod._app_initialized
    main_mod._app_initialized = False
    try:
        with patch("mcp_server_datahub.__main__.DataHubClient") as MockClient:
            with patch("mcp_server_datahub.__main__._DataHubClientMiddleware") as MockMW:
                with patch.object(mcp, "add_middleware"):
                    create_app()
            MockClient.assert_called_once()
            call_config = MockClient.call_args.kwargs["config"]
            assert call_config.token == "globaltoken"
            assert call_config.server == "http://datahub:8080"
            args = MockMW.call_args.args
            assert args[1] is MockClient.return_value
    finally:
        main_mod._app_initialized = original
