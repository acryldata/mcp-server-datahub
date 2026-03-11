"""Unit tests for per-request auth token extraction and _DataHubClientMiddleware."""

from unittest.mock import MagicMock, patch

import pytest

import mcp_server_datahub.__main__  # noqa: F401 -- registers /health route on mcp
from mcp_server_datahub.__main__ import (
    _DataHubClientMiddleware,
    _DataHubTokenVerifier,
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
        with patch("mcp_server_datahub.__main__._build_client") as mock_build:
            client = middleware._client_for_request()
            mock_build.assert_called_once_with("http://datahub:8080", "per-request")
            assert client is mock_build.return_value


def test_falls_back_to_default_client_when_no_request_token() -> None:
    default_client = MagicMock()
    middleware = _DataHubClientMiddleware("http://datahub:8080", default_client)
    with patch("mcp_server_datahub.__main__._token_from_request", return_value=None):
        result = middleware._client_for_request()
        assert result is default_client


def test_raises_when_no_token_and_no_default_client() -> None:
    middleware = _DataHubClientMiddleware("http://datahub:8080", None)
    with patch("mcp_server_datahub.__main__._token_from_request", return_value=None):
        with pytest.raises(ValueError, match="No DataHub token provided"):
            middleware._client_for_request()


def test_per_request_token_does_not_use_default_client() -> None:
    default_client = MagicMock()
    middleware = _DataHubClientMiddleware("http://datahub:8080", default_client)
    with patch(
        "mcp_server_datahub.__main__._token_from_request", return_value="req-token"
    ):
        with patch("mcp_server_datahub.__main__._build_client") as mock_build:
            result = middleware._client_for_request()
            assert result is mock_build.return_value
            assert result is not default_client


# ---------------------------------------------------------------------------
# _DataHubTokenVerifier
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_token_verifier_returns_access_token_on_valid_token() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch("mcp_server_datahub.__main__._build_client") as mock_build:
        with patch("mcp_server_datahub.__main__._verify_client"):
            result = await verifier.verify_token("good-token")
    assert result is not None
    assert result.token == "good-token"
    mock_build.assert_called_once_with("http://datahub:8080", "good-token")


@pytest.mark.anyio
async def test_token_verifier_returns_none_on_invalid_token() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch(
        "mcp_server_datahub.__main__._verify_client",
        side_effect=Exception("401 Unauthorized"),
    ):
        with patch("mcp_server_datahub.__main__._build_client"):
            result = await verifier.verify_token("bad-token")
    assert result is None


@pytest.mark.anyio
async def test_token_verifier_returns_none_on_any_exception() -> None:
    verifier = _DataHubTokenVerifier("http://datahub:8080")
    with patch(
        "mcp_server_datahub.__main__._verify_client",
        side_effect=RuntimeError("connection refused"),
    ):
        with patch("mcp_server_datahub.__main__._build_client"):
            result = await verifier.verify_token("some-token")
    assert result is None


# ---------------------------------------------------------------------------
# create_app — startup behaviour
# ---------------------------------------------------------------------------


def test_create_app_requires_datahub_gms_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import mcp_server_datahub.__main__ as main_mod

    monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
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
            args = MockMW.call_args.args if MockMW.call_args else ()
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
        with patch("mcp_server_datahub.__main__._build_client") as mock_build:
            with patch("mcp_server_datahub.__main__._verify_client"):
                with patch(
                    "mcp_server_datahub.__main__._DataHubClientMiddleware"
                ) as MockMW:
                    with patch.object(mcp, "add_middleware"):
                        create_app()
            mock_build.assert_called_once_with("http://datahub:8080", "globaltoken")
            args = MockMW.call_args.args
            assert args[1] is mock_build.return_value
    finally:
        main_mod._app_initialized = original
