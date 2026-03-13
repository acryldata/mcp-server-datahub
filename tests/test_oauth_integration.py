"""Integration tests for OAuth 2.1 authentication flow.

Tests the complete OAuth flow including:
- OAuth discovery endpoints
- Authorization proxy
- Token exchange
- Bearer token detection middleware
- OIDC token validator
- Error scenarios and edge cases
"""

import json
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlencode, urlparse

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from mcp_server_datahub.oauth_endpoints import (
    BearerTokenDetectionMiddleware,
    _OAuth2Session,
    _pop_session,
    _store_session,
    authorization_server_metadata,
    oauth_callback,
    protected_resource_metadata,
    register_client,
    token_proxy,
)


class TestOAuthDiscoveryEndpoints:
    """Test OAuth 2.1 discovery endpoints."""

    @pytest.fixture
    def mock_request(self):
        request = MagicMock(spec=Request)
        # Create a proper mock for headers
        mock_headers = MagicMock()
        mock_headers.get.return_value = "example.com"
        request.headers = mock_headers
        return request

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_authorization_server_metadata(
        self, mock_get_server_url, mock_request
    ):
        """Test OAuth authorization server metadata endpoint."""
        mock_get_server_url.return_value = "https://example.com"

        response = await authorization_server_metadata(mock_request)

        assert isinstance(response, JSONResponse)
        content = json.loads(response.body)

        assert content["issuer"] == "https://example.com"
        assert (
            content["authorization_endpoint"] == "https://example.com/oauth/authorize"
        )
        assert content["token_endpoint"] == "https://example.com/oauth/token"
        assert content["registration_endpoint"] == "https://example.com/oauth/register"
        assert "code" in content["response_types_supported"]
        assert "authorization_code" in content["grant_types_supported"]

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_protected_resource_metadata(self, mock_get_server_url, mock_request):
        """Test OAuth protected resource metadata endpoint."""
        mock_get_server_url.return_value = "https://example.com"

        response = await protected_resource_metadata(mock_request)

        assert isinstance(response, JSONResponse)
        content = json.loads(response.body)

        assert content["resource"] == "https://example.com"
        assert content["authorization_servers"] == ["https://example.com"]
        assert "header" in content["bearer_methods_supported"]

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client-123"})
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_register_client(self, mock_get_server_url, mock_request):
        """Test dynamic client registration endpoint."""
        mock_get_server_url.return_value = "https://example.com"
        mock_request.json = AsyncMock(
            return_value={"redirect_uris": ["http://localhost:8000/callback"]}
        )

        response = await register_client(mock_request)

        assert isinstance(response, JSONResponse)
        content = json.loads(response.body)

        assert content["client_id"] == "test-client-123"
        assert "https://example.com/oauth/callback" in content["redirect_uris"]
        assert content["token_endpoint_auth_method"] == "none"


class TestOAuthFlow:
    """Test OAuth authorization and token exchange flow."""

    @pytest.fixture
    def mock_request(self):
        request = MagicMock(spec=Request)
        # Create a proper mock for headers
        mock_headers = MagicMock()
        mock_headers.get.return_value = "example.com"
        request.headers = mock_headers
        return request

    def test_session_storage(self):
        """Test OAuth session storage and retrieval."""
        # Store a session
        _store_session("state123", "http://client/callback", "client123", "openid")

        # Retrieve the session
        session = _pop_session("state123")
        assert session is not None
        assert session.original_redirect_uri == "http://client/callback"
        assert session.client_id == "client123"
        assert session.scope == "openid"

        # Session should be gone after pop
        assert _pop_session("state123") is None

    def test_session_expiry(self):
        """Test OAuth session expiration."""
        # Create a session with a timestamp in the past
        old_timestamp = time.time() - 400  # 400 seconds ago (> 300s TTL)

        # Store session with old timestamp
        from mcp_server_datahub.oauth_endpoints import _sessions

        _sessions["expired"] = _OAuth2Session(
            original_redirect_uri="http://client/callback",
            client_id="client",
            scope="openid",
            timestamp=old_timestamp,
        )

        # Try to retrieve - should be expired and return None
        session = _pop_session("expired")
        assert session is None

    @patch.dict(
        os.environ,
        {
            "OIDC_CLIENT_ID": "test-client",
            "OAUTH_AUTHORIZE_ENDPOINT": "https://idp.example.com/auth",
        },
    )
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_authorize_proxy(self, mock_get_server_url, mock_request):
        """Test authorization request proxying."""
        mock_get_server_url.return_value = "https://example.com"
        mock_request.query_params = {
            "client_id": "test-client",
            "redirect_uri": "http://client/callback",
            "state": "state123",
            "scope": "openid profile",
            "response_type": "code",
        }

        from mcp_server_datahub.oauth_endpoints import authorize_proxy

        response = await authorize_proxy(mock_request)

        assert isinstance(response, RedirectResponse)

        # Parse the redirect URL
        redirect_url = response.headers["location"]
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)

        assert parsed.scheme == "https"
        assert parsed.netloc == "idp.example.com"
        assert parsed.path == "/auth"
        assert params["client_id"][0] == "test-client"
        assert params["redirect_uri"][0] == "https://example.com/oauth/callback"
        assert params["state"][0] == "state123"

    async def test_oauth_callback_success(self, mock_request):
        """Test successful OAuth callback handling."""
        # Store session first
        _store_session("state123", "http://client/callback", "client123", "openid")

        mock_request.query_params = {"code": "auth-code-123", "state": "state123"}

        response = await oauth_callback(mock_request)

        assert isinstance(response, RedirectResponse)
        redirect_url = response.headers["location"]
        assert redirect_url.startswith("http://client/callback")
        assert "code=auth-code-123" in redirect_url
        assert "state=state123" in redirect_url

    async def test_oauth_callback_error(self, mock_request):
        """Test OAuth callback error handling."""
        mock_request.query_params = {
            "error": "access_denied",
            "error_description": "User denied access",
            "state": "state123",
        }

        response = await oauth_callback(mock_request)

        assert isinstance(response, JSONResponse)
        assert response.status_code == 400
        content = json.loads(response.body)
        assert content["error"] == "access_denied"

    async def test_oauth_callback_invalid_state(self, mock_request):
        """Test OAuth callback with invalid state."""
        mock_request.query_params = {"code": "auth-code-123", "state": "invalid-state"}

        response = await oauth_callback(mock_request)

        assert isinstance(response, JSONResponse)
        assert response.status_code == 400
        content = json.loads(response.body)
        assert content["error"] == "invalid_state"

    @patch.dict(os.environ, {"OAUTH_TOKEN_ENDPOINT": "https://idp.example.com/token"})
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_token_proxy_success(self, mock_get_server_url, mock_request):
        """Test successful token exchange proxying."""
        mock_get_server_url.return_value = "https://example.com"

        # Mock the request body
        form_data = {
            "grant_type": "authorization_code",
            "code": "auth-code-123",
            "redirect_uri": "http://client/callback",
        }
        mock_request.body = AsyncMock(return_value=urlencode(form_data).encode())

        # Mock successful IdP response
        mock_token_response = {
            "access_token": "access-token-123",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_token_response
            mock_client.post.return_value = mock_response

            response = await token_proxy(mock_request)

        assert isinstance(response, JSONResponse)
        content = json.loads(response.body)
        assert content["access_token"] == "access-token-123"
        assert response.headers["cache-control"] == "no-store"

    @patch.dict(os.environ, {"OAUTH_TOKEN_ENDPOINT": "https://idp.example.com/token"})
    @patch("mcp_server_datahub.oauth_endpoints._get_server_url")
    async def test_token_proxy_idp_error(self, mock_get_server_url, mock_request):
        """Test token exchange with IdP error."""
        mock_get_server_url.return_value = "https://example.com"

        form_data = {"grant_type": "authorization_code", "code": "invalid-code"}
        mock_request.body = AsyncMock(return_value=urlencode(form_data).encode())

        # Mock IdP error response
        mock_error_response = {
            "error": "invalid_grant",
            "error_description": "Invalid authorization code",
        }

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.json.return_value = mock_error_response
            mock_client.post.return_value = mock_response

            response = await token_proxy(mock_request)

        assert isinstance(response, JSONResponse)
        assert response.status_code == 400
        content = json.loads(response.body)
        assert content["error"] == "invalid_grant"


class TestBearerTokenDetectionMiddleware:
    """Test Bearer token detection middleware."""

    @pytest.fixture
    def mock_app(self):
        async def app(scope, receive, send):
            response = JSONResponse({"message": "success"})
            await response(scope, receive, send)

        return app

    @pytest.fixture
    def mock_receive(self):
        return AsyncMock()

    @pytest.fixture
    def mock_send(self):
        return AsyncMock()

    @patch.dict(os.environ, {}, clear=True)
    async def test_middleware_no_oauth_configured(
        self, mock_app, mock_receive, mock_send
    ):
        """Test middleware passes through when OAuth not configured."""
        middleware = BearerTokenDetectionMiddleware(mock_app)

        scope = {"type": "http", "path": "/", "method": "GET", "headers": []}
        await middleware(scope, mock_receive, mock_send)

        # Should have called the app directly
        assert mock_send.called

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    async def test_middleware_with_bearer_token(
        self, mock_app, mock_receive, mock_send
    ):
        """Test middleware passes through with valid Bearer token."""
        middleware = BearerTokenDetectionMiddleware(mock_app, has_fallback_client=False)

        scope = {
            "type": "http",
            "path": "/",
            "method": "GET",
            "headers": [(b"authorization", b"Bearer valid-token")],
        }
        await middleware(scope, mock_receive, mock_send)

        # Should have called the app
        assert mock_send.called

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    async def test_middleware_no_token_no_fallback(
        self, mock_app, mock_receive, mock_send
    ):
        """Test middleware returns 401 when no token and no fallback."""
        middleware = BearerTokenDetectionMiddleware(mock_app, has_fallback_client=False)

        scope = {
            "type": "http",
            "path": "/",
            "method": "GET",
            "headers": [(b"host", b"example.com")],
        }
        await middleware(scope, mock_receive, mock_send)

        # Should send 401 response
        assert mock_send.called

        # Check if any call contains status 401
        calls = [str(call) for call in mock_send.call_args_list]
        has_401 = any("401" in call for call in calls)
        assert has_401, f"Expected 401 status in calls: {calls}"

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    async def test_middleware_with_fallback_client(
        self, mock_app, mock_receive, mock_send
    ):
        """Test middleware passes through when fallback client exists."""
        middleware = BearerTokenDetectionMiddleware(mock_app, has_fallback_client=True)

        scope = {
            "type": "http",
            "path": "/",
            "method": "GET",
            "headers": [(b"host", b"example.com")],
        }
        await middleware(scope, mock_receive, mock_send)

        # Should have called the app (no 401)
        assert mock_send.called

    @patch.dict(os.environ, {"OIDC_CLIENT_ID": "test-client"})
    async def test_middleware_exempt_paths(self, mock_app, mock_receive, mock_send):
        """Test middleware allows exempt paths without authentication."""
        middleware = BearerTokenDetectionMiddleware(mock_app, has_fallback_client=False)

        exempt_paths = [
            "/.well-known/oauth-authorization-server",
            "/oauth/register",
            "/oauth/authorize",
            "/oauth/callback",
            "/health",
        ]

        for path in exempt_paths:
            # Reset the mock for each path
            mock_send.reset_mock()
            scope = {
                "type": "http",
                "path": path,
                "method": "GET",
                "headers": [(b"host", b"example.com")],
            }
            await middleware(scope, mock_receive, mock_send)
            assert mock_send.called, f"Expected middleware to call send for path {path}"


class TestOIDCTokenValidator:
    """Test the generic OIDC token validator."""

    @patch.dict(
        os.environ,
        {
            "OIDC_ISSUER_URL": "https://oidc.example.com",
            "OIDC_AUDIENCE": "test-audience",
        },
    )
    def test_create_oidc_validator(self):
        """Test OIDC validator factory creation."""
        from mcp_server_datahub.oidc_token_validator import create_oidc_validator

        validator = create_oidc_validator()
        assert validator is not None
        assert validator.issuer_url == "https://oidc.example.com"
        assert validator.audience == "test-audience"

    @patch.dict(os.environ, {}, clear=True)
    def test_create_oidc_validator_disabled(self):
        """Test OIDC validator factory when disabled."""
        from mcp_server_datahub.oidc_token_validator import create_oidc_validator

        validator = create_oidc_validator()
        assert validator is None

    @patch.dict(os.environ, {"OIDC_ISSUER_URL": "https://oidc.example.com"})
    def test_create_oidc_validator_missing_audience(self):
        """Test OIDC validator factory with missing audience."""
        from mcp_server_datahub.oidc_token_validator import create_oidc_validator

        with pytest.raises(ValueError, match="OIDC_AUDIENCE must be set"):
            create_oidc_validator()

    def test_oidc_validator_success(self):
        """Test successful OIDC token validation."""
        from mcp_server_datahub.oidc_token_validator import OIDCTokenValidator

        with (
            patch(
                "mcp_server_datahub.oidc_token_validator.PyJWKClient"
            ) as mock_jwks_client,
            patch(
                "mcp_server_datahub.oidc_token_validator.jwt.decode"
            ) as mock_jwt_decode,
        ):
            # Mock successful validation
            mock_signing_key = MagicMock()
            mock_signing_key.key = "mock-key"
            mock_jwks_client_instance = MagicMock()
            mock_jwks_client_instance.get_signing_key_from_jwt.return_value = (
                mock_signing_key
            )
            mock_jwks_client.return_value = mock_jwks_client_instance

            mock_jwt_decode.return_value = {"sub": "user123", "aud": "test-audience"}

            validator = OIDCTokenValidator(
                issuer_url="https://oidc.example.com", audience="test-audience"
            )

            # Use simple token string - the JWT parsing is mocked
            test_token = "test-jwt-token"
            result = validator.validate_and_resolve(test_token)
            assert result == test_token

            mock_jwt_decode.assert_called_once()

    def test_oidc_validator_failure(self):
        """Test OIDC token validation failure."""
        from mcp_server_datahub.oidc_token_validator import OIDCTokenValidator
        import jwt

        with (
            patch(
                "mcp_server_datahub.oidc_token_validator.PyJWKClient"
            ) as mock_jwks_client,
            patch(
                "mcp_server_datahub.oidc_token_validator.jwt.decode"
            ) as mock_jwt_decode,
        ):
            # Mock validation failure
            mock_signing_key = MagicMock()
            mock_signing_key.key = "mock-key"
            mock_jwks_client_instance = MagicMock()
            mock_jwks_client_instance.get_signing_key_from_jwt.return_value = (
                mock_signing_key
            )
            mock_jwks_client.return_value = mock_jwks_client_instance

            mock_jwt_decode.side_effect = jwt.InvalidTokenError("Token expired")

            validator = OIDCTokenValidator(
                issuer_url="https://oidc.example.com", audience="test-audience"
            )

            test_token = "expired-jwt-token"
            with pytest.raises(jwt.InvalidTokenError):
                validator.validate_and_resolve(test_token)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
