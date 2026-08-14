import asyncio
import logging
import os
import re
from typing import Any, Optional

import click
from cachetools import TTLCache
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp import FastMCP
from fastmcp.server.auth import TokenVerifier
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import Middleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client
from mcp_server_datahub.version_requirements import VersionFilterMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)

_GET_ME_QUERY = "query getMe { me { corpUser { urn username exists } } }"
_AUTH_FAILURE_STATUS_RE = re.compile(
    r"\b(?:HTTP(?:\s+error)?\s*|status(?:_code)?\s*[:=]?\s*)?(401|403)\b",
    re.IGNORECASE,
)
_HTTP_CLIENT_CACHE_TTL_SECONDS = 300
_HTTP_CLIENT_CACHE_MAX_SIZE = 1024
_HTTP_MAX_CONCURRENT_VALIDATIONS = 8


class _DataHubClientMiddleware(Middleware):
    """Propagate one local DataHub client into every MCP request."""

    def __init__(self, client: DataHubClient) -> None:
        self._client = client

    async def on_message(self, context: Any, call_next: Any) -> Any:
        with with_datahub_client(self._client):
            return await call_next(context)


def _build_http_client(server_url: str, token: str) -> DataHubClient:
    return DataHubClient(
        config=DatahubClientConfig(
            server=server_url,
            token=token,
            client_mode=ClientMode.SDK,
            datahub_component=f"mcp-server-datahub/{__version__}",
        )
    )


def _build_and_verify_http_client(server_url: str, token: str) -> DataHubClient:
    client = _build_http_client(server_url, token)
    result = client._graph.execute_graphql(_GET_ME_QUERY)
    corp_user = (result.get("me") or {}).get("corpUser") or {}
    if corp_user.get("exists") is False:
        logger.critical(
            "DataHub returned a non-existent authenticated user during HTTP token "
            "validation (%s). Check the auth settings and "
            "METADATA_SERVICE_AUTH_ENABLED; the supplied token might be invalid.",
            corp_user.get("urn") or corp_user.get("username") or "unknown",
        )
    return client


class _DataHubTokenVerifier(TokenVerifier):
    """Validate HTTP bearer tokens against DataHub and cache their clients."""

    def __init__(
        self,
        server_url: str,
        max_concurrent_validations: int = _HTTP_MAX_CONCURRENT_VALIDATIONS,
    ) -> None:
        super().__init__()
        self._server_url = server_url
        self._clients: TTLCache[str, DataHubClient] = TTLCache(
            maxsize=_HTTP_CLIENT_CACHE_MAX_SIZE,
            ttl=_HTTP_CLIENT_CACHE_TTL_SECONDS,
        )
        self._validation_semaphore = asyncio.Semaphore(max_concurrent_validations)

    async def get_client(self, token: str) -> DataHubClient:
        cached_client = self._clients.get(token)
        if cached_client is not None:
            return cached_client

        async with self._validation_semaphore:
            # Re-check after waiting so queued requests reuse the result of the
            # validation that got the semaphore first.
            cached_client = self._clients.get(token)
            if cached_client is not None:
                return cached_client

            client = await asyncio.to_thread(
                _build_and_verify_http_client,
                self._server_url,
                token,
            )
            self._clients[token] = client
            return client

    def invalidate_client(self, token: str) -> None:
        self._clients.pop(token, None)

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        try:
            await self.get_client(token)
        except Exception as exc:
            # Fail closed without logging the credential itself.
            logger.warning("DataHub bearer-token validation failed: %s", exc)
            return None

        return AccessToken(
            token=token,
            client_id=f"mcp-server-datahub/{__version__}",
            scopes=[],
        )


def _is_datahub_auth_failure(exc: BaseException) -> bool:
    cause: Optional[BaseException] = exc
    seen: set[int] = set()
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        response = getattr(cause, "response", None)
        if _exception_status_code(cause) in (401, 403) or getattr(
            response, "status_code", None
        ) in (401, 403):
            return True
        cause = cause.__cause__ or cause.__context__
    return False


def _exception_status_code(exc: BaseException) -> Optional[int]:
    """Extract an HTTP status from SDK exception attributes or messages."""
    for source in (exc, getattr(exc, "info", None)):
        if source is None:
            continue
        values: list[Any] = [
            getattr(source, "status_code", None),
            getattr(source, "status", None),
            getattr(source, "http_status", None),
        ]
        if isinstance(source, dict):
            values.extend(
                [
                    source.get("status_code"),
                    source.get("status"),
                    source.get("http_status"),
                ]
            )
        for value in values:
            if isinstance(value, int):
                status_code = value
            elif isinstance(value, str) and value.isdigit():
                status_code = int(value)
            else:
                continue
            if status_code in (401, 403):
                return status_code

    match = _AUTH_FAILURE_STATUS_RE.search(str(exc))
    return int(match.group(1)) if match else None


class _AuthenticatedDataHubClientMiddleware(Middleware):
    """Use the DataHub client associated with the authenticated HTTP request."""

    def __init__(self, token_verifier: _DataHubTokenVerifier) -> None:
        self._token_verifier = token_verifier

    async def on_message(self, context: Any, call_next: Any) -> Any:
        access_token = get_access_token()
        if access_token is None:
            raise RuntimeError("Authenticated HTTP request has no access token")

        try:
            client = await self._token_verifier.get_client(access_token.token)
            with with_datahub_client(client):
                return await call_next(context)
        except Exception as exc:
            if _is_datahub_auth_failure(exc):
                self._token_verifier.invalidate_client(access_token.token)
            raise


# This endpoint deliberately remains outside MCP authentication so container
# runtimes and Kubernetes can probe the process without a DataHub credential.
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


_AppMode = Literal["local", "http"]
_app_mode: Optional[_AppMode] = None


def _get_existing_app(mode: _AppMode) -> Optional[FastMCP]:
    if _app_mode is None:
        return None
    if _app_mode != mode:
        raise RuntimeError(
            f"MCP app is already configured for {_app_mode!r} mode, not {mode!r} mode"
        )
    return mcp


def _configure_app(
    mode: _AppMode,
    client_middleware: Middleware,
    auth: Optional[TokenVerifier] = None,
) -> FastMCP:
    global _app_mode
    existing_app = _get_existing_app(mode)
    if existing_app is not None:
        return existing_app

    mcp.auth = auth
    # Client context must wrap tool-filtering middleware because those filters
    # also query DataHub.
    mcp.add_middleware(client_middleware)
    mcp.add_middleware(TelemetryMiddleware())
    mcp.add_middleware(VersionFilterMiddleware())
    mcp.add_middleware(DocumentToolsMiddleware())

    _app_mode = mode
    return mcp


def create_local_app() -> FastMCP:
    """Create the local app used by the CLI's stdio and SSE modes."""

    existing_app = _get_existing_app("local")
    if existing_app is not None:
        return existing_app

    client = DataHubClient.from_env(
        client_mode=ClientMode.SDK,
        datahub_component=f"mcp-server-datahub/{__version__}",
    )
    return _configure_app("local", _DataHubClientMiddleware(client))


def create_http_app() -> FastMCP:
    """Create the shared HTTP app with per-request DataHub authentication."""

    existing_app = _get_existing_app("http")
    if existing_app is not None:
        return existing_app

    server_url = os.environ.get("DATAHUB_GMS_URL")
    if not server_url:
        raise RuntimeError("DATAHUB_GMS_URL is required for HTTP mode")

    if os.environ.get("DATAHUB_GMS_TOKEN"):
        raise RuntimeError(
            "DATAHUB_GMS_TOKEN is not allowed in HTTP mode; remove the shared "
            "credential and send a DataHub token with each request"
        )

    token_verifier = _DataHubTokenVerifier(server_url)
    return _configure_app(
        "http",
        _AuthenticatedDataHubClientMiddleware(token_verifier),
        auth=token_verifier,
    )


@click.command()
@click.version_option(version=__version__)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(transport: Literal["stdio", "sse"], debug: bool) -> None:
    if debug:
        # Add LoggingMiddleware first so it wraps the complete middleware stack.
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))

    create_local_app().run(transport=transport, show_banner=False)


@click.command()
@click.version_option(version=__version__)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
)
@telemetry.with_telemetry()
def http_main(debug: bool) -> None:
    """Run HTTP with mandatory per-request DataHub credentials."""

    if debug:
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))

    create_http_app().run(
        transport="http",
        show_banner=False,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
