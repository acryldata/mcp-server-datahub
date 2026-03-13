import importlib
import logging
import os
import threading
from typing import Any, Optional

import click
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.main_client import DataHubClient
from datahub.telemetry import telemetry
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_headers
from fastmcp.server.middleware import Middleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from starlette.middleware import Middleware as ASGIMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from typing_extensions import Literal

from mcp_server_datahub._telemetry import TelemetryMiddleware
from mcp_server_datahub._version import __version__
from mcp_server_datahub.document_tools_middleware import DocumentToolsMiddleware
from mcp_server_datahub.mcp_server import mcp, register_all_tools, with_datahub_client
from mcp_server_datahub.oauth_endpoints import BearerTokenDetectionMiddleware
from mcp_server_datahub.token_validator import TokenValidator
from mcp_server_datahub.version_requirements import VersionFilterMiddleware

# Import oauth_endpoints to register the custom routes on `mcp`
import mcp_server_datahub.oauth_endpoints as _oauth_endpoints  # noqa: F401

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    """Configure structured logging for the MCP server."""

    class ThreadFormatter(logging.Formatter):
        """Custom formatter that shows clean thread info."""

        def __init__(self) -> None:
            super().__init__(
                fmt="%(asctime)s [%(levelname)s] [T%(thread_short)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            self._thread_names: dict[int | None, str] = {}
            self._thread_counter = 1

        def format(self, record: logging.LogRecord) -> str:
            thread_id = threading.current_thread().ident
            if thread_id not in self._thread_names:
                self._thread_names[thread_id] = f"{self._thread_counter:02d}"
                self._thread_counter += 1
            record.thread_short = self._thread_names[thread_id]  # type: ignore[attr-defined]
            return super().format(record)

    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    formatter = ThreadFormatter()
    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    logging.getLogger("mcp_server_datahub").setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# Register tools with OSS-compatible descriptions
register_all_tools(is_oss=True)


class _DataHubClientMiddleware(Middleware):
    """Middleware that provides a DataHubClient per request.

    Operates in two modes:
    1. **Per-request** (Bearer token in HTTP headers): Validates the token
       (if a ``token_validator`` is configured), then creates a
       ``DataHubClient`` using the resolved token and ``gms_url``.
    2. **Fallback** (no token / stdio mode): Uses the long-lived
       ``fallback_client`` created at startup from environment variables.

    Per-request clients are created fresh for each request and closed
    after the request completes.

    Must be added as the first middleware so the client ContextVar is
    available to all subsequent middlewares and tool handlers.
    """

    def __init__(
        self,
        fallback_client: Optional[DataHubClient] = None,
        gms_url: Optional[str] = None,
        token_validator: Optional[TokenValidator] = None,
    ) -> None:
        self._fallback_client = fallback_client
        self._gms_url = gms_url
        self._token_validator = token_validator

    async def on_message(
        self,
        context: Any,
        call_next: Any,
    ) -> Any:
        # Try to extract Bearer token from HTTP headers.
        # get_http_headers() returns {} when there is no HTTP request
        # (e.g. stdio transport), so this is safe to call unconditionally.
        headers = get_http_headers(include_all=True)
        auth_header = headers.get("authorization", "")

        per_request_client: Optional[DataHubClient] = None
        if auth_header.lower().startswith("bearer ") and self._gms_url:
            raw_token = auth_header[len("bearer ") :].strip()

            token = (
                self._token_validator.validate_and_resolve(raw_token)
                if self._token_validator is not None
                else raw_token
            )
            per_request_client = DataHubClient(
                server=self._gms_url,
                token=token,
            )

        client = per_request_client or self._fallback_client
        if client is None:
            raise RuntimeError(
                "No DataHub client available: no Bearer token in request "
                "and no fallback client configured. Set DATAHUB_GMS_TOKEN "
                "or provide an Authorization header."
            )

        try:
            with with_datahub_client(client):
                return await call_next(context)
        finally:
            # Close per-request clients; fallback client is long-lived.
            if per_request_client is not None:
                try:
                    per_request_client._graph.close()
                except Exception:
                    logger.debug("Failed to close per-request client", exc_info=True)


# Adds a health route to the MCP Server.
# Notice that this is only available when the MCP Server is run in HTTP/SSE modes.
# Doesn't make much sense to have it in the stdio mode since it is usually used as a subprocess of the client.
@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


_app_initialized = False
_has_fallback_client = False


def _load_token_validator_factory() -> Optional[TokenValidator]:
    """Load a token validator from the ``TOKEN_VALIDATOR_FACTORY`` env var.

    The env var should be a ``module:callable`` string (e.g.
    ``mypackage.auth:create_validator``). The callable is invoked with no
    arguments and must return an object implementing the ``TokenValidator``
    protocol.

    Returns ``None`` if the env var is not set.

    Raises ``RuntimeError`` if the env var is set but the import or call fails.
    """
    factory_spec = os.getenv("TOKEN_VALIDATOR_FACTORY", "")
    if not factory_spec:
        return None

    try:
        module_path, callable_name = factory_spec.rsplit(":", 1)
        module = importlib.import_module(module_path)
        factory = getattr(module, callable_name)
        validator = factory()
        logger.info("Loaded token validator from %s", factory_spec)
        return validator
    except Exception as e:
        raise RuntimeError(
            f"Failed to load token validator from TOKEN_VALIDATOR_FACTORY={factory_spec!r}: {e}"
        ) from e


def create_app() -> FastMCP:
    """Create and configure the MCP server with DataHub client and middlewares.

    This is the factory function used by ``fastmcp dev`` / ``fastmcp run``
    (via the ``__main__.py:create_app`` entrypoint) and is also called by the
    CLI ``main()`` entrypoint.

    The function is idempotent — calling it more than once returns the same
    ``mcp`` instance without adding duplicate middlewares.
    """
    global _app_initialized
    global _has_fallback_client
    if _app_initialized:
        return mcp

    gms_url = os.getenv("DATAHUB_GMS_URL")

    # Create fallback client from environment if possible.
    # This is optional — per-request Bearer tokens can supply the client.
    fallback_client: Optional[DataHubClient] = None
    try:
        fallback_client = DataHubClient.from_env(
            client_mode=ClientMode.SDK,
            datahub_component=f"mcp-server-datahub/{__version__}",
        )
        logger.info("Fallback DataHub client created from environment")
    except Exception as e:
        logger.info("No fallback DataHub client: %s", e)

    _has_fallback_client = fallback_client is not None

    # When OIDC_CLIENT_ID is set, OAuth is required — don't let a token-less
    # fallback client bypass the OAuth flow.
    if os.getenv("OIDC_CLIENT_ID"):
        _has_fallback_client = _has_fallback_client and bool(
            os.getenv("DATAHUB_GMS_TOKEN")
        )

    # Load pluggable token validator via factory env var.
    token_validator = _load_token_validator_factory()

    # _DataHubClientMiddleware must be first so the client ContextVar is
    # available to all subsequent middlewares and tool handlers.  This is
    # especially important for HTTP transport where each request runs in a
    # separate async context.
    mcp.add_middleware(
        _DataHubClientMiddleware(
            fallback_client=fallback_client,
            gms_url=gms_url,
            token_validator=token_validator,
        )
    )
    mcp.add_middleware(TelemetryMiddleware())
    mcp.add_middleware(VersionFilterMiddleware())
    mcp.add_middleware(DocumentToolsMiddleware())

    _app_initialized = True
    return mcp


@click.command()
@click.version_option(version=__version__)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "http"]),
    default="stdio",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
)
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Port to bind HTTP server (default: 8000)",
)
@telemetry.with_telemetry(
    capture_kwargs=["transport"],
)
def main(
    transport: Literal["stdio", "sse", "http"],
    debug: bool,
    port: int,
) -> None:
    setup_logging(debug=debug)

    if debug:
        # Add LoggingMiddleware before create_app() so it becomes the
        # outermost middleware (FastMCP reverses the list) and logs the
        # full request/response including all other middleware effects.
        mcp.add_middleware(LoggingMiddleware(include_payloads=True))

    create_app()

    logger.info("Starting MCP DataHub Server v%s", __version__)
    logger.info(
        "Transport: %s%s", transport, f" on port {port}" if transport != "stdio" else ""
    )

    # ASGI-level middleware for OAuth 2.1 bearer token detection.
    # Only relevant for HTTP/SSE transports — stdio doesn't go through HTTP.
    # When a fallback client exists, skip the 401 — let requests through
    # to the MCP layer where _DataHubClientMiddleware uses the fallback.
    oauth_asgi_middleware = [
        ASGIMiddleware(
            BearerTokenDetectionMiddleware,
            has_fallback_client=_has_fallback_client,
        )
    ]

    if transport == "http":
        mcp.run(
            transport=transport,
            show_banner=False,
            stateless_http=True,
            host="0.0.0.0",
            port=port,
            middleware=oauth_asgi_middleware,
        )
    else:
        mcp.run(
            transport=transport,
            show_banner=False,
            host="0.0.0.0",
            port=port,
            middleware=oauth_asgi_middleware,
        )


if __name__ == "__main__":
    main()
