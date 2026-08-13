# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:0.11.7 AS uv
FROM python:3.11-slim AS builder

ARG VERSION=0.0.0

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md LICENSE ./

# Install locked runtime dependencies separately so source-only changes reuse
# this layer.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/

# Docker build contexts do not include git metadata. Tell setuptools-scm the
# release version explicitly and install a non-editable wheel into the venv.
RUN --mount=type=cache,target=/root/.cache/uv \
    package_version="${VERSION#v}" && \
    SETUPTOOLS_SCM_PRETEND_VERSION="${package_version}" \
    uv sync --frozen --no-dev --no-editable


FROM python:3.11-slim AS runtime

ARG VERSION=0.0.0
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="DataHub MCP Server" \
      org.opencontainers.image.source="https://github.com/acryldata/mcp-server-datahub" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FASTMCP_HOST=0.0.0.0 \
    FASTMCP_PORT=8000

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

RUN useradd --uid 10001 --create-home --home-dir /home/mcp --shell /usr/sbin/nologin mcp
USER 10001

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; port = os.environ.get('FASTMCP_PORT', '8000'); urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2)"]

CMD ["mcp-server-datahub-http"]
