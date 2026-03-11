FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv

# Build-time version override for setuptools-scm when .git is unavailable
ARG MCP_SERVER_DATAHUB_VERSION=0.0.0

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev deps, no editable install yet)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY src/ ./src/

# Install the project itself
RUN SETUPTOOLS_SCM_PRETEND_VERSION=${MCP_SERVER_DATAHUB_VERSION} uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["mcp-server-datahub", "--transport", "http"]
