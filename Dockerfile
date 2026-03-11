FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev deps, no editable install yet)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY src/ ./src/

# Inject version at build time so setuptools-scm fallback (0.0.0) is not used.
# The .git directory is not available during Docker builds, so we write
# _version.py directly from the VERSION build arg.
ARG VERSION=0.0.0
RUN printf '__version__ = version = "%s"\n__version_tuple__ = version_tuple = tuple(int(x) if x.isdigit() else x for x in "%s".lstrip("v").split("."))\n__commit_id__ = commit_id = None\n' \
    "$VERSION" "$VERSION" \
    > src/mcp_server_datahub/_version.py

# Install the project itself
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["mcp-server-datahub", "--transport", "http"]
