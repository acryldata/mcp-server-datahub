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

# Install the project itself
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["mcp-server-datahub", "--transport", "http"]
