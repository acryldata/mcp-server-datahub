FROM python:3.10-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files and source code (needed for setuptools-scm version generation)
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

# Install dependencies and build the package
RUN uv sync --frozen --no-dev

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the MCP server with HTTP transport
CMD ["uv", "run", "mcp-server-datahub", "--transport", "http", "--host", "0.0.0.0", "--port", "8000"]