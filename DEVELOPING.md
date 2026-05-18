## Developing

### Setup

Requires [`uv`](https://docs.astral.sh/uv/) - see the [project README](README.md) for installation instructions.

```bash
make setup

# <authentication is the same as in production>
```

### Run using the MCP inspector

```bash
uv run fastmcp dev src/mcp_server_datahub/__main__.py:create_app --with-editable .
```

In the inspector UI, add environment variables for `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN`, then click Connect.

> **Note:** Use `fastmcp dev` (not `mcp dev`), since this project uses the standalone FastMCP package.

### Run using an MCP client

Use this configuration in your MCP client e.g. Claude Desktop, Cursor, etc.

```js
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uv>",  // e.g. /Users/hsheth/.local/bin/uv
      "args": [
        "--directory",
        "path/to/mcp-server-datahub",  // update this with an absolute path
        "run",
        "mcp-server-datahub"
      ],
      "env": {  // required if ~/.datahubenv does not exist
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

### Run linting

```bash
# Check linting
make lint-check

# Fix linting
make lint
```

### Run tests

#### Unit tests

Unit tests (no live server required):

```bash
uv run pytest tests/test_per_request_client.py -v
```

#### Integration tests (auth flow)

Integration tests require a **running MCP server** and valid authentication tokens.
They are split into two categories:

| Test file | What it tests |
|---|---|
| `test_multithread_client.py` | Concurrent requests with different tokens, client isolation |
| `test_fallback_token.py` | Fallback token behavior when no Bearer token is provided |

**Step 1: Start the MCP server**

```bash
# Required
export DATAHUB_GMS_URL="<your-datahub-url>"
export DATAHUB_GMS_TOKEN="<your-datahub-token>"

# Optional: enable OAuth flow (required for test_multithread_client)
# export OIDC_CLIENT_ID="<your-oidc-client-id>"
# export OAUTH_AUTHORIZE_ENDPOINT="<your-idp-authorize-url>"
# export OAUTH_TOKEN_ENDPOINT="<your-idp-token-url>"

# Optional: enable token validation
# export TOKEN_VALIDATOR_FACTORY="mcp_server_datahub.oidc_token_validator:create_oidc_validator"

uv run python -m mcp_server_datahub --transport http --port 8000 --debug
```

**Step 2: Run the tests** (in a separate terminal)

```bash
# Required: a valid token accepted by the running server
export MCP_TEST_AUTH_TOKEN="<your-valid-token>"

# Optional: a second valid token for multi-client isolation tests
# If not set, defaults to the same as MCP_TEST_AUTH_TOKEN
export MCP_TEST_AUTH_TOKEN_2="<another-valid-token>"

# Optional: override server URL (default: http://localhost:8000/mcp)
# export MCP_SERVER_URL="http://localhost:8000/mcp"

# Run all integration tests
uv run pytest tests/test_multithread_client.py tests/test_fallback_token.py -v

# Or run individually
uv run pytest tests/test_fallback_token.py -v
uv run pytest tests/test_multithread_client.py -v
```

**Environment variables summary:**

| Variable | Required | Description |
|---|---|---|
| `MCP_TEST_AUTH_TOKEN` | Yes | Valid token for the running server (GMS PAT, OIDC token, etc.) |
| `MCP_TEST_AUTH_TOKEN_2` | No | Second valid token for multi-client tests |
| `MCP_SERVER_URL` | No | Server URL (default: `http://localhost:8000/mcp`) |

> **Note:** Tests do not generate tokens themselves. You must provide pre-generated
> tokens via environment variables. This keeps test setup identical across OSS and
> internal environments.

## Publishing

We use setuptools-scm to manage the version number.

CI will automatically publish a new release to PyPI when a GitHub release is created.
