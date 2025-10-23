## Developing

### Setup

Requires [`uv`](https://docs.astral.sh/uv/) - see the [project README](README.md) for installation instructions.

```bash
make setup

# <authentication is the same as in production>
```

### Run using the MCP inspector

```bash
npx -y @modelcontextprotocol/inspector@latest

# In the UI, select "STDIO" and put in
# command: <full-path-to-uv>
# args: --directory path/to/mcp-server-datahub run mcp-server-datahub
```

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
        "mcp-server-datahub",
        "--disable-ssl-verification"  // optional: add this flag to disable SSL verification
      ],
      "env": {  // required if ~/.datahubenv does not exist
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>",
        "DATAHUB_DISABLE_SSL_VERIFICATION": "true"  // optional: disable SSL verification for self-signed certs
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

The test suite is currently very simplistic, and requires a live DataHub instance.

```bash
make test
```

## Publishing

We use setuptools-scm to manage the version number.

CI will automatically publish a new release to PyPI when a GitHub release is created.
