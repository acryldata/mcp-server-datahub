## Smoke Check

`smoke_check.py` exercises every available MCP tool against a live DataHub
instance to verify GraphQL compatibility.  Mutation tools are tested with
add-then-remove pairs so the instance is left in its original state.

### Prerequisites

- `uv` installed ([docs](https://docs.astral.sh/uv/))
- A running DataHub instance with `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN`
  set (or a `~/.datahubenv` file).
- For HTTP/SSE modes: `curl` (used by the orchestrator for health checks).

### Quick start

```bash
# Read-only tools only (safe, no changes to DataHub):
uv run python scripts/smoke_check.py

# Include mutation tools (adds then removes metadata):
uv run python scripts/smoke_check.py --mutations

# Include user tools (get_me):
uv run python scripts/smoke_check.py --user

# Everything:
uv run python scripts/smoke_check.py --all
```

### Transport modes

The smoke check can test the server via any transport mode. By default it
runs **in-process** (memory pipes), but `--url` and `--stdio-cmd` let you
test against a real HTTP/SSE server or a stdio subprocess.

| Mode | Flag | What it tests |
|------|------|---------------|
| In-process | *(default)* | Tools & middleware via memory pipes. Fast, no server process. |
| HTTP | `--url http://host:port/mcp` | Full HTTP (streamable-http) transport against a running server. |
| SSE | `--url http://host:port/sse` | SSE transport against a running server. |
| Stdio | `--stdio-cmd "uv run mcp-server-datahub"` | Launches server as a subprocess, communicates via stdin/stdout. |
| PyPI | `--pypi [version]` | Installs from PyPI in a clean temp venv and re-runs the smoke check. |

#### Examples

```bash
# Against a running HTTP server:
uv run mcp-server-datahub --transport http &
uv run python scripts/smoke_check.py --url http://127.0.0.1:8000/mcp
kill %1

# Via stdio subprocess:
uv run python scripts/smoke_check.py --stdio-cmd "uv run mcp-server-datahub"

# From the latest PyPI release:
uv run python scripts/smoke_check.py --pypi
```

### Test all modes at once

`test_all_modes.sh` is an orchestrator that automatically starts/stops
servers and runs the smoke check across all five transport modes:

```bash
bash scripts/test_all_modes.sh          # read-only tools
bash scripts/test_all_modes.sh --all    # everything (flags forwarded to smoke_check)
```

The script:
1. Runs the smoke check **in-process** (default mode).
2. Starts the server with `--transport http`, runs smoke check with `--url`, stops it.
3. Starts the server with `--transport sse`, runs smoke check with `--url`, stops it.
4. Runs the smoke check with `--stdio-cmd` (subprocess mode).
5. Starts the server via `fastmcp run :create_app` (the same code path used
   by `fastmcp dev`), runs smoke check with `--url`, stops it.
6. Prints a summary of pass/fail across all modes.

It uses the default FastMCP port **8000**. Stop any other server on that
port before running.

#### Logs

All stdout and stderr from each mode are captured in `scripts/logs/`:

```
scripts/logs/
  in-process-memory-pipes.stdout    # smoke check stdout
  in-process-memory-pipes.stderr    # smoke check stderr (debug logs)
  http-streamable-http.stdout
  http-streamable-http.stderr
  http_server.stdout                # server stdout (for HTTP mode)
  http_server.stderr                # server stderr (for HTTP mode)
  sse.stdout
  sse.stderr
  sse_server.stdout
  sse_server.stderr
  stdio-subprocess.stdout
  stdio-subprocess.stderr
  fastmcp-run-create_app-factory.stdout
  fastmcp-run-create_app-factory.stderr
  fastmcp-run_server.stdout
  fastmcp-run_server.stderr
```

These logs are `.gitignore`d. To troubleshoot a failure, look at the
`.stderr` file for the failing mode — it contains debug-level output from
both the DataHub SDK and FastMCP middleware.

### Step-by-step: running all modes manually

If you need to reproduce or debug individual modes outside of
`test_all_modes.sh`, here are the exact commands:

```bash
# --- Mode 1: In-process ---
uv run python scripts/smoke_check.py

# --- Mode 2: HTTP ---
uv run mcp-server-datahub --transport http &
# wait until: curl -sf http://127.0.0.1:8000/health
uv run python scripts/smoke_check.py --url http://127.0.0.1:8000/mcp
kill %1

# --- Mode 3: SSE ---
uv run mcp-server-datahub --transport sse &
# wait until: curl -sf http://127.0.0.1:8000/health
uv run python scripts/smoke_check.py --url http://127.0.0.1:8000/sse
kill %1

# --- Mode 4: Stdio ---
uv run python scripts/smoke_check.py --stdio-cmd "uv run mcp-server-datahub"

# --- Mode 5: fastmcp run (create_app factory) ---
uv run fastmcp run src/mcp_server_datahub/__main__.py:create_app --transport http &
# wait until: curl -sf http://127.0.0.1:8000/health
uv run python scripts/smoke_check.py --url http://127.0.0.1:8000/mcp
kill %1
```

### What it checks

**URN discovery** &mdash; Before running checks, the tool discovers real
URNs (datasets, tags, terms, owners, domains, structured properties) from
the live instance via both MCP search and direct GraphQL queries.

**Read-only tools** (always tested):
`search`, `get_entities`, `get_lineage`, `get_dataset_queries`,
`list_schema_fields`, `get_lineage_paths_between`, `search_documents`,
`grep_documents`

**Mutation tools** (`--mutations`):
`add_tags`/`remove_tags`, `add_terms`/`remove_terms`,
`add_owners`/`remove_owners`, `set_domains`/`remove_domains`,
`add_structured_properties`/`remove_structured_properties`,
`update_description`, `save_document`

**User tools** (`--user`):
`get_me`

Tools that are hidden by version filtering or other middleware are silently
skipped. Required URNs that cannot be discovered cause the check to fail
with a descriptive message.

### How it works

1. Tools are registered and middleware is applied (in-process mode only).
2. A `DataHubClient` is created from env vars for URN discovery (all modes).
3. A `fastmcp.Client` connects to the server using the selected transport.
4. Available tools are listed (middleware-filtered).
5. URNs are discovered dynamically from the live instance.
6. Each registered `@check(...)` function runs against the discovered URNs.
7. A final report is printed with pass/fail counts.

Exit code is `0` if all checks pass, `1` otherwise.
