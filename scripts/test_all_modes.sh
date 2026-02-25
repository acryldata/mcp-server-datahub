#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# test_all_modes.sh — Run smoke checks against every MCP transport mode
#
# Requires:
#   - DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN env vars (or ~/.datahubenv)
#   - uv (https://docs.astral.sh/uv/)
#
# Usage:
#   bash scripts/test_all_modes.sh            # test all modes
#   bash scripts/test_all_modes.sh --all      # pass extra flags to smoke_check
#
# Logs are written to scripts/logs/<mode>.{stdout,stderr} for troubleshooting.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SMOKE_CHECK="$SCRIPT_DIR/smoke_check.py"
LOG_DIR="$SCRIPT_DIR/logs"

# Extra arguments forwarded to every smoke_check invocation
EXTRA_ARGS=("$@")

# Server settings (FastMCP defaults: host=127.0.0.1, port=8000)
HOST="127.0.0.1"
PORT=8000
HTTP_URL="http://${HOST}:${PORT}/mcp"
SSE_URL="http://${HOST}:${PORT}/sse"
HEALTH_URL="http://${HOST}:${PORT}/health"

# Track results
declare -a MODE_NAMES=()
declare -a MODE_RESULTS=()
SERVER_PID=""

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

mkdir -p "$LOG_DIR"
# Clean previous logs
rm -f "$LOG_DIR"/*.stdout "$LOG_DIR"/*.stderr

# Sanitise a mode name into a safe filename slug
slug() {
    echo "$1" | tr '[:upper:] ()/' '[:lower:]_---' | tr -cs '[:alnum:]-_' '-' | sed 's/-$//'
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "  Stopping server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
    fi
}
trap cleanup EXIT

wait_for_server() {
    local url="$1"
    local max_attempts=30
    local attempt=0
    echo "  Waiting for server at $url ..."
    while ! curl -sf "$url" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [[ $attempt -ge $max_attempts ]]; then
            echo "  ERROR: Server did not start within ${max_attempts}s"
            return 1
        fi
        sleep 1
    done
    echo "  Server is ready."
}

start_server() {
    local transport="$1"
    local log_slug="$2"
    echo "  Starting server (transport=$transport, port=$PORT)..."
    cd "$PROJECT_DIR"
    uv run mcp-server-datahub --transport "$transport" \
        >"$LOG_DIR/${log_slug}_server.stdout" \
        2>"$LOG_DIR/${log_slug}_server.stderr" &
    SERVER_PID=$!
    # Give server a moment to bind
    wait_for_server "$HEALTH_URL"
}

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        echo "  Stopping server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
        # Brief pause to let the port be released
        sleep 1
    fi
}

run_smoke_check() {
    local mode_name="$1"
    shift
    local extra=("$@")
    local log_slug
    log_slug=$(slug "$mode_name")

    echo ""
    echo "======================================================================"
    echo "  MODE: $mode_name"
    echo "======================================================================"

    MODE_NAMES+=("$mode_name")

    if uv run python "$SMOKE_CHECK" ${extra[@]+"${extra[@]}"} ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} \
        >"$LOG_DIR/${log_slug}.stdout" \
        2>"$LOG_DIR/${log_slug}.stderr"; then
        MODE_RESULTS+=("PASS")
        echo "  ➜ $mode_name: PASS"
    else
        MODE_RESULTS+=("FAIL")
        echo "  ➜ $mode_name: FAIL"
    fi
    # Show the report section from stdout (between the two === lines)
    sed -n '/SMOKE CHECK REPORT/,/^Total:/p' "$LOG_DIR/${log_slug}.stdout"
}

# ---------------------------------------------------------------------------
# Mode 1: In-process
# ---------------------------------------------------------------------------
run_smoke_check "In-process (memory pipes)"

# ---------------------------------------------------------------------------
# Mode 2: HTTP (streamable-http)
# ---------------------------------------------------------------------------
start_server "http" "http"
run_smoke_check "HTTP (streamable-http)" --url "$HTTP_URL"
stop_server

# ---------------------------------------------------------------------------
# Mode 3: SSE
# ---------------------------------------------------------------------------
start_server "sse" "sse"
run_smoke_check "SSE" --url "$SSE_URL"
stop_server

# ---------------------------------------------------------------------------
# Mode 4: Stdio subprocess
# ---------------------------------------------------------------------------
run_smoke_check "Stdio (subprocess)" --stdio-cmd "uv run mcp-server-datahub"

# ---------------------------------------------------------------------------
# Mode 5: fastmcp run (create_app factory)
#
# This exercises the create_app() entry point that `fastmcp dev` uses.
# Under the hood, `fastmcp dev` runs:
#   npx @modelcontextprotocol/inspector uv run fastmcp run <spec>
# We use `uv run fastmcp run` directly as a substitute so that the test
# suite doesn't require Node.js / npx.  If the fastmcp implementation
# changes, this mode will catch regressions in the create_app() code path.
# ---------------------------------------------------------------------------
echo ""
echo "  Starting server via 'fastmcp run :create_app' (transport=http)..."
cd "$PROJECT_DIR"
uv run fastmcp run src/mcp_server_datahub/__main__.py:create_app --transport http \
    >"$LOG_DIR/fastmcp-run_server.stdout" \
    2>"$LOG_DIR/fastmcp-run_server.stderr" &
SERVER_PID=$!
wait_for_server "$HEALTH_URL"
run_smoke_check "fastmcp run (create_app factory)" --url "$HTTP_URL"
stop_server

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "======================================================================"
echo "  ALL MODES SUMMARY"
echo "======================================================================"

TOTAL=${#MODE_NAMES[@]}
PASS_COUNT=0
FAIL_COUNT=0

for i in "${!MODE_NAMES[@]}"; do
    if [[ "${MODE_RESULTS[$i]}" == "PASS" ]]; then
        icon="✓"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        icon="✗"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    printf "  %s  %-35s %s\n" "$icon" "${MODE_NAMES[$i]}" "${MODE_RESULTS[$i]}"
done

echo "----------------------------------------------------------------------"
echo "  Total: $TOTAL  |  Passed: $PASS_COUNT  |  Failed: $FAIL_COUNT"
echo "======================================================================"
echo ""
echo "  Logs: $LOG_DIR/"

if [[ $FAIL_COUNT -gt 0 ]]; then
    exit 1
fi
