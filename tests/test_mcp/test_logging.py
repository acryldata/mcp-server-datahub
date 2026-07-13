import subprocess
import sys


def _run_logging_probe(*, debug: bool) -> subprocess.CompletedProcess[str]:
    script = f"""
from loguru import logger
from mcp_server_datahub.__main__ import _configure_logging

_configure_logging(debug={debug!r})
logger.debug("MCP_DEBUG_MARKER")
logger.info("MCP_INFO_MARKER")
"""
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def test_default_logging_hides_debug_messages() -> None:
    result = _run_logging_probe(debug=False)

    assert result.returncode == 0, result.stderr
    assert "MCP_INFO_MARKER" in result.stderr
    assert "MCP_DEBUG_MARKER" not in result.stderr


def test_debug_logging_shows_debug_messages() -> None:
    result = _run_logging_probe(debug=True)

    assert result.returncode == 0, result.stderr
    assert "MCP_INFO_MARKER" in result.stderr
    assert "MCP_DEBUG_MARKER" in result.stderr
