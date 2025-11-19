"""Test configuration with compatibility layer for cross-repo testing."""

import os
import sys
from pathlib import Path

import pytest

# === Compatibility Layer for Cross-Repo Testing ===
# This allows tests to use datahub_integrations imports in both repos
repo_root = Path(__file__).resolve().parents[1]

possible_locations = [
    # Integrations service structure
    repo_root / "src" / "datahub_integrations",
    # OSS structure
    repo_root / "src" / "mcp_server_datahub",
]

# Find which package structure exists
using_oss = False
for loc in possible_locations:
    if loc.exists() and loc.name == "mcp_server_datahub":
        using_oss = True
        break

# If in OSS repo, create datahub_integrations compatibility shim
if using_oss:
    import types

    # Create datahub_integrations package
    datahub_integrations = types.ModuleType("datahub_integrations")
    sys.modules["datahub_integrations"] = datahub_integrations

    # Create datahub_integrations.mcp submodule
    mcp_module = types.ModuleType("datahub_integrations.mcp")
    sys.modules["datahub_integrations.mcp"] = mcp_module
    datahub_integrations.mcp = mcp_module  # type: ignore[attr-defined]  # Dynamic attribute

    # Import and expose mcp_server
    from mcp_server_datahub import mcp_server

    mcp_module.mcp_server = mcp_server  # type: ignore[attr-defined]  # Dynamic attribute
    sys.modules["datahub_integrations.mcp.mcp_server"] = mcp_server

# === End Compatibility Layer ===

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"
