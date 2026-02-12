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

    # Import and expose document_tools_middleware
    from mcp_server_datahub import document_tools_middleware

    mcp_module.document_tools_middleware = document_tools_middleware  # type: ignore[attr-defined]
    sys.modules["datahub_integrations.mcp.document_tools_middleware"] = (
        document_tools_middleware
    )

    # Import and expose version_requirements
    from mcp_server_datahub import version_requirements

    mcp_module.version_requirements = version_requirements  # type: ignore[attr-defined]
    sys.modules["datahub_integrations.mcp.version_requirements"] = version_requirements

    # Create datahub_integrations.mcp.tools submodule
    tools_module = types.ModuleType("datahub_integrations.mcp.tools")
    sys.modules["datahub_integrations.mcp.tools"] = tools_module
    mcp_module.tools = tools_module  # type: ignore[attr-defined]

    # Import tool modules directly and get from sys.modules to avoid __init__.py shadowing
    # The __init__.py re-exports functions which shadows the module names

    # Get actual module objects from sys.modules (not the shadowed function refs)
    descriptions_module = sys.modules["mcp_server_datahub.tools.descriptions"]
    documents_module = sys.modules["mcp_server_datahub.tools.documents"]
    domains_module = sys.modules["mcp_server_datahub.tools.domains"]
    get_me_module = sys.modules["mcp_server_datahub.tools.get_me"]
    owners_module = sys.modules["mcp_server_datahub.tools.owners"]
    save_document_module = sys.modules["mcp_server_datahub.tools.save_document"]
    structured_properties_module = sys.modules[
        "mcp_server_datahub.tools.structured_properties"
    ]
    tags_module = sys.modules["mcp_server_datahub.tools.tags"]
    terms_module = sys.modules["mcp_server_datahub.tools.terms"]

    tools_module.descriptions = descriptions_module  # type: ignore[attr-defined]
    tools_module.documents = documents_module  # type: ignore[attr-defined]
    tools_module.domains = domains_module  # type: ignore[attr-defined]
    tools_module.get_me = get_me_module  # type: ignore[attr-defined]
    tools_module.owners = owners_module  # type: ignore[attr-defined]
    tools_module.save_document = save_document_module  # type: ignore[attr-defined]
    tools_module.structured_properties = structured_properties_module  # type: ignore[attr-defined]
    tools_module.tags = tags_module  # type: ignore[attr-defined]
    tools_module.terms = terms_module  # type: ignore[attr-defined]

    sys.modules["datahub_integrations.mcp.tools.descriptions"] = descriptions_module
    sys.modules["datahub_integrations.mcp.tools.documents"] = documents_module
    sys.modules["datahub_integrations.mcp.tools.domains"] = domains_module
    sys.modules["datahub_integrations.mcp.tools.get_me"] = get_me_module
    sys.modules["datahub_integrations.mcp.tools.owners"] = owners_module
    sys.modules["datahub_integrations.mcp.tools.save_document"] = save_document_module
    sys.modules["datahub_integrations.mcp.tools.structured_properties"] = (
        structured_properties_module
    )
    sys.modules["datahub_integrations.mcp.tools.tags"] = tags_module
    sys.modules["datahub_integrations.mcp.tools.terms"] = terms_module

# === End Compatibility Layer ===

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"
