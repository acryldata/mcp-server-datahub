import os

import pytest

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
