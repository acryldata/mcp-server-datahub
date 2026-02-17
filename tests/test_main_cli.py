import pytest
from click.testing import CliRunner

from mcp_server_datahub.__main__ import main


@pytest.mark.parametrize(
    "args",
    [
        ["--transport", "stdio", "--host", "127.0.0.1"],
        ["--transport", "stdio", "--port", "8080"],
    ],
)
def test_stdio_rejects_host_and_port_options(args: list[str]) -> None:
    runner = CliRunner()
    result = runner.invoke(main, args)

    assert result.exit_code == 2
    assert (
        "--host/--port can only be used with --transport http or sse" in result.output
    )
