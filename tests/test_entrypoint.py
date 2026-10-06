import asyncio
import subprocess
import sys

import pytest

from distributed_cloud_scheduler.__main__ import create_event_loop


@pytest.mark.parametrize(
    "module", ["distributed_cloud_scheduler", "distributed_cloud_scheduler.worker"]
)
def test_cli_help_does_not_require_database_configuration(module: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    if module.endswith(".worker"):
        assert "--once" in result.stdout
    else:
        assert "--host" in result.stdout
        assert "--port" in result.stdout


def test_runtime_loop_supports_psycopg_on_windows() -> None:
    loop = create_event_loop()
    try:
        if sys.platform == "win32":
            assert isinstance(loop, asyncio.SelectorEventLoop)
        assert loop.run_until_complete(asyncio.sleep(0, result=42)) == 42
    finally:
        loop.close()
