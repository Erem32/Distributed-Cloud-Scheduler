"""Run async tests on the same platform-compatible loop as the application."""

import asyncio
from collections.abc import Callable

from distributed_cloud_scheduler.__main__ import create_event_loop


def pytest_asyncio_loop_factories() -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    return {"application": create_event_loop}
