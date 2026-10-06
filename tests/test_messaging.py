import ast
import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from distributed_cloud_scheduler.messaging import LocalQueue, Publisher


@pytest.mark.asyncio
async def test_local_queue_carries_identifiers_and_allows_duplicate_delivery() -> None:
    queue = LocalQueue()
    publisher: Publisher = queue
    first, second = uuid4(), uuid4()
    await publisher.publish(first)
    await publisher.publish(second)
    await publisher.publish(first)
    assert [await queue.receive() for _ in range(3)] == [first, second, first]


@pytest.mark.asyncio
async def test_local_queue_applies_backpressure_until_an_item_is_received() -> None:
    queue = LocalQueue(capacity=1)
    first, second = uuid4(), uuid4()
    await queue.publish(first)
    started = asyncio.Event()

    async def publish_second() -> None:
        started.set()
        await queue.publish(second)

    publishing = asyncio.create_task(publish_second())
    try:
        await started.wait()
        assert not publishing.done()
        assert await queue.receive() == first
        async with asyncio.timeout(1):
            await publishing
        assert await queue.receive() == second
    finally:
        if not publishing.done():
            publishing.cancel()
        await asyncio.gather(publishing, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_blocked_publication_does_not_insert_an_item() -> None:
    queue = LocalQueue(capacity=1)
    first, second = uuid4(), uuid4()
    await queue.publish(first)
    started = asyncio.Event()

    async def publish_second() -> None:
        started.set()
        await queue.publish(second)

    publishing = asyncio.create_task(publish_second())
    await started.wait()
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert await queue.receive() == first
    await queue.publish(first)
    assert await queue.receive() == first


@pytest.mark.parametrize("capacity", [0, -1, True, False, 1.5, "1"])
def test_local_queue_rejects_unbounded_or_invalid_capacity(capacity: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        LocalQueue(capacity=capacity)  # type: ignore[arg-type]


@pytest.mark.parametrize("module", ["jobs", "execution", "outbox", "messaging", "dispatcher"])
def test_domain_and_local_transport_do_not_import_cloud_dependencies(module: str) -> None:
    package = Path(__file__).resolve().parents[1] / "src" / "distributed_cloud_scheduler"
    tree = ast.parse((package / f"{module}.py").read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append(node.module)
    assert not any(name == "google" or name.startswith("google.") for name in imports)
