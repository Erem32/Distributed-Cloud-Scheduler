"""The small publisher contract and an in-memory local/test delivery adapter."""

import asyncio
from typing import Protocol
from uuid import UUID


class Publisher(Protocol):
    async def publish(self, job_id: UUID) -> None:
        """Publish an identifier; successful return means transport acceptance."""
        ...


class LocalQueue:
    """Bounded process-local delivery, without a durable broker guarantee."""

    def __init__(self, capacity: int = 1000) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._queue: asyncio.Queue[UUID] = asyncio.Queue(maxsize=capacity)

    async def publish(self, job_id: UUID) -> None:
        """Apply backpressure when the local delivery buffer is full."""
        await self._queue.put(job_id)

    async def receive(self) -> UUID:
        """Take the next published identifier; execution ownership is a DB concern."""
        return await self._queue.get()
