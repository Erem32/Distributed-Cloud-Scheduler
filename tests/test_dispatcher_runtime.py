"""Dispatcher shutdown drains a publication or leaves its lease replayable."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError

from distributed_cloud_scheduler import dispatcher
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.pubsub_config import PubSubSettings


def settings(**overrides: object) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test_user:dummy@localhost/scheduler_test",
        _env_file=None,
        **overrides,
    )


def resources(monkeypatch: pytest.MonkeyPatch) -> tuple[AsyncMock, AsyncMock]:
    database = AsyncMock()
    publisher = AsyncMock()
    monkeypatch.setattr(dispatcher, "Database", lambda configured: database)
    monkeypatch.setattr(dispatcher, "PubSubPublisher", lambda *args, **kwargs: publisher)
    return database, publisher


@pytest.mark.asyncio
async def test_once_attempts_one_publication_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, publisher = resources(monkeypatch)
    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(dispatcher, "dispatch_once", publish)
    await dispatcher.run_dispatcher(settings(), PubSubSettings(_env_file=None), once=True)
    publish.assert_awaited_once()
    publisher.close.assert_awaited_once()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_stops_admission_and_drains_current_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, publisher = resources(monkeypatch)
    entered, release, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    attempts = 0

    async def publish(*args: object) -> bool:
        nonlocal attempts
        attempts += 1
        entered.set()
        await release.wait()
        return True

    monkeypatch.setattr(dispatcher, "dispatch_once", publish)
    running = asyncio.create_task(
        dispatcher.run_dispatcher(settings(), PubSubSettings(_env_file=None), stop_event=stop)
    )
    async with asyncio.timeout(2):
        await entered.wait()
        stop.set()
        await asyncio.sleep(0)
        assert not running.done()
        publisher.close.assert_not_awaited()
        release.set()
        await running
    assert attempts == 1
    publisher.close.assert_awaited_once()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_cancels_a_stalled_publication_before_closing_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, publisher = resources(monkeypatch)
    entered, cancelled, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def publish(*args: object) -> bool:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return True

    monkeypatch.setattr(dispatcher, "dispatch_once", publish)
    running = asyncio.create_task(
        dispatcher.run_dispatcher(
            settings(dispatch_shutdown_timeout_seconds=0.01),
            PubSubSettings(_env_file=None),
            stop_event=stop,
        )
    )
    async with asyncio.timeout(2):
        await entered.wait()
        stop.set()
        await running
    assert cancelled.is_set()
    publisher.close.assert_awaited_once()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_transient_database_failure_retries_but_unexpected_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, publisher = resources(monkeypatch)
    attempts = AsyncMock(
        side_effect=[OperationalError("SELECT", {}, Exception("dummy")), RuntimeError("unexpected")]
    )
    monkeypatch.setattr(dispatcher, "dispatch_once", attempts)
    with pytest.raises(RuntimeError, match="unexpected"):
        await dispatcher.run_dispatcher(
            settings(dispatch_poll_interval_seconds=0.01), PubSubSettings(_env_file=None)
        )
    assert attempts.await_count == 2
    publisher.close.assert_awaited_once()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_publisher_initialization_failure_disposes_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, _ = resources(monkeypatch)

    def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("PUBSUB_TOPIC is required")

    monkeypatch.setattr(dispatcher, "PubSubPublisher", fail)
    with pytest.raises(ValueError, match="PUBSUB_TOPIC"):
        await dispatcher.run_dispatcher(settings(), PubSubSettings(_env_file=None))
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_external_cancellation_waits_for_publication_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, publisher = resources(monkeypatch)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def publish(*args: object) -> bool:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return True

    monkeypatch.setattr(dispatcher, "dispatch_once", publish)
    running = asyncio.create_task(
        dispatcher.run_dispatcher(settings(), PubSubSettings(_env_file=None))
    )
    async with asyncio.timeout(2):
        await entered.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    assert cancelled.is_set()
    publisher.close.assert_awaited_once()
    database.dispose.assert_awaited_once()
