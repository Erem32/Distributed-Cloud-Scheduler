"""A scheduler polls in bounded batches and drains one active admission transaction."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from distributed_cloud_scheduler import scheduler
from distributed_cloud_scheduler.config import Settings


def settings(**overrides: object) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test_user:dummy@localhost/scheduler_test",
        _env_file=None,
        **overrides,
    )


def database_resource(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    database = AsyncMock()
    monkeypatch.setattr(scheduler, "Database", lambda configured: database)
    return database


@pytest.mark.asyncio
async def test_once_admits_one_configured_batch_then_disposes_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = database_resource(monkeypatch)
    admit = AsyncMock(return_value=7)
    monkeypatch.setattr(scheduler, "schedule_due", admit)
    await scheduler.run_scheduler(settings(scheduler_batch_size=7), once=True)
    admit.assert_awaited_once_with(database, limit=7)
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_before_start_does_not_admit_a_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    database = database_resource(monkeypatch)
    admit = AsyncMock(return_value=1)
    monkeypatch.setattr(scheduler, "schedule_due", admit)
    stop = asyncio.Event()
    stop.set()
    await scheduler.run_scheduler(settings(), stop_event=stop)
    admit.assert_not_awaited()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_stops_new_batches_and_drains_active_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = database_resource(monkeypatch)
    entered, release, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def admit(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return 1

    monkeypatch.setattr(scheduler, "schedule_due", admit)
    running = asyncio.create_task(scheduler.run_scheduler(settings(), stop_event=stop))
    async with asyncio.timeout(2):
        await entered.wait()
        stop.set()
        await asyncio.sleep(0)
        assert not running.done()
        database.dispose.assert_not_awaited()
        release.set()
        await running
    assert calls == 1
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_cancels_stalled_batch_and_waits_for_transaction_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = database_resource(monkeypatch)
    entered, cancelled, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def admit(*args: object, **kwargs: object) -> int:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return 1

    monkeypatch.setattr(scheduler, "schedule_due", admit)
    running = asyncio.create_task(
        scheduler.run_scheduler(settings(scheduler_shutdown_timeout_seconds=0.01), stop_event=stop)
    )
    async with asyncio.timeout(2):
        await entered.wait()
        stop.set()
        await running
    assert cancelled.is_set()
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT", {}, Exception("dummy")),
        SQLAlchemyTimeoutError("dummy"),
    ],
)
async def test_polling_retries_database_outages_but_propagates_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    database = database_resource(monkeypatch)
    batches = AsyncMock(side_effect=[error, RuntimeError("unexpected")])
    monkeypatch.setattr(scheduler, "schedule_due", batches)
    with pytest.raises(RuntimeError, match="unexpected"):
        await scheduler.run_scheduler(settings(scheduler_poll_interval_seconds=0.01))
    assert batches.await_count == 2
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_once_surfaces_database_outage_and_disposes_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = database_resource(monkeypatch)
    monkeypatch.setattr(
        scheduler,
        "schedule_due",
        AsyncMock(side_effect=OperationalError("SELECT", {}, Exception("dummy"))),
    )
    with pytest.raises(OperationalError):
        await scheduler.run_scheduler(settings(), once=True)
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_empty_poll_wait_is_interruptible_by_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    database = database_resource(monkeypatch)
    first, stop = asyncio.Event(), asyncio.Event()
    calls = 0

    async def admit(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        first.set()
        return 0

    monkeypatch.setattr(scheduler, "schedule_due", admit)
    running = asyncio.create_task(
        scheduler.run_scheduler(settings(scheduler_poll_interval_seconds=10), stop_event=stop)
    )
    async with asyncio.timeout(2):
        await first.wait()
        stop.set()
        await running
    assert calls == 1
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_external_cancellation_finishes_batch_cleanup_before_database_disposal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = database_resource(monkeypatch)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def admit(*args: object, **kwargs: object) -> int:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return 1

    monkeypatch.setattr(scheduler, "schedule_due", admit)
    running = asyncio.create_task(scheduler.run_scheduler(settings()))
    async with asyncio.timeout(2):
        await entered.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    assert cancelled.is_set()
    database.dispose.assert_awaited_once()
