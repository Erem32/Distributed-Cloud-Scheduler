"""Bounded concurrent execution and shutdown behavior against real PostgreSQL."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from pydantic import JsonValue
from sqlalchemy.exc import OperationalError

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job


@pytest.fixture
def concurrency_settings(integration_database_url: str) -> Settings:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    return Settings(database_url=integration_database_url, _env_file=None)


@pytest_asyncio.fixture
async def concurrency_database(concurrency_settings: Settings) -> AsyncIterator[Database]:
    database = Database(concurrency_settings)
    try:
        yield database
    finally:
        await database.dispose()


async def submit_jobs(database: Database, count: int) -> list[UUID]:
    ids = []
    for index in range(count):
        async with database.session_factory() as session:
            job = await create_job(
                session, JobCreate(type="calculation", payload={"index": index})
            )
            ids.append(job.id)
    return ids


async def read_jobs(database: Database, ids: list[UUID]) -> list[Job]:
    async with database.session_factory() as session:
        jobs = [await session.get(Job, job_id) for job_id in ids]
        assert all(job is not None for job in jobs)
        return [job for job in jobs if job is not None]


async def clean_up_worker(
    running: asyncio.Task[None], stop: asyncio.Event, release: asyncio.Event | None = None
) -> None:
    """Leave no consumer tasks behind if an assertion or bounded wait fails."""
    stop.set()
    if release is not None:
        release.set()
    if not running.done():
        running.cancel()
    await asyncio.gather(running, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 3, 20])
async def test_worker_reaches_configured_limit_without_exceeding_it(
    concurrency_database: Database,
    concurrency_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    concurrency: int,
) -> None:
    count = 2 * concurrency + 1
    ids = await submit_jobs(concurrency_database, count)
    settings = concurrency_settings.model_copy(update={"worker_concurrency": concurrency})
    condition = asyncio.Condition()
    release = asyncio.Event()
    stop = asyncio.Event()
    active = 0
    peak = 0
    finished = 0
    executed: list[int] = []

    async def gated_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal active, peak, finished
        index = payload["index"]
        assert isinstance(index, int)
        async with condition:
            active += 1
            peak = max(peak, active)
            executed.append(index)
            condition.notify_all()
        try:
            await release.wait()
            return {"index": index}
        finally:
            async with condition:
                active -= 1
                finished += 1
                if finished == count:
                    stop.set()
                condition.notify_all()

    monkeypatch.setattr(worker, "execute_task", gated_handler)
    running = asyncio.create_task(worker.run_worker(settings, stop_event=stop))
    try:
        async with asyncio.timeout(10), condition:
            await condition.wait_for(lambda: active == concurrency)
        assert peak == concurrency
        release.set()
        async with asyncio.timeout(10):
            await running
    finally:
        await clean_up_worker(running, stop, release)

    assert peak == concurrency
    assert active == 0
    assert sorted(executed) == list(range(count))
    jobs = await read_jobs(concurrency_database, ids)
    assert all(job.status is JobStatus.SUCCEEDED for job in jobs)
    assert all(job.attempt_count == 1 for job in jobs)
    assert all(job.claim_token is None and job.lease_expires_at is None for job in jobs)
    assert [job.result_metadata for job in jobs] == [{"index": index} for index in range(count)]


@pytest.mark.asyncio
async def test_shutdown_drains_active_jobs_and_leaves_remaining_jobs_pending(
    concurrency_database: Database,
    concurrency_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await submit_jobs(concurrency_database, 5)
    settings = concurrency_settings.model_copy(update={"worker_concurrency": 2})
    condition = asyncio.Condition()
    release = asyncio.Event()
    stop = asyncio.Event()
    entered: list[int] = []

    async def gated_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        index = payload["index"]
        assert isinstance(index, int)
        async with condition:
            entered.append(index)
            condition.notify_all()
        await release.wait()
        return {"index": index}

    monkeypatch.setattr(worker, "execute_task", gated_handler)
    running = asyncio.create_task(worker.run_worker(settings, stop_event=stop))
    try:
        async with asyncio.timeout(5), condition:
            await condition.wait_for(lambda: len(entered) == 2)
        stop.set()
        # Waiting briefly for completion must time out while active handlers remain gated.
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await asyncio.shield(running)
        release.set()
        async with asyncio.timeout(5):
            await running
    finally:
        await clean_up_worker(running, stop, release)

    assert len(entered) == 2
    jobs = await read_jobs(concurrency_database, ids)
    assert [job.status for job in jobs].count(JobStatus.SUCCEEDED) == 2
    assert [job.status for job in jobs].count(JobStatus.PENDING) == 3
    for job in jobs:
        assert job.attempt_count == (1 if job.status is JobStatus.SUCCEEDED else 0)
        assert job.claim_token is None
        assert job.lease_expires_at is None


@pytest.mark.asyncio
async def test_shutdown_grace_expiry_cancels_claims_without_admitting_more_work(
    concurrency_database: Database,
    concurrency_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await submit_jobs(concurrency_database, 4)
    settings = concurrency_settings.model_copy(
        update={"worker_concurrency": 2, "worker_shutdown_timeout_seconds": 0.03}
    )
    condition = asyncio.Condition()
    stop = asyncio.Event()
    entered = 0
    cancelled = 0

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal entered, cancelled
        async with condition:
            entered += 1
            condition.notify_all()
        try:
            await asyncio.Event().wait()
            return {}
        except asyncio.CancelledError:
            cancelled += 1
            raise

    monkeypatch.setattr(worker, "execute_task", held_handler)
    running = asyncio.create_task(worker.run_worker(settings, stop_event=stop))
    try:
        async with asyncio.timeout(5), condition:
            await condition.wait_for(lambda: entered == 2)
        stop.set()
        async with asyncio.timeout(5):
            await running
    finally:
        await clean_up_worker(running, stop)

    assert entered == cancelled == 2
    jobs = await read_jobs(concurrency_database, ids)
    assert [job.status for job in jobs].count(JobStatus.FAILED) == 2
    assert [job.status for job in jobs].count(JobStatus.PENDING) == 2
    for job in jobs:
        assert job.claim_token is None
        assert job.lease_expires_at is None
        if job.status is JobStatus.FAILED:
            assert job.error_info == {"code": "cancelled", "retryable": True}
            assert job.attempt_count == 1
            assert job.finished_at is not None
        else:
            assert job.attempt_count == 0
            assert job.error_info is None


@pytest.mark.asyncio
async def test_fatal_consumer_error_propagates_after_other_claims_are_cleaned_up(
    concurrency_database: Database,
    concurrency_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await submit_jobs(concurrency_database, 1)
    settings = concurrency_settings.model_copy(update={"worker_concurrency": 2})
    started = asyncio.Event()
    cancelled = asyncio.Event()
    disposed = asyncio.Event()
    real_claim_pending = worker.claim_pending
    claims = 0

    class TrackedDatabase(Database):
        async def dispose(self) -> None:
            try:
                await super().dispose()
            finally:
                disposed.set()

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        try:
            await asyncio.Event().wait()
            return {}
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def failing_claim(database: Database, lease_seconds: int):
        nonlocal claims
        claims += 1
        if claims == 1:
            return await real_claim_pending(database, lease_seconds)
        await started.wait()
        raise RuntimeError("fatal consumer failure")

    monkeypatch.setattr(worker, "Database", TrackedDatabase)
    monkeypatch.setattr(worker, "claim_pending", failing_claim)
    monkeypatch.setattr(worker, "execute_task", held_handler)
    async with asyncio.timeout(5):
        with pytest.raises(RuntimeError, match="fatal consumer failure"):
            await worker.run_worker(settings)
    assert started.is_set()
    assert cancelled.is_set()
    assert disposed.is_set()
    job = (await read_jobs(concurrency_database, ids))[0]
    assert job.status is JobStatus.FAILED
    assert job.attempt_count == 1
    assert job.error_info == {"code": "cancelled", "retryable": True}
    assert job.claim_token is None
    assert job.lease_expires_at is None


@pytest.mark.asyncio
async def test_transient_database_poll_error_does_not_stop_the_worker(
    concurrency_database: Database,
    concurrency_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with concurrency_database.session_factory() as session:
        submitted = await create_job(
            session, JobCreate(type="calculation", payload={"numbers": [1, 2]})
        )
        job_id = submitted.id
    settings = concurrency_settings.model_copy(
        update={"worker_concurrency": 1, "worker_poll_interval_seconds": 0.01}
    )
    real_run_once = worker.run_once
    stop = asyncio.Event()
    polls = 0

    async def transient_poll(
        database: Database, configured: Settings, *, stop_event: asyncio.Event | None = None
    ) -> bool:
        nonlocal polls
        polls += 1
        if polls == 1:
            raise OperationalError("SELECT 1", {}, Exception("database temporarily unavailable"))
        result = await real_run_once(database, configured, stop_event=stop_event)
        stop.set()
        return result

    monkeypatch.setattr(worker, "run_once", transient_poll)
    async with asyncio.timeout(5):
        await worker.run_worker(settings, stop_event=stop)
    assert polls == 2
    job = (await read_jobs(concurrency_database, [job_id]))[0]
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempt_count == 1
    assert job.result_metadata == {"sum": 3}
