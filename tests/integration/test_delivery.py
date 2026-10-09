"""Job-ID transport uses the existing PostgreSQL ownership and execution rules."""

import asyncio
import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import JsonValue
from sqlalchemy import text

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.dispatcher import dispatch_once
from distributed_cloud_scheduler.execution import Claim, claim_job, claim_pending, recover_expired
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.messaging import LocalQueue
from distributed_cloud_scheduler.observability import METRICS
from distributed_cloud_scheduler.retries import RetryPolicy


@pytest_asyncio.fixture
async def delivery_database(migrated_integration_database_url: str) -> AsyncIterator[Database]:
    database = Database(
        Settings(database_url=migrated_integration_database_url, _env_file=None)
    )
    try:
        yield database
    finally:
        await database.dispose()


def settings_for(database: Database) -> Settings:
    return Settings(
        database_url=database.engine.url.render_as_string(hide_password=False), _env_file=None
    )


async def submit(database: Database) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session, JobCreate(type="calculation", payload={"numbers": [1, 2, 3]})
        )
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def receive(queue: LocalQueue) -> UUID:
    async with asyncio.timeout(2):
        return await queue.receive()


def metric_value(rendered: str, sample_name: str) -> int:
    for line in rendered.splitlines():
        if line.startswith(sample_name + " "):
            return int(line.rsplit(" ", maxsplit=1)[1])
    return 0


@pytest.mark.asyncio
async def test_api_outbox_local_delivery_executes_persisted_job(
    delivery_database: Database, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="distributed_cloud_scheduler.lifecycle")
    settings = settings_for(delivery_database)
    queue = LocalQueue()
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            submitted = await client.post(
                "/jobs", json={"type": "calculation", "payload": {"numbers": [1, 2, 3]}}
            )
            assert submitted.status_code == 201
            job_id = UUID(submitted.json()["id"])
            assert submitted.json()["status"] == "PENDING"
            async with delivery_database.session_factory() as session:
                assert await session.scalar(
                    text("SELECT status FROM dispatch_outbox WHERE job_id = :job_id"),
                    {"job_id": job_id},
                ) == "READY"

            assert await dispatch_once(delivery_database, queue, settings)
            assert not await dispatch_once(delivery_database, queue, settings)
            delivered_id = await receive(queue)
            assert delivered_id == job_id
            queued = (await client.get(f"/jobs/{job_id}")).json()
            assert queued["status"] == "QUEUED"
            assert queued["attempt_count"] == 0
            async with delivery_database.session_factory() as session:
                assert await session.scalar(
                    text("SELECT status FROM dispatch_outbox WHERE job_id = :job_id"),
                    {"job_id": job_id},
                ) == "PUBLISHED"

            claim = await claim_job(delivery_database, delivered_id, lease_seconds=60)
            assert claim is not None
            assert await worker.execute_claim(delivery_database, claim, settings)
            completed = (await client.get(f"/jobs/{job_id}")).json()
            assert completed["status"] == "SUCCEEDED"
            assert completed["attempt_count"] == 1
            assert completed["result_metadata"] == {"sum": 6}
            assert completed["error_info"] is None

    events = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "distributed_cloud_scheduler.lifecycle"
    ]
    job_events = [event for event in events if event.get("job_id") == str(job_id)]
    assert [event["event"] for event in job_events] == [
        "job_submission_created",
        "job_dispatch_completed",
        "job_attempt_completed",
    ]
    assert [event["service"] for event in job_events] == ["api", "dispatcher", "worker"]
    assert all(event["task_type"] == "calculation" for event in job_events)
    assert job_events[-1]["status"] == "SUCCEEDED"
    assert job_events[-1]["attempt"] == 1
    metrics = METRICS.render()
    assert 'dispatches_total{outcome="accepted"}' in metrics
    assert 'job_attempts_total{service="worker",outcome="succeeded"}' in metrics
    assert 'job_attempt_duration_seconds_count{service="worker"}' in metrics


@pytest.mark.asyncio
async def test_expired_owner_recovery_is_counted_without_inventing_duration(
    delivery_database: Database, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="distributed_cloud_scheduler.lifecycle")
    job_id = await submit(delivery_database)
    claim = await claim_pending(delivery_database, lease_seconds=60)
    assert claim is not None
    async with delivery_database.session_factory() as session, session.begin():
        await session.execute(
            text(
                "UPDATE jobs SET started_at = clock_timestamp() - interval '2 seconds', "
                "lease_expires_at = clock_timestamp() - interval '1 second' "
                "WHERE id = :job_id"
            ),
            {"job_id": job_id},
        )

    before = METRICS.render()
    assert await recover_expired(
        delivery_database, retry_policy=RetryPolicy(1, 1)
    ) == 1
    recovered = await read_job(delivery_database, job_id)
    assert recovered.status is JobStatus.RETRY_WAIT
    events = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "distributed_cloud_scheduler.lifecycle"
    ]
    event = next(item for item in events if item.get("job_id") == str(job_id))
    assert event["event"] == "job_attempt_recovered"
    assert event["status"] == "RETRY_WAIT"
    assert event["error_code"] == "worker_lost"
    assert "duration_seconds" not in event

    after = METRICS.render()
    attempt_metric = 'job_attempts_total{service="worker",outcome="retry_wait"}'
    retry_metric = 'job_retries_total{service="worker"}'
    duration_metric = 'job_attempt_duration_seconds_count{service="worker"}'
    assert metric_value(after, attempt_metric) - metric_value(before, attempt_metric) == 1
    assert metric_value(after, retry_metric) - metric_value(before, retry_metric) == 1
    assert metric_value(after, duration_metric) - metric_value(before, duration_metric) == 0


@pytest.mark.asyncio
async def test_duplicate_ids_during_and_after_execution_do_not_execute_twice(
    delivery_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(delivery_database)
    job_id = await submit(delivery_database)
    queue = LocalQueue()
    assert await dispatch_once(delivery_database, queue, settings)
    await queue.publish(job_id)
    await queue.publish(job_id)
    executed = 0
    execute_task = worker.execute_task

    async def counted_task(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal executed
        executed += 1
        return await execute_task(job_type, payload, attempt)

    monkeypatch.setattr(worker, "execute_task", counted_task)
    claim = await claim_job(delivery_database, await receive(queue), lease_seconds=60)
    assert claim is not None
    assert await claim_job(delivery_database, await receive(queue), lease_seconds=60) is None
    assert await worker.execute_claim(delivery_database, claim, settings)
    assert await claim_job(delivery_database, await receive(queue), lease_seconds=60) is None
    job = await read_job(delivery_database, job_id)
    assert executed == 1
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempt_count == 1
    assert job.result_metadata == {"sum": 6}


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_status", [JobStatus.PENDING, JobStatus.QUEUED])
async def test_concurrent_id_deliveries_grant_one_owner(
    delivery_database: Database, initial_status: JobStatus
) -> None:
    job_id = await submit(delivery_database)
    if initial_status is JobStatus.QUEUED:
        async with delivery_database.session_factory.begin() as session:
            await session.execute(
                text("UPDATE jobs SET status = 'QUEUED' WHERE id = :job_id"), {"job_id": job_id}
            )
    barrier = asyncio.Barrier(8)

    async def deliver() -> Claim | None:
        await barrier.wait()
        return await claim_job(delivery_database, job_id, lease_seconds=60)

    async with asyncio.timeout(10):
        claims = await asyncio.gather(*(deliver() for _ in range(8)))
    owners = [claim for claim in claims if claim is not None]
    assert len(owners) == 1
    assert owners[0].id == job_id
    job = await read_job(delivery_database, job_id)
    assert job.status is JobStatus.RUNNING
    assert job.attempt_count == 1
    assert job.claim_token == owners[0].token


@pytest.mark.asyncio
async def test_unknown_id_does_not_claim_an_unrelated_pending_job(
    delivery_database: Database,
) -> None:
    job_id = await submit(delivery_database)
    assert await claim_job(delivery_database, uuid4(), lease_seconds=60) is None
    job = await read_job(delivery_database, job_id)
    assert job.status is JobStatus.PENDING
    assert job.attempt_count == 0


@pytest.mark.asyncio
async def test_id_delivery_selects_target_even_when_older_pending_job_exists(
    delivery_database: Database,
) -> None:
    first_id = await submit(delivery_database)
    target_id = await submit(delivery_database)
    claim = await claim_job(delivery_database, target_id, lease_seconds=60)
    assert claim is not None
    assert claim.id == target_id
    first = await read_job(delivery_database, first_id)
    assert first.status is JobStatus.PENDING
    assert first.attempt_count == 0


@pytest.mark.asyncio
async def test_local_polling_can_execute_a_queued_job(delivery_database: Database) -> None:
    settings = settings_for(delivery_database)
    job_id = await submit(delivery_database)
    queue = LocalQueue()
    assert await dispatch_once(delivery_database, queue, settings)
    claim = await claim_pending(delivery_database, lease_seconds=60)
    assert claim is not None
    assert claim.id == job_id
    assert await worker.execute_claim(delivery_database, claim, settings)
    # Later delivery is harmless even when the local database worker already finished it.
    assert await receive(queue) == job_id
    assert await claim_job(delivery_database, job_id, lease_seconds=60) is None
    job = await read_job(delivery_database, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.attempt_count == 1
