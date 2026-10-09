"""Ownership and completion invariants proved against real PostgreSQL."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import JsonValue
from sqlalchemy import text

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_pending, complete_claim, recover_expired
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.retries import RetryPolicy

LONG_RETRY = RetryPolicy(base_seconds=60, max_seconds=60)


@pytest_asyncio.fixture
async def execution_database(migrated_integration_database_url: str) -> AsyncIterator[Database]:
    database = Database(
        Settings(database_url=migrated_integration_database_url, _env_file=None)
    )
    try:
        yield database
    finally:
        await database.dispose()


async def submit(database: Database, *, max_attempts: int = 3) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(
                type="calculation", payload={"numbers": [1, 2, 3]}, max_attempts=max_attempts
            ),
        )
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def expire_claim(database: Database, job_id: UUID) -> None:
    async with database.session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE jobs SET started_at = clock_timestamp() - INTERVAL '2 seconds', "
                "lease_expires_at = clock_timestamp() - INTERVAL '1 second' "
                "WHERE id = :job_id"
            ),
            {"job_id": job_id},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("contenders", [2, 8])
async def test_simultaneous_claims_grant_one_owner_and_one_attempt(
    execution_database: Database, contenders: int
) -> None:
    job_id = await submit(execution_database)
    barrier = asyncio.Barrier(contenders)

    async def contend():
        await barrier.wait()
        return await claim_pending(execution_database, lease_seconds=60)

    async with asyncio.timeout(10):
        claims = await asyncio.gather(*(contend() for _ in range(contenders)))
    owners = [claim for claim in claims if claim is not None]
    assert len(owners) == 1
    owner = owners[0]
    assert owner.id == job_id
    assert owner.attempt_count == 1
    assert owner.payload == {"numbers": [1, 2, 3]}

    job = await read_job(execution_database, job_id)
    assert job.status is JobStatus.RUNNING
    assert job.attempt_count == 1
    assert job.claim_token == owner.token
    assert job.started_at is not None
    assert job.lease_expires_at is not None
    assert job.started_at.tzinfo is not None
    assert job.lease_expires_at > job.started_at
    assert job.result_metadata is None
    assert job.error_info is None
    assert await claim_pending(execution_database, lease_seconds=60) is None

    # Ownership is durable, but the claiming transaction must release its row lock.
    async with execution_database.session_factory.begin() as session:
        row_id = await session.scalar(
            text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE NOWAIT"), {"job_id": job_id}
        )
        assert row_id == job_id


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [{"sum": 6}, {}])
async def test_wrong_token_and_terminal_completions_cannot_replace_success(
    execution_database: Database, result: dict[str, JsonValue]
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    wrong_owner = replace(claim, token=uuid4())
    assert not await complete_claim(execution_database, wrong_owner, result={"sum": 999})
    running = await read_job(execution_database, job_id)
    assert running.status is JobStatus.RUNNING
    assert running.claim_token == claim.token
    assert running.result_metadata is None
    assert running.finished_at is None

    assert await complete_claim(execution_database, claim, result=result)
    assert not await complete_claim(execution_database, claim, result={"sum": 999})
    assert not await complete_claim(
        execution_database, claim, error={"code": "late_failure", "retryable": False}
    )
    succeeded = await read_job(execution_database, job_id)
    assert succeeded.status is JobStatus.SUCCEEDED
    assert succeeded.result_metadata == result
    assert succeeded.error_info is None
    assert succeeded.attempt_count == 1
    assert succeeded.finished_at is not None
    assert succeeded.finished_at.tzinfo is not None
    assert succeeded.claim_token is None
    assert succeeded.lease_expires_at is None


@pytest.mark.asyncio
async def test_completion_for_unknown_job_is_rejected(execution_database: Database) -> None:
    await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    assert not await complete_claim(execution_database, replace(claim, id=uuid4()), result={})


@pytest.mark.asyncio
async def test_failure_at_attempt_budget_is_terminal_without_reexecution(
    execution_database: Database,
) -> None:
    job_id = await submit(execution_database, max_attempts=1)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    error = {"code": "controlled_failure", "retryable": True}
    assert await complete_claim(execution_database, claim, error=error)
    failed = await read_job(execution_database, job_id)
    assert failed.status is JobStatus.DEAD_LETTERED
    assert failed.dead_letter_reason == "attempts_exhausted"
    assert failed.dead_lettered_at == failed.finished_at
    assert failed.error_info == error
    assert failed.result_metadata is None
    assert failed.attempt_count == 1
    assert failed.finished_at is not None
    assert failed.claim_token is None
    assert failed.lease_expires_at is None
    assert await claim_pending(execution_database, lease_seconds=60) is None


@pytest.mark.asyncio
async def test_expired_completion_is_fenced_before_and_after_recovery(
    execution_database: Database,
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    assert await recover_expired(execution_database) == 0
    await expire_claim(execution_database, job_id)

    assert not await complete_claim(execution_database, claim, result={"sum": 6})
    expired = await read_job(execution_database, job_id)
    assert expired.status is JobStatus.RUNNING
    assert expired.claim_token == claim.token
    assert expired.result_metadata is None
    assert expired.error_info is None

    assert await recover_expired(execution_database, retry_policy=LONG_RETRY) == 1
    assert not await complete_claim(execution_database, claim, result={"sum": 6})
    recovered = await read_job(execution_database, job_id)
    assert recovered.status is JobStatus.RETRY_WAIT
    assert recovered.error_info
    assert recovered.result_metadata is None
    assert recovered.attempt_count == 1
    assert recovered.finished_at is not None
    assert recovered.next_attempt_at is not None
    assert recovered.claim_token is None
    assert recovered.lease_expires_at is None
    assert await recover_expired(execution_database) == 0
    assert await claim_pending(execution_database, lease_seconds=60) is None


@pytest.mark.asyncio
async def test_completion_rechecks_database_clock_after_waiting_for_unchanged_row_lock(
    execution_database: Database,
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=2)
    assert claim is not None
    application_name = f"completion_{uuid4().hex}"
    waiter_url = execution_database.engine.url.update_query_dict(
        {"application_name": application_name}
    ).render_as_string(hide_password=False)
    waiter_database = Database(Settings(database_url=waiter_url, _env_file=None))
    completion: asyncio.Task[bool] | None = None
    try:
        async with execution_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            await holder.execute(
                text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE"), {"job_id": job_id}
            )
            completion = asyncio.create_task(
                complete_claim(waiter_database, claim, result={"sum": 6})
            )
            async with asyncio.timeout(5):
                while True:
                    # A fresh transaction avoids caching a PostgreSQL activity snapshot.
                    async with execution_database.session_factory() as observer:
                        blocked = await observer.scalar(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                "WHERE application_name = :application_name "
                                "AND :holder_pid = ANY(pg_blocking_pids(pid)))"
                            ),
                            {"application_name": application_name, "holder_pid": holder_pid},
                        )
                    if blocked:
                        break

            # This confirms completion started waiting before ownership expired.
            assert await holder.scalar(
                text("SELECT lease_expires_at > clock_timestamp() FROM jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )
            # Wait for this lease's actual DB deadline, without changing the locked tuple.
            await holder.execute(
                text(
                    "SELECT pg_sleep(GREATEST(0, "
                    "EXTRACT(EPOCH FROM lease_expires_at - clock_timestamp())) + 0.01) "
                    "FROM jobs WHERE id = :job_id"
                ),
                {"job_id": job_id},
            )
        async with asyncio.timeout(5):
            assert not await completion
        job = await read_job(execution_database, job_id)
        assert job.status is JobStatus.RUNNING
        assert job.claim_token == claim.token
        assert job.result_metadata is None
        assert job.finished_at is None
    finally:
        if completion is not None and not completion.done():
            completion.cancel()
            await asyncio.gather(completion, return_exceptions=True)
        await waiter_database.dispose()


@pytest.mark.asyncio
async def test_recovery_uses_bounded_batches_and_preserves_attempt_counts(
    execution_database: Database,
) -> None:
    job_ids = [await submit(execution_database) for _ in range(3)]
    for _ in job_ids:
        claim = await claim_pending(execution_database, lease_seconds=60)
        assert claim is not None
        await expire_claim(execution_database, claim.id)
    assert await recover_expired(execution_database, limit=2, retry_policy=LONG_RETRY) == 2
    statuses = [(await read_job(execution_database, job_id)).status for job_id in job_ids]
    assert statuses.count(JobStatus.RETRY_WAIT) == 2
    assert statuses.count(JobStatus.RUNNING) == 1
    assert await recover_expired(execution_database, limit=2, retry_policy=LONG_RETRY) == 1
    assert await recover_expired(execution_database, limit=2) == 0
    for job_id in job_ids:
        job = await read_job(execution_database, job_id)
        assert job.status is JobStatus.RETRY_WAIT
        assert job.next_attempt_at is not None
        assert job.attempt_count == 1
        assert job.claim_token is None
        assert job.lease_expires_at is None
    assert await claim_pending(execution_database, lease_seconds=60) is None


@pytest.mark.asyncio
async def test_pending_job_at_attempt_budget_cannot_be_claimed(
    execution_database: Database,
) -> None:
    job_id = await submit(execution_database)
    async with execution_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET attempt_count = max_attempts WHERE id = :job_id"),
            {"job_id": job_id},
        )
    assert await claim_pending(execution_database, lease_seconds=60) is None
    job = await read_job(execution_database, job_id)
    assert job.status is JobStatus.PENDING
    assert job.attempt_count == job.max_attempts
    assert job.claim_token is None
    assert job.lease_expires_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcomes",
    [
        {"result": {"sum": float("nan")}},
        {"error": {"code": "\x00"}},
        {"result": {"value": "x" * 65_536}},
    ],
)
async def test_invalid_outcome_metadata_preserves_current_owner(
    execution_database: Database, outcomes: dict[str, dict[str, JsonValue]]
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    with pytest.raises(ValueError):
        await complete_claim(execution_database, claim, **outcomes)
    job = await read_job(execution_database, job_id)
    assert job.status is JobStatus.RUNNING
    assert job.claim_token == claim.token
    assert job.lease_expires_at is not None
    assert job.result_metadata is None
    assert job.error_info is None
    assert job.finished_at is None


@pytest.mark.asyncio
async def test_cyclic_outcome_is_rejected_without_changing_ownership(
    execution_database: Database,
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    cyclic: dict[str, JsonValue] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError):
        await complete_claim(execution_database, claim, result=cyclic)
    job = await read_job(execution_database, job_id)
    assert job.status is JobStatus.RUNNING
    assert job.claim_token == claim.token
    assert job.result_metadata is None
    assert job.error_info is None
    assert job.finished_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_seconds", [0, -1])
async def test_claim_rejects_nonpositive_lease(
    execution_database: Database, lease_seconds: int
) -> None:
    job_id = await submit(execution_database)
    with pytest.raises(ValueError):
        await claim_pending(execution_database, lease_seconds=lease_seconds)
    assert (await read_job(execution_database, job_id)).status is JobStatus.PENDING


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 1001])
async def test_recovery_rejects_unbounded_batch_size(
    execution_database: Database, limit: int
) -> None:
    with pytest.raises(ValueError):
        await recover_expired(execution_database, limit=limit)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcomes", [{}, {"result": {}, "error": {}}])
async def test_completion_requires_exactly_one_outcome(
    execution_database: Database, outcomes: dict[str, dict[str, JsonValue]]
) -> None:
    job_id = await submit(execution_database)
    claim = await claim_pending(execution_database, lease_seconds=60)
    assert claim is not None
    with pytest.raises(ValueError):
        await complete_claim(execution_database, claim, **outcomes)
    assert (await read_job(execution_database, job_id)).status is JobStatus.RUNNING
