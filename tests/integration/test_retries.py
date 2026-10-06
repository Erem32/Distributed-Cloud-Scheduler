"""Application-owned retries and diagnostic history against real PostgreSQL."""

import asyncio
import base64
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.attempts import AttemptStatus, JobAttempt
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import (
    Claim,
    claim_job,
    claim_pending,
    complete_claim,
    recover_expired,
)
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.outbox import (
    DispatchOutbox,
    DispatchStatus,
    claim_dispatch,
    complete_dispatch,
)
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app
from distributed_cloud_scheduler.retries import RetryPolicy

SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"
LONG_RETRY = RetryPolicy(base_seconds=60, max_seconds=60)
FAILURE = {"code": "controlled_failure", "retryable": True}


@pytest_asyncio.fixture
async def retry_database(integration_database_url: str) -> AsyncIterator[Database]:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    database = Database(settings_for_url(integration_database_url))
    try:
        yield database
    finally:
        await database.dispose()


def settings_for_url(url: str) -> Settings:
    return Settings(
        database_url=url,
        execution_retry_base_seconds=60,
        execution_retry_max_seconds=60,
        _env_file=None,
    )


def settings_for(database: Database) -> Settings:
    return settings_for_url(database.engine.url.render_as_string(hide_password=False))


async def submit(database: Database, *, fail_until_attempt: int = 1, max_attempts: int = 3) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(
                type="controlled_failure",
                payload={"fail_until_attempt": fail_until_attempt},
                max_attempts=max_attempts,
            ),
        )
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def history(database: Database, job_id: UUID) -> list[JobAttempt]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number)
            )
        )


async def outbox(database: Database, job_id: UUID) -> list[DispatchOutbox]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(DispatchOutbox)
                .where(DispatchOutbox.job_id == job_id)
                .order_by(DispatchOutbox.created_at, DispatchOutbox.id)
            )
        )


async def mark_due(database: Database, job_id: UUID) -> None:
    """Simulate time passing without sleeping; retain history's chosen original date."""
    async with database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = :job_id"),
            {"job_id": job_id},
        )
        await session.execute(
            text(
                "UPDATE dispatch_outbox SET available_at = clock_timestamp() "
                "WHERE job_id = :job_id AND status = 'READY'"
            ),
            {"job_id": job_id},
        )


async def expire(database: Database, job_id: UUID) -> None:
    async with database.session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE jobs SET started_at = clock_timestamp() - interval '2 seconds', "
                "lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = :job_id"
            ),
            {"job_id": job_id},
        )


def envelope(job_id: UUID) -> dict[str, object]:
    return {
        "message": {
            "messageId": uuid4().hex,
            "data": base64.b64encode(str(job_id).encode("ascii")).decode("ascii"),
            "attributes": {"attempt_count": "20"},
        },
        "subscription": SUBSCRIPTION,
        "deliveryAttempt": 99,
    }


@pytest.mark.asyncio
async def test_retry_schedule_history_and_outbox_commit_together_then_succeed(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    first = await claim_job(retry_database, job_id, lease_seconds=60)
    assert first is not None
    attempts = await history(retry_database, job_id)
    assert len(attempts) == 1
    assert attempts[0].status is AttemptStatus.RUNNING
    assert attempts[0].claim_token == first.token
    assert attempts[0].finished_at is None
    assert await worker.execute_claim(retry_database, first, settings_for(retry_database))

    waiting = await read_job(retry_database, job_id)
    attempts = await history(retry_database, job_id)
    obligations = await outbox(retry_database, job_id)
    failed_attempt = attempts[0]
    assert waiting.status is JobStatus.RETRY_WAIT
    assert waiting.attempt_count == 1
    assert waiting.finished_at is not None
    assert waiting.claim_token is None and waiting.lease_expires_at is None
    assert waiting.error_info == FAILURE
    assert failed_attempt.status is AttemptStatus.FAILED
    assert failed_attempt.error_info == FAILURE
    assert failed_attempt.result_metadata is None
    assert failed_attempt.finished_at is not None
    assert failed_attempt.finished_at == waiting.finished_at
    assert failed_attempt.next_attempt_at == waiting.next_attempt_at == obligations[-1].available_at
    delay = (failed_attempt.next_attempt_at - failed_attempt.finished_at).total_seconds()
    assert 30 <= delay <= 60
    assert len(obligations) == 2
    assert obligations[-1].status is DispatchStatus.READY
    assert obligations[-1].publish_attempt_count == 0

    assert await claim_job(retry_database, job_id, lease_seconds=60) is None
    assert await claim_pending(retry_database, lease_seconds=60) is None
    assert not await complete_claim(retry_database, first, result={"attempt": 99})
    assert not await complete_claim(retry_database, first, error=FAILURE)
    assert (await read_job(retry_database, job_id)).next_attempt_at == waiting.next_attempt_at
    assert len(await history(retry_database, job_id)) == 1
    assert len(await outbox(retry_database, job_id)) == 2

    await mark_due(retry_database, job_id)
    second = await claim_pending(retry_database, lease_seconds=60)
    assert second is not None and second.id == job_id and second.attempt_count == 2
    assert second.token != first.token
    assert not await complete_claim(retry_database, first, result={"attempt": 99})
    assert await worker.execute_claim(retry_database, second, settings_for(retry_database))
    finished = await read_job(retry_database, job_id)
    assert finished.status is JobStatus.SUCCEEDED
    assert finished.attempt_count == 2
    assert finished.result_metadata == {"attempt": 2}
    assert finished.error_info is None
    assert finished.next_attempt_at is None
    attempts = await history(retry_database, job_id)
    assert [attempt.status for attempt in attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCEEDED,
    ]
    assert attempts[0].next_attempt_at == waiting.next_attempt_at
    assert attempts[1].next_attempt_at is None
    assert attempts[1].result_metadata == {"attempt": 2}
    assert attempts[1].error_info is None
    assert len(await outbox(retry_database, job_id)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("max_attempts", [1, 3, 20])
async def test_retryable_failures_exhaust_exact_budget_and_public_history_is_ordered(
    retry_database: Database, max_attempts: int
) -> None:
    job_id = await submit(retry_database, fail_until_attempt=20, max_attempts=max_attempts)
    for attempt in range(1, max_attempts + 1):
        assert await worker.run_once(retry_database, settings_for(retry_database))
        job = await read_job(retry_database, job_id)
        assert job.attempt_count == attempt
        if attempt < max_attempts:
            assert job.status is JobStatus.RETRY_WAIT
            assert job.next_attempt_at is not None
            assert not await worker.run_once(retry_database, settings_for(retry_database))
            await mark_due(retry_database, job_id)
        else:
            assert job.status is JobStatus.DEAD_LETTERED
            assert job.dead_letter_reason == "attempts_exhausted"
            assert job.dead_lettered_at == job.finished_at
            assert job.finished_at is not None
            assert job.next_attempt_at is None
            assert job.error_info == FAILURE
    assert not await worker.run_once(retry_database, settings_for(retry_database))
    assert len(await outbox(retry_database, job_id)) == max_attempts
    app = create_app(settings_for(retry_database))
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/jobs/{job_id}/attempts")
    assert response.status_code == 200
    attempts = response.json()
    assert isinstance(attempts, list)
    assert len(attempts) == max_attempts <= 20
    assert [attempt["attempt_number"] for attempt in attempts] == list(range(1, max_attempts + 1))
    assert all(attempt["status"] == "FAILED" for attempt in attempts)
    assert all(attempt["error_info"] == FAILURE for attempt in attempts)
    assert all(
        "claim_token" not in attempt and "lease_expires_at" not in attempt for attempt in attempts
    )
    assert attempts[-1]["next_attempt_at"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_type", "payload", "code"),
    [
        ("unimplemented", {}, "unknown_task"),
        ("calculation", {"numbers": [True]}, "invalid_payload"),
    ],
)
async def test_permanent_task_errors_finish_after_one_attempt_despite_remaining_budget(
    retry_database: Database, job_type: str, payload: dict[str, object], code: str
) -> None:
    async with retry_database.session_factory() as session:
        job = await create_job(session, JobCreate(type=job_type, payload=payload, max_attempts=20))
        job_id = job.id
    assert await worker.run_once(retry_database, settings_for(retry_database))
    failed = await read_job(retry_database, job_id)
    assert failed.status is JobStatus.FAILED
    assert failed.dead_lettered_at is None and failed.dead_letter_reason is None
    assert failed.attempt_count == 1
    assert failed.error_info == {"code": code, "retryable": False}
    assert failed.next_attempt_at is None
    assert not await worker.run_once(retry_database, settings_for(retry_database))
    attempts = await history(retry_database, job_id)
    assert len(attempts) == len(await outbox(retry_database, job_id)) == 1
    assert attempts[0].status is AttemptStatus.FAILED
    assert attempts[0].error_info == failed.error_info
    assert attempts[0].next_attempt_at is None


@pytest.mark.asyncio
async def test_old_publication_completion_cannot_queue_or_accelerate_waiting_retry(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    publication = await claim_dispatch(retry_database, lease_seconds=60)
    assert publication is not None
    assert await worker.run_once(retry_database, settings_for(retry_database))
    waiting = await read_job(retry_database, job_id)
    assert waiting.status is JobStatus.RETRY_WAIT
    assert await complete_dispatch(retry_database, publication)
    unchanged = await read_job(retry_database, job_id)
    assert unchanged.status is JobStatus.RETRY_WAIT
    assert unchanged.next_attempt_at == waiting.next_attempt_at
    assert await claim_dispatch(retry_database, lease_seconds=60) is None
    assert await claim_job(retry_database, job_id, lease_seconds=60) is None
    await mark_due(retry_database, job_id)
    retry_publication = await claim_dispatch(retry_database, lease_seconds=60)
    assert retry_publication is not None and retry_publication.id != publication.id
    assert retry_publication.attempt_count == 1
    assert await complete_dispatch(retry_database, retry_publication)
    assert (await read_job(retry_database, job_id)).status is JobStatus.RETRY_WAIT
    assert await worker.run_once(retry_database, settings_for(retry_database))
    assert (await read_job(retry_database, job_id)).status is JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_eight_due_polling_and_delivery_contenders_across_engines_create_one_attempt(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    assert await worker.run_once(retry_database, settings_for(retry_database))
    await mark_due(retry_database, job_id)
    other = Database(settings_for(retry_database))
    barrier = asyncio.Barrier(8)

    async def contend(index: int) -> Claim | None:
        await barrier.wait()
        database = retry_database if index % 2 else other
        if index % 3:
            return await claim_job(database, job_id, lease_seconds=60)
        return await claim_pending(database, lease_seconds=60)

    try:
        async with asyncio.timeout(10):
            claims = await asyncio.gather(*(contend(index) for index in range(8)))
        owners = [claim for claim in claims if claim is not None]
        assert len(owners) == 1
        assert owners[0].attempt_count == 2
        attempts = await history(retry_database, job_id)
        assert [attempt.status for attempt in attempts] == [
            AttemptStatus.FAILED,
            AttemptStatus.RUNNING,
        ]
        assert attempts[1].claim_token == owners[0].token
        assert await worker.execute_claim(other, owners[0], settings_for(other))
    finally:
        await other.dispose()
    assert (await read_job(retry_database, job_id)).attempt_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("recover", [False, True])
async def test_failed_retry_outbox_insert_rolls_back_job_and_attempt_outcome(
    retry_database: Database, recover: bool
) -> None:
    job_id = await submit(retry_database)
    claim = await claim_job(retry_database, job_id, lease_seconds=60)
    assert claim is not None
    if recover:
        await expire(retry_database, job_id)
    async with retry_database.session_factory.begin() as session:
        await session.execute(
            text(
                "ALTER TABLE dispatch_outbox ADD CONSTRAINT test_reject_retry "
                "CHECK (false) NOT VALID"
            )
        )
    with pytest.raises(IntegrityError):
        if recover:
            await recover_expired(retry_database, job_id=job_id, retry_policy=LONG_RETRY)
        else:
            await complete_claim(retry_database, claim, error=FAILURE, retry_policy=LONG_RETRY)
    job = await read_job(retry_database, job_id)
    assert job.status is JobStatus.RUNNING
    assert job.claim_token == claim.token
    assert job.error_info is None
    assert job.next_attempt_at is None
    attempts = await history(retry_database, job_id)
    assert len(attempts) == len(await outbox(retry_database, job_id)) == 1
    assert attempts[0].status is AttemptStatus.RUNNING
    assert attempts[0].finished_at is None and attempts[0].next_attempt_at is None
    async with retry_database.session_factory.begin() as session:
        await session.execute(text("ALTER TABLE dispatch_outbox DROP CONSTRAINT test_reject_retry"))
    if recover:
        assert await recover_expired(retry_database, job_id=job_id, retry_policy=LONG_RETRY) == 1
    else:
        assert await complete_claim(retry_database, claim, error=FAILURE, retry_policy=LONG_RETRY)
    assert (await read_job(retry_database, job_id)).status is JobStatus.RETRY_WAIT
    assert (await history(retry_database, job_id))[0].status is AttemptStatus.FAILED
    assert len(await outbox(retry_database, job_id)) == 2


@pytest.mark.asyncio
async def test_failed_attempt_insert_rolls_back_claim_and_preserves_attempt_budget(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    async with retry_database.session_factory.begin() as session:
        await session.execute(
            text("ALTER TABLE job_attempts ADD CONSTRAINT test_reject_claim CHECK (false)")
        )
    with pytest.raises(IntegrityError):
        await claim_job(retry_database, job_id, lease_seconds=60)
    pending = await read_job(retry_database, job_id)
    assert pending.status is JobStatus.PENDING
    assert pending.attempt_count == 0 and pending.claim_token is None
    assert await history(retry_database, job_id) == []
    async with retry_database.session_factory.begin() as session:
        await session.execute(text("ALTER TABLE job_attempts DROP CONSTRAINT test_reject_claim"))
    claim = await claim_job(retry_database, job_id, lease_seconds=60)
    assert claim is not None and claim.attempt_count == 1


@pytest.mark.asyncio
async def test_expired_completion_and_two_recoverers_finalize_once_and_fence_new_attempt(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    first = await claim_job(retry_database, job_id, lease_seconds=60)
    assert first is not None
    await expire(retry_database, job_id)
    barrier = asyncio.Barrier(3)

    async def complete() -> bool:
        await barrier.wait()
        return await complete_claim(retry_database, first, result={"attempt": 999})

    async def recover() -> int:
        await barrier.wait()
        return await recover_expired(retry_database, job_id=job_id, retry_policy=LONG_RETRY)

    async with asyncio.timeout(10):
        completion, first_recovery, second_recovery = await asyncio.gather(
            complete(), recover(), recover()
        )
    assert completion is False
    assert first_recovery + second_recovery == 1
    waiting = await read_job(retry_database, job_id)
    assert waiting.status is JobStatus.RETRY_WAIT
    assert waiting.error_info == {"code": "worker_lost", "retryable": True}
    attempts = await history(retry_database, job_id)
    assert len(attempts) == 1 and attempts[0].status is AttemptStatus.FAILED
    assert attempts[0].next_attempt_at == waiting.next_attempt_at
    assert len(await outbox(retry_database, job_id)) == 2
    await mark_due(retry_database, job_id)
    second = await claim_job(retry_database, job_id, lease_seconds=60)
    assert second is not None and second.token != first.token
    assert not await complete_claim(retry_database, first, error=FAILURE)
    assert await recover_expired(retry_database, job_id=job_id, retry_policy=LONG_RETRY) == 0
    assert await complete_claim(retry_database, second, result={"attempt": 2})
    assert (await read_job(retry_database, job_id)).result_metadata == {"attempt": 2}


@pytest.mark.asyncio
async def test_push_acknowledges_waiting_duplicates_after_restart_and_only_executes_when_due(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    transport = PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None)
    app = create_push_app(settings_for(retry_database), transport)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://first") as client:
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
            waiting = await read_job(retry_database, job_id)
            assert waiting.status is JobStatus.RETRY_WAIT and waiting.attempt_count == 1
    restarted = create_push_app(settings_for(retry_database), transport)
    async with restarted.router.lifespan_context(restarted):
        async with AsyncClient(
            transport=ASGITransport(app=restarted), base_url="http://restart"
        ) as client:
            for _ in range(3):
                assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
            unchanged = await read_job(retry_database, job_id)
            assert unchanged.next_attempt_at == waiting.next_attempt_at
            assert unchanged.attempt_count == 1
            assert len(await history(retry_database, job_id)) == 1
            assert len(await outbox(retry_database, job_id)) == 2
            await mark_due(retry_database, job_id)
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    completed = await read_job(retry_database, job_id)
    assert completed.status is JobStatus.SUCCEEDED
    assert completed.attempt_count == 2 and completed.result_metadata == {"attempt": 2}


@pytest.mark.asyncio
async def test_keyed_replay_during_wait_and_after_restart_retains_submission_and_retry_schedule(
    retry_database: Database,
) -> None:
    body = {"type": "controlled_failure", "payload": {"fail_until_attempt": 1}}
    headers = {"Idempotency-Key": "retry-submission"}
    app = create_app(settings_for(retry_database))
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://first") as client:
            created = await client.post("/jobs", json=body, headers=headers)
            assert created.status_code == 201
            job_id = UUID(created.json()["id"])
            assert (await client.get(f"/jobs/{job_id}/attempts")).json() == []
            assert (await client.get(f"/jobs/{uuid4()}/attempts")).status_code == 404
            assert await worker.run_once(retry_database, settings_for(retry_database))
            replay = await client.post("/jobs", json=body, headers=headers)
            assert replay.status_code == 200 and replay.json()["status"] == "RETRY_WAIT"
            scheduled = replay.json()["next_attempt_at"]
            assert scheduled is not None
    restarted = create_app(settings_for(retry_database))
    async with restarted.router.lifespan_context(restarted):
        async with AsyncClient(
            transport=ASGITransport(app=restarted), base_url="http://restart"
        ) as client:
            replay = await client.post("/jobs", json=body, headers=headers)
            assert replay.status_code == 200 and replay.json()["id"] == str(job_id)
            assert replay.json()["next_attempt_at"] == scheduled
            assert not await worker.run_once(retry_database, settings_for(retry_database))
            await mark_due(retry_database, job_id)
            assert await worker.run_once(retry_database, settings_for(retry_database))
            terminal = await client.post("/jobs", json=body, headers=headers)
            assert terminal.status_code == 200
            assert terminal.json()["status"] == "SUCCEEDED"
            assert terminal.json()["attempt_count"] == 2
            assert terminal.json()["next_attempt_at"] is None
    async with retry_database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
    assert len(await outbox(retry_database, job_id)) == 2


@pytest.mark.asyncio
async def test_due_retry_locked_by_another_transaction_nacks_then_executes_on_redelivery(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    assert await worker.run_once(retry_database, settings_for(retry_database))
    await mark_due(retry_database, job_id)
    app = create_push_app(
        settings_for(retry_database),
        PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None),
    )
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            async with retry_database.session_factory.begin() as holder:
                await holder.execute(
                    text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE"),
                    {"job_id": job_id},
                )
                async with asyncio.timeout(5):
                    response = await client.post("/pubsub/push", json=envelope(job_id))
                assert response.status_code == 503
                assert response.json() == {"detail": "Job is not available for execution"}
                assert (await read_job(retry_database, job_id)).attempt_count == 1
                assert len(await history(retry_database, job_id)) == 1
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    completed = await read_job(retry_database, job_id)
    assert completed.status is JobStatus.SUCCEEDED
    assert completed.attempt_count == 2


@pytest.mark.asyncio
async def test_retry_outbox_insert_blocked_past_lease_rolls_back_outcome_and_history(
    retry_database: Database,
) -> None:
    job_id = await submit(retry_database)
    claim = await claim_job(retry_database, job_id, lease_seconds=2)
    assert claim is not None
    application_name = f"retry_completion_{uuid4().hex}"
    url = retry_database.engine.url.update_query_dict(
        {"application_name": application_name}
    ).render_as_string(hide_password=False)
    waiter = Database(settings_for_url(url))
    completion: asyncio.Task[bool] | None = None
    try:
        async with retry_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            await holder.execute(text("LOCK TABLE dispatch_outbox IN ACCESS EXCLUSIVE MODE"))
            completion = asyncio.create_task(
                complete_claim(waiter, claim, error=FAILURE, retry_policy=LONG_RETRY)
            )
            async with asyncio.timeout(5):
                while True:
                    async with retry_database.session_factory() as observer:
                        blocked = await observer.scalar(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                "WHERE application_name = :application_name "
                                "AND :holder_pid = ANY(pg_blocking_pids(pid)) "
                                "AND query LIKE '%INSERT INTO dispatch_outbox%')"
                            ),
                            {"application_name": application_name, "holder_pid": holder_pid},
                        )
                    if blocked:
                        break
            assert not completion.done()
            assert await holder.scalar(
                text("SELECT lease_expires_at > clock_timestamp() FROM jobs WHERE id = :job_id"),
                {"job_id": job_id},
            )
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
        original = await read_job(retry_database, job_id)
        assert original.status is JobStatus.RUNNING
        assert original.claim_token == claim.token
        assert original.error_info is None and original.next_attempt_at is None
        attempts = await history(retry_database, job_id)
        assert len(attempts) == 1 and attempts[0].status is AttemptStatus.RUNNING
        assert attempts[0].finished_at is None and attempts[0].next_attempt_at is None
        assert len(await outbox(retry_database, job_id)) == 1
    finally:
        if completion is not None and not completion.done():
            completion.cancel()
            await asyncio.gather(completion, return_exceptions=True)
        await waiter.dispose()
