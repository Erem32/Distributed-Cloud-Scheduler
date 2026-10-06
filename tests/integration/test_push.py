"""Pub/Sub HTTP acknowledgements follow authoritative PostgreSQL ownership."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from pydantic import JsonValue
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_job, complete_claim, recover_expired
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobRead, JobStatus, create_job
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app

SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"


@pytest_asyncio.fixture
async def push_database(integration_database_url: str) -> AsyncIterator[Database]:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    database = Database(Settings(database_url=integration_database_url, _env_file=None))
    try:
        yield database
    finally:
        await database.dispose()


def settings_for(database: Database, **overrides: object) -> Settings:
    return Settings(
        database_url=database.engine.url.render_as_string(hide_password=False),
        worker_concurrency=overrides.pop("worker_concurrency", 3),
        execution_retry_base_seconds=overrides.pop("execution_retry_base_seconds", 60),
        execution_retry_max_seconds=overrides.pop("execution_retry_max_seconds", 60),
        _env_file=None,
        **overrides,
    )


def transport_settings() -> PubSubSettings:
    return PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None)


@pytest_asyncio.fixture
async def push_client(push_database: Database) -> AsyncIterator[AsyncClient]:
    app = create_push_app(settings_for(push_database), transport_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


def envelope(job_id: UUID) -> dict[str, object]:
    return {
        "message": {
            "messageId": uuid4().hex,
            "data": base64.b64encode(str(job_id).encode("ascii")).decode("ascii"),
            "attributes": {"type": "controlled_failure", "attempt_count": "20"},
        },
        "subscription": SUBSCRIPTION,
        "deliveryAttempt": 99,
    }


async def submit(
    database: Database,
    job_type: str = "calculation",
    payload: dict[str, JsonValue] | None = None,
    *,
    max_attempts: int = 3,
) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(
                type=job_type,
                payload=payload if payload is not None else {"numbers": [1, 2, 3]},
                max_attempts=max_attempts,
            ),
        )
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def expire(database: Database, job_id: UUID) -> None:
    async with database.session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE jobs SET started_at = clock_timestamp() - interval '2 seconds', "
                "lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = :job_id"
            ),
            {"job_id": job_id},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_status", [JobStatus.PENDING, JobStatus.QUEUED])
async def test_push_executes_target_from_database_and_acknowledges_after_commit(
    push_database: Database, push_client: AsyncClient, initial_status: JobStatus
) -> None:
    older_id = await submit(push_database)
    target_id = await submit(push_database)
    if initial_status is JobStatus.QUEUED:
        async with push_database.session_factory.begin() as session:
            await session.execute(
                text("UPDATE jobs SET status = 'QUEUED' WHERE id = :job_id"), {"job_id": target_id}
            )
    response = await push_client.post("/pubsub/push", json=envelope(target_id))
    assert response.status_code == 204
    job = await read_job(push_database, target_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.result_metadata == {"sum": 6}
    assert job.attempt_count == 1
    assert job.finished_at is not None
    assert job.claim_token is None
    assert (await read_job(push_database, older_id)).status is JobStatus.PENDING
    assert (await push_client.post("/pubsub/push", json=envelope(target_id))).status_code == 204
    assert (await read_job(push_database, target_id)).attempt_count == 1


@pytest.mark.asyncio
async def test_api_submission_and_separate_push_app_share_authoritative_state(
    push_database: Database, push_client: AsyncClient
) -> None:
    app = create_app(settings_for(push_database))
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            submission = await client.post(
                "/jobs", json={"type": "calculation", "payload": {"numbers": [2, 3]}}
            )
            assert submission.status_code == 201
            job_id = UUID(submission.json()["id"])
            assert (
                await push_client.post("/pubsub/push", json=envelope(job_id))
            ).status_code == 204
            persisted = (await client.get(f"/jobs/{job_id}")).json()
            assert persisted["status"] == "SUCCEEDED"
            assert persisted["result_metadata"] == {"sum": 5}


@pytest.mark.asyncio
async def test_exhausted_task_failure_acknowledges_without_execution_retry(
    push_database: Database, push_client: AsyncClient
) -> None:
    job_id = await submit(
        push_database, "controlled_failure", {"fail_until_attempt": 3}, max_attempts=1
    )
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    failed = await read_job(push_database, job_id)
    assert failed.status is JobStatus.FAILED
    assert failed.error_info == {"code": "controlled_failure", "retryable": True}
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert (await read_job(push_database, job_id)).attempt_count == 1


@pytest.mark.asyncio
async def test_unknown_id_nacks_without_claiming_unrelated_job(
    push_database: Database, push_client: AsyncClient
) -> None:
    job_id = await submit(push_database)
    response = await push_client.post("/pubsub/push", json=envelope(uuid4()))
    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}
    job = await read_job(push_database, job_id)
    assert job.status is JobStatus.PENDING
    assert job.attempt_count == 0


@pytest.mark.asyncio
async def test_duplicate_while_running_nacks_and_never_executes_twice(
    push_database: Database, push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    executions = 0

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal executions
        executions += 1
        started.set()
        await release.wait()
        return {"sum": 6}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit(push_database)
    owner = asyncio.create_task(push_client.post("/pubsub/push", json=envelope(job_id)))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            duplicate = await push_client.post("/pubsub/push", json=envelope(job_id))
        assert duplicate.status_code == 503
        assert duplicate.json() == {"detail": "Job is not available for execution"}
        assert (await read_job(push_database, job_id)).status is JobStatus.RUNNING
    finally:
        release.set()
        response = await owner
    assert response.status_code == 204
    assert executions == 1
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert (await read_job(push_database, job_id)).attempt_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_type", "payload", "terminal_status"),
    [
        ("calculation", {"numbers": [1, 2, 3]}, JobStatus.SUCCEEDED),
        ("controlled_failure", {"fail_until_attempt": 3}, JobStatus.FAILED),
    ],
)
async def test_duplicate_across_independent_push_engines_and_restart_preserves_one_outcome(
    push_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    job_type: str,
    payload: dict[str, JsonValue],
    terminal_status: JobStatus,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    executions = 0
    execute_task = worker.execute_task

    async def held_handler(
        task_type: str, task_payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal executions
        executions += 1
        started.set()
        await release.wait()
        return await execute_task(task_type, task_payload, attempt)

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit(push_database, job_type, payload, max_attempts=1)
    owner_app = create_push_app(
        settings_for(push_database, worker_concurrency=1), transport_settings()
    )
    other_app = create_push_app(
        settings_for(push_database, worker_concurrency=1), transport_settings()
    )
    async with AsyncExitStack() as resources:
        await resources.enter_async_context(owner_app.router.lifespan_context(owner_app))
        await resources.enter_async_context(other_app.router.lifespan_context(other_app))
        assert owner_app.state.database.engine is not other_app.state.database.engine
        owner_client = await resources.enter_async_context(
            AsyncClient(transport=ASGITransport(app=owner_app), base_url="http://owner")
        )
        other_client = await resources.enter_async_context(
            AsyncClient(transport=ASGITransport(app=other_app), base_url="http://other")
        )
        owner = asyncio.create_task(owner_client.post("/pubsub/push", json=envelope(job_id)))
        try:
            async with asyncio.timeout(5):
                await started.wait()
                duplicate = await other_client.post("/pubsub/push", json=envelope(job_id))
            assert duplicate.status_code == 503
            assert duplicate.json() == {"detail": "Job is not available for execution"}
            running = await read_job(push_database, job_id)
            assert running.status is JobStatus.RUNNING
            assert running.attempt_count == 1
            assert executions == 1
        finally:
            release.set()
            async with asyncio.timeout(5):
                completed = await owner
        assert completed.status_code == 204
        terminal = await read_job(push_database, job_id)
        assert terminal.status is terminal_status
        terminal_snapshot = JobRead.model_validate(terminal).model_dump()
        assert (await other_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    restarted = create_push_app(settings_for(push_database), transport_settings())
    async with restarted.router.lifespan_context(restarted):
        async with AsyncClient(
            transport=ASGITransport(app=restarted), base_url="http://restart"
        ) as client:
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert executions == 1
    assert (
        JobRead.model_validate(await read_job(push_database, job_id)).model_dump()
        == terminal_snapshot
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("polling_owner", [False, True])
async def test_polling_and_push_share_ownership_and_do_not_add_duplicate_attempts(
    push_database: Database,
    push_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    polling_owner: bool,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    executions = 0

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal executions
        executions += 1
        started.set()
        await release.wait()
        return {"sum": 6}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit(push_database)
    if polling_owner:
        owner = asyncio.create_task(worker.run_once(push_database, settings_for(push_database)))
    else:
        owner = asyncio.create_task(push_client.post("/pubsub/push", json=envelope(job_id)))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            if polling_owner:
                duplicate = await push_client.post("/pubsub/push", json=envelope(job_id))
                assert duplicate.status_code == 503
                assert duplicate.json() == {"detail": "Job is not available for execution"}
            else:
                assert not await worker.run_once(push_database, settings_for(push_database))
        running = await read_job(push_database, job_id)
        assert running.status is JobStatus.RUNNING
        assert running.attempt_count == 1
        assert executions == 1
    finally:
        release.set()
        async with asyncio.timeout(5):
            completed = await owner
    if polling_owner:
        assert completed is True
    else:
        assert completed.status_code == 204
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    terminal = await read_job(push_database, job_id)
    assert terminal.status is JobStatus.SUCCEEDED
    assert terminal.attempt_count == 1
    assert terminal.result_metadata == {"sum": 6}
    assert executions == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 3])
async def test_capacity_is_immediate_bounded_and_reusable(
    push_database: Database, monkeypatch: pytest.MonkeyPatch, concurrency: int
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum = 0

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        if active == concurrency:
            started.set()
        try:
            await release.wait()
            return {"sum": 6}
        finally:
            active -= 1

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_ids = [await submit(push_database) for _ in range(concurrency + 1)]
    app = create_push_app(
        settings_for(push_database, worker_concurrency=concurrency), transport_settings()
    )
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            requests = [
                asyncio.create_task(client.post("/pubsub/push", json=envelope(job_id)))
                for job_id in job_ids[:-1]
            ]
            try:
                async with asyncio.timeout(5):
                    await started.wait()
                    overflow = await client.post("/pubsub/push", json=envelope(job_ids[-1]))
                assert overflow.status_code == 503
                assert overflow.json() == {"detail": "Worker capacity exhausted"}
                assert (await read_job(push_database, job_ids[-1])).attempt_count == 0
            finally:
                release.set()
                responses = await asyncio.gather(*requests)
            assert all(response.status_code == 204 for response in responses)
            assert maximum == concurrency
            assert (
                await client.post("/pubsub/push", json=envelope(job_ids[-1]))
            ).status_code == 204


@pytest.mark.asyncio
async def test_locked_eligible_row_nacks_then_redelivery_executes(
    push_database: Database, push_client: AsyncClient
) -> None:
    job_id = await submit(push_database)
    async with push_database.session_factory.begin() as holder:
        await holder.execute(
            text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE"), {"job_id": job_id}
        )
        async with asyncio.timeout(5):
            response = await push_client.post("/pubsub/push", json=envelope(job_id))
        assert response.status_code == 503
        assert (await read_job(push_database, job_id)).attempt_count == 0
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert (await read_job(push_database, job_id)).attempt_count == 1


@pytest.mark.asyncio
async def test_target_expired_recovery_preserves_other_owners_and_fences_stale_result(
    push_database: Database, push_client: AsyncClient
) -> None:
    first_id, other_id = await submit(push_database), await submit(push_database)
    first = await claim_job(push_database, first_id, lease_seconds=60)
    other = await claim_job(push_database, other_id, lease_seconds=60)
    assert first is not None and other is not None
    await expire(push_database, first_id)
    await expire(push_database, other_id)
    assert (await push_client.post("/pubsub/push", json=envelope(first_id))).status_code == 204
    failed = await read_job(push_database, first_id)
    assert failed.status is JobStatus.RETRY_WAIT
    assert failed.next_attempt_at is not None
    assert failed.error_info == {"code": "worker_lost", "retryable": True}
    assert failed.attempt_count == 1
    assert (await read_job(push_database, other_id)).status is JobStatus.RUNNING
    assert not await complete_claim(push_database, first, result={"sum": 999})
    assert await recover_expired(push_database) == 1


@pytest.mark.asyncio
async def test_fenced_completion_nacks_without_overwriting_recovered_outcome(
    push_database: Database, push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = await submit(push_database)

    async def obsolete_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        await expire(push_database, job_id)
        assert await recover_expired(push_database, job_id=job_id) == 1
        return {"sum": 999}

    monkeypatch.setattr(worker, "execute_task", obsolete_handler)
    response = await push_client.post("/pubsub/push", json=envelope(job_id))
    assert response.status_code == 503
    assert response.json() == {"detail": "Execution ownership lost"}
    failed = await read_job(push_database, job_id)
    assert failed.status is JobStatus.RETRY_WAIT
    assert failed.next_attempt_at is not None
    assert failed.result_metadata is None
    assert failed.error_info == {"code": "worker_lost", "retryable": True}
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204


@pytest.mark.asyncio
async def test_completion_database_failure_keeps_delivery_outstanding(
    push_database: Database, push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = await submit(push_database)

    async def unavailable(*args: object, **kwargs: object) -> bool:
        raise OperationalError("secret_marker", {}, Exception("secret_marker"))

    monkeypatch.setattr(worker, "complete_claim", unavailable)
    response = await push_client.post("/pubsub/push", json=envelope(job_id))
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert "secret_marker" not in response.text
    pending_outcome = await read_job(push_database, job_id)
    assert pending_outcome.status is JobStatus.RUNNING
    assert pending_outcome.result_metadata is None
    assert pending_outcome.attempt_count == 1
    assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 503
    assert (await read_job(push_database, job_id)).attempt_count == 1


@pytest.mark.asyncio
async def test_request_cancellation_persists_failure_and_releases_admission(
    push_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()
    execute_task = worker.execute_task

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        await asyncio.Event().wait()
        return {}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit(push_database)
    app = create_push_app(settings_for(push_database, worker_concurrency=1), transport_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            running = asyncio.create_task(client.post("/pubsub/push", json=envelope(job_id)))
            async with asyncio.timeout(5):
                await started.wait()
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
            cancelled = await read_job(push_database, job_id)
            assert cancelled.status is JobStatus.RETRY_WAIT
            assert cancelled.next_attempt_at is not None
            assert cancelled.error_info == {"code": "cancelled", "retryable": True}
            monkeypatch.setattr(worker, "execute_task", execute_task)
            next_id = await submit(push_database)
            assert (await client.post("/pubsub/push", json=envelope(next_id))).status_code == 204


@pytest.mark.asyncio
async def test_total_request_timeout_cancels_execution_before_acknowledgement(
    push_database: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        await asyncio.Event().wait()
        return {}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit(push_database)
    transport = transport_settings()
    app = create_push_app(settings_for(push_database), transport)
    transport.push_request_timeout_seconds = 0.2
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/pubsub/push", json=envelope(job_id))
            assert started.is_set()
            assert response.status_code == 503
            assert response.json() == {"detail": "Push request timed out"}
            cancelled = await read_job(push_database, job_id)
            assert cancelled.status is JobStatus.RETRY_WAIT
            assert cancelled.next_attempt_at is not None
            assert cancelled.error_info == {"code": "cancelled", "retryable": True}
            transport.push_request_timeout_seconds = 45
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
