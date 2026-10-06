import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from pydantic import JsonValue
from sqlalchemy import text
from sqlalchemy.engine import make_url

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_pending, recover_expired
from distributed_cloud_scheduler.jobs import JobCreate, create_job, get_job


@pytest.fixture
def worker_settings(integration_database_url: str) -> Settings:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    url = make_url(integration_database_url).update_query_dict(
        {"application_name": f"worker_test_{uuid4().hex}"}
    )
    return Settings(
        database_url=url.render_as_string(hide_password=False),
        execution_retry_base_seconds=60,
        execution_retry_max_seconds=60,
        _env_file=None,
    )


@pytest_asyncio.fixture
async def worker_database(worker_settings: Settings) -> AsyncIterator[Database]:
    database = Database(worker_settings)
    try:
        yield database
    finally:
        await database.dispose()


async def submit_job(database: Database, job_type: str, payload: dict[str, JsonValue]) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(session, JobCreate(type=job_type, payload=payload))
        return job.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_type", "payload", "result", "error"),
    [
        ("calculation", {"numbers": [1, 2.5]}, {"sum": 3.5}, None),
        ("sleep", {"seconds": 0}, {"slept_seconds": 0}, None),
        ("controlled_failure", {"fail_until_attempt": 0}, {"attempt": 1}, None),
        (
            "controlled_failure",
            {"fail_until_attempt": 1},
            None,
            {"code": "controlled_failure", "retryable": True},
        ),
        ("unimplemented", {}, None, {"code": "unknown_task", "retryable": False}),
        ("calculation", {"numbers": [True]}, None, {"code": "invalid_payload", "retryable": False}),
    ],
)
async def test_api_submission_local_execution_and_result_inspection(
    worker_database: Database,
    worker_settings: Settings,
    job_type: str,
    payload: dict[str, JsonValue],
    result: dict[str, JsonValue] | None,
    error: dict[str, JsonValue] | None,
) -> None:
    app = create_app(worker_settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/jobs", json={"type": job_type, "payload": payload, "max_attempts": 1}
            )
            assert response.status_code == 201
            job_id = response.json()["id"]
            assert response.json()["status"] == "PENDING"

            assert await worker.run_once(worker_database, worker_settings) is True
            job = (await client.get(f"/jobs/{job_id}")).json()
            expected_status = (
                "SUCCEEDED"
                if error is None
                else "DEAD_LETTERED"
                if error["retryable"]
                else "FAILED"
            )
            assert job["status"] == expected_status
            assert job["attempt_count"] == 1
            assert job["result_metadata"] == result
            assert job["error_info"] == error
            assert job["started_at"] is not None
            assert job["finished_at"] is not None
            assert "claim_token" not in job
            assert "lease_expires_at" not in job
            # A one-attempt budget keeps every failure terminal.
            assert await worker.run_once(worker_database, worker_settings) is False


@pytest.mark.asyncio
async def test_task_timeout_is_persisted_with_a_durable_retry(
    worker_database: Database, worker_settings: Settings
) -> None:
    job_id = await submit_job(worker_database, "sleep", {"seconds": 1})
    settings = worker_settings.model_copy(update={"worker_task_timeout_seconds": 0.03})
    assert await worker.run_once(worker_database, settings) is True
    async with worker_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "RETRY_WAIT"
        assert job.next_attempt_at is not None
        assert job.error_info == {"code": "task_timeout", "retryable": True}
        assert job.result_metadata is None
    assert await worker.run_once(worker_database, settings) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("exception_type", [RuntimeError, TimeoutError])
async def test_unexpected_handler_exception_is_classified_without_exposing_text(
    worker_database: Database,
    worker_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    exception_type: type[Exception],
) -> None:
    async def broken_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        raise exception_type("secret_payload_marker")

    monkeypatch.setattr(worker, "execute_task", broken_handler)
    job_id = await submit_job(worker_database, "calculation", {"numbers": [1]})
    assert await worker.run_once(worker_database, worker_settings) is True
    async with worker_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "RETRY_WAIT"
        assert job.next_attempt_at is not None
        assert job.error_info == {"code": "internal_error", "retryable": True}
        assert "secret_payload_marker" not in str(job.error_info)


@pytest.mark.asyncio
async def test_task_await_holds_no_database_transaction_or_row_lock(
    worker_database: Database,
    worker_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        await release.wait()
        return {"sum": 1}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit_job(worker_database, "calculation", {"numbers": [1]})
    running = asyncio.create_task(worker.run_once(worker_database, worker_settings))
    try:
        async with asyncio.timeout(3):
            await started.wait()
        async with worker_database.session_factory.begin() as session:
            locked_id = await session.scalar(
                text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE NOWAIT"), {"job_id": job_id}
            )
            assert locked_id == job_id
            application_name = make_url(worker_settings.database_url.get_secret_value()).query[
                "application_name"
            ]
            idle_transactions = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = :name "
                    "AND state = 'idle in transaction'"
                ),
                {"name": application_name},
            )
            assert idle_transactions == 0
    finally:
        release.set()
        outcome = await running
    assert outcome is True


@pytest.mark.asyncio
async def test_stale_execution_cannot_overwrite_recovered_owner(
    worker_database: Database,
    worker_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        await release.wait()
        return {"sum": 999}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit_job(worker_database, "calculation", {"numbers": [1]})
    claim = await claim_pending(worker_database, lease_seconds=worker_settings.worker_lease_seconds)
    assert claim is not None
    running = asyncio.create_task(worker.execute_claim(worker_database, claim, worker_settings))
    try:
        async with asyncio.timeout(3):
            await started.wait()
        async with worker_database.session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE jobs SET created_at = clock_timestamp() - interval '3 seconds', "
                    "started_at = clock_timestamp() - interval '2 seconds', "
                    "lease_expires_at = clock_timestamp() - interval '1 second' "
                    "WHERE id = :job_id"
                ),
                {"job_id": job_id},
            )
        assert await recover_expired(worker_database) == 1
    finally:
        release.set()
        outcome = await running
    assert outcome is False
    async with worker_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "RETRY_WAIT"
        assert job.next_attempt_at is not None
        assert job.error_info == {"code": "worker_lost", "retryable": True}
        assert job.result_metadata is None


@pytest.mark.asyncio
async def test_cancellation_schedules_matching_owner_for_retry_and_propagates(
    worker_database: Database,
    worker_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()

    async def held_handler(
        job_type: str, payload: dict[str, JsonValue], attempt: int
    ) -> dict[str, JsonValue]:
        started.set()
        await asyncio.Event().wait()
        return {}

    monkeypatch.setattr(worker, "execute_task", held_handler)
    job_id = await submit_job(worker_database, "sleep", {"seconds": 1})
    running = asyncio.create_task(worker.run_once(worker_database, worker_settings))
    async with asyncio.timeout(3):
        await started.wait()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    async with worker_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "RETRY_WAIT"
        assert job.next_attempt_at is not None
        assert job.error_info == {"code": "cancelled", "retryable": True}
        assert job.result_metadata is None
