"""Durable delayed admission and its races use actual PostgreSQL locks/transactions."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.attempts import JobAttempt
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import (
    claim_job,
    claim_pending,
    complete_claim,
)
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobRead, JobStatus, create_job
from distributed_cloud_scheduler.outbox import (
    DispatchOutbox,
    DispatchStatus,
    claim_dispatch,
    complete_dispatch,
)
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app
from distributed_cloud_scheduler.retries import RetryPolicy
from distributed_cloud_scheduler.scheduler import schedule_due

SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"
FAILURE = {"code": "controlled_failure", "retryable": True}
LEGACY_DIGEST = "5dfcf9a1ceff59c1889e385e9eee08c5013f2936a9a97a64a3e83a59b956c75a"


def settings_for_url(url: str, application_name: str | None = None) -> Settings:
    if application_name is not None:
        url = (
            make_url(url)
            .update_query_dict({"application_name": application_name})
            .render_as_string(hide_password=False)
        )
    return Settings(database_url=url, _env_file=None)


def settings_for(database: Database, application_name: str | None = None) -> Settings:
    return settings_for_url(
        database.engine.url.render_as_string(hide_password=False), application_name
    )


@pytest_asyncio.fixture
async def scheduled_database(integration_database_url: str) -> AsyncIterator[Database]:
    command.upgrade(Config(str(Path(__file__).resolve().parents[2] / "alembic.ini")), "head")
    database = Database(settings_for_url(integration_database_url))
    try:
        yield database
    finally:
        await database.dispose()


@asynccontextmanager
async def client_for(database: Database, *, push: bool = False) -> AsyncIterator[AsyncClient]:
    app = (
        create_push_app(
            settings_for(database), PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None)
        )
        if push
        else create_app(settings_for(database))
    )
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


async def database_now(database: Database) -> datetime:
    async with database.session_factory() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        return now


async def submit(database: Database, scheduled_at: datetime | None, *, budget: int = 3) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(
                type="calculation",
                payload={"numbers": [1, 2, 3]},
                scheduled_at=scheduled_at,
                max_attempts=budget,
            ),
        )
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def obligations(database: Database, job_id: UUID | None = None) -> list[DispatchOutbox]:
    async with database.session_factory() as session:
        query = select(DispatchOutbox).order_by(DispatchOutbox.created_at, DispatchOutbox.id)
        if job_id is not None:
            query = query.where(DispatchOutbox.job_id == job_id)
        return list(await session.scalars(query))


async def history(database: Database, job_id: UUID) -> list[JobAttempt]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.generation, JobAttempt.attempt_number)
            )
        )


def envelope(job_id: UUID) -> dict[str, object]:
    return {
        "message": {
            "messageId": uuid4().hex,
            "data": base64.b64encode(str(job_id).encode("ascii")).decode("ascii"),
        },
        "subscription": SUBSCRIPTION,
    }


@pytest.mark.asyncio
async def test_future_schedule_persists_without_dispatch_attempts_or_early_execution(
    scheduled_database: Database,
) -> None:
    future = await database_now(scheduled_database) + timedelta(hours=1)
    submission = {
        "type": "calculation",
        "payload": {"numbers": [1, 2, 3]},
        "scheduled_at": future.astimezone(timezone(timedelta(hours=2))).isoformat(),
    }
    async with client_for(scheduled_database) as client:
        response = await client.post("/jobs", json=submission)
        assert response.status_code == 201
        record = response.json()
        job_id = UUID(record["id"])
        assert record["status"] == "SCHEDULED"
        assert datetime.fromisoformat(record["scheduled_at"]) == future
        assert record["scheduled_at"].endswith("Z")
        assert record["generation"] == record["attempt_count"] == 0
        assert record["started_at"] is record["finished_at"] is record["next_attempt_at"] is None
        assert (await client.get(f"/jobs/{job_id}")).json() == record
        listing = await client.get("/jobs")
        assert [row["id"] for row in listing.json()["items"]] == [str(job_id)]
        assert (await client.get(f"/jobs/{job_id}/attempts")).json() == []
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 409
    assert await schedule_due(scheduled_database) == 0
    assert await claim_pending(scheduled_database, lease_seconds=60) is None
    assert await claim_job(scheduled_database, job_id, lease_seconds=60) is None
    assert await claim_dispatch(scheduled_database, lease_seconds=60) is None
    assert not await worker.run_once(scheduled_database, settings_for(scheduled_database))
    assert await obligations(scheduled_database, job_id) == []
    assert await history(scheduled_database, job_id) == []
    async with client_for(scheduled_database, push=True) as client:
        for _ in range(3):
            assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert (await read_job(scheduled_database, job_id)).status is JobStatus.SCHEDULED
    assert await history(scheduled_database, job_id) == []


@pytest.mark.asyncio
async def test_due_schedule_requires_admission_then_executes_preserving_original_date(
    scheduled_database: Database,
) -> None:
    original_date = await database_now(scheduled_database) - timedelta(hours=1)
    job_id = await submit(scheduled_database, original_date)
    assert await claim_job(scheduled_database, job_id, lease_seconds=60) is None
    assert await claim_pending(scheduled_database, lease_seconds=60) is None
    async with client_for(scheduled_database, push=True) as client:
        assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert await schedule_due(scheduled_database) == 1
    current = await read_job(scheduled_database, job_id)
    assert current.status is JobStatus.PENDING
    assert current.scheduled_at == original_date
    assert current.attempt_count == 0
    assert current.started_at is current.finished_at is None
    assert await history(scheduled_database, job_id) == []
    rows = await obligations(scheduled_database, job_id)
    assert len(rows) == 1 and rows[0].generation == 0
    assert rows[0].status is DispatchStatus.READY
    assert rows[0].available_at >= original_date
    assert await schedule_due(scheduled_database) == 0
    assert await worker.run_once(scheduled_database, settings_for(scheduled_database))
    finished = await read_job(scheduled_database, job_id)
    assert finished.status is JobStatus.SUCCEEDED and finished.result_metadata == {"sum": 6}
    assert finished.scheduled_at == original_date
    assert len(await history(scheduled_database, job_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_null", [False, True])
async def test_absent_or_null_schedule_keeps_immediate_job_and_atomic_outbox(
    scheduled_database: Database,
    explicit_null: bool,
) -> None:
    submission: dict[str, object] = {"type": "calculation", "payload": {}}
    if explicit_null:
        submission["scheduled_at"] = None
    async with client_for(scheduled_database) as client:
        response = await client.post("/jobs", json=submission)
    assert response.status_code == 201
    assert response.json()["status"] == "PENDING" and response.json()["scheduled_at"] is None
    job_id = UUID(response.json()["id"])
    assert len(await obligations(scheduled_database, job_id)) == 1
    assert await schedule_due(scheduled_database) == 0
    assert await claim_job(scheduled_database, job_id, lease_seconds=60) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_date",
    [
        "2030-01-01",
        "2030-01-01T12:00:00",
        "2030-01-01 12:00:00Z",
        "2030-01-01T12:00:00.1234567Z",
        "0001-01-01T00:00:00+01:00",
        1_900_000_000,
        True,
    ],
)
async def test_invalid_scheduled_api_input_is_sanitized_and_creates_nothing(
    scheduled_database: Database,
    bad_date: object,
) -> None:
    async with client_for(scheduled_database) as client:
        response = await client.post(
            "/jobs",
            json={
                "type": "calculation",
                "payload": {"private-marker": "hidden"},
                "scheduled_at": bad_date,
            },
        )
    assert response.status_code == 422
    assert "private-marker" not in response.text and "hidden" not in response.text
    async with scheduled_database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
    assert await obligations(scheduled_database) == []


@pytest.mark.asyncio
async def test_bounded_scheduler_order_uses_date_then_uuid_and_leaves_future_rows(
    scheduled_database: Database,
) -> None:
    now = await database_now(scheduled_database)
    early = await submit(scheduled_database, now - timedelta(days=1))
    tied = [await submit(scheduled_database, now - timedelta(hours=1)) for _ in range(3)]
    late = await submit(scheduled_database, now - timedelta(minutes=1))
    future = await submit(scheduled_database, now + timedelta(days=1))
    expected = [early, *sorted(tied), late]
    assert await schedule_due(scheduled_database, limit=2) == 2
    assert {row.job_id for row in await obligations(scheduled_database)} == set(expected[:2])
    assert await schedule_due(scheduled_database, limit=2) == 2
    assert {row.job_id for row in await obligations(scheduled_database)} == set(expected[:4])
    assert await schedule_due(scheduled_database, limit=2) == 1
    assert await schedule_due(scheduled_database, limit=2) == 0
    assert {row.job_id for row in await obligations(scheduled_database)} == set(expected)
    assert (await read_job(scheduled_database, future)).status is JobStatus.SCHEDULED
    assert all(
        [(await read_job(scheduled_database, job_id)).attempt_count == 0 for job_id in expected]
    )


@pytest.mark.asyncio
async def test_scheduler_skips_a_locked_due_job_and_admits_it_after_lock_release(
    scheduled_database: Database,
) -> None:
    past = await database_now(scheduled_database) - timedelta(hours=1)
    locked_id = await submit(scheduled_database, past - timedelta(hours=1))
    other_id = await submit(scheduled_database, past)
    async with scheduled_database.session_factory.begin() as holder:
        await holder.execute(
            text("SELECT id FROM jobs WHERE id = :id FOR UPDATE"), {"id": locked_id}
        )
        async with asyncio.timeout(3):
            assert await schedule_due(scheduled_database, limit=1) == 1
        assert [row.job_id for row in await obligations(scheduled_database)] == [other_id]
        assert (await read_job(scheduled_database, locked_id)).status is JobStatus.SCHEDULED
    assert await schedule_due(scheduled_database, limit=1) == 1
    assert len(await obligations(scheduled_database)) == 2


@pytest.mark.asyncio
async def test_eight_scheduler_replicas_admit_each_job_once_without_consuming_attempts(
    scheduled_database: Database,
) -> None:
    past = await database_now(scheduled_database) - timedelta(hours=1)
    job_ids = [await submit(scheduled_database, past) for _ in range(20)]
    other = Database(settings_for(scheduled_database))
    barrier = asyncio.Barrier(8)

    async def admit(index: int) -> int:
        await barrier.wait()
        return await schedule_due(scheduled_database if index % 2 else other, limit=3)

    try:
        async with asyncio.timeout(10):
            admitted = await asyncio.gather(*(admit(index) for index in range(8)))
        assert sum(admitted) == 20
        assert all(0 <= count <= 3 for count in admitted)
        rows = await obligations(scheduled_database)
        assert len(rows) == 20 and {row.job_id for row in rows} == set(job_ids)
        assert await schedule_due(other) == 0
        for job_id in job_ids:
            job = await read_job(scheduled_database, job_id)
            assert job.status is JobStatus.PENDING and job.attempt_count == 0
            assert await history(scheduled_database, job_id) == []
    finally:
        await other.dispose()


@pytest.mark.asyncio
async def test_scheduler_polling_and_push_race_commits_one_admission_and_owned_attempt(
    scheduled_database: Database,
) -> None:
    past = await database_now(scheduled_database) - timedelta(hours=1)
    job_id = await submit(scheduled_database, past)
    other = Database(settings_for(scheduled_database))
    barrier = asyncio.Barrier(8)

    async def admit(index: int) -> int:
        await barrier.wait()
        return await schedule_due(scheduled_database if index % 2 else other)

    async def poll(index: int) -> bool:
        await barrier.wait()
        database = scheduled_database if index % 2 else other
        return await worker.run_once(database, settings_for(database))

    try:
        async with (
            client_for(scheduled_database, push=True) as first,
            client_for(other, push=True) as second,
        ):

            async def deliver(client: AsyncClient) -> int:
                await barrier.wait()
                return (await client.post("/pubsub/push", json=envelope(job_id))).status_code

            async with asyncio.timeout(10):
                responses = await asyncio.gather(
                    *(admit(index) for index in range(4)),
                    poll(0),
                    poll(1),
                    deliver(first),
                    deliver(second),
                )
        assert sum(responses[:4]) == 1
        assert all(response in (204, 503) for response in responses[6:])
        if (await read_job(scheduled_database, job_id)).status is JobStatus.PENDING:
            assert await worker.run_once(scheduled_database, settings_for(scheduled_database))
        finished = await read_job(scheduled_database, job_id)
        assert finished.status is JobStatus.SUCCEEDED
        assert finished.attempt_count == 1 and finished.result_metadata == {"sum": 6}
        assert len(await obligations(scheduled_database, job_id)) == 1
        assert len(await history(scheduled_database, job_id)) == 1
    finally:
        await other.dispose()


@pytest.mark.asyncio
async def test_failed_outbox_insert_rolls_back_entire_scheduler_batch(
    scheduled_database: Database,
) -> None:
    past = await database_now(scheduled_database) - timedelta(hours=1)
    jobs = [await submit(scheduled_database, past) for _ in range(3)]
    before = [
        JobRead.model_validate(await read_job(scheduled_database, job_id)).model_dump()
        for job_id in jobs
    ]
    async with scheduled_database.session_factory.begin() as session:
        await session.execute(
            text("ALTER TABLE dispatch_outbox ADD CONSTRAINT test_reject_admission CHECK (false)")
        )
    with pytest.raises(IntegrityError):
        await schedule_due(scheduled_database)
    assert [
        JobRead.model_validate(await read_job(scheduled_database, job_id)).model_dump()
        for job_id in jobs
    ] == before
    assert await obligations(scheduled_database) == []
    async with scheduled_database.session_factory.begin() as session:
        await session.execute(
            text("ALTER TABLE dispatch_outbox DROP CONSTRAINT test_reject_admission")
        )
    assert await schedule_due(scheduled_database) == 3


async def wait_for_database_block(
    database: Database,
    application_name: str,
    holder_pid: int,
    query_pattern: str,
) -> None:
    async with asyncio.timeout(5):
        while True:
            async with database.session_factory() as observer:
                blocked = await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE application_name = :name AND :pid = ANY(pg_blocking_pids(pid)) "
                        "AND query LIKE :pattern)"
                    ),
                    {"name": application_name, "pid": holder_pid, "pattern": query_pattern},
                )
            if blocked:
                return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_cancelled_admission_rolls_back_and_early_push_ack_retains_durable_intent(
    scheduled_database: Database,
) -> None:
    past = await database_now(scheduled_database) - timedelta(hours=1)
    job_id = await submit(scheduled_database, past)
    before = JobRead.model_validate(await read_job(scheduled_database, job_id)).model_dump()
    name = f"scheduler_cancel_{uuid4().hex}"
    other = Database(settings_for(scheduled_database, name))
    admission: asyncio.Task[int] | None = None
    try:
        async with scheduled_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await holder.execute(text("LOCK TABLE dispatch_outbox IN ACCESS EXCLUSIVE MODE"))
            admission = asyncio.create_task(schedule_due(other))
            try:
                await wait_for_database_block(
                    scheduled_database, name, holder_pid, "%INSERT INTO dispatch_outbox%"
                )
                async with client_for(scheduled_database, push=True) as push_client:
                    async with asyncio.timeout(3):
                        response = await push_client.post("/pubsub/push", json=envelope(job_id))
                    assert response.status_code == 204
                admission.cancel()
                with pytest.raises(asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await admission
            finally:
                if admission is not None and not admission.done():
                    admission.cancel()
                    await asyncio.gather(admission, return_exceptions=True)
        assert (
            JobRead.model_validate(await read_job(scheduled_database, job_id)).model_dump()
            == before
        )
        assert await obligations(scheduled_database, job_id) == []
        assert await history(scheduled_database, job_id) == []
        assert await schedule_due(other) == 1
    finally:
        await other.dispose()


@pytest.mark.asyncio
async def test_one_database_clock_snapshot_does_not_expand_due_batch_while_query_waits(
    scheduled_database: Database,
) -> None:
    due_at = await database_now(scheduled_database) + timedelta(seconds=0.75)
    job_id = await submit(scheduled_database, due_at)
    name = f"scheduler_cutoff_{uuid4().hex}"
    other = Database(settings_for(scheduled_database, name))
    admission: asyncio.Task[int] | None = None
    try:
        async with scheduled_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await holder.execute(text("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE"))
            admission = asyncio.create_task(schedule_due(other))
            try:
                await wait_for_database_block(scheduled_database, name, holder_pid, "%FROM jobs%")
                async with asyncio.timeout(3):
                    while await database_now(scheduled_database) <= due_at:
                        await asyncio.sleep(0.01)
            except BaseException:
                admission.cancel()
                await asyncio.gather(admission, return_exceptions=True)
                raise
        async with asyncio.timeout(5):
            assert await admission == 0
        assert (await read_job(scheduled_database, job_id)).status is JobStatus.SCHEDULED
        assert await schedule_due(other) == 1
    finally:
        if admission is not None and not admission.done():
            admission.cancel()
            await asyncio.gather(admission, return_exceptions=True)
        await other.dispose()


@pytest.mark.asyncio
async def test_admission_survives_scheduler_restart_before_publication(
    scheduled_database: Database,
) -> None:
    original_date = await database_now(scheduled_database) - timedelta(hours=1)
    job_id = await submit(scheduled_database, original_date)
    first = Database(settings_for(scheduled_database))
    assert await schedule_due(first) == 1
    await first.dispose()
    restarted = Database(settings_for(scheduled_database))
    try:
        assert await schedule_due(restarted) == 0
        publication = await claim_dispatch(restarted, lease_seconds=60)
        assert publication is not None and publication.job_id == job_id
        assert await complete_dispatch(restarted, publication)
        async with client_for(restarted, push=True) as push_client:
            assert (
                await push_client.post("/pubsub/push", json=envelope(job_id))
            ).status_code == 204
        finished = await read_job(restarted, job_id)
        assert finished.status is JobStatus.SUCCEEDED and finished.scheduled_at == original_date
        assert (
            len(await obligations(restarted, job_id)) == len(await history(restarted, job_id)) == 1
        )
    finally:
        await restarted.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [JobStatus.PENDING, JobStatus.QUEUED])
async def test_initial_execution_defensively_rechecks_future_date_after_synthetic_admission(
    scheduled_database: Database,
    state: JobStatus,
) -> None:
    future = await database_now(scheduled_database) + timedelta(hours=1)
    job_id = await submit(scheduled_database, future)
    async with scheduled_database.session_factory.begin() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        job.status = state
    assert await claim_job(scheduled_database, job_id, lease_seconds=60) is None
    assert await claim_pending(scheduled_database, lease_seconds=60) is None
    async with client_for(scheduled_database, push=True) as client:
        assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 503
    assert (await read_job(scheduled_database, job_id)).attempt_count == 0
    assert await history(scheduled_database, job_id) == []


@pytest.mark.asyncio
async def test_synthetic_outbox_for_scheduled_job_is_skipped_without_changing_admission(
    scheduled_database: Database,
) -> None:
    job_id = await submit(
        scheduled_database, await database_now(scheduled_database) - timedelta(hours=1)
    )
    async with scheduled_database.session_factory.begin() as session:
        session.add(DispatchOutbox(job_id=job_id))
    assert await claim_dispatch(scheduled_database, lease_seconds=60) is None
    rows = await obligations(scheduled_database, job_id)
    assert len(rows) == 1 and rows[0].status is DispatchStatus.SKIPPED
    assert (await read_job(scheduled_database, job_id)).status is JobStatus.SCHEDULED
    assert await schedule_due(scheduled_database) == 1
    publication = await claim_dispatch(scheduled_database, lease_seconds=60)
    assert publication is not None and publication.id != rows[0].id


@pytest.mark.asyncio
async def test_schedule_identity_replays_during_wait_and_after_manual_recovery(
    scheduled_database: Database,
) -> None:
    original_date = await database_now(scheduled_database) - timedelta(hours=1)
    body = {
        "type": "calculation",
        "payload": {"numbers": [1, 2, 3]},
        "max_attempts": 1,
        "scheduled_at": original_date.isoformat(),
    }
    headers = {"Idempotency-Key": "scheduled-private-marker"}
    async with client_for(scheduled_database) as client:
        initial = await client.post("/jobs", json=body, headers=headers)
        assert initial.status_code == 201
        job_id = UUID(initial.json()["id"])
        equivalent = {
            **body,
            "scheduled_at": original_date.astimezone(timezone(timedelta(hours=2))).isoformat(),
        }
        replay = await client.post("/jobs", json=equivalent, headers=headers)
        assert replay.status_code == 200 and replay.json() == initial.json()
        for changed in (None, (original_date + timedelta(microseconds=1)).isoformat()):
            conflict = await client.post(
                "/jobs", json={**body, "scheduled_at": changed}, headers=headers
            )
            assert conflict.status_code == 409 and "private-marker" not in conflict.text
        assert await schedule_due(scheduled_database) == 1
        owner = await claim_job(scheduled_database, job_id, lease_seconds=60)
        assert owner is not None
        assert await complete_claim(scheduled_database, owner, error=FAILURE)
        assert (await read_job(scheduled_database, job_id)).status is JobStatus.DEAD_LETTERED
        recovered = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        assert recovered.status_code == 202
        assert recovered.json()["status"] == "PENDING" and recovered.json()["generation"] == 1
        assert datetime.fromisoformat(recovered.json()["scheduled_at"]) == original_date
        assert await schedule_due(scheduled_database) == 0
        assert not await complete_claim(scheduled_database, owner, result={"sum": 999})
    async with client_for(scheduled_database) as restarted:
        replay = await restarted.post("/jobs", json=equivalent, headers=headers)
        assert replay.status_code == 200 and replay.json()["generation"] == 1
    assert await worker.run_once(scheduled_database, settings_for(scheduled_database))
    finished = await read_job(scheduled_database, job_id)
    assert finished.status is JobStatus.SUCCEEDED and finished.generation == 1
    assert finished.scheduled_at == original_date
    assert [
        (row.generation, row.attempt_number) for row in await history(scheduled_database, job_id)
    ] == [
        (0, 1),
        (1, 1),
    ]
    assert [row.generation for row in await obligations(scheduled_database, job_id)] == [0, 1]


@pytest.mark.asyncio
async def test_automatic_retry_retains_schedule_but_uses_its_independent_due_time(
    scheduled_database: Database,
) -> None:
    original_date = await database_now(scheduled_database) - timedelta(days=1)
    job_id = await submit(scheduled_database, original_date)
    assert await schedule_due(scheduled_database) == 1
    first = await claim_job(scheduled_database, job_id, lease_seconds=60)
    assert first is not None
    assert await complete_claim(
        scheduled_database,
        first,
        error=FAILURE,
        retry_policy=RetryPolicy(base_seconds=60, max_seconds=60),
    )
    waiting = await read_job(scheduled_database, job_id)
    assert waiting.status is JobStatus.RETRY_WAIT and waiting.scheduled_at == original_date
    assert waiting.next_attempt_at is not None and waiting.next_attempt_at > await database_now(
        scheduled_database
    )
    assert await schedule_due(scheduled_database) == 0
    assert await claim_job(scheduled_database, job_id, lease_seconds=60) is None
    assert await claim_pending(scheduled_database, lease_seconds=60) is None
    async with client_for(scheduled_database, push=True) as client:
        assert (await client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
    assert len(await history(scheduled_database, job_id)) == 1
    assert len(await obligations(scheduled_database, job_id)) == 2
    async with scheduled_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = :id"),
            {"id": job_id},
        )
    second = await claim_job(scheduled_database, job_id, lease_seconds=60)
    assert second is not None and second.attempt_count == 2 and second.generation == 0
    assert await complete_claim(scheduled_database, second, result={"sum": 6})
    assert (await read_job(scheduled_database, job_id)).scheduled_at == original_date


@pytest.mark.asyncio
async def test_pre_upgrade_keyed_job_replays_with_null_schedule_using_original_digest(
    integration_database_url: str,
) -> None:
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0007")
    database = Database(settings_for_url(integration_database_url))
    job_id = uuid4()
    try:
        async with database.session_factory.begin() as session:
            await session.execute(
                text(
                    "INSERT INTO jobs (id, type, payload, idempotency_key, request_fingerprint) "
                    "VALUES (:id, 'calculation', '{\"numbers\":[1,2,3]}'::jsonb, "
                    "'legacy:key', :digest)"
                ),
                {"id": job_id, "digest": LEGACY_DIGEST},
            )
            await session.execute(
                text("INSERT INTO dispatch_outbox (id, job_id) VALUES (:id, :job_id)"),
                {"id": uuid4(), "job_id": job_id},
            )
        command.upgrade(config, "head")
        async with client_for(database) as client:
            for explicit_null in (False, True):
                body: dict[str, object] = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}}
                if explicit_null:
                    body["scheduled_at"] = None
                replay = await client.post(
                    "/jobs", json=body, headers={"Idempotency-Key": "legacy:key"}
                )
                assert replay.status_code == 200 and replay.json()["id"] == str(job_id)
                assert (
                    replay.json()["status"] == "PENDING" and replay.json()["scheduled_at"] is None
                )
        assert (await read_job(database, job_id)).request_fingerprint == LEGACY_DIGEST
        assert len(await obligations(database)) == 1
    finally:
        await database.dispose()
