"""Bounded priority head starts and aging are selection policies in PostgreSQL."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.attempts import JobAttempt
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_job, claim_pending, complete_claim
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.outbox import (
    DispatchClaim,
    DispatchOutbox,
    DispatchStatus,
    claim_dispatch,
    complete_dispatch,
    fail_dispatch,
)
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app
from distributed_cloud_scheduler.retries import RetryPolicy
from distributed_cloud_scheduler.scheduler import schedule_due

SELECTORS = ["scheduler", "polling", "dispatcher"]
SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"
IMMEDIATE_DIGEST = "5dfcf9a1ceff59c1889e385e9eee08c5013f2936a9a97a64a3e83a59b956c75a"
SCHEDULED_DIGEST = "0db6a9ce94186dfe80758f0556914f1c5adf26d265697fba19b95f9215fe4809"


def migration_config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


def settings_for(database: Database) -> Settings:
    return Settings(
        database_url=database.engine.url.render_as_string(hide_password=False), _env_file=None
    )


@pytest_asyncio.fixture
async def priority_database(integration_database_url: str) -> AsyncIterator[Database]:
    command.upgrade(migration_config(), "head")
    database = Database(Settings(database_url=integration_database_url, _env_file=None))
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


async def now(database: Database) -> datetime:
    async with database.session_factory() as session:
        timestamp = await session.scalar(select(func.clock_timestamp()))
        assert isinstance(timestamp, datetime)
        return timestamp


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def obligations(database: Database) -> list[DispatchOutbox]:
    async with database.session_factory() as session:
        return list(await session.scalars(select(DispatchOutbox)))


async def submit(
    database: Database,
    selector: str,
    priority: int,
    *,
    anchor: datetime | None = None,
    scheduled_at: datetime | None = None,
    budget: int = 3,
) -> UUID:
    if selector == "scheduler" and scheduled_at is None:
        scheduled_at = await now(database) - timedelta(minutes=1)
    async with database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(
                type="calculation",
                payload={"numbers": [1, 2, 3]},
                priority=priority,
                scheduled_at=scheduled_at,
                max_attempts=budget,
            ),
        )
        job_id = job.id
    if anchor is not None:
        # Controlled eligible timestamps model waiting without sleeping or changing
        # original task data. Production updates these at each eligibility episode.
        async with database.session_factory.begin() as session:
            await session.execute(
                text("UPDATE jobs SET ready_at = :anchor WHERE id = :id"),
                {"anchor": anchor, "id": job_id},
            )
            if selector == "dispatcher":
                await session.execute(
                    text("UPDATE dispatch_outbox SET available_at = :anchor WHERE job_id = :id"),
                    {"anchor": anchor, "id": job_id},
                )
    return job_id


async def choose(database: Database, selector: str) -> UUID | None:
    if selector == "polling":
        owner = await claim_pending(database, lease_seconds=60)
        return owner.id if owner is not None else None
    if selector == "dispatcher":
        owner = await claim_dispatch(database, lease_seconds=60)
        return owner.job_id if owner is not None else None
    before = {row.job_id for row in await obligations(database)}
    if await schedule_due(database, limit=1) == 0:
        return None
    admitted = {row.job_id for row in await obligations(database)} - before
    assert len(admitted) == 1
    return admitted.pop()


def envelope(job_id: UUID) -> dict[str, object]:
    return {
        "message": {
            "messageId": uuid4().hex,
            "data": base64.b64encode(str(job_id).encode("ascii")).decode("ascii"),
        },
        "subscription": SUBSCRIPTION,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_each_database_selector_orders_equal_readiness_by_priority(
    priority_database: Database,
    selector: str,
) -> None:
    anchor = await now(priority_database) - timedelta(seconds=1)
    low = await submit(priority_database, selector, 0, anchor=anchor)
    middle = await submit(priority_database, selector, 4, anchor=anchor)
    high = await submit(priority_database, selector, 9, anchor=anchor)
    assert [await choose(priority_database, selector) for _ in range(3)] == [high, middle, low]
    assert await choose(priority_database, selector) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_equal_ranks_use_readiness_then_uuid_instead_of_raw_priority(
    priority_database: Database,
    selector: str,
) -> None:
    anchor = await now(priority_database) - timedelta(seconds=10)
    old_low = await submit(priority_database, selector, 0, anchor=anchor)
    new_high = await submit(priority_database, selector, 1, anchor=anchor + timedelta(seconds=5))
    tied = [await submit(priority_database, selector, 0, anchor=anchor) for _ in range(2)]
    earlier = [old_low, *tied]
    if selector == "dispatcher":
        # Dispatch's stable UUID is its obligation, rather than the related job.
        row_ids = {row.job_id: row.id for row in await obligations(priority_database)}
        earlier.sort(key=row_ids.__getitem__)
    else:
        earlier.sort()
    assert [await choose(priority_database, selector) for _ in range(4)] == [*earlier, new_high]


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
@pytest.mark.parametrize("old_age", [44, 45, 46])
async def test_maximum_priority_head_start_has_an_exact_forty_five_second_boundary(
    priority_database: Database,
    selector: str,
    old_age: int,
) -> None:
    fresh = await now(priority_database) - timedelta(seconds=1)
    low = await submit(priority_database, selector, 0, anchor=fresh - timedelta(seconds=old_age))
    high = await submit(priority_database, selector, 9, anchor=fresh)
    assert await choose(priority_database, selector) == (high if old_age < 45 else low)


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_sustained_fresh_urgent_arrivals_cannot_starve_aged_unlocked_low_work(
    priority_database: Database,
    selector: str,
) -> None:
    initial = await now(priority_database)
    low = await submit(priority_database, selector, 0, anchor=initial - timedelta(seconds=60))
    previous = [
        await submit(priority_database, selector, 9, anchor=initial - timedelta(seconds=25)),
        await submit(priority_database, selector, 9, anchor=initial - timedelta(seconds=20)),
    ]
    selected: list[UUID | None] = []
    for _ in range(12):
        # Arrival rate exceeds selection rate. Every pass has fresh priority-9 work,
        # yet finite earlier ranked work is drained and the waiting low job progresses.
        for _ in range(3):
            await submit(priority_database, selector, 9)
        selected.append(await choose(priority_database, selector))
    assert selected[:3] == [*previous, low]
    assert selected.count(low) == 1
    async with priority_database.session_factory() as session:
        if selector == "dispatcher":
            remaining = await session.scalar(
                select(func.count())
                .select_from(DispatchOutbox)
                .where(DispatchOutbox.status == DispatchStatus.READY)
            )
        else:
            status = JobStatus.SCHEDULED if selector == "scheduler" else JobStatus.PENDING
            remaining = await session.scalar(
                select(func.count()).select_from(Job).where(Job.status == status)
            )
        assert remaining == 27


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_locked_urgent_candidate_does_not_block_available_lower_priority(
    priority_database: Database,
    selector: str,
) -> None:
    anchor = await now(priority_database) - timedelta(seconds=1)
    high = await submit(priority_database, selector, 9, anchor=anchor)
    low = await submit(priority_database, selector, 0, anchor=anchor)
    async with priority_database.session_factory.begin() as holder:
        if selector == "dispatcher":
            await holder.execute(
                text("SELECT id FROM dispatch_outbox WHERE job_id = :id FOR UPDATE"), {"id": high}
            )
        else:
            await holder.execute(
                text("SELECT id FROM jobs WHERE id = :id FOR UPDATE"), {"id": high}
            )
        async with asyncio.timeout(3):
            assert await choose(priority_database, selector) == low
    assert await choose(priority_database, selector) == high


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_priority_and_artificial_age_never_bypass_future_eligibility(
    priority_database: Database,
    selector: str,
) -> None:
    timestamp = await now(priority_database)
    low = await submit(priority_database, selector, 0)
    if selector == "dispatcher":
        high = await submit(priority_database, selector, 9)
        async with priority_database.session_factory.begin() as session:
            await session.execute(
                text("UPDATE dispatch_outbox SET available_at = :future WHERE job_id = :id"),
                {"future": timestamp + timedelta(days=1), "id": high},
            )
    else:
        high = await submit(
            priority_database,
            selector,
            9,
            scheduled_at=timestamp + timedelta(days=1),
            anchor=timestamp - timedelta(days=1),
        )
        if selector == "polling":
            async with priority_database.session_factory.begin() as session:
                await session.execute(
                    text("UPDATE jobs SET status = 'PENDING' WHERE id = :id"), {"id": high}
                )
    assert await choose(priority_database, selector) == low
    assert await choose(priority_database, selector) is None
    assert (await read_job(priority_database, high)).attempt_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", SELECTORS)
async def test_rank_order_is_independent_of_database_session_timezone(
    priority_database: Database,
    selector: str,
) -> None:
    timestamp = await now(priority_database) - timedelta(seconds=20)
    low = await submit(priority_database, selector, 0, anchor=timestamp)
    high = await submit(priority_database, selector, 5, anchor=timestamp + timedelta(seconds=10))
    url = priority_database.engine.url
    options = url.query.get("options", "")
    assert isinstance(options, str)
    url = url.update_query_dict({"options": f"{options} -cTimeZone=Pacific/Auckland"})
    other = Database(
        Settings(database_url=url.render_as_string(hide_password=False), _env_file=None)
    )
    try:
        assert await choose(other, selector) == high
        assert await choose(priority_database, selector) == low
    finally:
        await other.dispose()


@pytest.mark.asyncio
async def test_expired_publication_uses_lease_expiry_not_the_old_available_timestamp(
    priority_database: Database,
) -> None:
    timestamp = await now(priority_database)
    high = await submit(priority_database, "dispatcher", 9, anchor=timestamp - timedelta(hours=1))
    low = await submit(priority_database, "dispatcher", 0, anchor=timestamp - timedelta(seconds=50))
    old_token = uuid4()
    async with priority_database.session_factory.begin() as session:
        row = await session.scalar(select(DispatchOutbox).where(DispatchOutbox.job_id == high))
        assert row is not None
        row.status = DispatchStatus.PUBLISHING
        row.claim_token = old_token
        row.lease_expires_at = timestamp - timedelta(seconds=1)
        row.publish_attempt_count = 1
        old = DispatchClaim(row.id, high, old_token, 1)
    assert await choose(priority_database, "dispatcher") == low
    new = await claim_dispatch(priority_database, lease_seconds=60)
    assert new is not None and new.job_id == high and new.attempt_count == 2
    assert not await complete_dispatch(priority_database, old)
    assert await complete_dispatch(priority_database, new)


@pytest.mark.asyncio
async def test_joined_dispatch_priority_does_not_lock_the_job_or_wait_for_its_owner(
    priority_database: Database,
) -> None:
    job_id = await submit(priority_database, "dispatcher", 9)
    async with priority_database.session_factory.begin() as holder:
        await holder.execute(text("SELECT id FROM jobs WHERE id = :id FOR UPDATE"), {"id": job_id})
        async with asyncio.timeout(3):
            owner = await claim_dispatch(priority_database, lease_seconds=60)
        assert owner is not None and owner.job_id == job_id
    assert await complete_dispatch(priority_database, owner)


@pytest.mark.asyncio
async def test_failed_publication_backoff_resets_dispatch_episode_without_mutating_job_readiness(
    priority_database: Database,
) -> None:
    job_id = await submit(
        priority_database, "dispatcher", 9, anchor=await now(priority_database) - timedelta(hours=1)
    )
    original = await read_job(priority_database, job_id)
    owner = await claim_dispatch(priority_database, lease_seconds=60)
    assert owner is not None
    assert await fail_dispatch(priority_database, owner, delay_seconds=60)
    row = (await obligations(priority_database))[0]
    assert row.available_at > await now(priority_database)
    assert await claim_dispatch(priority_database, lease_seconds=60) is None
    current = await read_job(priority_database, job_id)
    assert current.priority == 9 and current.ready_at == original.ready_at


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["polling", "dispatcher"])
async def test_independent_prioritized_selectors_commit_only_one_owner(
    priority_database: Database,
    selector: str,
) -> None:
    timestamp = await now(priority_database) - timedelta(seconds=1)
    job_id = await submit(priority_database, selector, 9, anchor=timestamp)
    other = Database(settings_for(priority_database))
    barrier = asyncio.Barrier(8)

    async def contend(index: int) -> UUID | None:
        await barrier.wait()
        return await choose(priority_database if index % 2 else other, selector)

    try:
        async with asyncio.timeout(10):
            selected = await asyncio.gather(*(contend(index) for index in range(8)))
        assert selected.count(job_id) == 1 and selected.count(None) == 7
        if selector == "polling":
            async with priority_database.session_factory() as session:
                assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 1
        else:
            assert (await obligations(priority_database))[0].publish_attempt_count == 1
    finally:
        await other.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("push", [False, True])
async def test_targeted_low_id_executes_despite_higher_priority_waiting_work(
    priority_database: Database,
    push: bool,
) -> None:
    timestamp = await now(priority_database) - timedelta(seconds=1)
    low = await submit(priority_database, "polling", 0, anchor=timestamp)
    high = await submit(priority_database, "polling", 9, anchor=timestamp)
    if push:
        async with client_for(priority_database, push=True) as client:
            assert (await client.post("/pubsub/push", json=envelope(low))).status_code == 204
    else:
        owner = await claim_job(priority_database, low, lease_seconds=60)
        assert owner is not None and owner.id == low
        assert await complete_claim(priority_database, owner, result={"sum": 6})
    assert (await read_job(priority_database, low)).status is JobStatus.SUCCEEDED
    assert (await read_job(priority_database, high)).status is JobStatus.PENDING
    assert await choose(priority_database, "polling") == high


@pytest.mark.asyncio
async def test_priority_and_internal_readiness_persist_publicly_without_exposing_ready_at(
    priority_database: Database,
) -> None:
    timestamp = await now(priority_database)
    async with client_for(priority_database) as client:
        for scheduled_at in (None, timestamp - timedelta(days=1), timestamp + timedelta(days=1)):
            response = await client.post(
                "/jobs",
                json={
                    "type": "calculation",
                    "payload": {},
                    "priority": 7,
                    "scheduled_at": scheduled_at.isoformat() if scheduled_at is not None else None,
                },
            )
            assert response.status_code == 201 and response.json()["priority"] == 7
            assert "ready_at" not in response.json()
            job_id = UUID(response.json()["id"])
            stored = await read_job(priority_database, job_id)
            expected = max(stored.created_at, scheduled_at) if scheduled_at else stored.created_at
            assert stored.ready_at == expected
            inspected = await client.get(f"/jobs/{job_id}")
            assert inspected.json()["priority"] == 7 and "ready_at" not in inspected.json()
        page = await client.get("/jobs")
        assert all(row["priority"] == 7 and "ready_at" not in row for row in page.json()["items"])


@pytest.mark.asyncio
async def test_automatic_retry_starts_new_wait_episode_preserving_priority_and_original_schedule(
    priority_database: Database,
) -> None:
    original_schedule = await now(priority_database) - timedelta(days=1)
    job_id = await submit(
        priority_database, "scheduler", 8, scheduled_at=original_schedule, anchor=original_schedule
    )
    assert await schedule_due(priority_database) == 1
    original = await read_job(priority_database, job_id)
    assert original.ready_at == original_schedule
    first = await claim_job(priority_database, job_id, lease_seconds=60)
    assert first is not None
    assert await complete_claim(
        priority_database,
        first,
        error={"code": "controlled_failure", "retryable": True},
        retry_policy=RetryPolicy(base_seconds=60, max_seconds=60),
    )
    waiting = await read_job(priority_database, job_id)
    assert waiting.priority == 8 and waiting.scheduled_at == original_schedule
    assert waiting.ready_at == waiting.next_attempt_at > original.ready_at
    assert await choose(priority_database, "polling") is None
    assert await schedule_due(priority_database) == 0
    async with priority_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = :id"),
            {"id": job_id},
        )
    second = await claim_job(priority_database, job_id, lease_seconds=60)
    assert second is not None
    assert await complete_claim(priority_database, second, result={"sum": 6})
    assert (await read_job(priority_database, job_id)).priority == 8


@pytest.mark.asyncio
async def test_manual_recovery_resets_age_and_retains_nonzero_submission_identity(
    priority_database: Database,
) -> None:
    headers = {"Idempotency-Key": "priority-private-marker"}
    body = {
        "type": "calculation",
        "payload": {"numbers": [1, 2, 3]},
        "priority": 1,
        "max_attempts": 1,
    }
    async with client_for(priority_database) as client:
        initial = await client.post("/jobs", json=body, headers=headers)
        job_id = UUID(initial.json()["id"])
        async with priority_database.session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE jobs SET ready_at = clock_timestamp() - "
                    "INTERVAL '1 hour' WHERE id = :id"
                ),
                {"id": job_id},
            )
        before = await read_job(priority_database, job_id)
        owner = await claim_job(priority_database, job_id, lease_seconds=60)
        assert owner is not None
        assert await complete_claim(
            priority_database, owner, error={"code": "controlled_failure", "retryable": True}
        )
        recovery_before = await now(priority_database)
        response = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        recovery_after = await now(priority_database)
        assert response.status_code == 202 and response.json()["priority"] == 1
        current = await read_job(priority_database, job_id)
        assert recovery_before <= current.ready_at <= recovery_after
        assert current.ready_at > before.ready_at
        assert current.priority == before.priority == 1
        assert current.request_fingerprint == before.request_fingerprint
        replay = await client.post("/jobs", json=body, headers=headers)
        assert replay.status_code == 200 and replay.json()["generation"] == 1
        conflict = await client.post("/jobs", json={**body, "priority": 2}, headers=headers)
        assert conflict.status_code == 409 and "private-marker" not in conflict.text
    high = await submit(priority_database, "polling", 9)
    assert await choose(priority_database, "polling") == high
    assert await choose(priority_database, "polling") == job_id


@pytest.mark.asyncio
async def test_priority_changes_conflict_but_default_zero_replays_across_api_restarts(
    priority_database: Database,
) -> None:
    body = {"type": "calculation", "payload": {}}
    headers = {"Idempotency-Key": "priority-zero"}
    async with client_for(priority_database) as client:
        initial = await client.post("/jobs", json=body, headers=headers)
        assert initial.status_code == 201 and initial.json()["priority"] == 0
    async with client_for(priority_database) as restarted:
        replay = await restarted.post("/jobs", json={**body, "priority": 0}, headers=headers)
        assert replay.status_code == 200 and replay.json() == initial.json()
        conflict = await restarted.post("/jobs", json={**body, "priority": 9}, headers=headers)
        assert conflict.status_code == 409
    assert len(await obligations(priority_database)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("scheduled", [False, True])
async def test_pre_priority_keyed_rows_replay_with_zero_after_upgrade(
    integration_database_url: str,
    scheduled: bool,
) -> None:
    command.upgrade(migration_config(), "0008")
    database = Database(Settings(database_url=integration_database_url, _env_file=None))
    job_id = uuid4()
    scheduled_at = "2030-02-03T04:05:06.123456Z" if scheduled else None
    digest = SCHEDULED_DIGEST if scheduled else IMMEDIATE_DIGEST
    try:
        async with database.session_factory.begin() as session:
            await session.execute(
                text(
                    "INSERT INTO jobs (id, type, payload, status, scheduled_at, idempotency_key, "
                    "request_fingerprint) VALUES (:id, 'calculation', "
                    "'{\"numbers\":[1,2,3]}'::jsonb, "
                    ":status, CAST(:schedule AS timestamptz), 'legacy:priority', :digest)"
                ),
                {
                    "id": job_id,
                    "status": "SCHEDULED" if scheduled else "PENDING",
                    "schedule": scheduled_at,
                    "digest": digest,
                },
            )
            if not scheduled:
                await session.execute(
                    text("INSERT INTO dispatch_outbox (id, job_id) VALUES (:id, :job_id)"),
                    {"id": uuid4(), "job_id": job_id},
                )
        command.upgrade(migration_config(), "head")
        async with client_for(database) as client:
            for explicit_zero in (False, True):
                body: dict[str, object] = {
                    "type": "calculation",
                    "payload": {"numbers": [1, 2, 3]},
                    "scheduled_at": scheduled_at,
                }
                if explicit_zero:
                    body["priority"] = 0
                replay = await client.post(
                    "/jobs", json=body, headers={"Idempotency-Key": "legacy:priority"}
                )
                assert replay.status_code == 200 and replay.json()["id"] == str(job_id)
                assert replay.json()["priority"] == 0
        retained = await read_job(database, job_id)
        assert retained.request_fingerprint == digest
        assert len(await obligations(database)) == (0 if scheduled else 1)
    finally:
        await database.dispose()
