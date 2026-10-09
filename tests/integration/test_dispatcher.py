import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler import dispatcher
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import Claim, claim_job
from distributed_cloud_scheduler.jobs import Job, JobCreate, create_job, get_job
from distributed_cloud_scheduler.messaging import LocalQueue
from distributed_cloud_scheduler.outbox import (
    DispatchClaim,
    DispatchOutbox,
    claim_dispatch,
    complete_dispatch,
    fail_dispatch,
)
from distributed_cloud_scheduler.worker import execute_claim, run_once


@pytest.fixture
def dispatch_settings(migrated_integration_database_url: str) -> Settings:
    url = make_url(migrated_integration_database_url).update_query_dict(
        {"application_name": f"dispatch_test_{uuid4().hex}"}
    )
    return Settings(database_url=url.render_as_string(hide_password=False), _env_file=None)


@pytest_asyncio.fixture
async def dispatch_database(dispatch_settings: Settings) -> AsyncIterator[Database]:
    database = Database(dispatch_settings)
    try:
        yield database
    finally:
        await database.dispose()


async def submit_job(database: Database) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(session, JobCreate(type="calculation", payload={"numbers": [1]}))
        return job.id


async def read_obligation(database: Database, job_id: UUID) -> DispatchOutbox:
    async with database.session_factory() as session:
        row = await session.scalar(select(DispatchOutbox).where(DispatchOutbox.job_id == job_id))
        assert row is not None
        return row


async def database_time(database: Database) -> datetime:
    async with database.session_factory() as session:
        return cast(datetime, await session.scalar(select(func.clock_timestamp())))


async def expire_obligation(database: Database, job_id: UUID) -> None:
    async with database.session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE dispatch_outbox SET created_at = clock_timestamp() - interval '3 seconds', "
                "available_at = clock_timestamp() - interval '2 seconds', "
                "lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = :job_id"
            ),
            {"job_id": job_id},
        )


@pytest.mark.asyncio
async def test_job_and_dispatch_obligation_commit_atomically(dispatch_database: Database) -> None:
    job_id = await submit_job(dispatch_database)
    obligation = await read_obligation(dispatch_database, job_id)
    assert obligation.status == "READY"
    assert obligation.publish_attempt_count == 0
    assert obligation.claim_token is None
    async with dispatch_database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        assert await session.scalar(select(func.count()).select_from(DispatchOutbox)) == 1


@pytest.mark.asyncio
async def test_failed_outbox_insert_rolls_back_job_as_well(dispatch_database: Database) -> None:
    async with dispatch_database.session_factory.begin() as session:
        await session.execute(
            text("ALTER TABLE dispatch_outbox ADD CONSTRAINT test_reject_insert CHECK (false)")
        )
    with pytest.raises(IntegrityError):
        await submit_job(dispatch_database)
    async with dispatch_database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(DispatchOutbox)) == 0


@pytest.mark.asyncio
async def test_successful_publication_marks_outbox_and_queues_pending_job(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    job_id = await submit_job(dispatch_database)
    queue = LocalQueue()
    assert await dispatcher.dispatch_once(dispatch_database, queue, dispatch_settings) is True
    assert await queue.receive() == job_id
    obligation = await read_obligation(dispatch_database, job_id)
    assert obligation.status == "PUBLISHED"
    assert obligation.publish_attempt_count == 1
    assert obligation.published_at is not None
    assert obligation.claim_token is None
    assert obligation.lease_expires_at is None
    assert obligation.last_error_code is None
    async with dispatch_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "QUEUED"
        assert job.attempt_count == 0
    assert await dispatcher.dispatch_once(dispatch_database, queue, dispatch_settings) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("exception_type", [RuntimeError, TimeoutError])
async def test_publish_failure_keeps_durable_obligation_with_bounded_backoff(
    dispatch_database: Database,
    dispatch_settings: Settings,
    exception_type: type[Exception],
) -> None:
    class FailingPublisher:
        async def publish(self, job_id: UUID) -> None:
            raise exception_type("secret_transport_marker")

    job_id = await submit_job(dispatch_database)
    settings = dispatch_settings.model_copy(update={"dispatch_retry_max_seconds": 3})
    for attempt, delay in [(1, 1), (2, 2), (3, 3)]:
        before = await database_time(dispatch_database)
        assert await dispatcher.dispatch_once(
            dispatch_database, FailingPublisher(), settings
        ) is True
        after = await database_time(dispatch_database)
        obligation = await read_obligation(dispatch_database, job_id)
        assert obligation.status == "READY"
        assert obligation.publish_attempt_count == attempt
        assert obligation.last_error_code == "publisher_unavailable"
        assert obligation.claim_token is None
        assert obligation.lease_expires_at is None
        assert before + timedelta(seconds=delay) <= obligation.available_at
        assert obligation.available_at <= after + timedelta(seconds=delay)
        assert "secret_transport_marker" not in str(obligation.last_error_code)
        assert await dispatcher.dispatch_once(
            dispatch_database, FailingPublisher(), settings
        ) is False
        async with dispatch_database.session_factory.begin() as session:
            await session.execute(
                text(
                    "UPDATE dispatch_outbox "
                    "SET available_at = clock_timestamp() - interval '1 second' "
                    "WHERE job_id = :job_id"
                ),
                {"job_id": job_id},
            )
    queue = LocalQueue()
    assert await dispatcher.dispatch_once(dispatch_database, queue, settings) is True
    assert await queue.receive() == job_id
    assert (await read_obligation(dispatch_database, job_id)).status == "PUBLISHED"


@pytest.mark.asyncio
async def test_actual_publish_timeout_is_classified_and_remains_replayable(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    class BlockedPublisher:
        async def publish(self, job_id: UUID) -> None:
            await asyncio.Event().wait()

    job_id = await submit_job(dispatch_database)
    settings = dispatch_settings.model_copy(update={"dispatch_publish_timeout_seconds": 0.03})
    assert await dispatcher.dispatch_once(dispatch_database, BlockedPublisher(), settings) is True
    obligation = await read_obligation(dispatch_database, job_id)
    assert obligation.status == "READY"
    assert obligation.last_error_code == "publish_timeout"


@pytest.mark.asyncio
async def test_cancelled_publication_leaves_lease_for_restart_replay(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    started = asyncio.Event()

    class BlockedPublisher:
        async def publish(self, job_id: UUID) -> None:
            started.set()
            await asyncio.Event().wait()

    job_id = await submit_job(dispatch_database)
    publishing = asyncio.create_task(
        dispatcher.dispatch_once(dispatch_database, BlockedPublisher(), dispatch_settings)
    )
    try:
        async with asyncio.timeout(3):
            await started.wait()
        publishing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publishing
    finally:
        if not publishing.done():
            publishing.cancel()
        await asyncio.gather(publishing, return_exceptions=True)
    obligation = await read_obligation(dispatch_database, job_id)
    assert obligation.status == "PUBLISHING"
    assert obligation.claim_token is not None
    assert obligation.last_error_code is None
    await expire_obligation(dispatch_database, job_id)
    queue = LocalQueue()
    assert await dispatcher.dispatch_once(dispatch_database, queue, dispatch_settings) is True
    assert await queue.receive() == job_id


@pytest.mark.asyncio
async def test_crash_after_publish_replays_one_job_id_without_double_execution(
    dispatch_database: Database,
    dispatch_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def crash_before_marking(database: Database, claim: DispatchClaim) -> bool:
        raise RuntimeError("Simulated dispatcher crash after transport acceptance")

    job_id = await submit_job(dispatch_database)
    queue = LocalQueue()
    with monkeypatch.context() as crash:
        crash.setattr(dispatcher, "complete_dispatch", crash_before_marking)
        with pytest.raises(RuntimeError, match="Simulated dispatcher crash"):
            await dispatcher.dispatch_once(dispatch_database, queue, dispatch_settings)
    first_delivery = await queue.receive()
    assert (await read_obligation(dispatch_database, job_id)).status == "PUBLISHING"
    await expire_obligation(dispatch_database, job_id)
    assert await dispatcher.dispatch_once(dispatch_database, queue, dispatch_settings) is True
    second_delivery = await queue.receive()
    assert first_delivery == second_delivery == job_id

    claim = await claim_job(
        dispatch_database, first_delivery, dispatch_settings.worker_lease_seconds
    )
    assert claim is not None
    assert await claim_job(
        dispatch_database, second_delivery, dispatch_settings.worker_lease_seconds
    ) is None
    assert await execute_claim(dispatch_database, claim, dispatch_settings) is True
    async with dispatch_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "SUCCEEDED"
        assert job.attempt_count == 1
        assert job.result_metadata == {"sum": 1}


@pytest.mark.asyncio
async def test_publication_await_holds_no_transaction_or_row_lock(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockedPublisher:
        async def publish(self, job_id: UUID) -> None:
            started.set()
            await release.wait()

    job_id = await submit_job(dispatch_database)
    publishing = asyncio.create_task(
        dispatcher.dispatch_once(dispatch_database, BlockedPublisher(), dispatch_settings)
    )
    try:
        async with asyncio.timeout(3):
            await started.wait()
        async with dispatch_database.session_factory.begin() as session:
            locked_job = await session.scalar(
                text("SELECT id FROM jobs WHERE id = :job_id FOR UPDATE NOWAIT"), {"job_id": job_id}
            )
            assert locked_job == job_id
            locked_obligation = await session.scalar(
                text("SELECT job_id FROM dispatch_outbox WHERE job_id = :job_id FOR UPDATE NOWAIT"),
                {"job_id": job_id},
            )
            assert locked_obligation == job_id
            name = make_url(dispatch_settings.database_url.get_secret_value()).query[
                "application_name"
            ]
            assert await session.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = :name "
                    "AND state = 'idle in transaction'"
                ),
                {"name": name},
            ) == 0
    finally:
        release.set()
        await publishing


@pytest.mark.asyncio
async def test_concurrent_dispatchers_each_hold_distinct_outbox_ownership(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    count = 8
    job_ids = [await submit_job(dispatch_database) for _ in range(count)]
    published: list[UUID] = []
    all_started = asyncio.Event()
    release = asyncio.Event()

    class GatedPublisher:
        async def publish(self, job_id: UUID) -> None:
            published.append(job_id)
            if len(published) == count:
                all_started.set()
            await release.wait()

    publishers = [
        asyncio.create_task(
            dispatcher.dispatch_once(dispatch_database, GatedPublisher(), dispatch_settings)
        )
        for _ in range(count)
    ]
    try:
        async with asyncio.timeout(3):
            await all_started.wait()
        assert sorted(published) == sorted(job_ids)
        async with dispatch_database.session_factory() as session:
            assert await session.scalar(
                select(func.count(func.distinct(DispatchOutbox.claim_token)))
            ) == count
    finally:
        release.set()
        await asyncio.gather(*publishers)
    async with dispatch_database.session_factory() as session:
        assert await session.scalar(
            select(func.count())
            .select_from(DispatchOutbox)
            .where(DispatchOutbox.status == "PUBLISHED")
        ) == count


@pytest.mark.asyncio
async def test_completed_local_jobs_have_explicitly_skipped_obligations(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    job_id = await submit_job(dispatch_database)
    assert await run_once(dispatch_database, dispatch_settings) is True

    class UnexpectedPublisher:
        async def publish(self, job_id: UUID) -> None:
            pytest.fail("A completed local job must not be published")

    assert await dispatcher.dispatch_once(
        dispatch_database, UnexpectedPublisher(), dispatch_settings
    ) is False
    obligation = await read_obligation(dispatch_database, job_id)
    assert obligation.status == "SKIPPED"
    assert obligation.claim_token is None
    assert obligation.lease_expires_at is None
    assert obligation.publish_attempt_count == 0


@pytest.mark.asyncio
async def test_publish_completion_does_not_replace_running_execution_state(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    job_id = await submit_job(dispatch_database)

    class ClaimingPublisher:
        claim: Claim | None = None

        async def publish(self, job_id: UUID) -> None:
            self.claim = await claim_job(
                dispatch_database, job_id, dispatch_settings.worker_lease_seconds
            )
            assert self.claim is not None

    publisher = ClaimingPublisher()
    assert await dispatcher.dispatch_once(dispatch_database, publisher, dispatch_settings) is True
    async with dispatch_database.session_factory() as session:
        job = await get_job(session, job_id)
        assert job is not None
        assert job.status == "RUNNING"
        assert job.attempt_count == 1
    assert (await read_obligation(dispatch_database, job_id)).status == "PUBLISHED"
    assert publisher.claim is not None
    assert await execute_claim(dispatch_database, publisher.claim, dispatch_settings) is True


@pytest.mark.asyncio
async def test_wrong_expired_and_replaced_dispatch_tokens_cannot_finalize(
    dispatch_database: Database, dispatch_settings: Settings
) -> None:
    job_id = await submit_job(dispatch_database)
    claim = await claim_dispatch(dispatch_database, dispatch_settings.dispatch_lease_seconds)
    assert claim is not None
    wrong = replace(claim, token=uuid4())
    assert await complete_dispatch(dispatch_database, wrong) is False
    assert await fail_dispatch(dispatch_database, wrong, delay_seconds=1) is False
    await expire_obligation(dispatch_database, job_id)
    assert await complete_dispatch(dispatch_database, claim) is False
    assert await fail_dispatch(dispatch_database, claim, delay_seconds=1) is False
    replacement = await claim_dispatch(dispatch_database, dispatch_settings.dispatch_lease_seconds)
    assert replacement is not None
    assert replacement.token != claim.token
    assert replacement.attempt_count == claim.attempt_count + 1
    assert await complete_dispatch(dispatch_database, claim) is False
    assert await fail_dispatch(dispatch_database, claim, delay_seconds=1) is False
    assert await complete_dispatch(dispatch_database, replacement) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("finalization", ["complete", "fail"])
async def test_dispatch_finalization_rechecks_clock_after_unchanged_row_lock_wait(
    dispatch_database: Database, finalization: str
) -> None:
    job_id = await submit_job(dispatch_database)
    claim = await claim_dispatch(dispatch_database, lease_seconds=2)
    assert claim is not None
    application_name = f"dispatch_waiter_{uuid4().hex}"
    waiter_url = dispatch_database.engine.url.update_query_dict(
        {"application_name": application_name}
    ).render_as_string(hide_password=False)
    waiter_database = Database(Settings(database_url=waiter_url, _env_file=None))
    finalizing: asyncio.Task[bool] | None = None

    async def finalize() -> bool:
        if finalization == "complete":
            return await complete_dispatch(waiter_database, claim)
        return await fail_dispatch(waiter_database, claim, delay_seconds=1)

    try:
        async with dispatch_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            await holder.execute(
                text("SELECT id FROM dispatch_outbox WHERE id = :outbox_id FOR UPDATE"),
                {"outbox_id": claim.id},
            )
            finalizing = asyncio.create_task(finalize())
            async with asyncio.timeout(5):
                while True:
                    # Fresh transactions keep PostgreSQL activity observations current.
                    async with dispatch_database.session_factory() as observer:
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

            assert await holder.scalar(
                text(
                    "SELECT lease_expires_at > clock_timestamp() "
                    "FROM dispatch_outbox WHERE id = :outbox_id"
                ),
                {"outbox_id": claim.id},
            )
            # Let the actual DB deadline pass without updating the locked tuple.
            # A changed tuple could trigger MVCC predicate rechecking and hide the bug.
            await holder.execute(
                text(
                    "SELECT pg_sleep(GREATEST(0, "
                    "EXTRACT(EPOCH FROM lease_expires_at - clock_timestamp())) + 0.01) "
                    "FROM dispatch_outbox WHERE id = :outbox_id"
                ),
                {"outbox_id": claim.id},
            )
        async with asyncio.timeout(5):
            assert not await finalizing
        obligation = await read_obligation(dispatch_database, job_id)
        assert obligation.status == "PUBLISHING"
        assert obligation.claim_token == claim.token
        assert obligation.publish_attempt_count == 1
        assert obligation.published_at is None
        assert obligation.last_error_code is None
        async with dispatch_database.session_factory() as session:
            job = await get_job(session, job_id)
            assert job is not None
            assert job.status == "PENDING"
            assert job.attempt_count == 0
    finally:
        if finalizing is not None and not finalizing.done():
            finalizing.cancel()
            await asyncio.gather(finalizing, return_exceptions=True)
        await waiter_database.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_seconds", [0, -1, True, 1.5])
async def test_outbox_claim_rejects_invalid_lease_budget_before_query(
    dispatch_database: Database, lease_seconds: object
) -> None:
    with pytest.raises(ValueError):
        await claim_dispatch(dispatch_database, lease_seconds)  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delay", "code"),
    [(-1, "publisher_unavailable"), (float("inf"), "publisher_unavailable"), (1, "")],
)
async def test_failure_marker_rejects_invalid_metadata(
    dispatch_database: Database, delay: float, code: str
) -> None:
    await submit_job(dispatch_database)
    claim = await claim_dispatch(dispatch_database, lease_seconds=30)
    assert claim is not None
    with pytest.raises(ValueError):
        await fail_dispatch(dispatch_database, claim, delay_seconds=delay, error_code=code)
