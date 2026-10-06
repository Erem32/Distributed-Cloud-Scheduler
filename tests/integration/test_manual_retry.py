"""Dead-letter recovery preserves history and fences commands across PostgreSQL replicas."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler import worker
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.attempts import AttemptRead, JobAttempt
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_job, complete_claim, recover_expired
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobRead, JobStatus, create_job
from distributed_cloud_scheduler.manual_retry import JobNotFound, RetryConflict, retry_job
from distributed_cloud_scheduler.outbox import (
    DispatchOutbox,
    DispatchStatus,
    claim_dispatch,
    complete_dispatch,
    fail_dispatch,
)
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app

FAILURE = {"code": "controlled_failure", "retryable": True}
SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"


@pytest_asyncio.fixture
async def recovery_database(integration_database_url: str) -> AsyncIterator[Database]:
    command.upgrade(Config(str(Path(__file__).resolve().parents[2] / "alembic.ini")), "head")
    database = Database(Settings(database_url=integration_database_url, _env_file=None))
    try:
        yield database
    finally:
        await database.dispose()


def settings_for(database: Database, application_name: str | None = None) -> Settings:
    url = database.engine.url
    if application_name is not None:
        url = url.update_query_dict({"application_name": application_name})
    return Settings(database_url=url.render_as_string(hide_password=False), _env_file=None)


@asynccontextmanager
async def client_for(
    database: Database, application_name: str | None = None, *, push: bool = False
) -> AsyncIterator[AsyncClient]:
    app = (
        create_push_app(
            settings_for(database, application_name),
            PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None),
        )
        if push
        else create_app(settings_for(database, application_name))
    )
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


async def submit(database: Database, *, max_attempts: int = 1) -> UUID:
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


async def history(database: Database, job_id: UUID) -> list[JobAttempt]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.generation, JobAttempt.attempt_number)
            )
        )


async def obligations(database: Database, job_id: UUID) -> list[DispatchOutbox]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(DispatchOutbox)
                .where(DispatchOutbox.job_id == job_id)
                .order_by(DispatchOutbox.generation, DispatchOutbox.created_at, DispatchOutbox.id)
            )
        )


async def dead_letter(database: Database, job_id: UUID) -> None:
    claim = await claim_job(database, job_id, lease_seconds=60)
    assert claim is not None
    assert await complete_claim(database, claim, error=FAILURE)
    assert (await read_job(database, job_id)).status is JobStatus.DEAD_LETTERED


def envelope(job_id: UUID) -> dict[str, object]:
    return {
        "message": {
            "messageId": uuid4().hex,
            "data": base64.b64encode(str(job_id).encode("ascii")).decode("ascii"),
        },
        "subscription": SUBSCRIPTION,
        "deliveryAttempt": 100,
    }


@pytest.mark.asyncio
async def test_manual_retry_clears_current_context_but_retains_submission_and_history(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    original = await read_job(recovery_database, job_id)
    assert original.generation == 0
    assert original.dead_letter_reason == "attempts_exhausted"
    assert original.dead_lettered_at == original.finished_at
    assert original.dead_lettered_at is not None and original.dead_lettered_at.tzinfo is not None
    original_history = [
        AttemptRead.model_validate(row).model_dump()
        for row in await history(recovery_database, job_id)
    ]
    async with client_for(recovery_database) as client:
        response = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        assert response.status_code == 202
        pending = response.json()
        assert pending["status"] == "PENDING"
        assert pending["generation"] == 1 and pending["attempt_count"] == 0
        assert (
            pending["created_at"]
            == JobRead.model_validate(original).model_dump(mode="json")["created_at"]
        )
        assert pending["type"] == original.type and pending["payload"] == original.payload
        assert pending["max_attempts"] == original.max_attempts
        for field in (
            "started_at",
            "finished_at",
            "next_attempt_at",
            "result_metadata",
            "error_info",
            "dead_lettered_at",
            "dead_letter_reason",
        ):
            assert pending[field] is None
        assert "claim_token" not in pending and "idempotency_key" not in pending
        rows = await obligations(recovery_database, job_id)
        assert [row.generation for row in rows] == [0, 1]
        assert rows[1].status is DispatchStatus.READY
        assert rows[1].publish_attempt_count == 0
        async with recovery_database.session_factory() as observer:
            assert await observer.scalar(
                text(
                    "SELECT available_at <= clock_timestamp() FROM dispatch_outbox WHERE id = :id"
                ),
                {"id": rows[1].id},
            )
        assert await worker.run_once(recovery_database, settings_for(recovery_database))
        success = (await client.get(f"/jobs/{job_id}")).json()
        assert success["status"] == "SUCCEEDED"
        assert success["generation"] == 1 and success["attempt_count"] == 1
        assert success["result_metadata"] == {"sum": 6}
        assert success["dead_lettered_at"] is None and success["dead_letter_reason"] is None
        attempts = (await client.get(f"/jobs/{job_id}/attempts")).json()
        assert [(row["generation"], row["attempt_number"], row["status"]) for row in attempts] == [
            (0, 1, "FAILED"),
            (1, 1, "SUCCEEDED"),
        ]
    current_history = await history(recovery_database, job_id)
    assert [AttemptRead.model_validate(row).model_dump() for row in current_history[:1]] == (
        original_history
    )
    assert current_history[0].claim_token != current_history[1].claim_token


@pytest.mark.asyncio
async def test_permanent_failure_can_be_reopened_and_fail_again_without_becoming_dead_lettered(
    recovery_database: Database,
) -> None:
    async with recovery_database.session_factory() as session:
        job = await create_job(
            session, JobCreate(type="unimplemented", payload={}, max_attempts=20)
        )
        job_id = job.id
    async with client_for(recovery_database) as client:
        for generation in range(2):
            assert await worker.run_once(recovery_database, settings_for(recovery_database))
            failed = await read_job(recovery_database, job_id)
            assert failed.status is JobStatus.FAILED
            assert failed.generation == generation and failed.attempt_count == 1
            assert failed.error_info == {"code": "unknown_task", "retryable": False}
            assert failed.dead_lettered_at is None and failed.dead_letter_reason is None
            if generation == 0:
                reopened = await client.post(
                    f"/jobs/{job_id}/retry", json={"expected_generation": 0}
                )
                assert reopened.status_code == 202
    assert [
        (row.generation, row.attempt_number) for row in await history(recovery_database, job_id)
    ] == [(0, 1), (1, 1)]


@pytest.mark.asyncio
async def test_automatic_retry_in_reopened_generation_resets_budget_and_tags_delayed_dispatch(
    recovery_database: Database,
) -> None:
    async with recovery_database.session_factory() as session:
        job = await create_job(
            session,
            JobCreate(type="controlled_failure", payload={"fail_until_attempt": 1}, max_attempts=3),
        )
        job_id = job.id
    # Three transient infrastructure failures exhaust the first generation independently
    # of the deterministic handler's eventual successful result.
    for number in range(1, 4):
        claim = await claim_job(recovery_database, job_id, lease_seconds=60)
        assert claim is not None and claim.attempt_count == number and claim.generation == 0
        assert await complete_claim(recovery_database, claim, error=FAILURE)
        if number < 3:
            async with recovery_database.session_factory.begin() as session:
                await session.execute(
                    text("UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = :id"),
                    {"id": job_id},
                )
    assert (await read_job(recovery_database, job_id)).status is JobStatus.DEAD_LETTERED
    async with recovery_database.session_factory() as session:
        await retry_job(session, job_id, expected_generation=0)
    assert await worker.run_once(recovery_database, settings_for(recovery_database))
    waiting = await read_job(recovery_database, job_id)
    assert waiting.status is JobStatus.RETRY_WAIT
    assert waiting.generation == 1 and waiting.attempt_count == 1 and waiting.max_attempts == 3
    assert waiting.next_attempt_at is not None and waiting.dead_lettered_at is None
    rows = await obligations(recovery_database, job_id)
    assert [row.generation for row in rows] == [0, 0, 0, 1, 1]
    assert rows[-1].available_at == waiting.next_attempt_at
    attempts = await history(recovery_database, job_id)
    assert [(row.generation, row.attempt_number) for row in attempts] == [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 1),
    ]
    assert attempts[-1].next_attempt_at == waiting.next_attempt_at
    assert await claim_job(recovery_database, job_id, lease_seconds=60) is None
    async with recovery_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = :id"),
            {"id": job_id},
        )
    assert await worker.run_once(recovery_database, settings_for(recovery_database))
    succeeded = await read_job(recovery_database, job_id)
    assert succeeded.status is JobStatus.SUCCEEDED
    assert succeeded.generation == 1 and succeeded.attempt_count == 2
    assert succeeded.result_metadata == {"attempt": 2}
    assert [
        (row.generation, row.attempt_number) for row in await history(recovery_database, job_id)
    ] == [(0, 1), (0, 2), (0, 3), (1, 1), (1, 2)]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_state", ["FAILED", "DEAD_LETTERED"])
async def test_legacy_terminal_job_without_known_history_can_be_manually_recovered(
    recovery_database: Database, legacy_state: str
) -> None:
    job_id = await submit(recovery_database)
    async with recovery_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET status = :status WHERE id = :id"),
            {"status": legacy_state, "id": job_id},
        )
    old = await read_job(recovery_database, job_id)
    assert old.dead_lettered_at is None and old.dead_letter_reason is None
    assert await history(recovery_database, job_id) == []
    async with client_for(recovery_database) as client:
        response = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        assert response.status_code == 202 and response.json()["generation"] == 1
    assert await worker.run_once(recovery_database, settings_for(recovery_database))
    assert (await read_job(recovery_database, job_id)).status is JobStatus.SUCCEEDED
    assert [
        (row.generation, row.attempt_number) for row in await history(recovery_database, job_id)
    ] == [(1, 1)]


@pytest.mark.asyncio
async def test_worker_loss_at_budget_is_dead_lettered_then_recoverable(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    lost = await claim_job(recovery_database, job_id, lease_seconds=60)
    assert lost is not None
    async with recovery_database.session_factory.begin() as session:
        await session.execute(
            text(
                "UPDATE jobs SET started_at = clock_timestamp() - interval '2 seconds', "
                "lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = :id"
            ),
            {"id": job_id},
        )
    assert await recover_expired(recovery_database, job_id=job_id) == 1
    failed = await read_job(recovery_database, job_id)
    assert failed.status is JobStatus.DEAD_LETTERED
    assert failed.dead_lettered_at == failed.finished_at
    assert failed.dead_letter_reason == "attempts_exhausted"
    assert failed.error_info == {"code": "worker_lost", "retryable": True}
    async with recovery_database.session_factory() as session:
        reopened = await retry_job(session, job_id, expected_generation=0)
        assert reopened.generation == 1
    current = await claim_job(recovery_database, job_id, lease_seconds=60)
    assert current is not None and current.generation == 1 and current.attempt_count == 1
    assert not await complete_claim(recovery_database, lost, result={"sum": 999})
    # Even possession of the current token/count cannot bypass generation fencing.
    assert not await complete_claim(recovery_database, replace(current, generation=0), result={})
    assert await worker.execute_claim(recovery_database, current, settings_for(recovery_database))
    assert (await read_job(recovery_database, job_id)).result_metadata == {"sum": 6}


@pytest.mark.asyncio
async def test_concurrent_manual_commands_across_independent_api_engines_have_one_winner(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    barrier = asyncio.Barrier(8)
    async with client_for(recovery_database) as first, client_for(recovery_database) as second:

        async def retry(index: int) -> Response:
            await barrier.wait()
            return await (first if index % 2 else second).post(
                f"/jobs/{job_id}/retry", json={"expected_generation": 0}
            )

        async with asyncio.timeout(10):
            responses = await asyncio.gather(*(retry(index) for index in range(8)))
    assert [response.status_code for response in responses].count(202) == 1
    assert [response.status_code for response in responses].count(409) == 7
    current = await read_job(recovery_database, job_id)
    assert current.generation == 1 and current.status is JobStatus.PENDING
    assert current.attempt_count == 0
    assert [row.generation for row in await obligations(recovery_database, job_id)] == [0, 1]
    assert len(await history(recovery_database, job_id)) == 1


@pytest.mark.asyncio
async def test_stale_command_cannot_reopen_a_later_terminal_generation(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    async with client_for(recovery_database) as client:
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 202
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 409
        await dead_letter(recovery_database, job_id)
        before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 409
        assert (
            JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
        )
        response = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 1})
        assert response.status_code == 202 and response.json()["generation"] == 2
    assert [
        (row.generation, row.attempt_number) for row in await history(recovery_database, job_id)
    ] == [(0, 1), (1, 1)]
    assert [row.generation for row in await obligations(recovery_database, job_id)] == [0, 1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["PENDING", "QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED"])
async def test_manual_retry_rejects_ineligible_states_without_mutation(
    recovery_database: Database, state: str
) -> None:
    job_id = await submit(recovery_database, max_attempts=3)
    if state == "QUEUED":
        publication = await claim_dispatch(recovery_database, lease_seconds=60)
        assert publication is not None and await complete_dispatch(recovery_database, publication)
    elif state in ("RUNNING", "RETRY_WAIT", "SUCCEEDED"):
        claim = await claim_job(recovery_database, job_id, lease_seconds=60)
        assert claim is not None
        if state == "RETRY_WAIT":
            assert await complete_claim(recovery_database, claim, error=FAILURE)
        elif state == "SUCCEEDED":
            assert await complete_claim(recovery_database, claim, result={"sum": 6})
    before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
    async with client_for(recovery_database) as client:
        response = await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
    assert response.status_code == 409
    assert set(response.json()) == {"detail"}
    assert JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
    assert (await read_job(recovery_database, job_id)).status.value == state
    assert len(await obligations(recovery_database, job_id)) == (2 if state == "RETRY_WAIT" else 1)


@pytest.mark.asyncio
async def test_unknown_retry_and_history_return_404_and_domain_errors_release_transaction(
    recovery_database: Database,
) -> None:
    job_id = uuid4()
    async with client_for(recovery_database) as client:
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 404
        assert (await client.get(f"/jobs/{job_id}/attempts")).status_code == 404
    async with recovery_database.session_factory() as session:
        with pytest.raises(JobNotFound):
            await retry_job(session, job_id, expected_generation=0)
        assert not session.in_transaction()
    pending_id = await submit(recovery_database)
    async with recovery_database.session_factory() as session:
        with pytest.raises(RetryConflict):
            await retry_job(session, pending_id, expected_generation=0)
        assert not session.in_transaction()


@pytest.mark.asyncio
async def test_generation_overflow_refuses_retry_without_losing_history(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    async with recovery_database.session_factory.begin() as session:
        await session.execute(
            text("UPDATE jobs SET generation = 2147483647 WHERE id = :id"), {"id": job_id}
        )
    async with client_for(recovery_database) as client:
        response = await client.post(
            f"/jobs/{job_id}/retry", json={"expected_generation": 2147483647}
        )
    assert response.status_code == 409
    assert (await read_job(recovery_database, job_id)).generation == 2147483647
    assert (
        len(await history(recovery_database, job_id))
        == len(await obligations(recovery_database, job_id))
        == 1
    )


@pytest.mark.asyncio
async def test_failed_manual_outbox_insert_rolls_back_reopen_and_preserves_context(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
    async with recovery_database.session_factory.begin() as session:
        await session.execute(
            text(
                "ALTER TABLE dispatch_outbox ADD CONSTRAINT test_reject_reopen "
                "CHECK (generation = 0)"
            )
        )
    async with client_for(recovery_database) as client:
        with pytest.raises(IntegrityError):
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        assert (
            JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
        )
        assert (
            len(await history(recovery_database, job_id))
            == len(await obligations(recovery_database, job_id))
            == 1
        )
        async with recovery_database.session_factory.begin() as session:
            await session.execute(
                text("ALTER TABLE dispatch_outbox DROP CONSTRAINT test_reject_reopen")
            )
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 202


async def wait_for_outbox_insert(
    database: Database, application_name: str, holder_pid: int
) -> None:
    async with asyncio.timeout(5):
        while True:
            async with database.session_factory() as observer:
                blocked = await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE application_name = :name AND :pid = ANY(pg_blocking_pids(pid)) "
                        "AND query LIKE '%INSERT INTO dispatch_outbox%')"
                    ),
                    {"name": application_name, "pid": holder_pid},
                )
            if blocked:
                return


@pytest.mark.asyncio
async def test_cancelled_reopen_waiting_on_outbox_rolls_back_all_state(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
    name = f"manual_retry_cancel_{uuid4().hex}"
    request: asyncio.Task[Response] | None = None
    async with client_for(recovery_database, name) as client:
        async with recovery_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await holder.execute(text("LOCK TABLE dispatch_outbox IN ACCESS EXCLUSIVE MODE"))
            request = asyncio.create_task(
                client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
            )
            try:
                await wait_for_outbox_insert(recovery_database, name, holder_pid)
                assert not request.done()
                request.cancel()
                with pytest.raises(asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await request
            finally:
                if not request.done():
                    request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
        assert (
            JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
        )
        assert (
            len(await history(recovery_database, job_id))
            == len(await obligations(recovery_database, job_id))
            == 1
        )
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 202


@pytest.mark.asyncio
async def test_reopen_does_not_wait_on_old_publisher_or_allow_it_to_queue_new_generation(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    old_publication = await claim_dispatch(recovery_database, lease_seconds=60)
    assert old_publication is not None and old_publication.generation == 0
    await dead_letter(recovery_database, job_id)
    async with client_for(recovery_database) as client:
        async with recovery_database.session_factory.begin() as holder:
            await holder.execute(
                text("SELECT id FROM dispatch_outbox WHERE id = :id FOR UPDATE"),
                {"id": old_publication.id},
            )
            # Avoid job -> old outbox lock inversion against publisher's outbox -> job order.
            async with asyncio.timeout(3):
                reopened = await client.post(
                    f"/jobs/{job_id}/retry", json={"expected_generation": 0}
                )
            assert reopened.status_code == 202
        assert await complete_dispatch(recovery_database, old_publication)
        pending = await read_job(recovery_database, job_id)
        assert pending.status is JobStatus.PENDING and pending.generation == 1
        publication = await claim_dispatch(recovery_database, lease_seconds=60)
        assert publication is not None and publication.generation == 1
        assert not await complete_dispatch(recovery_database, replace(publication, generation=0))
        assert not await fail_dispatch(
            recovery_database, replace(publication, generation=0), delay_seconds=0
        )
        assert await complete_dispatch(recovery_database, publication)
        assert (await read_job(recovery_database, job_id)).status is JobStatus.QUEUED


@pytest.mark.asyncio
async def test_old_ready_dispatch_is_skipped_and_id_only_old_delivery_can_claim_current_generation(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    async with recovery_database.session_factory() as session:
        await retry_job(session, job_id, expected_generation=0)
    publication = await claim_dispatch(recovery_database, lease_seconds=60)
    assert publication is not None and publication.generation == 1
    rows = await obligations(recovery_database, job_id)
    assert rows[0].status is DispatchStatus.SKIPPED
    async with client_for(recovery_database, push=True) as push_client:
        assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
        before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
        assert before["status"] is JobStatus.SUCCEEDED and before["generation"] == 1
        assert (await push_client.post("/pubsub/push", json=envelope(job_id))).status_code == 204
        assert (
            JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
        )
    assert await complete_dispatch(recovery_database, publication)
    assert (await read_job(recovery_database, job_id)).status is JobStatus.SUCCEEDED
    assert len(await history(recovery_database, job_id)) == 2


@pytest.mark.asyncio
async def test_terminal_dead_letter_duplicate_ack_retains_failure_context_and_history(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    await dead_letter(recovery_database, job_id)
    before = JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump()
    attempts = [
        AttemptRead.model_validate(row).model_dump()
        for row in await history(recovery_database, job_id)
    ]
    async with client_for(recovery_database, push=True) as push_client:
        responses = [
            await push_client.post("/pubsub/push", json=envelope(job_id)) for _ in range(4)
        ]
    assert all(response.status_code == 204 for response in responses)
    assert JobRead.model_validate(await read_job(recovery_database, job_id)).model_dump() == before
    assert [
        AttemptRead.model_validate(row).model_dump()
        for row in await history(recovery_database, job_id)
    ] == attempts
    assert len(await obligations(recovery_database, job_id)) == 1


@pytest.mark.asyncio
async def test_keyed_submission_replay_returns_reopened_generation_without_changing_key_content(
    recovery_database: Database,
) -> None:
    submission = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}, "max_attempts": 1}
    headers = {"Idempotency-Key": "manual-recovery-private-marker"}
    async with client_for(recovery_database) as client:
        initial = await client.post("/jobs", json=submission, headers=headers)
        assert initial.status_code == 201
        job_id = UUID(initial.json()["id"])
        await dead_letter(recovery_database, job_id)
        original = await read_job(recovery_database, job_id)
        assert (
            await client.post(f"/jobs/{job_id}/retry", json={"expected_generation": 0})
        ).status_code == 202
    async with client_for(recovery_database) as restarted:
        replay = await restarted.post("/jobs", json=submission, headers=headers)
        assert replay.status_code == 200
        assert replay.json()["id"] == str(job_id)
        assert replay.json()["generation"] == 1 and replay.json()["status"] == "PENDING"
        conflict = await restarted.post(
            "/jobs", json={**submission, "max_attempts": 2}, headers=headers
        )
        assert conflict.status_code == 409
        assert "private-marker" not in conflict.text
    current = await read_job(recovery_database, job_id)
    assert current.idempotency_key == original.idempotency_key
    assert current.request_fingerprint == original.request_fingerprint
    assert len(await obligations(recovery_database, job_id)) == 2


@pytest.mark.asyncio
async def test_history_pages_across_many_generations_without_exposing_ownership(
    recovery_database: Database,
) -> None:
    job_id = await submit(recovery_database)
    for generation in range(25):
        await dead_letter(recovery_database, job_id)
        if generation < 24:
            async with recovery_database.session_factory() as session:
                await retry_job(session, job_id, expected_generation=generation)
    async with client_for(recovery_database) as client:
        default = (await client.get(f"/jobs/{job_id}/attempts")).json()
        assert len(default) == 20
        assert [row["generation"] for row in default] == list(range(20))
        page = (
            await client.get(f"/jobs/{job_id}/attempts", params={"limit": 7, "offset": 5})
        ).json()
        assert [row["generation"] for row in page] == list(range(5, 12))
        tail = (
            await client.get(f"/jobs/{job_id}/attempts", params={"limit": 100, "offset": 20})
        ).json()
        assert [row["generation"] for row in tail] == list(range(20, 25))
        selected = (await client.get(f"/jobs/{job_id}/attempts", params={"generation": 24})).json()
        assert len(selected) == 1 and selected[0]["generation"] == 24
        assert (
            await client.get(f"/jobs/{job_id}/attempts", params={"generation": 25})
        ).json() == []
        assert (await client.get(f"/jobs/{job_id}/attempts", params={"offset": 10000})).json() == []
        assert all(
            row["attempt_number"] == 1 and row["status"] == "FAILED" for row in default + tail
        )
        assert all("claim_token" not in row and "job_id" not in row for row in default + tail)
    assert len(await history(recovery_database, job_id)) == 25


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
        {"offset": 10001},
        {"generation": -1},
        {"generation": "wrong"},
    ],
)
async def test_attempt_history_rejects_invalid_pagination(
    recovery_database: Database, params: dict[str, object]
) -> None:
    job_id = await submit(recovery_database)
    async with client_for(recovery_database) as client:
        assert (await client.get(f"/jobs/{job_id}/attempts", params=params)).status_code == 422
