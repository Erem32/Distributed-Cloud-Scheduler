"""Submission identity and transaction races proved against real PostgreSQL."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.dispatcher import dispatch_once
from distributed_cloud_scheduler.idempotency import submission_fingerprint
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobRead
from distributed_cloud_scheduler.messaging import LocalQueue
from distributed_cloud_scheduler.outbox import DispatchOutbox
from distributed_cloud_scheduler.worker import run_once

SUBMISSION = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}}
KEY = "private-key-marker"


@pytest_asyncio.fixture
async def idempotency_database(
    migrated_integration_database_url: str,
) -> AsyncIterator[Database]:
    database = Database(
        Settings(database_url=migrated_integration_database_url, _env_file=None)
    )
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
    database: Database, application_name: str | None = None
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings_for(database, application_name))
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


async def assert_counts(database: Database, expected: int) -> None:
    async with database.session_factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM jobs")) == expected
        assert await session.scalar(text("SELECT count(*) FROM dispatch_outbox")) == expected


def assert_public_job(response: Response) -> None:
    assert set(response.json()) == set(JobRead.model_fields)
    assert "idempotency_key" not in response.json()
    assert "request_fingerprint" not in response.json()


async def wait_until_blocked(
    database: Database, application_name: str, holder_pid: int, *, outbox_insert: bool = False
) -> None:
    async with asyncio.timeout(5):
        while True:
            # Fresh transactions avoid a cached PostgreSQL activity snapshot.
            async with database.session_factory() as observer:
                blocked = await observer.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE application_name = :application_name "
                        "AND :holder_pid = ANY(pg_blocking_pids(pid)) "
                        "AND (NOT :outbox_insert OR query LIKE '%INSERT INTO dispatch_outbox%'))"
                    ),
                    {
                        "application_name": application_name,
                        "holder_pid": holder_pid,
                        "outbox_insert": outbox_insert,
                    },
                )
            if blocked:
                return


@pytest.mark.asyncio
async def test_ordinary_concurrent_requests_remain_independent_without_a_key(
    idempotency_database: Database,
) -> None:
    async with (
        client_for(idempotency_database) as first,
        client_for(idempotency_database) as second,
    ):
        responses = await asyncio.gather(
            *((first if index % 2 else second).post("/jobs", json=SUBMISSION) for index in range(8))
        )
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["id"] for response in responses}) == 8
    await assert_counts(idempotency_database, 8)
    async with idempotency_database.session_factory() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM jobs WHERE idempotency_key IS NULL "
                    "AND request_fingerprint IS NULL"
                )
            )
            == 8
        )


@pytest.mark.asyncio
async def test_nested_order_defaults_and_unicode_spelling_replay_the_same_job(
    idempotency_database: Database,
) -> None:
    async with client_for(idempotency_database) as client:
        initial = await client.post(
            "/jobs",
            content=b'{"type":"calculation","payload":{"b":2,"a":{"z":"\\u00e9","x":true}}}',
            headers={"Content-Type": "application/json", "Idempotency-Key": KEY},
        )
        replay = await client.post(
            "/jobs",
            json={
                "max_attempts": 3,
                "payload": {"a": {"x": True, "z": "é"}, "b": 2},
                "type": "calculation",
            },
            headers={"Idempotency-Key": KEY},
        )
    assert initial.status_code == 201
    assert replay.status_code == 200
    assert initial.json() == replay.json()
    assert_public_job(initial)
    assert_public_job(replay)
    assert initial.json()["attempt_count"] == 0
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("original", "conflicting"),
    [
        (SUBMISSION, {"type": "sleep", "payload": {"numbers": [1, 2, 3]}}),
        (SUBMISSION, {"type": "calculation", "payload": {"numbers": [3, 2, 1]}}),
        (SUBMISSION, {**SUBMISSION, "max_attempts": 4}),
        (SUBMISSION, {"type": "calculation", "payload": {"numbers": [1, 2, 3], "extra": None}}),
        (
            {"type": "calculation", "payload": {"value": 1}},
            {"type": "calculation", "payload": {"value": 1.0}},
        ),
        (
            {"type": "calculation", "payload": {"value": 0.0}},
            {"type": "calculation", "payload": {"value": -0.0}},
        ),
        (
            {"type": "calculation", "payload": {"value": True}},
            {"type": "calculation", "payload": {"value": 1}},
        ),
        (
            {"type": "calculation", "payload": {"value": "é"}},
            {"type": "calculation", "payload": {"value": "e\u0301"}},
        ),
    ],
)
async def test_conflicting_content_is_rejected_without_changing_the_original(
    idempotency_database: Database, original: dict[str, object], conflicting: dict[str, object]
) -> None:
    async with client_for(idempotency_database) as client:
        initial = await client.post("/jobs", json=original, headers={"Idempotency-Key": KEY})
        conflict = await client.post("/jobs", json=conflicting, headers={"Idempotency-Key": KEY})
        assert initial.status_code == 201
        assert conflict.status_code == 409
        assert set(conflict.json()) == {"detail"}
        assert KEY not in conflict.text
        retrieved = await client.get(f"/jobs/{initial.json()['id']}")
        assert retrieved.json() == initial.json()
        replay = await client.post("/jobs", json=original, headers={"Idempotency-Key": KEY})
        assert replay.status_code == 200
        assert replay.json() == initial.json()
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
async def test_keys_are_exact_case_sensitive_and_accept_the_maximum_length(
    idempotency_database: Database,
) -> None:
    keys = ["Case.Key:_-1", "case.Key:_-1", "a" * 128]
    async with client_for(idempotency_database) as client:
        created = [
            await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": key})
            for key in keys
        ]
        replayed = [
            await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": key})
            for key in keys
        ]
    assert all(response.status_code == 201 for response in created)
    assert all(response.status_code == 200 for response in replayed)
    assert len({response.json()["id"] for response in created}) == 3
    assert [response.json() for response in created] == [response.json() for response in replayed]
    await assert_counts(idempotency_database, 3)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key",
    [
        b"",
        b" leading",
        b"trailing ",
        b"two keys",
        b"one,two",
        b"x" * 129,
        b"private\tmarker",
        b"private\x00marker",
        b'"quoted"',
        b"caf\xe9",
    ],
)
async def test_invalid_key_headers_create_nothing_and_do_not_echo_inputs(
    idempotency_database: Database, key: bytes
) -> None:
    async with client_for(idempotency_database) as client:
        response = await client.post("/jobs", json=SUBMISSION, headers=[(b"Idempotency-Key", key)])
    assert response.status_code == 422
    assert response.json()["detail"]
    assert all(set(error) == {"type", "loc", "msg"} for error in response.json()["detail"])
    assert "private" not in response.text
    await assert_counts(idempotency_database, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("second_key", [KEY, "different-private-marker"])
async def test_repeated_key_headers_are_rejected_even_when_values_match(
    idempotency_database: Database, second_key: str
) -> None:
    async with client_for(idempotency_database) as client:
        response = await client.post(
            "/jobs",
            json=SUBMISSION,
            headers=[("Idempotency-Key", KEY), ("idempotency-key", second_key)],
        )
    assert response.status_code == 422
    assert KEY not in response.text
    assert second_key not in response.text
    await assert_counts(idempotency_database, 0)


@pytest.mark.asyncio
async def test_invalid_submission_does_not_reserve_a_valid_key(
    idempotency_database: Database,
) -> None:
    async with client_for(idempotency_database) as client:
        invalid = await client.post(
            "/jobs", json={**SUBMISSION, "max_attempts": 0}, headers={"Idempotency-Key": KEY}
        )
        assert invalid.status_code == 422
        await assert_counts(idempotency_database, 0)
        corrected = await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": KEY})
        assert corrected.status_code == 201
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("different_content", [False, True])
async def test_simultaneous_submissions_across_independent_apps_have_one_winner(
    idempotency_database: Database, different_content: bool
) -> None:
    barrier = asyncio.Barrier(8)
    async with (
        client_for(idempotency_database) as first,
        client_for(idempotency_database) as second,
    ):

        async def submit(index: int) -> Response:
            await barrier.wait()
            body = (
                {"type": "calculation", "payload": {"numbers": [index]}}
                if different_content
                else SUBMISSION
            )
            return await (first if index % 2 else second).post(
                "/jobs", json=body, headers={"Idempotency-Key": KEY}
            )

        async with asyncio.timeout(10):
            responses = await asyncio.gather(*(submit(index) for index in range(8)))
        winners = [response for response in responses if response.status_code == 201]
        assert len(winners) == 1
        winner = winners[0].json()
        others = [response for response in responses if response.status_code != 201]
        assert all(
            response.status_code == (409 if different_content else 200) for response in others
        )
        if not different_content:
            assert all(response.json() == winner for response in others)
        stored = await first.get(f"/jobs/{winner['id']}")
        assert stored.json() == winner
    assert winner["attempt_count"] == 0
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_winner", [False, True])
@pytest.mark.parametrize("matching", [False, True])
async def test_unique_index_waiter_observes_commit_or_becomes_owner_after_rollback(
    idempotency_database: Database, commit_winner: bool, matching: bool
) -> None:
    application_name = f"idempotency_waiter_{uuid4().hex}"
    original = JobCreate.model_validate(SUBMISSION)
    contender_body = (
        SUBMISSION if matching else {"type": "calculation", "payload": {"numbers": [9]}}
    )
    waiting: asyncio.Task[Response] | None = None
    async with client_for(idempotency_database, application_name) as client:
        async with idempotency_database.session_factory() as holder:
            transaction = await holder.begin()
            try:
                holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(holder_pid, int)
                job = Job(
                    type=original.type,
                    payload=original.payload,
                    max_attempts=original.max_attempts,
                    idempotency_key=KEY,
                    request_fingerprint=submission_fingerprint(original),
                )
                holder.add(job)
                await holder.flush()
                holder.add(DispatchOutbox(job_id=job.id))
                await holder.flush()
                original_id = job.id
                waiting = asyncio.create_task(
                    client.post("/jobs", json=contender_body, headers={"Idempotency-Key": KEY})
                )
                await wait_until_blocked(idempotency_database, application_name, holder_pid)
                assert not waiting.done()
                if commit_winner:
                    await transaction.commit()
                else:
                    await transaction.rollback()
                async with asyncio.timeout(5):
                    response = await waiting
                expected_status = (200 if matching else 409) if commit_winner else 201
                assert response.status_code == expected_status
                if response.status_code != 409:
                    assert (UUID(response.json()["id"]) == original_id) is commit_winner
                    assert response.json()["payload"] == contender_body["payload"]
                    assert response.json()["attempt_count"] == 0
            finally:
                if waiting is not None and not waiting.done():
                    waiting.cancel()
                    await asyncio.gather(waiting, return_exceptions=True)
                if transaction.is_active:
                    await transaction.rollback()
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
async def test_failed_outbox_insert_rolls_back_the_key_and_allows_retry(
    idempotency_database: Database,
) -> None:
    async with idempotency_database.session_factory.begin() as session:
        await session.execute(
            text("ALTER TABLE dispatch_outbox ADD CONSTRAINT test_reject_insert CHECK (false)")
        )
    async with client_for(idempotency_database) as client:
        with pytest.raises(IntegrityError):
            await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": KEY})
        await assert_counts(idempotency_database, 0)
        async with idempotency_database.session_factory.begin() as session:
            await session.execute(
                text("ALTER TABLE dispatch_outbox DROP CONSTRAINT test_reject_insert")
            )
        retried = await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": KEY})
        assert retried.status_code == 201
        assert retried.json()["attempt_count"] == 0
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
async def test_cancellation_after_key_insert_rolls_back_job_and_key_before_retry(
    idempotency_database: Database,
) -> None:
    application_name = f"idempotency_cancel_{uuid4().hex}"
    submitting: asyncio.Task[Response] | None = None
    async with client_for(idempotency_database, application_name) as client:
        async with idempotency_database.session_factory.begin() as holder:
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            assert isinstance(holder_pid, int)
            await holder.execute(text("LOCK TABLE dispatch_outbox IN ACCESS EXCLUSIVE MODE"))
            submitting = asyncio.create_task(
                client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": KEY})
            )
            try:
                # Observe the outbox insert waiting: the keyed job insert has already succeeded.
                await wait_until_blocked(
                    idempotency_database, application_name, holder_pid, outbox_insert=True
                )
                submitting.cancel()
                with pytest.raises(asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await submitting
            finally:
                if not submitting.done():
                    submitting.cancel()
                    await asyncio.gather(submitting, return_exceptions=True)
        await assert_counts(idempotency_database, 0)
        retried = await client.post("/jobs", json=SUBMISSION, headers={"Idempotency-Key": KEY})
        assert retried.status_code == 201
    await assert_counts(idempotency_database, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("submission", "terminal_status"),
    [
        (SUBMISSION, "SUCCEEDED"),
        (
            {"type": "controlled_failure", "payload": {"fail_until_attempt": 3}, "max_attempts": 1},
            "DEAD_LETTERED",
        ),
    ],
)
async def test_replay_after_dispatch_execution_and_app_restart_returns_current_state(
    idempotency_database: Database, submission: dict[str, object], terminal_status: str
) -> None:
    settings = settings_for(idempotency_database)
    async with client_for(idempotency_database) as client:
        initial = await client.post("/jobs", json=submission, headers={"Idempotency-Key": KEY})
        assert initial.status_code == 201
        job_id = UUID(initial.json()["id"])
        queue = LocalQueue()
        assert await dispatch_once(idempotency_database, queue, settings)
        queued = await client.post("/jobs", json=submission, headers={"Idempotency-Key": KEY})
        assert queued.status_code == 200
        assert queued.json()["status"] == "QUEUED"
        assert queued.json()["attempt_count"] == 0
        assert await run_once(idempotency_database, settings)
        terminal = await client.get(f"/jobs/{job_id}")
        assert terminal.json()["status"] == terminal_status
        assert terminal.json()["attempt_count"] == 1
    async with client_for(idempotency_database) as restarted:
        replay = await restarted.post("/jobs", json=submission, headers={"Idempotency-Key": KEY})
        assert replay.status_code == 200
        assert replay.json() == terminal.json()
        assert_public_job(replay)
    await assert_counts(idempotency_database, 1)
    async with idempotency_database.session_factory() as session:
        outbox = (
            await session.execute(text("SELECT status, publish_attempt_count FROM dispatch_outbox"))
        ).one()
        assert outbox.status == "PUBLISHED"
        assert outbox.publish_attempt_count == 1
