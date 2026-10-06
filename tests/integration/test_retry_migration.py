"""Retry/history migration compatibility and constraints proved with PostgreSQL."""

from collections.abc import Iterator
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

from distributed_cloud_scheduler.attempts import AttemptStatus, list_attempts
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import (
    Claim,
    claim_job,
    complete_claim,
    recover_expired,
)
from distributed_cloud_scheduler.jobs import Job, JobStatus
from distributed_cloud_scheduler.retries import RetryPolicy


def migration_config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


@pytest.fixture
def retry_engine(integration_database_url: str) -> Iterator[Engine]:
    command.upgrade(migration_config(), "0006")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def insert_job(engine: Engine) -> UUID:
    job_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO jobs (id, type, payload) VALUES (:id, 'calculation', '{}'::jsonb)"),
            {"id": job_id},
        )
    return job_id


def test_retry_migration_preserves_existing_jobs_and_outbox_and_backfills_only_running(
    integration_database_url: str,
) -> None:
    config = migration_config()
    engine = create_engine(integration_database_url, poolclass=NullPool)
    job_ids = {state: uuid4() for state in (
        "PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "DEAD_LETTERED"
    )}
    token = uuid4()
    try:
        command.upgrade(config, "0005")
        with engine.begin() as connection:
            for state, job_id in job_ids.items():
                connection.execute(
                    text(
                        "INSERT INTO jobs (id, type, payload, status, attempt_count, "
                        "idempotency_key, request_fingerprint) VALUES (:id, 'calculation', "
                        "'{\"numbers\":[1,2,3]}'::jsonb, :state, :count, :key, :fingerprint)"
                    ),
                    {"id": job_id, "state": "PENDING" if state == "RUNNING" else state,
                     "count": 2 if state in ("RUNNING", "SUCCEEDED", "FAILED") else 0,
                     "key": state, "fingerprint": "a" * 64},
                )
                connection.execute(
                    text(
                        "INSERT INTO dispatch_outbox (id, job_id, status, publish_attempt_count, "
                        "published_at) VALUES (:id, :id, 'PUBLISHED', 2, clock_timestamp())"
                    ),
                    {"id": job_id},
                )
            connection.execute(
                text(
                    "UPDATE jobs SET status = 'RUNNING', claim_token = :token, "
                    "started_at = clock_timestamp(), lease_expires_at = "
                    "clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
                ),
                {"id": job_ids["RUNNING"], "token": token},
            )
            connection.execute(
                text(
                    "UPDATE jobs SET started_at = clock_timestamp() - INTERVAL '1 second', "
                    "finished_at = clock_timestamp(), result_metadata = CAST(:result AS jsonb) "
                    "WHERE id = :id"
                ),
                {"id": job_ids["SUCCEEDED"], "result": '{"sum":6}'},
            )
            connection.execute(
                text(
                    "UPDATE jobs SET started_at = clock_timestamp() - INTERVAL '1 second', "
                    "finished_at = clock_timestamp(), "
                    "error_info = CAST(:error AS jsonb) "
                    "WHERE id = :id"
                ),
                {"id": job_ids["FAILED"],
                 "error": '{"code":"controlled_failure","retryable":true}'},
            )
            # The old schema permits zero-count RUNNING rows, although the application
            # never admits them. Preserve that row without inventing attempt number zero.
            connection.execute(
                text(
                    "INSERT INTO jobs (id, type, payload, status, claim_token, started_at, "
                    "lease_expires_at) VALUES (:id, 'calculation', '{}'::jsonb, 'RUNNING', "
                    ":token, clock_timestamp(), clock_timestamp() + INTERVAL '60 seconds')"
                ),
                {"id": uuid4(), "token": uuid4()},
            )
            original_jobs = [dict(row) for row in connection.execute(
                text("SELECT * FROM jobs ORDER BY id")
            ).mappings()]
            original_outbox = [dict(row) for row in connection.execute(
                text("SELECT * FROM dispatch_outbox ORDER BY id")
            ).mappings()]

        for _ in range(2):
            command.upgrade(config, "0006")
            with engine.connect() as connection:
                migrated_jobs = [dict(row) for row in connection.execute(
                    text("SELECT * FROM jobs ORDER BY id")
                ).mappings()]
                for row in migrated_jobs:
                    assert row.pop("next_attempt_at") is None
                assert migrated_jobs == original_jobs
                assert [dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()] == original_outbox
                attempts = [dict(row) for row in connection.execute(
                    text("SELECT * FROM job_attempts")
                ).mappings()]
                running = next(row for row in original_jobs if row["id"] == job_ids["RUNNING"])
                assert attempts == [{
                    "job_id": running["id"], "attempt_number": 2, "claim_token": token,
                    "started_at": running["started_at"], "finished_at": None,
                    "status": "RUNNING", "result_metadata": None, "error_info": None,
                    "next_attempt_at": None,
                }]
                assert all(attempt["attempt_number"] > 0 for attempt in attempts)
                assert "ix_jobs_retry_due" in {
                    index["name"] for index in inspect(connection).get_indexes("jobs")
                }

            command.downgrade(config, "0005")
            with engine.connect() as connection:
                assert "job_attempts" not in inspect(connection).get_table_names()
                assert [dict(row) for row in connection.execute(
                    text("SELECT * FROM jobs ORDER BY id")
                ).mappings()] == original_jobs
                assert [dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()] == original_outbox
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True], ids=["live_completion", "expired_recovery"])
async def test_migrated_running_owner_can_finish_or_recover_without_fabricated_history(
    integration_database_url: str, expired: bool,
) -> None:
    config = migration_config()
    engine = create_engine(integration_database_url, poolclass=NullPool)
    job_id, token = uuid4(), uuid4()
    try:
        command.upgrade(config, "0005")
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO jobs (id, type, payload, status, attempt_count, max_attempts, "
                "claim_token, started_at, lease_expires_at) VALUES (:id, 'calculation', "
                "'{\"numbers\":[1,2,3]}'::jsonb, 'RUNNING', 2, 3, :token, "
                "clock_timestamp() - INTERVAL '2 seconds', "
                "clock_timestamp() + CAST(:lease AS interval))"
            ), {"id": job_id, "token": token, "lease": "-1 second" if expired else "60 seconds"})
            connection.execute(text(
                "INSERT INTO dispatch_outbox (id, job_id, status, published_at) "
                "VALUES (:id, :job_id, 'PUBLISHED', clock_timestamp())"
            ), {"id": uuid4(), "job_id": job_id})
        # Current runtime includes generation fields introduced after the backfill.
        command.upgrade(config, "head")
    finally:
        engine.dispose()

    database = Database(Settings(database_url=integration_database_url, _env_file=None))
    claim = Claim(job_id, "calculation", {"numbers": [1, 2, 3]}, 2, token)
    try:
        async with database.session_factory() as session:
            attempts = await list_attempts(session, job_id)
            assert len(attempts) == 1
            assert attempts[0].attempt_number == 2
            assert attempts[0].status is AttemptStatus.RUNNING
            assert attempts[0].claim_token == token
        if expired:
            assert not await complete_claim(database, claim, result={"sum": 6})
            assert await recover_expired(database, retry_policy=RetryPolicy(1, 1)) == 1
            async with database.session_factory() as session:
                job = await session.get(Job, job_id)
                assert job is not None
                assert job.status is JobStatus.RETRY_WAIT
                assert job.attempt_count == 2
                assert job.next_attempt_at is not None
                attempts = await list_attempts(session, job_id)
                assert attempts[0].status is AttemptStatus.FAILED
                assert attempts[0].error_info == {"code": "worker_lost", "retryable": True}
                assert attempts[0].next_attempt_at == job.next_attempt_at
                assert await session.scalar(text("SELECT COUNT(*) FROM dispatch_outbox")) == 2
            assert await claim_job(database, job_id, lease_seconds=60) is None
            # Wait for the actual persisted PostgreSQL due time rather than inventing a
            # past deadline or assuming a wall-clock delay on the test process.
            async with database.session_factory() as session:
                await session.execute(text(
                    "SELECT pg_sleep(GREATEST(0, "
                    "EXTRACT(EPOCH FROM next_attempt_at - clock_timestamp())) + 0.01) "
                    "FROM jobs WHERE id = :id"
                ), {"id": job_id})
            retry = await claim_job(database, job_id, lease_seconds=60)
            assert retry is not None
            assert retry.attempt_count == 3
            assert await complete_claim(database, retry, result={"sum": 6})
        else:
            assert await complete_claim(database, claim, result={"sum": 6})
        assert not await complete_claim(database, claim, result={"sum": 999})
        async with database.session_factory() as session:
            job = await session.get(Job, job_id)
            assert job is not None
            assert job.status is JobStatus.SUCCEEDED
            assert job.result_metadata == {"sum": 6}
            assert job.attempt_count == (3 if expired else 2)
            attempts = await list_attempts(session, job_id)
            assert [attempt.attempt_number for attempt in attempts] == ([2, 3] if expired else [2])
            assert attempts[-1].status is AttemptStatus.SUCCEEDED
            assert attempts[-1].result_metadata == {"sum": 6}
            assert attempts[-1].error_info is None
            assert attempts[-1].next_attempt_at is None
            assert await session.scalar(text("SELECT COUNT(*) FROM dispatch_outbox")) == (
                2 if expired else 1
            )
    finally:
        await database.dispose()


def test_retry_downgrade_stops_waiting_job_and_preserves_outbox_and_last_failure(
    retry_engine: Engine,
) -> None:
    job_id = insert_job(retry_engine)
    with retry_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE jobs SET status = 'RETRY_WAIT', attempt_count = 1, "
                "started_at = clock_timestamp() - INTERVAL '1 second', "
                "error_info = CAST(:error AS jsonb), "
                "next_attempt_at = clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
            ),
            {"id": job_id, "error": '{"code":"controlled_failure","retryable":true}'},
        )
        connection.execute(
            text(
                "INSERT INTO dispatch_outbox (id, job_id, available_at) "
                "SELECT :id, id, next_attempt_at FROM jobs WHERE id = :job_id"
            ),
            {"id": uuid4(), "job_id": job_id},
        )
        original_job = dict(connection.execute(text("SELECT * FROM jobs")).mappings().one())
        original_outbox = dict(connection.execute(
            text("SELECT * FROM dispatch_outbox")
        ).mappings().one())

    command.downgrade(migration_config(), "0005")
    expected_job = original_job.copy()
    expected_job.pop("next_attempt_at")
    expected_job["status"] = "FAILED"
    with retry_engine.connect() as connection:
        assert dict(connection.execute(text("SELECT * FROM jobs")).mappings().one()) == expected_job
        assert dict(connection.execute(
            text("SELECT * FROM dispatch_outbox")
        ).mappings().one()) == original_outbox
    command.upgrade(migration_config(), "0006")
    with retry_engine.connect() as connection:
        upgraded_job = dict(connection.execute(text("SELECT * FROM jobs")).mappings().one())
        assert upgraded_job.pop("next_attempt_at") is None
        assert upgraded_job == expected_job
        assert connection.scalar(text("SELECT COUNT(*) FROM job_attempts")) == 0


@pytest.mark.parametrize("changes", [
    "status = 'RETRY_WAIT'",
    "next_attempt_at = clock_timestamp()",
    "status = 'RETRY_WAIT', next_attempt_at = clock_timestamp()",
    "status = 'RETRY_WAIT', next_attempt_at = clock_timestamp(), attempt_count = max_attempts",
    "status = 'FAILED', next_attempt_at = clock_timestamp(), attempt_count = 1",
])
def test_retry_schedule_constraints_reject_invalid_job_state(
    retry_engine: Engine, changes: str,
) -> None:
    job_id = insert_job(retry_engine)
    with pytest.raises(IntegrityError) as failure:
        with retry_engine.begin() as connection:
            connection.execute(text(f"UPDATE jobs SET {changes} WHERE id = :id"), {"id": job_id})
    assert failure.value.orig.diag.constraint_name == "ck_jobs_retry_wait_schedule"
    with retry_engine.connect() as connection:
        job = connection.execute(text("SELECT status, attempt_count, next_attempt_at FROM jobs"))
        assert job.one() == ("PENDING", 0, None)


@pytest.mark.parametrize("changes", [
    {"number": "0"},
    {"number": "21"},
    {"token": "NULL"},
    {"started": "NULL"},
    {"status": "'BOGUS'"},
    {"finished": "clock_timestamp()"},
    {"result": "'{}'::jsonb"},
    {"error": "'{}'::jsonb"},
    {"next": "clock_timestamp() + INTERVAL '1 second'"},
    {"status": "'SUCCEEDED'", "finished": "clock_timestamp()"},
    {"status": "'SUCCEEDED'", "finished": "clock_timestamp()", "result": "'[]'::jsonb"},
    {"status": "'SUCCEEDED'", "finished": "clock_timestamp()", "result": "'{}'::jsonb",
     "error": "'{}'::jsonb"},
    {"status": "'SUCCEEDED'", "finished": "clock_timestamp()", "result": "'{}'::jsonb",
     "next": "clock_timestamp() + INTERVAL '1 second'"},
    {"status": "'FAILED'", "finished": "clock_timestamp()"},
    {"status": "'FAILED'", "error": "'{}'::jsonb"},
    {"status": "'FAILED'", "finished": "clock_timestamp()", "error": "'[]'::jsonb"},
    {"status": "'FAILED'", "finished": "clock_timestamp()", "error": "'{}'::jsonb",
     "result": "'{}'::jsonb"},
    {"status": "'FAILED'", "finished": "clock_timestamp()", "error": "'{}'::jsonb",
     "next": "clock_timestamp() - INTERVAL '1 second'"},
    {"status": "'FAILED'", "finished": "clock_timestamp() - INTERVAL '2 seconds'",
     "error": "'{}'::jsonb"},
])
def test_attempt_constraints_reject_invalid_number_ownership_and_outcome(
    retry_engine: Engine, changes: dict[str, str],
) -> None:
    job_id = insert_job(retry_engine)
    values = {
        "number": "1", "token": ":token", "started": "clock_timestamp() - INTERVAL '1 second'",
        "finished": "NULL", "status": "'RUNNING'", "result": "NULL", "error": "NULL",
        "next": "NULL",
    } | changes
    with pytest.raises(IntegrityError):
        with retry_engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO job_attempts (job_id, attempt_number, claim_token, started_at, "
                "finished_at, status, result_metadata, error_info, next_attempt_at) "
                f"VALUES (:id, {values['number']}, {values['token']}, {values['started']}, "
                f"{values['finished']}, {values['status']}, {values['result']}, {values['error']}, "
                f"{values['next']})"
            ), {"id": job_id, "token": uuid4()})
    with retry_engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM job_attempts")) == 0


def test_attempt_primary_key_and_foreign_key_keep_history_bound_to_one_admission(
    retry_engine: Engine,
) -> None:
    job_id = insert_job(retry_engine)
    statement = text(
        "INSERT INTO job_attempts (job_id, attempt_number, claim_token, started_at) "
        "VALUES (:id, 1, :token, clock_timestamp())"
    )
    with retry_engine.begin() as connection:
        connection.execute(statement, {"id": job_id, "token": uuid4()})
    with pytest.raises(IntegrityError) as duplicate:
        with retry_engine.begin() as connection:
            connection.execute(statement, {"id": job_id, "token": uuid4()})
    assert duplicate.value.orig.diag.constraint_name == "pk_job_attempts"
    with pytest.raises(IntegrityError) as missing_job:
        with retry_engine.begin() as connection:
            connection.execute(statement, {"id": uuid4(), "token": uuid4()})
    assert missing_job.value.orig.diag.constraint_name == "fk_job_attempts_job_id_jobs"
    with retry_engine.begin() as connection:
        connection.execute(text("DELETE FROM jobs WHERE id = :id"), {"id": job_id})
    with retry_engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM job_attempts")) == 0
