"""Delayed-admission schema compatibility and rollback proved with PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, DateTime, Engine, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.pool import NullPool


def migration_config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


@pytest.fixture
def scheduling_engine(integration_database_url: str) -> Iterator[Engine]:
    command.upgrade(migration_config(), "0008")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def connection_snapshot(connection: Connection) -> dict[str, list[dict[str, object]]]:
    """Capture immutable records without depending on the current ORM revision."""
    return {
        table: [dict(row) for row in connection.execute(text(query)).mappings()]
        for table, query in {
            "jobs": "SELECT * FROM jobs ORDER BY id",
            "job_attempts": (
                "SELECT * FROM job_attempts ORDER BY job_id, generation, attempt_number"
            ),
            "dispatch_outbox": "SELECT * FROM dispatch_outbox ORDER BY id",
            "alembic_version": "SELECT * FROM alembic_version",
        }.items()
    }


def snapshot(engine: Engine) -> dict[str, list[dict[str, object]]]:
    with engine.connect() as connection:
        return connection_snapshot(connection)


def insert_scheduled_job(engine: Engine) -> UUID:
    job_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO jobs (id, type, payload, status, scheduled_at, "
            "idempotency_key, request_fingerprint) VALUES (:id, 'calculation', "
            "'{}'::jsonb, 'SCHEDULED', clock_timestamp() + INTERVAL '1 hour', "
            ":key, :fingerprint)"
        ), {"id": job_id, "key": str(job_id), "fingerprint": "a" * 64})
    return job_id


def assert_scheduling_schema(engine: Engine) -> None:
    with engine.connect() as connection:
        inspector = inspect(connection)
        scheduled_column = next(column for column in inspector.get_columns("jobs")
                                if column["name"] == "scheduled_at")
        assert scheduled_column["nullable"] is True
        assert scheduled_column["default"] is None
        assert isinstance(scheduled_column["type"], DateTime)
        assert scheduled_column["type"].timezone is True
        scheduled_index = next(index for index in inspector.get_indexes("jobs")
                               if index["name"] == "ix_jobs_scheduled_due")
        assert scheduled_index["column_names"] == ["scheduled_at", "id"]
        assert scheduled_index["unique"] is False
        assert "SCHEDULED" in scheduled_index["dialect_options"]["postgresql_where"]
        constraints = {constraint["name"]: constraint["sqltext"]
                       for constraint in inspector.get_check_constraints("jobs")}
        assert "ck_jobs_scheduled_state" in constraints
        assert "SCHEDULED" in constraints["ck_jobs_job_status"]


def test_schedule_upgrade_round_trip_preserves_lifecycles_identity_history_and_obligations(
    integration_database_url: str,
) -> None:
    """Using no scheduling feature allows an exact 0007 round trip, even after recovery."""
    config = migration_config()
    command.upgrade(config, "0007")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    states = ("PENDING", "QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED",
              "DEAD_LETTERED")
    try:
        with engine.begin() as connection:
            for index, state in enumerate(states):
                job_id, token = uuid4(), uuid4()
                generation = index % 2
                count = 0 if state in ("PENDING", "QUEUED") else 1
                connection.execute(text(
                    "INSERT INTO jobs (id, type, payload, status, generation, attempt_count, "
                    "idempotency_key, request_fingerprint, claim_token, started_at, "
                    "finished_at, lease_expires_at, next_attempt_at, result_metadata, "
                    "error_info, dead_lettered_at, dead_letter_reason) VALUES "
                    "(:id, 'calculation', '{\"numbers\":[1,2,3]}'::jsonb, :state, "
                    ":generation, :count, :key, :fingerprint, :token, "
                    "CASE WHEN :count > 0 THEN clock_timestamp() - INTERVAL '1 second' END, "
                    "CASE WHEN :finished THEN clock_timestamp() END, "
                    "CASE WHEN :running THEN clock_timestamp() + INTERVAL '60 seconds' END, "
                    "CASE WHEN :retry THEN clock_timestamp() + INTERVAL '60 seconds' END, "
                    "CAST(:result AS jsonb), CAST(:error AS jsonb), "
                    "CASE WHEN :dead THEN clock_timestamp() END, :reason)"
                ), {"id": job_id, "state": state, "generation": generation, "count": count,
                    "key": state, "fingerprint": "a" * 64,
                    "token": token if state == "RUNNING" else None,
                    "finished": bool(count and state != "RUNNING"),
                    "running": state == "RUNNING", "retry": state == "RETRY_WAIT",
                    "result": '{"sum":6}' if state == "SUCCEEDED" else None,
                    "error": '{"code":"controlled_failure","retryable":true}'
                    if state in ("RETRY_WAIT", "FAILED", "DEAD_LETTERED") else None,
                    "dead": state == "DEAD_LETTERED",
                    "reason": "attempts_exhausted" if state == "DEAD_LETTERED" else None})
                if generation:
                    connection.execute(text(
                        "INSERT INTO job_attempts (job_id, generation, attempt_number, "
                        "claim_token, started_at, finished_at, status, error_info) VALUES "
                        "(:id, 0, 1, :token, clock_timestamp() - INTERVAL '2 seconds', "
                        "clock_timestamp() - INTERVAL '1 second', 'FAILED', CAST(:error AS jsonb))"
                    ), {"id": job_id, "token": uuid4(),
                        "error": '{"code":"worker_lost","retryable":true}'})
                if count:
                    connection.execute(text(
                        "INSERT INTO job_attempts (job_id, generation, attempt_number, "
                        "claim_token, started_at, finished_at, status, result_metadata, "
                        "error_info, next_attempt_at) SELECT id, generation, attempt_count, "
                        ":token, started_at, finished_at, :state, result_metadata, "
                        "error_info, next_attempt_at FROM jobs WHERE id = :id"
                    ), {"id": job_id, "token": token,
                        "state": state if state in ("RUNNING", "SUCCEEDED") else "FAILED"})
                dispatch_state = ("READY", "PUBLISHING", "PUBLISHED", "SKIPPED")[index % 4]
                connection.execute(text(
                    "INSERT INTO dispatch_outbox (id, job_id, generation, status, "
                    "publish_attempt_count, claim_token, lease_expires_at, published_at, "
                    "last_error_code) VALUES (:id, :job_id, :generation, :state, 2, :token, "
                    "CASE WHEN :publishing THEN clock_timestamp() + INTERVAL '60 seconds' END, "
                    "CASE WHEN :published THEN clock_timestamp() END, 'publisher_unavailable')"
                ), {"id": uuid4(), "job_id": job_id, "generation": generation,
                    "state": dispatch_state,
                    "token": uuid4() if dispatch_state == "PUBLISHING" else None,
                    "publishing": dispatch_state == "PUBLISHING",
                    "published": dispatch_state == "PUBLISHED"})
        original = snapshot(engine)
        for _ in range(2):
            command.upgrade(config, "0008")
            upgraded = snapshot(engine)
            assert upgraded.pop("alembic_version") == [{"version_num": "0008"}]
            for job in upgraded["jobs"]:
                assert job.pop("scheduled_at") is None
            assert upgraded == {key: records for key, records in original.items()
                                if key != "alembic_version"}
            assert_scheduling_schema(engine)
            command.check(config)
            command.downgrade(config, "0007")
            assert snapshot(engine) == original
            with engine.connect() as connection:
                inspector = inspect(connection)
                assert "scheduled_at" not in {column["name"]
                                              for column in inspector.get_columns("jobs")}
                assert "ix_jobs_scheduled_due" not in {index["name"]
                                                     for index in inspector.get_indexes("jobs")}
                assert "ck_jobs_scheduled_state" not in {
                    constraint["name"] for constraint in inspector.get_check_constraints("jobs")
                }
    finally:
        engine.dispose()


@pytest.mark.parametrize("changes", [
    "scheduled_at = NULL",
    "attempt_count = 1",
    "generation = 1",
    "started_at = clock_timestamp()",
    "finished_at = clock_timestamp()",
    "result_metadata = '{}'::jsonb",
    "error_info = '{}'::jsonb",
])
def test_scheduled_state_requires_initial_untouched_admission(
    scheduling_engine: Engine, changes: str,
) -> None:
    insert_scheduled_job(scheduling_engine)
    original = snapshot(scheduling_engine)
    with pytest.raises(IntegrityError) as failure:
        with scheduling_engine.begin() as connection:
            connection.execute(text(f"UPDATE jobs SET {changes}"))
    assert failure.value.orig.diag.constraint_name == "ck_jobs_scheduled_state"
    assert snapshot(scheduling_engine) == original


def test_scheduled_state_cannot_hold_execution_ownership_or_retry_timing(
    scheduling_engine: Engine,
) -> None:
    insert_scheduled_job(scheduling_engine)
    original = snapshot(scheduling_engine)
    for changes, constraint in (
        ("claim_token = :token, lease_expires_at = clock_timestamp() + INTERVAL '1 second'",
         "ck_jobs_running_ownership"),
        ("next_attempt_at = clock_timestamp() + INTERVAL '1 second'",
         "ck_jobs_retry_wait_schedule"),
        ("dead_lettered_at = clock_timestamp(), dead_letter_reason = 'attempts_exhausted'",
         "ck_jobs_dead_letter_context"),
    ):
        with pytest.raises(IntegrityError) as failure:
            with scheduling_engine.begin() as connection:
                connection.execute(text(f"UPDATE jobs SET {changes}"), {"token": uuid4()})
        assert failure.value.orig.diag.constraint_name == constraint
        assert snapshot(scheduling_engine) == original


@pytest.mark.parametrize("state", ["SCHEDULED", "PENDING", "SUCCEEDED", "FAILED",
                                  "DEAD_LETTERED"])
def test_downgrade_refuses_original_schedule_even_after_admission_or_terminal_outcome(
    scheduling_engine: Engine, state: str,
) -> None:
    job_id = insert_scheduled_job(scheduling_engine)
    with scheduling_engine.begin() as connection:
        connection.execute(text("UPDATE jobs SET status = :state WHERE id = :id"),
                           {"id": job_id, "state": state})
    original = snapshot(scheduling_engine)
    with pytest.raises(DBAPIError) as failure:
        command.downgrade(migration_config(), "0007")
    assert failure.value.orig.sqlstate == "P0001"
    assert "Cannot downgrade 0008: scheduled submissions exist" in str(failure.value.orig)
    assert snapshot(scheduling_engine) == original
    assert_scheduling_schema(scheduling_engine)


def test_downgrade_locks_before_guard_and_observes_concurrently_committed_submission(
    scheduling_engine: Engine,
) -> None:
    """A held submission must commit before downgrade may inspect its metadata."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        with scheduling_engine.connect() as writer:
            transaction = writer.begin()
            try:
                writer_pid = writer.scalar(text("SELECT pg_backend_pid()"))
                writer.execute(text(
                    "INSERT INTO jobs (id, type, payload, status, scheduled_at, "
                    "idempotency_key, request_fingerprint) VALUES (:id, 'calculation', "
                    "'{}'::jsonb, 'SCHEDULED', clock_timestamp() + INTERVAL '1 hour', "
                    "'concurrent-schedule', :fingerprint)"
                ), {"id": uuid4(), "fingerprint": "b" * 64})
                expected_committed = connection_snapshot(writer)
                downgrade = executor.submit(command.downgrade, migration_config(), "0007")
                deadline = monotonic() + 5
                waiting_pid = None
                with scheduling_engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as observer:
                    while monotonic() < deadline:
                        waiting_pid = observer.scalar(text(
                            "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
                            "AND :writer_pid = ANY(pg_blocking_pids(pid)) "
                            "AND query LIKE 'LOCK TABLE jobs,%'"
                        ), {"writer_pid": writer_pid})
                        if waiting_pid is not None:
                            break
                        sleep(0.01)
                assert waiting_pid is not None, (
                    "Downgrade did not wait on the submission table lock"
                )
                assert not downgrade.done()
                transaction.commit()
                with pytest.raises(DBAPIError) as failure:
                    downgrade.result(timeout=5)
                assert failure.value.orig.sqlstate == "P0001"
                assert "scheduled submissions exist" in str(failure.value.orig)
            finally:
                if transaction.is_active:
                    transaction.rollback()
    assert snapshot(scheduling_engine) == expected_committed
    assert_scheduling_schema(scheduling_engine)
