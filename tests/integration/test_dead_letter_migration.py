"""Generation/dead-letter schema compatibility proved against real PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.pool import NullPool


def migration_config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


@pytest.fixture
def dead_letter_engine(integration_database_url: str) -> Iterator[Engine]:
    command.upgrade(migration_config(), "0007")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def insert_job(engine: Engine, *, state: str = "PENDING") -> UUID:
    job_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO jobs (id, type, payload, status) "
            "VALUES (:id, 'calculation', '{}'::jsonb, :state)"
        ), {"id": job_id, "state": state})
    return job_id


def insert_attempt(engine: Engine, job_id: UUID, *, generation: int = 0) -> None:
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO job_attempts (job_id, generation, attempt_number, claim_token, "
            "started_at, finished_at, status, error_info) VALUES (:id, :generation, 1, "
            ":token, clock_timestamp() - INTERVAL '1 second', clock_timestamp(), 'FAILED', "
            "CAST(:error AS jsonb))"
        ), {"id": job_id, "generation": generation, "token": uuid4(),
            "error": '{"code":"controlled_failure","retryable":true}'})


def snapshot(engine: Engine) -> dict[str, list[dict[str, object]]]:
    """Read stable complete records without depending on the current ORM revision."""
    with engine.connect() as connection:
        return connection_snapshot(connection)


def connection_snapshot(connection: Connection) -> dict[str, list[dict[str, object]]]:
    """Also inspect a writer's uncommitted records to compare after its commit."""
    return {
        table: [dict(row) for row in connection.execute(text(query)).mappings()]
        for table, query in {
            "jobs": "SELECT * FROM jobs ORDER BY id",
            "job_attempts": "SELECT * FROM job_attempts ORDER BY job_id, attempt_number",
            "dispatch_outbox": "SELECT * FROM dispatch_outbox ORDER BY id",
            "alembic_version": "SELECT * FROM alembic_version",
        }.items()
    }


def test_generation_upgrade_round_trip_preserves_every_historical_lifecycle_and_record(
    integration_database_url: str,
) -> None:
    config = migration_config()
    command.upgrade(config, "0006")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    states = ("PENDING", "QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED",
              "DEAD_LETTERED")
    try:
        with engine.begin() as connection:
            for index, state in enumerate(states):
                job_id, token = uuid4(), uuid4()
                count = 1 if state in ("RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED") else 0
                connection.execute(text(
                    "INSERT INTO jobs (id, type, payload, status, attempt_count, "
                    "idempotency_key, request_fingerprint) VALUES (:id, 'calculation', "
                    "'{\"numbers\":[1,2,3]}'::jsonb, :state, :count, :key, :fingerprint)"
                ), {"id": job_id, "state": "PENDING" if state in ("RUNNING", "RETRY_WAIT")
                    else state, "count": count, "key": state, "fingerprint": "a" * 64})
                if state == "RUNNING":
                    connection.execute(text(
                        "UPDATE jobs SET status = 'RUNNING', claim_token = :token, "
                        "started_at = clock_timestamp(), lease_expires_at = "
                        "clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
                    ), {"id": job_id, "token": token})
                elif count:
                    connection.execute(text(
                        "UPDATE jobs SET started_at = clock_timestamp() - INTERVAL '1 second', "
                        "finished_at = clock_timestamp(), "
                        "result_metadata = CAST(:result AS jsonb), "
                        "error_info = CAST(:error AS jsonb) WHERE id = :id"
                    ), {"id": job_id, "result": '{"sum":6}' if state == "SUCCEEDED" else None,
                        "error": None if state == "SUCCEEDED" else
                        '{"code":"controlled_failure","retryable":true}'})
                    if state == "RETRY_WAIT":
                        connection.execute(text(
                            "UPDATE jobs SET status = 'RETRY_WAIT', next_attempt_at = "
                            "clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
                        ), {"id": job_id})
                if count:
                    connection.execute(text(
                        "INSERT INTO job_attempts (job_id, attempt_number, claim_token, "
                        "started_at, "
                        "finished_at, status, result_metadata, error_info, next_attempt_at) "
                        "SELECT id, attempt_count, :token, started_at, finished_at, :status, "
                        "result_metadata, error_info, next_attempt_at FROM jobs WHERE id = :id"
                    ), {"id": job_id, "token": token, "status": state if state in
                        ("RUNNING", "SUCCEEDED") else "FAILED"})
                dispatch_state = ("READY", "PUBLISHING", "PUBLISHED", "SKIPPED")[index % 4]
                connection.execute(text(
                    "INSERT INTO dispatch_outbox (id, job_id, status, publish_attempt_count, "
                    "claim_token, lease_expires_at, published_at, last_error_code) VALUES "
                    "(:id, :job_id, :status, 2, :token, "
                    "CASE WHEN :publishing THEN clock_timestamp() + INTERVAL '60 seconds' "
                    "ELSE NULL END, CASE WHEN :published THEN clock_timestamp() "
                    "ELSE NULL END, 'publisher_unavailable')"
                ), {"id": uuid4(), "job_id": job_id, "status": dispatch_state,
                    "token": uuid4() if dispatch_state == "PUBLISHING" else None,
                    "publishing": dispatch_state == "PUBLISHING",
                    "published": dispatch_state == "PUBLISHED"})
        original = snapshot(engine)
        for _ in range(2):
            command.upgrade(config, "0007")
            upgraded = snapshot(engine)
            assert upgraded.pop("alembic_version") == [{"version_num": "0007"}]
            for table, records in upgraded.items():
                for record in records:
                    assert record.pop("generation") == 0
                    if table == "jobs":
                        assert record.pop("dead_lettered_at") is None
                        assert record.pop("dead_letter_reason") is None
                assert records == original[table]
            with engine.connect() as connection:
                inspector = inspect(connection)
                assert inspector.get_pk_constraint("job_attempts") == {
                    "constrained_columns": ["job_id", "generation", "attempt_number"],
                    "name": "pk_job_attempts", "comment": None,
                    "dialect_options": {"postgresql_include": []},
                }
                for table in ("jobs", "job_attempts", "dispatch_outbox"):
                    generation = next(column for column in inspector.get_columns(table)
                                      if column["name"] == "generation")
                    assert generation["nullable"] is False
                    assert generation["default"] == "0"
                    assert f"ck_{table}_generation_nonnegative" in {
                        constraint["name"] for constraint in inspector.get_check_constraints(table)
                    }
            command.downgrade(config, "0006")
            assert snapshot(engine) == original
    finally:
        engine.dispose()


@pytest.mark.parametrize("table", ["jobs", "job_attempts", "dispatch_outbox"])
def test_downgrade_refuses_any_recovery_generation_without_partial_changes(
    dead_letter_engine: Engine, table: str,
) -> None:
    job_id = insert_job(dead_letter_engine, state="FAILED")
    insert_attempt(dead_letter_engine, job_id)
    with dead_letter_engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO dispatch_outbox (id, job_id) VALUES (:id, :job_id)"
        ), {"id": uuid4(), "job_id": job_id})
        connection.execute(text(f"UPDATE {table} SET generation = 1"))
    original = snapshot(dead_letter_engine)
    with pytest.raises(DBAPIError) as failure:
        command.downgrade(migration_config(), "0006")
    assert failure.value.orig.sqlstate == "P0001"
    assert "Cannot downgrade 0007: manual recovery generations exist" in str(failure.value.orig)
    assert snapshot(dead_letter_engine) == original
    with dead_letter_engine.connect() as connection:
        assert inspect(connection).get_pk_constraint("job_attempts")["constrained_columns"] == [
            "job_id", "generation", "attempt_number"
        ]


def test_downgrade_locks_before_guard_and_observes_concurrently_committed_recovery(
    dead_letter_engine: Engine,
) -> None:
    """A held recovery must commit before the guard can inspect or remove metadata."""
    job_id = insert_job(dead_letter_engine, state="FAILED")
    with ThreadPoolExecutor(max_workers=1) as executor:
        with dead_letter_engine.connect() as writer:
            transaction = writer.begin()
            try:
                writer_pid = writer.scalar(text("SELECT pg_backend_pid()"))
                writer.execute(text("SELECT id FROM jobs WHERE id = :id FOR UPDATE"),
                               {"id": job_id})
                writer.execute(text(
                    "UPDATE jobs SET status = 'PENDING', generation = 1 WHERE id = :id"
                ), {"id": job_id})
                writer.execute(text(
                    "INSERT INTO dispatch_outbox (id, job_id, generation) "
                    "VALUES (:id, :job_id, 1)"
                ), {"id": uuid4(), "job_id": job_id})
                expected_committed = connection_snapshot(writer)
                downgrade = executor.submit(command.downgrade, migration_config(), "0006")
                deadline = monotonic() + 5
                waiting_pid = None
                with dead_letter_engine.connect().execution_options(
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
                assert waiting_pid is not None, "Downgrade did not wait on the recovery table lock"
                assert not downgrade.done()
                transaction.commit()
                with pytest.raises(DBAPIError) as failure:
                    downgrade.result(timeout=5)
                assert failure.value.orig.sqlstate == "P0001"
                assert "manual recovery generations exist" in str(failure.value.orig)
            finally:
                # Release the writer before waiting for the migration thread on any
                # assertion failure, so fixture cleanup cannot hang behind our lock.
                if transaction.is_active:
                    transaction.rollback()
    assert snapshot(dead_letter_engine) == expected_committed
    with dead_letter_engine.connect() as connection:
        assert inspect(connection).get_pk_constraint("job_attempts")["constrained_columns"] == [
            "job_id", "generation", "attempt_number"
        ]


@pytest.mark.parametrize("table", ["jobs", "job_attempts", "dispatch_outbox"])
@pytest.mark.parametrize("generation", [-1, None], ids=["negative", "null"])
def test_generation_constraints_apply_independently_to_jobs_attempts_and_obligations(
    dead_letter_engine: Engine, table: str, generation: int | None,
) -> None:
    job_id = insert_job(dead_letter_engine)
    insert_attempt(dead_letter_engine, job_id)
    with dead_letter_engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO dispatch_outbox (id, job_id) VALUES (:id, :job_id)"
        ), {"id": uuid4(), "job_id": job_id})
    original = snapshot(dead_letter_engine)
    with pytest.raises(IntegrityError) as failure:
        with dead_letter_engine.begin() as connection:
            connection.execute(text(f"UPDATE {table} SET generation = :generation"),
                               {"generation": generation})
    if generation is None:
        assert failure.value.orig.diag.column_name == "generation"
    else:
        assert failure.value.orig.diag.constraint_name == f"ck_{table}_generation_nonnegative"
    assert snapshot(dead_letter_engine) == original


@pytest.mark.parametrize("state, timestamp, reason", [
    ("DEAD_LETTERED", None, "attempts_exhausted"),
    ("DEAD_LETTERED", "now", None),
    ("DEAD_LETTERED", "now", "unknown_reason"),
    ("PENDING", "now", "attempts_exhausted"),
    ("FAILED", "now", "attempts_exhausted"),
    ("SUCCEEDED", "now", "attempts_exhausted"),
])
def test_dead_letter_context_rejects_partial_unknown_or_non_dead_letter_state(
    dead_letter_engine: Engine, state: str, timestamp: str | None, reason: str | None,
) -> None:
    job_id = insert_job(dead_letter_engine)
    original = snapshot(dead_letter_engine)
    with pytest.raises(IntegrityError) as failure:
        with dead_letter_engine.begin() as connection:
            connection.execute(text(
                "UPDATE jobs SET status = :state, dead_lettered_at = "
                "CASE WHEN :timestamp = 'now' THEN clock_timestamp() ELSE NULL END, "
                "dead_letter_reason = :reason WHERE id = :id"
            ), {"id": job_id, "state": state, "timestamp": timestamp, "reason": reason})
    assert failure.value.orig.diag.constraint_name == "ck_jobs_dead_letter_context"
    assert snapshot(dead_letter_engine) == original


def test_explicit_dead_letter_context_and_legacy_unknown_context_remain_distinct(
    dead_letter_engine: Engine,
) -> None:
    explicit = insert_job(dead_letter_engine)
    legacy = insert_job(dead_letter_engine, state="DEAD_LETTERED")
    with dead_letter_engine.begin() as connection:
        connection.execute(text(
            "UPDATE jobs SET status = 'DEAD_LETTERED', dead_lettered_at = clock_timestamp(), "
            "dead_letter_reason = 'attempts_exhausted' WHERE id = :id"
        ), {"id": explicit})
    with dead_letter_engine.connect() as connection:
        records = {row["id"]: row for row in connection.execute(text(
            "SELECT id, status, generation, dead_lettered_at, dead_letter_reason FROM jobs"
        )).mappings()}
        assert records[explicit]["status"] == "DEAD_LETTERED"
        assert records[explicit]["generation"] == 0
        assert records[explicit]["dead_lettered_at"] is not None
        assert records[explicit]["dead_letter_reason"] == "attempts_exhausted"
        assert records[legacy]["status"] == "DEAD_LETTERED"
        assert records[legacy]["dead_lettered_at"] is None
        assert records[legacy]["dead_letter_reason"] is None


def test_attempt_primary_key_preserves_reused_numbers_across_generations_and_fences_duplicates(
    dead_letter_engine: Engine,
) -> None:
    job_id = insert_job(dead_letter_engine)
    insert_attempt(dead_letter_engine, job_id, generation=0)
    insert_attempt(dead_letter_engine, job_id, generation=1)
    with pytest.raises(IntegrityError) as failure:
        insert_attempt(dead_letter_engine, job_id, generation=1)
    assert failure.value.orig.diag.constraint_name == "pk_job_attempts"
    # Retained history/obligations need not match the job's *current* generation.
    with dead_letter_engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO dispatch_outbox (id, job_id, generation) VALUES (:id, :job_id, 2)"
        ), {"id": uuid4(), "job_id": job_id})
    with dead_letter_engine.connect() as connection:
        assert connection.execute(text(
            "SELECT generation, attempt_number FROM job_attempts ORDER BY generation"
        )).all() == [(0, 1), (1, 1)]
        assert connection.scalar(text("SELECT generation FROM jobs")) == 0
        assert connection.scalar(text("SELECT generation FROM dispatch_outbox")) == 2
    with pytest.raises(IntegrityError) as missing_job:
        insert_attempt(dead_letter_engine, uuid4(), generation=1)
    assert missing_job.value.orig.diag.constraint_name == "fk_job_attempts_job_id_jobs"
    with dead_letter_engine.begin() as connection:
        connection.execute(text("DELETE FROM jobs WHERE id = :id"), {"id": job_id})
    with dead_letter_engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM job_attempts")) == 0
        assert connection.scalar(text("SELECT COUNT(*) FROM dispatch_outbox")) == 0
