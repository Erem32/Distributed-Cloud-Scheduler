"""Priority identity, readiness backfill and UTC indexes use real PostgreSQL."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, DateTime, Engine, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.pool import NullPool

ORIGIN = datetime(2026, 1, 1, tzinfo=UTC)
RANK_SQL = "((ready_at AT TIME ZONE 'UTC') - priority * INTERVAL '5 seconds')"


def migration_config() -> Config:
    return Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))


@pytest.fixture
def priority_engine(integration_database_url: str) -> Iterator[Engine]:
    command.upgrade(migration_config(), "0009")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def connection_snapshot(connection: Connection) -> dict[str, list[dict[str, object]]]:
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


def insert_legacy_job(
    connection: Connection, state: str, generation: int, scheduled_at: datetime | None,
) -> UUID:
    """Persist a valid legacy lifecycle, including retained ownership/outcomes/history."""
    job_id, token = uuid4(), uuid4()
    count = 0 if state in ("PENDING", "QUEUED", "SCHEDULED") else 1
    started_at = ORIGIN + timedelta(minutes=50) if count else None
    finished_at = ORIGIN + timedelta(minutes=51) if count and state != "RUNNING" else None
    retry_at = ORIGIN + timedelta(hours=2) if state == "RETRY_WAIT" else None
    result = '{"sum":6}' if state == "SUCCEEDED" else None
    error = '{"code":"worker_lost","retryable":true}' if state in (
        "RETRY_WAIT", "FAILED", "DEAD_LETTERED"
    ) else None
    connection.execute(text(
        "INSERT INTO jobs (id, type, payload, status, generation, attempt_count, created_at, "
        "scheduled_at, idempotency_key, request_fingerprint, claim_token, lease_expires_at, "
        "started_at, finished_at, next_attempt_at, result_metadata, error_info, "
        "dead_lettered_at, dead_letter_reason) VALUES (:id, 'calculation', "
        "'{\"numbers\":[1,2,3]}'::jsonb, :state, :generation, :count, :created, :scheduled, "
        ":key, :fingerprint, :token, :lease, :started, :finished, :retry, "
        "CAST(:result AS jsonb), CAST(:error AS jsonb), :dead_at, :dead_reason)"
    ), {"id": job_id, "state": state, "generation": generation, "count": count,
        "created": ORIGIN, "scheduled": scheduled_at, "key": str(job_id),
        "fingerprint": "a" * 64, "token": token if state == "RUNNING" else None,
        "lease": ORIGIN + timedelta(hours=1) if state == "RUNNING" else None,
        "started": started_at, "finished": finished_at, "retry": retry_at,
        "result": result, "error": error,
        "dead_at": finished_at if state == "DEAD_LETTERED" else None,
        "dead_reason": "attempts_exhausted" if state == "DEAD_LETTERED" else None})
    if generation:
        connection.execute(text(
            "INSERT INTO job_attempts (job_id, generation, attempt_number, claim_token, "
            "started_at, finished_at, status, error_info) VALUES (:id, 0, 1, :token, "
            ":started, :finished, 'FAILED', CAST(:error AS jsonb))"
        ), {"id": job_id, "token": uuid4(), "started": ORIGIN,
            "finished": ORIGIN + timedelta(seconds=1),
            "error": '{"code":"worker_lost","retryable":true}'})
    if count:
        connection.execute(text(
            "INSERT INTO job_attempts (job_id, generation, attempt_number, claim_token, "
            "started_at, finished_at, status, result_metadata, error_info, next_attempt_at) "
            "VALUES (:id, :generation, 1, :token, :started, :finished, :state, "
            "CAST(:result AS jsonb), CAST(:error AS jsonb), :retry)"
        ), {"id": job_id, "generation": generation, "token": token, "started": started_at,
            "finished": finished_at, "state": state if state in ("RUNNING", "SUCCEEDED")
            else "FAILED", "result": result, "error": error, "retry": retry_at})
    return job_id


def insert_legacy_obligation(
    connection: Connection, job_id: UUID, generation: int, created_at: datetime, state: str,
) -> None:
    connection.execute(text(
        "INSERT INTO dispatch_outbox (id, job_id, generation, status, available_at, created_at, "
        "publish_attempt_count, claim_token, lease_expires_at, published_at, last_error_code) "
        "VALUES (:id, :job_id, :generation, :state, :available, :created, 3, :token, :lease, "
        ":published, 'publisher_unavailable')"
    ), {"id": uuid4(), "job_id": job_id, "generation": generation, "state": state,
        "available": created_at + timedelta(minutes=1), "created": created_at,
        "token": uuid4() if state == "PUBLISHING" else None,
        "lease": created_at + timedelta(minutes=2) if state == "PUBLISHING" else None,
        "published": created_at + timedelta(seconds=1) if state == "PUBLISHED" else None})


def assert_priority_schema(engine: Engine) -> None:
    with engine.connect() as connection:
        inspector = inspect(connection)
        columns = {column["name"]: column for column in inspector.get_columns("jobs")}
        assert columns["priority"]["nullable"] is False
        assert columns["priority"]["default"] == "0"
        assert columns["ready_at"]["nullable"] is False
        assert columns["ready_at"]["default"] == "now()"
        assert isinstance(columns["ready_at"]["type"], DateTime)
        assert columns["ready_at"]["type"].timezone is True
        assert "ck_jobs_priority_range" in {
            constraint["name"] for constraint in inspector.get_check_constraints("jobs")
        }
        indexes = {index["name"]: index for index in inspector.get_indexes("jobs")}
        assert {"ix_jobs_scheduled_due", "ix_jobs_retry_due", "ix_jobs_running_lease"} <= (
            set(indexes)
        )
        for name, states in (
            ("ix_jobs_scheduled_priority", ("SCHEDULED",)),
            ("ix_jobs_available_priority", ("PENDING", "QUEUED", "RETRY_WAIT")),
        ):
            index = indexes[name]
            assert index["column_names"] == [None, "ready_at", "id"]
            assert index["unique"] is False
            assert "UTC" in index["expressions"][0]
            assert "priority" in index["expressions"][0]
            assert all(state in index["dialect_options"]["postgresql_where"] for state in states)


def test_priority_upgrade_backfills_known_eligibility_and_roundtrips_all_existing_records(
    integration_database_url: str,
) -> None:
    config = migration_config()
    command.upgrade(config, "0008")
    engine = create_engine(integration_database_url, poolclass=NullPool)
    expected_ready: dict[UUID, datetime] = {}
    try:
        with engine.begin() as connection:
            for index, state in enumerate(("PENDING", "QUEUED", "RUNNING", "RETRY_WAIT",
                                           "SUCCEEDED", "FAILED", "DEAD_LETTERED")):
                job_id = insert_legacy_job(connection, state, 0, None)
                expected_ready[job_id] = (
                    ORIGIN + timedelta(hours=2) if state == "RETRY_WAIT" else ORIGIN
                )
                insert_legacy_obligation(
                    connection, job_id, 0, ORIGIN,
                    ("READY", "PUBLISHING", "PUBLISHED", "SKIPPED")[index % 4],
                )
            for date in (ORIGIN - timedelta(days=1), ORIGIN + timedelta(days=1)):
                job_id = insert_legacy_job(connection, "SCHEDULED", 0, date)
                expected_ready[job_id] = max(ORIGIN, date)
            admitted_date = ORIGIN + timedelta(minutes=10)
            job_id = insert_legacy_job(connection, "QUEUED", 0, admitted_date)
            expected_ready[job_id] = admitted_date
            insert_legacy_obligation(
                connection, job_id, 0, ORIGIN + timedelta(minutes=20), "READY"
            )
            for state in ("PENDING", "QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED"):
                job_id = insert_legacy_job(connection, state, 2, admitted_date)
                recovery_at = ORIGIN + timedelta(minutes=40)
                expected_ready[job_id] = (
                    ORIGIN + timedelta(hours=2) if state == "RETRY_WAIT" else recovery_at
                )
                insert_legacy_obligation(connection, job_id, 0, ORIGIN, "PUBLISHED")
                insert_legacy_obligation(connection, job_id, 2, recovery_at, "PUBLISHED")
                insert_legacy_obligation(
                    connection, job_id, 2, recovery_at + timedelta(minutes=5), "READY"
                )
            # Current-generation evidence can be absent in manually inserted legacy
            # data. An old generation's outbox must not fabricate the recovery time.
            job_id = insert_legacy_job(connection, "PENDING", 2, admitted_date)
            expected_ready[job_id] = admitted_date
            insert_legacy_obligation(connection, job_id, 0, ORIGIN, "PUBLISHED")
        original = snapshot(engine)
        for _ in range(2):
            command.upgrade(config, "0009")
            upgraded = snapshot(engine)
            assert upgraded.pop("alembic_version") == [{"version_num": "0009"}]
            for job in upgraded["jobs"]:
                assert job.pop("priority") == 0
                assert job.pop("ready_at") == expected_ready[job["id"]]
            assert upgraded == {key: records for key, records in original.items()
                                if key != "alembic_version"}
            assert_priority_schema(engine)
            command.check(config)
            command.downgrade(config, "0008")
            assert snapshot(engine) == original
            with engine.connect() as connection:
                inspector = inspect(connection)
                assert {"priority", "ready_at"}.isdisjoint(
                    column["name"] for column in inspector.get_columns("jobs")
                )
                assert {"ix_jobs_scheduled_priority", "ix_jobs_available_priority"}.isdisjoint(
                    index["name"] for index in inspector.get_indexes("jobs")
                )
    finally:
        engine.dispose()


def test_priority_and_readiness_defaults_are_database_owned(priority_engine: Engine) -> None:
    with priority_engine.begin() as connection:
        expected_now = connection.scalar(text("SELECT transaction_timestamp()"))
        row = connection.execute(text(
            "INSERT INTO jobs (id, type, payload) VALUES (:id, 'calculation', '{}'::jsonb) "
            "RETURNING priority, ready_at, created_at"
        ), {"id": uuid4()}).one()
        assert row == (0, expected_now, expected_now)


@pytest.mark.parametrize("column,value", [
    ("priority", -1), ("priority", 10), ("priority", None), ("ready_at", None),
])
def test_database_rejects_invalid_priority_or_missing_readiness(
    priority_engine: Engine, column: str, value: object,
) -> None:
    with priority_engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO jobs (id, type, payload) VALUES (:id, 'calculation', '{}'::jsonb)"
        ), {"id": uuid4()})
    original = snapshot(priority_engine)
    with pytest.raises(IntegrityError) as failure:
        with priority_engine.begin() as connection:
            connection.execute(text(f"UPDATE jobs SET {column} = :value"), {"value": value})
    if value is None:
        assert failure.value.orig.diag.column_name == column
    else:
        assert failure.value.orig.diag.constraint_name == "ck_jobs_priority_range"
    assert snapshot(priority_engine) == original


@pytest.mark.parametrize("state,index_name", [
    ("SCHEDULED", "ix_jobs_scheduled_priority"),
    ("PENDING", "ix_jobs_available_priority"),
])
def test_utc_rank_indexes_are_usable_and_order_independently_of_session_timezone(
    priority_engine: Engine, state: str, index_name: str,
) -> None:
    base = datetime(2026, 3, 29, 1, 0, tzinfo=UTC)
    # Eligibility and UUID ties deliberately distinguish every ordering component.
    values = [(1, 0, -46), (2, 9, 0), (3, 1, -10), (4, 2, -5),
              (6, 0, 0), (5, 0, 0)]
    with priority_engine.begin() as connection:
        for number, priority, seconds in values:
            connection.execute(text(
                "INSERT INTO jobs (id, type, payload, status, scheduled_at, ready_at, priority) "
                "VALUES (:id, 'calculation', '{}'::jsonb, :state, :date, :date, :priority)"
            ), {"id": UUID(int=number), "state": state,
                "date": base + timedelta(seconds=seconds), "priority": priority})
    expected = [UUID(int=number) for number in (1, 2, 3, 4, 5, 6)]
    predicate = (
        "status = 'SCHEDULED'" if state == "SCHEDULED"
        else "status IN ('PENDING', 'QUEUED', 'RETRY_WAIT')"
    )
    query = f"SELECT id FROM jobs WHERE {predicate} ORDER BY {RANK_SQL}, ready_at, id LIMIT 10"
    for timezone in ("UTC", "Europe/Warsaw", "Australia/Lord_Howe"):
        with priority_engine.begin() as connection:
            connection.execute(text("SELECT set_config('TimeZone', :zone, true)"),
                               {"zone": timezone})
            connection.execute(text("SET LOCAL enable_seqscan = off"))
            assert list(connection.execute(text(query)).scalars()) == expected
            plan = "\n".join(connection.execute(text(f"EXPLAIN {query}")).scalars())
            assert index_name in plan
    assert_priority_schema(priority_engine)


@pytest.mark.parametrize("state", ["PENDING", "SCHEDULED", "SUCCEEDED", "FAILED",
                                  "DEAD_LETTERED"])
def test_downgrade_refuses_nondefault_submission_identity_including_terminal_states(
    priority_engine: Engine, state: str,
) -> None:
    with priority_engine.begin() as connection:
        scheduled = ORIGIN + timedelta(days=1) if state == "SCHEDULED" else None
        job_id = insert_legacy_job(connection, state, 0, scheduled)
        connection.execute(text("UPDATE jobs SET priority = 9 WHERE id = :id"), {"id": job_id})
    original = snapshot(priority_engine)
    with pytest.raises(DBAPIError) as failure:
        command.downgrade(migration_config(), "0008")
    assert failure.value.orig.sqlstate == "P0001"
    assert "Cannot downgrade 0009: nondefault priorities exist" in str(failure.value.orig)
    assert snapshot(priority_engine) == original
    assert_priority_schema(priority_engine)


def test_default_priority_downgrade_retains_earlier_schedule_identity_guard(
    priority_engine: Engine,
) -> None:
    with priority_engine.begin() as connection:
        insert_legacy_job(connection, "SCHEDULED", 0, ORIGIN + timedelta(days=1))
    command.downgrade(migration_config(), "0008")
    original = snapshot(priority_engine)
    with pytest.raises(DBAPIError) as failure:
        command.downgrade(migration_config(), "0007")
    assert failure.value.orig.sqlstate == "P0001"
    assert "scheduled submissions exist" in str(failure.value.orig)
    assert snapshot(priority_engine) == original


def test_downgrade_locks_before_guard_and_retains_concurrently_committed_priority_submission(
    priority_engine: Engine,
) -> None:
    with ThreadPoolExecutor(max_workers=1) as executor:
        with priority_engine.connect() as writer:
            transaction = writer.begin()
            try:
                writer_pid = writer.scalar(text("SELECT pg_backend_pid()"))
                writer.execute(text(
                    "INSERT INTO jobs (id, type, payload, priority, idempotency_key, "
                    "request_fingerprint) VALUES (:id, 'calculation', '{}'::jsonb, 9, "
                    "'concurrent-priority', :fingerprint)"
                ), {"id": uuid4(), "fingerprint": "b" * 64})
                expected_committed = connection_snapshot(writer)
                downgrade = executor.submit(command.downgrade, migration_config(), "0008")
                deadline = monotonic() + 5
                waiting_pid = None
                with priority_engine.connect().execution_options(
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
                assert waiting_pid is not None, "Downgrade did not wait on priority submission"
                assert not downgrade.done()
                transaction.commit()
                with pytest.raises(DBAPIError) as failure:
                    downgrade.result(timeout=5)
                assert failure.value.orig.sqlstate == "P0001"
                assert "nondefault priorities exist" in str(failure.value.orig)
            finally:
                if transaction.is_active:
                    transaction.rollback()
    assert snapshot(priority_engine) == expected_committed
    assert_priority_schema(priority_engine)
