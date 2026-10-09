from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database


@pytest.mark.asyncio
async def test_real_connection_and_readiness(integration_database_url: str) -> None:
    settings = Settings(database_url=integration_database_url, _env_file=None)
    database = Database(settings)
    try:
        async with database.session_factory() as session:
            assert await session.scalar(text("SELECT 1")) == 1
        await database.check_connection()
    finally:
        await database.dispose()

    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/ready")
            assert response.status_code == 200


def test_migration_round_trip(integration_database_url: str) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert MigrationContext.configure(connection).get_current_revision() == "0009"
            assert set(inspect(connection).get_table_names()) == {
                "alembic_version", "jobs", "dispatch_outbox", "job_attempts"
            }

        command.downgrade(config, "0001")
        with engine.connect() as connection:
            assert MigrationContext.configure(connection).get_current_revision() == "0001"
            assert "jobs" not in inspect(connection).get_table_names()

        command.downgrade(config, "base")
        with engine.connect() as connection:
            assert MigrationContext.configure(connection).get_current_revision() is None

        command.upgrade(config, "head")
        with engine.connect() as connection:
            assert MigrationContext.configure(connection).get_current_revision() == "0009"
    finally:
        engine.dispose()


def test_idempotency_migration_preserves_jobs_and_outbox_through_round_trip(
    integration_database_url: str,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    engine = create_engine(integration_database_url, poolclass=NullPool)
    job_ids = {status: uuid4() for status in ("PENDING", "RUNNING", "FAILED")}
    try:
        command.upgrade(config, "0004")
        with engine.begin() as connection:
            for status, job_id in job_ids.items():
                connection.execute(
                    text(
                        "INSERT INTO jobs (id, type, payload, status) "
                        "VALUES (:id, 'calculation', '{\"numbers\":[1,2,3]}'::jsonb, :status)"
                    ),
                    {"id": job_id, "status": "PENDING" if status == "RUNNING" else status},
                )
                connection.execute(
                    text("INSERT INTO dispatch_outbox (id, job_id) VALUES (:id, :id)"),
                    {"id": job_id},
                )
            connection.execute(
                text(
                    "UPDATE jobs SET status = 'RUNNING', attempt_count = 1, claim_token = :token, "
                    "started_at = clock_timestamp(), "
                    "lease_expires_at = clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
                ),
                {"id": job_ids["RUNNING"], "token": uuid4()},
            )
            connection.execute(
                text(
                    "UPDATE dispatch_outbox SET status = 'PUBLISHED', publish_attempt_count = 2, "
                    "published_at = clock_timestamp() WHERE job_id = :id"
                ),
                {"id": job_ids["RUNNING"]},
            )
            original_jobs = [
                dict(row) for row in connection.execute(text("SELECT * FROM jobs ORDER BY id"))
                .mappings()
            ]
            original_outbox = [
                dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()
            ]

        command.upgrade(config, "0005")
        with engine.connect() as connection:
            migrated_jobs = [
                dict(row) for row in connection.execute(text("SELECT * FROM jobs ORDER BY id"))
                .mappings()
            ]
            for row in migrated_jobs:
                assert row.pop("idempotency_key") is None
                assert row.pop("request_fingerprint") is None
            assert migrated_jobs == original_jobs
            assert [
                dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()
            ] == original_outbox
            assert connection.scalar(text(
                "SELECT coll.collname FROM pg_attribute attr "
                "JOIN pg_collation coll ON coll.oid = attr.attcollation "
                "WHERE attr.attrelid = 'jobs'::regclass AND attr.attname = 'idempotency_key'"
            )) == "C"

        # A keyed record also retains all non-idempotency data when metadata is removed.
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE jobs SET idempotency_key = 'retained:job', "
                    "request_fingerprint = :fingerprint WHERE id = :id"
                ),
                {"fingerprint": "f" * 64, "id": job_ids["FAILED"]},
            )
        command.downgrade(config, "0004")
        with engine.connect() as connection:
            assert [
                dict(row) for row in connection.execute(text("SELECT * FROM jobs ORDER BY id"))
                .mappings()
            ] == original_jobs
            assert [
                dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()
            ] == original_outbox
        command.upgrade(config, "0005")
        with engine.connect() as connection:
            remigrated_jobs = [
                dict(row) for row in connection.execute(text("SELECT * FROM jobs ORDER BY id"))
                .mappings()
            ]
            for row in remigrated_jobs:
                assert row.pop("idempotency_key") is None
                assert row.pop("request_fingerprint") is None
            assert remigrated_jobs == original_jobs
            assert [
                dict(row) for row in connection.execute(
                    text("SELECT * FROM dispatch_outbox ORDER BY id")
                ).mappings()
            ] == original_outbox
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("key", "fingerprint"),
    [
        (None, "a" * 64),
        ("key", None),
        ("", "a" * 64),
        (" ", "a" * 64),
        ("a\n", "a" * 64),
        ("é", "a" * 64),
        ("comma,key", "a" * 64),
        ("key", ""),
        ("key", "a" * 63),
        ("key", "A" * 64),
        ("key", "g" * 64),
        ("key", "a" * 63 + "\n"),
    ],
)
def test_idempotency_database_constraints_reject_invalid_metadata(
    integration_database_url: str, key: str | None, fingerprint: str | None,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    engine = create_engine(integration_database_url, poolclass=NullPool)
    try:
        command.upgrade(config, "head")
        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO jobs "
                        "(id, type, payload, idempotency_key, request_fingerprint) "
                        "VALUES (:id, 'calculation', '{}'::jsonb, :key, :fingerprint)"
                    ),
                    {"id": uuid4(), "key": key, "fingerprint": fingerprint},
                )
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM jobs")) == 0
    finally:
        engine.dispose()


def test_idempotency_database_uniqueness_is_case_sensitive_and_allows_unkeyed_jobs(
    integration_database_url: str,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    engine = create_engine(integration_database_url, poolclass=NullPool)
    insert_job = text(
        "INSERT INTO jobs (id, type, payload, idempotency_key, request_fingerprint) "
        "VALUES (:id, 'calculation', '{}'::jsonb, :key, :fingerprint)"
    )
    try:
        command.upgrade(config, "head")
        with engine.begin() as connection:
            for key in (None, None, "Case", "case", "a" * 128):
                connection.execute(
                    insert_job,
                    {"id": uuid4(), "key": key, "fingerprint": "a" * 64 if key else None},
                )
        with pytest.raises(IntegrityError) as error:
            with engine.begin() as connection:
                connection.execute(
                    insert_job, {"id": uuid4(), "key": "Case", "fingerprint": "b" * 64}
                )
        assert error.value.orig.diag.constraint_name == "uq_jobs_idempotency_key"
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM jobs")) == 5
    finally:
        engine.dispose()


def test_outbox_migration_backfills_only_active_jobs_and_preserves_jobs_on_downgrade(
    integration_database_url: str,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = Config(str(project_root / "alembic.ini"))
    engine = create_engine(integration_database_url, poolclass=NullPool)
    job_ids = {
        status: uuid4()
        for status in ("PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "DEAD_LETTERED")
    }
    expected_dispatch_ids = {job_ids[status] for status in ("PENDING", "QUEUED", "RUNNING")}
    snapshot_query = text("SELECT * FROM jobs ORDER BY id")
    try:
        command.upgrade(config, "0003")
        with engine.begin() as connection:
            for status, job_id in job_ids.items():
                connection.execute(
                    text(
                        "INSERT INTO jobs (id, type, payload, status) "
                        "VALUES (:id, 'calculation', CAST(:payload AS JSONB), :status)"
                    ),
                    {
                        "id": job_id,
                        "payload": '{"numbers":[1,2,3]}',
                        "status": "PENDING" if status == "RUNNING" else status,
                    },
                )
            connection.execute(
                text(
                    "UPDATE jobs SET status = 'RUNNING', attempt_count = 1, claim_token = :token, "
                    "started_at = clock_timestamp(), "
                    "lease_expires_at = clock_timestamp() + INTERVAL '60 seconds' WHERE id = :id"
                ),
                {"id": job_ids["RUNNING"], "token": uuid4()},
            )
            original_jobs = [
                dict(row) for row in connection.execute(snapshot_query).mappings()
            ]

        command.upgrade(config, "0004")
        with engine.connect() as connection:
            dispatches = connection.execute(
                text(
                    "SELECT job_id, status, publish_attempt_count, claim_token, lease_expires_at, "
                    "published_at, available_at <= clock_timestamp() AS due FROM dispatch_outbox"
                )
            ).mappings().all()
            assert len(dispatches) == 3
            assert {row["job_id"] for row in dispatches} == expected_dispatch_ids
            for row in dispatches:
                assert row["status"] == "READY"
                assert row["publish_attempt_count"] == 0
                assert row["claim_token"] is None
                assert row["lease_expires_at"] is None
                assert row["published_at"] is None
                assert row["due"] is True
            assert [
                dict(row) for row in connection.execute(snapshot_query).mappings()
            ] == original_jobs

        command.downgrade(config, "0003")
        with engine.connect() as connection:
            assert MigrationContext.configure(connection).get_current_revision() == "0003"
            assert "dispatch_outbox" not in inspect(connection).get_table_names()
            assert [
                dict(row) for row in connection.execute(snapshot_query).mappings()
            ] == original_jobs

        command.upgrade(config, "0004")
        with engine.connect() as connection:
            assert set(connection.scalars(text("SELECT job_id FROM dispatch_outbox"))) == (
                expected_dispatch_ids
            )
            assert [
                dict(row) for row in connection.execute(snapshot_query).mappings()
            ] == original_jobs
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("test_url", "application_url", "message"),
    [
        (
            "postgresql+psycopg://localhost/scheduler",
            "postgresql+psycopg://localhost/scheduler",
            "dedicated database ending in _test",
        ),
        (
            "postgresql+psycopg://127.0.0.1/scheduler_test",
            "postgresql+psycopg://localhost/scheduler_test",
            "different database from DATABASE_URL",
        ),
        (
            "sqlite:///scheduler_test",
            "postgresql+psycopg://localhost/scheduler",
            "postgresql\\+psycopg driver",
        ),
    ],
)
def test_fixture_rejects_unsafe_database_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    test_url: str,
    application_url: str,
    message: str,
) -> None:
    monkeypatch.setenv("TEST_DATABASE_URL", test_url)
    monkeypatch.setenv("DATABASE_URL", application_url)
    with pytest.raises(pytest.fail.Exception, match=message):
        request.getfixturevalue("integration_database_url")


def test_fixture_rejects_application_database_from_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    database_url = "postgresql+psycopg://localhost/scheduler_test"
    (tmp_path / ".env").write_text(f"DATABASE_URL={database_url}\n", encoding="utf-8")
    monkeypatch.setenv("TEST_DATABASE_URL", database_url)
    with pytest.raises(pytest.fail.Exception, match="different database from DATABASE_URL"):
        request.getfixturevalue("integration_database_url")


@pytest.mark.parametrize(
    ("environment_url", "message"),
    [
        (None, "dedicated database ending in _test"),
        ("sqlite:///scheduler_test", "postgresql\\+psycopg driver"),
    ],
)
def test_fixture_reads_dotenv_with_environment_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    environment_url: str | None,
    message: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://localhost/scheduler")
    (tmp_path / ".env").write_text(
        "TEST_DATABASE_URL=postgresql+psycopg://localhost/scheduler\n", encoding="utf-8"
    )
    if environment_url is None:
        monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("TEST_DATABASE_URL", environment_url)
    with pytest.raises(pytest.fail.Exception, match=message):
        request.getfixturevalue("integration_database_url")
