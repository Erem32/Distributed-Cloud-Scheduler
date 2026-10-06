from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database


@pytest.fixture
def migrated_job_database_url(integration_database_url: str) -> str:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    return integration_database_url


@pytest_asyncio.fixture
async def jobs_client(migrated_job_database_url: str) -> AsyncIterator[AsyncClient]:
    settings = Settings(database_url=migrated_job_database_url, _env_file=None)
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_submission_is_durable_pending_and_can_be_retrieved(
    jobs_client: AsyncClient,
    migrated_job_database_url: str,
) -> None:
    payload = {"numbers": [1, 2, 3], "nested": {"message": "Zażółć", "flag": True, "empty": None}}
    response = await jobs_client.post("/jobs", json={"type": "calculation", "payload": payload})
    assert response.status_code == 201
    job = response.json()
    UUID(job["id"])
    assert job["type"] == "calculation"
    assert job["payload"] == payload
    assert job["status"] == "PENDING"
    assert job["attempt_count"] == 0
    assert job["max_attempts"] == 3
    assert job["started_at"] is None
    assert job["finished_at"] is None
    assert datetime.fromisoformat(job["created_at"]).utcoffset() == UTC.utcoffset(None)

    retrieved = await jobs_client.get(f"/jobs/{job['id']}")
    assert retrieved.status_code == 200
    assert retrieved.json() == job

    database = Database(Settings(database_url=migrated_job_database_url, _env_file=None))
    try:
        async with database.session_factory() as session:
            row = (
                await session.execute(
                    text("SELECT status, attempt_count, payload FROM jobs WHERE id = :job_id"),
                    {"job_id": UUID(job["id"])},
                )
            ).one()
            assert row.status == "PENDING"
            assert row.attempt_count == 0
            assert row.payload == payload
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_empty_list_missing_job_and_invalid_uuid(jobs_client: AsyncClient) -> None:
    response = await jobs_client.get("/jobs")
    assert response.status_code == 200
    assert response.json() == {"items": [], "limit": 20, "offset": 0}
    assert (await jobs_client.get(f"/jobs/{uuid4()}")).status_code == 404
    assert (await jobs_client.get("/jobs/not-a-uuid")).status_code == 422


@pytest.mark.asyncio
async def test_pagination_orders_creation_time_then_uuid_with_stable_ties(
    jobs_client: AsyncClient,
    migrated_job_database_url: str,
) -> None:
    ids = []
    for index in range(4):
        response = await jobs_client.post(
            "/jobs", json={"type": "calculation", "payload": {"index": index}, "max_attempts": 20}
        )
        assert response.status_code == 201
        ids.append(response.json()["id"])

    database = Database(Settings(database_url=migrated_job_database_url, _env_file=None))
    try:
        async with database.session_factory.begin() as session:
            await session.execute(
                text("UPDATE jobs SET created_at = :created_at"),
                {"created_at": datetime(2026, 1, 1, tzinfo=UTC)},
            )
            await session.execute(
                text("UPDATE jobs SET created_at = :created_at WHERE id = :job_id"),
                {"created_at": datetime(2026, 1, 2, tzinfo=UTC), "job_id": UUID(ids[0])},
            )
    finally:
        await database.dispose()

    expected = [ids[0], *sorted(ids[1:], key=UUID, reverse=True)]
    first = (await jobs_client.get("/jobs", params={"limit": 2})).json()
    second = (await jobs_client.get("/jobs", params={"limit": 2, "offset": 2})).json()
    assert first["limit"] == second["limit"] == 2
    assert first["offset"] == 0
    assert second["offset"] == 2
    assert [item["id"] for item in first["items"] + second["items"]] == expected
    assert (await jobs_client.get("/jobs", params={"limit": 2})).json() == first
    assert (await jobs_client.get("/jobs", params={"offset": 10_000})).json()["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 101}, {"limit": "not-an-integer"}, {"offset": -1}, {"offset": 10_001}],
)
async def test_list_rejects_unbounded_or_invalid_pagination(
    jobs_client: AsyncClient, params: dict[str, object]
) -> None:
    assert (await jobs_client.get("/jobs", params=params)).status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "submission",
    [
        {"type": "calculation", "payload": None},
        {"type": "calculation", "payload": []},
        {"type": "UPPERCASE", "payload": {}},
        {"type": "calculation", "payload": {}, "max_attempts": True},
        {"type": "calculation", "payload": {}, "max_attempts": 0},
        {"type": "calculation", "payload": {}, "status": "SUCCEEDED"},
        {"type": "calculation", "payload": {"data": "é" * 32_763}},
    ],
)
async def test_invalid_submission_returns_422_without_persisting(
    jobs_client: AsyncClient, submission: dict[str, object]
) -> None:
    response = await jobs_client.post("/jobs", json=submission)
    assert response.status_code == 422
    assert response.json()["detail"]
    assert (await jobs_client.get("/jobs")).json()["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        b'{"type":"calculation","payload":{"nested":[NaN]}}',
        b'{"type":"calculation","payload":{"nested":[Infinity]}}',
        b'{"type":"calculation","payload":{"nested":[-Infinity]}}',
        b'{"type":"calculation","payload":{"nested":[1e9999]}}',
        b'{"type":"calculation","payload":{"nested":["\\u0000"]}}',
        b'{"type":"calculation","payload":{"nested":{"\\u0000":"value"}}}',
        b'{"type":"calculation","payload":{"nested":["\\ud800"]}}',
        b'{"type":"calculation","payload":{"\\ud800":"value"}}',
        b'{"type":"calculation","payload":{},"\\ud800":"extra"}',
    ],
)
async def test_non_json_or_jsonb_incompatible_inputs_have_safe_422_responses(
    jobs_client: AsyncClient, content: bytes
) -> None:
    response = await jobs_client.post(
        "/jobs", content=content, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    assert response.json()["detail"]
    assert all(set(error) == {"type", "loc", "msg"} for error in response.json()["detail"])
    assert (await jobs_client.get("/jobs")).json()["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_change",
    [
        "status = 'UNKNOWN'",
        "type = 'UPPERCASE'",
        "payload = NULL",
        "payload = 'null'::jsonb",
        "payload = '[]'::jsonb",
        "max_attempts = 0",
        "max_attempts = 21",
        "attempt_count = -1",
        "attempt_count = max_attempts + 1",
    ],
)
async def test_database_constraints_reject_invalid_values(
    jobs_client: AsyncClient,
    migrated_job_database_url: str,
    invalid_change: str,
) -> None:
    response = await jobs_client.post("/jobs", json={"type": "calculation", "payload": {}})
    assert response.status_code == 201
    job_id = response.json()["id"]
    database = Database(Settings(database_url=migrated_job_database_url, _env_file=None))
    try:
        async with database.session_factory() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    # The SQL fragments above are fixed test inputs, never request data.
                    await session.execute(
                        text(f"UPDATE jobs SET {invalid_change} WHERE id = :job_id"),
                        {"job_id": UUID(job_id)},
                    )
    finally:
        await database.dispose()
    retrieved = (await jobs_client.get(f"/jobs/{job_id}")).json()
    assert retrieved["status"] == "PENDING"
    assert retrieved["attempt_count"] == 0
    assert retrieved["max_attempts"] == 3
    assert retrieved["payload"] == {}


@pytest.mark.asyncio
async def test_job_database_outage_returns_sanitized_503(unused_tcp_port: int) -> None:
    settings = Settings(
        database_url=f"postgresql+psycopg://scheduler:secret_marker@127.0.0.1:{unused_tcp_port}/scheduler",
        database_connect_timeout_seconds=1,
        _env_file=None,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/jobs", json={"type": "calculation", "payload": {}})
            assert response.status_code == 503
            assert response.json() == {"detail": "Database unavailable"}
            assert "secret_marker" not in response.text
            assert (await client.get("/health")).status_code == 200
