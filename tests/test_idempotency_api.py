"""Submission headers are documented and rejected without database side effects."""

from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from distributed_cloud_scheduler import api
from distributed_cloud_scheduler.config import Settings


def app_settings() -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test:dummy@127.0.0.1:1/scheduler_test",
        _env_file=None,
    )


def test_openapi_exposes_optional_key_and_replay_conflict_responses() -> None:
    operation = api.create_app(app_settings()).openapi()["paths"]["/jobs"]["post"]
    parameter = next(item for item in operation["parameters"] if item["name"] == "Idempotency-Key")
    assert parameter["in"] == "header"
    assert parameter["required"] is False
    options = parameter["schema"]["anyOf"]
    key_schema = next(item for item in options if item["type"] == "string")
    assert key_schema["minLength"] == 1
    assert key_schema["maxLength"] == 128
    assert set(operation["responses"]) >= {"200", "201", "409", "422", "503"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [("Idempotency-Key", "")],
        [("Idempotency-Key", "secret_marker,second")],
        [("Idempotency-Key", " secret_marker")],
        [("Idempotency-Key", "secret_marker ")],
        [("Idempotency-Key", "secret_marker/second")],
        [("Idempotency-Key", "secret_marker" + "x" * 128)],
        [("Idempotency-Key", "secret_marker"), ("Idempotency-Key", "secret_marker")],
        [("Idempotency-Key", "secret_marker"), ("idempotency-key", "second")],
        [(b"Idempotency-Key", b"secret_marker\xff")],
        [(b"Idempotency-Key", b"secret_marker\x00")],
        [(b"Idempotency-Key", b"secret_marker\n")],
    ],
)
async def test_invalid_or_repeated_key_is_sanitized_without_submission(
    monkeypatch: pytest.MonkeyPatch, headers: list[tuple[str, str]]
) -> None:
    submit = AsyncMock()
    monkeypatch.setattr(api, "create_or_replay_job", submit)
    app = api.create_app(app_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/jobs", headers=headers, json={"type": "calculation", "payload": {}}
            )
    assert response.status_code == 422
    assert "secret_marker" not in response.text
    submit.assert_not_awaited()
