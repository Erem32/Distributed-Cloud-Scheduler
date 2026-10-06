"""Manual recovery's strict command contract and sanitized failures."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from distributed_cloud_scheduler import api
from distributed_cloud_scheduler.config import Settings


def app_settings() -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test:dummy@127.0.0.1:1/scheduler_test",
        _env_file=None,
    )


def test_openapi_exposes_manual_command_and_bounded_history() -> None:
    schema = api.create_app(app_settings()).openapi()
    operation = schema["paths"]["/jobs/{job_id}/retry"]["post"]
    assert set(operation["responses"]) >= {"202", "404", "409", "422", "503"}
    assert operation["requestBody"]["required"] is True
    body_reference = operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    command = schema["components"]["schemas"][body_reference.rsplit("/", 1)[-1]]
    assert command["additionalProperties"] is False
    assert command["required"] == ["expected_generation"]
    generation = command["properties"]["expected_generation"]
    assert generation["type"] == "integer" and generation["minimum"] == 0
    assert generation["maximum"] == 2147483647
    history = schema["paths"]["/jobs/{job_id}/attempts"]["get"]
    parameters = {parameter["name"]: parameter["schema"] for parameter in history["parameters"]}
    assert parameters["limit"]["minimum"] == 1 and parameters["limit"]["maximum"] == 100
    assert parameters["offset"]["minimum"] == 0 and parameters["offset"]["maximum"] == 10000
    assert "generation" in parameters


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"expected_generation": None},
        {"expected_generation": True},
        {"expected_generation": False},
        {"expected_generation": "0"},
        {"expected_generation": 0.0},
        {"expected_generation": -1},
        {"expected_generation": 2147483648},
        {"expected_generation": 0, "private_marker": "secret_marker"},
        {"expected_generation": 0, "payload": {"private_marker": "secret_marker"}},
    ],
)
async def test_invalid_manual_command_has_no_recovery_side_effects(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    retry = AsyncMock()
    monkeypatch.setattr(api, "retry_job", retry)
    app = api.create_app(app_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/jobs/{uuid4()}/retry", json=body)
    assert response.status_code == 422
    assert "secret_marker" not in response.text
    assert all(set(error) == {"type", "loc", "msg"} for error in response.json()["detail"])
    retry.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("SELECT secret_marker", {}, Exception("secret_marker")),
        SQLAlchemyTimeoutError("secret_marker"),
    ],
)
async def test_manual_retry_database_failures_return_sanitized_503(
    monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    monkeypatch.setattr(api, "retry_job", AsyncMock(side_effect=failure))
    app = api.create_app(app_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/jobs/{uuid4()}/retry", json={"expected_generation": 0})
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert "secret_marker" not in response.text
