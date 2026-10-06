"""Liveness, readiness failure handling, and database resource lifecycle."""

import asyncio
import socket
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database


def make_settings(**overrides: object) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test_user:secret_password@127.0.0.1:1/scheduler_test",
        _env_file=None,
        **overrides,
    )


@pytest.mark.asyncio
async def test_liveness_does_not_query_an_unavailable_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check = AsyncMock(side_effect=OSError("database unreachable"))
    monkeypatch.setattr(Database, "check_connection", check)
    app = create_app(make_settings())
    assert not hasattr(app.state, "database")
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_readiness_checks_database(monkeypatch: pytest.MonkeyPatch) -> None:
    check = AsyncMock()
    monkeypatch.setattr(Database, "check_connection", check)
    app = create_app(make_settings())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    check.assert_awaited_once()


@pytest.mark.asyncio
async def test_readiness_returns_503_for_a_refused_local_connection() -> None:
    # Reserve a loopback port without listening, so no external service is involved.
    with socket.socket() as reserved_port:
        reserved_port.bind(("127.0.0.1", 0))
        port = reserved_port.getsockname()[1]
        settings = Settings(
            database_url=(
                f"postgresql+psycopg://test_user:secret_password@127.0.0.1:{port}/scheduler_test"
            ),
            _env_file=None,
        )
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                ready = await client.get("/ready")
                health = await client.get("/health")
    assert ready.status_code == 503
    assert ready.json() == {"status": "not_ready"}
    assert "secret_password" not in ready.text
    assert health.status_code == 200


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, Exception("secret_password database credentials")),
        OSError("secret_password network failure"),
        TimeoutError("secret_password connection timeout"),
    ],
)
@pytest.mark.asyncio
async def test_readiness_failure_is_sanitized(
    error: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Database, "check_connection", AsyncMock(side_effect=error))
    app = create_app(make_settings())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            ready = await client.get("/ready")
            health = await client.get("/health")
    assert ready.status_code == 503
    assert ready.json() == {"status": "not_ready"}
    assert "secret_password" not in ready.text
    assert health.status_code == 200


@pytest.mark.asyncio
async def test_readiness_timeout_cancels_a_stalled_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.Event()

    async def stalled_check(self: Database) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(Database, "check_connection", stalled_check)
    app = create_app(make_settings(readiness_timeout_seconds=0.01))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            async with asyncio.timeout(1):
                response = await client.get("/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert cancelled.is_set()


@pytest.mark.parametrize("raise_in_lifespan", [False, True])
@pytest.mark.asyncio
async def test_lifespan_disposes_engine_even_after_an_exception(
    raise_in_lifespan: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app(make_settings())
    dispose = AsyncMock()
    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    try:
        async with app.router.lifespan_context(app):
            if raise_in_lifespan:
                raise RuntimeError("simulated request failure")
    except RuntimeError:
        if not raise_in_lifespan:
            raise
    dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_factories_own_independent_database_resources() -> None:
    first = create_app(make_settings())
    second = create_app(make_settings())
    async with first.router.lifespan_context(first), second.router.lifespan_context(second):
        assert first.state.database is not second.state.database
        assert first.state.database.engine is not second.state.database.engine
        assert first.state.database.session_factory is not second.state.database.session_factory
