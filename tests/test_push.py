"""Push wire validation, resource lifecycle and bounded HTTP failure policy."""

import asyncio
import base64
import json
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from fastapi import Response
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

from distributed_cloud_scheduler import push
from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import Claim
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.retries import RetryPolicy

SUBSCRIPTION = "projects/local-project/subscriptions/jobs-push"
JOB_ID = UUID("12345678-1234-5678-9abc-123456789abc")


def make_settings(**overrides: object) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://test:secret_marker@127.0.0.1:1/scheduler_test",
        _env_file=None,
        **overrides,
    )


def transport_settings(**overrides: object) -> PubSubSettings:
    return PubSubSettings(pubsub_subscription=SUBSCRIPTION, _env_file=None, **overrides)


def envelope(identifier: bytes = str(JOB_ID).encode("ascii")) -> dict[str, object]:
    return {
        "message": {"data": base64.b64encode(identifier).decode("ascii"), "messageId": "123"},
        "subscription": SUBSCRIPTION,
    }


@pytest_asyncio.fixture
async def push_client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[AsyncClient]:
    monkeypatch.setattr(push, "recover_expired", AsyncMock(return_value=0))
    monkeypatch.setattr(push, "claim_job", AsyncMock(return_value=None))
    monkeypatch.setattr(
        push, "_unclaimed_response", AsyncMock(return_value=Response(status_code=204))
    )
    app = push.create_push_app(make_settings(), transport_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client


@pytest.mark.parametrize("timeout", [1, 15, 20])
def test_factory_requires_budget_beyond_task_deadline(timeout: float) -> None:
    with pytest.raises(ValueError, match="PUSH_REQUEST_TIMEOUT_SECONDS"):
        push.create_push_app(
            make_settings(), transport_settings(push_request_timeout_seconds=timeout)
        )


def test_factory_requires_subscription_without_changing_public_api() -> None:
    with pytest.raises(ValueError, match="PUBSUB_SUBSCRIPTION"):
        push.create_push_app(make_settings(), PubSubSettings(_env_file=None))
    assert "/pubsub/push" not in create_app(make_settings()).openapi()["paths"]


def test_worker_openapi_describes_wrapped_body_and_empty_acknowledgement() -> None:
    schema = push.create_push_app(make_settings(), transport_settings()).openapi()
    assert set(schema["paths"]) == {"/health", "/ready", "/pubsub/push"}
    operation = schema["paths"]["/pubsub/push"]["post"]
    body = operation["requestBody"]["content"]["application/json"]
    assert body["schema"]["required"] == ["message", "subscription"]
    assert body["example"]["subscription"] == SUBSCRIPTION
    assert "content" not in operation["responses"]["204"]


@pytest.mark.asyncio
@pytest.mark.parametrize("message_id_field", ["messageId", "message_id"])
async def test_validated_id_and_transport_metadata(
    push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, message_id_field: str
) -> None:
    claim = AsyncMock(return_value=None)
    recover = AsyncMock(return_value=0)
    monkeypatch.setattr(push, "claim_job", claim)
    monkeypatch.setattr(push, "recover_expired", recover)
    body = {
        "message": {
            "data": base64.b64encode(str(JOB_ID).encode("ascii")).decode("ascii"),
            message_id_field: "123",
            "publishTime": "2026-10-06T00:00:00Z",
            "orderingKey": "unused",
            "attributes": {"type": "untrusted", "attempt_count": "99"},
        },
        "subscription": SUBSCRIPTION,
        "deliveryAttempt": 99,
    }
    assert (await push_client.post("/pubsub/push", json=body)).status_code == 204
    assert claim.await_args.args[1:] == (JOB_ID, 60)
    assert recover.await_args.kwargs == {
        "job_id": JOB_ID,
        "retry_policy": RetryPolicy(),
        "service": "push_worker",
    }


INVALID_ENVELOPES: list[object] = [
    None,
    [],
    {},
    {"message": [], "subscription": SUBSCRIPTION},
    {"message": {"data": "abc"}, "subscription": SUBSCRIPTION},
    {"message": {"data": "abc", "messageId": ""}, "subscription": SUBSCRIPTION},
    {"message": {"data": 1, "messageId": "123"}, "subscription": SUBSCRIPTION},
    {"message": {"data": "abc", "messageId": True}, "subscription": SUBSCRIPTION},
    {"message": {"data": "!" * 48, "messageId": "123"}, "subscription": SUBSCRIPTION},
    envelope(b"x" * 36),
    envelope(JOB_ID.hex.encode("ascii")),
    envelope(str(JOB_ID).upper().encode("ascii")),
    envelope(b" " + str(JOB_ID).encode("ascii")),
    envelope(b'{"job_id":"123"}'),
    envelope(b"\xff" * 36),
    {**envelope(), "subscription": "projects/local-project/subscriptions/other"},
]


@pytest.mark.asyncio
@pytest.mark.parametrize("body", INVALID_ENVELOPES)
async def test_malformed_envelope_does_not_touch_database(
    push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    recover = AsyncMock()
    monkeypatch.setattr(push, "recover_expired", recover)
    response = await push_client.post("/pubsub/push", content=json.dumps(body))
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid push envelope"}
    recover.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"", b"{", b"\xff", b'{"unused":NaN}'])
async def test_invalid_json_is_a_sanitized_nack(push_client: AsyncClient, body: bytes) -> None:
    response = await push_client.post("/pubsub/push", content=body)
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid push envelope"}


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [8192, 8193])
async def test_streamed_body_limit_includes_ignored_metadata(
    push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    recover = AsyncMock(return_value=0)
    monkeypatch.setattr(push, "recover_expired", recover)
    body = {**envelope(), "padding": ""}
    body["padding"] = "x" * (size - len(json.dumps(body).encode("utf-8")))
    encoded = json.dumps(body).encode("utf-8")
    assert len(encoded) == size

    async def chunks() -> AsyncIterator[bytes]:
        yield encoded[:4096]
        yield encoded[4096:]

    response = await push_client.post("/pubsub/push", content=chunks())
    assert response.status_code == (204 if size == 8192 else 413)
    if size > 8192:
        recover.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["recover_expired", "claim_job", "_unclaimed_response"])
@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("SELECT secret_marker", {}, Exception("secret_marker")),
        OSError("secret_marker"),
    ],
)
async def test_database_failures_remain_retryable_and_sanitized(
    push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, stage: str, failure: Exception
) -> None:
    monkeypatch.setattr(push, stage, AsyncMock(side_effect=failure))
    response = await push_client.post("/pubsub/push", json=envelope())
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    assert "secret_marker" not in response.text


@pytest.mark.asyncio
async def test_whole_request_deadline_cancels_stalled_database_and_releases_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> int:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return 0

    monkeypatch.setattr(push, "recover_expired", stalled)
    settings = transport_settings()
    app = push.create_push_app(make_settings(), settings)
    # Shorten only after factory validation to avoid a slow timeout regression test.
    settings.push_request_timeout_seconds = 0.01
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/pubsub/push", json=envelope())
            assert response.status_code == 503
            assert response.json() == {"detail": "Push request timed out"}
            assert cancelled.is_set()
            monkeypatch.setattr(push, "recover_expired", AsyncMock(return_value=0))
            monkeypatch.setattr(push, "claim_job", AsyncMock(return_value=None))
            monkeypatch.setattr(
                push, "_unclaimed_response", AsyncMock(return_value=Response(status_code=204))
            )
            assert (await client.post("/pubsub/push", json=envelope())).status_code == 204


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", ["timeout", "cancel"])
async def test_body_read_is_bounded_and_interruption_releases_capacity(
    monkeypatch: pytest.MonkeyPatch, interrupt: str
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    recover = AsyncMock(return_value=0)
    monkeypatch.setattr(push, "recover_expired", recover)
    monkeypatch.setattr(push, "claim_job", AsyncMock(return_value=None))
    monkeypatch.setattr(
        push, "_unclaimed_response", AsyncMock(return_value=Response(status_code=204))
    )
    settings = transport_settings()
    app = push.create_push_app(make_settings(), settings)
    if interrupt == "timeout":
        settings.push_request_timeout_seconds = 0.01

    async def stalled_body() -> AsyncIterator[bytes]:
        yield b"{"
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            pending = asyncio.create_task(client.post("/pubsub/push", content=stalled_body()))
            async with asyncio.timeout(2):
                await started.wait()
                if interrupt == "cancel":
                    pending.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
                else:
                    response = await pending
                    assert response.status_code == 503
                    assert response.json() == {"detail": "Push request timed out"}
            assert cancelled.is_set()
            recover.assert_not_awaited()
            settings.push_request_timeout_seconds = 45
            assert (await client.post("/pubsub/push", json=envelope())).status_code == 204


@pytest.mark.asyncio
async def test_actual_database_connection_refusal_nacks_and_liveness_stays_available() -> None:
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        settings = Settings(
            database_url=(
                f"postgresql+psycopg://test:secret_marker@127.0.0.1:{port}/scheduler_test"
            ),
            _env_file=None,
        )
        app = push.create_push_app(settings, transport_settings())
        async with app.router.lifespan_context(app):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post("/pubsub/push", json=envelope())
                assert response.status_code == 503
                assert response.json() == {"detail": "Database unavailable"}
                assert "secret_marker" not in response.text
                assert (await client.get("/ready")).status_code == 503
                assert (await client.get("/health")).status_code == 200


@pytest.mark.asyncio
async def test_liveness_readiness_and_lifespan_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    check = AsyncMock(side_effect=OSError("secret_marker"))
    dispose = AsyncMock()
    monkeypatch.setattr(Database, "check_connection", check)
    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    app = push.create_push_app(make_settings(), transport_settings())
    assert not hasattr(app.state, "database")
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/health")).json() == {"status": "ok"}
            check.assert_not_awaited()
            ready = await client.get("/ready")
            assert ready.status_code == 503
            assert ready.json() == {"status": "not_ready"}
            check.side_effect = None
            assert (await client.get("/ready")).json() == {"status": "ready"}
    dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_readiness_query_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def stalled(self: Database) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(Database, "check_connection", stalled)
    app = push.create_push_app(make_settings(readiness_timeout_seconds=0.01), transport_settings())
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/ready")).status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, OperationalError("secret_marker", {}, Exception())])
async def test_uncommitted_completion_never_acknowledges(
    push_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, outcome: bool | Exception
) -> None:
    monkeypatch.setattr(
        push, "claim_job", AsyncMock(return_value=Claim(JOB_ID, "calculation", {}, 1, uuid4()))
    )
    execute = (
        AsyncMock(side_effect=outcome)
        if isinstance(outcome, Exception)
        else AsyncMock(return_value=outcome)
    )
    monkeypatch.setattr(push, "execute_claim", execute)
    response = await push_client.post("/pubsub/push", json=envelope())
    assert response.status_code == 503
    assert "secret_marker" not in response.text


def test_push_cli_help_and_invalid_port_do_not_require_runtime_configuration() -> None:
    command = [sys.executable, "-m", "distributed_cloud_scheduler.push"]
    help_result = subprocess.run([*command, "--help"], capture_output=True, text=True, check=False)
    assert help_result.returncode == 0
    assert "--host" in help_result.stdout
    assert "--port" in help_result.stdout
    invalid_result = subprocess.run(
        [*command, "--port", "0"], capture_output=True, text=True, check=False
    )
    assert invalid_result.returncode == 2
    assert "--port must be between 1 and 65535" in invalid_result.stderr
