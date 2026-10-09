"""Dedicated HTTP worker for wrapped Pub/Sub delivery and durable acknowledgement."""

import argparse
import asyncio
import base64
import binascii
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import cast
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import claim_job, recover_expired
from distributed_cloud_scheduler.jobs import JobStatus, get_job
from distributed_cloud_scheduler.observability import install_http_metrics, metrics_response
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.worker import execute_claim

MAX_PUSH_BODY_BYTES = 8 * 1024
TERMINAL_STATUSES = frozenset((JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED))


class _PushMessage(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    data: str = Field(min_length=1, max_length=128)
    message_id: str = Field(
        min_length=1,
        max_length=1024,
        validation_alias=AliasChoices("messageId", "message_id"),
    )


class _PushEnvelope(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    message: _PushMessage
    subscription: str


def _invalid_json_constant(value: str) -> None:
    raise ValueError("Non-finite JSON numbers are not supported")


async def _read_job_id(request: Request, subscription: str) -> UUID:
    """Bound the streamed body before decoding an ID-only wrapped envelope."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_PUSH_BODY_BYTES:
            raise HTTPException(status_code=413, detail="Push envelope too large")
        body.extend(chunk)
    try:
        envelope = _PushEnvelope.model_validate(
            json.loads(body, parse_constant=_invalid_json_constant)
        )
        if envelope.subscription != subscription:
            raise ValueError("Unexpected subscription")
        identifier = base64.b64decode(envelope.message.data, validate=True)
        if len(identifier) != 36:
            raise ValueError("Job ID must be 36 bytes")
        decoded = identifier.decode("ascii")
        job_id = UUID(decoded)
        if str(job_id) != decoded:
            raise ValueError("Job ID must use canonical UUID spelling")
    except (ValidationError, ValueError, UnicodeError, binascii.Error, RecursionError):
        raise HTTPException(status_code=400, detail="Invalid push envelope") from None
    return job_id


async def _unclaimed_response(database: Database, job_id: UUID) -> Response:
    """A skipped lock or active owner must keep the broker delivery outstanding."""
    async with database.session_factory() as session:
        job = await get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status in TERMINAL_STATUSES:
            return Response(status_code=204)
        if job.status is JobStatus.SCHEDULED:
            # Admission intent survives both scheduler rollback and an early ACK.
            # Successful admission commits a fresh outbox for subsequent delivery.
            return Response(status_code=204)
        if job.status is JobStatus.RETRY_WAIT:
            now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
            if job.next_attempt_at is not None and job.next_attempt_at > now:
                return Response(status_code=204)
            # A due claim may be locked by an uncommitted owner. Preserve redelivery
            # until it commits rather than ACKing the last available due delivery.
    raise HTTPException(status_code=503, detail="Job is not available for execution")


def create_push_app(
    settings: Settings | None = None, pubsub_settings: PubSubSettings | None = None
) -> FastAPI:
    """Create a separate worker service; Cloud Run IAM must authenticate production push."""
    configured = settings if settings is not None else Settings()
    transport = pubsub_settings if pubsub_settings is not None else PubSubSettings()
    subscription = transport.pubsub_subscription
    if subscription is None:
        raise ValueError("PUBSUB_SUBSCRIPTION is required for the push worker")
    if transport.push_request_timeout_seconds <= configured.worker_task_timeout_seconds + 5:
        raise ValueError(
            "PUSH_REQUEST_TIMEOUT_SECONDS must exceed WORKER_TASK_TIMEOUT_SECONDS by 5"
        )
    active_requests = 0

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        database = Database(configured)
        application.state.database = database
        try:
            yield
        finally:
            await database.dispose()

    app = FastAPI(title="Distributed Cloud Task Scheduler Push Worker", lifespan=lifespan)
    install_http_metrics(app, "push_worker")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        """Expose this push worker process's bounded Prometheus metrics."""
        return metrics_response()

    @app.get("/ready", responses={503: {"description": "Database is unavailable."}})
    async def ready(response: Response) -> dict[str, str]:
        database = cast(Database, app.state.database)
        try:
            async with asyncio.timeout(configured.readiness_timeout_seconds):
                await database.check_connection()
        except (SQLAlchemyError, OSError, TimeoutError):
            response.status_code = 503
            return {"status": "not_ready"}
        return {"status": "ready"}

    @app.post(
        "/pubsub/push",
        status_code=204,
        response_class=Response,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "required": ["message", "subscription"],
                            "properties": {
                                "message": {
                                    "type": "object",
                                    "required": ["data", "messageId"],
                                    "properties": {
                                        "data": {
                                            "type": "string",
                                            "format": "byte",
                                            "description": "Base64 of a canonical ASCII job UUID.",
                                        },
                                        "messageId": {
                                            "type": "string",
                                            "minLength": 1,
                                            "description": "message_id is also accepted.",
                                        },
                                    },
                                },
                                "subscription": {"type": "string"},
                            },
                        },
                        "example": {
                            "message": {
                                "messageId": "123456789",
                                "data": base64.b64encode(
                                    b"12345678-1234-5678-9abc-123456789abc"
                                ).decode("ascii"),
                            },
                            "subscription": subscription,
                        },
                    }
                },
            }
        },
        responses={
            400: {"description": "Invalid envelope; broker redelivery remains required."},
            404: {"description": "Unknown job; broker redelivery remains required."},
            413: {"description": "Envelope exceeds the worker's body limit."},
            503: {"description": "Execution is outstanding; broker should redeliver."},
        },
    )
    async def deliver(request: Request) -> Response:
        """Acknowledge committed outcomes or retry intent; execute only eligible work."""
        nonlocal active_requests
        # No await separates this check and reservation on the application's event loop.
        if active_requests >= configured.worker_concurrency:
            raise HTTPException(status_code=503, detail="Worker capacity exhausted")
        active_requests += 1
        try:
            async with asyncio.timeout(transport.push_request_timeout_seconds):
                job_id = await _read_job_id(request, subscription)
                database = cast(Database, app.state.database)
                await recover_expired(
                    database,
                    job_id=job_id,
                    retry_policy=configured.execution_retry_policy,
                    service="push_worker",
                )
                claim = await claim_job(database, job_id, configured.worker_lease_seconds)
                if claim is None:
                    return await _unclaimed_response(database, job_id)
                if not await execute_claim(
                    database, claim, configured, service="push_worker"
                ):
                    raise HTTPException(status_code=503, detail="Execution ownership lost")
                return Response(status_code=204)
        except TimeoutError:
            raise HTTPException(status_code=503, detail="Push request timed out") from None
        except (SQLAlchemyError, OSError):
            raise HTTPException(status_code=503, detail="Database unavailable") from None
        finally:
            active_requests -= 1

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Pub/Sub HTTP worker.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    arguments = parser.parse_args()
    if not 1 <= arguments.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    config = uvicorn.Config(
        "distributed_cloud_scheduler.push:create_push_app",
        factory=True,
        host=arguments.host,
        port=arguments.port,
    )
    server = uvicorn.Server(config)
    with asyncio.Runner(loop_factory=create_event_loop) as runner:
        runner.run(server.serve())


if __name__ == "__main__":
    main()
