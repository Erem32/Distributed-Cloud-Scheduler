"""FastAPI application factory with lifecycle-owned database resources."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession

from distributed_cloud_scheduler.attempts import AttemptRead, list_attempts
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.idempotency import (
    IDEMPOTENCY_KEY_PATTERN,
    IdempotencyConflict,
    validate_idempotency_key,
)
from distributed_cloud_scheduler.jobs import (
    JobCreate,
    JobList,
    JobRead,
    create_or_replay_job,
    get_job,
    list_jobs,
)
from distributed_cloud_scheduler.manual_retry import (
    JobNotFound,
    JobRetry,
    RetryConflict,
    retry_job,
)
from distributed_cloud_scheduler.observability import (
    METRICS,
    install_http_metrics,
    log_event,
    metrics_response,
)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Give one operation its own session, closing it even when the request fails."""
    database = cast(Database, request.app.state.database)
    async with database.session_factory() as session:
        yield session


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create an independent application without connecting during import/startup."""
    configured = settings if settings is not None else Settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        database = Database(configured)
        application.state.database = database
        try:
            yield
        finally:
            await database.dispose()

    app = FastAPI(title="Distributed Cloud Task Scheduler", lifespan=lifespan)
    install_http_metrics(app, "api")

    @app.exception_handler(OperationalError)
    @app.exception_handler(SQLAlchemyTimeoutError)
    async def database_unavailable(request: Request, error: Exception) -> Response:
        """Expose connection failures without returning database details."""
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError) -> Response:
        """Return useful errors without echoing non-JSON numbers or invalid payload data."""
        details = [
            {"type": item["type"], "loc": item["loc"], "msg": item["msg"]}
            for item in error.errors()
        ]
        # ASCII escaping also handles invalid Unicode in a supplied field name.
        return Response(
            content=json.dumps({"detail": details}, ensure_ascii=True, allow_nan=False),
            status_code=422,
            media_type="application/json",
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Report process liveness independently of external dependencies."""
        return {"status": "ok"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        """Expose this API process's bounded Prometheus metrics."""
        return metrics_response()

    @app.get("/ready", responses={503: {"description": "Database is unavailable."}})
    async def ready(response: Response) -> dict[str, str]:
        """Bound the readiness query and hide connection details on failures."""
        database = cast(Database, app.state.database)
        try:
            async with asyncio.timeout(configured.readiness_timeout_seconds):
                await database.check_connection()
        except (SQLAlchemyError, OSError, TimeoutError):
            response.status_code = 503
            return {"status": "not_ready"}
        return {"status": "ready"}

    @app.post(
        "/jobs",
        response_model=JobRead,
        status_code=201,
        responses={
            200: {"model": JobRead, "description": "Matching key: current persisted job."},
            409: {"description": "The key is already bound to different submission content."},
            503: {"description": "Database is unavailable."},
        },
    )
    async def submit_job(
        submission: JobCreate,
        request: Request,
        response: Response,
        session: Annotated[AsyncSession, Depends(get_session)],
        idempotency_key: Annotated[
            str | None,
            Header(
                alias="Idempotency-Key",
                min_length=1,
                max_length=128,
                pattern=IDEMPOTENCY_KEY_PATTERN,
                description=(
                    "Optional case-sensitive global key. Matching content replays the current job; "
                    "different content conflicts. Retained while the job exists."
                ),
            ),
        ] = None,
    ) -> JobRead:
        """Commit immediate/scheduled intent, or replay the current job for its key."""
        if len(request.headers.getlist("Idempotency-Key")) > 1:
            raise HTTPException(status_code=422, detail="Supply one Idempotency-Key header")
        if idempotency_key is not None:
            try:
                validate_idempotency_key(idempotency_key)
            except ValueError:
                raise HTTPException(status_code=422, detail="Invalid Idempotency-Key") from None
        try:
            submitted = await create_or_replay_job(
                session, submission, idempotency_key=idempotency_key
            )
        except IdempotencyConflict:
            raise HTTPException(
                status_code=409, detail="Idempotency-Key is already used for different content"
            ) from None
        response.status_code = 201 if submitted.created else 200
        outcome = "created" if submitted.created else "replayed"
        METRICS.increment("jobs_submitted_total", outcome=outcome)
        log_event(
            f"job_submission_{outcome}",
            "api",
            job_id=str(submitted.job.id),
            task_type=submitted.job.type,
            status=submitted.job.status.value,
        )
        return JobRead.model_validate(submitted.job)

    @app.get(
        "/jobs/{job_id}", response_model=JobRead, responses={404: {"description": "Not found."}}
    )
    async def read_job(
        job_id: UUID,
        session: Annotated[AsyncSession, Depends(get_session)],
    ) -> JobRead:
        job = await get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobRead.model_validate(job)

    @app.get("/jobs", response_model=JobList)
    async def read_jobs(
        session: Annotated[AsyncSession, Depends(get_session)],
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
    ) -> JobList:
        jobs = await list_jobs(session, limit=limit, offset=offset)
        return JobList(
            items=[JobRead.model_validate(job) for job in jobs], limit=limit, offset=offset
        )

    @app.get(
        "/jobs/{job_id}/attempts",
        response_model=list[AttemptRead],
        responses={404: {"description": "Job not found."}},
    )
    async def read_attempts(
        job_id: UUID,
        session: Annotated[AsyncSession, Depends(get_session)],
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
        generation: Annotated[int | None, Query(ge=0, le=2_147_483_647)] = None,
    ) -> list[AttemptRead]:
        """Inspect paginated generation/attempt history; legacy history may be absent."""
        if await get_job(session, job_id) is None:
            raise HTTPException(status_code=404, detail="Job not found")
        rows = await list_attempts(
            session, job_id, limit=limit, offset=offset, generation=generation
        )
        return [AttemptRead.model_validate(row) for row in rows]

    @app.post(
        "/jobs/{job_id}/retry",
        response_model=JobRead,
        status_code=202,
        responses={
            404: {"description": "Job not found."},
            409: {"description": "Stale generation or job is ineligible for manual retry."},
            503: {"description": "Database is unavailable."},
        },
    )
    async def manually_retry_job(
        job_id: UUID,
        recovery: JobRetry,
        session: Annotated[AsyncSession, Depends(get_session)],
    ) -> JobRead:
        """Reopen FAILED/DEAD_LETTERED once for the supplied current generation."""
        try:
            job = await retry_job(session, job_id, recovery.expected_generation)
        except JobNotFound:
            raise HTTPException(status_code=404, detail="Job not found") from None
        except RetryConflict:
            raise HTTPException(
                status_code=409, detail="Job state or generation does not allow manual retry"
            ) from None
        log_event(
            "job_manually_retried",
            "api",
            job_id=str(job.id),
            task_type=job.type,
            status=job.status.value,
        )
        return JobRead.model_validate(job)

    return app
