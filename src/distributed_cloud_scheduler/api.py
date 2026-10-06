"""FastAPI application factory with lifecycle-owned database resources."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import (
    JobCreate,
    JobList,
    JobRead,
    create_job,
    get_job,
    list_jobs,
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

    @app.post("/jobs", response_model=JobRead, status_code=201)
    async def submit_job(
        submission: JobCreate,
        session: Annotated[AsyncSession, Depends(get_session)],
    ) -> JobRead:
        """Persist a pending job; execution and dispatch are separate later features."""
        job = await create_job(session, submission)
        return JobRead.model_validate(job)

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

    return app
