"""Explicit, generation-fenced recovery of terminal jobs without losing history."""

from datetime import datetime
from typing import cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from distributed_cloud_scheduler.jobs import Job, JobStatus
from distributed_cloud_scheduler.outbox import DispatchOutbox

MAX_GENERATION = 2_147_483_647


class JobRetry(BaseModel):
    """A caller must name the terminal generation it intends to reopen."""

    model_config = ConfigDict(extra="forbid", strict=True)

    expected_generation: int = Field(ge=0, le=MAX_GENERATION)


class JobNotFound(Exception):
    """The requested job does not exist."""


class RetryConflict(Exception):
    """Current state or generation prevents the requested recovery."""


async def retry_job(session: AsyncSession, job_id: UUID, expected_generation: int) -> Job:
    """Commit one new generation and immediate dispatch under the job's row lock."""
    if (
        isinstance(expected_generation, bool)
        or not isinstance(expected_generation, int)
        or not 0 <= expected_generation <= MAX_GENERATION
    ):
        raise ValueError("expected_generation must be an integer between 0 and 2147483647")
    async with session.begin():
        job = await session.scalar(
            select(Job)
            .where(Job.id == job_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if job is None:
            raise JobNotFound
        if (
            job.generation != expected_generation
            or job.generation == MAX_GENERATION
            or job.status not in (JobStatus.FAILED, JobStatus.DEAD_LETTERED)
        ):
            raise RetryConflict
        now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        job.generation += 1
        job.status = JobStatus.PENDING
        job.attempt_count = 0
        job.started_at = None
        job.finished_at = None
        job.next_attempt_at = None
        job.claim_token = None
        job.lease_expires_at = None
        job.result_metadata = None
        job.error_info = None
        job.dead_lettered_at = None
        job.dead_letter_reason = None
        # Leave earlier obligations alone: publication locks outbox before job.
        session.add(
            DispatchOutbox(job_id=job.id, generation=job.generation, available_at=now)
        )
        await session.flush()
        await session.refresh(job)
    return job
