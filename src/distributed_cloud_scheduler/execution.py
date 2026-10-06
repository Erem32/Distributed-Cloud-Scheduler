"""Short transactional job claims, fenced completion, and expired-owner recovery."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import func, select, update

from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import Job, JobStatus, validate_json_object


@dataclass(frozen=True, slots=True)
class Claim:
    """Snapshot of committed ownership, detached from any database transaction."""

    id: UUID
    type: str
    payload: dict[str, JsonValue]
    attempt_count: int
    token: UUID


async def claim_pending(database: Database, lease_seconds: int) -> Claim | None:
    """Claim the oldest pending or queued job without holding its lock during execution."""
    return await _claim_available(database, lease_seconds)


async def claim_job(database: Database, job_id: UUID, lease_seconds: int) -> Claim | None:
    """Claim a delivered job ID using the same ownership rules as local polling."""
    return await _claim_available(database, lease_seconds, job_id=job_id)


async def _claim_available(
    database: Database, lease_seconds: int, *, job_id: UUID | None = None
) -> Claim | None:
    """Persist one owner in a short transaction, regardless of how its job was selected."""
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds < 1:
        raise ValueError("lease_seconds must be a positive integer")
    async with database.session_factory() as session, session.begin():
        candidates = select(Job).where(
            Job.status.in_((JobStatus.PENDING, JobStatus.QUEUED)),
            Job.attempt_count < Job.max_attempts,
        )
        if job_id is not None:
            candidates = candidates.where(Job.id == job_id)
        job = await session.scalar(
            candidates
            .order_by(Job.created_at, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if job is None:
            return None
        now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        token = uuid4()
        job.status = JobStatus.RUNNING
        job.claim_token = token
        job.started_at = now
        job.finished_at = None
        job.lease_expires_at = now + timedelta(seconds=lease_seconds)
        job.attempt_count += 1
        job.result_metadata = None
        job.error_info = None
        await session.flush()
        await session.refresh(job)
        claim = Claim(job.id, job.type, job.payload, job.attempt_count, token)
    return claim


async def complete_claim(
    database: Database,
    claim: Claim,
    *,
    result: dict[str, JsonValue] | None = None,
    error: dict[str, JsonValue] | None = None,
) -> bool:
    """Commit one outcome only while the token still owns an unexpired RUNNING job."""
    if (result is None) == (error is None):
        raise ValueError("Provide exactly one of result or error")
    if result is not None:
        validate_json_object(result, "result metadata")
    if error is not None:
        validate_json_object(error, "error info")
    async with database.session_factory() as session, session.begin():
        # Acquire the lock first: the lease may expire while waiting behind another transaction.
        locked_id = await session.scalar(
            select(Job.id)
            .where(
                Job.id == claim.id,
                Job.status == JobStatus.RUNNING,
                Job.claim_token == claim.token,
            )
            .with_for_update()
        )
        if locked_id is None:
            return False
        completed_id = await session.scalar(
            update(Job)
            .where(
                Job.id == claim.id,
                Job.status == JobStatus.RUNNING,
                Job.claim_token == claim.token,
                Job.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                status=JobStatus.SUCCEEDED if result is not None else JobStatus.FAILED,
                finished_at=func.clock_timestamp(),
                claim_token=None,
                lease_expires_at=None,
                result_metadata=result,
                error_info=error,
            )
            .returning(Job.id)
        )
    return completed_id is not None


async def recover_expired(
    database: Database, limit: int = 100, *, job_id: UUID | None = None
) -> int:
    """Fail a bounded batch or one delivered expired owner without execution retries."""
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    async with database.session_factory() as session, session.begin():
        candidates = select(Job.id).where(
            Job.status == JobStatus.RUNNING,
            Job.lease_expires_at <= func.clock_timestamp(),
        )
        if job_id is not None:
            candidates = candidates.where(Job.id == job_id)
        expired_ids = list(
            await session.scalars(
                candidates
                .order_by(Job.lease_expires_at, Job.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        if not expired_ids:
            return 0
        recovered_ids = await session.scalars(
            update(Job)
            .where(
                Job.id.in_(expired_ids),
                Job.status == JobStatus.RUNNING,
                Job.lease_expires_at <= func.clock_timestamp(),
            )
            .values(
                status=JobStatus.FAILED,
                finished_at=func.clock_timestamp(),
                claim_token=None,
                lease_expires_at=None,
                result_metadata=None,
                error_info={"code": "worker_lost", "retryable": True},
            )
            .returning(Job.id)
        )
        recovered_count = len(list(recovered_ids))
    return recovered_count
