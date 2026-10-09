"""Short transactional job claims, fenced completion, and expired-owner recovery."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from distributed_cloud_scheduler.attempts import AttemptStatus, JobAttempt
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import Job, JobStatus, validate_json_object
from distributed_cloud_scheduler.outbox import DispatchOutbox
from distributed_cloud_scheduler.priority import priority_rank
from distributed_cloud_scheduler.retries import RetryPolicy

DEFAULT_RETRY_POLICY = RetryPolicy()


@dataclass(frozen=True, slots=True)
class Claim:
    """Snapshot of committed ownership, detached from any database transaction."""

    id: UUID
    type: str
    payload: dict[str, JsonValue]
    attempt_count: int
    token: UUID
    generation: int = 0


async def claim_pending(database: Database, lease_seconds: int) -> Claim | None:
    """Claim due work by bounded priority/age preference in a short transaction."""
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
            or_(
                and_(
                    Job.status.in_((JobStatus.PENDING, JobStatus.QUEUED)),
                    or_(
                        Job.generation > 0,
                        Job.scheduled_at.is_(None),
                        Job.scheduled_at <= func.clock_timestamp(),
                    ),
                ),
                and_(
                    Job.status == JobStatus.RETRY_WAIT,
                    Job.next_attempt_at <= func.clock_timestamp(),
                ),
            ),
            Job.attempt_count < Job.max_attempts,
        )
        if job_id is not None:
            candidates = candidates.where(Job.id == job_id)
        job = await session.scalar(
            candidates.order_by(priority_rank(Job.ready_at, Job.priority), Job.ready_at, Job.id)
            .limit(1).with_for_update(skip_locked=True)
        )
        if job is None:
            return None
        now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        token = uuid4()
        job.status = JobStatus.RUNNING
        job.claim_token = token
        job.started_at = now
        job.finished_at = None
        job.next_attempt_at = None
        job.lease_expires_at = now + timedelta(seconds=lease_seconds)
        job.attempt_count += 1
        job.result_metadata = None
        job.error_info = None
        session.add(
            JobAttempt(
                job_id=job.id,
                generation=job.generation,
                attempt_number=job.attempt_count,
                claim_token=token,
                started_at=now,
            )
        )
        await session.flush()
        await session.refresh(job)
        claim = Claim(job.id, job.type, job.payload, job.attempt_count, token, job.generation)
    return claim


async def complete_claim(
    database: Database,
    claim: Claim,
    *,
    result: dict[str, JsonValue] | None = None,
    error: dict[str, JsonValue] | None = None,
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
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
        job = await session.scalar(
            select(Job)
            .where(
                Job.id == claim.id,
                Job.generation == claim.generation,
                Job.status == JobStatus.RUNNING,
                Job.claim_token == claim.token,
                Job.attempt_count == claim.attempt_count,
            )
            .with_for_update()
        )
        if job is None:
            return False
        attempt = await _current_attempt(session, job)
        now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        if job.lease_expires_at is None or job.lease_expires_at <= now:
            return False
        lease_deadline = job.lease_expires_at
        _finish_attempt(session, job, attempt, now, result, error, retry_policy)
        await session.flush()
        # History/outbox writes may wait on locks too. Roll back the entire outcome
        # if that wait outlived ownership; a later recovery still sees RUNNING.
        finalized_at = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        if finalized_at >= lease_deadline:
            await session.rollback()
            return False
    return True


async def _current_attempt(session: AsyncSession, job: Job) -> JobAttempt:
    """A job's locked owner must match its atomically inserted history record."""
    attempt = await session.get(JobAttempt, (job.id, job.generation, job.attempt_count))
    if (
        attempt is None
        or attempt.claim_token != job.claim_token
        or attempt.status is not AttemptStatus.RUNNING
    ):
        raise RuntimeError("Running ownership has no matching attempt record")
    return attempt


def _finish_attempt(
    session: AsyncSession,
    job: Job,
    attempt: JobAttempt,
    now: datetime,
    result: dict[str, JsonValue] | None,
    error: dict[str, JsonValue] | None,
    retry_policy: RetryPolicy,
) -> None:
    """Keep outcome, history and replacement dispatch in one locked transaction."""
    retry_at = None
    if (
        error is not None
        and error.get("retryable") is True
        and job.attempt_count < job.max_attempts
    ):
        retry_at = now + timedelta(seconds=retry_policy.delay_seconds(job.attempt_count))
        job.ready_at = retry_at
        session.add(
            DispatchOutbox(job_id=job.id, generation=job.generation, available_at=retry_at)
        )
    job.status = (
        JobStatus.RETRY_WAIT
        if retry_at is not None
        else JobStatus.SUCCEEDED
        if result is not None
        else JobStatus.DEAD_LETTERED
        if error is not None and error.get("retryable") is True
        else JobStatus.FAILED
    )
    dead_lettered = job.status is JobStatus.DEAD_LETTERED
    job.dead_lettered_at = now if dead_lettered else None
    job.dead_letter_reason = "attempts_exhausted" if dead_lettered else None
    job.finished_at = now
    job.next_attempt_at = retry_at
    job.claim_token = None
    job.lease_expires_at = None
    job.result_metadata = result
    job.error_info = error
    attempt.status = AttemptStatus.SUCCEEDED if result is not None else AttemptStatus.FAILED
    attempt.finished_at = now
    attempt.next_attempt_at = retry_at
    attempt.result_metadata = result
    attempt.error_info = error


async def recover_expired(
    database: Database,
    limit: int = 100,
    *,
    job_id: UUID | None = None,
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
) -> int:
    """Finalize expired attempts and durably retry a bounded batch or delivered owner."""
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    async with database.session_factory() as session, session.begin():
        candidates = select(Job).where(
            Job.status == JobStatus.RUNNING,
            Job.lease_expires_at <= func.clock_timestamp(),
        )
        if job_id is not None:
            candidates = candidates.where(Job.id == job_id)
        expired_jobs = list(
            await session.scalars(
                candidates.order_by(Job.lease_expires_at, Job.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        if not expired_jobs:
            return 0
        for job in expired_jobs:
            attempt = await _current_attempt(session, job)
            now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
            _finish_attempt(
                session,
                job,
                attempt,
                now,
                None,
                {"code": "worker_lost", "retryable": True},
                retry_policy,
            )
        await session.flush()
        recovered_count = len(expired_jobs)
    return recovered_count
