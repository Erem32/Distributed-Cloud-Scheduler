"""Durable publication obligations with short leases and fenced finalization."""

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    and_,
    func,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from distributed_cloud_scheduler.database import Base, Database
from distributed_cloud_scheduler.jobs import Job, JobStatus


class DispatchStatus(StrEnum):
    """Publication state is independent of the job's execution state."""

    READY = "READY"
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"
    SKIPPED = "SKIPPED"


class DispatchOutbox(Base):
    """An obligation committed atomically with a job and retried until finalized."""

    __tablename__ = "dispatch_outbox"
    __table_args__ = (
        CheckConstraint("generation >= 0", name="generation_nonnegative"),
        CheckConstraint("publish_attempt_count >= 0", name="publish_attempt_count_range"),
        CheckConstraint(
            "(status = 'PUBLISHING' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status <> 'PUBLISHING' AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name="publishing_ownership",
        ),
        CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="last_error_code_format",
        ),
        Index(
            "ix_dispatch_outbox_ready",
            "available_at",
            "id",
            postgresql_where=text("status = 'READY'"),
        ),
        Index(
            "ix_dispatch_outbox_publishing_lease",
            "lease_expires_at",
            "id",
            postgresql_where=text("status = 'PUBLISHING'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    status: Mapped[DispatchStatus] = mapped_column(
        Enum(
            DispatchStatus,
            name="dispatch_status",
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
        default=DispatchStatus.READY,
        server_default=text("'READY'"),
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    publish_attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    claim_token: Mapped[UUID | None] = mapped_column(nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


@dataclass(frozen=True, slots=True)
class DispatchClaim:
    """Committed publisher ownership with no open database transaction."""

    id: UUID
    job_id: UUID
    token: UUID
    attempt_count: int
    generation: int = 0


async def claim_dispatch(database: Database, lease_seconds: int) -> DispatchClaim | None:
    """Claim due publication; skip up to 100 terminal or obsolete obligations."""
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds < 1:
        raise ValueError("lease_seconds must be a positive integer")
    async with database.session_factory() as session, session.begin():
        for _ in range(100):
            row = await session.scalar(
                select(DispatchOutbox)
                .where(
                    or_(
                        and_(
                            DispatchOutbox.status == DispatchStatus.READY,
                            DispatchOutbox.available_at <= func.clock_timestamp(),
                        ),
                        and_(
                            DispatchOutbox.status == DispatchStatus.PUBLISHING,
                            DispatchOutbox.lease_expires_at <= func.clock_timestamp(),
                        ),
                    )
                )
                .order_by(DispatchOutbox.available_at, DispatchOutbox.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return None
            job = (
                await session.execute(
                    select(Job.status, Job.generation).where(Job.id == row.job_id)
                )
            ).one()
            if job.generation != row.generation or job.status in (
                JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED
            ):
                row.status = DispatchStatus.SKIPPED
                row.claim_token = None
                row.lease_expires_at = None
                row.last_error_code = None
                await session.flush()
                continue
            now = cast(datetime, await session.scalar(select(func.clock_timestamp())))
            token = uuid4()
            row.status = DispatchStatus.PUBLISHING
            row.claim_token = token
            row.lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.publish_attempt_count += 1
            await session.flush()
            claim = DispatchClaim(
                row.id, row.job_id, token, row.publish_attempt_count, row.generation
            )
            break
        else:
            return None
    return claim


async def _lock_dispatch(session: AsyncSession, claim: DispatchClaim) -> UUID | None:
    """Acquire ownership's row lock before evaluating any time-sensitive predicate."""
    locked_id = await session.scalar(
        select(DispatchOutbox.id)
        .where(
            DispatchOutbox.id == claim.id,
            DispatchOutbox.job_id == claim.job_id,
            DispatchOutbox.generation == claim.generation,
            DispatchOutbox.status == DispatchStatus.PUBLISHING,
            DispatchOutbox.claim_token == claim.token,
        )
        .with_for_update()
    )
    return locked_id


async def complete_dispatch(database: Database, claim: DispatchClaim) -> bool:
    """Mark accepted publication and queue only a job that remains PENDING."""
    async with database.session_factory() as session, session.begin():
        if await _lock_dispatch(session, claim) is None:
            return False
        published_id = await session.scalar(
            update(DispatchOutbox)
            .where(
                DispatchOutbox.id == claim.id,
                DispatchOutbox.job_id == claim.job_id,
                DispatchOutbox.status == DispatchStatus.PUBLISHING,
                DispatchOutbox.claim_token == claim.token,
                DispatchOutbox.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                status=DispatchStatus.PUBLISHED,
                published_at=func.clock_timestamp(),
                claim_token=None,
                lease_expires_at=None,
                last_error_code=None,
            )
            .returning(DispatchOutbox.id)
        )
        if published_id is None:
            return False
        await session.execute(
            update(Job)
            .where(
                Job.id == claim.job_id,
                Job.generation == claim.generation,
                Job.status == JobStatus.PENDING,
            )
            .values(status=JobStatus.QUEUED)
        )
    return True


async def fail_dispatch(
    database: Database,
    claim: DispatchClaim,
    *,
    delay_seconds: float,
    error_code: str = "publisher_unavailable",
) -> bool:
    """Keep publication failure durable with a bounded delay and sanitized code."""
    if (
        isinstance(delay_seconds, bool)
        or not isinstance(delay_seconds, (int, float))
        or not math.isfinite(delay_seconds)
        or not 0 <= delay_seconds <= 3600
    ):
        raise ValueError("delay_seconds must be finite and between 0 and 3600")
    if not isinstance(error_code, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", error_code) is None:
        raise ValueError("error_code must be a lowercase identifier between 1 and 64 characters")
    async with database.session_factory() as session, session.begin():
        if await _lock_dispatch(session, claim) is None:
            return False
        failed_id = await session.scalar(
            update(DispatchOutbox)
            .where(
                DispatchOutbox.id == claim.id,
                DispatchOutbox.job_id == claim.job_id,
                DispatchOutbox.status == DispatchStatus.PUBLISHING,
                DispatchOutbox.claim_token == claim.token,
                DispatchOutbox.lease_expires_at > func.clock_timestamp(),
            )
            .values(
                status=DispatchStatus.READY,
                available_at=func.clock_timestamp() + timedelta(seconds=delay_seconds),
                claim_token=None,
                lease_expires_at=None,
                last_error_code=error_code,
            )
            .returning(DispatchOutbox.id)
        )
    return failed_id is not None
