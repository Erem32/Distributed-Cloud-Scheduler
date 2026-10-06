"""Persisted jobs, public schemas, and focused transactional data access."""

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from distributed_cloud_scheduler.database import Base
from distributed_cloud_scheduler.idempotency import (
    IdempotencyConflict,
    submission_fingerprint,
    validate_idempotency_key,
)

MAX_PAYLOAD_BYTES = 64 * 1024
_SCHEDULE_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)


class JobStatus(StrEnum):
    """Lifecycle vocabulary for persisted job transitions."""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"


class Job(Base):
    """Authoritative persisted submission and execution state."""

    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "char_length(type) BETWEEN 1 AND 64 AND type ~ '^[a-z][a-z0-9_]*$'",
            name="valid_type",
        ),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        CheckConstraint("max_attempts BETWEEN 1 AND 20", name="max_attempts_range"),
        CheckConstraint("generation >= 0", name="generation_nonnegative"),
        CheckConstraint(
            "status <> 'SCHEDULED' OR (scheduled_at IS NOT NULL AND attempt_count = 0 "
            "AND generation = 0 AND started_at IS NULL AND finished_at IS NULL "
            "AND result_metadata IS NULL AND error_info IS NULL)",
            name="scheduled_state",
        ),
        CheckConstraint(
            "(dead_lettered_at IS NULL AND dead_letter_reason IS NULL) OR "
            "(status = 'DEAD_LETTERED' AND dead_lettered_at IS NOT NULL "
            "AND dead_letter_reason IS NOT NULL AND dead_letter_reason = 'attempts_exhausted')",
            name="dead_letter_context",
        ),
        CheckConstraint(
            "attempt_count >= 0 AND attempt_count <= max_attempts", name="attempt_count_range"
        ),
        CheckConstraint(
            "(status = 'RUNNING' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND started_at IS NOT NULL AND lease_expires_at > started_at) OR "
            "(status <> 'RUNNING' AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name="running_ownership",
        ),
        CheckConstraint(
            "result_metadata IS NULL OR jsonb_typeof(result_metadata) = 'object'",
            name="result_metadata_object",
        ),
        CheckConstraint(
            "error_info IS NULL OR jsonb_typeof(error_info) = 'object'", name="error_info_object"
        ),
        CheckConstraint(
            "(status = 'RETRY_WAIT' AND next_attempt_at IS NOT NULL "
            "AND attempt_count > 0 AND attempt_count < max_attempts) OR "
            "(status <> 'RETRY_WAIT' AND next_attempt_at IS NULL)",
            name="retry_wait_schedule",
        ),
        CheckConstraint(
            "(idempotency_key IS NULL AND request_fingerprint IS NULL) OR "
            "(idempotency_key IS NOT NULL AND request_fingerprint IS NOT NULL)",
            name="idempotency_metadata_paired",
        ),
        CheckConstraint(
            "idempotency_key IS NULL OR (char_length(idempotency_key) BETWEEN 1 AND 128 "
            "AND idempotency_key !~ '[^A-Za-z0-9._:-]')",
            name="idempotency_key_format",
        ),
        CheckConstraint(
            "request_fingerprint IS NULL OR (char_length(request_fingerprint) = 64 "
            "AND request_fingerprint ~ '^[0-9a-f]{64}$')",
            name="request_fingerprint_format",
        ),
        UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
        Index("ix_jobs_created_at_id", "created_at", "id"),
        Index(
            "ix_jobs_scheduled_due", "scheduled_at", "id",
            postgresql_where=text("status = 'SCHEDULED'"),
        ),
        Index(
            "ix_jobs_running_lease",
            "lease_expires_at",
            "id",
            postgresql_where=text("status = 'RUNNING'"),
        ),
        Index(
            "ix_jobs_retry_due",
            "next_attempt_at",
            "id",
            postgresql_where=text("status = 'RETRY_WAIT'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(
            JobStatus,
            name="job_status",
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
        default=JobStatus.PENDING,
        server_default=text("'PENDING'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_letter_reason: Mapped[str | None] = mapped_column(String(64))
    generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(128, collation="C"))
    request_fingerprint: Mapped[str | None] = mapped_column(String(64))
    claim_token: Mapped[UUID | None] = mapped_column(nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_metadata: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    error_info: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )


def validate_json_object(
    value: dict[str, JsonValue], label: str = "payload"
) -> dict[str, JsonValue]:
    """Bound task data and outcomes to JSON objects PostgreSQL can safely store."""
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    # Serialize first so circular structures cannot trap the validation walk.
    try:
        encoded = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError):
        raise ValueError(f"{label} must contain finite JSON values and valid Unicode") from None
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise ValueError(f"{label} must fit within {MAX_PAYLOAD_BYTES} UTF-8 JSON bytes")
    pending: list[JsonValue] = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if "\x00" in item:
                raise ValueError(f"{label} strings and keys must not contain the NUL character")
        elif isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ValueError(f"{label} JSON object keys must be strings")
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif item is not None and not isinstance(item, (bool, int, float)):
            raise ValueError(f"{label} must contain JSON values")
    return value


class JobCreate(BaseModel):
    """A submission contains task data, never caller-controlled execution state."""

    model_config = ConfigDict(extra="forbid", strict=True)

    type: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    payload: dict[str, JsonValue]
    max_attempts: int = Field(default=3, ge=1, le=20)
    scheduled_at: AwareDatetime | None = Field(
        default=None,
        description=(
            "Original execution date: RFC 3339 with an offset and at most six fractional digits. "
            "Every non-null date requires scheduler admission, including past dates."
        ),
        examples=["2030-01-01T12:00:00.000000Z"],
    )

    @field_validator("scheduled_at", mode="before")
    @classmethod
    def parse_schedule(cls, value: object) -> object:
        """Accept aware datetimes or explicit RFC 3339 text, never epoch coercion."""
        if value is None or isinstance(value, datetime):
            return value
        if not isinstance(value, str) or _SCHEDULE_TIMESTAMP.fullmatch(value) is None:
            raise ValueError(
                "scheduled_at must be RFC 3339 with an offset and microsecond precision"
            )
        try:
            return datetime.fromisoformat(value.upper())
        except ValueError:
            raise ValueError("scheduled_at must be a valid RFC 3339 timestamp") from None

    @field_validator("scheduled_at")
    @classmethod
    def normalize_schedule(cls, value: datetime | None) -> datetime | None:
        """Choose one bounded UTC instant before persistence and request fingerprinting."""
        try:
            return value.astimezone(UTC) if value is not None else None
        except (OverflowError, ValueError):
            raise ValueError("scheduled_at must fit within the UTC datetime range") from None

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Bound compact UTF-8 JSON and reject values PostgreSQL JSONB cannot store."""
        return validate_json_object(value)


class JobRead(BaseModel):
    """Public persisted job representation with UTC timestamps."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: str
    payload: dict[str, JsonValue]
    status: JobStatus
    created_at: AwareDatetime
    started_at: AwareDatetime | None
    finished_at: AwareDatetime | None
    next_attempt_at: AwareDatetime | None = None
    scheduled_at: AwareDatetime | None = None
    dead_lettered_at: AwareDatetime | None = None
    dead_letter_reason: str | None = None
    generation: int = 0
    attempt_count: int
    max_attempts: int
    result_metadata: dict[str, JsonValue] | None = None
    error_info: dict[str, JsonValue] | None = None

    @field_validator(
        "created_at", "started_at", "finished_at", "next_attempt_at", "dead_lettered_at",
        "scheduled_at",
    )
    @classmethod
    def normalize_timestamps(cls, value: datetime | None) -> datetime | None:
        """Keep responses in UTC even if a database session uses another time zone."""
        return value.astimezone(UTC) if value is not None else None


class JobList(BaseModel):
    """A bounded, deterministically ordered page of jobs."""

    items: list[JobRead]
    limit: int
    offset: int


async def create_job(session: AsyncSession, submission: JobCreate) -> Job:
    """Commit immediate dispatch or durable scheduled admission intent without publishing."""
    # The outbox's dispatch functions use Job; defer this model import to avoid a cycle.
    from distributed_cloud_scheduler.outbox import DispatchOutbox

    async with session.begin():
        job = Job(
            type=submission.type,
            payload=submission.payload,
            max_attempts=submission.max_attempts,
            scheduled_at=submission.scheduled_at,
            status=(
                JobStatus.SCHEDULED
                if submission.scheduled_at is not None
                else JobStatus.PENDING
            ),
        )
        session.add(job)
        await session.flush()
        if submission.scheduled_at is None:
            session.add(DispatchOutbox(job_id=job.id))
        await session.flush()
        await session.refresh(job)
    return job


@dataclass(frozen=True)
class SubmissionResult:
    """A committed new job or the current representation of its matching replay."""

    job: Job
    created: bool


async def create_or_replay_job(
    session: AsyncSession,
    submission: JobCreate,
    *,
    idempotency_key: str | None = None,
) -> SubmissionResult:
    """Commit one job/outbox per key, resolving concurrent submissions in PostgreSQL."""
    if idempotency_key is None:
        return SubmissionResult(job=await create_job(session, submission), created=True)

    key = validate_idempotency_key(idempotency_key)
    fingerprint = submission_fingerprint(submission)
    from distributed_cloud_scheduler.outbox import DispatchOutbox

    async with session.begin():
        statement = (
            insert(Job)
            .values(
                type=submission.type,
                payload=submission.payload,
                max_attempts=submission.max_attempts,
                scheduled_at=submission.scheduled_at,
                status=(
                    JobStatus.SCHEDULED
                    if submission.scheduled_at is not None
                    else JobStatus.PENDING
                ),
                idempotency_key=key,
                request_fingerprint=fingerprint,
            )
            .on_conflict_do_nothing(constraint="uq_jobs_idempotency_key")
            .returning(Job)
        )
        job = (await session.scalars(statement)).one_or_none()
        created = job is not None
        if job is not None:
            if submission.scheduled_at is None:
                session.add(DispatchOutbox(job_id=job.id))
            await session.flush()
        else:
            # READ COMMITTED gives this separate statement a snapshot containing the
            # winner whose uncommitted unique-index entry may have blocked our INSERT.
            job = (await session.scalars(select(Job).where(Job.idempotency_key == key))).one()
            if job.request_fingerprint != fingerprint:
                raise IdempotencyConflict
    return SubmissionResult(job=job, created=created)


async def get_job(session: AsyncSession, job_id: UUID) -> Job | None:
    """Read a job by its identifier."""
    return await session.get(Job, job_id)


async def list_jobs(session: AsyncSession, limit: int, offset: int) -> list[Job]:
    """List newest jobs first with a UUID tie-breaker for equal timestamps."""
    result = await session.scalars(
        select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(limit).offset(offset)
    )
    return list(result)
