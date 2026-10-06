"""Persisted jobs, public schemas, and focused transactional data access."""

import json
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator
from sqlalchemy import CheckConstraint, DateTime, Enum, Index, Integer, String, func, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from distributed_cloud_scheduler.database import Base

MAX_PAYLOAD_BYTES = 64 * 1024


class JobStatus(StrEnum):
    """Lifecycle vocabulary for persisted job transitions."""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
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
        Index("ix_jobs_created_at_id", "created_at", "id"),
        Index(
            "ix_jobs_running_lease",
            "lease_expires_at",
            "id",
            postgresql_where=text("status = 'RUNNING'"),
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
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
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
    attempt_count: int
    max_attempts: int
    result_metadata: dict[str, JsonValue] | None = None
    error_info: dict[str, JsonValue] | None = None

    @field_validator("created_at", "started_at", "finished_at")
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
    """Commit a PENDING job and its dispatch obligation without publishing."""
    # The outbox's dispatch functions use Job; defer this model import to avoid a cycle.
    from distributed_cloud_scheduler.outbox import DispatchOutbox

    async with session.begin():
        job = Job(
            type=submission.type,
            payload=submission.payload,
            max_attempts=submission.max_attempts,
        )
        session.add(job)
        await session.flush()
        session.add(DispatchOutbox(job_id=job.id))
        await session.flush()
        await session.refresh(job)
    return job


async def get_job(session: AsyncSession, job_id: UUID) -> Job | None:
    """Read a job by its identifier."""
    return await session.get(Job, job_id)


async def list_jobs(session: AsyncSession, limit: int, offset: int) -> list[Job]:
    """List newest jobs first with a UUID tie-breaker for equal timestamps."""
    result = await session.scalars(
        select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(limit).offset(offset)
    )
    return list(result)
