"""Durable execution attempt history with internal ownership and public diagnostics."""

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, JsonValue, field_validator
from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, Integer, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from distributed_cloud_scheduler.database import Base


class AttemptStatus(StrEnum):
    """An individual attempt ends independently of the job's retry decision."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class JobAttempt(Base):
    """One atomically admitted attempt, finalized only by its fenced owner or recovery."""

    __tablename__ = "job_attempts"
    __table_args__ = (
        CheckConstraint("attempt_number BETWEEN 1 AND 20", name="attempt_number_range"),
        CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at", name="finished_after_start"
        ),
        CheckConstraint(
            "result_metadata IS NULL OR jsonb_typeof(result_metadata) = 'object'",
            name="result_metadata_object",
        ),
        CheckConstraint(
            "error_info IS NULL OR jsonb_typeof(error_info) = 'object'", name="error_info_object"
        ),
        CheckConstraint(
            "(status = 'RUNNING' AND finished_at IS NULL AND result_metadata IS NULL "
            "AND error_info IS NULL AND next_attempt_at IS NULL) OR "
            "(status = 'SUCCEEDED' AND finished_at IS NOT NULL AND result_metadata IS NOT NULL "
            "AND error_info IS NULL AND next_attempt_at IS NULL) OR "
            "(status = 'FAILED' AND finished_at IS NOT NULL AND result_metadata IS NULL "
            "AND error_info IS NOT NULL)",
            name="outcome_state",
        ),
        CheckConstraint(
            "next_attempt_at IS NULL OR (status = 'FAILED' AND next_attempt_at >= finished_at)",
            name="retry_schedule",
        ),
    )

    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, primary_key=True)
    claim_token: Mapped[UUID] = mapped_column(nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[AttemptStatus] = mapped_column(
        Enum(
            AttemptStatus,
            name="attempt_status",
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
        ),
        nullable=False,
        default=AttemptStatus.RUNNING,
        server_default=text("'RUNNING'"),
    )
    result_metadata: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    error_info: Mapped[dict[str, JsonValue] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AttemptRead(BaseModel):
    """Public chronological history, excluding internal claim tokens and task payloads."""

    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    status: AttemptStatus
    started_at: AwareDatetime
    finished_at: AwareDatetime | None
    result_metadata: dict[str, JsonValue] | None
    error_info: dict[str, JsonValue] | None
    next_attempt_at: AwareDatetime | None

    @field_validator("started_at", "finished_at", "next_attempt_at")
    @classmethod
    def normalize_timestamps(cls, value: datetime | None) -> datetime | None:
        """Keep diagnostic timestamps in UTC regardless of the database session zone."""
        return value.astimezone(UTC) if value is not None else None


async def list_attempts(session: AsyncSession, job_id: UUID) -> list[JobAttempt]:
    """Read at most the twenty budgeted attempts, ordered by admission number."""
    attempts = await session.scalars(
        select(JobAttempt)
        .where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.attempt_number)
        .limit(20)
    )
    return list(attempts)
