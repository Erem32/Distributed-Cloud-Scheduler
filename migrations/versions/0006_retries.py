"""Add durable retry timing and execution attempt history.

Revision ID: 0006
Revises: 0005

Existing RUNNING ownership with a positive count can backfill its current attempt.
Completed historical attempts and zero-count non-application claims are not invented.
Downgrade stops retry-waiting jobs
as FAILED while preserving their last error, attempt count and all outbox rows;
the older dispatcher skips those now-terminal publication obligations.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Persist retry decisions and only the current known legacy RUNNING attempt."""
    op.add_column("jobs", sa.Column("next_attempt_at", sa.DateTime(timezone=True)))
    op.drop_constraint(op.f("ck_jobs_job_status"), "jobs", type_="check")
    op.create_check_constraint(
        op.f("ck_jobs_job_status"),
        "jobs",
        "status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
        "'DEAD_LETTERED', 'RETRY_WAIT')",
    )
    op.create_check_constraint(
        op.f("ck_jobs_retry_wait_schedule"),
        "jobs",
        "(status = 'RETRY_WAIT' AND next_attempt_at IS NOT NULL AND attempt_count > 0 "
        "AND attempt_count < max_attempts) OR "
        "(status <> 'RETRY_WAIT' AND next_attempt_at IS NULL)",
    )
    op.create_index(
        "ix_jobs_retry_due", "jobs", ["next_attempt_at", "id"],
        postgresql_where=sa.text("status = 'RETRY_WAIT'"),
    )
    op.create_table(
        "job_attempts",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("claim_token", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "RUNNING", "SUCCEEDED", "FAILED", name=op.f("ck_job_attempts_attempt_status"),
                native_enum=False, create_constraint=True,
            ),
            nullable=False,
            server_default=sa.text("'RUNNING'"),
        ),
        sa.Column("result_metadata", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("error_info", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "attempt_number BETWEEN 1 AND 20", name=op.f("ck_job_attempts_attempt_number_range")
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name=op.f("ck_job_attempts_finished_after_start"),
        ),
        sa.CheckConstraint(
            "result_metadata IS NULL OR jsonb_typeof(result_metadata) = 'object'",
            name=op.f("ck_job_attempts_result_metadata_object"),
        ),
        sa.CheckConstraint(
            "error_info IS NULL OR jsonb_typeof(error_info) = 'object'",
            name=op.f("ck_job_attempts_error_info_object"),
        ),
        sa.CheckConstraint(
            "(status = 'RUNNING' AND finished_at IS NULL AND result_metadata IS NULL "
            "AND error_info IS NULL AND next_attempt_at IS NULL) OR "
            "(status = 'SUCCEEDED' AND finished_at IS NOT NULL AND result_metadata IS NOT NULL "
            "AND error_info IS NULL AND next_attempt_at IS NULL) OR "
            "(status = 'FAILED' AND finished_at IS NOT NULL AND result_metadata IS NULL "
            "AND error_info IS NOT NULL)",
            name=op.f("ck_job_attempts_outcome_state"),
        ),
        sa.CheckConstraint(
            "next_attempt_at IS NULL OR (status = 'FAILED' AND next_attempt_at >= finished_at)",
            name=op.f("ck_job_attempts_retry_schedule"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name=op.f("fk_job_attempts_job_id_jobs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("job_id", "attempt_number", name=op.f("pk_job_attempts")),
    )
    op.execute(sa.text(
        "INSERT INTO job_attempts (job_id, attempt_number, claim_token, started_at, status) "
        "SELECT id, attempt_count, claim_token, started_at, 'RUNNING' FROM jobs "
        "WHERE status = 'RUNNING' AND attempt_count > 0"
    ))


def downgrade() -> None:
    """Remove history/timing; stop pending retries without altering dispatch rows."""
    op.drop_table("job_attempts")
    op.drop_index("ix_jobs_retry_due", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_retry_wait_schedule"), "jobs", type_="check")
    op.execute(sa.text("UPDATE jobs SET status = 'FAILED' WHERE status = 'RETRY_WAIT'"))
    op.drop_constraint(op.f("ck_jobs_job_status"), "jobs", type_="check")
    op.create_check_constraint(
        op.f("ck_jobs_job_status"),
        "jobs",
        "status IN ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTERED')",
    )
    op.drop_column("jobs", "next_attempt_at")
