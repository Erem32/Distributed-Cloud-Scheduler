"""Add immutable bounded priority and eligibility-episode aging metadata.

Revision ID: 0009
Revises: 0008

Default priority preserves retained submission identity. Historical readiness is
derived from the known retry deadline, recovery obligation or initial eligibility;
no attempt, publication record or original request field is rewritten.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Backfill known readiness and index immutable UTC priority rank expressions."""
    op.add_column(
        "jobs", sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0"))
    )
    op.create_check_constraint(
        op.f("ck_jobs_priority_range"), "jobs", "priority BETWEEN 0 AND 9"
    )
    op.add_column("jobs", sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(sa.text(
        "UPDATE jobs AS job SET ready_at = CASE "
        "WHEN job.status = 'RETRY_WAIT' THEN job.next_attempt_at "
        "WHEN job.generation > 0 THEN COALESCE("
        "(SELECT MIN(obligation.created_at) FROM dispatch_outbox AS obligation "
        "WHERE obligation.job_id = job.id AND obligation.generation = job.generation), "
        "GREATEST(job.created_at, job.scheduled_at)) "
        "ELSE GREATEST(job.created_at, job.scheduled_at) END"
    ))
    op.alter_column("jobs", "ready_at", nullable=False, server_default=sa.func.now())
    # timestamptz - interval depends on session timezone. Convert to a UTC wall
    # timestamp first so PostgreSQL can index an immutable expression and selectors
    # rank identically across replicas whose session timezones differ.
    rank = sa.text("((ready_at AT TIME ZONE 'UTC') - priority * INTERVAL '5 seconds')")
    op.create_index(
        "ix_jobs_scheduled_priority", "jobs", [rank, "ready_at", "id"],
        postgresql_where=sa.text("status = 'SCHEDULED'"),
    )
    op.create_index(
        "ix_jobs_available_priority", "jobs", [rank, "ready_at", "id"],
        postgresql_where=sa.text("status IN ('PENDING', 'QUEUED', 'RETRY_WAIT')"),
    )


def downgrade() -> None:
    """Restore default-only jobs without losing a nondefault submission identity."""
    op.execute(sa.text(
        "LOCK TABLE jobs, job_attempts, dispatch_outbox IN ACCESS EXCLUSIVE MODE"
    ))
    op.execute(sa.text(
        "DO $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM jobs WHERE priority <> 0) THEN "
        "RAISE EXCEPTION 'Cannot downgrade 0009: nondefault priorities exist; "
        "retain original priority and submission identity'; "
        "END IF; END $$"
    ))
    op.drop_index("ix_jobs_available_priority", table_name="jobs")
    op.drop_index("ix_jobs_scheduled_priority", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_priority_range"), "jobs", type_="check")
    op.drop_column("jobs", "ready_at")
    op.drop_column("jobs", "priority")
