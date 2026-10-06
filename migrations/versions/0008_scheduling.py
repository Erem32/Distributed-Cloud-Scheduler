"""Persist delayed admission intent without changing existing jobs or obligations.

Revision ID: 0008
Revises: 0007

SCHEDULED is durable scheduler responsibility until due admission commits an
outbox obligation. Original scheduling metadata remains submission identity
after execution, so downgrade must refuse whenever that metadata exists.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add initial schedule metadata and a bounded due-selection index."""
    op.add_column("jobs", sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True))
    op.drop_constraint(op.f("ck_jobs_job_status"), "jobs", type_="check")
    op.create_check_constraint(
        op.f("ck_jobs_job_status"),
        "jobs",
        "status IN ('PENDING', 'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT', "
        "'SUCCEEDED', 'FAILED', 'DEAD_LETTERED')",
    )
    op.create_check_constraint(
        op.f("ck_jobs_scheduled_state"),
        "jobs",
        "status <> 'SCHEDULED' OR (scheduled_at IS NOT NULL AND attempt_count = 0 "
        "AND generation = 0 AND started_at IS NULL AND finished_at IS NULL "
        "AND result_metadata IS NULL AND error_info IS NULL)",
    )
    op.create_index(
        "ix_jobs_scheduled_due", "jobs", ["scheduled_at", "id"],
        postgresql_where=sa.text("status = 'SCHEDULED'"),
    )


def downgrade() -> None:
    """Refuse loss of scheduling identity; restore the previous null-only schema."""
    # Stop writers before checking: a submission or admission committed after a
    # snapshot must not lose its original date or immutable key interpretation.
    op.execute(sa.text(
        "LOCK TABLE jobs, job_attempts, dispatch_outbox IN ACCESS EXCLUSIVE MODE"
    ))
    op.execute(sa.text(
        "DO $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM jobs WHERE scheduled_at IS NOT NULL) THEN "
        "RAISE EXCEPTION 'Cannot downgrade 0008: scheduled submissions exist; "
        "retain original scheduling metadata and submission identity'; "
        "END IF; END $$"
    ))
    op.drop_index("ix_jobs_scheduled_due", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_scheduled_state"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_job_status"), "jobs", type_="check")
    op.create_check_constraint(
        op.f("ck_jobs_job_status"),
        "jobs",
        "status IN ('PENDING', 'QUEUED', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', "
        "'FAILED', 'DEAD_LETTERED')",
    )
    op.drop_column("jobs", "scheduled_at")
