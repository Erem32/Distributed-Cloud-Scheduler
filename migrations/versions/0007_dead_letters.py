"""Retain manual-recovery generations and explicit application dead-letter context.

Revision ID: 0007
Revises: 0006

Historical jobs, attempts and publication obligations retain generation zero.
Legacy dead-letter rows keep unknown context NULL rather than invented evidence.
Downgrade refuses after manual recovery so generations cannot collide or disappear.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add generation fencing without changing historical outcomes or obligations."""
    for table_name in ("jobs", "job_attempts", "dispatch_outbox"):
        op.add_column(
            table_name,
            sa.Column("generation", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
        op.create_check_constraint(
            op.f(f"ck_{table_name}_generation_nonnegative"), table_name, "generation >= 0"
        )

    op.add_column("jobs", sa.Column("dead_lettered_at", sa.DateTime(timezone=True)))
    op.add_column("jobs", sa.Column("dead_letter_reason", sa.String(length=64)))
    op.create_check_constraint(
        op.f("ck_jobs_dead_letter_context"),
        "jobs",
        "(dead_lettered_at IS NULL AND dead_letter_reason IS NULL) OR "
        "(status = 'DEAD_LETTERED' AND dead_lettered_at IS NOT NULL "
        "AND dead_letter_reason IS NOT NULL AND dead_letter_reason = 'attempts_exhausted')",
    )
    op.drop_constraint(op.f("pk_job_attempts"), "job_attempts", type_="primary")
    op.create_primary_key(
        op.f("pk_job_attempts"), "job_attempts", ["job_id", "generation", "attempt_number"]
    )


def downgrade() -> None:
    """Refuse destructive collapse; otherwise restore generation-zero schema exactly."""
    # Stop writers before evaluating the guard. Otherwise a manual recovery could
    # commit after its snapshot and lose generation metadata during later DDL.
    # Coordinate stopped runtimes for migrations; PostgreSQL still safely aborts
    # a deadlock if an existing transaction holds these tables in another order.
    op.execute(sa.text(
        "LOCK TABLE jobs, job_attempts, dispatch_outbox IN ACCESS EXCLUSIVE MODE"
    ))
    op.execute(sa.text(
        "DO $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM jobs WHERE generation > 0) "
        "OR EXISTS (SELECT 1 FROM job_attempts WHERE generation > 0) "
        "OR EXISTS (SELECT 1 FROM dispatch_outbox WHERE generation > 0) THEN "
        "RAISE EXCEPTION 'Cannot downgrade 0007: manual recovery generations exist; "
        "retain generation history and publication obligations'; "
        "END IF; END $$"
    ))
    op.drop_constraint(op.f("pk_job_attempts"), "job_attempts", type_="primary")
    op.create_primary_key(op.f("pk_job_attempts"), "job_attempts", ["job_id", "attempt_number"])
    op.drop_constraint(op.f("ck_jobs_dead_letter_context"), "jobs", type_="check")
    op.drop_column("jobs", "dead_letter_reason")
    op.drop_column("jobs", "dead_lettered_at")
    for table_name in ("dispatch_outbox", "job_attempts", "jobs"):
        op.drop_constraint(
            op.f(f"ck_{table_name}_generation_nonnegative"), table_name, type_="check"
        )
        op.drop_column(table_name, "generation")
