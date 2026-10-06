"""Add persisted job submissions and lifecycle invariants.

Revision ID: 0002
Revises: 0001
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create the authoritative job table without execution infrastructure."""
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("type", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "QUEUED",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "DEAD_LETTERED",
                name=op.f("ck_jobs_job_status"),
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.CheckConstraint(
            "char_length(type) BETWEEN 1 AND 64 AND type ~ '^[a-z][a-z0-9_]*$'",
            name=op.f("ck_jobs_valid_type"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name=op.f("ck_jobs_payload_object")
        ),
        sa.CheckConstraint(
            "max_attempts BETWEEN 1 AND 20", name=op.f("ck_jobs_max_attempts_range")
        ),
        sa.CheckConstraint(
            "attempt_count >= 0 AND attempt_count <= max_attempts",
            name=op.f("ck_jobs_attempt_count_range"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
    )
    op.create_index("ix_jobs_created_at_id", "jobs", ["created_at", "id"])


def downgrade() -> None:
    """Remove the table and its non-native status constraint."""
    op.drop_index("ix_jobs_created_at_id", table_name="jobs")
    op.drop_table("jobs")
