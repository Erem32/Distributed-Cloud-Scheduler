"""Add fenced execution ownership and persisted task outcomes.

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add nullable outcomes and ownership required only for RUNNING jobs."""
    op.add_column("jobs", sa.Column("claim_token", sa.Uuid(), nullable=True))
    op.add_column("jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "jobs", sa.Column("result_metadata", postgresql.JSONB(none_as_null=True), nullable=True)
    )
    op.add_column(
        "jobs", sa.Column("error_info", postgresql.JSONB(none_as_null=True), nullable=True)
    )
    op.create_check_constraint(
        op.f("ck_jobs_running_ownership"),
        "jobs",
        "(status = 'RUNNING' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL "
        "AND started_at IS NOT NULL AND lease_expires_at > started_at) OR "
        "(status <> 'RUNNING' AND claim_token IS NULL AND lease_expires_at IS NULL)",
    )
    op.create_check_constraint(
        op.f("ck_jobs_result_metadata_object"),
        "jobs",
        "result_metadata IS NULL OR jsonb_typeof(result_metadata) = 'object'",
    )
    op.create_check_constraint(
        op.f("ck_jobs_error_info_object"),
        "jobs",
        "error_info IS NULL OR jsonb_typeof(error_info) = 'object'",
    )
    op.create_index(
        "ix_jobs_running_lease",
        "jobs",
        ["lease_expires_at", "id"],
        postgresql_where=sa.text("status = 'RUNNING'"),
    )


def downgrade() -> None:
    """Remove ownership constraints before removing their columns."""
    op.drop_index("ix_jobs_running_lease", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_error_info_object"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_result_metadata_object"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_running_ownership"), "jobs", type_="check")
    op.drop_column("jobs", "error_info")
    op.drop_column("jobs", "result_metadata")
    op.drop_column("jobs", "lease_expires_at")
    op.drop_column("jobs", "claim_token")
