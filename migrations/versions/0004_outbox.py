"""Persist durable publication obligations alongside jobs.

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create outbox state and backfill jobs that may still need execution."""
    op.create_table(
        "dispatch_outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "READY",
                "PUBLISHING",
                "PUBLISHED",
                "SKIPPED",
                name=op.f("ck_dispatch_outbox_dispatch_status"),
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
            server_default=sa.text("'READY'"),
        ),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "publish_attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("claim_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "publish_attempt_count >= 0",
            name=op.f("ck_dispatch_outbox_publish_attempt_count_range"),
        ),
        sa.CheckConstraint(
            "(status = 'PUBLISHING' AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status <> 'PUBLISHING' AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name=op.f("ck_dispatch_outbox_publishing_ownership"),
        ),
        sa.CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_]{0,63}$'",
            name=op.f("ck_dispatch_outbox_last_error_code_format"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name=op.f("fk_dispatch_outbox_job_id_jobs"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dispatch_outbox")),
    )
    op.create_index(
        "ix_dispatch_outbox_ready",
        "dispatch_outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("status = 'READY'"),
    )
    op.create_index(
        "ix_dispatch_outbox_publishing_lease",
        "dispatch_outbox",
        ["lease_expires_at", "id"],
        postgresql_where=sa.text("status = 'PUBLISHING'"),
    )
    # Existing UUIDs are unique within this new table; no extension is needed to backfill IDs.
    op.execute(
        "INSERT INTO dispatch_outbox (id, job_id) "
        "SELECT id, id FROM jobs WHERE status IN ('PENDING', 'QUEUED', 'RUNNING')"
    )


def downgrade() -> None:
    """Remove obligations while preserving the existing job/execution schema."""
    op.drop_index("ix_dispatch_outbox_publishing_lease", table_name="dispatch_outbox")
    op.drop_index("ix_dispatch_outbox_ready", table_name="dispatch_outbox")
    op.drop_table("dispatch_outbox")
