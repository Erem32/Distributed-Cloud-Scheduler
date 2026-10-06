"""Persist unique submission keys and immutable request fingerprints.

Revision ID: 0005
Revises: 0004
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Add optional identity metadata without assigning keys to existing jobs."""
    op.add_column("jobs", sa.Column("idempotency_key", sa.String(128, collation="C")))
    op.add_column("jobs", sa.Column("request_fingerprint", sa.String(64)))
    op.create_check_constraint(
        op.f("ck_jobs_idempotency_metadata_paired"),
        "jobs",
        "(idempotency_key IS NULL AND request_fingerprint IS NULL) OR "
        "(idempotency_key IS NOT NULL AND request_fingerprint IS NOT NULL)",
    )
    op.create_check_constraint(
        op.f("ck_jobs_idempotency_key_format"),
        "jobs",
        "idempotency_key IS NULL OR (char_length(idempotency_key) BETWEEN 1 AND 128 "
        "AND idempotency_key !~ '[^A-Za-z0-9._:-]')",
    )
    op.create_check_constraint(
        op.f("ck_jobs_request_fingerprint_format"),
        "jobs",
        "request_fingerprint IS NULL OR (char_length(request_fingerprint) = 64 "
        "AND request_fingerprint ~ '^[0-9a-f]{64}$')",
    )
    op.create_unique_constraint(op.f("uq_jobs_idempotency_key"), "jobs", ["idempotency_key"])


def downgrade() -> None:
    """Remove key bindings while retaining all jobs and dispatch obligations."""
    op.drop_constraint(op.f("uq_jobs_idempotency_key"), "jobs", type_="unique")
    op.drop_constraint(op.f("ck_jobs_request_fingerprint_format"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_idempotency_key_format"), "jobs", type_="check")
    op.drop_constraint(op.f("ck_jobs_idempotency_metadata_paired"), "jobs", type_="check")
    op.drop_column("jobs", "request_fingerprint")
    op.drop_column("jobs", "idempotency_key")
