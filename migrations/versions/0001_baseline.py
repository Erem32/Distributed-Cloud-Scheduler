"""Establish a migration chain before introducing domain tables.

Revision ID: 0001
Revises: None
"""

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Alembic creates its version table; there are no domain tables yet."""


def downgrade() -> None:
    """The baseline has no domain schema changes to undo."""
