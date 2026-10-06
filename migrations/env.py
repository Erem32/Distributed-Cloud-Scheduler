"""Run migrations explicitly using the application's environment configuration."""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from distributed_cloud_scheduler import (
    attempts,  # noqa: F401 -- register all model metadata
    jobs,
    outbox,  # noqa: F401 -- register all model metadata
)
from distributed_cloud_scheduler.config import Settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = jobs.Job.metadata


def run_migrations_offline() -> None:
    """Generate SQL without creating a database connection."""
    context.configure(
        url=Settings().database_url.get_secret_value(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Use psycopg's synchronous driver for this independent command."""
    # A direct URL avoids ConfigParser interpolation of percent-encoded credentials.
    engine = create_engine(
        Settings().database_url.get_secret_value(),
        poolclass=NullPool,
    )
    try:
        with engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
