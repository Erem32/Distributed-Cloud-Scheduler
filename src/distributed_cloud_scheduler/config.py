"""Validated environment configuration without default database credentials."""

from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from distributed_cloud_scheduler.retries import RetryPolicy


class Settings(BaseSettings):
    """Runtime settings read when an application is explicitly created."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: SecretStr
    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=30, allow_inf_nan=False)
    database_connect_timeout_seconds: int = Field(default=3, ge=1, le=30)
    worker_task_timeout_seconds: float = Field(default=15, gt=0, le=30, allow_inf_nan=False)
    worker_lease_seconds: int = Field(default=60, ge=1, le=300)
    worker_poll_interval_seconds: float = Field(default=0.5, gt=0, le=10, allow_inf_nan=False)
    worker_concurrency: int = Field(default=1, ge=1, le=20)
    worker_shutdown_timeout_seconds: float = Field(default=20, gt=0, le=60, allow_inf_nan=False)
    execution_retry_base_seconds: float = Field(default=1, ge=0.001, le=300, allow_inf_nan=False)
    execution_retry_max_seconds: float = Field(default=60, ge=0.001, le=300, allow_inf_nan=False)
    dispatch_publish_timeout_seconds: float = Field(default=10, gt=0, le=30, allow_inf_nan=False)
    dispatch_lease_seconds: int = Field(default=30, ge=1, le=300)
    dispatch_retry_base_seconds: float = Field(default=1, gt=0, le=30, allow_inf_nan=False)
    dispatch_retry_max_seconds: float = Field(default=60, gt=0, le=300, allow_inf_nan=False)
    dispatch_poll_interval_seconds: float = Field(default=0.5, gt=0, le=10, allow_inf_nan=False)
    dispatch_shutdown_timeout_seconds: float = Field(default=20, gt=0, le=60, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_lease_budget(self) -> Self:
        """Leave a margin to persist outcomes after the bounded task execution."""
        if self.worker_lease_seconds <= self.worker_task_timeout_seconds + 5:
            raise ValueError("WORKER_LEASE_SECONDS must exceed WORKER_TASK_TIMEOUT_SECONDS by 5")
        if self.dispatch_lease_seconds <= self.dispatch_publish_timeout_seconds + 5:
            raise ValueError(
                "DISPATCH_LEASE_SECONDS must exceed DISPATCH_PUBLISH_TIMEOUT_SECONDS by 5"
            )
        if self.dispatch_retry_max_seconds < self.dispatch_retry_base_seconds:
            raise ValueError(
                "DISPATCH_RETRY_MAX_SECONDS must be at least DISPATCH_RETRY_BASE_SECONDS"
            )
        if self.execution_retry_max_seconds < self.execution_retry_base_seconds:
            raise ValueError(
                "EXECUTION_RETRY_MAX_SECONDS must be at least EXECUTION_RETRY_BASE_SECONDS"
            )
        return self

    @property
    def execution_retry_policy(self) -> RetryPolicy:
        """Pass a transport-independent retry policy into execution transactions."""
        return RetryPolicy(self.execution_retry_base_seconds, self.execution_retry_max_seconds)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        """Reject unsupported or incomplete URLs without including their secrets."""
        try:
            url = make_url(value.get_secret_value())
        except (ArgumentError, ValueError):
            raise ValueError("DATABASE_URL must be a valid PostgreSQL connection URL") from None
        if url.drivername != "postgresql+psycopg":
            raise ValueError("DATABASE_URL must use the postgresql+psycopg driver")
        if not url.host or any(character.isspace() for character in url.host):
            raise ValueError("DATABASE_URL must include a database host")
        if not url.database or not url.database.strip():
            raise ValueError("DATABASE_URL must include a database name")
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("DATABASE_URL must use a valid port")
        return value
