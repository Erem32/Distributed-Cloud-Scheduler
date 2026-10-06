"""Configuration validation and environment precedence."""

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from distributed_cloud_scheduler.config import Settings

VALID_URL = "postgresql+psycopg://test_user:secret_password@localhost:5432/scheduler_test"


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError, match="database_url"):
        Settings(_env_file=None)


def test_database_url_is_secret() -> None:
    settings = Settings(database_url=VALID_URL, _env_file=None)
    assert isinstance(settings.database_url, SecretStr)
    assert settings.database_url.get_secret_value() == VALID_URL
    assert "secret_password" not in repr(settings)
    assert "secret_password" not in settings.model_dump_json()


@pytest.mark.parametrize(
    "url",
    [
        "",
        "not-a-url",
        "sqlite:///scheduler.db",
        "postgresql://test_user:secret_password@localhost/scheduler_test",
        "postgresql+asyncpg://test_user:secret_password@localhost/scheduler_test",
        "postgresql+psycopg://test_user:secret_password@/scheduler_test",
        "postgresql+psycopg://test_user:secret_password@localhost",
        "postgresql+psycopg://test_user:secret_password@localhost:notaport/scheduler_test",
        "postgresql+psycopg://test_user:secret_password@localhost:0/scheduler_test",
        "postgresql+psycopg://test_user:secret_password@localhost:65536/scheduler_test",
    ],
)
def test_invalid_database_urls_are_rejected_without_exposing_secrets(url: str) -> None:
    with pytest.raises(ValidationError) as error:
        Settings(database_url=url, _env_file=None)
    assert "secret_password" not in str(error.value)
    if url:
        assert url not in str(error.value)


def test_environment_overrides_dotenv_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "DATABASE_URL=postgresql+psycopg://dotenv_user:dummy@dotenv-host/dotenv_db\n"
        "READINESS_TIMEOUT_SECONDS=5\n"
        "UNRELATED_COMPOSE_SETTING=ignored\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DATABASE_URL", VALID_URL)
    monkeypatch.setenv("READINESS_TIMEOUT_SECONDS", "1.5")
    settings = Settings(_env_file=dotenv)
    assert settings.database_url.get_secret_value() == VALID_URL
    assert settings.readiness_timeout_seconds == 1.5


def test_dotenv_file_provides_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    dotenv = tmp_path / ".env"
    dotenv.write_text(f"DATABASE_URL={VALID_URL}\nUNRELATED=ignored\n", encoding="utf-8")
    assert Settings(_env_file=dotenv).database_url.get_secret_value() == VALID_URL


@pytest.mark.parametrize("timeout", [0, -1, 31, float("inf"), float("nan")])
def test_readiness_timeout_is_finite_positive_and_bounded(timeout: float) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=VALID_URL, readiness_timeout_seconds=timeout, _env_file=None)


@pytest.mark.parametrize("timeout", [0, -1, 31, 1.5])
def test_database_connect_timeout_is_a_bounded_positive_integer(timeout: float) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=VALID_URL, database_connect_timeout_seconds=timeout, _env_file=None)


@pytest.mark.parametrize(
    "overrides",
    [
        {"worker_task_timeout_seconds": 0},
        {"worker_task_timeout_seconds": float("inf")},
        {"worker_task_timeout_seconds": 31},
        {"worker_poll_interval_seconds": 0},
        {"worker_poll_interval_seconds": float("nan")},
        {"worker_poll_interval_seconds": 11},
        {"worker_lease_seconds": 301},
        {"worker_lease_seconds": 20, "worker_task_timeout_seconds": 15},
        {"worker_concurrency": 0},
        {"worker_concurrency": 21},
        {"worker_concurrency": 1.5},
        {"worker_shutdown_timeout_seconds": 0},
        {"worker_shutdown_timeout_seconds": float("inf")},
        {"worker_shutdown_timeout_seconds": 61},
    ],
)
def test_worker_budgets_are_bounded_and_allow_completion_margin(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=VALID_URL, _env_file=None, **overrides)


def test_worker_budget_can_be_configured() -> None:
    settings = Settings(
        database_url=VALID_URL,
        worker_task_timeout_seconds=10,
        worker_lease_seconds=30,
        worker_poll_interval_seconds=0.25,
        _env_file=None,
    )
    assert settings.worker_task_timeout_seconds == 10
    assert settings.worker_lease_seconds == 30
    assert settings.worker_poll_interval_seconds == 0.25


@pytest.mark.parametrize(
    "overrides",
    [
        {"dispatch_publish_timeout_seconds": 0},
        {"dispatch_publish_timeout_seconds": float("inf")},
        {"dispatch_publish_timeout_seconds": 31},
        {"dispatch_lease_seconds": 301},
        {"dispatch_lease_seconds": 15, "dispatch_publish_timeout_seconds": 10},
        {"dispatch_retry_base_seconds": 0},
        {"dispatch_retry_max_seconds": float("nan")},
        {"dispatch_retry_base_seconds": 10, "dispatch_retry_max_seconds": 5},
    ],
)
def test_dispatch_budgets_are_bounded_and_allow_completion_margin(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=VALID_URL, _env_file=None, **overrides)
