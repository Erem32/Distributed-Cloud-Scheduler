"""Scheduled dates and submission identity have a precise, portable public contract."""

from datetime import UTC, date, datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.idempotency import submission_fingerprint
from distributed_cloud_scheduler.jobs import JobCreate

URL = "postgresql+psycopg://test_user:dummy@localhost/scheduler_test"
INSTANT = datetime(2030, 2, 3, 4, 5, 6, 123456, tzinfo=UTC)
LEGACY_DIGEST = "5dfcf9a1ceff59c1889e385e9eee08c5013f2936a9a97a64a3e83a59b956c75a"


@pytest.mark.parametrize(
    "value",
    [
        "2030-02-03T04:05:06.123456Z",
        "2030-02-03t04:05:06.123456z",
        "2030-02-03T06:05:06.123456+02:00",
        "2030-02-02T23:05:06.123456-05:00",
        INSTANT,
        INSTANT.astimezone(timezone(timedelta(hours=2))),
    ],
)
def test_scheduled_instants_normalize_offsets_to_utc(value: object) -> None:
    submission = JobCreate.model_validate(
        {"type": "calculation", "payload": {}, "scheduled_at": value}
    )
    assert submission.scheduled_at == INSTANT
    assert submission.scheduled_at.tzinfo is UTC


@pytest.mark.parametrize(
    "value",
    [
        "2030-02-03T04:05:06Z",
        "2030-02-03T04:05:06.1Z",
        "2030-02-03T04:05:06.123Z",
        "2030-02-03T04:05:06.123456+00:00",
        "0001-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999Z",
    ],
)
def test_scheduled_dates_support_explicit_offsets_and_microsecond_precision(value: str) -> None:
    submission = JobCreate.model_validate_json(
        '{"type":"calculation","payload":{},"scheduled_at":"' + value + '"}'
    )
    assert submission.scheduled_at is not None
    assert submission.scheduled_at.tzinfo is UTC


@pytest.mark.parametrize(
    "value",
    [
        "2030-02-03",
        "2030-02-03T04:05:06",
        "2030-02-03 04:05:06Z",
        "2030-02-03T04:05Z",
        "2030-02-03T04:05:06+0200",
        "2030-02-03T04:05:06+02",
        "2030-02-03T04:05:06.1234567Z",
        "2030-02-03T04:05:06,1Z",
        "2030-02-03T04:05:60Z",
        "2030-02-03T24:00:00Z",
        "2030-02-30T04:05:06Z",
        "2030-02-03T04:05:06+24:00",
        "2030-02-03T04:05:06+02:60",
        "2030-02-03T04:05:06Z\n",
        " 2030-02-03T04:05:06Z",
        "２０３０-02-03T04:05:06Z",
        "0001-01-01T00:00:00+01:00",
        "9999-12-31T23:59:59-01:00",
        "2030-W05-7T04:05:06Z",
        "20300203T040506Z",
        datetime(2030, 2, 3),
        datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        date(2030, 2, 3),
        1_900_000_000,
        1_900_000_000.0,
        True,
        False,
        [],
        {},
    ],
)
def test_scheduled_date_rejects_ambiguous_coerced_lossy_and_unrepresentable_values(
    value: object,
) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": {}, "scheduled_at": value})


def test_null_and_absent_schedule_preserve_exact_pre_milestone_10_digest() -> None:
    submission = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}}
    absent = JobCreate.model_validate(submission)
    explicit_null = JobCreate.model_validate({**submission, "scheduled_at": None})
    assert absent.scheduled_at is explicit_null.scheduled_at is None
    assert submission_fingerprint(absent) == LEGACY_DIGEST
    assert submission_fingerprint(explicit_null) == LEGACY_DIGEST


def test_same_instant_is_same_identity_but_schedule_and_microseconds_are_significant() -> None:
    submission = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}}

    def digest(value: object) -> str:
        return submission_fingerprint(
            JobCreate.model_validate({**submission, "scheduled_at": value})
        )

    original = digest("2030-02-03T04:05:06.123456Z")
    assert digest("2030-02-03T06:05:06.123456+02:00") == original
    assert digest(INSTANT) == original
    assert digest(INSTANT + timedelta(microseconds=1)) != original
    assert digest(None) != original
    assert digest("2030-02-03T04:05:06Z") == digest("2030-02-03T04:05:06.000000+00:00")


def test_scheduler_settings_default_to_bounded_local_polling() -> None:
    settings = Settings(database_url=URL, _env_file=None)
    assert settings.scheduler_batch_size == 100
    assert settings.scheduler_poll_interval_seconds == 0.5
    assert settings.scheduler_shutdown_timeout_seconds == 20


@pytest.mark.parametrize(
    "overrides",
    [
        {"scheduler_batch_size": 0},
        {"scheduler_batch_size": -1},
        {"scheduler_batch_size": 1001},
        {"scheduler_batch_size": 1.5},
        {"scheduler_poll_interval_seconds": 0},
        {"scheduler_poll_interval_seconds": -1},
        {"scheduler_poll_interval_seconds": 10.01},
        {"scheduler_poll_interval_seconds": float("inf")},
        {"scheduler_poll_interval_seconds": float("nan")},
        {"scheduler_shutdown_timeout_seconds": 0},
        {"scheduler_shutdown_timeout_seconds": 60.01},
        {"scheduler_shutdown_timeout_seconds": float("inf")},
        {"scheduler_shutdown_timeout_seconds": float("nan")},
    ],
)
def test_scheduler_configuration_rejects_unbounded_budgets(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=URL, _env_file=None, **overrides)


def test_scheduler_settings_accept_bounds_and_environment_precedence(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        f"DATABASE_URL={URL}\nSCHEDULER_BATCH_SIZE=1000\n"
        "SCHEDULER_POLL_INTERVAL_SECONDS=10\nSCHEDULER_SHUTDOWN_TIMEOUT_SECONDS=60\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SCHEDULER_BATCH_SIZE", "1")
    configured = Settings(_env_file=dotenv)
    assert configured.scheduler_batch_size == 1
    assert configured.scheduler_poll_interval_seconds == 10
    assert configured.scheduler_shutdown_timeout_seconds == 60


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1, 1001, 1.0, "1", True, False, None])
async def test_domain_batch_limit_rejects_coercion_before_database_access(limit: object) -> None:
    from distributed_cloud_scheduler.scheduler import schedule_due

    database = AsyncMock()
    with pytest.raises(ValueError, match="limit"):
        await schedule_due(database, limit=limit)
    database.session_factory.assert_not_called()
