"""Priority extends public submissions without changing default submission identity."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.idempotency import submission_fingerprint
from distributed_cloud_scheduler.jobs import JobCreate

IMMEDIATE_DIGEST = "5dfcf9a1ceff59c1889e385e9eee08c5013f2936a9a97a64a3e83a59b956c75a"
SCHEDULED_DIGEST = "0db6a9ce94186dfe80758f0556914f1c5adf26d265697fba19b95f9215fe4809"
SCHEDULE = datetime(2030, 2, 3, 4, 5, 6, 123456, tzinfo=UTC)


@pytest.mark.parametrize("priority", range(10))
def test_priority_accepts_only_the_bounded_integer_levels(priority: int) -> None:
    submission = JobCreate.model_validate(
        {"type": "calculation", "payload": {}, "priority": priority}
    )
    assert submission.priority == priority


@pytest.mark.parametrize("priority", [None, -1, 10, 2**100, True, False, 1.0, 1.5, "1", [], {}])
def test_priority_rejects_null_coercions_and_out_of_range_values(priority: object) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": {}, "priority": priority})


@pytest.mark.parametrize(
    "scheduled_at, expected_digest",
    [
        (None, IMMEDIATE_DIGEST),
        (SCHEDULE, SCHEDULED_DIGEST),
    ],
)
def test_omitted_and_explicit_zero_preserve_actual_pre_priority_fingerprints(
    scheduled_at: datetime | None,
    expected_digest: str,
) -> None:
    body = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}, "scheduled_at": scheduled_at}
    omitted = JobCreate.model_validate(body)
    explicit = JobCreate.model_validate({**body, "priority": 0})
    assert omitted.priority == explicit.priority == 0
    assert submission_fingerprint(omitted) == expected_digest
    assert submission_fingerprint(explicit) == expected_digest


def test_nonzero_priority_is_part_of_submission_identity_and_keeps_offset_equivalence() -> None:
    body = {"type": "calculation", "payload": {"numbers": [1, 2, 3]}, "scheduled_at": SCHEDULE}
    hashes = [
        submission_fingerprint(JobCreate.model_validate({**body, "priority": priority}))
        for priority in range(10)
    ]
    assert len(set(hashes)) == 10
    equivalent = JobCreate.model_validate(
        {**body, "priority": 9, "scheduled_at": "2030-02-03T06:05:06.123456+02:00"}
    )
    assert submission_fingerprint(equivalent) == hashes[9]


def test_openapi_exposes_bounded_priority_and_hides_internal_readiness() -> None:
    app = create_app(
        Settings(
            database_url="postgresql+psycopg://test_user:dummy@localhost/scheduler_test",
            _env_file=None,
        )
    )
    schemas = app.openapi()["components"]["schemas"]
    priority = schemas["JobCreate"]["properties"]["priority"]
    assert priority["type"] == "integer"
    assert priority["default"] == 0
    assert priority["minimum"] == 0 and priority["maximum"] == 9
    assert "priority" not in schemas["JobCreate"].get("required", [])
    assert schemas["JobRead"]["properties"]["priority"]["type"] == "integer"
    assert "ready_at" not in schemas["JobCreate"]["properties"]
    assert "ready_at" not in schemas["JobRead"]["properties"]
