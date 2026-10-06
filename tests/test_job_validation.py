import math

import pytest
from pydantic import ValidationError

from distributed_cloud_scheduler.jobs import JobCreate


def test_valid_submission_preserves_json_and_defaults() -> None:
    payload = {"nested": [True, None, 1, 1.5, {"message": "Zażółć"}]}
    submission = JobCreate.model_validate({"type": "calculation", "payload": payload})
    assert submission.type == "calculation"
    assert submission.payload == payload
    assert submission.max_attempts == 3


@pytest.mark.parametrize("job_type", ["a", "a" * 64, "controlled_failure", "http_request", "job_2"])
def test_task_type_valid_boundaries(job_type: str) -> None:
    assert JobCreate.model_validate({"type": job_type, "payload": {}}).type == job_type


@pytest.mark.parametrize("job_type", ["", "a" * 65, "2job", "UPPERCASE", " sleep ", "http-request"])
def test_task_type_rejects_invalid_slugs(job_type: str) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": job_type, "payload": {}})


@pytest.mark.parametrize("max_attempts", [1, 20])
def test_attempt_budget_valid_boundaries(max_attempts: int) -> None:
    submission = JobCreate.model_validate(
        {"type": "calculation", "payload": {}, "max_attempts": max_attempts}
    )
    assert submission.max_attempts == max_attempts


@pytest.mark.parametrize("max_attempts", [0, 21, -1, True, False, 1.5, "3", None])
def test_attempt_budget_rejects_invalid_values(max_attempts: object) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate(
            {"type": "calculation", "payload": {}, "max_attempts": max_attempts}
        )


@pytest.mark.parametrize("payload", [None, [], "string", 1, True])
def test_payload_must_be_an_object(payload: object) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": payload})


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_nested_nonfinite_numbers_are_rejected(value: float) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": {"nested": [value]}})


@pytest.mark.parametrize(
    "payload",
    [
        {"nested": ["\x00"]},
        {"nested": {"\x00": "value"}},
        {"nested": ["\ud800"]},
        {"\ud800": "value"},
        {"nested": [object()]},
    ],
)
def test_payload_rejects_values_that_postgresql_jsonb_cannot_store(payload: object) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": payload})


def test_payload_budget_counts_compact_utf8_bytes() -> None:
    # {"data":""} occupies 11 bytes; each é takes two UTF-8 bytes.
    data = "é" * 32_762 + "a"
    submission = JobCreate.model_validate({"type": "calculation", "payload": {"data": data}})
    assert submission.payload["data"] == data
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": {"data": data + "a"}})


@pytest.mark.parametrize(
    "field", ["status", "attempt_count", "created_at", "priority", "scheduled_at"]
)
def test_client_cannot_set_state_or_unimplemented_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate({"type": "calculation", "payload": {}, field: "value"})


@pytest.mark.parametrize("submission", [{}, {"type": "calculation"}, {"payload": {}}])
def test_submission_requires_type_and_payload(submission: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate(submission)
