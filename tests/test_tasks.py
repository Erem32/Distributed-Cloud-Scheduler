import math

import pytest
from pydantic import JsonValue

from distributed_cloud_scheduler.tasks import TaskError, execute_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "result"),
    [
        ({"numbers": []}, {"sum": 0}),
        ({"numbers": [1, 2, 0.5, -1]}, {"sum": 2.5}),
        ({"numbers": [1_000_000_000] * 1000}, {"sum": 1_000_000_000_000}),
    ],
)
async def test_calculation_uses_bounded_numbers(
    payload: dict[str, JsonValue], result: dict[str, JsonValue]
) -> None:
    assert await execute_task("calculation", payload, 1) == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("job_type", "payload"),
    [
        ("calculation", {}),
        ("calculation", {"numbers": [True]}),
        ("calculation", {"numbers": ["1"]}),
        ("calculation", {"numbers": [math.nan]}),
        ("calculation", {"numbers": [math.inf]}),
        ("calculation", {"numbers": [1_000_000_001]}),
        ("calculation", {"numbers": [-1_000_000_001]}),
        ("calculation", {"numbers": [0] * 1001}),
        ("calculation", {"numbers": [], "extra": 1}),
        ("sleep", {"seconds": True}),
        ("sleep", {"seconds": "1"}),
        ("sleep", {"seconds": -1}),
        ("sleep", {"seconds": 10.1}),
        ("sleep", {"seconds": math.nan}),
        ("sleep", {"seconds": math.inf}),
        ("controlled_failure", {"fail_until_attempt": True}),
        ("controlled_failure", {"fail_until_attempt": "1"}),
        ("controlled_failure", {"fail_until_attempt": -1}),
        ("controlled_failure", {"fail_until_attempt": 21}),
        ("controlled_failure", {"fail_until_attempt": 1.5}),
    ],
)
async def test_invalid_handler_payloads_have_safe_permanent_error(
    job_type: str, payload: dict[str, JsonValue]
) -> None:
    with pytest.raises(TaskError) as caught:
        await execute_task(job_type, payload, 1)
    assert caught.value.code == "invalid_payload"
    assert caught.value.retryable is False
    assert str(caught.value) == "invalid_payload"


@pytest.mark.asyncio
async def test_sleep_is_awaited_and_returns_bounded_metadata() -> None:
    assert await execute_task("sleep", {"seconds": 0}, 1) == {"slept_seconds": 0}


@pytest.mark.asyncio
async def test_controlled_failure_changes_at_requested_attempt() -> None:
    with pytest.raises(TaskError) as caught:
        await execute_task("controlled_failure", {"fail_until_attempt": 2}, 2)
    assert caught.value.code == "controlled_failure"
    assert caught.value.retryable is True
    assert await execute_task("controlled_failure", {"fail_until_attempt": 2}, 3) == {"attempt": 3}
    assert await execute_task("controlled_failure", {"fail_until_attempt": 0}, 1) == {"attempt": 1}


@pytest.mark.asyncio
async def test_unknown_task_has_safe_permanent_error() -> None:
    with pytest.raises(TaskError) as caught:
        await execute_task("unimplemented", {"secret": "do not expose"}, 1)
    assert caught.value.code == "unknown_task"
    assert caught.value.retryable is False
    assert str(caught.value) == "unknown_task"
