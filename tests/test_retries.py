"""Retry delay boundaries and configuration keep execution budgets deterministic."""

import math

import pytest
from pydantic import ValidationError

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.retries import RetryPolicy


@pytest.mark.parametrize("attempt,cap", [(1, 2), (2, 4), (3, 8), (4, 10), (20, 10)])
@pytest.mark.parametrize("sample,fraction", [(0, 0.5), (0.5, 0.75), (1, 1)])
def test_equal_jitter_has_positive_floor_and_hard_exponential_cap(
    attempt: int,
    cap: float,
    sample: float,
    fraction: float,
) -> None:
    assert RetryPolicy(2, 10).delay_seconds(attempt, sample=sample) == cap * fraction


def test_production_delay_samples_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def sample() -> float:
        nonlocal calls
        calls += 1
        return 0.25

    monkeypatch.setattr("distributed_cloud_scheduler.retries.random.random", sample)
    assert RetryPolicy(2, 10).delay_seconds(2) == 2.5
    assert calls == 1


@pytest.mark.parametrize("attempt", [0, 21, -1, 1.5, True])
def test_invalid_attempt_numbers_do_not_produce_a_delay(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt_number"):
        RetryPolicy().delay_seconds(attempt, sample=0)


@pytest.mark.parametrize("sample", [-0.1, 1.1, math.nan, math.inf, True])
def test_invalid_random_samples_do_not_produce_a_delay(sample: float) -> None:
    with pytest.raises(ValueError, match="sample"):
        RetryPolicy().delay_seconds(1, sample=sample)


@pytest.mark.parametrize(
    "base,maximum",
    [
        (0, 60),
        (0.0001, 60),
        (-1, 60),
        (True, 60),
        (math.nan, 60),
        (math.inf, 60),
        (1, 0),
        (1, 301),
        (1, math.nan),
        (1, math.inf),
        (2, 1),
    ],
)
def test_invalid_policy_is_rejected_before_any_execution(base: float, maximum: float) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(base, maximum)


@pytest.mark.parametrize("field", ["execution_retry_base_seconds", "execution_retry_max_seconds"])
@pytest.mark.parametrize("value", [0, 0.0001, -1, 301, math.nan, math.inf])
def test_execution_retry_settings_have_finite_positive_limits(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+psycopg://test:dummy@127.0.0.1:1/scheduler_test",
            _env_file=None,
            **{field: value},
        )


def test_execution_retry_settings_are_separate_from_publication_retry_settings() -> None:
    configured = Settings(
        database_url="postgresql+psycopg://test:dummy@127.0.0.1:1/scheduler_test",
        _env_file=None,
        execution_retry_base_seconds=2,
        execution_retry_max_seconds=10,
        dispatch_retry_base_seconds=5,
        dispatch_retry_max_seconds=30,
    )
    assert configured.execution_retry_policy == RetryPolicy(2, 10)
    assert configured.execution_retry_policy.delay_seconds(2, sample=1) == 4


def test_execution_retry_settings_reject_inverted_delay_bounds() -> None:
    with pytest.raises(ValidationError, match="EXECUTION_RETRY_MAX_SECONDS"):
        Settings(
            database_url="postgresql+psycopg://test:dummy@127.0.0.1:1/scheduler_test",
            _env_file=None,
            execution_retry_base_seconds=10,
            execution_retry_max_seconds=2,
        )
