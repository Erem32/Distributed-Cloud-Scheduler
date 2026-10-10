"""Reliability evidence reconciles seeded faults with every durable outcome."""

import argparse
import importlib.util
import json
import math
import random
import sys
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(scope="module")
def reliability() -> ModuleType:
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    specification = importlib.util.spec_from_file_location(
        "local_reliability_under_test", scripts / "local_reliability.py"
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    with pytest.MonkeyPatch.context() as patch:
        patch.syspath_prepend(str(scripts))
        specification.loader.exec_module(module)
    return module


def options(**changes: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "jobs": 300,
        "repetitions": 3,
        "concurrency": 5,
        "seed": 20261010,
        "run_timeout": 60.0,
        "suite_timeout": 900.0,
        "output": Path("reliability-results"),
    }
    values.update(changes)
    return argparse.Namespace(**values)


def test_fault_schedule_has_exact_reproducible_ten_percent_membership(
    reliability: ModuleType,
) -> None:
    recovery = reliability.fault_indices(300, 20261010)
    control = reliability.fault_indices(300, 20261010)
    assert recovery == control
    assert len(recovery) == len(set(recovery)) == 30
    assert recovery == sorted(recovery)
    assert all(0 <= index < 300 for index in recovery)
    assert recovery != reliability.fault_indices(300, 20261011)


@pytest.mark.parametrize("jobs", [10, 100, 1000])
def test_fault_schedule_count_is_exact_at_supported_batch_sizes(
    reliability: ModuleType, jobs: int
) -> None:
    assert len(reliability.fault_indices(jobs, 0)) == jobs // 10


def test_fault_schedule_does_not_seed_or_advance_runtime_global_randomness(
    reliability: ModuleType,
) -> None:
    before = random.getstate()
    try:
        reliability.fault_indices(300, 42)
        assert random.getstate() == before
    finally:
        random.setstate(before)


@pytest.mark.parametrize("attempt", [0, 1, 20])
def test_safe_payload_returns_only_a_copied_bounded_controlled_handler_integer(
    reliability: ModuleType, attempt: int
) -> None:
    original = {"fail_until_attempt": attempt}
    retained = reliability.safe_payload(original)
    assert retained == original
    assert retained is not original
    retained["fail_until_attempt"] = 21
    assert original == {"fail_until_attempt": attempt}


@pytest.mark.parametrize(
    "payload",
    [
        {"fail_until_attempt": True}, {"fail_until_attempt": False},
        {"fail_until_attempt": "1"}, {"fail_until_attempt": 1.0},
        {"fail_until_attempt": -1}, {"fail_until_attempt": 21},
        {"fail_until_attempt": 1, "token": "dummy-sensitive-payload"},
        {"password": "dummy-sensitive-payload"},
        {"fail_until_attempt": {"token": "dummy-sensitive-payload"}},
        {}, None, ["dummy-sensitive-payload"],
    ],
)
def test_safe_payload_redacts_unexpected_shapes_without_changing_the_original(
    reliability: ModuleType, payload: object
) -> None:
    before = deepcopy(payload)
    assert reliability.safe_payload(payload) is None
    assert payload == before


@pytest.mark.asyncio
async def test_snapshot_marks_unexpected_payload_redacted_and_retains_safe_control_input(
    reliability: ModuleType,
) -> None:
    original = [
        {"id": "00000000-0000-0000-0000-000000000001", "payload": {"fail_until_attempt": 1}},
        {
            "id": "00000000-0000-0000-0000-000000000002",
            "payload": {"token": "dummy-sensitive-payload"},
        },
    ]
    before = deepcopy(original)
    rows = [
        SimpleNamespace(mappings=lambda: original),
        SimpleNamespace(mappings=lambda: []),
        SimpleNamespace(mappings=lambda: []),
    ]
    connection = SimpleNamespace(execute=AsyncMock(side_effect=rows))
    connect = AsyncMock()
    connect.__aenter__.return_value = connection
    snapshot = await reliability.snapshot_database(SimpleNamespace(connect=lambda: connect))
    assert snapshot["jobs"][0]["payload"] == {"fail_until_attempt": 1}
    assert snapshot["jobs"][0].get("payload_redacted", False) is False
    assert snapshot["jobs"][1]["payload"] is None
    assert snapshot["jobs"][1]["payload_redacted"] is True
    assert "dummy-sensitive-payload" not in json.dumps(snapshot)
    assert original == before


def test_default_and_exact_boundary_options_are_valid(reliability: ModuleType) -> None:
    reliability.validate_options(options())
    reliability.validate_options(options(jobs=10, repetitions=1, seed=0, concurrency=1))
    reliability.validate_options(
        options(jobs=1000, repetitions=5, seed=2**32 - 5, concurrency=20, run_timeout=120)
    )
    reliability.validate_options(options(repetitions=1, seed=2**32 - 1))


@pytest.mark.parametrize(
    "changes",
    [
        {"jobs": 0}, {"jobs": 9}, {"jobs": 11}, {"jobs": 1001}, {"jobs": True},
        {"repetitions": 0}, {"repetitions": 6}, {"repetitions": True},
        {"concurrency": 0}, {"concurrency": 2}, {"concurrency": 21}, {"concurrency": True},
        {"seed": -1}, {"seed": 2**32}, {"seed": True}, {"seed": 1.5},
        {"seed": 2**32 - 1, "repetitions": 2},
        {"run_timeout": 0}, {"run_timeout": -1}, {"run_timeout": 120.1},
        {"run_timeout": math.nan}, {"run_timeout": math.inf}, {"run_timeout": True},
        {"suite_timeout": 0}, {"suite_timeout": -1}, {"suite_timeout": 900.1},
        {"suite_timeout": math.nan}, {"suite_timeout": math.inf}, {"suite_timeout": True},
        {"run_timeout": 100, "suite_timeout": 60},
    ],
)
def test_options_enforce_finite_batch_seed_concurrency_and_time_bounds(
    reliability: ModuleType, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        reliability.validate_options(options(**changes))


def timestamp(seconds: float) -> str:
    return (datetime(2026, 10, 10, tzinfo=UTC) + timedelta(seconds=seconds)).isoformat()


def cohort(max_attempts: int = 3) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """One recorded fault and nine ordinary successes; no database behavior is mocked."""
    jobs, attempts, outbox, submissions = [], [], [], []
    failure = {"code": "controlled_failure", "retryable": True}
    for index in range(10):
        job_id = f"00000000-0000-0000-0000-{index + 1:012d}"
        injected = index == 0
        recovered = injected and max_attempts == 3
        dead_lettered = injected and max_attempts == 1
        attempt_count = 2 if recovered else 1
        final_start = 3.25 if recovered else 1
        final_finish = 3.5 if recovered else 2 if dead_lettered else 1.25
        result = None if dead_lettered else {"attempt": attempt_count}
        jobs.append({
            "id": job_id, "type": "controlled_failure",
            "payload": {"fail_until_attempt": int(injected)},
            "status": "DEAD_LETTERED" if dead_lettered else "SUCCEEDED", "generation": 0,
            "attempt_count": attempt_count, "max_attempts": max_attempts, "priority": 0,
            "created_at": timestamp(0), "started_at": timestamp(final_start),
            "finished_at": timestamp(final_finish), "next_attempt_at": None, "scheduled_at": None,
            "dead_lettered_at": timestamp(final_finish) if dead_lettered else None,
            "dead_letter_reason": "attempts_exhausted" if dead_lettered else None,
            "result_metadata": result, "error_info": dict(failure) if dead_lettered else None,
        })
        attempts.append({
            "job_id": job_id, "generation": 0, "attempt_number": 1,
            "status": "FAILED" if injected else "SUCCEEDED",
            "started_at": timestamp(1), "finished_at": timestamp(2 if injected else 1.25),
            "next_attempt_at": timestamp(2.75) if recovered else None,
            "result_metadata": None if injected else {"attempt": 1},
            "error_info": dict(failure) if injected else None,
        })
        outbox.append({
            "id": f"10000000-0000-0000-0000-{index + 1:012d}", "job_id": job_id,
            "generation": 0, "status": "READY", "publish_attempt_count": 0,
            "created_at": timestamp(0), "available_at": timestamp(0),
        })
        if recovered:
            attempts.append({
                "job_id": job_id, "generation": 0, "attempt_number": 2, "status": "SUCCEEDED",
                "started_at": timestamp(3.25), "finished_at": timestamp(3.5),
                "next_attempt_at": None, "result_metadata": {"attempt": 2}, "error_info": None,
            })
            outbox.append({
                "id": "20000000-0000-0000-0000-000000000001", "job_id": job_id,
                "generation": 0, "status": "READY", "publish_attempt_count": 0,
                "created_at": timestamp(2), "available_at": timestamp(2.75),
            })
        submissions.append({"submission_index": index, "id": job_id, "injected_fault": injected})
    return jobs, attempts, outbox, submissions


@pytest.fixture
def recovery_records() -> tuple:
    return cohort(3)


@pytest.fixture
def control_records() -> tuple:
    return cohort(1)


def account(reliability: ModuleType, records: tuple, *, budget: int = 3) -> dict:
    return reliability.account_records(*records, max_attempts=budget, faults=[0])


def test_recovery_accounting_reconciles_retry_and_two_durable_intents(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    result = account(reliability, recovery_records)
    assert result["jobs"] == 10
    assert result["attempts"] == result["outbox"] == 11
    assert result["initially_succeeded"] == 9
    assert result["eventually_succeeded"] == 10
    assert result["dead_lettered"] == 0
    assert result["failed_attempts"] == result["automatic_retries"] == 1
    assert result["automatic_retry_intents"] == result["automatic_retry_executions"] == 1
    assert result["attempts_per_job_mean"] == pytest.approx(11 / 10)
    assert result["attempts_per_job_counts"] == {"1": 9, "2": 1}
    assert result["failed_attempt_rate"] == pytest.approx(1 / 11)
    assert result["initial_success_rate"] == pytest.approx(9 / 10)
    assert result["eventual_success_rate"] == 1


def test_expected_control_dead_letter_is_valid_experiment_accounting(
    reliability: ModuleType, control_records: tuple
) -> None:
    result = account(reliability, control_records, budget=1)
    assert result["jobs"] == result["attempts"] == result["outbox"] == 10
    assert result["initially_succeeded"] == result["eventually_succeeded"] == 9
    assert result["dead_lettered"] == result["failed_attempts"] == 1
    assert result["automatic_retries"] == 0
    assert result["automatic_retry_intents"] == result["automatic_retry_executions"] == 0
    assert result["attempts_per_job_mean"] == 1
    assert result["attempts_per_job_counts"] == {"1": 10, "2": 0}
    assert result["failed_attempt_rate"] == result["dead_letter_rate"] == pytest.approx(1 / 10)


@pytest.mark.parametrize("record_index", [0, 1, 2, 3])
def test_accounting_rejects_missing_duplicate_and_extra_records(
    reliability: ModuleType, recovery_records: tuple, record_index: int
) -> None:
    replacements = ([], recovery_records[record_index][:-1], recovery_records[record_index] * 2)
    for replacement in replacements:
        damaged = list(deepcopy(recovery_records))
        damaged[record_index] = replacement
        with pytest.raises(ValueError):
            account(reliability, tuple(damaged))


@pytest.mark.parametrize(
    "record_index, field, invalid_value",
    [
        (0, "id", "00000000-0000-0000-0000-999999999999"),
        (0, "type", "sleep"), (0, "priority", 1), (0, "max_attempts", 1),
        (0, "generation", 1), (0, "status", "RUNNING"), (0, "status", "FAILED"),
        (0, "attempt_count", 3), (0, "result_metadata", {"attempt": 1}),
        (0, "error_info", {"code": "worker_lost", "retryable": True}),
        (0, "next_attempt_at", timestamp(4)), (0, "scheduled_at", timestamp(0)),
        (0, "dead_lettered_at", timestamp(3.5)), (0, "dead_letter_reason", "attempts_exhausted"),
        (0, "created_at", timestamp(2)), (0, "started_at", timestamp(1)),
        (0, "finished_at", None),
        (1, "job_id", "00000000-0000-0000-0000-999999999999"),
        (1, "generation", 1), (1, "attempt_number", 2), (1, "status", "RUNNING"),
        (1, "result_metadata", {"attempt": 1}),
        (1, "error_info", {"code": "worker_lost", "retryable": True}),
        (1, "error_info", {"code": "controlled_failure", "retryable": False}),
        (1, "next_attempt_at", None), (1, "started_at", timestamp(2.5)),
        (1, "finished_at", None),
        (2, "job_id", "00000000-0000-0000-0000-999999999999"),
        (2, "generation", 1), (2, "status", "PUBLISHED"), (2, "publish_attempt_count", 1),
        (2, "available_at", timestamp(0.1)),
        (3, "id", "00000000-0000-0000-0000-999999999999"),
        (3, "submission_index", 1), (3, "injected_fault", False),
    ],
)
def test_accounting_rejects_unexpected_identity_execution_or_retry_context(
    reliability: ModuleType, recovery_records: tuple, record_index: int, field: str,
    invalid_value: object,
) -> None:
    recovery_records[record_index][0][field] = invalid_value
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


@pytest.mark.parametrize("delay", [0.49, 1.01])
def test_recovery_rejects_persisted_backoff_outside_equal_jitter_bounds(
    reliability: ModuleType, recovery_records: tuple, delay: float
) -> None:
    recovery_records[1][0]["next_attempt_at"] = timestamp(2 + delay)
    recovery_records[2][1]["available_at"] = timestamp(2 + delay)
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


@pytest.mark.parametrize("delay", [0.5, 1.0])
def test_recovery_accepts_both_equal_jitter_boundaries(
    reliability: ModuleType, recovery_records: tuple, delay: float
) -> None:
    recovery_records[1][0]["next_attempt_at"] = timestamp(2 + delay)
    recovery_records[2][1]["available_at"] = timestamp(2 + delay)
    assert account(reliability, recovery_records)["automatic_retries"] == 1


def test_recovery_rejects_claim_before_its_durable_due_time(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    recovery_records[1][1]["started_at"] = timestamp(2.74)
    recovery_records[0][0]["started_at"] = timestamp(2.74)
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


def test_recovery_rejects_outbox_due_time_that_differs_from_retry_history(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    recovery_records[2][1]["available_at"] = timestamp(2.8)
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


def test_retry_outbox_creation_can_precede_failure_outcome_within_its_transaction(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    recovery_records[2][1]["created_at"] = timestamp(1.9)
    assert account(reliability, recovery_records)["automatic_retries"] == 1


def test_retry_outbox_cannot_be_created_after_its_recorded_failure_outcome(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    recovery_records[2][1]["created_at"] = timestamp(2.1)
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("dead_lettered_at", None), ("dead_lettered_at", timestamp(3)),
        ("dead_letter_reason", None), ("dead_letter_reason", "worker_lost"),
        ("result_metadata", {"attempt": 1}),
        ("error_info", {"code": "unknown_task", "retryable": False}),
    ],
)
def test_control_rejects_wrong_exhaustion_context(
    reliability: ModuleType, control_records: tuple, field: str, invalid_value: object
) -> None:
    control_records[0][0][field] = invalid_value
    with pytest.raises(ValueError):
        account(reliability, control_records, budget=1)


def test_nonfault_failure_cannot_be_explained_by_expected_control_dead_letters(
    reliability: ModuleType, control_records: tuple
) -> None:
    control_records[0][1]["status"] = "DEAD_LETTERED"
    with pytest.raises(ValueError):
        account(reliability, control_records, budget=1)


@pytest.mark.parametrize("faults", [[], [0, 0], [-1], [10], [0, 1]])
def test_accounting_rejects_incomplete_duplicate_or_out_of_range_fault_mapping(
    reliability: ModuleType, recovery_records: tuple, faults: list[int]
) -> None:
    with pytest.raises(ValueError):
        reliability.account_records(*recovery_records, max_attempts=3, faults=faults)


def test_empty_cohort_and_unplanned_attempt_budget_cannot_pass_accounting(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    with pytest.raises(ValueError):
        reliability.account_records([], [], [], [], max_attempts=3, faults=[])
    with pytest.raises(ValueError):
        account(reliability, recovery_records, budget=2)


@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("attempt_number", 3), ("status", "FAILED"), ("result_metadata", {"attempt": 1}),
        ("error_info", {"code": "controlled_failure", "retryable": True}),
        ("next_attempt_at", timestamp(5)), ("finished_at", timestamp(3.75)),
    ],
)
def test_recovered_final_attempt_must_match_final_job_outcome(
    reliability: ModuleType, recovery_records: tuple, field: str, invalid_value: object
) -> None:
    recovery_records[1][1][field] = invalid_value
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


def test_recovery_rejects_backwards_matching_last_attempt_and_job_times(
    reliability: ModuleType, recovery_records: tuple
) -> None:
    recovery_records[0][0]["finished_at"] = timestamp(3)
    recovery_records[1][1]["finished_at"] = timestamp(3)
    with pytest.raises(ValueError):
        account(reliability, recovery_records)


def raw_run(budget: int = 3) -> dict:
    jobs, attempts, outbox, submissions = cohort(budget)
    by_id = {job["id"]: job for job in jobs}
    merged = []
    for submission in submissions:
        job = {**by_id[submission["id"]], **submission}
        job.update({
            "http_status": 201,
            "request_started_monotonic_seconds": 0.0,
            "request_finished_monotonic_seconds": 0.01 * (submission["submission_index"] + 1),
            "request_duration_seconds": 0.01 * (submission["submission_index"] + 1),
            "observed_completed_monotonic_seconds": 4.0 if submission["injected_fault"] else 1.5,
        })
        merged.append(job)
    return {
        "complete": True, "jobs": merged, "attempts": attempts, "outbox": outbox,
        "fault_indices": [0], "max_attempts": budget, "expected_count": 10,
        "window_seconds": 4.0,
    }


def test_recovery_summary_uses_history_and_reconciles_timing_decomposition(
    reliability: ModuleType,
) -> None:
    raw = raw_run(3)
    assert raw["jobs"][0]["started_at"] == timestamp(3.25)
    assert raw["attempts"][0]["started_at"] == timestamp(1)
    summary = reliability.summarize_run(raw)
    assert summary["initially_succeeded"] == 9
    assert summary["eventually_succeeded"] == 10
    assert summary["dead_lettered"] == 0
    assert summary["automatic_retries"] == summary["failed_attempts"] == 1
    timings = summary["timing_seconds"]
    for name, seconds in {
        "recovery": 1.5, "backoff": 0.75, "due_to_claim": 0.5, "final_claim_to_outcome": 0.25,
    }.items():
        assert timings[name] == {"count": 1, "p50": seconds, "p95": seconds, "p99": seconds}
    assert timings["successful_lifecycle"] == {"count": 10, "p50": 1.25, "p95": 3.5, "p99": 3.5}
    assert timings["request"]["count"] == 10
    assert timings["request"]["p50"] == pytest.approx(0.05)
    assert timings["request"]["p95"] == timings["request"]["p99"] == pytest.approx(0.1)
    assert timings["recovery"]["p50"] == sum(
        timings[name]["p50"] for name in ("backoff", "due_to_claim", "final_claim_to_outcome")
    )
    assert summary["recovery_samples"] == [{
        "id": raw["jobs"][0]["id"], "submission_index": 0,
        "recovery": 1.5, "backoff": 0.75, "due_to_claim": 0.5, "final_claim_to_outcome": 0.25,
    }]


def test_control_summary_keeps_expected_dead_letters_and_null_absent_recovery_samples(
    reliability: ModuleType,
) -> None:
    summary = reliability.summarize_run(raw_run(1))
    assert summary["initially_succeeded"] == summary["eventually_succeeded"] == 9
    assert summary["dead_lettered"] == 1
    assert summary["automatic_retries"] == 0
    assert summary["timing_seconds"]["successful_lifecycle"]["count"] == 9
    for name in ("recovery", "backoff", "due_to_claim", "final_claim_to_outcome"):
        assert summary["timing_seconds"][name] == {
            "count": 0, "p50": None, "p95": None, "p99": None
        }


def test_postgresql_completion_observation_may_precede_the_http_submission_response(
    reliability: ModuleType,
) -> None:
    raw = raw_run()
    raw["jobs"][0]["request_finished_monotonic_seconds"] = 5.0
    raw["jobs"][0]["request_duration_seconds"] = 5.0
    raw["window_seconds"] = 5.5
    summary = reliability.summarize_run(raw)
    assert summary["eventually_succeeded"] == 10
    assert summary["timing_seconds"]["client_observed_completion"]["p99"] == 4.0


@pytest.mark.parametrize("window", [0, -1, math.nan, math.inf])
def test_summary_rejects_invalid_measurement_windows(
    reliability: ModuleType, window: float
) -> None:
    raw = raw_run()
    raw["window_seconds"] = window
    with pytest.raises(ValueError):
        reliability.summarize_run(raw)


@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("http_status", 409), ("http_status", 503),
        ("request_duration_seconds", -1), ("request_duration_seconds", math.nan),
        ("request_duration_seconds", math.inf),
        ("request_started_monotonic_seconds", math.nan),
        ("observed_completed_monotonic_seconds", None),
        ("observed_completed_monotonic_seconds", -1),
        ("observed_completed_monotonic_seconds", math.inf),
        ("finished_at", None), ("created_at", "invalid timestamp"),
    ],
)
def test_summary_rejects_nonfinite_backwards_or_incomplete_observations(
    reliability: ModuleType, field: str, invalid_value: object
) -> None:
    raw = raw_run()
    raw["jobs"][0][field] = invalid_value
    with pytest.raises(ValueError):
        reliability.summarize_run(raw)


def test_summary_cannot_accept_incomplete_or_missing_job_observations(
    reliability: ModuleType,
) -> None:
    raw = raw_run()
    raw["complete"] = False
    with pytest.raises(ValueError):
        reliability.summarize_run(raw)
    raw["complete"] = True
    raw["jobs"].pop()
    with pytest.raises(ValueError):
        reliability.summarize_run(raw)


@pytest.fixture
def resources() -> SimpleNamespace:
    return SimpleNamespace(
        cleanup=AsyncMock(), cleanup_results=[], owner="012345abcdef",
        network="dcbenchmark-012345abcdef", containers=[], network_created=False,
    )


@pytest.mark.asyncio
async def test_failed_suite_retains_partial_evidence_and_cleans_owned_resources(
    reliability: ModuleType, resources: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(reliability, "Resources", lambda owner: resources)

    async def fail(arguments: argparse.Namespace, owned: object, report: dict) -> None:
        report["runs"].append({"complete": False, "fault_indices": [0], "partial_jobs": 2})
        raise ValueError("dummy-secret-credential")

    monkeypatch.setattr(reliability, "execute_suite", fail)
    output = tmp_path / "new-output"
    assert await reliability.run(options(output=output)) == 1
    resources.cleanup.assert_awaited_once()
    serialized = (output / "suite.json").read_text(encoding="utf-8")
    assert "dummy-secret-credential" not in serialized
    report = json.loads(serialized)
    assert report["complete"] is False
    assert report["cleanup_complete"] is True
    assert report["runs"] == [{"complete": False, "fault_indices": [0], "partial_jobs": 2}]


@pytest.mark.asyncio
async def test_reliability_artifact_failure_still_cleans_owned_resources(
    reliability: ModuleType, resources: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(reliability, "Resources", lambda owner: resources)
    monkeypatch.setattr(reliability, "execute_suite", AsyncMock(side_effect=RuntimeError("failed")))

    def fail_write(*args: object, **kwargs: object) -> None:
        raise OSError("Artifact filesystem unavailable")

    monkeypatch.setattr(reliability, "write_artifact", fail_write)
    try:
        await reliability.run(options(output=tmp_path / "new-output"))
    except OSError:
        pass
    resources.cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_reliability_does_not_overwrite_existing_output_or_start_work(
    reliability: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "existing-output"
    output.mkdir()
    sentinel = output / "suite.json"
    sentinel.write_text("existing user data", encoding="utf-8")
    execute = AsyncMock()
    monkeypatch.setattr(reliability, "execute_suite", execute)
    with pytest.raises(FileExistsError):
        await reliability.run(options(output=output))
    execute.assert_not_awaited()
    assert sentinel.read_text(encoding="utf-8") == "existing user data"


@pytest.mark.asyncio
async def test_measured_cohort_rejects_extra_postgresql_job_before_merging_http_mapping(
    reliability: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    jobs, attempts, outbox, submissions = cohort()
    foreign_id = "00000000-0000-0000-0000-999999999999"
    jobs.append({**jobs[1], "id": foreign_id, "status": "SCHEDULED"})
    snapshot = {"jobs": jobs, "attempts": attempts, "outbox": outbox}
    monkeypatch.setattr(reliability, "snapshot_database", AsyncMock(return_value=snapshot))
    assert reliability.fault_indices(10, 2) == [0]
    posted = 0

    async def post(url: str, *, json: dict) -> SimpleNamespace:
        nonlocal posted
        job_id = submissions[posted]["id"]
        posted += 1
        return SimpleNamespace(status_code=201, json=lambda: {"id": job_id})

    async def states(statement: object) -> list[tuple[str, str]]:
        return [(job["id"], job["status"]) for job in jobs] if posted == 10 else []

    connection = SimpleNamespace(execute=AsyncMock(side_effect=states))
    connect = AsyncMock()
    connect.__aenter__.return_value = connection
    engine = SimpleNamespace(connect=lambda: connect)
    client = SimpleNamespace(post=AsyncMock(side_effect=post))
    raw = {}
    with pytest.raises(ValueError, match="unexpected, missing or duplicate jobs"):
        await reliability.measured_cohort(
            client=client, engine=engine, api_url="http://127.0.0.1:18080",
            count=10, seed=2, max_attempts=3, timeout=2, raw=raw,
        )
    assert posted == len(raw["jobs"]) == 10
    assert raw["complete"] is False
    assert "summary" not in raw
    assert any(
        job["id"] == foreign_id for job in raw["postgresql_snapshot_after_failure"]["jobs"]
    )
