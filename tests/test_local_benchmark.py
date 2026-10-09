"""Local measurements reject misleading statistics and unbounded Docker work."""

import argparse
import asyncio
import importlib.util
import json
import math
import sys
import threading
from copy import deepcopy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(scope="module")
def benchmark() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "local_benchmark.py"
    specification = importlib.util.spec_from_file_location("local_benchmark_under_test", path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def options(**changes: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "jobs": 300,
        "warmup": 20,
        "repetitions": 3,
        "concurrency": [1, 5, 10, 20],
        "workloads": ["calculation", "sleep"],
        "run_timeout": 120.0,
        "suite_timeout": 900.0,
        "output": Path("benchmark-results"),
    }
    values.update(changes)
    return argparse.Namespace(**values)


@pytest.mark.parametrize("quantile, expected", [(0.5, 10), (0.95, 19), (0.99, 20), (1.0, 20)])
def test_percentiles_are_nearest_rank_from_unsorted_raw_samples(
    benchmark: ModuleType, quantile: float, expected: float
) -> None:
    values = list(reversed(range(1, 21)))
    assert benchmark.percentile(values, quantile) == expected
    assert values == list(reversed(range(1, 21)))


def test_nearest_rank_preserves_single_sample_and_duplicates(benchmark: ModuleType) -> None:
    assert benchmark.percentile([0.25], 0.99) == 0.25
    assert benchmark.percentile([9, 1, 1, 1], 0.5) == 1
    assert benchmark.percentile([9, 1, 1, 1], 0.95) == 9


@pytest.mark.parametrize("quantile", [0, -0.1, 1.1, math.nan, math.inf])
def test_percentiles_reject_invalid_quantiles(benchmark: ModuleType, quantile: float) -> None:
    with pytest.raises(ValueError):
        benchmark.percentile([1], quantile)


@pytest.mark.parametrize("values", [[], [math.nan], [math.inf], [-math.inf]])
def test_percentiles_reject_missing_or_nonfinite_measurements(
    benchmark: ModuleType, values: list[float]
) -> None:
    with pytest.raises(ValueError):
        benchmark.percentile(values, 0.5)


def test_default_options_and_exact_batch_limits_are_allowed(benchmark: ModuleType) -> None:
    benchmark.validate_options(options())
    benchmark.validate_options(options(jobs=1000, warmup=100, repetitions=5))
    benchmark.validate_options(options(jobs=1, warmup=1, repetitions=1, concurrency=[1]))


@pytest.mark.parametrize(
    "changes",
    [
        {"jobs": 0},
        {"jobs": -1},
        {"jobs": 1001},
        {"warmup": -1},
        {"warmup": 0},
        {"warmup": 101},
        {"repetitions": 0},
        {"repetitions": 6},
        {"concurrency": []},
        {"concurrency": [2]},
        {"concurrency": [21]},
        {"concurrency": [1, 1]},
        {"workloads": []},
        {"workloads": ["controlled_failure"]},
        {"workloads": ["sleep", "sleep"]},
        {"run_timeout": 0},
        {"run_timeout": 120.1},
        {"run_timeout": math.nan},
        {"run_timeout": math.inf},
        {"suite_timeout": 0},
        {"suite_timeout": 900.1},
        {"suite_timeout": math.nan},
        {"suite_timeout": math.inf},
    ],
)
def test_options_enforce_hard_job_time_and_workload_bounds(
    benchmark: ModuleType, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        benchmark.validate_options(options(**changes))


@pytest.mark.parametrize(
    "arguments",
    [
        ["pull", "postgres:17"],
        ["build", "."],
        ["push", "example/runtime"],
        ["system", "prune"],
        ["image", "pull", "postgres:17"],
        ["image", "prune"],
        ["run", "postgres:17"],
        ["run", "--pull", "always", "postgres:17"],
        ["run", "--pull", "missing", "postgres:17"],
        ["run", "--pull", "never", "--pull", "always", "postgres:17"],
    ],
)
def test_docker_guard_cannot_fetch_images_build_or_prune(
    benchmark: ModuleType, arguments: list[str]
) -> None:
    with pytest.raises(ValueError):
        benchmark.docker_command(arguments)


def test_docker_guard_allows_only_explicit_no_pull_run(benchmark: ModuleType) -> None:
    arguments = ["run", "--pull", "never", "postgres:17"]
    assert benchmark.docker_command(arguments) == ["docker", *arguments]
    assert benchmark.docker_command(["image", "inspect", "postgres:17"]) == [
        "docker", "image", "inspect", "postgres:17"
    ]


@pytest.fixture
def records() -> tuple[list[dict], list[dict], list[dict], list[str], dict]:
    expected_ids = ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"]
    result = {"sum": 499500}
    jobs = [
        {
            "id": job_id,
            "status": "SUCCEEDED",
            "generation": 0,
            "attempt_count": 1,
            "result_metadata": dict(result),
            "error_info": None,
            "created_at": "2026-10-09T12:00:00+00:00",
            "started_at": "2026-10-09T12:00:01+00:00",
            "finished_at": "2026-10-09T12:00:02+00:00",
        }
        for job_id in expected_ids
    ]
    attempts = [
        {
            "job_id": job_id,
            "generation": 0,
            "attempt_number": 1,
            "status": "SUCCEEDED",
            "result_metadata": dict(result),
            "error_info": None,
            "started_at": "2026-10-09T12:00:01+00:00",
            "finished_at": "2026-10-09T12:00:02+00:00",
        }
        for job_id in expected_ids
    ]
    outbox = [
        {
            "id": f"10000000-0000-0000-0000-00000000000{index}",
            "job_id": job_id,
            "generation": 0,
            "status": "READY",
            "publish_attempt_count": 0,
        }
        for index, job_id in enumerate(expected_ids, start=1)
    ]
    return jobs, attempts, outbox, expected_ids, result


def test_accounting_accepts_expected_durable_jobs_attempts_and_unpublished_obligations(
    benchmark: ModuleType, records: tuple
) -> None:
    accounting = benchmark.account_records(*records)
    assert accounting["expected_jobs"] == accounting["jobs"] == 2
    assert accounting["succeeded"] == accounting["attempts"] == accounting["outbox"] == 2
    assert accounting["failed"] == 0


@pytest.mark.parametrize("record_index", [0, 1, 2])
def test_accounting_rejects_zero_missing_and_duplicate_durable_records(
    benchmark: ModuleType, records: tuple, record_index: int
) -> None:
    for replacement in ([], records[record_index][:-1], records[record_index] * 2):
        damaged = list(deepcopy(records))
        damaged[record_index] = replacement
        with pytest.raises(ValueError):
            benchmark.account_records(*damaged)


def test_accounting_rejects_empty_and_duplicate_expected_ids(
    benchmark: ModuleType, records: tuple
) -> None:
    with pytest.raises(ValueError):
        benchmark.account_records([], [], [], [], records[4])
    duplicated = list(records)
    duplicated[3] = [records[3][0], records[3][0]]
    with pytest.raises(ValueError):
        benchmark.account_records(*duplicated)


@pytest.mark.parametrize(
    "record_index, field, invalid_value",
    [
        (0, "id", "00000000-0000-0000-0000-000000000003"),
        (0, "status", "RUNNING"),
        (0, "status", "FAILED"),
        (0, "generation", 1),
        (0, "attempt_count", 2),
        (0, "result_metadata", {"sum": -1}),
        (0, "error_info", {"code": "controlled_failure"}),
        (0, "finished_at", None),
        (0, "created_at", "2026-10-09T12:00:03+00:00"),
        (0, "started_at", "2026-10-09T12:00:02+00:00"),
        (1, "job_id", "00000000-0000-0000-0000-000000000003"),
        (1, "status", "RUNNING"),
        (1, "generation", 1),
        (1, "attempt_number", 2),
        (1, "result_metadata", {"sum": -1}),
        (1, "error_info", {"code": "controlled_failure"}),
        (1, "finished_at", None),
        (1, "finished_at", "2026-10-09T12:00:03+00:00"),
        (2, "job_id", "00000000-0000-0000-0000-000000000003"),
        (2, "generation", 1),
        (2, "status", "PUBLISHED"),
        (2, "publish_attempt_count", 1),
    ],
)
def test_accounting_rejects_unexpected_identity_outcome_or_execution_history(
    benchmark: ModuleType,
    records: tuple,
    record_index: int,
    field: str,
    invalid_value: object,
) -> None:
    records[record_index][0][field] = invalid_value
    with pytest.raises(ValueError):
        benchmark.account_records(*records)


def test_accounting_rejects_backwards_matching_outcome_timestamps(
    benchmark: ModuleType, records: tuple
) -> None:
    records[0][0]["finished_at"] = "2026-10-09T12:00:00+00:00"
    records[1][0]["finished_at"] = "2026-10-09T12:00:00+00:00"
    with pytest.raises(ValueError):
        benchmark.account_records(*records)


@pytest.mark.parametrize(
    "role, cpu_limit, memory_limit, expected_image",
    [
        ("database", "2", "512m", "postgres:17"),
        ("api", "1", "256m", "distributed-cloud-scheduler:local"),
        ("worker", "1", "256m", "distributed-cloud-scheduler:local"),
    ],
)
def test_run_commands_have_explicit_local_images_and_resource_ceilings(
    benchmark: ModuleType, role: str, cpu_limit: str, memory_limit: str, expected_image: str
) -> None:
    command = benchmark.build_run_command(
        name=f"benchmark-test-{role}",
        owner="benchmark-test",
        role=role,
        network="benchmark-test-network",
        env_path="dummy.env",
        port={"api": 18080, "database": 18081}.get(role),
        concurrency=20,
    )
    assert command[:2] == ["docker", "run"]
    assert command[command.index("--pull") + 1] == "never"
    assert command[command.index("--cpus") + 1] == cpu_limit
    assert command[command.index("--memory") + 1] == memory_limit
    assert command[command.index("--network") + 1] == "benchmark-test-network"
    assert command[command.index("--name") + 1] == f"benchmark-test-{role}"
    assert expected_image in command
    if role == "api":
        assert "127.0.0.1:18080:8000" in command
    elif role == "database":
        assert "127.0.0.1:18081:5432" in command
    else:
        assert "--publish" not in command and "-p" not in command


@pytest.fixture
def raw_run() -> dict:
    jobs = []
    for index in range(3):
        jobs.append(
            {
                "status": "SUCCEEDED" if index < 2 else "FAILED",
                "request_duration_seconds": [1.0, 3.0, 2.0][index],
                "request_started_monotonic_seconds": float(index),
                "observed_completed_monotonic_seconds": [5.0, 9.0, 12.0][index],
                "created_at": "2026-10-09T12:00:00+00:00",
                "started_at": f"2026-10-09T12:00:0{index + 1}+00:00",
                "finished_at": f"2026-10-09T12:00:0{2 * (index + 1)}+00:00",
            }
        )
    samples = [
        {
            "observed_monotonic_seconds": float(index + 1),
            "command_started_monotonic_seconds": float(index + 1) - 0.1,
            "command_duration_seconds": 0.1,
            "containers": {
                role: {
                    "cpu_percent": float(10 * (index + 1)),
                    "memory_working_set_bytes": 1000 + 200 * index,
                    "memory_limit_bytes": 1_000_000,
                }
                for role in ("api", "worker", "database")
            },
        }
        for index in range(2)
    ]
    return {"jobs": jobs, "window_seconds": 12.0, "expected_count": 3, "resource_samples": samples}


def test_summary_uses_success_count_full_window_and_raw_nearest_rank_latencies(
    benchmark: ModuleType, raw_run: dict
) -> None:
    summary = benchmark.summarize_run(raw_run)
    assert summary["count"] == 3
    assert summary["succeeded"] == 2
    assert summary["failed"] == 1
    assert summary["failure_rate"] == pytest.approx(1 / 3)
    assert summary["throughput_jobs_per_second"] == pytest.approx(2 / 12)
    expected_medians_and_peaks = {
        "request": (2, 3),
        "queue": (2, 3),
        "claim_to_outcome": (2, 3),
        "persisted_lifecycle": (4, 6),
        "client_observed_completion": (8, 10),
    }
    for name, (median, peak) in expected_medians_and_peaks.items():
        assert summary["latency_seconds"][name] == {
            "count": 3, "p50": median, "p95": peak, "p99": peak
        }
    for role in ("api", "worker", "database"):
        measurements = summary["resource_usage"][role]
        assert measurements["sample_count"] == 2
        assert measurements["cpu_percent_peak"] == 20
        assert measurements["memory_working_set_bytes_peak"] == 1200


@pytest.mark.parametrize("window", [0, -1, math.nan, math.inf])
def test_summary_rejects_invalid_throughput_denominator(
    benchmark: ModuleType, raw_run: dict, window: float
) -> None:
    raw_run["window_seconds"] = window
    with pytest.raises(ValueError):
        benchmark.summarize_run(raw_run)


@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("request_duration_seconds", -0.1),
        ("request_duration_seconds", math.nan),
        ("request_duration_seconds", math.inf),
        ("observed_completed_monotonic_seconds", -1),
        ("created_at", "2026-10-09T12:00:05+00:00"),
        ("finished_at", "2026-10-09T11:59:59+00:00"),
    ],
)
def test_summary_rejects_negative_or_nonfinite_observations_and_backwards_timestamps(
    benchmark: ModuleType, raw_run: dict, field: str, invalid_value: object
) -> None:
    raw_run["jobs"][0][field] = invalid_value
    with pytest.raises(ValueError):
        benchmark.summarize_run(raw_run)


def test_summary_cannot_accept_an_explicitly_incomplete_run(
    benchmark: ModuleType, raw_run: dict
) -> None:
    raw_run["complete"] = False
    with pytest.raises(ValueError):
        benchmark.summarize_run(raw_run)


@pytest.fixture
def accepted_raw_run(raw_run: dict) -> dict:
    raw_run["complete"] = True
    raw_run["measure_resources"] = True
    raw_run["resource_sampling"] = {
        "requested_interval_seconds": 2.0,
        "command_count": 2,
        "command_wall_seconds": 0.2,
        "failed_samples": 0,
    }
    for job in raw_run["jobs"]:
        job["status"] = "SUCCEEDED"
        job["http_status"] = 201
    return raw_run


def test_summary_accepts_complete_run_with_each_role_observed(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    assert benchmark.summarize_run(accepted_raw_run)["succeeded"] == 3


@pytest.mark.parametrize("status", [400, 409, 422, 500, 503])
def test_summary_cannot_accept_http_submission_errors(
    benchmark: ModuleType, accepted_raw_run: dict, status: int
) -> None:
    accepted_raw_run["jobs"][0]["http_status"] = status
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


@pytest.mark.parametrize(
    "field", ["created_at", "started_at", "finished_at", "observed_completed_monotonic_seconds"]
)
def test_summary_cannot_accept_missing_completion_observations(
    benchmark: ModuleType, accepted_raw_run: dict, field: str
) -> None:
    accepted_raw_run["jobs"][0][field] = None
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


def test_summary_cannot_accept_a_missing_job(benchmark: ModuleType, accepted_raw_run: dict) -> None:
    accepted_raw_run["jobs"].pop()
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


@pytest.mark.parametrize(
    "field, invalid_value",
    [
        ("cpu_percent", -1),
        ("cpu_percent", math.nan),
        ("cpu_percent", math.inf),
        ("memory_working_set_bytes", -1),
        ("memory_working_set_bytes", math.inf),
        ("memory_limit_bytes", 0),
    ],
)
def test_summary_rejects_invalid_resource_observations(
    benchmark: ModuleType, accepted_raw_run: dict, field: str, invalid_value: float
) -> None:
    accepted_raw_run["resource_samples"][0]["containers"]["worker"][field] = invalid_value
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


def test_summary_requires_resource_observations_when_measurement_is_requested(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    accepted_raw_run["resource_samples"] = []
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


def test_summary_requires_each_measured_role_to_have_resource_coverage(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    for sample in accepted_raw_run["resource_samples"]:
        del sample["containers"]["worker"]
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


def test_summary_rejects_failed_resource_samples_in_an_acceptance_run(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    accepted_raw_run["resource_sampling"]["command_count"] = 3
    accepted_raw_run["resource_sampling"]["failed_samples"] = 1
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


@pytest.mark.parametrize("status", ["RUNNING", "FAILED", "DEAD_LETTERED"])
def test_summary_cannot_accept_unfinished_or_unsuccessful_jobs(
    benchmark: ModuleType, accepted_raw_run: dict, status: str
) -> None:
    accepted_raw_run["jobs"][0]["status"] = status
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


def test_resource_sampling_overlap_does_not_extend_the_throughput_window(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    accepted_raw_run["resource_samples"] = [accepted_raw_run["resource_samples"][0]]
    sample = accepted_raw_run["resource_samples"][0]
    sample["command_started_monotonic_seconds"] = 11.9
    sample["observed_monotonic_seconds"] = 14.0
    sample["command_duration_seconds"] = 2.1
    summary = benchmark.summarize_run(accepted_raw_run)
    assert summary["window_seconds"] == 12
    assert summary["throughput_jobs_per_second"] == pytest.approx(3 / 12)


def test_resource_observations_must_overlap_the_measured_batch(
    benchmark: ModuleType, accepted_raw_run: dict
) -> None:
    for sample in accepted_raw_run["resource_samples"]:
        sample["command_started_monotonic_seconds"] = 13.0
        sample["observed_monotonic_seconds"] = 14.0
        sample["command_duration_seconds"] = 1.0
    with pytest.raises(ValueError):
        benchmark.summarize_run(accepted_raw_run)


@pytest.mark.asyncio
async def test_failed_container_start_remains_owned_and_cleanup_removes_its_volume(
    benchmark: ModuleType,
) -> None:
    resources = benchmark.Resources("012345abcdef")
    name = resources.name("database")
    resources.network_created = True

    async def call(arguments: list[str], **kwargs: object) -> str:
        if arguments[0] == "run":
            raise RuntimeError("Container start failed after creation")
        if arguments[0] in {"inspect", "network"} and "inspect" in arguments:
            return resources.owner
        return "removed"

    docker_call = AsyncMock(side_effect=call)
    resources.docker.call = docker_call
    command = benchmark.build_run_command(
        name=name, owner=resources.owner, role="database", network=resources.network,
        env_path="dummy.env", port=18081,
    )
    with pytest.raises(RuntimeError, match="Container start failed"):
        await resources.run(command)
    assert resources.containers == [name]
    await resources.cleanup()
    arguments = [awaited.args[0] for awaited in docker_call.await_args_list]
    assert ["rm", "--force", "--volumes", name] in arguments
    assert ["network", "rm", resources.network] in arguments
    assert resources.containers == []
    assert resources.network_created is False


@pytest.mark.asyncio
async def test_cleanup_refuses_resources_with_a_different_ownership_label(
    benchmark: ModuleType,
) -> None:
    resources = benchmark.Resources("012345abcdef")
    resources.containers.append(resources.name("worker"))
    resources.network_created = True
    docker_call = AsyncMock(return_value="another-owner")
    resources.docker.call = docker_call
    with pytest.raises(RuntimeError):
        await resources.cleanup()
    assert resources.containers == [resources.name("worker")]
    assert resources.network_created is True
    assert all("rm" not in awaited.args[0] for awaited in docker_call.await_args_list)


@pytest.mark.asyncio
async def test_cleanup_attempts_other_owned_resources_after_one_removal_fails(
    benchmark: ModuleType,
) -> None:
    resources = benchmark.Resources("012345abcdef")
    worker, api = resources.name("worker"), resources.name("api")
    resources.containers.extend([worker, api])

    async def call(arguments: list[str], **kwargs: object) -> str:
        if arguments[0] == "inspect":
            return resources.owner
        if arguments[0] == "rm" and arguments[-1] == api:
            raise RuntimeError("Removal failed")
        return "removed"

    docker_call = AsyncMock(side_effect=call)
    resources.docker.call = docker_call
    with pytest.raises(RuntimeError):
        await resources.cleanup()
    assert resources.containers == [api]
    arguments = [awaited.args[0] for awaited in docker_call.await_args_list]
    assert ["rm", "--force", "--volumes", worker] in arguments


@pytest.mark.asyncio
async def test_cleanup_cannot_claim_success_when_docker_cannot_confirm_resource_absence(
    benchmark: ModuleType,
) -> None:
    resources = benchmark.Resources("012345abcdef")
    resources.containers.append(resources.name("database"))
    resources.network_created = True
    async def unavailable(arguments: list[str], **kwargs: object) -> None:
        if kwargs.get("required", True):
            raise RuntimeError("Local Docker unavailable")
        return None

    resources.docker.call = AsyncMock(side_effect=unavailable)
    with pytest.raises(RuntimeError):
        await resources.cleanup()
    assert resources.containers == [resources.name("database")]
    assert resources.network_created is True


@pytest.mark.asyncio
async def test_artifact_write_failure_still_cleans_owned_resources(
    benchmark: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resources = SimpleNamespace(
        cleanup=AsyncMock(), cleanup_results=[], owner="012345abcdef",
        network="dcbenchmark-012345abcdef", containers=[], network_created=False,
    )
    monkeypatch.setattr(benchmark, "Resources", lambda owner: resources)
    monkeypatch.setattr(benchmark, "execute_suite", AsyncMock(side_effect=RuntimeError("failed")))

    def fail_write(*args: object, **kwargs: object) -> None:
        raise OSError("Artifact filesystem unavailable")

    monkeypatch.setattr(benchmark, "write_artifact", fail_write)
    try:
        await benchmark.run(options(output=tmp_path / "new-output"))
    except OSError:
        pass
    resources.cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_suite_failure_records_sanitized_partial_report_and_cleanup_result(
    benchmark: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resources = SimpleNamespace(
        cleanup=AsyncMock(), cleanup_results=[], owner="012345abcdef",
        network="dcbenchmark-012345abcdef", containers=[], network_created=False,
    )
    monkeypatch.setattr(benchmark, "Resources", lambda owner: resources)
    monkeypatch.setattr(
        benchmark, "execute_suite", AsyncMock(side_effect=ValueError("dummy-secret-credential"))
    )
    output = tmp_path / "new-output"
    assert await benchmark.run(options(output=output)) == 1
    resources.cleanup.assert_awaited_once()
    serialized = (output / "suite.json").read_text(encoding="utf-8")
    assert "dummy-secret-credential" not in serialized
    report = json.loads(serialized)
    assert report["complete"] is False
    assert report["cleanup_complete"] is True
    assert report["error"]["type"] == "ValueError"


@pytest.mark.asyncio
async def test_existing_output_directory_is_preserved_before_any_docker_work(
    benchmark: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "existing-output"
    output.mkdir()
    sentinel = output / "suite.json"
    sentinel.write_text("existing user data", encoding="utf-8")
    execute = AsyncMock()
    monkeypatch.setattr(benchmark, "execute_suite", execute)
    with pytest.raises(FileExistsError):
        await benchmark.run(options(output=output))
    execute.assert_not_awaited()
    assert sentinel.read_text(encoding="utf-8") == "existing user data"


@pytest.mark.parametrize(
    "endpoint",
    ["ssh://user@example.com", "tcp://203.0.113.1:2375", "tcp://docker.example.com:2376"],
)
def test_docker_endpoint_guard_rejects_remote_daemons(
    benchmark: ModuleType, endpoint: str
) -> None:
    with pytest.raises(ValueError):
        benchmark.validate_docker_endpoint(endpoint)


@pytest.mark.parametrize(
    "endpoint", ["unix:///var/run/docker.sock", "npipe:////./pipe/docker_engine", "tcp://127.0.0.1:2375"]
)
def test_docker_endpoint_guard_allows_local_socket_pipe_and_loopback(
    benchmark: ModuleType, endpoint: str
) -> None:
    assert benchmark.validate_docker_endpoint(endpoint)


@pytest.mark.asyncio
async def test_docker_mutation_settles_before_cancellation_returns_to_cleanup(
    benchmark: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    docker = benchmark.Docker()
    docker.context = "dummy-local-context"
    docker.ensure_local = AsyncMock()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def invoke(command: list[str], **kwargs: object) -> object:
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(timeout=2)
        return SimpleNamespace(returncode=0, stdout="created")

    monkeypatch.setattr(benchmark.subprocess, "run", invoke)
    pending = asyncio.create_task(docker.call(["network", "create", "dummy-network"]))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            pending.cancel()
            await asyncio.sleep(0)
            assert not pending.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await pending
    finally:
        release.set()


@pytest.mark.asyncio
async def test_remote_docker_environment_is_rejected_before_running_any_command(
    benchmark: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DOCKER_HOST", "tcp://203.0.113.1:2375")
    calls = []

    def invoke(*args: object, **kwargs: object) -> object:
        calls.append(args)
        raise AssertionError("A remote Docker command must never run")

    monkeypatch.setattr(benchmark.subprocess, "run", invoke)
    with pytest.raises(ValueError):
        await benchmark.Docker().call(["version"])
    assert calls == []
