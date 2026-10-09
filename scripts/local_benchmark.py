"""Finite local HTTP/PostgreSQL benchmarks using only already available images.

Run with the project's virtual environment. Every invocation owns disposable
Docker resources and an exclusive output directory; existing demo data is untouched.
This measures finite batches, not cloud capacity or sustained-arrival performance.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import re
import socket
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from distributed_cloud_scheduler.__main__ import create_event_loop

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_IMAGE = "distributed-cloud-scheduler:local"
POSTGRES_IMAGE = "postgres:17"
DATABASE_NAME = "scheduler_benchmark_test"
DATABASE_USER = "scheduler_benchmark"
SUBMISSION_CONCURRENCY = 10
POLL_INTERVAL_SECONDS = 0.05
RESOURCE_INTERVAL_SECONDS = 2.0
RESOURCE_ROLES = ("api", "worker", "database")
OWNER_LABEL = "scheduler-benchmark.owner"
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "DEAD_LETTERED"}
WORKLOADS = {
    "calculation": {"payload": {"numbers": list(range(1000))}, "result": {"sum": 499500}},
    "sleep": {"payload": {"seconds": 0.05}, "result": {"slept_seconds": 0.05}},
}


def percentile(values: list[float], quantile: float) -> float:
    """Return an exact nearest-rank quantile, without interpolation."""
    if not values or not all(math.isfinite(value) and value >= 0 for value in values):
        raise ValueError("Percentiles require nonempty finite observations")
    if not math.isfinite(quantile) or not 0 < quantile <= 1:
        raise ValueError("Quantile must be finite and in (0, 1]")
    return sorted(values)[math.ceil(quantile * len(values)) - 1]


def validate_options(arguments: argparse.Namespace) -> None:
    """Keep all configurations finite, local, and within documented ceilings."""
    for name, maximum in (("jobs", 1000), ("warmup", 100), ("repetitions", 5)):
        value = getattr(arguments, name)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            raise ValueError(f"--{name} must be an integer between 1 and {maximum}")
    for name, maximum in (("run_timeout", 120), ("suite_timeout", 900)):
        value = getattr(arguments, name)
        if isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= maximum:
            raise ValueError(f"--{name.replace('_', '-')} must be in (0, {maximum}]")
    if arguments.suite_timeout < arguments.run_timeout:
        raise ValueError("--suite-timeout must be at least --run-timeout")
    if (
        not arguments.concurrency
        or any(isinstance(value, bool) or not isinstance(value, int)
               or value not in (1, 5, 10, 20) for value in arguments.concurrency)
        or len(set(arguments.concurrency)) != len(arguments.concurrency)
    ):
        raise ValueError("--concurrency must contain unique values from 1, 5, 10, 20")
    if (
        not arguments.workloads
        or any(value not in WORKLOADS for value in arguments.workloads)
        or len(set(arguments.workloads)) != len(arguments.workloads)
    ):
        raise ValueError("--workloads must contain unique calculation/sleep names")


def docker_command(arguments: list[str]) -> list[str]:
    """Allow only local inspection and owned-resource lifecycle commands."""
    allowed = {"version", "info", "image", "network", "run", "exec", "inspect", "stats",
               "stop", "rm", "logs", "ps", "context"}
    if not arguments or arguments[0] not in allowed:
        raise ValueError("Only local Docker lifecycle commands are allowed")
    if arguments[0] == "image" and (len(arguments) < 2 or arguments[1] != "inspect"):
        raise ValueError("Image download/build/publication is outside benchmark scope")
    if arguments[0] == "context" and (
        len(arguments) < 2 or arguments[1] not in {"show", "inspect"}
    ):
        raise ValueError("Only read-only Docker context configuration inspection is allowed")
    if arguments[0] == "network" and (
        len(arguments) < 2 or arguments[1] not in {"create", "inspect", "rm", "ls"}
    ):
        raise ValueError("Only owned local network lifecycle operations are allowed")
    if arguments[0] == "run":
        if arguments.count("--pull") != 1:
            raise ValueError("Docker run requires exactly one --pull never")
        try:
            pull_index = arguments.index("--pull")
        except ValueError:
            raise ValueError("Docker run requires --pull never") from None
        if pull_index + 1 >= len(arguments) or arguments[pull_index + 1] != "never":
            raise ValueError("Docker run requires --pull never")
    return ["docker", *arguments]


def validate_docker_endpoint(endpoint: str) -> str:
    """Reject remote daemon access before any image/server command is executed."""
    if endpoint.startswith("npipe:////./pipe/"):
        return "local_named_pipe"
    if endpoint.startswith("unix:///"):
        return "local_unix_socket"
    if re.fullmatch(r"tcp://(?:127\.0\.0\.1|localhost|\[::1\]):[0-9]+", endpoint):
        return "loopback_tcp"
    raise ValueError("Docker must use a local socket, named pipe, or loopback endpoint")


def build_run_command(
    *, name: str, owner: str, role: str, network: str, env_path: str,
    port: int | None = None, concurrency: int = 1,
) -> list[str]:
    """Build fixed-image, fixed-limit containers; public ports bind only loopback."""
    if role not in RESOURCE_ROLES:
        raise ValueError("Unknown benchmark role")
    if concurrency not in (1, 5, 10, 20):
        raise ValueError("Unsupported worker concurrency")
    arguments = [
        "run", "--pull", "never", "--detach", "--name", name,
        "--label", f"{OWNER_LABEL}={owner}", "--network", network,
        "--cpus", "2" if role == "database" else "1",
        "--memory", "512m" if role == "database" else "256m",
        "--memory-swap", "512m" if role == "database" else "256m",
        "--env-file", env_path,
        "--log-driver", "local", "--log-opt", "max-size=1m", "--log-opt", "max-file=2",
    ]
    if role in {"api", "database"}:
        if port is None or not 1 <= port <= 65535:
            raise ValueError("A valid loopback port is required")
        arguments += ["--publish", f"127.0.0.1:{port}:{5432 if role == 'database' else 8000}"]
    elif port is not None:
        raise ValueError("Workers must not publish ports")
    if role == "database":
        arguments.append(POSTGRES_IMAGE)
    else:
        if role == "worker":
            arguments += ["--env", f"WORKER_CONCURRENCY={concurrency}"]
        arguments.append(RUNTIME_IMAGE)
        if role == "worker":
            arguments += ["python", "-m", "distributed_cloud_scheduler.worker"]
    return docker_command(arguments)


def _date(value: str | datetime) -> datetime:
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def _distribution(values: list[float]) -> dict[str, int | float | None]:
    return {
        "count": len(values),
        **{name: percentile(values, quantile) if values else None
           for name, quantile in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))},
    }


def summarize_run(raw: dict[str, Any]) -> dict[str, Any]:
    """Derive all reported measurements from retained individual observations."""
    jobs = raw["jobs"]
    if raw.get("complete") is False:
        raise ValueError("An incomplete run cannot be summarized as accepted")
    if any(job.get("http_status", 201) != 201 for job in jobs):
        raise ValueError("A submission error cannot be summarized as an accepted run")
    if raw.get("expected_count", len(jobs)) != len(jobs):
        raise ValueError("Submission counts do not reconcile")
    if raw.get("measure_resources") and raw.get("resource_sampling", {}).get("failed_samples", 0):
        raise ValueError("Resource sampling failed; run cannot be accepted")
    if raw.get("complete") is True and (
        not jobs or any(job.get("status") != "SUCCEEDED" or any(
            job.get(field) is None for field in (
                "created_at", "started_at", "finished_at", "observed_completed_monotonic_seconds",
            )
        ) for job in jobs)
    ):
        raise ValueError("An accepted run must have complete successful outcomes and timings")
    window = raw["window_seconds"]
    if not math.isfinite(window) or window <= 0:
        raise ValueError("Measurement window must be finite and positive")
    values: dict[str, list[float]] = {
        name: [] for name in (
            "request", "queue", "claim_to_outcome", "persisted_lifecycle",
            "client_observed_completion",
        )
    }
    for job in jobs:
        values["request"].append(job["request_duration_seconds"])
        if job.get("observed_completed_monotonic_seconds") is not None:
            values["client_observed_completion"].append(
                job["observed_completed_monotonic_seconds"]
                - job["request_started_monotonic_seconds"]
            )
        if job.get("created_at") and job.get("started_at"):
            values["queue"].append(
                (_date(job["started_at"]) - _date(job["created_at"])).total_seconds()
            )
        if job.get("started_at") and job.get("finished_at"):
            values["claim_to_outcome"].append(
                (_date(job["finished_at"]) - _date(job["started_at"])).total_seconds()
            )
        if job.get("created_at") and job.get("finished_at"):
            values["persisted_lifecycle"].append(
                (_date(job["finished_at"]) - _date(job["created_at"])).total_seconds()
            )
    if any(value < 0 or not math.isfinite(value) for items in values.values() for value in items):
        raise ValueError("Latency observations must be finite and nonnegative")
    succeeded = sum(job.get("status") == "SUCCEEDED" for job in jobs)
    resources = {}
    samples = raw.get("resource_samples", [])
    for sample in samples:
        observed = sample["observed_monotonic_seconds"]
        if not math.isfinite(observed) or observed < 0:
            raise ValueError("Resource observation time must be finite and nonnegative")
        for measurement in sample["containers"].values():
            cpu = measurement["cpu_percent"]
            memory = measurement["memory_working_set_bytes"]
            limit = measurement["memory_limit_bytes"]
            if (not math.isfinite(cpu) or cpu < 0 or not math.isfinite(memory)
                    or not math.isfinite(limit) or memory < 0 or limit <= 0 or memory > limit):
                raise ValueError("Resource measurements must be finite and within limits")
    # A short batch can finish while Docker's first collection command is pending.
    # Keep that observation and label its overlap; never extend the throughput window.
    window_start = raw.get("measurement_window_start_monotonic_seconds", 0)
    window_end = raw.get("measurement_window_end_monotonic_seconds", window)
    overlapping_samples = [sample for sample in samples
                           if sample.get("command_started_monotonic_seconds",
                                         sample["observed_monotonic_seconds"]) < window_end
                           and sample["observed_monotonic_seconds"] > window_start]
    if raw.get("measure_resources") and not any(
        set(sample["containers"]) == set(RESOURCE_ROLES) for sample in overlapping_samples
    ):
        raise ValueError("No complete resource collection interval overlapped the measurement")
    for role in RESOURCE_ROLES:
        role_samples = [sample["containers"][role] for sample in overlapping_samples
                        if role in sample["containers"]]
        resources[role] = {
            "sample_count": len(role_samples),
            "sample_selection": "collection interval overlaps submission-and-drain window",
            "cpu_percent_peak": max(
                (sample["cpu_percent"] for sample in role_samples), default=None,
            ),
            "memory_working_set_bytes_peak": max(
                (sample["memory_working_set_bytes"] for sample in role_samples), default=None,
            ),
        }
    return {
        "count": len(jobs), "succeeded": succeeded, "failed": len(jobs) - succeeded,
        "failure_rate": (len(jobs) - succeeded) / len(jobs) if jobs else 0.0,
        "window_seconds": window, "throughput_jobs_per_second": succeeded / window,
        "latency_seconds": {name: _distribution(items) for name, items in values.items()},
        "resource_usage": resources,
    }


def account_records(
    jobs: list[dict[str, Any]], attempts: list[dict[str, Any]], outbox: list[dict[str, Any]],
    expected_ids: list[str], expected_result: dict[str, Any],
) -> dict[str, Any]:
    """Require exact successful generation-zero execution and dispatch accounting."""
    expected = set(expected_ids)
    if not expected or len(expected) != len(expected_ids) or len(jobs) != len(expected):
        raise ValueError("Duplicate or missing submitted jobs")
    actual = [str(job["id"]) for job in jobs]
    if len(set(actual)) != len(actual) or set(actual) != expected:
        raise ValueError("Unexpected, duplicate or missing PostgreSQL jobs")
    if len(attempts) != len(expected) or len(outbox) != len(expected):
        raise ValueError("Attempt or outbox count does not match submitted jobs")
    for rows, label in ((attempts, "attempt"), (outbox, "outbox")):
        identifiers = [str(row["job_id"]) for row in rows]
        if len(set(identifiers)) != len(identifiers) or set(identifiers) != expected:
            raise ValueError(f"Unexpected, duplicate or missing {label} records")
    by_id = {str(job["id"]): job for job in jobs}
    for job in jobs:
        if (
            job["status"] != "SUCCEEDED" or job["generation"] != 0
            or job["attempt_count"] != 1 or job["result_metadata"] != expected_result
            or job["error_info"] is not None
            or any(job.get(field) is None for field in ("created_at", "started_at", "finished_at"))
        ):
            raise ValueError("Job result or execution accounting did not reconcile")
        if not _date(job["created_at"]) <= _date(job["started_at"]) <= _date(job["finished_at"]):
            raise ValueError("PostgreSQL job timestamps are inconsistent")
    for attempt in attempts:
        job = by_id[str(attempt["job_id"])]
        if (
            attempt["generation"] != 0 or attempt["attempt_number"] != 1
            or attempt["status"] != "SUCCEEDED" or attempt["result_metadata"] != expected_result
            or attempt["error_info"] is not None
            or attempt["started_at"] != job["started_at"]
            or attempt["finished_at"] != job["finished_at"]
        ):
            raise ValueError("Attempt outcome or timestamps did not reconcile")
    outbox_ids = [str(row["id"]) for row in outbox]
    if len(set(outbox_ids)) != len(outbox_ids):
        raise ValueError("Duplicate outbox identifiers")
    for obligation in outbox:
        if (obligation["generation"] != 0 or obligation["status"] != "READY"
                or obligation["publish_attempt_count"] != 0):
            raise ValueError("Dispatch accounting did not reconcile")
    return {
        "expected_jobs": len(expected), "jobs": len(jobs), "succeeded": len(jobs),
        "failed": 0, "attempts": len(attempts), "outbox": len(outbox),
        "generation": 0, "attempts_per_job": 1, "outbox_per_job": 1,
    }


class Docker:
    """Bound subprocesses and keep raw diagnostics (which may contain secrets) private."""

    def __init__(self) -> None:
        self.context: str | None = None
        self.endpoint_kind: str | None = None

    async def ensure_local(self) -> None:
        if self.context is not None:
            return
        if os.environ.get("DOCKER_HOST"):
            validate_docker_endpoint(os.environ["DOCKER_HOST"])

        async def local_config(arguments: list[str]) -> str:
            result = await asyncio.to_thread(
                subprocess.run, docker_command(arguments), capture_output=True, text=True,
                timeout=5, check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            if result.returncode:
                raise RuntimeError("Local Docker context configuration unavailable")
            return result.stdout.strip()

        context = os.environ.get("DOCKER_CONTEXT") or await local_config(["context", "show"])
        endpoint = json.loads(await local_config([
            "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", context,
        ]))
        self.endpoint_kind = validate_docker_endpoint(endpoint)
        self.context = context

    async def call(
        self, arguments: list[str], *, timeout: float = 20, required: bool = True,
    ) -> str | None:
        await self.ensure_local()
        guarded = docker_command(arguments)
        command = [guarded[0], "--context", self.context or "default", *guarded[1:]]
        environment = dict(os.environ)
        environment.pop("DOCKER_HOST", None)
        environment["DOCKER_CONTEXT"] = self.context or "default"

        def invoke() -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                command, capture_output=True, text=True, timeout=timeout, check=False,
                env=environment,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )

        try:
            pending = asyncio.create_task(asyncio.to_thread(invoke))
            try:
                result = await asyncio.shield(pending)
            except asyncio.CancelledError:
                # A mutation must settle before cleanup examines its resources.
                try:
                    await pending
                except (OSError, subprocess.TimeoutExpired):
                    pass
                raise
        except (OSError, subprocess.TimeoutExpired):
            if required:
                message = f"Local Docker {arguments[0]} unavailable or timed out"
                raise RuntimeError(message) from None
            return None
        if result.returncode:
            if required:
                raise RuntimeError(f"Local Docker {arguments[0]} failed")
            return None
        return result.stdout.strip()


class Resources:
    """Only label-verified resources created by this invocation may be removed."""

    def __init__(self, owner: str) -> None:
        if re.fullmatch(r"[a-f0-9]{12}", owner) is None:
            raise ValueError("Invalid benchmark ownership identifier")
        self.owner = owner
        self.network = f"dcbenchmark-{owner}"
        self.containers: list[str] = []
        self.network_created = False
        self.docker = Docker()
        self.cleanup_results: list[dict[str, Any]] = []

    def name(self, role: str) -> str:
        if re.fullmatch(r"[a-z0-9-]+", role) is None:
            raise ValueError("Invalid resource role")
        return f"dcbenchmark-{self.owner}-{role}"

    async def create_network(self) -> None:
        self.network_created = True
        await self.docker.call([
            "network", "create", "--label", f"{OWNER_LABEL}={self.owner}", self.network,
        ])

    async def run(self, command: list[str]) -> str:
        name = command[command.index("--name") + 1]
        if not name.startswith(f"dcbenchmark-{self.owner}-"):
            raise ValueError("Container does not belong to this invocation")
        if name not in self.containers:
            self.containers.append(name)
        result = await self.docker.call(command[1:])
        return result or ""

    async def remove_container(self, name: str) -> None:
        if name not in self.containers:
            raise ValueError("Cannot remove an untracked container")
        owner = await self.docker.call(
            ["inspect", "--format", '{{index .Config.Labels "scheduler-benchmark.owner"}}', name],
            required=False,
        )
        if owner is None:
            present = await self.docker.call([
                "ps", "--all", "--filter", f"name={name}", "--format", "{{.Names}}",
            ])
            if present is None or name in present.splitlines():
                raise RuntimeError("Could not verify container ownership for cleanup")
            self.containers.remove(name)
            return
        if owner != self.owner:
            raise RuntimeError("Container ownership differs; cleanup refused")
        await self.docker.call(["stop", "--time", "5", name], timeout=12, required=False)
        await self.docker.call(["rm", "--force", "--volumes", name], timeout=15)
        self.cleanup_results.append({"kind": "container", "name": name, "removed": True})
        self.containers.remove(name)

    async def cleanup(self) -> None:
        failures = []
        for name in list(reversed(self.containers)):
            try:
                await self.remove_container(name)
            except (RuntimeError, ValueError):
                failures.append(name)
        if self.network_created:
            owner = await self.docker.call([
                "network", "inspect", "--format",
                '{{index .Labels "scheduler-benchmark.owner"}}', self.network,
            ], required=False)
            if owner == self.owner:
                result = await self.docker.call(
                    ["network", "rm", self.network], required=False,
                )
                if result is not None:
                    self.cleanup_results.append({
                        "kind": "network", "name": self.network, "removed": True,
                    })
                    self.network_created = False
                else:
                    failures.append(self.network)
            elif owner is not None:
                failures.append(self.network)
            else:
                try:
                    present = await self.docker.call([
                        "network", "ls", "--filter", f"name={self.network}",
                        "--format", "{{.Name}}",
                    ])
                    if present is None or self.network in present.splitlines():
                        failures.append(self.network)
                    else:
                        self.network_created = False
                except RuntimeError:
                    failures.append(self.network)
        if failures:
            raise RuntimeError("Owned-resource cleanup incomplete; inspect recorded resource names")


def _free_port() -> int:
    with socket.socket() as bound:
        bound.bind(("127.0.0.1", 0))
        return bound.getsockname()[1]


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, (UUID, Path)):
        return str(value)
    raise TypeError("Unsupported artifact value")


def write_artifact(path: Path, content: dict[str, Any]) -> None:
    """Replace only invocation-owned artifacts with a complete serialized snapshot."""
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(content, default=_json_default, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


async def preflight(resources: Resources) -> dict[str, Any]:
    image_ids = {}
    for image in (RUNTIME_IMAGE, POSTGRES_IMAGE):
        image_ids[image] = await resources.docker.call([
            "image", "inspect", "--format", "{{.Id}}", image,
        ])
    # Source equality prevents benchmarking an available but obsolete runtime image.
    probe = (
        "import hashlib,importlib.metadata,json,pathlib,platform;"
        "import distributed_cloud_scheduler as p;"
        "root=pathlib.Path(p.__file__).parent;"
        "print(json.dumps({'python':platform.python_version(),"
        "'source_sha256':{str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest()"
        " for f in sorted(root.rglob('*.py'))},"
        "'dependencies':{n:importlib.metadata.version(n) for n in "
        "['distributed-cloud-scheduler','fastapi','pydantic','SQLAlchemy','psycopg','alembic',"
        "'uvicorn']}}))"
    )
    name = resources.name("preflight")
    output = await resources.run(docker_command([
        "run", "--pull", "never", "--name", name,
        "--label", f"{OWNER_LABEL}={resources.owner}", "--network", "none",
        "--cpus", "1", "--memory", "256m", RUNTIME_IMAGE, "python", "-c", probe,
    ]))
    runtime = json.loads(output)
    source_root = PROJECT_ROOT / "src" / "distributed_cloud_scheduler"
    source = {
        path.relative_to(source_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(source_root.rglob("*.py"))
    }
    if source != runtime["source_sha256"]:
        raise RuntimeError("Runtime image source differs from the checkout; benchmark refused")
    await resources.remove_container(name)
    info = await resources.docker.call([
        "info", "--format",
        '{"server_version":{{json .ServerVersion}},"os":{{json .OperatingSystem}},'
        '"kernel":{{json .KernelVersion}},"architecture":{{json .Architecture}},'
        '"cpus":{{json .NCPU}},"memory_bytes":{{json .MemTotal}}}',
    ])
    git = await asyncio.to_thread(subprocess.run, ["git", "rev-parse", "HEAD"],
                                  cwd=PROJECT_ROOT, capture_output=True, text=True,
                                  timeout=5, check=True)
    status = await asyncio.to_thread(subprocess.run, ["git", "status", "--porcelain"],
                                     cwd=PROJECT_ROOT, capture_output=True, text=True,
                                     timeout=5, check=True)
    host: dict[str, Any] = {
        "os": platform.platform(), "python": platform.python_version(),
        "architecture": platform.machine(), "processor": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "driver_dependencies": {
            name: importlib.metadata.version(name) for name in ("httpx", "SQLAlchemy", "psycopg")
        },
    }
    if sys.platform == "win32":
        machine = await asyncio.to_thread(
            subprocess.run, [
                "powershell", "-NoProfile", "-NonInteractive", "-Command",
                "$c=Get-CimInstance Win32_Processor; $m=Get-CimInstance Win32_ComputerSystem; "
                "@{cpu=$c.Name; physical_cores=$c.NumberOfCores; "
                "memory_bytes=$m.TotalPhysicalMemory}|ConvertTo-Json -Compress",
            ], capture_output=True, text=True, timeout=10, check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if machine.returncode == 0:
            host.update(json.loads(machine.stdout))
    return {
        "host": host, "docker": {**json.loads(info or "{}"),
                                 "endpoint_kind": resources.docker.endpoint_kind},
        "images": image_ids,
        "runtime": runtime, "source_git_revision": git.stdout.strip(),
        "checkout_dirty": bool(status.stdout.strip()),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


async def reset_database(engine: AsyncEngine) -> None:
    """Fence destructive state reset to this harness's private disposable database."""
    async with asyncio.timeout(10), engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        if database != DATABASE_NAME or engine.url.host != "127.0.0.1":
            raise ValueError("Refusing to truncate a non-benchmark database")
        await connection.execute(text(
            "SET LOCAL lock_timeout = '3s';"
        ))
        await connection.execute(text(
            "TRUNCATE TABLE public.job_attempts, public.dispatch_outbox, public.jobs"
        ))


async def snapshot_database(engine: AsyncEngine) -> dict[str, list[dict[str, Any]]]:
    # Do not include ownership tokens, keys, payloads, database URLs or credentials.
    columns = {
        "jobs": "id, type, status, generation, attempt_count, max_attempts, priority, "
                "created_at, started_at, finished_at, result_metadata, error_info",
        "attempts": "job_id, generation, attempt_number, started_at, finished_at, "
                    "status, result_metadata, error_info, next_attempt_at",
        "outbox": "id, job_id, generation, status, publish_attempt_count, created_at, available_at",
    }
    tables = {"jobs": "jobs", "attempts": "job_attempts", "outbox": "dispatch_outbox"}
    result = {}
    async with asyncio.timeout(10), engine.connect() as connection:
        for key, selected in columns.items():
            rows = await connection.execute(text(
                f"SELECT {selected} FROM public.{tables[key]} ORDER BY "
                f"{'id' if key != 'attempts' else 'job_id, generation, attempt_number'}"
            ))
            result[key] = [dict(row) for row in rows.mappings()]
    return result


def _memory_bytes(value: str) -> int:
    match = re.fullmatch(r"([0-9.]+)\s*([A-Za-z]+)", value.strip())
    if not match:
        raise ValueError("Unrecognized Docker memory measurement")
    number, unit = match.groups()
    powers = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3,
              "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    if unit not in powers:
        raise ValueError("Unrecognized Docker memory unit")
    return int(float(number) * powers[unit])


async def sample_resources(
    resources: Resources, names: dict[str, str], raw: dict[str, Any],
    origin: float, stop: asyncio.Event,
) -> None:
    sampling = raw["resource_sampling"]
    role_by_name = {name: role for role, name in names.items()}
    while not stop.is_set():
        started = time.monotonic()
        output = await resources.docker.call([
            "stats", "--no-stream", "--format", "{{json .}}", *names.values(),
        ], timeout=10, required=False)
        finished = time.monotonic()
        sampling["command_count"] += 1
        sampling["command_wall_seconds"] += finished - started
        if output is not None:
            containers = {}
            try:
                for line in output.splitlines():
                    record = json.loads(line)
                    role = role_by_name[record["Name"]]
                    used, limit = record["MemUsage"].split("/")
                    containers[role] = {
                        "cpu_percent": float(record["CPUPerc"].rstrip("%")),
                        "memory_working_set_bytes": _memory_bytes(used),
                        "memory_limit_bytes": _memory_bytes(limit),
                    }
                if set(containers) != set(RESOURCE_ROLES):
                    sampling["failed_samples"] += 1
                raw["resource_samples"].append({
                    "observed_monotonic_seconds": finished - origin,
                    "command_started_monotonic_seconds": started - origin,
                    "command_duration_seconds": finished - started,
                    "containers": containers,
                })
            except (ValueError, KeyError):
                sampling["failed_samples"] += 1
        else:
            sampling["failed_samples"] += 1
        try:
            interval = max(0.001, RESOURCE_INTERVAL_SECONDS - (finished - started))
            async with asyncio.timeout(interval):
                await stop.wait()
        except TimeoutError:
            pass


async def measured_batch(
    *, client: httpx.AsyncClient, engine: AsyncEngine, resources: Resources,
    names: dict[str, str], api_url: str, workload: str, count: int,
    timeout: float, raw: dict[str, Any], measure_resources: bool,
) -> None:
    origin = time.monotonic()
    raw.update({
        "jobs": [], "resource_samples": [], "complete": False,
        "measure_resources": measure_resources,
        "resource_sampling": {
            "requested_interval_seconds": RESOURCE_INTERVAL_SECONDS,
            "command_count": 0, "command_wall_seconds": 0.0, "failed_samples": 0,
        },
        "measurement_started_at": datetime.now(UTC), "expected_count": count,
        "monotonic_origin_seconds": origin,
    })
    indices = iter(range(count))
    submitted = asyncio.Event()
    stop_sampler = asyncio.Event()
    observations: dict[str, float] = {}
    submissions: list[dict[str, Any]] = raw["jobs"]
    polling = {"count": 0, "query_wall_seconds": 0.0, "interval_seconds": POLL_INTERVAL_SECONDS}
    raw["completion_polling"] = polling

    async def submit() -> None:
        for index in indices:
            started = time.monotonic()
            try:
                response = await client.post(f"{api_url}/jobs", json={
                    "type": workload, "payload": WORKLOADS[workload]["payload"],
                    "priority": 0, "max_attempts": 1,
                })
            except (httpx.TransportError, asyncio.CancelledError) as error:
                ended = time.monotonic()
                submissions.append({
                    "submission_index": index, "http_status": None,
                    "request_started_monotonic_seconds": started - origin,
                    "request_finished_monotonic_seconds": ended - origin,
                    "request_duration_seconds": ended - started,
                    "request_error_type": type(error).__name__,
                })
                raise
            ended = time.monotonic()
            item = {
                "submission_index": index, "http_status": response.status_code,
                "request_started_monotonic_seconds": started - origin,
                "request_finished_monotonic_seconds": ended - origin,
                "request_duration_seconds": ended - started,
            }
            submissions.append(item)
            if response.status_code != 201:
                raise RuntimeError("Benchmark submission returned an unexpected HTTP status")
            item["id"] = str(UUID(response.json()["id"]))

    async def observe() -> None:
        while True:
            started = time.monotonic()
            async with engine.connect() as connection:
                rows = await connection.execute(text("SELECT id, status FROM public.jobs"))
                states = [(str(job_id), status) for job_id, status in rows]
            ended = time.monotonic()
            polling["count"] += 1
            polling["query_wall_seconds"] += ended - started
            for job_id, status in states:
                if status in TERMINAL_STATUSES:
                    observations.setdefault(job_id, ended - origin)
            if submitted.is_set() and len(observations) == count:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    sampler = asyncio.create_task(sample_resources(
        resources, names, raw, origin, stop_sampler,
    )) if measure_resources else None
    accepted = False
    try:
        async with asyncio.timeout(timeout), asyncio.TaskGroup() as children:
            observer = children.create_task(observe())
            submitters = [children.create_task(submit())
                          for _ in range(min(count, SUBMISSION_CONCURRENCY))]
            await asyncio.gather(*submitters)
            submitted.set()
            await observer
        raw["measurement_window_start_monotonic_seconds"] = min(
            job["request_started_monotonic_seconds"] for job in submissions
        )
        raw["measurement_window_end_monotonic_seconds"] = time.monotonic() - origin
        raw["window_seconds"] = (raw["measurement_window_end_monotonic_seconds"]
                                 - raw["measurement_window_start_monotonic_seconds"])
        raw["submission_finished_monotonic_seconds"] = max(
            job["request_finished_monotonic_seconds"] for job in submissions
        )
        raw["submission_window_seconds"] = (raw["submission_finished_monotonic_seconds"]
                                           - raw["measurement_window_start_monotonic_seconds"])
        raw["drain_window_seconds"] = (
            raw["window_seconds"] - raw["submission_window_seconds"]
        )
        snapshot = await snapshot_database(engine)
        raw["attempts"] = snapshot["attempts"]
        raw["outbox"] = snapshot["outbox"]
        by_id = {str(job["id"]): job for job in snapshot["jobs"]}
        for submission in submissions:
            submission.update(by_id[submission["id"]])
            submission["id"] = str(submission["id"])
            submission["observed_completed_monotonic_seconds"] = observations.get(submission["id"])
        raw["accounting"] = account_records(
            snapshot["jobs"], snapshot["attempts"], snapshot["outbox"],
            [str(job["id"]) for job in submissions], WORKLOADS[workload]["result"],
        )
        accepted = True
    finally:
        stop_sampler.set()
        if "window_seconds" not in raw:
            raw["window_seconds"] = time.monotonic() - origin
        for submission in submissions:
            if "id" in submission:
                submission["observed_completed_monotonic_seconds"] = observations.get(
                    str(submission["id"]),
                )
        if not accepted:
            try:
                snapshot = await snapshot_database(engine)
                raw["postgresql_snapshot_after_failure"] = snapshot
                raw["attempts"] = snapshot["attempts"]
                raw["outbox"] = snapshot["outbox"]
                by_id = {str(job["id"]): job for job in snapshot["jobs"]}
                for submission in submissions:
                    job_id = str(submission.get("id", ""))
                    if job_id in by_id:
                        submission.update(by_id[job_id])
                        submission["id"] = job_id
            except (Exception, asyncio.CancelledError) as error:
                raw["failure_snapshot_error_type"] = type(error).__name__
        if sampler is not None:
            await sampler
        raw["resource_sampling"]["observer_finished_monotonic_seconds"] = time.monotonic() - origin
        raw["resource_sampling"]["samples_after_measurement_window"] = sum(
            sample["observed_monotonic_seconds"] > raw.get(
                "measurement_window_end_monotonic_seconds", raw["window_seconds"],
            )
            for sample in raw["resource_samples"]
        )
        raw["resource_sampling"]["samples_completed_within_window"] = sum(
            sample["observed_monotonic_seconds"] <= raw.get(
                "measurement_window_end_monotonic_seconds", raw["window_seconds"],
            )
            for sample in raw["resource_samples"]
        )
        raw["resource_sampling"]["selection"] = "collection interval overlaps measured batch"
        if accepted:
            raw["complete"] = True
            try:
                raw["summary"] = summarize_run(raw)
            except Exception:
                raw["complete"] = False
                raise


async def wait_ready(client: httpx.AsyncClient, api_url: str) -> None:
    async with asyncio.timeout(30):
        while True:
            try:
                response = await client.get(f"{api_url}/ready")
                if response.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.1)


async def execute_suite(
    arguments: argparse.Namespace, resources: Resources, report: dict[str, Any],
) -> None:
    report["environment"] = await preflight(resources)
    await resources.create_network()
    database_port, api_port = _free_port(), _free_port()
    while api_port == database_port:
        api_port = _free_port()
    password = uuid4().hex
    names = {role: resources.name(role) for role in RESOURCE_ROLES}
    report["owned_resources"] = {"network": resources.network, "containers": names}
    report["loopback_ports"] = {"database": database_port, "api": api_port}
    url = URL.create("postgresql+psycopg", username=DATABASE_USER, password=password,
                     host="127.0.0.1", port=database_port, database=DATABASE_NAME)
    internal_url = URL.create("postgresql+psycopg", username=DATABASE_USER, password=password,
                              host=names["database"], port=5432, database=DATABASE_NAME)
    engine = create_async_engine(
        url, pool_size=2, max_overflow=0, pool_pre_ping=True,
        connect_args={"connect_timeout": 3, "options": "-c statement_timeout=5000"},
    )
    try:
        with tempfile.TemporaryDirectory(prefix="scheduler-benchmark-") as temporary:
            database_env = Path(temporary) / "database.env"
            application_env = Path(temporary) / "application.env"
            database_env.write_text(
                f"POSTGRES_DB={DATABASE_NAME}\nPOSTGRES_USER={DATABASE_USER}\n"
                f"POSTGRES_PASSWORD={password}\n", encoding="utf-8",
            )
            application_env.write_text(
                f"DATABASE_URL={internal_url.render_as_string(hide_password=False)}\n"
                f"WORKER_POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}\n", encoding="utf-8",
            )
            await resources.run(build_run_command(
                name=names["database"], owner=resources.owner, role="database",
                network=resources.network, env_path=str(database_env), port=database_port,
            ))
            async with asyncio.timeout(30):
                while True:
                    ready = await resources.docker.call([
                        "exec", names["database"], "pg_isready", "-h", "127.0.0.1",
                        "-U", DATABASE_USER,
                        "-d", DATABASE_NAME,
                    ], timeout=5, required=False)
                    if ready is not None:
                        break
                    await asyncio.sleep(0.1)
            migration = resources.name("migrate")
            await resources.run(docker_command([
                "run", "--pull", "never", "--name", migration,
                "--label", f"{OWNER_LABEL}={resources.owner}", "--network", resources.network,
                "--cpus", "1", "--memory", "256m", "--env-file", str(application_env),
                RUNTIME_IMAGE, "alembic", "upgrade", "head",
            ]))
            await resources.remove_container(migration)
            await resources.run(build_run_command(
                name=names["api"], owner=resources.owner, role="api",
                network=resources.network, env_path=str(application_env), port=api_port,
            ))
            api_url = f"http://127.0.0.1:{api_port}"
            async with httpx.AsyncClient(
                trust_env=False, timeout=httpx.Timeout(10),
                limits=httpx.Limits(max_connections=SUBMISSION_CONCURRENCY,
                                    max_keepalive_connections=SUBMISSION_CONCURRENCY),
            ) as client:
                await wait_ready(client, api_url)
                async with engine.connect() as connection:
                    report["environment"]["postgres_version"] = await connection.scalar(
                        text("SHOW server_version"),
                    )
                    report["environment"]["migration_revision"] = await connection.scalar(
                        text("SELECT version_num FROM public.alembic_version"),
                    )
                configurations = [(workload, concurrency) for workload in arguments.workloads
                                  for concurrency in arguments.concurrency]
                for repetition in range(1, arguments.repetitions + 1):
                    order = configurations if repetition % 2 else list(reversed(configurations))
                    for workload, concurrency in order:
                        if names["worker"] in resources.containers:
                            await resources.remove_container(names["worker"])
                        await reset_database(engine)
                        await resources.run(build_run_command(
                            name=names["worker"], owner=resources.owner, role="worker",
                            network=resources.network, env_path=str(application_env),
                            concurrency=concurrency,
                        ))
                        run_number = len(report["runs"]) + 1
                        run: dict[str, Any] = {
                            "run_number": run_number, "repetition": repetition,
                            "workload": workload, "worker_concurrency": concurrency,
                        }
                        entry: dict[str, Any] = dict(run)
                        report["runs"].append(entry)
                        print(f"Run {run_number}: {workload}, concurrency {concurrency}, "
                              f"repetition {repetition}", flush=True)
                        run_path = arguments.output / f"run-{run_number:02d}.json"
                        run["raw_file"] = run_path.name
                        entry["raw_file"] = run_path.name
                        run["warmup"] = {}
                        try:
                            await measured_batch(
                                client=client, engine=engine, resources=resources, names=names,
                                api_url=api_url, workload=workload, count=arguments.warmup,
                                timeout=arguments.run_timeout, raw=run["warmup"],
                                measure_resources=False,
                            )
                            await reset_database(engine)
                            run["measurement"] = {}
                            await measured_batch(
                                client=client, engine=engine, resources=resources, names=names,
                                api_url=api_url, workload=workload, count=arguments.jobs,
                                timeout=arguments.run_timeout, raw=run["measurement"],
                                measure_resources=True,
                            )
                            run["complete"] = True
                            run["summary"] = run["measurement"]["summary"]
                            print(f"  {run['summary']['succeeded']} successes, "
                                  f"{run['summary']['throughput_jobs_per_second']:.2f} jobs/s",
                                  flush=True)
                        except (Exception, asyncio.CancelledError) as error:
                            run["error"] = safe_error(error)
                            raise
                        finally:
                            entry["complete"] = run.get("complete", False)
                            for field in ("summary", "error"):
                                if field in run:
                                    entry[field] = run[field]
                            entry["warmup_complete"] = run["warmup"].get("complete", False)
                            write_artifact(run_path, run)
                            write_artifact(arguments.output / "suite.json", report)
    finally:
        async with asyncio.timeout(10):
            await engine.dispose()


def safe_error(error: BaseException) -> dict[str, Any]:
    """Describe controlled failures without dependency text, DSNs or submitted data."""
    result: dict[str, Any] = {"type": type(error).__name__, "message": "Local benchmark failed"}
    if isinstance(error, BaseExceptionGroup):
        result["children"] = [safe_error(child) for child in error.exceptions]
        return result
    safe_prefixes = (
        "Runtime image source differs", "Local Docker", "Benchmark submission",
        "Owned-resource cleanup", "Container ownership", "Duplicate or missing",
        "Unexpected, duplicate or missing", "Attempt or outbox count",
        "Job result or execution", "PostgreSQL job timestamps", "Attempt outcome or timestamps",
        "Duplicate outbox", "Dispatch accounting", "No complete resource", "Docker must use",
        "Resource sampling failed",
    )
    if isinstance(error, (RuntimeError, ValueError)) and str(error).startswith(safe_prefixes):
        result["message"] = str(error)
    return result


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 5, 10, 20])
    parser.add_argument("--workloads", nargs="+", default=["calculation", "sleep"])
    parser.add_argument("--run-timeout", type=float, default=60)
    parser.add_argument("--suite-timeout", type=float, default=900)
    parser.add_argument("--output", type=Path, required=True,
                        help="New exclusive directory; existing paths are never overwritten")
    arguments = parser.parse_args()
    try:
        validate_options(arguments)
    except ValueError as error:
        parser.error(str(error))
    return arguments


async def run(arguments: argparse.Namespace) -> int:
    arguments.output.mkdir(parents=True, exist_ok=False)
    resources = Resources(uuid4().hex[:12])
    report: dict[str, Any] = {
        "format_version": 1, "started_at": datetime.now(UTC), "complete": False,
        "configuration": {
            "jobs_per_run": arguments.jobs, "warmup_jobs_per_run": arguments.warmup,
            "repetitions": arguments.repetitions, "worker_concurrency": arguments.concurrency,
            "workloads": arguments.workloads, "submission_concurrency": SUBMISSION_CONCURRENCY,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "run_timeout_seconds": arguments.run_timeout,
            "suite_timeout_seconds": arguments.suite_timeout,
            "resource_limits": {
                "database": {"cpus": 2, "memory_bytes": 512 * 1024**2},
                "api": {"cpus": 1, "memory_bytes": 256 * 1024**2},
                "worker": {"cpus": 1, "memory_bytes": 256 * 1024**2},
            },
            "worker_database_pool": {"pool_size": 5, "max_overflow": 10},
            "workload_definitions": WORKLOADS,
            "completion_observation": "private PostgreSQL status polling",
        },
        "commands": {
            "invocation": [sys.executable, "scripts/local_benchmark.py", *sys.argv[1:]],
            "migration": [RUNTIME_IMAGE, "alembic", "upgrade", "head"],
            "worker": [RUNTIME_IMAGE, "python", "-m", "distributed_cloud_scheduler.worker"],
            "resource_sample": ["docker", "stats", "--no-stream", "--format", "{{json .}}",
                                "<owned-api>", "<owned-worker>", "<owned-database>"],
        },
        "runs": [],
        "limitations": [
            "Finite batches and this host/runtime only; no sustained or cloud capacity claim.",
            "PostgreSQL timestamps precede commits; claim-to-outcome includes transaction time.",
            "Client-observed completion includes database polling delay and observer overhead.",
            "Resource observations are sampled, may extend beyond short measurement windows, "
            "exclude cache from memory and omit complete host/driver resource accounting.",
            "Calculation is small and sleep simulates waits; neither measures CPU-heavy capacity.",
            "Worker concurrency 20 can exceed the unchanged pool capacity of 15 connections.",
            "Polling worker executes directly; broker/dispatcher/scheduler capacity is unmeasured.",
        ],
    }
    exit_code = 1
    try:
        async with asyncio.timeout(arguments.suite_timeout):
            await execute_suite(arguments, resources, report)
        report["complete"] = True
        exit_code = 0
    except (Exception, asyncio.CancelledError) as error:
        # Exception text from dependencies may expose credentials or submitted data.
        report["error"] = safe_error(error)
        print(f"Benchmark incomplete ({type(error).__name__}); partial artifacts retained.",
              file=sys.stderr, flush=True)
    finally:
        report["measurement_finished_at"] = datetime.now(UTC)
        report["ownership"] = {"owner": resources.owner, "network": resources.network,
                               "tracked_containers_before_cleanup": list(resources.containers)}
        try:
            write_artifact(arguments.output / "suite.json", report)
        except (OSError, TypeError, ValueError):
            exit_code = 1
            report["complete"] = False
            print("Could not save partial artifact; cleanup continues.", file=sys.stderr)
        try:
            await resources.cleanup()
            report["cleanup_complete"] = True
        except (Exception, asyncio.CancelledError):
            report["cleanup_complete"] = False
            report["complete"] = False
            exit_code = 1
            print("Owned Docker cleanup incomplete; inspect suite.json.", file=sys.stderr)
        report["cleanup"] = resources.cleanup_results
        report["remaining_owned_resources"] = {
            "containers": list(resources.containers),
            "network": resources.network if resources.network_created else None,
        }
        report["finished_at"] = datetime.now(UTC)
        try:
            write_artifact(arguments.output / "suite.json", report)
        except (OSError, TypeError, ValueError):
            exit_code = 1
            print("Could not save final artifact; owned-resource cleanup was attempted.",
                  file=sys.stderr)
    return exit_code


def main() -> int:
    arguments = parse_arguments()
    try:
        with asyncio.Runner(loop_factory=create_event_loop) as runner:
            return runner.run(run(arguments))
    except FileExistsError:
        print("Output path already exists; choose a new directory.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Benchmark interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
