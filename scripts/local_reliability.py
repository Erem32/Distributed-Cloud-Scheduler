"""Seeded local handler-failure recovery and attempt-budget exhaustion experiment.

Uses the benchmark's owned-resource and local-only Docker guards. Existing demo
data, runtime behavior, schema and production jitter are unchanged. This is a
controlled handler experiment, not a broker, outage or worker-crash experiment.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import math
import random
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import local_benchmark as benchmark
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

Resources = benchmark.Resources
write_artifact = benchmark.write_artifact
COHORTS = {"recovery": 3, "exhaustion_control": 1}
CONTROLLED_ERROR = {"code": "controlled_failure", "retryable": True}
MAX_SEED = 2**32 - 1


def validate_options(arguments: argparse.Namespace) -> None:
    """Require finite documented bounds before creating any output or resources."""
    if (isinstance(arguments.jobs, bool) or not isinstance(arguments.jobs, int)
            or not 10 <= arguments.jobs <= 1000 or arguments.jobs % 10):
        raise ValueError("--jobs must be a multiple of ten between 10 and 1000")
    if (isinstance(arguments.repetitions, bool) or not isinstance(arguments.repetitions, int)
            or not 1 <= arguments.repetitions <= 5):
        raise ValueError("--repetitions must be between 1 and 5")
    if (isinstance(arguments.concurrency, bool) or not isinstance(arguments.concurrency, int)
            or arguments.concurrency not in (1, 5, 10, 20)):
        raise ValueError("--concurrency must be one of 1, 5, 10 or 20")
    if (isinstance(arguments.seed, bool) or not isinstance(arguments.seed, int)
            or not 0 <= arguments.seed <= MAX_SEED
            or arguments.seed + arguments.repetitions - 1 > MAX_SEED):
        raise ValueError("All repetition seeds must fit unsigned 32-bit integers")
    for name, maximum in (("run_timeout", 120), ("suite_timeout", 900)):
        value = getattr(arguments, name)
        if isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= maximum:
            raise ValueError(f"--{name.replace('_', '-')} must be in (0, {maximum}]")
    if arguments.suite_timeout < arguments.run_timeout:
        raise ValueError("--suite-timeout must be at least --run-timeout")


def fault_indices(jobs: int, seed: int) -> list[int]:
    """Seed only the exact ten-percent fault membership, never runtime jitter."""
    if isinstance(jobs, bool) or not isinstance(jobs, int) or not 10 <= jobs <= 1000 or jobs % 10:
        raise ValueError("Fault schedules require 10-1000 jobs in multiples of ten")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= MAX_SEED:
        raise ValueError("Fault seed must fit an unsigned 32-bit integer")
    return sorted(random.Random(seed).sample(range(jobs), jobs // 10))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"Reliability accounting: {message}")


def _date(value: str | datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        require(isinstance(parsed, datetime) and parsed.utcoffset() is not None,
                "timestamps must be aware datetimes")
        return parsed
    except (TypeError, ValueError, OverflowError):
        raise ValueError("Reliability accounting: invalid authoritative timestamp") from None


def _fields(records: list[dict[str, Any]], fields: set[str], category: str) -> None:
    for record in records:
        require(isinstance(record, dict) and fields.issubset(record),
                f"missing {category} fields")


def _is_result(value: Any, attempt_number: int) -> bool:
    return (isinstance(value, dict) and set(value) == {"attempt"}
            and type(value["attempt"]) is int and value["attempt"] == attempt_number)


def safe_payload(value: Any) -> dict[str, int] | None:
    """Retain only the bounded controlled-handler integer, even in failure snapshots."""
    if (isinstance(value, dict) and set(value) == {"fail_until_attempt"}
            and type(value["fail_until_attempt"]) is int
            and 0 <= value["fail_until_attempt"] <= 20):
        return {"fail_until_attempt": value["fail_until_attempt"]}
    return None


def account_records(
    jobs: list[dict[str, Any]], attempts: list[dict[str, Any]], outbox: list[dict[str, Any]],
    submissions: list[dict[str, Any]], *, max_attempts: int, faults: list[int],
) -> dict[str, Any]:
    """Reconcile each index, final state, attempt and unpublished dispatch obligation."""
    require(type(max_attempts) is int and max_attempts in (1, 3), "unexpected attempt budget")
    count = len(submissions)
    require(10 <= count <= 1000 and count % 10 == 0, "unexpected submitted cohort size")
    _fields(submissions, {"submission_index", "id", "injected_fault"}, "submission")
    indices = [item["submission_index"] for item in submissions]
    identifiers = [str(item["id"]) for item in submissions]
    require(all(type(index) is int for index in indices) and set(indices) == set(range(count))
            and len(set(indices)) == count, "duplicate, missing or unexpected submission indices")
    require(len(set(identifiers)) == count, "duplicate submitted UUIDs")
    require(len(faults) == count // 10 and len(set(faults)) == len(faults)
            and all(type(index) is int and 0 <= index < count for index in faults),
            "fault membership is not exactly ten percent")
    selected = set(faults)
    for submission in submissions:
        require(type(submission["injected_fault"]) is bool
                and submission["injected_fault"] == (submission["submission_index"] in selected),
                "submission fault mapping differs from the recorded schedule")
        try:
            require(str(UUID(str(submission["id"]))) == str(submission["id"]),
                    "submission UUID must be canonical")
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Reliability accounting: invalid submission UUID") from None
    _fields(jobs, {
        "id", "type", "status", "generation", "attempt_count", "max_attempts", "priority",
        "created_at", "started_at", "finished_at", "next_attempt_at", "scheduled_at",
        "dead_lettered_at", "dead_letter_reason", "result_metadata", "error_info",
    }, "job")
    _fields(attempts, {
        "job_id", "generation", "attempt_number", "status", "started_at", "finished_at",
        "next_attempt_at", "result_metadata", "error_info",
    }, "attempt")
    _fields(outbox, {
        "id", "job_id", "generation", "status", "publish_attempt_count", "created_at",
        "available_at",
    }, "outbox")
    actual_ids = [str(job["id"]) for job in jobs]
    require(len(jobs) == count and len(set(actual_ids)) == count
            and set(actual_ids) == set(identifiers), "unexpected, missing or duplicate jobs")
    expected_retry_count = len(faults) if max_attempts == 3 else 0
    require(len(attempts) == count + expected_retry_count, "unexpected attempt count")
    require(len(outbox) == count + expected_retry_count, "unexpected outbox count")
    attempt_keys = [(str(row["job_id"]), row["generation"], row["attempt_number"])
                    for row in attempts]
    require(len(set(attempt_keys)) == len(attempt_keys), "duplicate attempt identities")
    outbox_ids = [str(row["id"]) for row in outbox]
    require(len(set(outbox_ids)) == len(outbox_ids), "duplicate outbox identities")
    require({str(row["job_id"]) for row in attempts} == set(identifiers),
            "unexpected or missing attempt job IDs")
    require({str(row["job_id"]) for row in outbox} == set(identifiers),
            "unexpected or missing outbox job IDs")
    by_id = {str(item["id"]): item for item in submissions}
    for job in jobs:
        job_id = str(job["id"])
        submission = by_id[job_id]
        fault = submission["submission_index"] in selected
        recovered = fault and max_attempts == 3
        exhausted = fault and max_attempts == 1
        expected_attempts = 2 if recovered else 1
        require(job["type"] == "controlled_failure" and type(job["generation"]) is int
                and job["generation"] == 0
                and job["max_attempts"] == max_attempts and job["priority"] == 0
                and type(job["attempt_count"]) is int and job["attempt_count"] == expected_attempts
                and job["next_attempt_at"] is None and job["scheduled_at"] is None,
                "job configuration or generation differs from its cohort")
        if "payload" in job:
            require(job["payload"] == {"fail_until_attempt": 1 if fault else 0},
                    "persisted handler input differs from its fault index")
        history = sorted((row for row in attempts if str(row["job_id"]) == job_id),
                         key=lambda row: row["attempt_number"])
        require(len(history) == expected_attempts
                and [row["attempt_number"] for row in history] == list(
                    range(1, expected_attempts + 1)
                ) and all(type(row["attempt_number"]) is int
                          and type(row["generation"]) is int and row["generation"] == 0
                          for row in history), "attempt history is not contiguous generation zero")
        created = _date(job["created_at"])
        for attempt in history:
            require(created <= _date(attempt["started_at"]) <= _date(attempt["finished_at"]),
                    "attempt timestamp order is inconsistent")
        first, last = history[0], history[-1]
        require(_date(job["started_at"]) == _date(last["started_at"])
                and _date(job["finished_at"]) == _date(last["finished_at"]),
                "job timestamps do not describe the final attempt")
        if fault:
            require(first["status"] == "FAILED" and first["result_metadata"] is None
                    and first["error_info"] == CONTROLLED_ERROR,
                    "injected first-attempt failure does not match its history")
        else:
            require(first["status"] == "SUCCEEDED" and first["error_info"] is None
                    and first["next_attempt_at"] is None
                    and _is_result(first["result_metadata"], 1),
                    "unfaulted first-attempt outcome is unexpected")
        if recovered:
            failed_at = _date(first["finished_at"])
            retry_at = _date(first["next_attempt_at"])
            backoff = (retry_at - failed_at).total_seconds()
            require(0.5 <= backoff <= 1, "chosen retry backoff is outside equal-jitter bounds")
            require(retry_at <= _date(last["started_at"]), "retry execution started before due")
        elif fault:
            require(first["next_attempt_at"] is None, "exhausted work has an automatic retry")
        if exhausted:
            require(job["status"] == "DEAD_LETTERED" and job["result_metadata"] is None
                    and job["error_info"] == CONTROLLED_ERROR
                    and job["dead_letter_reason"] == "attempts_exhausted"
                    and _date(job["dead_lettered_at"]) == _date(job["finished_at"]),
                    "exhaustion context does not match the final failed attempt")
        else:
            require(job["status"] == "SUCCEEDED" and job["error_info"] is None
                    and _is_result(job["result_metadata"], expected_attempts)
                    and job["dead_lettered_at"] is None and job["dead_letter_reason"] is None,
                    "successful final job outcome is unexpected")
            require(last["status"] == "SUCCEEDED" and last["error_info"] is None
                    and last["next_attempt_at"] is None
                    and _is_result(last["result_metadata"], expected_attempts),
                    "successful final attempt outcome is unexpected")
        obligations = [row for row in outbox if str(row["job_id"]) == job_id]
        require(len(obligations) == expected_attempts and all(
            type(row["generation"]) is int and row["generation"] == 0 and row["status"] == "READY"
            and type(row["publish_attempt_count"]) is int and row["publish_attempt_count"] == 0
            for row in obligations
        ), "dispatch obligation ownership or outcome is unexpected")
        initial = [row for row in obligations if _date(row["available_at"]) == created]
        require(len(initial) == 1 and _date(initial[0]["created_at"]) == created,
                "initial dispatch does not match submission timing")
        if recovered:
            replacement = [row for row in obligations if row is not initial[0]]
            require(len(replacement) == 1
                    and _date(replacement[0]["available_at"]) == _date(first["next_attempt_at"])
                    and created <= _date(replacement[0]["created_at"])
                    <= _date(first["finished_at"]),
                    "replacement dispatch does not match retry intent")
    succeeded = count if max_attempts == 3 else count - len(faults)
    dead_lettered = 0 if max_attempts == 3 else len(faults)
    return {
        "jobs": count, "injected_faults": len(faults), "initially_succeeded": count - len(faults),
        "eventually_succeeded": succeeded, "dead_lettered": dead_lettered,
        "permanently_failed": 0, "unexpected_failures": 0, "attempts": len(attempts),
        "failed_attempts": len(faults), "automatic_retry_intents": expected_retry_count,
        "automatic_retry_executions": expected_retry_count,
        "automatic_retries": expected_retry_count,
        "outbox": len(outbox), "attempts_per_job_mean": len(attempts) / count,
        "attempts_per_job_counts": {
            "1": count - expected_retry_count, "2": expected_retry_count,
        },
        "initial_success_rate": (count - len(faults)) / count,
        "eventual_success_rate": succeeded / count, "dead_letter_rate": dead_lettered / count,
        "failed_attempt_rate": len(faults) / len(attempts), "generation": 0,
    }


def _distribution(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values), **{name: benchmark.percentile(values, quantile) if values else None
                                for name, quantile in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))},
    }


def summarize_run(raw: dict[str, Any]) -> dict[str, Any]:
    """Use authoritative attempt history for initial success and recovery timing."""
    require(raw.get("complete") is True, "incomplete observations cannot be accepted")
    jobs = raw["jobs"]
    require(len(jobs) == raw["expected_count"], "expected job count did not reconcile")
    window = raw["window_seconds"]
    require(math.isfinite(window) and window > 0, "invalid measurement window")
    accounting = account_records(
        jobs, raw["attempts"], raw["outbox"], jobs,
        max_attempts=raw["max_attempts"], faults=raw["fault_indices"],
    )
    timings: dict[str, list[float]] = {name: [] for name in (
        "request", "successful_lifecycle", "client_observed_completion",
        "recovery", "backoff", "due_to_claim", "final_claim_to_outcome",
    )}
    recovery_samples = []
    for job in jobs:
        require(job.get("http_status") == 201, "unexpected submission HTTP status")
        require(all(job.get(name) is not None for name in (
            "request_duration_seconds", "request_started_monotonic_seconds",
            "request_finished_monotonic_seconds", "observed_completed_monotonic_seconds",
        )), "missing client timing observations")
        duration = job["request_duration_seconds"]
        start = job["request_started_monotonic_seconds"]
        finish = job["request_finished_monotonic_seconds"]
        observed = job["observed_completed_monotonic_seconds"]
        require(all(math.isfinite(value) and value >= 0
                    for value in (duration, start, finish, observed))
                and finish >= start and observed >= start
                and math.isclose(duration, finish - start, abs_tol=0.000001),
                "client timing observations are inconsistent")
        timings["request"].append(duration)
        timings["client_observed_completion"].append(observed - start)
        if job["status"] == "SUCCEEDED":
            timings["successful_lifecycle"].append(
                (_date(job["finished_at"]) - _date(job["created_at"])).total_seconds(),
            )
        if job["injected_fault"] and job["status"] == "SUCCEEDED":
            history = sorted((row for row in raw["attempts"]
                              if str(row["job_id"]) == str(job["id"])),
                             key=lambda row: row["attempt_number"])
            first, last = history[0], history[-1]
            failed = _date(first["finished_at"])
            due = _date(first["next_attempt_at"])
            started, finished = _date(last["started_at"]), _date(last["finished_at"])
            phases = {
                "recovery": (finished - failed).total_seconds(),
                "backoff": (due - failed).total_seconds(),
                "due_to_claim": (started - due).total_seconds(),
                "final_claim_to_outcome": (finished - started).total_seconds(),
            }
            require(math.isclose(phases["recovery"], sum(
                phases[name] for name in ("backoff", "due_to_claim", "final_claim_to_outcome")
            ), abs_tol=0.000001), "recovery timing decomposition did not reconcile")
            for name, value in phases.items():
                timings[name].append(value)
            recovery_samples.append({
                "id": str(job["id"]), "submission_index": job["submission_index"], **phases,
            })
    require(all(math.isfinite(value) and value >= 0
                for values in timings.values() for value in values),
            "latency observations must be finite and nonnegative")
    return {
        **accounting, "accounting": accounting, "window_seconds": window,
        "timing_seconds": {name: _distribution(values) for name, values in timings.items()},
        "recovery_samples": recovery_samples,
    }


async def snapshot_database(engine: AsyncEngine) -> dict[str, list[dict[str, Any]]]:
    """Retain diagnostic fields but exclude keys, ownership tokens and credentials."""
    columns = {
        "jobs": "id, type, payload, status, generation, attempt_count, max_attempts, priority, "
                "created_at, started_at, finished_at, next_attempt_at, scheduled_at, "
                "dead_lettered_at, dead_letter_reason, result_metadata, error_info",
        "attempts": "job_id, generation, attempt_number, started_at, finished_at, "
                    "status, result_metadata, error_info, next_attempt_at",
        "outbox": "id, job_id, generation, status, publish_attempt_count, created_at, available_at",
    }
    tables = {"jobs": "jobs", "attempts": "job_attempts", "outbox": "dispatch_outbox"}
    snapshot = {}
    async with asyncio.timeout(10), engine.connect() as connection:
        for name, selected in columns.items():
            rows = await connection.execute(text(
                f"SELECT {selected} FROM public.{tables[name]} ORDER BY "
                f"{'id' if name != 'attempts' else 'job_id, generation, attempt_number'}"
            ))
            snapshot[name] = [dict(row) for row in rows.mappings()]
            if name == "jobs":
                for job in snapshot[name]:
                    payload = safe_payload(job["payload"])
                    if payload is None:
                        job["payload_redacted"] = True
                    job["payload"] = payload
    return snapshot


def _attach_snapshot(raw: dict[str, Any], snapshot: dict[str, Any]) -> None:
    raw["attempts"] = snapshot["attempts"]
    raw["outbox"] = snapshot["outbox"]
    raw["database_job_count"] = len(snapshot["jobs"])
    by_id = {str(job["id"]): job for job in snapshot["jobs"]}
    for submission in raw["jobs"]:
        job_id = str(submission.get("id", ""))
        if job_id in by_id:
            submission.update(by_id[job_id])
            submission["id"] = job_id


async def measured_cohort(
    *, client: httpx.AsyncClient, engine: AsyncEngine, api_url: str, count: int,
    seed: int, max_attempts: int, timeout: float, raw: dict[str, Any],
) -> None:
    origin = time.monotonic()
    selected = set(fault_indices(count, seed))
    raw.update({
        "jobs": [], "complete": False, "expected_count": count, "seed": seed,
        "fault_indices": sorted(selected), "max_attempts": max_attempts,
        "measurement_started_at": datetime.now(UTC), "monotonic_origin_seconds": origin,
    })
    indices = iter(range(count))
    submissions: list[dict[str, Any]] = raw["jobs"]
    observations: dict[str, float] = {}
    submitted = asyncio.Event()
    polling = {"count": 0, "query_wall_seconds": 0.0,
               "interval_seconds": benchmark.POLL_INTERVAL_SECONDS}
    raw["completion_polling"] = polling

    async def submit() -> None:
        for index in indices:
            started = time.monotonic()
            item: dict[str, Any] = {
                "submission_index": index, "injected_fault": index in selected,
                "request_started_monotonic_seconds": started - origin,
            }
            submissions.append(item)
            try:
                response = await client.post(f"{api_url}/jobs", json={
                    "type": "controlled_failure",
                    "payload": {"fail_until_attempt": 1 if index in selected else 0},
                    "max_attempts": max_attempts, "priority": 0,
                })
                item["http_status"] = response.status_code
                require(response.status_code == 201, "unexpected submission HTTP status")
                item["id"] = str(UUID(response.json()["id"]))
            except (Exception, asyncio.CancelledError) as error:
                item["request_error_type"] = type(error).__name__
                raise
            finally:
                finished = time.monotonic()
                item["request_finished_monotonic_seconds"] = finished - origin
                item["request_duration_seconds"] = finished - started

    async def observe() -> None:
        while True:
            started = time.monotonic()
            async with engine.connect() as connection:
                rows = await connection.execute(text("SELECT id, status FROM public.jobs"))
                states = [(str(job_id), status) for job_id, status in rows]
            finished = time.monotonic()
            polling["count"] += 1
            polling["query_wall_seconds"] += finished - started
            for job_id, status in states:
                if status in benchmark.TERMINAL_STATUSES:
                    observations.setdefault(job_id, finished - origin)
            if submitted.is_set() and len(observations) == count:
                return
            await asyncio.sleep(benchmark.POLL_INTERVAL_SECONDS)

    try:
        async with asyncio.timeout(timeout), asyncio.TaskGroup() as children:
            observer = children.create_task(observe())
            submitters = [children.create_task(submit())
                          for _ in range(min(count, benchmark.SUBMISSION_CONCURRENCY))]
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
        raw["drain_window_seconds"] = raw["window_seconds"] - raw["submission_window_seconds"]
        snapshot = await snapshot_database(engine)
        raw["accounting"] = account_records(
            snapshot["jobs"], snapshot["attempts"], snapshot["outbox"], submissions,
            max_attempts=max_attempts, faults=sorted(selected),
        )
        _attach_snapshot(raw, snapshot)
        for job in submissions:
            job["observed_completed_monotonic_seconds"] = observations.get(str(job["id"]))
        raw["complete"] = True
        try:
            raw["summary"] = summarize_run(raw)
            raw["accounting"] = raw["summary"]["accounting"]
        except Exception:
            raw["complete"] = False
            raise
    finally:
        if "window_seconds" not in raw:
            raw["window_seconds"] = time.monotonic() - origin
        for job in submissions:
            if "id" in job:
                job["observed_completed_monotonic_seconds"] = observations.get(str(job["id"]))
        if not raw["complete"]:
            try:
                snapshot = await snapshot_database(engine)
                raw["postgresql_snapshot_after_failure"] = snapshot
                _attach_snapshot(raw, snapshot)
            except (Exception, asyncio.CancelledError) as error:
                raw["failure_snapshot_error_type"] = type(error).__name__
        raw["observer_finished_monotonic_seconds"] = time.monotonic() - origin


async def execute_suite(
    arguments: argparse.Namespace, resources: Resources, report: dict[str, Any],
) -> None:
    environment = await benchmark.preflight(resources)
    environment["helper_harness_sha256"] = environment.pop("harness_sha256")
    environment["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["environment"] = environment
    await resources.create_network()
    database_port, api_port = benchmark._free_port(), benchmark._free_port()
    while api_port == database_port:
        api_port = benchmark._free_port()
    password = uuid4().hex
    names = {role: resources.name(role) for role in benchmark.RESOURCE_ROLES}
    report["owned_resources"] = {"network": resources.network, "containers": names}
    report["loopback_ports"] = {"database": database_port, "api": api_port}
    url = URL.create("postgresql+psycopg", username=benchmark.DATABASE_USER, password=password,
                     host="127.0.0.1", port=database_port, database=benchmark.DATABASE_NAME)
    internal_url = URL.create(
        "postgresql+psycopg", username=benchmark.DATABASE_USER, password=password,
        host=names["database"], port=5432, database=benchmark.DATABASE_NAME,
    )
    engine = create_async_engine(
        url, pool_size=2, max_overflow=0, pool_pre_ping=True,
        connect_args={"connect_timeout": 3, "options": "-c statement_timeout=5000"},
    )
    try:
        with tempfile.TemporaryDirectory(prefix="scheduler-reliability-") as temporary:
            database_env = Path(temporary) / "database.env"
            application_env = Path(temporary) / "application.env"
            database_env.write_text(
                f"POSTGRES_DB={benchmark.DATABASE_NAME}\nPOSTGRES_USER={benchmark.DATABASE_USER}\n"
                f"POSTGRES_PASSWORD={password}\n", encoding="utf-8",
            )
            application_env.write_text(
                f"DATABASE_URL={internal_url.render_as_string(hide_password=False)}\n"
                f"WORKER_POLL_INTERVAL_SECONDS={benchmark.POLL_INTERVAL_SECONDS}\n"
                "EXECUTION_RETRY_BASE_SECONDS=1\nEXECUTION_RETRY_MAX_SECONDS=1\n",
                encoding="utf-8",
            )
            await resources.run(benchmark.build_run_command(
                name=names["database"], owner=resources.owner, role="database",
                network=resources.network, env_path=str(database_env), port=database_port,
            ))
            async with asyncio.timeout(30):
                while True:
                    ready = await resources.docker.call([
                        "exec", names["database"], "pg_isready", "-h", "127.0.0.1",
                        "-U", benchmark.DATABASE_USER, "-d", benchmark.DATABASE_NAME,
                    ], timeout=5, required=False)
                    if ready is not None:
                        break
                    await asyncio.sleep(0.1)
            migration = resources.name("migrate")
            await resources.run(benchmark.docker_command([
                "run", "--pull", "never", "--name", migration,
                "--label", f"{benchmark.OWNER_LABEL}={resources.owner}",
                "--network", resources.network, "--cpus", "1", "--memory", "256m",
                "--env-file", str(application_env), benchmark.RUNTIME_IMAGE,
                "alembic", "upgrade", "head",
            ]))
            await resources.remove_container(migration)
            await resources.run(benchmark.build_run_command(
                name=names["api"], owner=resources.owner, role="api", network=resources.network,
                env_path=str(application_env), port=api_port,
            ))
            api_url = f"http://127.0.0.1:{api_port}"
            async with httpx.AsyncClient(
                trust_env=False, timeout=httpx.Timeout(10),
                limits=httpx.Limits(max_connections=benchmark.SUBMISSION_CONCURRENCY,
                                    max_keepalive_connections=benchmark.SUBMISSION_CONCURRENCY),
            ) as client:
                await benchmark.wait_ready(client, api_url)
                async with asyncio.timeout(10), engine.connect() as connection:
                    environment["postgres_version"] = await connection.scalar(
                        text("SHOW server_version"),
                    )
                    environment["migration_revision"] = await connection.scalar(
                        text("SELECT version_num FROM public.alembic_version"),
                    )
                for repetition in range(1, arguments.repetitions + 1):
                    seed = arguments.seed + repetition - 1
                    order = list(COHORTS) if repetition % 2 else list(reversed(COHORTS))
                    for cohort in order:
                        if names["worker"] in resources.containers:
                            await resources.remove_container(names["worker"])
                        await benchmark.reset_database(engine)
                        await resources.run(benchmark.build_run_command(
                            name=names["worker"], owner=resources.owner, role="worker",
                            network=resources.network, env_path=str(application_env),
                            concurrency=arguments.concurrency,
                        ))
                        run_number = len(report["runs"]) + 1
                        raw: dict[str, Any] = {
                            "run_number": run_number, "repetition": repetition, "cohort": cohort,
                            "worker_concurrency": arguments.concurrency,
                            "raw_file": f"run-{run_number:02d}.json",
                        }
                        entry = dict(raw)
                        report["runs"].append(entry)
                        print(f"Run {run_number}: {cohort}, repetition {repetition}, seed {seed}",
                              flush=True)
                        try:
                            await measured_cohort(
                                client=client, engine=engine, api_url=api_url, count=arguments.jobs,
                                seed=seed, max_attempts=COHORTS[cohort],
                                timeout=arguments.run_timeout, raw=raw,
                            )
                            summary = raw["summary"]
                            print(f"  {summary['eventually_succeeded']} succeeded, "
                                  f"{summary['dead_lettered']} dead letters, "
                                  f"{summary['automatic_retries']} automatic retries", flush=True)
                        except (Exception, asyncio.CancelledError) as error:
                            raw["error"] = safe_error(error)
                            raise
                        finally:
                            entry["complete"] = raw.get("complete", False)
                            summary_keys = ("seed", "fault_indices", "max_attempts",
                                            "summary", "error")
                            for key in summary_keys:
                                if key in raw:
                                    entry[key] = raw[key]
                            write_artifact(arguments.output / raw["raw_file"], raw)
                            write_artifact(arguments.output / "suite.json", report)
    finally:
        async with asyncio.timeout(10):
            await engine.dispose()


def safe_error(error: BaseException) -> dict[str, Any]:
    result = benchmark.safe_error(error)
    if isinstance(error, BaseExceptionGroup):
        result["children"] = [safe_error(child) for child in error.exceptions]
    elif isinstance(error, ValueError) and str(error).startswith("Reliability accounting:"):
        result["message"] = str(error)
    if result["message"] == "Local benchmark failed":
        result["message"] = "Local reliability experiment failed"
    return result


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=300)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20261010)
    parser.add_argument("--run-timeout", type=float, default=60)
    parser.add_argument("--suite-timeout", type=float, default=900)
    parser.add_argument("--output", type=Path, required=True,
                        help="New exclusive output directory; existing paths are refused")
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
        "format_version": 1, "experiment": "controlled_handler_failure",
        "started_at": datetime.now(UTC), "complete": False, "runs": [],
        "configuration": {
            "jobs_per_cohort": arguments.jobs, "repetitions": arguments.repetitions,
            "cohorts": COHORTS, "worker_concurrency": arguments.concurrency,
            "submission_concurrency": benchmark.SUBMISSION_CONCURRENCY,
            "poll_interval_seconds": benchmark.POLL_INTERVAL_SECONDS,
            "first_seed": arguments.seed,
            "repetition_seeds": list(range(arguments.seed, arguments.seed + arguments.repetitions)),
            "fault_fraction": 0.1, "seeded_component": "fault-index membership only",
            "handler": "controlled_failure", "injected_fail_until_attempt": 1,
            "unfaulted_fail_until_attempt": 0, "priority": 0, "warmup": False,
            "retry_policy": {"base_seconds": 1, "max_seconds": 1,
                             "first_backoff_seconds_min": 0.5, "first_backoff_seconds_max": 1},
            "run_timeout_seconds": arguments.run_timeout,
            "suite_timeout_seconds": arguments.suite_timeout,
            "resource_limits": {
                "database": {"cpus": 2, "memory_bytes": 512 * 1024**2},
                "api": {"cpus": 1, "memory_bytes": 256 * 1024**2},
                "worker": {"cpus": 1, "memory_bytes": 256 * 1024**2},
            },
            "worker_database_pool": {"pool_size": 5, "max_overflow": 10},
            "completion_observation": "private PostgreSQL status polling",
        },
        "commands": {
            "invocation": [sys.executable, "scripts/local_reliability.py", *sys.argv[1:]],
            "migration": [benchmark.RUNTIME_IMAGE, "alembic", "upgrade", "head"],
            "worker": [benchmark.RUNTIME_IMAGE, "python", "-m",
                       "distributed_cloud_scheduler.worker"],
        },
        "limitations": [
            "Controlled handler failures only; no broker DLQ, crash, outage or cloud claim.",
            "READY outboxes are unpublished durable intent, not broker delivery evidence.",
            "Fault membership is seeded; UUIDs, runtime equal jitter and timings vary.",
            "PostgreSQL timestamps precede commits; observed completion includes polling delay.",
            "Job.started_at is the last attempt; initial outcome and recovery use attempt history.",
            "Only 30 recovery samples per default recovery run; p99 is the observed maximum.",
            "Finite local cohorts establish no SLA or universal recovery guarantee.",
        ],
    }
    exit_code = 1
    try:
        async with asyncio.timeout(arguments.suite_timeout):
            await execute_suite(arguments, resources, report)
        report["complete"] = True
        exit_code = 0
    except (Exception, asyncio.CancelledError) as error:
        report["error"] = safe_error(error)
        print(f"Reliability experiment incomplete ({type(error).__name__}); artifacts retained.",
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
            print("Could not save final artifact; owned cleanup was attempted.", file=sys.stderr)
    return exit_code


def main() -> int:
    arguments = parse_arguments()
    try:
        with asyncio.Runner(loop_factory=benchmark.create_event_loop) as runner:
            return runner.run(run(arguments))
    except FileExistsError:
        print("Output path already exists; choose a new directory.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Reliability experiment interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
