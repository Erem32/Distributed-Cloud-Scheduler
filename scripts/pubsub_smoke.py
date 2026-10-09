"""Exercise live API identity, retries, scheduling, priority and emulator push.

Set TEST_DATABASE_URL to a separate database ending in _test and explicitly set
PUBSUB_EMULATOR_HOST. Start the optional compose.pubsub.yaml emulator first. When
the emulator runs in Docker and this script on the host, use --push-host
host.docker.internal. Inside Docker, supply this container's reachable DNS name.
Only randomized emulator resources and one disposable database schema are changed.
"""

import argparse
import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import UUID, uuid4

import psycopg
from dotenv import dotenv_values
from psycopg import sql
from sqlalchemy import func, select, update
from sqlalchemy.engine import make_url

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.dispatcher import dispatch_once
from distributed_cloud_scheduler.execution import claim_job, recover_expired
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.outbox import DispatchOutbox, DispatchStatus
from distributed_cloud_scheduler.pubsub import PubSubPublisher
from distributed_cloud_scheduler.pubsub_config import PubSubSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request(
    url: str,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    """Local smoke traffic bypasses proxy variables and never authenticates to cloud."""
    request_headers = {"Content-Type": "application/json"} if body is not None else {}
    request_headers.update(headers or {})
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(
            Request(url, data=body, method=method, headers=request_headers), timeout=5
        ) as r:
            return r.status, r.read()
    except HTTPError as error:
        return error.code, error.read()


def emulator_resource(host: str, resource: str, method: str, body: dict | None = None) -> None:
    status, _ = request(
        f"http://{host}/v1/{resource}",
        method,
        json.dumps(body).encode() if body is not None else None,
    )
    require(status == 200, f"Emulator {method} failed with HTTP {status}")


def acknowledged_pushes(log_path: Path) -> int:
    # Access logs prove that duplicate messages reached the live server and were ACKed.
    return log_path.read_bytes().count(b'POST /pubsub/push HTTP/1.1" 204')


def stop_child(child: subprocess.Popen) -> None:
    """Also stop a Windows venv launcher's interpreter and release its file handles."""
    if child.poll() is None:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/PID", str(child.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        else:
            child.terminate()
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def wait_for_server(child: subprocess.Popen, server_url: str, label: str) -> None:
    deadline = time.monotonic() + 20
    while True:
        require(child.poll() is None, f"Live {label} exited during startup")
        try:
            ready, _ = request(f"{server_url}/ready")
            if ready == 200:
                return
        except (OSError, URLError):
            pass
        require(time.monotonic() < deadline, f"Live {label} was not ready")
        time.sleep(0.05)


async def api_submission(
    api_url: str, submission: dict, key: str | None = None
) -> tuple[int, dict]:
    headers = {"Idempotency-Key": key} if key is not None else {}
    status, body = await asyncio.to_thread(
        request, f"{api_url}/jobs", "POST", json.dumps(submission).encode(), headers
    )
    return status, json.loads(body)


async def public_job(api_url: str, job_id: UUID) -> dict:
    status, body = await asyncio.to_thread(request, f"{api_url}/jobs/{job_id}")
    require(status == 200, "Live API could not read the submitted job")
    return json.loads(body)


async def public_attempts(api_url: str, job_id: UUID, query: str = "") -> list[dict]:
    suffix = f"?{query}" if query else ""
    status, body = await asyncio.to_thread(request, f"{api_url}/jobs/{job_id}/attempts{suffix}")
    require(status == 200, "Live API could not read execution attempt history")
    history = json.loads(body)
    require(isinstance(history, list), "Attempt history was not a plain ordered list")
    require(
        all(
            isinstance(attempt, dict)
            and not {"claim_token", "owner_token", "idempotency_key", "request_fingerprint"}
            .intersection(attempt)
            for attempt in history
        ),
        "Attempt history exposed internal ownership or submission metadata",
    )
    return history


async def api_retry(api_url: str, job_id: UUID, generation: int) -> tuple[int, dict]:
    status, body = await asyncio.to_thread(
        request,
        f"{api_url}/jobs/{job_id}/retry",
        "POST",
        json.dumps({"expected_generation": generation}).encode(),
    )
    return status, json.loads(body)


async def require_keyed_rows(database: Database, key: str, job_id: UUID) -> None:
    async with database.session_factory() as session:
        job_count = await session.scalar(
            select(func.count()).select_from(Job).where(Job.idempotency_key == key)
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(DispatchOutbox).where(DispatchOutbox.job_id == job_id)
        )
    require(job_count == 1 and outbox_count == 1, "Key replay added a job or outbox obligation")


async def wait_for_ack(log_path: Path, expected: int) -> None:
    async with asyncio.timeout(20):
        while acknowledged_pushes(log_path) < expected:
            await asyncio.sleep(0.05)


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        require(job is not None, "Submitted job disappeared")
        assert job is not None
        return job


async def read_outbox(database: Database, job_id: UUID) -> DispatchOutbox:
    async with database.session_factory() as session:
        row = await session.scalar(select(DispatchOutbox).where(DispatchOutbox.job_id == job_id))
        require(row is not None, "Submitted outbox obligation disappeared")
        assert row is not None
        return row


async def read_outbox_rows(database: Database, job_id: UUID) -> list[DispatchOutbox]:
    async with database.session_factory() as session:
        rows = await session.scalars(
            select(DispatchOutbox)
            .where(DispatchOutbox.job_id == job_id)
            .order_by(DispatchOutbox.created_at, DispatchOutbox.id)
        )
        return list(rows)


async def before_due(database: Database, due: datetime) -> bool:
    """Compare with PostgreSQL's clock rather than the host's wall clock."""
    async with database.session_factory() as session:
        return bool(await session.scalar(select(func.clock_timestamp() < due)))


async def dispatch_when_due(
    database: Database, publisher: PubSubPublisher, settings: Settings, due: datetime
) -> None:
    async with asyncio.timeout(20):
        while await before_due(database, due):
            await asyncio.sleep(0.05)
    require(
        await dispatch_once(database, publisher, settings), "Due execution retry was not published"
    )


async def submit(
    database: Database, job_type: str, payload: dict, *, max_attempts: int = 3
) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(
            session, JobCreate(type=job_type, payload=payload, max_attempts=max_attempts)
        )
        return job.id


async def wait_for_outcome(database: Database, job_id: UUID, status: JobStatus) -> Job:
    async with asyncio.timeout(20):
        while True:
            job = await read_job(database, job_id)
            if job.status is status:
                return job
            require(
                job.status not in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED),
                "Job reached an unexpected terminal outcome",
            )
            await asyncio.sleep(0.05)


async def exercise_execution_retries(
    database: Database,
    publisher: PubSubPublisher,
    settings: Settings,
    api_url: str,
    log_path: Path,
) -> None:
    """Prove database-owned attempts through the real API, SDK and push adapter."""
    transient_submission = {
        "type": "controlled_failure",
        "payload": {"fail_until_attempt": 1},
        "max_attempts": 3,
    }
    transient_key = f"smoke-transient:{uuid4().hex}"
    status, created = await api_submission(api_url, transient_submission, transient_key)
    require(status == 201, "Live API did not accept the transient retry job")
    transient_id = UUID(created["id"])
    acknowledgements = acknowledged_pushes(log_path)
    require(await dispatch_once(database, publisher, settings), "Transient job was not dispatched")
    waiting = await wait_for_outcome(database, transient_id, JobStatus.RETRY_WAIT)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        waiting.attempt_count == 1
        and waiting.next_attempt_at is not None
        and waiting.error_info is not None
        and waiting.error_info.get("code") == "controlled_failure",
        "Transient failure did not persist one attempt and its retry due time before ACK",
    )
    assert waiting.next_attempt_at is not None
    due = waiting.next_attempt_at
    rows = await read_outbox_rows(database, transient_id)
    require(
        len(rows) == 2
        and rows[0].status is DispatchStatus.PUBLISHED
        and rows[1].status is DispatchStatus.READY
        and rows[1].available_at == due
        and rows[1].publish_attempt_count == 0,
        "Retry outcome did not atomically create its unpublished due-time obligation",
    )
    history = await public_attempts(api_url, transient_id)
    require(
        len(history) == 1
        and history[0].get("attempt_number") == 1
        and history[0].get("status") == "FAILED"
        and history[0].get("finished_at") is not None
        and history[0].get("next_attempt_at") is not None,
        "Transient outcome did not expose its finalized retry attempt",
    )
    require(await before_due(database, due), "Positive retry delay elapsed before its gate check")
    require(
        not await dispatch_once(database, publisher, settings),
        "Dispatcher published an execution retry before its database due time",
    )
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(transient_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    unchanged = await read_job(database, transient_id)
    require(
        unchanged.status is JobStatus.RETRY_WAIT
        and unchanged.attempt_count == 1
        and unchanged.next_attempt_at == due
        and await public_attempts(api_url, transient_id) == history
        and [row.id for row in await read_outbox_rows(database, transient_id)]
        == [row.id for row in rows],
        "An early broker duplicate consumed an attempt or changed the persisted retry",
    )
    acknowledgements = acknowledged_pushes(log_path)
    await dispatch_when_due(database, publisher, settings, due)
    completed = await wait_for_outcome(database, transient_id, JobStatus.SUCCEEDED)
    await wait_for_ack(log_path, acknowledgements + 1)
    rows = await read_outbox_rows(database, transient_id)
    history = await public_attempts(api_url, transient_id)
    require(
        completed.attempt_count == 2
        and completed.result_metadata == {"attempt": 2}
        and completed.next_attempt_at is None
        and len(rows) == 2
        and all(
            row.status is DispatchStatus.PUBLISHED and row.publish_attempt_count == 1
            for row in rows
        )
        and [attempt.get("attempt_number") for attempt in history] == [1, 2]
        and [attempt.get("status") for attempt in history] == ["FAILED", "SUCCEEDED"],
        "Due retry did not succeed with exactly two ordered attempts and publications",
    )
    terminal = await public_job(api_url, transient_id)
    status, replayed = await api_submission(api_url, transient_submission, transient_key)
    require(
        status == 200 and replayed == terminal,
        "Submission identity did not retain the current retried job",
    )
    print(
        "PASS: transient failure -> durable RETRY_WAIT/ACK; early duplicate and due gate; "
        "second attempt succeeds with ordered public history",
        flush=True,
    )

    exhaustion_submission = {
        "type": "controlled_failure",
        "payload": {"fail_until_attempt": 20},
        "max_attempts": 2,
    }
    status, created = await api_submission(api_url, exhaustion_submission)
    require(status == 201, "Live API did not accept the bounded failure job")
    exhausted_id = UUID(created["id"])
    acknowledgements = acknowledged_pushes(log_path)
    require(await dispatch_once(database, publisher, settings), "Exhaustion job was not dispatched")
    waiting = await wait_for_outcome(database, exhausted_id, JobStatus.RETRY_WAIT)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(waiting.next_attempt_at is not None, "First exhausted-job failure omitted its due time")
    assert waiting.next_attempt_at is not None
    acknowledgements = acknowledged_pushes(log_path)
    await dispatch_when_due(database, publisher, settings, waiting.next_attempt_at)
    exhausted = await wait_for_outcome(database, exhausted_id, JobStatus.DEAD_LETTERED)
    await wait_for_ack(log_path, acknowledgements + 1)
    rows = await read_outbox_rows(database, exhausted_id)
    history = await public_attempts(api_url, exhausted_id)
    require(
        exhausted.attempt_count == 2
        and exhausted.next_attempt_at is None
        and exhausted.error_info is not None
        and exhausted.error_info.get("code") == "controlled_failure"
        and exhausted.dead_letter_reason == "attempts_exhausted"
        and exhausted.dead_lettered_at is not None
        and len(rows) == 2
        and all(row.status is DispatchStatus.PUBLISHED for row in rows)
        and [attempt.get("attempt_number") for attempt in history] == [1, 2]
        and all(attempt.get("status") == "FAILED" for attempt in history),
        "Execution retry budget did not end at two finalized failed attempts",
    )
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(exhausted_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        (await read_job(database, exhausted_id)).attempt_count == 2
        and await public_attempts(api_url, exhausted_id) == history
        and len(await read_outbox_rows(database, exhausted_id)) == 2,
        "An exhausted-job broker duplicate extended the attempt budget",
    )
    print("PASS: exhausted retries retain dead-letter context and stable duplicate ACK", flush=True)

    status, created = await api_submission(api_url, {"type": "unknown_task_probe", "payload": {}})
    require(status == 201, "Live API did not accept the permanent failure probe")
    permanent_id = UUID(created["id"])
    acknowledgements = acknowledged_pushes(log_path)
    require(
        await dispatch_once(database, publisher, settings), "Permanent failure was not dispatched"
    )
    permanent = await wait_for_outcome(database, permanent_id, JobStatus.FAILED)
    await wait_for_ack(log_path, acknowledgements + 1)
    rows = await read_outbox_rows(database, permanent_id)
    history = await public_attempts(api_url, permanent_id)
    require(
        permanent.attempt_count == 1
        and permanent.next_attempt_at is None
        and permanent.dead_lettered_at is None
        and permanent.dead_letter_reason is None
        and permanent.error_info == {"code": "unknown_task", "retryable": False}
        and len(rows) == 1
        and rows[0].status is DispatchStatus.PUBLISHED
        and len(history) == 1
        and history[0].get("status") == "FAILED",
        "Permanent failure scheduled another obligation or consumed another attempt",
    )
    print(
        "PASS: unknown task is a permanent one-attempt failure with no retry obligation", flush=True
    )


async def exercise_manual_recovery(
    database: Database,
    publisher: PubSubPublisher,
    settings: Settings,
    api_url: str,
    log_path: Path,
    exhausted_id: UUID,
) -> None:
    """Recover intentional worker loss, then prove stale commands stay fenced."""
    recovery_key = f"smoke-recovery:{uuid4().hex}"
    submission = {"type": "calculation", "payload": {"numbers": [17, 25]}, "max_attempts": 1}
    status, created = await api_submission(api_url, submission, recovery_key)
    require(status == 201 and created.get("generation") == 0, "Recovery submission failed")
    recovered_id = UUID(created["id"])
    claim = await claim_job(database, recovered_id, settings.worker_lease_seconds)
    require(claim is not None, "Worker-loss probe could not claim its submitted job")
    # Deliberately expire this disposable schema's committed ownership. Production
    # recovery must finalize that history row rather than inventing a terminal job.
    async with database.session_factory() as session, session.begin():
        await session.execute(
            update(Job)
            .where(Job.id == recovered_id)
            .values(lease_expires_at=Job.started_at + timedelta(microseconds=1))
        )
    require(
        await recover_expired(database, job_id=recovered_id) == 1,
        "Expired worker ownership was not recovered",
    )
    terminal = await public_job(api_url, recovered_id)
    prior_history = await public_attempts(api_url, recovered_id)
    prior_rows = await read_outbox_rows(database, recovered_id)
    require(
        terminal.get("status") == "DEAD_LETTERED"
        and terminal.get("generation") == 0
        and terminal.get("attempt_count") == 1
        and terminal.get("dead_letter_reason") == "attempts_exhausted"
        and terminal.get("dead_lettered_at") is not None
        and terminal.get("error_info") == {"code": "worker_lost", "retryable": True}
        and len(prior_history) == 1
        and prior_history[0].get("generation") == 0
        and prior_history[0].get("status") == "FAILED"
        and prior_history[0].get("error_info") == terminal.get("error_info")
        and len(prior_rows) == 1
        and prior_rows[0].status is DispatchStatus.READY,
        "Worker-loss exhaustion did not preserve final context and its owned attempt",
    )
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(recovered_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        await public_job(api_url, recovered_id) == terminal
        and await public_attempts(api_url, recovered_id) == prior_history,
        "A terminal duplicate changed dead-letter context or attempt history",
    )
    status, reopened = await api_retry(api_url, recovered_id, 0)
    require(
        status == 202
        and reopened.get("generation") == 1
        and reopened.get("status") == "PENDING"
        and reopened.get("attempt_count") == 0
        and reopened.get("created_at") == created.get("created_at")
        and reopened.get("payload") == submission["payload"]
        and reopened.get("max_attempts") == 1
        and all(
            reopened.get(field) is None
            for field in (
                "started_at", "finished_at", "next_attempt_at", "result_metadata", "error_info",
                "dead_lettered_at", "dead_letter_reason",
            )
        ),
        "Manual recovery did not reset current state with an independent generation budget",
    )
    require(
        await public_attempts(api_url, recovered_id) == prior_history,
        "Manual retry replaced the prior generation's finalized history",
    )
    status, replayed = await api_submission(api_url, submission, recovery_key)
    require(status == 200 and replayed == reopened, "Recovery changed submission identity")
    status, _ = await api_retry(api_url, recovered_id, 0)
    require(status == 409, "A repeated recovery command reopened the same generation twice")
    acknowledgements = acknowledged_pushes(log_path)
    require(
        await dispatch_once(database, publisher, settings), "Manual recovery was not dispatched"
    )
    recovered = await wait_for_outcome(database, recovered_id, JobStatus.SUCCEEDED)
    await wait_for_ack(log_path, acknowledgements + 1)
    history = await public_attempts(api_url, recovered_id)
    rows = await read_outbox_rows(database, recovered_id)
    require(
        recovered.generation == 1
        and recovered.attempt_count == 1
        and recovered.result_metadata == {"sum": 42}
        and recovered.dead_lettered_at is None
        and recovered.dead_letter_reason is None
        and len(history) == 2
        and history[:1] == prior_history
        and [(row.get("generation"), row.get("attempt_number")) for row in history]
        == [(0, 1), (1, 1)]
        and history[1].get("status") == "SUCCEEDED"
        and len(rows) == 2
        and rows[0].generation == 0
        and rows[0].status is DispatchStatus.SKIPPED
        and rows[0].publish_attempt_count == 0
        and rows[1].generation == 1
        and rows[1].status is DispatchStatus.PUBLISHED
        and rows[1].publish_attempt_count == 1,
        "Recovered generation did not execute once while preserving/skipping prior obligations",
    )
    require(
        await public_attempts(api_url, recovered_id, "generation=0") == prior_history
        and await public_attempts(api_url, recovered_id, "generation=1") == history[1:]
        and await public_attempts(api_url, recovered_id, "limit=1&offset=1") == history[1:],
        "Generation history filtering or pagination changed the retained order",
    )
    terminal = await public_job(api_url, recovered_id)
    status, replayed = await api_submission(api_url, submission, recovery_key)
    require(status == 200 and replayed == terminal, "Terminal recovery replay lost its identity")
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(recovered_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        await public_job(api_url, recovered_id) == terminal
        and await public_attempts(api_url, recovered_id) == history,
        "A duplicate overwrote the recovered terminal generation",
    )
    print(
        "PASS: worker-loss dead letter -> HTTP manual retry -> SDK/emulator/push success; "
        "old obligation skipped, history retained, keyed replay and duplicate ACK stable",
        flush=True,
    )

    original = await public_attempts(api_url, exhausted_id)
    status, reopened = await api_retry(api_url, exhausted_id, 0)
    require(
        status == 202 and reopened.get("generation") == 1 and reopened.get("attempt_count") == 0,
        "Manual retry did not reopen an exhausted task with a fresh attempt budget",
    )
    acknowledgements = acknowledged_pushes(log_path)
    require(
        await dispatch_once(database, publisher, settings), "Reopened failure was not dispatched"
    )
    exhausted = await wait_for_outcome(database, exhausted_id, JobStatus.DEAD_LETTERED)
    await wait_for_ack(log_path, acknowledgements + 1)
    history = await public_attempts(api_url, exhausted_id)
    require(
        exhausted.generation == 1
        and exhausted.attempt_count == 1
        and exhausted.dead_letter_reason == "attempts_exhausted"
        and history[:1] == original
        and [(row.get("generation"), row.get("attempt_number")) for row in history]
        == [(0, 1), (1, 1)]
        and all(row.get("status") == "FAILED" for row in history),
        "A repeatedly failed recovery lost history or extended its per-generation budget",
    )
    terminal = await public_job(api_url, exhausted_id)
    row_ids = [row.id for row in await read_outbox_rows(database, exhausted_id)]
    status, _ = await api_retry(api_url, exhausted_id, 0)
    require(
        status == 409
        and await public_job(api_url, exhausted_id) == terminal
        and await public_attempts(api_url, exhausted_id) == history
        and [row.id for row in await read_outbox_rows(database, exhausted_id)] == row_ids,
        "A stale recovery command reopened a later eligible terminal generation",
    )
    print(
        "PASS: repeated recovery failure is bounded; stale generation command returns 409",
        flush=True,
    )


async def runtime_once(module: str, environment: dict[str, str]) -> None:
    """Run a separate runtime process, retaining its database state after exit."""
    completed = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", f"distributed_cloud_scheduler.{module}", "--once"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        timeout=20,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    require(completed.returncode == 0, f"Live {module} --once exited unsuccessfully")


async def exercise_scheduled_delivery(
    database: Database,
    publisher: PubSubPublisher,
    settings: Settings,
    api_url: str,
    log_path: Path,
    environment: dict[str, str],
) -> None:
    """Restart scheduler processes around due admission and durable publication."""
    async with database.session_factory() as session:
        database_now = await session.scalar(select(func.clock_timestamp()))
    require(isinstance(database_now, datetime), "Database did not return its scheduling clock")
    assert isinstance(database_now, datetime)
    due = database_now + timedelta(seconds=12)
    scheduled_key = f"smoke-scheduled:{uuid4().hex}"
    submission = {
        "type": "calculation",
        "payload": {"numbers": [19, 23]},
        "scheduled_at": due.isoformat(timespec="microseconds"),
    }
    status, created = await api_submission(api_url, submission, scheduled_key)
    require(
        status == 201
        and created.get("status") == "SCHEDULED"
        and created.get("attempt_count") == 0
        and created.get("generation") == 0
        and datetime.fromisoformat(created["scheduled_at"]) == due,
        "Live API did not preserve a UTC scheduled submission as durable admission intent",
    )
    scheduled_id = UUID(created["id"])
    offset_submission = {
        **submission,
        "scheduled_at": due.astimezone(timezone(timedelta(hours=2))).isoformat(
            timespec="microseconds"
        ),
    }
    status, replayed = await api_submission(api_url, offset_submission, scheduled_key)
    require(
        status == 200 and replayed == created,
        "Equivalent scheduling instants with different offsets did not replay one identity",
    )
    status, conflict = await api_submission(
        api_url,
        {**submission, "scheduled_at": (due + timedelta(microseconds=1)).isoformat()},
        scheduled_key,
    )
    require(
        status == 409 and scheduled_key not in json.dumps(conflict),
        "A different original schedule did not return a sanitized identity conflict",
    )
    async with database.session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(Job).where(Job.idempotency_key == scheduled_key)
        )
    require(
        count == 1
        and not await read_outbox_rows(database, scheduled_id)
        and not await public_attempts(api_url, scheduled_id),
        "Scheduled submission/replay produced execution history or a premature outbox obligation",
    )
    require(await before_due(database, due), "Scheduled due time elapsed before the early probe")
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(scheduled_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    await runtime_once("scheduler", environment)
    await runtime_once("worker", environment)
    require(
        await before_due(database, due)
        and await public_job(api_url, scheduled_id) == created
        and not await public_attempts(api_url, scheduled_id)
        and not await read_outbox_rows(database, scheduled_id)
        and not await dispatch_once(database, publisher, settings),
        "Early SDK delivery, polling or scheduler admission bypassed the database due time",
    )
    print(
        "PASS: future keyed schedule normalizes offsets; early SDK delivery ACKs without "
        "attempts/outbox, and separate scheduler/polling processes respect the due gate",
        flush=True,
    )
    async with asyncio.timeout(20):
        while await before_due(database, due):
            await asyncio.sleep(0.05)
    # An ID does not substitute for scheduler admission, even after the original
    # due time. The earlier scheduler exited before due; this durable row survives.
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(scheduled_id)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        await public_job(api_url, scheduled_id) == created
        and not await public_attempts(api_url, scheduled_id)
        and not await read_outbox_rows(database, scheduled_id),
        "A due but unadmitted scheduled ID bypassed authoritative admission",
    )
    await runtime_once("scheduler", environment)
    admitted = await public_job(api_url, scheduled_id)
    rows = await read_outbox_rows(database, scheduled_id)
    require(
        admitted.get("status") == "PENDING"
        and admitted.get("attempt_count") == 0
        and admitted.get("scheduled_at") == created.get("scheduled_at")
        and len(rows) == 1
        and rows[0].status is DispatchStatus.READY
        and rows[0].generation == 0
        and rows[0].publish_attempt_count == 0
        and not await public_attempts(api_url, scheduled_id),
        "Restarted scheduler did not atomically persist one immediate admission obligation",
    )
    # The admission process has exited before publication. A fresh scheduler must
    # retain the committed outbox and avoid admitting the same scheduled row again.
    await runtime_once("scheduler", environment)
    require(
        await public_job(api_url, scheduled_id) == admitted
        and [row.id for row in await read_outbox_rows(database, scheduled_id)] == [rows[0].id],
        "A scheduler restart after admission duplicated or lost durable publication intent",
    )
    acknowledgements = acknowledged_pushes(log_path)
    await runtime_once("dispatcher", environment)
    completed = await wait_for_outcome(database, scheduled_id, JobStatus.SUCCEEDED)
    await wait_for_ack(log_path, acknowledgements + 1)
    history = await public_attempts(api_url, scheduled_id)
    rows = await read_outbox_rows(database, scheduled_id)
    require(
        completed.attempt_count == 1
        and completed.result_metadata == {"sum": 42}
        and completed.scheduled_at == due
        and completed.started_at is not None
        and completed.started_at >= due
        and len(history) == 1
        and history[0].get("status") == "SUCCEEDED"
        and history[0].get("generation") == 0
        and len(rows) == 1
        and rows[0].status is DispatchStatus.PUBLISHED
        and rows[0].publish_attempt_count == 1,
        "Scheduled dispatcher/SDK/push flow did not execute one due owned attempt",
    )
    terminal = await public_job(api_url, scheduled_id)
    status, replayed = await api_submission(api_url, offset_submission, scheduled_key)
    require(
        status == 200 and replayed == terminal,
        "Terminal scheduled identity did not retain its normalized original due date",
    )
    print(
        "PASS: scheduler restart before due and after admission retains one obligation; "
        "dispatcher CLI -> SDK/emulator/push executes once after due with original date/history",
        flush=True,
    )


async def priority_submission(api_url: str, priority: int, scheduled_at: str | None = None) -> UUID:
    status, created = await api_submission(
        api_url,
        {
            "type": "calculation",
            "payload": {"numbers": [20, 22]},
            "priority": priority,
            "scheduled_at": scheduled_at,
        },
    )
    require(
        status == 201 and created.get("priority") == priority and "ready_at" not in created,
        "Live API did not preserve public priority while keeping readiness internal",
    )
    return UUID(created["id"])


async def priority_publication(
    database: Database,
    api_url: str,
    log_path: Path,
    environment: dict[str, str],
    expected: list[UUID],
) -> None:
    """Observe committed outbox selection rather than broker delivery ordering."""
    published: set[UUID] = set()
    for job_id in expected:
        acknowledgements = acknowledged_pushes(log_path)
        await runtime_once("dispatcher", environment)
        published.add(job_id)
        for candidate in expected:
            row = await read_outbox(database, candidate)
            require(
                row.status is (
                    DispatchStatus.PUBLISHED if candidate in published else DispatchStatus.READY
                ),
                "Dispatcher priority selection did not publish exactly the expected obligation",
            )
        completed = await wait_for_outcome(database, job_id, JobStatus.SUCCEEDED)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            completed.attempt_count == 1
            and completed.result_metadata == {"sum": 42}
            and len(await public_attempts(api_url, job_id)) == 1,
            "Selected priority obligation did not complete one owned SDK/push attempt",
        )


async def exercise_priority_selection(
    database: Database,
    publisher: PubSubPublisher,
    api_url: str,
    log_path: Path,
    environment: dict[str, str],
) -> None:
    """Prove selection and aging locally, with ID-only delivery kept authoritative."""
    low = await priority_submission(api_url, 0)
    medium = await priority_submission(api_url, 4)
    high = await priority_submission(api_url, 9)
    await priority_publication(database, api_url, log_path, environment, [high, medium, low])
    print(
        "PASS: live API priorities -> dispatcher CLI selects high/medium/low outbox obligations; "
        "real SDK/push outcomes do not depend on broker delivery order",
        flush=True,
    )

    async with database.session_factory() as session:
        database_now = await session.scalar(select(func.clock_timestamp()))
    require(isinstance(database_now, datetime), "Database did not return its priority clock")
    assert isinstance(database_now, datetime)
    past_due = (database_now - timedelta(seconds=1)).isoformat(timespec="microseconds")
    scheduled_low = await priority_submission(api_url, 0, past_due)
    tied_high = [await priority_submission(api_url, 9, past_due) for _ in range(2)]
    expected = [*sorted(tied_high), scheduled_low]
    # Give these disposable rows one equal eligibility timestamp so the live
    # scheduler and polling runtimes must resolve a real rank/UUID tie.
    async with database.session_factory() as session, session.begin():
        tied_at = await session.scalar(select(func.clock_timestamp()))
        await session.execute(
            update(Job).where(Job.id.in_(expected)).values(ready_at=tied_at)
        )
    scheduler_environment = {**environment, "SCHEDULER_BATCH_SIZE": "1"}
    admitted: set[UUID] = set()
    for job_id in expected:
        await runtime_once("scheduler", scheduler_environment)
        admitted.add(job_id)
        for candidate in expected:
            job = await read_job(database, candidate)
            rows = await read_outbox_rows(database, candidate)
            require(
                job.status is (JobStatus.PENDING if candidate in admitted else JobStatus.SCHEDULED)
                and job.attempt_count == 0
                and len(rows) == (1 if candidate in admitted else 0),
                "Scheduler priority/tie selection did not atomically admit one expected job",
            )
    claimed: set[UUID] = set()
    for job_id in expected:
        await runtime_once("worker", environment)
        claimed.add(job_id)
        for candidate in expected:
            job = await read_job(database, candidate)
            require(
                job.status is (JobStatus.SUCCEEDED if candidate in claimed else JobStatus.PENDING)
                and job.attempt_count == (1 if candidate in claimed else 0),
                "Polling worker priority/tie selection did not execute one expected job",
            )
    # Polling completed these jobs without broker delivery. The dispatcher must
    # skip their original obligations before the next selection experiment.
    await runtime_once("dispatcher", environment)
    for job_id in expected:
        row = await read_outbox(database, job_id)
        require(
            row.status is DispatchStatus.SKIPPED and row.publish_attempt_count == 0,
            "Polling completion did not leave terminal priority obligations safely skippable",
        )
    print(
        "PASS: scheduler batch-one admission and local worker CLI select priority then UUID ties; "
        "completed polling obligations are skipped without publication",
        flush=True,
    )

    high = await priority_submission(api_url, 9)
    low = await priority_submission(api_url, 0)
    acknowledgements = acknowledged_pushes(log_path)
    await publisher.publish(low)
    completed = await wait_for_outcome(database, low, JobStatus.SUCCEEDED)
    await wait_for_ack(log_path, acknowledgements + 1)
    require(
        completed.attempt_count == 1
        and (await read_job(database, high)).status is JobStatus.PENDING
        and (await read_job(database, high)).attempt_count == 0
        and (await read_outbox(database, high)).status is DispatchStatus.READY,
        "A delivered low-priority ID was incorrectly gated by other jobs' priority",
    )
    await priority_publication(database, api_url, log_path, environment, [high])
    await runtime_once("dispatcher", environment)
    require(
        (await read_outbox(database, low)).status is DispatchStatus.SKIPPED,
        "Targeted low-ID execution left an obsolete publication obligation active",
    )
    print(
        "PASS: targeted low-priority SDK ID executes while urgent work awaits dispatch; "
        "priority remains a selection policy",
        flush=True,
    )

    aged_low = await priority_submission(api_url, 0)
    last_high: UUID | None = None
    # Advance only this disposable job's eligibility age instead of waiting for
    # a 45-second real-time head start. Fresh urgent API arrivals continue at each
    # pass; the same low job must progress once its age exceeds the maximum boost.
    for age in (30, 35, 40, 46):
        async with database.session_factory() as session, session.begin():
            aged_at = await session.scalar(select(func.clock_timestamp() - timedelta(seconds=age)))
            await session.execute(update(Job).where(Job.id == aged_low).values(ready_at=aged_at))
            await session.execute(
                update(DispatchOutbox)
                .where(DispatchOutbox.job_id == aged_low)
                .values(available_at=aged_at)
            )
        last_high = await priority_submission(api_url, 9)
        selected = aged_low if age > 45 else last_high
        await priority_publication(database, api_url, log_path, environment, [selected])
        waiting = last_high if age > 45 else aged_low
        require(
            (await read_job(database, waiting)).status is JobStatus.PENDING
            and (await read_job(database, waiting)).attempt_count == 0
            and (await read_outbox(database, waiting)).status is DispatchStatus.READY,
            "A priority publication pass also consumed its unselected contender",
        )
    require(last_high is not None, "Priority aging probe did not submit urgent work")
    assert last_high is not None
    await priority_publication(database, api_url, log_path, environment, [last_high])
    print(
        "PASS: continuous urgent API arrivals win within their head start; controlled aged-low "
        "work progresses beyond 45 seconds, with one owned attempt per selected ID",
        flush=True,
    )


async def exercise(
    settings: Settings,
    cloud: PubSubSettings,
    worker_url: str,
    api_url: str,
    log_path: Path,
    environment: dict[str, str],
) -> None:
    database = Database(settings)
    publisher = PubSubPublisher(cloud, publish_timeout_seconds=1)
    try:
        success_key = f"smoke-success:{uuid4().hex}"
        success_submission = {"type": "calculation", "payload": {"numbers": [8, 13, 21]}}
        replay_submission = {
            "max_attempts": 3,
            "payload": {"numbers": [8, 13, 21]},
            "type": "calculation",
            "scheduled_at": None,
            "priority": 0,
        }
        status, created = await api_submission(api_url, success_submission, success_key)
        require(status == 201 and created.get("status") == "PENDING", "Keyed API create failed")
        success_id = UUID(created["id"])
        status, replayed = await api_submission(api_url, replay_submission, success_key)
        require(status == 200 and replayed == created, "Matching API replay did not return its job")
        status, conflict = await api_submission(
            api_url, {"type": "calculation", "payload": {"numbers": [99]}}, success_key
        )
        require(
            status == 409 and success_key not in json.dumps(conflict),
            "Conflicting submission did not return a sanitized HTTP 409",
        )
        await require_keyed_rows(database, success_key, success_id)
        require(
            (await read_job(database, success_id)).attempt_count == 0,
            "API replay consumed an execution attempt",
        )
        require(
            (await read_outbox(database, success_id)).status is DispatchStatus.READY,
            "Submission did not persist its READY obligation",
        )
        acknowledgements = acknowledged_pushes(log_path)
        dispatched = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "distributed_cloud_scheduler.dispatcher", "--once"],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            timeout=20,
            check=False,
        )
        require(dispatched.returncode == 0, "Live dispatcher --once exited unsuccessfully")
        job = await wait_for_outcome(database, success_id, JobStatus.SUCCEEDED)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            job.attempt_count == 1 and job.result_metadata == {"sum": 42},
            "Push did not execute authoritative calculation state once",
        )
        row = await read_outbox(database, success_id)
        require(
            row.status is DispatchStatus.PUBLISHED and row.publish_attempt_count == 1,
            "Real broker acceptance was not persisted",
        )
        terminal = await public_job(api_url, success_id)
        status, replayed = await api_submission(api_url, replay_submission, success_key)
        require(
            status == 200 and replayed == terminal and replayed.get("status") == "SUCCEEDED",
            "Terminal API replay did not return the job's current representation",
        )
        acknowledgements = acknowledged_pushes(log_path)
        await publisher.publish(success_id)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            await public_job(api_url, success_id) == terminal,
            "Broker duplicate changed the successful job representation",
        )
        await require_keyed_rows(database, success_key, success_id)
        row = await read_outbox(database, success_id)
        require(
            row.status is DispatchStatus.PUBLISHED and row.publish_attempt_count == 1,
            "Submission replay or broker duplicate added a publication attempt",
        )
        print(
            "PASS: live API create/replay/conflict -> SDK -> emulator -> push; terminal replay/ACK",
            flush=True,
        )

        failed_id = await submit(
            database, "controlled_failure", {"fail_until_attempt": 1}, max_attempts=1
        )
        acknowledgements = acknowledged_pushes(log_path)
        require(
            await dispatch_once(database, publisher, settings), "Failure job was not dispatched"
        )
        job = await wait_for_outcome(database, failed_id, JobStatus.DEAD_LETTERED)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            job.attempt_count == 1
            and job.error_info is not None
            and job.error_info.get("code") == "controlled_failure",
            "Task failure was not durably classified before ACK",
        )
        acknowledgements = acknowledged_pushes(log_path)
        await publisher.publish(failed_id)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            (await read_job(database, failed_id)).attempt_count == 1,
            "Broker duplicate retried a terminal task failure",
        )
        print("PASS: terminal task failure and its broker duplicate are ACKed", flush=True)

        await exercise_execution_retries(database, publisher, settings, api_url, log_path)
        await exercise_manual_recovery(
            database, publisher, settings, api_url, log_path, failed_id
        )

        status, _ = await asyncio.to_thread(
            request, f"{worker_url}/pubsub/push", "POST", b'{"message":{"data":"!"}}'
        )
        require(status == 400, "Malformed push was not negatively acknowledged with HTTP 400")
        unknown = json.dumps(
            {
                "subscription": cloud.pubsub_subscription,
                "message": {
                    "messageId": uuid4().hex,
                    "data": base64.b64encode(str(uuid4()).encode()).decode(),
                },
            }
        ).encode()
        status, _ = await asyncio.to_thread(request, f"{worker_url}/pubsub/push", "POST", unknown)
        require(status == 404, "Unknown ID did not receive HTTP 404")
        status, _ = await asyncio.to_thread(
            request, f"{worker_url}/pubsub/push", "POST", b" " * (64 * 1024)
        )
        require(status == 413, "Oversized push did not receive HTTP 413")
        print(
            "PASS: malformed, unknown and oversized pushes receive negative acknowledgements",
            flush=True,
        )

        outage_id = await submit(database, "calculation", {"numbers": [2, 3]})
        # Hold a bound but non-listening socket so no other service can acquire this port.
        with socket.socket() as closed_broker:
            closed_broker.bind(("127.0.0.1", 0))
            outage_cloud = cloud.model_copy(
                update={"pubsub_emulator_host": f"127.0.0.1:{closed_broker.getsockname()[1]}"}
            )
            unavailable = PubSubPublisher(outage_cloud, publish_timeout_seconds=0.5)
            try:
                require(
                    await dispatch_once(database, unavailable, settings),
                    "Unavailable broker did not exercise a publication attempt",
                )
            finally:
                await unavailable.close()
        row = await read_outbox(database, outage_id)
        require(
            row.status is DispatchStatus.READY
            and row.publish_attempt_count == 1
            and row.last_error_code in ("publish_timeout", "publisher_unavailable"),
            "Unavailable broker did not leave durable retryable publication state",
        )
        require(
            (await read_job(database, outage_id)).attempt_count == 0,
            "Publication failure consumed an execution attempt",
        )
        acknowledgements = acknowledged_pushes(log_path)
        async with asyncio.timeout(10):
            while not await dispatch_once(database, publisher, settings):
                await asyncio.sleep(0.05)
        job = await wait_for_outcome(database, outage_id, JobStatus.SUCCEEDED)
        await wait_for_ack(log_path, acknowledgements + 1)
        row = await read_outbox(database, outage_id)
        require(
            row.status is DispatchStatus.PUBLISHED
            and row.publish_attempt_count == 2
            and row.last_error_code is None,
            "Recovered broker did not finalize its second publication attempt",
        )
        require(
            job.attempt_count == 1 and job.result_metadata == {"sum": 5},
            "Recovered publication did not execute exactly one owned attempt",
        )
        print("PASS: broker outage stays READY; real SDK retry succeeds", flush=True)

        await exercise_scheduled_delivery(
            database, publisher, settings, api_url, log_path, environment
        )
        await exercise_priority_selection(database, publisher, api_url, log_path, environment)

        # Leave these jobs pending so they cannot change the fault checks' dispatch ordering.
        nested_key = f"smoke-nested:{uuid4().hex}"
        status, nested_created = await api_submission(
            api_url,
            {"type": "custom_probe", "payload": {"b": 2, "a": {"y": 4, "x": 3}}},
            nested_key,
        )
        require(status == 201, "Live API did not accept the nested JSON submission")
        status, nested_replayed = await api_submission(
            api_url,
            {
                "payload": {"a": {"x": 3, "y": 4}, "b": 2},
                "max_attempts": 3,
                "type": "custom_probe",
            },
            nested_key,
        )
        require(
            status == 200 and nested_replayed == nested_created,
            "Live API did not normalize nested key order and the default attempt limit",
        )
        await require_keyed_rows(database, nested_key, UUID(nested_created["id"]))
        no_key_results = await asyncio.gather(
            api_submission(api_url, success_submission),
            api_submission(api_url, success_submission),
        )
        require(
            all(status == 201 for status, _ in no_key_results)
            and no_key_results[0][1]["id"] != no_key_results[1][1]["id"],
            "No-key submissions were not independent HTTP 201 jobs",
        )
        for _, created in no_key_results:
            independent_id = UUID(created["id"])
            require(
                (await read_job(database, independent_id)).attempt_count == 0
                and (await read_outbox(database, independent_id)).status is DispatchStatus.READY,
                "No-key API submission did not leave its independent dispatch obligation",
            )
        print(
            "PASS: nested canonical replay and independent no-key live API submissions", flush=True
        )
    finally:
        await publisher.close()
        await database.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--push-host", default="127.0.0.1", help="Worker hostname reachable from the emulator"
    )
    arguments = parser.parse_args()
    require(
        bool(arguments.push_host) and all(c not in arguments.push_host for c in "/?#@ \t\r\n"),
        "--push-host must be a hostname or IP address",
    )
    values = dotenv_values(PROJECT_ROOT / ".env")
    test_url = os.environ.get("TEST_DATABASE_URL") or values.get("TEST_DATABASE_URL")
    require(bool(test_url), "Set TEST_DATABASE_URL to a separate database ending in _test")
    assert test_url is not None
    url = make_url(test_url)
    require(
        url.drivername == "postgresql+psycopg"
        and bool(url.database)
        and url.database.endswith("_test"),
        "TEST_DATABASE_URL must use PostgreSQL psycopg and a database ending in _test",
    )
    application_url = os.environ.get("DATABASE_URL") or values.get("DATABASE_URL")
    require(
        not application_url or make_url(application_url).database != url.database,
        "TEST_DATABASE_URL must differ from the application database",
    )
    emulator_host = os.environ.get("PUBSUB_EMULATOR_HOST")
    require(bool(emulator_host), "Explicitly set PUBSUB_EMULATOR_HOST; cloud smoke is unsupported")
    unique = uuid4().hex
    topic = f"projects/scheduler-local/topics/smoke-{unique}"
    subscription = f"projects/scheduler-local/subscriptions/smoke-{unique}"
    cloud = PubSubSettings(
        pubsub_topic=topic,
        pubsub_subscription=subscription,
        pubsub_emulator_host=emulator_host,
        _env_file=None,
    )
    assert emulator_host is not None
    status, _ = request(f"http://{emulator_host}/v1/projects/scheduler-local/topics")
    require(status == 200, "Pub/Sub emulator is not ready")
    schema = f"pubsub_smoke_{unique}"
    connection_url = url.set(drivername="postgresql").render_as_string(hide_password=False)
    with psycopg.connect(connection_url, autocommit=True, connect_timeout=5) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        child = None
        api_child = None
        created_resources: list[str] = []
        try:
            isolated_url = url.update_query_dict({"options": f"-csearch_path={schema}"})
            settings = Settings(
                database_url=isolated_url.render_as_string(hide_password=False),
                dispatch_publish_timeout_seconds=1,
                dispatch_retry_base_seconds=0.05,
                dispatch_retry_max_seconds=1,
                execution_retry_base_seconds=4,
                execution_retry_max_seconds=8,
                _env_file=None,
            )
            environment = os.environ.copy()
            environment.update(
                DATABASE_URL=settings.database_url.get_secret_value(),
                PUBSUB_TOPIC=topic,
                PUBSUB_SUBSCRIPTION=subscription,
                PUBSUB_EMULATOR_HOST=emulator_host,
                DISPATCH_PUBLISH_TIMEOUT_SECONDS="1",
                EXECUTION_RETRY_BASE_SECONDS="4",
                EXECUTION_RETRY_MAX_SECONDS="8",
            )
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=PROJECT_ROOT,
                env=environment,
                check=True,
            )
            with socket.socket() as listener, socket.socket() as api_listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
                api_listener.bind(("127.0.0.1", 0))
                api_port = api_listener.getsockname()[1]
            worker_url = f"http://127.0.0.1:{port}"
            api_url = f"http://127.0.0.1:{api_port}"
            with tempfile.TemporaryDirectory(prefix="scheduler-pubsub-smoke-") as temp_dir:
                log_path = Path(temp_dir) / "push.log"
                api_log_path = Path(temp_dir) / "api.log"
                with log_path.open("wb") as output, api_log_path.open("wb") as api_output:
                    child = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "distributed_cloud_scheduler.push",
                            "--host",
                            "0.0.0.0",
                            "--port",
                            str(port),
                        ],
                        cwd=PROJECT_ROOT,
                        env=environment,
                        stdin=subprocess.DEVNULL,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
                    )
                    try:
                        wait_for_server(child, worker_url, "push worker")
                        api_child = subprocess.Popen(
                            [
                                sys.executable,
                                "-m",
                                "distributed_cloud_scheduler",
                                "--host",
                                "127.0.0.1",
                                "--port",
                                str(api_port),
                            ],
                            cwd=PROJECT_ROOT,
                            env=environment,
                            stdin=subprocess.DEVNULL,
                            stdout=api_output,
                            stderr=subprocess.STDOUT,
                            creationflags=(
                                subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
                            ),
                        )
                        wait_for_server(api_child, api_url, "submission API")
                        health, _ = request(f"{worker_url}/health")
                        openapi, body = request(f"{worker_url}/openapi.json")
                        require(
                            health == 200 and openapi == 200,
                            "Live push worker probes/OpenAPI failed",
                        )
                        paths = json.loads(body)["paths"]
                        require(
                            "/pubsub/push" in paths and "/jobs" not in paths,
                            "Push runtime did not expose its separate HTTP interface",
                        )
                        openapi, body = request(f"{api_url}/openapi.json")
                        require(
                            openapi == 200
                            and "/jobs" in json.loads(body)["paths"]
                            and "/pubsub/push" not in json.loads(body)["paths"],
                            "Submission API did not expose its separate HTTP interface",
                        )
                        emulator_resource(emulator_host, topic, "PUT", {})
                        created_resources.append(topic)
                        emulator_resource(
                            emulator_host,
                            subscription,
                            "PUT",
                            {
                                "topic": topic,
                                "ackDeadlineSeconds": 60,
                                "pushConfig": {
                                    "pushEndpoint": f"http://{arguments.push_host}:{port}/pubsub/push"
                                },
                            },
                        )
                        created_resources.append(subscription)
                        with asyncio.Runner(loop_factory=create_event_loop) as runner:
                            runner.run(
                                exercise(
                                    settings, cloud, worker_url, api_url, log_path, environment
                                )
                            )
                    finally:
                        try:
                            if api_child is not None:
                                stop_child(api_child)
                                api_child = None
                        finally:
                            stop_child(child)
                            child = None
        finally:
            if api_child is not None:
                stop_child(api_child)
            if child is not None:
                stop_child(child)
            cleanup_failed = False
            try:
                for resource in reversed(created_resources):
                    try:
                        emulator_resource(emulator_host, resource, "DELETE")
                    except (OSError, URLError, RuntimeError):
                        cleanup_failed = True
            finally:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            require(
                not cleanup_failed, "Failed to remove one or more disposable emulator resources"
            )
    print("Pub/Sub emulator end-to-end smoke passed; disposable resources removed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
