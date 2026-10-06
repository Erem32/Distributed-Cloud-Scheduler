"""Prove independent worker processes share durable ownership and crash recovery."""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from pydantic import JsonValue
from sqlalchemy import select, text

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job


@pytest_asyncio.fixture
async def process_database(integration_database_url: str) -> AsyncIterator[Database]:
    project_root = Path(__file__).resolve().parents[2]
    command.upgrade(Config(str(project_root / "alembic.ini")), "head")
    database = Database(Settings(database_url=integration_database_url, _env_file=None))
    try:
        yield database
    finally:
        await database.dispose()


async def submit(database: Database, job_type: str, payload: dict[str, JsonValue]) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(session, JobCreate(type=job_type, payload=payload))
        return job.id


async def read_job(database: Database, job_id: UUID) -> Job:
    async with database.session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


def start_worker(database: Database) -> subprocess.Popen[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": database.engine.url.render_as_string(hide_password=False),
            "WORKER_CONCURRENCY": "2",
            "WORKER_TASK_TIMEOUT_SECONDS": "15",
            "WORKER_LEASE_SECONDS": "60",
            "WORKER_POLL_INTERVAL_SECONDS": "0.05",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return subprocess.Popen(
        [sys.executable, "-m", "distributed_cloud_scheduler.worker", "--once"],
        cwd=Path(__file__).resolve().parents[2],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )


async def assert_worker_exits_successfully(process: subprocess.Popen[str]) -> None:
    _, stderr = await asyncio.to_thread(process.communicate, timeout=15)
    assert process.returncode == 0, stderr


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=5)


@pytest.mark.asyncio
async def test_independent_once_processes_complete_distinct_jobs_once(
    process_database: Database,
) -> None:
    expected_results = {}
    for index in range(8):
        job_id = await submit(process_database, "calculation", {"numbers": [index, index + 1]})
        expected_results[job_id] = {"sum": 2 * index + 1}

    children: list[subprocess.Popen[str]] = []
    try:
        # Start every child before awaiting any, with one engine per independent process.
        for _ in range(4):
            children.append(start_worker(process_database))
        outcomes = await asyncio.gather(
            *(assert_worker_exits_successfully(child) for child in children), return_exceptions=True
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
    finally:
        await asyncio.gather(*(asyncio.to_thread(stop_process, child) for child in children))

    async with process_database.session_factory() as session:
        jobs = list(await session.scalars(select(Job)))
    assert len(jobs) == 8
    succeeded = [job for job in jobs if job.status is JobStatus.SUCCEEDED]
    pending = [job for job in jobs if job.status is JobStatus.PENDING]
    # --once admits one job per process even when normal worker concurrency is two.
    assert len(succeeded) == len({job.id for job in succeeded}) == 4
    assert len(pending) == 4
    for job in succeeded:
        assert job.attempt_count == 1
        assert job.result_metadata == expected_results[job.id]
        assert job.error_info is None
        assert job.claim_token is None
        assert job.lease_expires_at is None
    for job in pending:
        assert job.attempt_count == 0
        assert job.started_at is None
        assert job.finished_at is None
        assert job.result_metadata is None
        assert job.error_info is None


@pytest.mark.asyncio
async def test_forced_process_crash_is_recovered_by_another_process_without_reexecution(
    process_database: Database,
) -> None:
    job_id = await submit(process_database, "sleep", {"seconds": 10})
    child = start_worker(process_database)
    try:
        async with asyncio.timeout(10):
            while True:
                job = await read_job(process_database, job_id)
                if job.status is JobStatus.RUNNING:
                    break
                assert child.poll() is None, "Worker exited before its durable claim was observed"
        assert job.attempt_count == 1
        assert job.claim_token is not None
        assert job.lease_expires_at is not None
        # SIGKILL/TerminateProcess deliberately bypasses graceful cleanup on both platforms.
        child.kill()
        await asyncio.to_thread(child.communicate, timeout=5)
    finally:
        await asyncio.to_thread(stop_process, child)

    abandoned = await read_job(process_database, job_id)
    assert abandoned.status is JobStatus.RUNNING
    assert abandoned.attempt_count == 1
    assert abandoned.claim_token == job.claim_token
    assert abandoned.result_metadata is None
    assert abandoned.error_info is None

    async with process_database.session_factory.begin() as session:
        # Simulate the DB deadline passing while retaining a valid chronological interval.
        await session.execute(
            text(
                "UPDATE jobs SET created_at = clock_timestamp() - INTERVAL '3 seconds', "
                "started_at = clock_timestamp() - INTERVAL '2 seconds', "
                "lease_expires_at = clock_timestamp() - INTERVAL '1 second' "
                "WHERE id = :job_id"
            ),
            {"job_id": job_id},
        )
    recovery_worker = start_worker(process_database)
    try:
        await assert_worker_exits_successfully(recovery_worker)
    finally:
        await asyncio.to_thread(stop_process, recovery_worker)

    recovered = await read_job(process_database, job_id)
    assert recovered.status is JobStatus.FAILED
    assert recovered.error_info == {"code": "worker_lost", "retryable": True}
    assert recovered.result_metadata is None
    assert recovered.attempt_count == 1
    assert recovered.finished_at is not None
    assert recovered.claim_token is None
    assert recovered.lease_expires_at is None
