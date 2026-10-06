"""Exercise real SDK publication, emulator push and PostgreSQL on Windows or Linux.

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
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import UUID, uuid4

import psycopg
from dotenv import dotenv_values
from psycopg import sql
from sqlalchemy import select
from sqlalchemy.engine import make_url

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.dispatcher import dispatch_once
from distributed_cloud_scheduler.jobs import Job, JobCreate, JobStatus, create_job
from distributed_cloud_scheduler.outbox import DispatchOutbox, DispatchStatus
from distributed_cloud_scheduler.pubsub import PubSubPublisher
from distributed_cloud_scheduler.pubsub_config import PubSubSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def request(url: str, method: str = "GET", body: bytes | None = None) -> tuple[int, bytes]:
    """Local smoke traffic bypasses proxy variables and never authenticates to cloud."""
    headers = {"Content-Type": "application/json"} if body is not None else {}
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(Request(url, data=body, method=method, headers=headers), timeout=5) as r:
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


async def submit(database: Database, job_type: str, payload: dict) -> UUID:
    async with database.session_factory() as session:
        job = await create_job(session, JobCreate(type=job_type, payload=payload))
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


async def exercise(
    settings: Settings,
    cloud: PubSubSettings,
    worker_url: str,
    log_path: Path,
    environment: dict[str, str],
) -> None:
    database = Database(settings)
    publisher = PubSubPublisher(cloud, publish_timeout_seconds=1)
    try:
        success_id = await submit(database, "calculation", {"numbers": [8, 13, 21]})
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
        acknowledgements = acknowledged_pushes(log_path)
        await publisher.publish(success_id)
        await wait_for_ack(log_path, acknowledgements + 1)
        require(
            (await read_job(database, success_id)).attempt_count == 1,
            "Broker duplicate changed the successful execution attempt",
        )
        print("PASS: SDK -> emulator -> HTTP push -> persisted result; duplicate ACK", flush=True)

        failed_id = await submit(database, "controlled_failure", {"fail_until_attempt": 1})
        acknowledgements = acknowledged_pushes(log_path)
        require(
            await dispatch_once(database, publisher, settings), "Failure job was not dispatched"
        )
        job = await wait_for_outcome(database, failed_id, JobStatus.FAILED)
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
        created_resources: list[str] = []
        try:
            isolated_url = url.update_query_dict({"options": f"-csearch_path={schema}"})
            settings = Settings(
                database_url=isolated_url.render_as_string(hide_password=False),
                dispatch_publish_timeout_seconds=1,
                dispatch_retry_base_seconds=0.05,
                dispatch_retry_max_seconds=1,
                _env_file=None,
            )
            environment = os.environ.copy()
            environment.update(
                DATABASE_URL=settings.database_url.get_secret_value(),
                PUBSUB_TOPIC=topic,
                PUBSUB_SUBSCRIPTION=subscription,
                PUBSUB_EMULATOR_HOST=emulator_host,
                DISPATCH_PUBLISH_TIMEOUT_SECONDS="1",
            )
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                cwd=PROJECT_ROOT,
                env=environment,
                check=True,
            )
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            worker_url = f"http://127.0.0.1:{port}"
            with tempfile.TemporaryDirectory(prefix="scheduler-pubsub-smoke-") as temp_dir:
                log_path = Path(temp_dir) / "push.log"
                with log_path.open("wb") as output:
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
                        deadline = time.monotonic() + 20
                        while True:
                            require(child.poll() is None, "Live push worker exited during startup")
                            try:
                                ready, _ = request(f"{worker_url}/ready")
                                if ready == 200:
                                    break
                            except (OSError, URLError):
                                pass
                            require(time.monotonic() < deadline, "Live push worker was not ready")
                            time.sleep(0.05)
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
                            runner.run(exercise(settings, cloud, worker_url, log_path, environment))
                    finally:
                        stop_child(child)
                        child = None
        finally:
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
