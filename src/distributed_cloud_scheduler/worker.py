"""Bounded local workers with fenced completion and coordinated shutdown."""

import argparse
import asyncio
import logging
import signal
from types import FrameType

from pydantic import JsonValue
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.execution import (
    Claim,
    claim_pending,
    complete_claim,
    recover_expired,
)
from distributed_cloud_scheduler.tasks import TaskError, execute_task

logger = logging.getLogger(__name__)


async def execute_claim(database: Database, claim: Claim, settings: Settings) -> bool:
    """Execute outside a DB transaction, then commit only for this live owner."""
    result: dict[str, JsonValue] | None = None
    error: dict[str, JsonValue] | None = None
    try:
        execution_timeout = asyncio.timeout(settings.worker_task_timeout_seconds)
        try:
            async with execution_timeout:
                result = await execute_task(claim.type, claim.payload, claim.attempt_count)
        except TaskError as failure:
            error = {"code": failure.code, "retryable": failure.retryable}
        except TimeoutError:
            error = {
                "code": "task_timeout" if execution_timeout.expired() else "internal_error",
                "retryable": True,
            }
        except Exception:
            # An unexpected handler error must not expose its text or submitted data.
            error = {"code": "internal_error", "retryable": True}
        return await complete_claim(database, claim, result=result, error=error)
    except asyncio.CancelledError:
        try:
            async with asyncio.timeout(settings.database_connect_timeout_seconds + 1):
                await complete_claim(
                    database, claim, error={"code": "cancelled", "retryable": True}
                )
        except (SQLAlchemyError, OSError, TimeoutError, asyncio.CancelledError):
            # If cleanup cannot reach PostgreSQL, lease recovery handles the lost owner.
            pass
        raise


async def run_once(
    database: Database, settings: Settings, *, stop_event: asyncio.Event | None = None
) -> bool:
    """Inspect expired ownership and process at most one pending job."""
    if stop_event is not None and stop_event.is_set():
        return False
    await recover_expired(database)
    if stop_event is not None and stop_event.is_set():
        return False
    claim = await claim_pending(database, lease_seconds=settings.worker_lease_seconds)
    if claim is None:
        return False
    await execute_claim(database, claim, settings)
    return True


async def _worker_loop(
    database: Database, settings: Settings, stop_event: asyncio.Event, *, once: bool
) -> None:
    if once:
        await run_once(database, settings, stop_event=stop_event)
        return
    while not stop_event.is_set():
        try:
            worked = await run_once(database, settings, stop_event=stop_event)
        except (OperationalError, SQLAlchemyTimeoutError):
            logger.warning("Database connection unavailable; retrying after polling interval.")
            worked = False
        if not worked:
            try:
                async with asyncio.timeout(settings.worker_poll_interval_seconds):
                    await stop_event.wait()
            except TimeoutError:
                pass


async def run_worker(
    settings: Settings, *, once: bool = False, stop_event: asyncio.Event | None = None
) -> None:
    """Share an engine across bounded loops; finish all children before disposing it."""
    database = Database(settings)
    stop = stop_event if stop_event is not None else asyncio.Event()
    loops = [
        asyncio.create_task(_worker_loop(database, settings, stop, once=once))
        for _ in range(1 if once else settings.worker_concurrency)
    ]
    stopped = asyncio.create_task(stop.wait())
    try:
        completed, _ = await asyncio.wait([*loops, stopped], return_when=asyncio.FIRST_COMPLETED)
        # A failed loop is observed immediately rather than hidden behind healthy loops.
        for task in completed:
            if task is not stopped:
                task.result()
        stop.set()
        _, pending = await asyncio.wait(loops, timeout=settings.worker_shutdown_timeout_seconds)
        for task in pending:
            task.cancel()
        results = await asyncio.gather(*loops, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result
    finally:
        stop.set()
        for task in loops:
            if not task.done():
                task.cancel()
        stopped.cancel()
        await asyncio.gather(*loops, stopped, return_exceptions=True)
        await database.dispose()


async def _run_cli(settings: Settings, *, once: bool) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    previous_handlers = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }
    loop_handlers: set[signal.Signals] = set()

    def request_stop() -> None:
        if stop.is_set() and main_task is not None:
            main_task.cancel()
        else:
            stop.set()

    def portable_signal(signum: int, frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(request_stop)

    try:
        for signum in previous_handlers:
            try:
                loop.add_signal_handler(signum, request_stop)
                loop_handlers.add(signum)
            except NotImplementedError:
                signal.signal(signum, portable_signal)
        await run_worker(settings, once=once, stop_event=stop)
    finally:
        for signum, previous in previous_handlers.items():
            if signum in loop_handlers:
                loop.remove_signal_handler(signum)
            signal.signal(signum, previous if previous is not None else signal.SIG_DFL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute locally persisted jobs.")
    parser.add_argument("--once", action="store_true", help="Process at most one job and exit.")
    arguments = parser.parse_args()
    with asyncio.Runner(loop_factory=create_event_loop) as runner:
        try:
            runner.run(_run_cli(Settings(), once=arguments.once))
        except (KeyboardInterrupt, asyncio.CancelledError):
            return


if __name__ == "__main__":
    main()
