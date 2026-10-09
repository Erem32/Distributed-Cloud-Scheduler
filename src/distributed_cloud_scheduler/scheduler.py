"""Bounded transactional admission of due jobs, independent of transport/execution."""

import argparse
import asyncio
import logging
import signal
from datetime import datetime
from types import FrameType
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.jobs import Job, JobStatus
from distributed_cloud_scheduler.outbox import DispatchOutbox
from distributed_cloud_scheduler.priority import priority_rank

logger = logging.getLogger(__name__)


async def schedule_due(database: Database, limit: int = 100) -> int:
    """Atomically admit one priority/age-ranked due batch under PostgreSQL locks."""
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer between 1 and 1000")
    async with database.session_factory() as session, session.begin():
        cutoff = cast(datetime, await session.scalar(select(func.clock_timestamp())))
        jobs = list(await session.scalars(
            select(Job)
            .where(Job.status == JobStatus.SCHEDULED, Job.scheduled_at <= cutoff)
            .order_by(priority_rank(Job.ready_at, Job.priority), Job.ready_at, Job.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ))
        for job in jobs:
            job.status = JobStatus.PENDING
            session.add(
                DispatchOutbox(job_id=job.id, generation=job.generation, available_at=cutoff)
            )
        await session.flush()
    return len(jobs)


async def _scheduler_loop(
    database: Database, settings: Settings, stop: asyncio.Event, *, once: bool
) -> None:
    if once:
        if not stop.is_set():
            await schedule_due(database, limit=settings.scheduler_batch_size)
        return
    while not stop.is_set():
        try:
            admitted = await schedule_due(database, limit=settings.scheduler_batch_size)
        except (OperationalError, SQLAlchemyTimeoutError):
            logger.warning("Database connection unavailable; retrying after polling interval.")
            admitted = 0
        if not admitted:
            try:
                async with asyncio.timeout(settings.scheduler_poll_interval_seconds):
                    await stop.wait()
            except TimeoutError:
                pass


async def run_scheduler(
    settings: Settings, *, once: bool = False, stop_event: asyncio.Event | None = None
) -> None:
    """Own scheduler resources, drain the current transaction and cancel after grace."""
    database = Database(settings)
    stop = stop_event if stop_event is not None else asyncio.Event()
    admission = asyncio.create_task(_scheduler_loop(database, settings, stop, once=once))
    stopped = asyncio.create_task(stop.wait())
    try:
        completed, _ = await asyncio.wait((admission, stopped), return_when=asyncio.FIRST_COMPLETED)
        if admission in completed:
            admission.result()
        stop.set()
        _, pending = await asyncio.wait(
            (admission,), timeout=settings.scheduler_shutdown_timeout_seconds
        )
        for task in pending:
            task.cancel()
        results = await asyncio.gather(admission, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result
    finally:
        stop.set()
        for child in (admission, stopped):
            if not child.done():
                child.cancel()
        await asyncio.gather(admission, stopped, return_exceptions=True)
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
        await run_scheduler(settings, once=once, stop_event=stop)
    finally:
        for signum, previous in previous_handlers.items():
            if signum in loop_handlers:
                loop.remove_signal_handler(signum)
            signal.signal(signum, previous if previous is not None else signal.SIG_DFL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Admit due scheduled jobs for durable dispatch.")
    parser.add_argument(
        "--once", action="store_true", help="Admit at most one bounded batch and exit."
    )
    arguments = parser.parse_args()
    with asyncio.Runner(loop_factory=create_event_loop) as runner:
        try:
            runner.run(_run_cli(Settings(), once=arguments.once))
        except (KeyboardInterrupt, asyncio.CancelledError):
            return


if __name__ == "__main__":
    main()
