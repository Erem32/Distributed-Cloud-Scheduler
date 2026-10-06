"""Publish leased outbox obligations without holding a database transaction."""

import argparse
import asyncio
import logging
import signal
from types import FrameType

from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from distributed_cloud_scheduler.__main__ import create_event_loop
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.messaging import Publisher
from distributed_cloud_scheduler.outbox import claim_dispatch, complete_dispatch, fail_dispatch
from distributed_cloud_scheduler.pubsub import PubSubPublisher
from distributed_cloud_scheduler.pubsub_config import PubSubSettings

logger = logging.getLogger(__name__)


async def dispatch_once(database: Database, publisher: Publisher, settings: Settings) -> bool:
    """Attempt one due obligation; mark acceptance only with the current live token."""
    claim = await claim_dispatch(database, lease_seconds=settings.dispatch_lease_seconds)
    if claim is None:
        return False
    publish_timeout = asyncio.timeout(settings.dispatch_publish_timeout_seconds)
    try:
        async with publish_timeout:
            await publisher.publish(claim.job_id)
    except TimeoutError:
        error_code = "publish_timeout" if publish_timeout.expired() else "publisher_unavailable"
    except Exception:
        # Cancellation is deliberately excluded: its leased obligation stays replayable.
        error_code = "publisher_unavailable"
    else:
        # A crash here replays publication after lease expiry. Duplicates are expected.
        await complete_dispatch(database, claim)
        return True

    delay = min(
        settings.dispatch_retry_base_seconds * 2 ** min(claim.attempt_count - 1, 16),
        settings.dispatch_retry_max_seconds,
    )
    await fail_dispatch(database, claim, delay_seconds=delay, error_code=error_code)
    return True


async def _dispatch_loop(
    database: Database,
    publisher: Publisher,
    settings: Settings,
    stop: asyncio.Event,
    *,
    once: bool,
) -> None:
    while not stop.is_set():
        try:
            worked = await dispatch_once(database, publisher, settings)
        except (OperationalError, SQLAlchemyTimeoutError):
            if once:
                raise
            logger.warning(
                "Database connection unavailable; retrying dispatch after polling interval."
            )
            worked = False
        if once:
            return
        if not worked:
            try:
                async with asyncio.timeout(settings.dispatch_poll_interval_seconds):
                    await stop.wait()
            except TimeoutError:
                pass


async def run_dispatcher(
    settings: Settings,
    pubsub_settings: PubSubSettings,
    *,
    once: bool = False,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Publish serially; stop admission, then drain or cancel the current obligation."""
    database = Database(settings)
    publisher: PubSubPublisher | None = None
    dispatch: asyncio.Task[None] | None = None
    stopped: asyncio.Task[bool] | None = None
    stop = stop_event if stop_event is not None else asyncio.Event()
    try:
        publisher = PubSubPublisher(
            pubsub_settings, publish_timeout_seconds=settings.dispatch_publish_timeout_seconds
        )
        dispatch = asyncio.create_task(
            _dispatch_loop(database, publisher, settings, stop, once=once)
        )
        stopped = asyncio.create_task(stop.wait())
        completed, _ = await asyncio.wait((dispatch, stopped), return_when=asyncio.FIRST_COMPLETED)
        if dispatch in completed:
            dispatch.result()
        stop.set()
        _, pending = await asyncio.wait(
            (dispatch,), timeout=settings.dispatch_shutdown_timeout_seconds
        )
        for task in pending:
            task.cancel()
        results = await asyncio.gather(dispatch, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result
    finally:
        stop.set()
        children = [task for task in (dispatch, stopped) if task is not None]
        for child in children:
            if not child.done():
                child.cancel()
        await asyncio.gather(*children, return_exceptions=True)
        try:
            if publisher is not None:
                await publisher.close()
        finally:
            await database.dispose()


async def _run_cli(settings: Settings, pubsub_settings: PubSubSettings, *, once: bool) -> None:
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
        await run_dispatcher(settings, pubsub_settings, once=once, stop_event=stop)
    finally:
        for signum, previous in previous_handlers.items():
            if signum in loop_handlers:
                loop.remove_signal_handler(signum)
            signal.signal(signum, previous if previous is not None else signal.SIG_DFL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish durable job obligations to Pub/Sub.")
    parser.add_argument(
        "--once", action="store_true", help="Attempt at most one obligation and exit."
    )
    arguments = parser.parse_args()
    with asyncio.Runner(loop_factory=create_event_loop) as runner:
        try:
            runner.run(_run_cli(Settings(), PubSubSettings(), once=arguments.once))
        except (KeyboardInterrupt, asyncio.CancelledError):
            return


if __name__ == "__main__":
    main()
