"""Publish leased outbox obligations without holding a database transaction."""

import asyncio

from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.database import Database
from distributed_cloud_scheduler.messaging import Publisher
from distributed_cloud_scheduler.outbox import claim_dispatch, complete_dispatch, fail_dispatch


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
