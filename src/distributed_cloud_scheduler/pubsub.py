"""Google Pub/Sub publisher adapter with bounded, explicitly awaited RPCs."""

import math
from uuid import UUID

import grpc  # type: ignore[import-untyped]
from google.pubsub_v1 import PublishRequest, PubsubMessage
from google.pubsub_v1.services.publisher import PublisherAsyncClient
from google.pubsub_v1.services.publisher.transports import PublisherGrpcAsyncIOTransport

from distributed_cloud_scheduler.pubsub_config import PubSubSettings


class PubSubPublisher:
    """Publish only job IDs; PostgreSQL and the dispatcher own durable retry."""

    def __init__(self, cloud: PubSubSettings, *, publish_timeout_seconds: float) -> None:
        if cloud.pubsub_topic is None:
            raise ValueError("PUBSUB_TOPIC is required for the publisher")
        if not math.isfinite(publish_timeout_seconds) or not 0 < publish_timeout_seconds <= 30:
            raise ValueError("publish_timeout_seconds must be finite and between 0 and 30")
        self._topic = cloud.pubsub_topic
        self._timeout = publish_timeout_seconds
        self._closed = False
        self._close_complete = False
        if cloud.pubsub_emulator_host is None:
            # Supplying the transport prevents the SDK from rereading emulator env.
            transport = PublisherGrpcAsyncIOTransport()
        else:
            transport = PublisherGrpcAsyncIOTransport(
                channel=grpc.aio.insecure_channel(cloud.pubsub_emulator_host)
            )
        self._client = PublisherAsyncClient(transport=transport)

    async def publish(self, job_id: UUID) -> None:
        """Return after broker acceptance; cancellation can still mean ambiguous acceptance."""
        if self._closed:
            raise RuntimeError("Cannot publish after the publisher is closed")
        response = await self._client.publish(
            request=PublishRequest(
                topic=self._topic,
                messages=[PubsubMessage(data=str(job_id).encode("ascii"))],
            ),
            retry=None,
            timeout=self._timeout,
        )
        if len(response.message_ids) != 1 or not response.message_ids[0]:
            raise RuntimeError("Pub/Sub did not confirm acceptance of the job identifier")

    async def close(self) -> None:
        """Close this adapter's channel after its runtime has stopped publication."""
        if not self._close_complete:
            self._closed = True
            # The SDK's async transport exposes an unannotated close coroutine.
            await self._client.transport.close()  # type: ignore[no-untyped-call]
            self._close_complete = True
