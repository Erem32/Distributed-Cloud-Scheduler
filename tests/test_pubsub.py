"""The Pub/Sub adapter waits for broker acceptance and owns its channel lifecycle."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.pubsub_v1 import PublishResponse

from distributed_cloud_scheduler.messaging import Publisher
from distributed_cloud_scheduler.pubsub import PubSubPublisher
from distributed_cloud_scheduler.pubsub_config import PubSubSettings

TOPIC = "projects/local-example/topics/jobs"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    fake = SimpleNamespace(
        publish=AsyncMock(return_value=PublishResponse(message_ids=["accepted-message"])),
        transport=SimpleNamespace(close=AsyncMock()),
    )
    factory = Mock(return_value=fake)
    fake.factory = factory
    monkeypatch.setattr("distributed_cloud_scheduler.pubsub.PublisherAsyncClient", factory)
    monkeypatch.setattr(
        "distributed_cloud_scheduler.pubsub.PublisherGrpcAsyncIOTransport",
        Mock(return_value=fake.transport),
    )
    return fake


def cloud(**overrides: object) -> PubSubSettings:
    return PubSubSettings(
        pubsub_topic=TOPIC, pubsub_emulator_host=None, _env_file=None, **overrides
    )


@pytest.mark.asyncio
async def test_publisher_sends_only_canonical_uuid_and_disables_sdk_retries(
    client: SimpleNamespace,
) -> None:
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    boundary: Publisher = publisher
    job_id = uuid4()
    try:
        await boundary.publish(job_id)
        request = client.publish.await_args.kwargs["request"]
        assert request.topic == TOPIC
        assert len(request.messages) == 1
        assert request.messages[0].data == str(job_id).encode("ascii")
        assert not request.messages[0].attributes
        assert not request.messages[0].ordering_key
        assert client.publish.await_args.kwargs["retry"] is None
        assert client.publish.await_args.kwargs["timeout"] == 10
    finally:
        await publisher.close()


@pytest.mark.asyncio
async def test_successful_return_waits_for_broker_confirmation(client: SimpleNamespace) -> None:
    entered, accepted = asyncio.Event(), asyncio.Event()

    async def publish_rpc(**kwargs: object) -> PublishResponse:
        entered.set()
        await accepted.wait()
        return PublishResponse(message_ids=["accepted-message"])

    client.publish.side_effect = publish_rpc
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    publishing = asyncio.create_task(publisher.publish(uuid4()))
    try:
        await entered.wait()
        assert not publishing.done()
        accepted.set()
        await publishing
    finally:
        publishing.cancel()
        await asyncio.gather(publishing, return_exceptions=True)
        await publisher.close()


@pytest.mark.asyncio
async def test_broker_failure_propagates_to_durable_dispatcher(client: SimpleNamespace) -> None:
    client.publish.side_effect = ServiceUnavailable("broker unavailable")
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    try:
        with pytest.raises(ServiceUnavailable):
            await publisher.publish(uuid4())
        assert client.publish.await_count == 1
    finally:
        await publisher.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("message_ids", [[], [""], ["one", "two"]])
async def test_missing_or_invalid_confirmation_does_not_report_acceptance(
    client: SimpleNamespace, message_ids: list[str]
) -> None:
    client.publish.return_value = PublishResponse(message_ids=message_ids)
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    try:
        with pytest.raises(RuntimeError, match="did not confirm acceptance"):
            await publisher.publish(uuid4())
    finally:
        await publisher.close()


@pytest.mark.asyncio
async def test_cancellation_reaches_awaited_rpc(client: SimpleNamespace) -> None:
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def publish_rpc(**kwargs: object) -> PublishResponse:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return PublishResponse(message_ids=["unreachable"])

    client.publish.side_effect = publish_rpc
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    publishing = asyncio.create_task(publisher.publish(uuid4()))
    try:
        await entered.wait()
        publishing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publishing
        assert cancelled.is_set()
    finally:
        publishing.cancel()
        await asyncio.gather(publishing, return_exceptions=True)
        await publisher.close()


@pytest.mark.asyncio
async def test_close_awaits_transport_once_and_prevents_further_publication(
    client: SimpleNamespace,
) -> None:
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    await publisher.close()
    await publisher.close()
    client.transport.close.assert_awaited_once_with()
    with pytest.raises(RuntimeError, match="closed"):
        await publisher.publish(uuid4())
    client.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_close_can_be_retried_without_reopening_publication(
    client: SimpleNamespace,
) -> None:
    client.transport.close.side_effect = [OSError("close failed"), None]
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    with pytest.raises(OSError, match="close failed"):
        await publisher.close()
    with pytest.raises(RuntimeError, match="closed"):
        await publisher.publish(uuid4())
    await publisher.close()
    assert client.transport.close.await_count == 2


@pytest.mark.asyncio
async def test_explicit_production_settings_do_not_reread_emulator_environment(
    client: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "unexpected-emulator:8085")
    transport_factory = Mock(return_value=client.transport)
    monkeypatch.setattr(
        "distributed_cloud_scheduler.pubsub.PublisherGrpcAsyncIOTransport", transport_factory
    )
    publisher = PubSubPublisher(cloud(), publish_timeout_seconds=10)
    try:
        transport_factory.assert_called_once_with()
        client.factory.assert_called_once_with(transport=client.transport)
    finally:
        await publisher.close()


@pytest.mark.asyncio
async def test_emulator_uses_explicit_insecure_channel_from_settings(
    client: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    channel = object()
    create_channel = Mock(return_value=channel)
    transport = object()
    create_transport = Mock(return_value=transport)
    monkeypatch.setattr(
        "distributed_cloud_scheduler.pubsub.grpc.aio.insecure_channel", create_channel
    )
    monkeypatch.setattr(
        "distributed_cloud_scheduler.pubsub.PublisherGrpcAsyncIOTransport", create_transport
    )
    settings = PubSubSettings(
        pubsub_topic=TOPIC, pubsub_emulator_host="pubsub:8085", _env_file=None
    )
    publisher = PubSubPublisher(settings, publish_timeout_seconds=10)
    try:
        create_channel.assert_called_once_with("pubsub:8085")
        create_transport.assert_called_once_with(channel=channel)
        client.factory.assert_called_once_with(transport=transport)
    finally:
        await publisher.close()


def test_topic_is_required_before_client_creation(client: SimpleNamespace) -> None:
    settings = PubSubSettings(pubsub_topic=None, _env_file=None)
    with pytest.raises(ValueError, match="PUBSUB_TOPIC is required"):
        PubSubPublisher(settings, publish_timeout_seconds=10)
    client.factory.assert_not_called()


@pytest.mark.parametrize("timeout", [0, -1, 31, float("inf"), float("nan")])
def test_invalid_publish_budget_is_rejected_before_client_creation(
    client: SimpleNamespace, timeout: float
) -> None:
    with pytest.raises(ValueError, match="publish_timeout_seconds"):
        PubSubPublisher(cloud(), publish_timeout_seconds=timeout)
    client.factory.assert_not_called()
