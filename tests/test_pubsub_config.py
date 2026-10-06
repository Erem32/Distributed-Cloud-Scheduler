"""Transport configuration is optional locally and explicit when constructed."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from distributed_cloud_scheduler.pubsub_config import PubSubSettings


@pytest.fixture(autouse=True)
def clear_transport_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PUBSUB_TOPIC",
        "PUBSUB_SUBSCRIPTION",
        "PUBSUB_EMULATOR_HOST",
        "PUSH_REQUEST_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_local_startup_does_not_require_transport_configuration() -> None:
    settings = PubSubSettings(_env_file=None)
    assert settings.pubsub_topic is None
    assert settings.pubsub_subscription is None
    assert settings.pubsub_emulator_host is None
    assert settings.push_request_timeout_seconds == 45


def test_dotenv_and_environment_supply_independent_transport_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "PUBSUB_TOPIC=projects/local-example/topics/jobs\n"
        "PUBSUB_SUBSCRIPTION=projects/local-example/subscriptions/jobs-push\n"
        "PUBSUB_EMULATOR_HOST=localhost:8085\n"
        "PUSH_REQUEST_TIMEOUT_SECONDS=40\n"
        "UNRELATED_DATABASE_SETTING=ignored\n",
        encoding="utf-8",
    )
    from_file = PubSubSettings(_env_file=dotenv)
    assert from_file.pubsub_topic == "projects/local-example/topics/jobs"
    assert from_file.pubsub_subscription == "projects/local-example/subscriptions/jobs-push"
    assert from_file.pubsub_emulator_host == "localhost:8085"
    assert from_file.push_request_timeout_seconds == 40
    monkeypatch.setenv("PUBSUB_TOPIC", "projects/other-project/topics/new-jobs")
    monkeypatch.setenv("PUBSUB_EMULATOR_HOST", "127.0.0.1:9000")
    monkeypatch.setenv("PUSH_REQUEST_TIMEOUT_SECONDS", "30.5")
    from_environment = PubSubSettings(_env_file=dotenv)
    assert from_environment.pubsub_topic == "projects/other-project/topics/new-jobs"
    assert from_environment.pubsub_emulator_host == "127.0.0.1:9000"
    assert from_environment.push_request_timeout_seconds == 30.5


@pytest.mark.parametrize("field", ["pubsub_topic", "pubsub_subscription"])
@pytest.mark.parametrize(
    "resource",
    [
        "",
        "jobs",
        "projects/test-project/topics/jobs/extra",
        "projects//topics/jobs",
        "projects/secret project/topics/jobs",
        "projects/secret@host/topics/jobs",
        "projects/test-project/topics/x",
        "projects/test-project/topics/1jobs",
        "projects/test-project/topics/goog-reserved",
        "projects/test-project/topics/" + "a" * 256,
    ],
)
def test_invalid_resources_are_rejected_without_echoing_input(field: str, resource: str) -> None:
    if field == "pubsub_subscription":
        resource = resource.replace("/topics/", "/subscriptions/")
    with pytest.raises(ValidationError) as failure:
        PubSubSettings(_env_file=None, **{field: resource})
    if resource:
        assert resource not in str(failure.value)


def test_topic_and_subscription_resources_cannot_be_interchanged() -> None:
    with pytest.raises(ValidationError):
        PubSubSettings(pubsub_topic="projects/local-example/subscriptions/jobs", _env_file=None)
    with pytest.raises(ValidationError):
        PubSubSettings(pubsub_subscription="projects/local-example/topics/jobs", _env_file=None)


@pytest.mark.parametrize(
    "host", ["localhost:8085", "127.0.0.1:8085", "pubsub:8085", "[::1]:8085"]
)
def test_emulator_accepts_explicit_host_and_port(host: str) -> None:
    assert PubSubSettings(pubsub_emulator_host=host, _env_file=None).pubsub_emulator_host == host


@pytest.mark.parametrize(
    "host",
    [
        "",
        "localhost",
        "localhost:0",
        "localhost:65536",
        "localhost:invalid",
        "localhost:8085:9000",
        "http://localhost:8085",
        "localhost:8085/path",
        "localhost:8085?query=value",
        "localhost:8085?",
        "localhost:8085#fragment",
        "localhost:8085#",
        "user:secret@localhost:8085",
        "local host:8085",
        "::1:8085",
        "[bad-ipv6]:8085",
    ],
)
def test_invalid_emulator_targets_are_rejected_without_echoing_input(host: str) -> None:
    with pytest.raises(ValidationError) as failure:
        PubSubSettings(pubsub_emulator_host=host, _env_file=None)
    if host:
        assert host not in str(failure.value)


@pytest.mark.parametrize("timeout", [0, -1, 61, float("inf"), float("nan")])
def test_push_request_budget_is_finite_positive_and_bounded(timeout: float) -> None:
    with pytest.raises(ValidationError):
        PubSubSettings(push_request_timeout_seconds=timeout, _env_file=None)
