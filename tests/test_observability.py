"""Redacted JSON lifecycle events and bounded Prometheus metrics."""

import json
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.engine import URL

from distributed_cloud_scheduler.api import create_app
from distributed_cloud_scheduler.config import Settings
from distributed_cloud_scheduler.execution import Claim
from distributed_cloud_scheduler.observability import METRICS, MetricsRegistry, log_event
from distributed_cloud_scheduler.pubsub_config import PubSubSettings
from distributed_cloud_scheduler.push import create_push_app
from distributed_cloud_scheduler.worker import _record_attempt


def sample_value(rendered: str, sample_name: str) -> int:
    for line in rendered.splitlines():
        if line.startswith(sample_name + " "):
            return int(line.rsplit(" ", maxsplit=1)[1])
    return 0


def make_settings() -> Settings:
    return Settings(
        database_url=str(
            URL.create(
                "postgresql+psycopg",
                username="scheduler",
                password="not-a-real-secret",
                host="127.0.0.1",
                port=1,
                database="scheduler_test",
            )
        ),
        _env_file=None,
    )


def test_lifecycle_event_is_json_and_uses_only_approved_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from distributed_cloud_scheduler import observability

    messages: list[str] = []
    monkeypatch.setattr(observability._event_logger, "info", messages.append)
    log_event(
        "job_attempt_completed",
        "worker",
        job_id="12345678-1234-5678-9abc-123456789abc",
        task_type="controlled_failure",
        attempt=2,
        status="RETRY_WAIT",
        duration_seconds=0.25,
        error_code="controlled_failure",
    )
    event = json.loads(messages[0])
    assert event == {
        "event": "job_attempt_completed",
        "service": "worker",
        "job_id": "12345678-1234-5678-9abc-123456789abc",
        "task_type": "controlled_failure",
        "attempt": 2,
        "status": "RETRY_WAIT",
        "duration_seconds": 0.25,
        "error_code": "controlled_failure",
    }
    assert "payload" not in event
    assert "exception" not in event


def test_metrics_registry_reconciles_counters_gauges_and_cumulative_buckets() -> None:
    metrics = MetricsRegistry()
    metrics.increment("jobs_submitted_total", outcome="created")
    metrics.increment("dispatches_total", outcome="accepted")
    metrics.adjust_active("worker", 1)
    metrics.observe("job_attempt_duration_seconds", 0.2, service="worker")
    metrics.adjust_active("worker", -1)

    rendered = metrics.render()
    assert 'jobs_submitted_total{outcome="created"} 1' in rendered
    assert 'dispatches_total{outcome="accepted"} 1' in rendered
    assert 'active_executions{service="worker"} 0' in rendered
    assert 'job_attempt_duration_seconds_bucket{service="worker",le="0.25"} 1' in rendered
    assert 'job_attempt_duration_seconds_bucket{service="worker",le="+Inf"} 1' in rendered
    assert 'job_attempt_duration_seconds_count{service="worker"} 1' in rendered


def test_metric_labels_reject_unbounded_values_and_invalid_gauges() -> None:
    metrics = MetricsRegistry()
    with pytest.raises(ValueError, match="Invalid labels"):
        metrics.increment("job_attempts_total", service="worker", outcome="succeeded", task="x")
    with pytest.raises(ValueError, match="Invalid route"):
        metrics.increment(
            "http_requests_total",
            service="api",
            route="/jobs/12345678-1234-5678-9abc-123456789abc",
            status="200",
        )
    with pytest.raises(ValueError, match="Invalid active execution"):
        metrics.adjust_active("api", 1)
    with pytest.raises(RuntimeError, match="cannot become negative"):
        metrics.adjust_active("worker", -1)


def test_retry_and_dead_letter_metrics_follow_committed_attempt_outcomes() -> None:
    before = METRICS.render()
    retry_claim = Claim(
        uuid4(), "controlled_failure", {}, 1, uuid4(), max_attempts=3
    )
    exhausted_claim = Claim(
        uuid4(), "controlled_failure", {}, 3, uuid4(), max_attempts=3
    )

    _record_attempt(
        retry_claim,
        "worker",
        None,
        {"code": "controlled_failure", "retryable": True},
        0.01,
    )
    _record_attempt(
        exhausted_claim,
        "worker",
        None,
        {"code": "controlled_failure", "retryable": True},
        0.02,
    )

    after = METRICS.render()
    for metric in (
        'job_attempts_total{service="worker",outcome="retry_wait"}',
        'job_attempts_total{service="worker",outcome="dead_lettered"}',
        'job_retries_total{service="worker"}',
        'job_dead_lettered_total{service="worker"}',
    ):
        assert sample_value(after, metric) - sample_value(before, metric) == 1
    histogram_count = 'job_attempt_duration_seconds_count{service="worker"}'
    assert sample_value(after, histogram_count) - sample_value(before, histogram_count) == 2


@pytest.mark.asyncio
async def test_api_metrics_endpoint_exposes_request_count_and_histogram() -> None:
    app = create_app(make_settings())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/health")).status_code == 200
            response = await client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "http_requests_total" in response.text
    assert "http_request_duration_seconds_bucket" in response.text
    assert 'service="api",route="/health",status="200"' in response.text


@pytest.mark.asyncio
async def test_push_metrics_endpoint_exposes_only_process_local_http_metrics() -> None:
    app = create_push_app(
        make_settings(),
        PubSubSettings(
            pubsub_subscription="projects/test-project/subscriptions/test-subscription",
            _env_file=None,
        ),
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/health")).status_code == 200
            response = await client.get("/metrics")
    assert response.status_code == 200
    assert 'service="push_worker",route="/health",status="200"' in response.text
    assert "active_executions{service=\"push_worker\"} 0" in response.text
