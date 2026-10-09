"""Redacted lifecycle events and bounded process-local Prometheus metrics."""

import json
import logging
import math
import re
import sys
import threading
from collections import defaultdict
from time import perf_counter
from typing import Literal, TextIO
from uuid import UUID

from fastapi import FastAPI, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

Service = Literal["api", "worker", "push_worker", "dispatcher", "scheduler"]

_SERVICE_NAMES = frozenset(("api", "worker", "push_worker", "dispatcher", "scheduler"))
_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
_ATTEMPT_BUCKETS = (*_HTTP_BUCKETS, 15, 30)
_ROUTES = frozenset(
    (
        "/health",
        "/ready",
        "/metrics",
        "/jobs",
        "/jobs/{job_id}",
        "/jobs/{job_id}/attempts",
        "/jobs/{job_id}/retry",
        "/pubsub/push",
        "unmatched",
    )
)
_METRIC_LABELS: dict[str, tuple[str, ...]] = {
    "jobs_submitted_total": ("outcome",),
    "dispatches_total": ("outcome",),
    "job_attempts_total": ("service", "outcome"),
    "job_retries_total": ("service",),
    "job_dead_lettered_total": ("service",),
    "http_requests_total": ("service", "route", "status"),
    "active_executions": ("service",),
    "http_request_duration_seconds": ("service", "route", "status"),
    "job_attempt_duration_seconds": ("service",),
}
_COUNTER_HELP: dict[str, str] = {
    "jobs_submitted_total": "Job submissions, separated into newly created and replayed.",
    "dispatches_total": "Publisher attempts finalized as accepted or failed.",
    "job_attempts_total": "Execution outcomes committed by the current process.",
    "job_retries_total": "Committed transitions to RETRY_WAIT.",
    "job_dead_lettered_total": "Committed transitions to DEAD_LETTERED.",
    "http_requests_total": "HTTP requests handled by this process.",
}
_GAUGE_HELP = {
    "active_executions": "Execution claims currently being processed in this process."
}
_HISTOGRAM_HELP = {
    "http_request_duration_seconds": "HTTP request duration, including response handling.",
    "job_attempt_duration_seconds": "Handler wall time, excluding claim and outcome persistence.",
}
_SERVICE_LABELS = frozenset(("worker", "push_worker"))
_OUTCOME_LABELS: dict[str, frozenset[str]] = {
    "jobs_submitted_total": frozenset(("created", "replayed")),
    "dispatches_total": frozenset(("accepted", "failed")),
    "job_attempts_total": frozenset(("succeeded", "retry_wait", "failed", "dead_lettered")),
}


class HTTPMetricsMiddleware:
    """Observe ASGI responses without introducing cancellation/task-group boundaries."""

    def __init__(self, app: ASGIApp, service: Literal["api", "push_worker"]) -> None:
        self.app = app
        self.service = service

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = perf_counter()
        status = 500

        async def record_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, record_status)
        finally:
            labels = {
                "service": self.service,
                "route": _route_template(scope["path"]),
                "status": str(status),
            }
            METRICS.increment("http_requests_total", **labels)
            METRICS.observe("http_request_duration_seconds", perf_counter() - started, **labels)


class MetricsRegistry:
    """Thread-safe counters, gauges and histograms with an explicit label schema."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[str, ...]], int] = defaultdict(int)
        self._gauges: dict[tuple[str, tuple[str, ...]], int] = {
            ("active_executions", (f'service="{service}"',)): 0
            for service in _SERVICE_LABELS
        }
        self._histograms: dict[
            tuple[str, tuple[str, ...]], tuple[int, float, list[int]]
        ] = {}

    @staticmethod
    def _labels(name: str, labels: dict[str, str]) -> tuple[str, ...]:
        expected = _METRIC_LABELS.get(name)
        if expected is None or set(labels) != set(expected):
            raise ValueError(f"Invalid labels for metric {name}")
        values: list[str] = []
        for label in expected:
            value = labels[label]
            if label == "service" and value not in _SERVICE_NAMES:
                raise ValueError("Invalid service metric label")
            if label == "outcome" and value not in _OUTCOME_LABELS.get(name, ()):
                raise ValueError("Invalid outcome metric label")
            if label == "route" and value not in _ROUTES:
                raise ValueError("Invalid route metric label")
            if label == "status" and (
                len(value) != 3 or not value.isdecimal() or not 100 <= int(value) <= 599
            ):
                raise ValueError("Invalid status metric label")
            values.append(f'{label}="{_escape_label(value)}"')
        return tuple(values)

    def increment(self, name: str, **labels: str) -> None:
        if name not in _COUNTER_HELP:
            raise ValueError(f"Unknown counter: {name}")
        key = (name, self._labels(name, labels))
        with self._lock:
            self._counters[key] += 1

    def adjust_active(self, service: Service, amount: int) -> None:
        if service not in _SERVICE_LABELS or amount not in (-1, 1):
            raise ValueError("Invalid active execution adjustment")
        key = ("active_executions", (f'service="{service}"',))
        with self._lock:
            value = self._gauges.get(key, 0) + amount
            if value < 0:
                raise RuntimeError("Active execution gauge cannot become negative")
            self._gauges[key] = value

    def observe(self, name: str, value: float, **labels: str) -> None:
        if name not in _HISTOGRAM_HELP or not math.isfinite(value) or value < 0:
            raise ValueError(f"Invalid observation for histogram {name}")
        key = (name, self._labels(name, labels))
        buckets = _HTTP_BUCKETS if name == "http_request_duration_seconds" else _ATTEMPT_BUCKETS
        with self._lock:
            count, total, bucket_counts = self._histograms.get(
                key, (0, 0.0, [0] * len(buckets))
            )
            self._histograms[key] = (
                count + 1,
                total + value,
                [current + (value <= boundary) for current, boundary in zip(
                    bucket_counts, buckets, strict=True
                )],
            )

    def render(self) -> str:
        """Render one internally consistent snapshot in Prometheus text format."""
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = dict(self._histograms)
        lines: list[str] = []
        for name, help_text in _COUNTER_HELP.items():
            lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} counter"))
            for (metric, labels), value in sorted(counters.items()):
                if metric == name:
                    lines.append(f"{name}{_format_labels(labels)} {value}")
        for name, help_text in _GAUGE_HELP.items():
            lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} gauge"))
            for (metric, labels), value in sorted(gauges.items()):
                if metric == name:
                    lines.append(f"{name}{_format_labels(labels)} {value}")
        for name, help_text in _HISTOGRAM_HELP.items():
            lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} histogram"))
            boundaries = (
                _HTTP_BUCKETS
                if name == "http_request_duration_seconds"
                else _ATTEMPT_BUCKETS
            )
            for (metric, labels), (count, total, bucket_counts) in sorted(histograms.items()):
                if metric != name:
                    continue
                for bucket_count, boundary in zip(bucket_counts, boundaries, strict=True):
                    bucket_labels = (*labels, 'le="' + format(boundary, "g") + '"')
                    lines.append(
                        f"{name}_bucket{_format_labels(bucket_labels)} {bucket_count}"
                    )
                infinity_labels = (*labels, 'le="+Inf"')
                lines.append(
                    f"{name}_bucket{_format_labels(infinity_labels)} {count}"
                )
                lines.append(f"{name}_sum{_format_labels(labels)} {total:.9g}")
                lines.append(f"{name}_count{_format_labels(labels)} {count}")
        return "\n".join(lines) + "\n"


METRICS = MetricsRegistry()
_event_logger = logging.getLogger("distributed_cloud_scheduler.lifecycle")


class _CurrentStdoutHandler(logging.StreamHandler[TextIO]):
    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stdout
        super().emit(record)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_labels(labels: tuple[str, ...]) -> str:
    return "{" + ",".join(labels) + "}" if labels else ""


def log_event(
    event: str,
    service: Service,
    *,
    job_id: str | None = None,
    task_type: str | None = None,
    attempt: int | None = None,
    status: str | None = None,
    duration_seconds: float | None = None,
    error_code: str | None = None,
) -> None:
    """Write a compact JSON event containing only approved, sanitized fields."""
    if not event.isascii() or not _ERROR_CODE.fullmatch(event):
        raise ValueError("Invalid lifecycle event")
    if service not in _SERVICE_NAMES:
        raise ValueError("Invalid lifecycle service")
    fields: dict[str, str | int | float] = {"event": event, "service": service}
    if job_id is not None:
        fields["job_id"] = job_id
    if task_type is not None:
        fields["task_type"] = task_type
    if attempt is not None:
        fields["attempt"] = attempt
    if status is not None:
        fields["status"] = status
    if duration_seconds is not None:
        if not math.isfinite(duration_seconds) or duration_seconds < 0:
            raise ValueError("Invalid lifecycle duration")
        fields["duration_seconds"] = round(duration_seconds, 9)
    if error_code is not None:
        if not _ERROR_CODE.fullmatch(error_code):
            raise ValueError("Invalid lifecycle error code")
        fields["error_code"] = error_code
    if not _event_logger.handlers:
        handler = _CurrentStdoutHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        _event_logger.addHandler(handler)
    _event_logger.setLevel(logging.INFO)
    _event_logger.propagate = False
    message = json.dumps(fields, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
    _event_logger.info(message)


def install_http_metrics(app: FastAPI, service: Literal["api", "push_worker"]) -> None:
    """Measure bounded route-template labels without recording request data."""
    app.add_middleware(HTTPMetricsMiddleware, service=service)


def _route_template(path: str) -> str:
    """Map request paths to finite templates without retaining caller identifiers."""
    if path in ("/health", "/ready", "/metrics", "/jobs", "/pubsub/push"):
        return path
    parts = path.strip("/").split("/")
    if len(parts) in (2, 3) and parts[0] == "jobs":
        try:
            UUID(parts[1])
        except ValueError:
            return "unmatched"
        if len(parts) == 2:
            return "/jobs/{job_id}"
        if parts[2] in ("attempts", "retry"):
            return f"/jobs/{{job_id}}/{parts[2]}"
    return "unmatched"


def metrics_response() -> Response:
    """Return the current process's Prometheus text snapshot."""
    return Response(
        content=METRICS.render(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
