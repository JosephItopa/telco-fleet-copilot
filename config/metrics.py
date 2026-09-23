"""Dependency-free self-monitoring for the platform services.

Exposes a small in-process metrics registry rendered as JSON at ``/metrics``.
This is the platform's own observability only; application telemetry comes from
the Kubernetes collectors.
"""

from __future__ import annotations

import json
import threading
from typing import Any

from starlette.responses import Response

_registry: list["Metric"] = []
_registry_lock = threading.Lock()


class Metric:
    kind = "metric"

    def __init__(self, name: str, help_text: str = "", label_names: tuple[str, ...] = ()) -> None:
        self.name = name
        self.help = help_text
        self.label_names = tuple(label_names)
        self._lock = threading.Lock()
        self.samples: dict[tuple[str, ...], float] = {}
        self.observations: dict[tuple[str, ...], list[float]] = {}
        with _registry_lock:
            _registry.append(self)

    def labels(self, *values: str) -> "BoundMetric":
        if len(values) != len(self.label_names):
            raise ValueError(
                f"{self.name} expects {len(self.label_names)} label(s) {self.label_names}, got {len(values)}"
            )
        return BoundMetric(self, tuple(str(value) for value in values))

    def _sample_payload(self, key: tuple[str, ...]) -> dict[str, Any]:
        payload: dict[str, Any] = {"labels": dict(zip(self.label_names, key))}
        if self.kind == "histogram":
            values = self.observations.get(key, [])
            count = len(values)
            total = sum(values)
            payload.update(
                {
                    "count": count,
                    "sum": round(total, 6),
                    "avg": round(total / count, 6) if count else 0.0,
                    "min": round(min(values), 6) if values else None,
                    "max": round(max(values), 6) if values else None,
                }
            )
        else:
            payload["value"] = self.samples.get(key, 0.0)
        return payload

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            keys = sorted(set(self.samples) | set(self.observations))
            samples = [self._sample_payload(key) for key in keys]
        return {"type": self.kind, "help": self.help, "label_names": list(self.label_names), "samples": samples}


class BoundMetric:
    def __init__(self, metric: Metric, key: tuple[str, ...]) -> None:
        self.metric = metric
        self.key = key

    def inc(self, amount: float = 1) -> None:
        with self.metric._lock:
            self.metric.samples[self.key] = self.metric.samples.get(self.key, 0.0) + amount

    def set(self, value: float) -> None:
        with self.metric._lock:
            self.metric.samples[self.key] = float(value)

    def observe(self, value: float) -> None:
        with self.metric._lock:
            self.metric.observations.setdefault(self.key, []).append(float(value))


class Counter(Metric):
    kind = "counter"


class Gauge(Metric):
    kind = "gauge"


class Histogram(Metric):
    kind = "histogram"


# --- platform metrics ------------------------------------------------------
SERVICE_INFO = Gauge("aiops_service_info", "Service metadata", ("service", "version"))
UP = Gauge("aiops_up", "Service up flag", ("service",))

EVENTS_PRODUCED = Counter("aiops_events_produced_total", "Telemetry events produced", ("service", "topic"))
EVENTS_CONSUMED = Counter("aiops_events_consumed_total", "Telemetry events consumed", ("service", "topic"))
EVENTS_FAILED = Counter("aiops_events_failed_total", "Events routed to retry or DLQ", ("service", "reason"))
INGEST_LATENCY = Histogram("aiops_ingest_latency_seconds", "Batch processing latency", ("service",))
DB_WRITES = Counter("aiops_db_writes_total", "Database writes", ("service", "operation"))
INCIDENTS = Counter("aiops_incidents_total", "Incident lifecycle transitions", ("service", "transition"))
AI_REQUESTS = Counter("aiops_ai_requests_total", "AI inference requests", ("service", "model", "outcome"))
AI_LATENCY = Histogram("aiops_ai_latency_seconds", "AI inference latency", ("service", "model"))


def render() -> dict[str, Any]:
    with _registry_lock:
        metrics = list(_registry)
    return {metric.name: metric.snapshot() for metric in metrics}


def metrics_response() -> Response:
    return Response(json.dumps(render(), indent=2), media_type="application/json")


def mark_up(service: str, version: str = "1.0.0") -> None:
    UP.labels(service).set(1)
    SERVICE_INFO.labels(service, version).set(1)
