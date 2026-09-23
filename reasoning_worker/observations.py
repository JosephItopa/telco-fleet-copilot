"""Rolling telemetry observations and daily/weekly/30-day report aggregation.

Each detector run contributes one observation per app endpoint: the observed
request volume (checks), failures, and average response time. Reports reduce the
observations in a time window to availability, traffic, response-time, and
per-endpoint endpoint tables.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

MAX_OBSERVATIONS = 100_000
RETENTION_DAYS = 35
TABLE_SIZE = 4


@dataclass
class Observation:
    timestamp: str
    endpoint: str
    cluster: str | None
    checks: float
    failures: float
    avg_response_ms: float | None


class ObservationStore:
    def __init__(self) -> None:
        self._items: deque[Observation] = deque(maxlen=MAX_OBSERVATIONS)
        self._lock = threading.Lock()

    def add_many(self, items: list[Observation]) -> int:
        if not items:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
        with self._lock:
            self._items.extend(items)
            while self._items and _parse(self._items[0].timestamp) < cutoff:
                self._items.popleft()
        return len(items)

    def all(self) -> list[Observation]:
        with self._lock:
            return list(self._items)

    def count(self) -> int:
        with self._lock:
            return len(self._items)


def _parse(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def observations_from_snapshot(collected_at: str | None, apps: list[dict[str, Any]]) -> list[Observation]:
    """Convert detector app snapshots into observations."""
    timestamp = collected_at or datetime.now(timezone.utc).isoformat()
    out: list[Observation] = []
    for app in apps or []:
        metrics = app.get("metrics", {}) or {}
        requests = metrics.get("requests_by_status", {}) or {}
        checks = sum(float(value) for value in requests.values())
        failures = sum(float(value) for status, value in requests.items() if str(status).startswith("5"))
        count = float(metrics.get("latency_count") or 0)
        total = float(metrics.get("latency_sum") or 0)
        avg_response_ms = (total / count * 1000.0) if count else None
        out.append(
            Observation(
                timestamp=timestamp,
                endpoint=str(app.get("target")),
                cluster=app.get("cluster"),
                checks=checks,
                failures=failures,
                avg_response_ms=avg_response_ms,
            )
        )
    return out


def _endpoint_labels(latest: list[Observation]) -> set[str]:
    seen: dict[str, int] = {}
    for observation in latest:
        seen[observation.endpoint] = seen.get(observation.endpoint, 0) + 1
    return {endpoint for endpoint, count in seen.items() if count > 1}


def _label(observation: Observation, duplicated: set[str]) -> str:
    if observation.endpoint in duplicated and observation.cluster:
        return f"{observation.endpoint} ({observation.cluster})"
    return observation.endpoint


def _latest_per_endpoint(observations: list[Observation]) -> list[Observation]:
    """Keep only the newest observation per endpoint so cumulative counters are
    not summed across detector runs."""
    latest: dict[tuple[str, str | None], Observation] = {}
    for observation in observations:
        key = (observation.endpoint, observation.cluster)
        current = latest.get(key)
        if current is None or _parse(observation.timestamp) > _parse(current.timestamp):
            latest[key] = observation
    return list(latest.values())


def build_report(observations: list[Observation], hours: int) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    window = [observation for observation in observations if _parse(observation.timestamp) >= start]
    latest = _latest_per_endpoint(window)
    duplicated = _endpoint_labels(latest)

    total_checks = sum(observation.checks for observation in latest)
    total_failures = sum(observation.failures for observation in latest)
    availability = ((total_checks - total_failures) / total_checks * 100.0) if total_checks else None

    weighted = [(observation.avg_response_ms, observation.checks) for observation in latest if observation.avg_response_ms is not None]
    weight = sum(checks for _, checks in weighted)
    avg_response_ms = (sum(value * checks for value, checks in weighted) / weight) if weight else None

    per_endpoint: list[dict[str, Any]] = []
    for observation in latest:
        endpoint_availability = (
            (observation.checks - observation.failures) / observation.checks * 100.0 if observation.checks else None
        )
        per_endpoint.append(
            {
                "endpoint": _label(observation, duplicated),
                "cluster": observation.cluster,
                "avg_response_ms": round(observation.avg_response_ms, 1)
                if observation.avg_response_ms is not None
                else None,
                "availability": round(endpoint_availability, 2) if endpoint_availability is not None else None,
                "checks": int(observation.checks),
                "failures": int(observation.failures),
            }
        )

    with_response = [item for item in per_endpoint if item["avg_response_ms"] is not None]
    with_availability = [item for item in per_endpoint if item["availability"] is not None]

    return {
        "window_hours": hours,
        "from": start.isoformat(),
        "to": now.isoformat(),
        "availability": round(availability, 1) if availability is not None else None,
        "avg_response_ms": round(avg_response_ms) if avg_response_ms is not None else None,
        "total_checks": int(total_checks),
        "failures": int(total_failures),
        "endpoints": per_endpoint,
        "top_fast": sorted(with_response, key=lambda item: item["avg_response_ms"])[:TABLE_SIZE],
        "top_slow": sorted(with_response, key=lambda item: item["avg_response_ms"], reverse=True)[:TABLE_SIZE],
        "most_unstable": sorted(with_availability, key=lambda item: item["availability"])[:TABLE_SIZE],
        "best_availability": sorted(with_availability, key=lambda item: item["availability"], reverse=True)[:TABLE_SIZE],
    }


def build_reports(observations: list[Observation]) -> dict[str, Any]:
    return {
        "daily": build_report(observations, 24),
        "weekly": build_report(observations, 24 * 7),
        "monthly": build_report(observations, 24 * 30),
        "observations": len(observations),
    }
